//! Test live contra un broker Redpanda local (Docker).
//!
//! Se ejecuta con:
//!   cargo test -p tachyon-source --test live_redpanda -- --ignored
//!
//! Precondición: un broker en `localhost:9092` con el topic `tachyon-orders`
//! (2 particiones). El topic se crea si no existe.
//!
//! Flujo completo del conector de fuente:
//!   producir (rdkafka) -> RdkafkaSource (consumer group) -> Decoder (JSON)
//!   -> RedpandaPartitionStream -> StreamingTable (DataFusion) -> SQL.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::prelude::SessionContext;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use tachyon_source::consumer::RdkafkaSource;
use tachyon_source::decode::{DecodeFormat, Decoder};
use tachyon_source::stream::RedpandaPartitionStream;

const BROKER: &str = "localhost:9092";
const TOPIC: &str = "tachyon-orders";

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]))
}

/// Produce `n` mensajes JSON al topic y espera sus confirmaciones.
async fn produce_messages(producer: &FutureProducer, n: usize) {
    for i in 1..=n {
        let key = i.to_string();
        let msg = format!(r#"{{"order_id":{i},"status":"paid","amount":{i}.0}}"#);
        let record = FutureRecord::to(TOPIC)
            .key(key.as_str())
            .payload(msg.as_str());
        let fut = producer
            .send_result(record)
            .unwrap_or_else(|(e, _)| panic!("encolando mensaje {i}: {e}"));
        match fut.await {
            Ok(Ok(_delivery)) => {}
            Ok(Err((e, _msg))) => panic!("entrega del mensaje {i}: {e}"),
            Err(_) => panic!("futuro de entrega cancelado para el mensaje {i}"),
        }
    }
}

/// Elimina y recrea el topic para que el test sea idempotente (sin leftover
/// de corridas anteriores).
async fn recreate_topic(broker: &str) {
    let mut admin_config = ClientConfig::new();
    admin_config.set("bootstrap.servers", broker);
    let admin: AdminClient<DefaultClientContext> =
        admin_config.create().expect("creando admin client");
    let opts = AdminOptions::new();

    // Borrar el topic si existe (el error "no existe" se ignora).
    match admin.delete_topics(&[TOPIC], &opts).await {
        Ok(results) => {
            for r in results {
                // El error es una tupla `(msg, code)`; `UnknownTopicOrPartition`
                // es esperado si el topic no existe.
                if let Err((msg, code)) = r {
                    use rdkafka::error::RDKafkaErrorCode;
                    assert!(
                        code == RDKafkaErrorCode::UnknownTopicOrPartition
                            || msg.contains("unknown topic"),
                        "error inesperado al borrar topic: {msg:?} ({code:?})"
                    );
                }
            }
        }
        Err(e) => panic!("borrando topic: {e}"),
    }

    // Crear el topic con 2 particiones.
    let new_topic = NewTopic::new(TOPIC, 2, TopicReplication::Fixed(1));
    match admin.create_topics(std::iter::once(&new_topic), &opts).await {
        Ok(results) => {
            for r in results {
                if let Err((msg, code)) = r {
                    panic!("creando topic: {msg:?} ({code:?})");
                }
            }
        }
        Err(e) => panic!("creando topic: {e}"),
    }
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn end_to_end_redpanda_to_datafusion() {
    // --- 0. Topic fresco por corrida ---
    recreate_topic(BROKER).await;

    // --- 1. Producer: produce 4 mensajes JSON ---
    let mut producer_config = ClientConfig::new();
    producer_config.set("bootstrap.servers", BROKER);
    let producer: FutureProducer = producer_config.create().expect("creando producer");
    produce_messages(&producer, 4).await;

    // --- 2. Consumer: grupo único por corrida, desde el inicio ---
    let group_id = format!(
        "tachyon-live-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("bootstrap.servers", BROKER)
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "10000");

    let source = RdkafkaSource::new(&consumer_config, TOPIC).expect("creando source");
    // Cada `execute()` del plan consume el stream una sola vez; el factory
    // toma el source (una sola ejecución por consulta).
    let source = Arc::new(Mutex::new(Some(source)));
    let make_stream = Arc::new(move || {
        source
            .lock()
            .unwrap()
            .take()
            .expect("source ya consumido")
            .record_stream()
    });

    // --- 3. Decoder + PartitionStream + StreamingTable ---
    let schema = orders_schema();
    let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
    let ps = RedpandaPartitionStream::new(0, schema.clone(), decoder, 4, make_stream);

    let table = StreamingTable::try_new(schema, vec![Arc::new(ps)])
        .expect("tabla")
        .with_infinite_table(true);
    let ctx = SessionContext::new();
    ctx.register_table("orders", Arc::new(table)).expect("register");

    // --- 4. SQL sobre el stream infinito (LIMIT cierra la consulta) ---
    let df = ctx
        .sql("SELECT order_id, status, amount FROM orders LIMIT 4")
        .await
        .expect("plan");
    let batches = tokio::time::timeout(Duration::from_secs(30), df.collect())
        .await
        .expect("timeout esperando 4 filas")
        .expect("collect");

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 4, "se esperaban 4 filas desde Redpanda");

    // Verifica el contenido: los 4 order_ids producidos.
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("columna order_id")
                .values()
                .to_vec()
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3, 4], "order_ids producidos");

    let statuses: Vec<&str> = batches
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("columna status");
            (0..b.num_rows()).map(move |i| arr.value(i))
        })
        .collect();
    assert!(statuses.iter().all(|s| *s == "paid"));

    let amounts: Vec<f64> = batches
        .iter()
        .flat_map(|b| {
            b.column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("columna amount")
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(amounts.len(), 4);
}
