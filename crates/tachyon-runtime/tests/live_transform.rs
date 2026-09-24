//! Test live de Slice 3: pipeline real Redpanda -> transformación stateless.
//!
//! Se ejecuta con:
//!   cargo test -p tachyon-runtime --test live_transform -- --ignored
//!
//! Precondición: un broker Redpanda en `localhost:9092`. El test crea un topic
//! fresco, produce eventos, y corre la query stateless (filter + join) contra
//! el conector de fuente real, verificando la salida transformada.
//!
//! Valida el wiring completo fuente->DataFusion con el conector rdkafka real
//! (no la factory in-memory de los tests unitarios).
//!
//! Nota de alcance: DataFusion 54 solo soporta en modo streaming (fuente
//! unbounded) los operadores que no son `PipelineBreaking`. Verificado:
//! - filter y project: SÍ corren sobre streams infinitos (este test).
//! - aggregate (GROUP BY): NO — `AggregateExec` unbounded es pipeline-breaking.
//! - join (SymmetricHashJoinExec Partitioned): NO emite sobre fuentes infinitas.
//! La agregación y el join stateful llegan en la Fase 1 como operadores propios
//! (ver DESIGN.md §6.2 y MVP.md riesgo R2).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::streaming::StreamingTable;
use futures::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use tachyon_runtime::{execute_query, InputSource, StreamTable, StreamTableFactory};
use tachyon_source::consumer::RdkafkaSource;
use tachyon_source::decode::{DecodeFormat, Decoder};
use tachyon_source::stream::RedpandaPartitionStream;

const BROKER: &str = "localhost:9092";
const ORDERS_TOPIC: &str = "tachyon-orders";

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]))
}

/// Elimina y recrea el topic para que el test sea idempotente.
async fn recreate_topic(broker: &str) {
    let mut admin_config = ClientConfig::new();
    admin_config.set("bootstrap.servers", broker);
    let admin: AdminClient<DefaultClientContext> =
        admin_config.create().expect("creando admin client");
    let opts = AdminOptions::new();

    match admin.delete_topics(&[ORDERS_TOPIC], &opts).await {
        Ok(results) => {
            for r in results {
                if let Err((msg, code)) = r {
                    use rdkafka::error::RDKafkaErrorCode;
                    assert!(
                        code == RDKafkaErrorCode::UnknownTopicOrPartition
                            || msg.contains("unknown topic"),
                        "error inesperado al borrar {ORDERS_TOPIC}: {msg:?} ({code:?})"
                    );
                }
            }
        }
        Err(e) => panic!("borrando {ORDERS_TOPIC}: {e}"),
    }

    let new_topic = NewTopic::new(ORDERS_TOPIC, 2, TopicReplication::Fixed(1));
    match admin.create_topics(std::iter::once(&new_topic), &opts).await {
        Ok(results) => {
            for r in results {
                if let Err((msg, code)) = r {
                    panic!("creando {ORDERS_TOPIC}: {msg:?} ({code:?})");
                }
            }
        }
        Err(e) => panic!("creando {ORDERS_TOPIC}: {e}"),
    }
}

/// Produce 6 eventos de orders (1 cancelled).
async fn produce_events(producer: &FutureProducer) {
    let orders: [(i64, &str, f64); 6] = [
        (1, "paid", 100.0),
        (2, "paid", 200.0),
        (3, "cancelled", 300.0),
        (4, "paid", 400.0),
        (5, "shipped", 500.0),
        (6, "paid", 600.0),
    ];
    for (order_id, status, amount) in orders {
        let key = order_id.to_string();
        let msg = format!(
            r#"{{"order_id":{order_id},"status":"{status}","amount":{amount}}}"#
        );
        send(producer, ORDERS_TOPIC, &key, &msg).await;
    }
}

async fn send(producer: &FutureProducer, topic: &str, key: &str, msg: &str) {
    let record = FutureRecord::to(topic)
        .key(key)
        .payload(msg);
    let fut = producer
        .send_result(record)
        .unwrap_or_else(|(e, _)| panic!("encolando en {topic}: {e}"));
    match fut.await {
        Ok(Ok(_)) => {}
        Ok(Err((e, _))) => panic!("entrega en {topic}: {e}"),
        Err(_) => panic!("futuro de entrega cancelado en {topic}"),
    }
}

/// Cablea un source Redpanda para un topic concreto en una `StreamingTable`.
fn make_stream_table(
    broker: &str,
    group_id: &str,
    topic: &str,
    schema: SchemaRef,
    batch_size: usize,
) -> Result<StreamTable, anyhow::Error> {
    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("bootstrap.servers", broker)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "10000");

    let source = RdkafkaSource::new(&consumer_config, topic)?;
    let source = Arc::new(Mutex::new(Some(source)));
    let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
    let make_stream = Arc::new(move || {
        source
            .lock()
            .unwrap()
            .take()
            .expect("source ya consumido")
            .record_stream()
    });
    let ps = RedpandaPartitionStream::new(0, schema.clone(), decoder, batch_size, make_stream);
    Ok(Arc::new(
        StreamingTable::try_new(schema, vec![Arc::new(ps)])?
            .with_infinite_table(true),
    ))
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_redpanda_stateless_transform() {
    // --- 0. Topic fresco ---
    recreate_topic(BROKER).await;

    // --- 1. Producer: 6 orders ---
    let mut producer_config = ClientConfig::new();
    producer_config.set("bootstrap.servers", BROKER);
    let producer: FutureProducer = producer_config.create().expect("creando producer");
    produce_events(&producer).await;

    // --- 2. Factory cableada al conector real ---
    let run_id = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let factory: Box<StreamTableFactory> = Box::new(move |name, schema| {
        let group_id = format!("tachyon-{run_id}-{name}");
        let topic = match name {
            "orders" => ORDERS_TOPIC,
            other => anyhow::bail!("input inesperado: {other}"),
        };
        make_stream_table(BROKER, &group_id, topic, schema, 6)
    });

    // --- 3. Query stateless: filter (<> cancelled) + project ---
    let inputs = vec![InputSource {
        name: "orders".into(),
        schema: orders_schema(),
    }];

    let select_sql = "SELECT order_id, status \
                      FROM orders WHERE status <> 'cancelled' LIMIT 5";

    let stream = tokio::time::timeout(
        Duration::from_secs(30),
        execute_query(select_sql, &inputs, &factory),
    )
    .await
    .expect("timeout ejecutando la transformación")
    .expect("ejecutando la transformación");

    // --- 4. Verificar la salida transformada ---
    // El stream es infinito (no termina), así que leemos hasta alcanzar las 5
    // filas esperadas (6 orders - 1 cancelled) y luego descartamos el resto.
    let mut stream = stream;
    let mut batches = Vec::new();
    let mut total = 0usize;
    while total < 5 {
        let next = tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .expect("timeout esperando batches");
        match next {
            Some(batch) => {
                let batch = batch.expect("batch sin error");
                total += batch.num_rows();
                batches.push(batch);
            }
            None => break, // el stream terminó antes de tiempo
        }
    }

    // 6 orders, 1 cancelled -> 5 filas no-canceladas.
    assert_eq!(total, 5, "5 filas: orders no-cancelados");

    // El project solo expone order_id y status (no amount).
    let out_schema = batches[0].schema();
    let out_names: Vec<&str> = out_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(out_names, vec!["order_id", "status"], "solo columnas proyectadas");

    // Recopila (order_id, status) de la salida.
    let mut rows: Vec<(i64, String)> = Vec::new();
    for b in &batches {
        let order_id = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("columna order_id");
        let status = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("columna status");
        for i in 0..b.num_rows() {
            rows.push((order_id.value(i), status.value(i).to_string()));
        }
    }
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (1, "paid".to_string()),
            (2, "paid".to_string()),
            (4, "paid".to_string()),
            (5, "shipped".to_string()),
            (6, "paid".to_string()),
        ],
        "el filter quita el order 3 (cancelled)"
    );
}
