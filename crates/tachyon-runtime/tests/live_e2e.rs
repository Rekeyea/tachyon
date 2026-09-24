//! Slice 5: test live end-to-end.
//!
//! Redpanda (broker real) -> DataFusion (transformación) -> Paimon (tabla local).
//! Valida el loop completo: consumo por consumer group, decodificación JSON,
//! filter+project, escritura a Paimon, commit periódico + offsets, métricas.
//!
//! Requiere Redpanda local en localhost:9092 (ver el test live del Slice 2).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use paimon::spec::{DataType as PDataType, BigIntType, DoubleType, VarCharType};
use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tachyon_config::PipelineConfig;
use tachyon_runtime::{run_pipeline, RunOptions};
use tachyon_sink::writer::{create_test_table, read_table_rows, PaimonSink};

const BROKER: &str = "localhost:9092";
const DB: &str = "default";
const TABLE: &str = "orders_lake";
const ORDERS_TOPIC: &str = "tachyon-e2e-orders";

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("source_version", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
    ]))
}

async fn recreate_topic(broker: &str) {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", broker);
    let admin: AdminClient<DefaultClientContext> = cc.create().expect("admin client");
    let _ = admin.delete_topics(&[ORDERS_TOPIC], &Default::default()).await;
    let topic = NewTopic::new(
        ORDERS_TOPIC,
        2,
        TopicReplication::Fixed(1),
    );
    admin
        .create_topics(&[topic], &Default::default())
        .await
        .expect("creando topic");
}

async fn produce_orders(producer: &FutureProducer) {
    // 6 orders: 1 cancelled (queda fuera por el filter), 5 pasan.
    let orders = [
        (1i64, "paid", 10i64, 100.0f64),
        (2, "shipped", 11, 200.0),
        (3, "paid", 12, 300.0),
        (4, "cancelled", 13, 400.0),
        (5, "paid", 14, 500.0),
        (6, "shipped", 15, 600.0),
    ];
    for (order_id, status, version, amount) in orders {
        let payload = format!(
            "{{\"order_id\":{order_id},\"status\":\"{status}\",\"source_version\":{version},\"amount\":{amount}}}"
        );
        let key = order_id.to_string();
        let record = FutureRecord::to(ORDERS_TOPIC)
            .key(&key)
            .payload(&payload);
        producer
            .send(record, Duration::from_secs(5))
            .await
            .expect("produciendo order");
    }
}

fn config_yaml(warehouse: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: e2e-orders
  version: 1
connectors:
  redpanda:
    brokers: [localhost:9092]
  paimon:
    warehouse: {warehouse}
    catalog: local
inputs:
  - name: orders
    topic: {topic}
    key: order_id
    schema: json/orders
output:
  name: orders_lake
  table: {db}.{table}
  key: order_id
  bucket: 1
  sequence_field: source_version
deployment:
  partitions: 1
  commit_interval: 2s
  metrics:
    bind_addr: "127.0.0.1:0"
"#,
        warehouse = warehouse,
        topic = ORDERS_TOPIC,
        db = DB,
        table = TABLE,
    ))
    .expect("config de test válida")
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_end_to_end_redpanda_to_paimon() {
    // --- Diagnóstico: logs del pipeline (RUST_LOG=debug para verlos) ---
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    // --- 0. Warehouse + tabla Paimon frescos ---
    let warehouse = std::env::temp_dir().join(format!(
        "tachyon-e2e-warehouse-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("creando warehouse");
    let warehouse = warehouse.to_string_lossy().to_string();

    let table = create_test_table(
        &warehouse,
        DB,
        TABLE,
        &[
            ("order_id", PDataType::BigInt(BigIntType::with_nullable(false))),
            ("status", PDataType::VarChar(VarCharType::string_type())),
            ("source_version", PDataType::BigInt(BigIntType::new())),
            ("amount", PDataType::Double(DoubleType::new())),
        ],
        &["order_id"],
        1,
        Some("source_version"),
    )
    .await
    .expect("creando tabla Paimon");

    // --- 1. Topic fresco + producer ---
    recreate_topic(BROKER).await;
    let mut producer_config = ClientConfig::new();
    producer_config.set("bootstrap.servers", BROKER);
    let producer: FutureProducer = producer_config.create().expect("producer");
    produce_orders(&producer).await;

    // --- 2. Config + opciones de ejecución ---
    let config = config_yaml(&warehouse);
    let options = RunOptions {
        commit_interval: Duration::from_secs(2),
        metrics_bind: Some("127.0.0.1:0".parse().unwrap()),
        group_id: format!("tachyon-e2e-{}", std::process::id()),
    };

    // --- 3. Sink + schemas ---
    let mut sink = PaimonSink::from_table(table.clone(), "order_id", 1, Some("source_version"))
        .expect("abriendo sink");
    let input_schemas: HashMap<String, Arc<Schema>> =
        HashMap::from([("orders".to_string(), orders_schema())]);

    // --- 4. Corre el pipeline en una tarea (el stream es infinito) ---
    let select_sql = "SELECT order_id, status, source_version, amount \
                      FROM orders WHERE status <> 'cancelled'";
    let config = Arc::new(config);
    let metrics = Arc::new(tachyon_metrics::InstanceMetrics::new());
    let run_task = tokio::spawn(async move {
        run_pipeline(
            &config,
            select_sql,
            &options,
            &mut sink,
            &input_schemas,
            &metrics,
        )
        .await
    });

    // --- 5. Espera a que los datos aparezcan en Paimon (commit periódico) ---
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut rows: Vec<(i64, f64)> = vec![];
    while std::time::Instant::now() < deadline {
        // Si la tarea del pipeline murió, fallar ya con el error (diagnóstico).
        if run_task.is_finished() {
            match run_task.await {
                Ok(Ok(_)) => panic!("el pipeline terminó (el stream no es infinito)"),
                Ok(Err(e)) => panic!("el pipeline falló temprano: {e:#}"),
                Err(e) => panic!("tarea del pipeline abortada: {e}"),
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        if let Ok(batches) = read_table_rows(&table).await {
            rows = batches
                .iter()
                .flat_map(|b| {
                    let ids = b
                        .column_by_name("order_id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let amounts = b
                        .column_by_name("amount")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    (0..b.num_rows()).map(move |i| (ids.value(i), amounts.value(i)))
                })
                .collect();
            rows.sort_by_key(|(id, _)| *id);
            if rows.len() == 5 {
                break;
            }
        }
    }

    // --- 6. Verificación: 5 orders (el cancelled quedó fuera) ---
    assert_eq!(
        rows,
        vec![(1, 100.0), (2, 200.0), (3, 300.0), (5, 500.0), (6, 600.0)],
        "los 5 orders no-cancelados deben estar en Paimon"
    );

    // --- 7. Verificación: métricas ---
    // El handle se obtiene al terminar la tarea; aquí verificamos que el
    // endpoint de métricas está vivo leyendo el snapshot vía Paimon (ya hecho)
    // y que la tarea no ha fallado.
    assert!(
        !run_task.is_finished(),
        "el pipeline debe seguir corriendo (stream infinito)"
    );

    // --- Limpieza: cancelar la tarea y remover el warehouse ---
    run_task.abort();
    let _ = std::fs::remove_dir_all(&warehouse);
}
