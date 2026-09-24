//! Slice 5 (criterio 4 del MVP): recuperación ante fallo.
//!
//! Matar una instancia y re-iniciarla con el mismo consumer group debe:
//! 1. Re-consumir desde el último offset commitado (no desde el inicio).
//! 2. No perder eventos: la salida cubre todos los eventos.
//! 3. No duplicar en la salida (idempotencia por clave + sequence en Paimon).
//!
//! Escenario:
//! - Lote 1: orders 1-3 (1 cancelled) -> 2 en Paimon. Matar la instancia.
//! - Lote 2: orders 4-6 (1 cancelled) -> re-iniciar (mismo consumer group).
//! - Esperado: 4 orders en Paimon, y la instancia nueva debe haber leído
//!   solo los 2 eventos nuevos (rows_read == 2, no 4) -> prueba de que
//!   re-consumió desde el offset commitado, no desde el inicio.
//!
//! Requiere Redpanda local en localhost:9092.

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
use tachyon_metrics::InstanceMetrics;
use tachyon_runtime::{run_pipeline, RunOptions};
use tachyon_sink::writer::{create_test_table, read_table_rows, PaimonSink};

const BROKER: &str = "localhost:9092";
const DB: &str = "default";
const TABLE: &str = "orders_lake";
const ORDERS_TOPIC: &str = "tachyon-recovery-orders";

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
    let topic = NewTopic::new(ORDERS_TOPIC, 2, TopicReplication::Fixed(1));
    admin
        .create_topics(&[topic], &Default::default())
        .await
        .expect("creando topic");
}

/// Produce un lote de orders (con key = order_id para el particionado).
async fn produce_orders(producer: &FutureProducer, orders: &[(i64, &str, i64, f64)]) {
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
  name: recovery-orders
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
  partitions: 2
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

/// Lee las filas (order_id, amount) de una tabla Paimon.
async fn read_rows(table: &paimon::table::Table) -> Vec<(i64, f64)> {
    match read_table_rows(table).await {
        Ok(batches) => batches
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
            .collect(),
        Err(_) => vec![],
    }
}

/// Arranca una instancia del pipeline (stream infinito) y devuelve la tarea.
async fn start_instance(
    warehouse: &str,
    table: &paimon::table::Table,
    group_id: &str,
    select_sql: &str,
) -> (tokio::task::JoinHandle<Result<tachyon_runtime::PipelineHandle, anyhow::Error>>, Arc<InstanceMetrics>)
{
    let config = Arc::new(config_yaml(warehouse));
    let options = RunOptions {
        commit_interval: Duration::from_secs(2),
        metrics_bind: Some("127.0.0.1:0".parse().unwrap()),
        group_id: group_id.to_string(),
    };
    let mut sink =
        PaimonSink::from_table(table.clone(), "order_id", 1, Some("source_version"))
            .expect("abriendo sink");
    let input_schemas: HashMap<String, Arc<Schema>> =
        HashMap::from([("orders".to_string(), orders_schema())]);
    let metrics = Arc::new(InstanceMetrics::new());
    let select_sql = select_sql.to_string();
    let task = tokio::spawn({
        let metrics = metrics.clone();
        async move {
            run_pipeline(&config, &select_sql, &options, &mut sink, &input_schemas, &metrics).await
        }
    });
    (task, metrics)
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_recovery_restart_from_committed_offset() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    let select_sql = "SELECT order_id, status, source_version, amount \
                      FROM orders WHERE status <> 'cancelled'";
    let group_id = format!("tachyon-recovery-{}", std::process::id());

    // --- 0. Warehouse + tabla Paimon frescos ---
    let warehouse = std::env::temp_dir().join(format!(
        "tachyon-recovery-warehouse-{}",
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

    // --- 2. Lote 1: orders 1-3 (3 es cancelled) -> 2 pasan ---
    let lote1 = [(1i64, "paid", 10i64, 100.0f64), (2, "shipped", 11, 200.0), (3, "cancelled", 12, 300.0)];
    produce_orders(&producer, &lote1).await;

    // --- 3. Instancia 1: consume el lote 1 y commita (Paimon + offsets) ---
    let (task1, _metrics1) = start_instance(&warehouse, &table, &group_id, select_sql).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut rows = vec![];
    while std::time::Instant::now() < deadline {
        if task1.is_finished() {
            match task1.await {
                Ok(Ok(_)) => panic!("instancia 1 terminó (el stream no es infinito)"),
                Ok(Err(e)) => panic!("instancia 1 falló: {e:#}"),
                Err(e) => panic!("tarea de instancia 1 abortada: {e}"),
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        rows = read_rows(&table).await;
        rows.sort_by_key(|(id, _)| *id);
        if rows.len() == 2 {
            break;
        }
    }
    assert_eq!(
        rows,
        vec![(1, 100.0), (2, 200.0)],
        "el lote 1 debe estar en Paimon antes de matar la instancia"
    );

    // --- 4. MATAR la instancia 1 ---
    // El commit de offsets (async) se emitió en el mismo tick que el commit de
    // Paimon (ya visible). Esperar un poco para que llegue al broker antes de
    // que la instancia 2 se una al grupo.
    task1.abort();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // --- 5. Lote 2: orders 4-6 (5 es cancelled) -> 2 pasan ---
    let lote2 = [(4i64, "paid", 20i64, 400.0f64), (5, "cancelled", 21, 500.0), (6, "shipped", 22, 600.0)];
    produce_orders(&producer, &lote2).await;

    // --- 6. Instancia 2: MISMO consumer group -> re-consume desde el offset commitado ---
    let (task2, metrics2) = start_instance(&warehouse, &table, &group_id, select_sql).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut rows = vec![];
    while std::time::Instant::now() < deadline {
        if task2.is_finished() {
            match task2.await {
                Ok(Ok(_)) => panic!("instancia 2 terminó (el stream no es infinito)"),
                Ok(Err(e)) => panic!("instancia 2 falló: {e:#}"),
                Err(e) => panic!("tarea de instancia 2 abortada: {e}"),
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        rows = read_rows(&table).await;
        rows.sort_by_key(|(id, _)| *id);
        if rows.len() == 4 {
            break;
        }
    }

    // --- 7. Verificación: salida completa, sin pérdidas ni duplicados ---
    assert_eq!(
        rows,
        vec![(1, 100.0), (2, 200.0), (4, 400.0), (6, 600.0)],
        "la salida debe cubrir los 4 orders de ambos lotes (sin perder, sin duplicar)"
    );

    // --- 8. Verificación: la instancia 2 re-consumió desde el offset commitado ---
    // Si hubiera re-consumido desde el inicio, rows_read sería 4 (los 2 del
    // lote 1 + los 2 del lote 2). Al re-consumir desde el offset commitado,
    // solo lee los 2 eventos nuevos.
    let rows_read = metrics2.rows_read.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        rows_read, 2,
        "la instancia reiniciada debe leer solo los 2 eventos nuevos (re-consumo desde el offset commitado), no desde el inicio"
    );

    // La instancia 2 sigue corriendo (stream infinito).
    assert!(!task2.is_finished(), "la instancia 2 debe seguir corriendo");

    // --- Limpieza ---
    task2.abort();
    let _ = std::fs::remove_dir_all(&warehouse);
}
