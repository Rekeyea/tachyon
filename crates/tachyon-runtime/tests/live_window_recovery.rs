//! Crash del camino de ventana.
//!
//! Dos muertes, el mismo `commit_user`:
//! - Después del snapshot la ventana cerrada no se vuelve a sumar, y el
//!   segundo arranque no relee esos eventos.
//! - Antes del snapshot no hay fila. El replay reconstruye la ventana abierta
//!   y la cierra una sola vez.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use paimon::spec::{BigIntType, DataType as PDataType};
use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tachyon_config::PipelineConfig;
use tachyon_metrics::InstanceMetrics;
use tachyon_runtime::{run_pipeline, PipelineHandle, PreparedInput, RunOptions};
use tachyon_sink::writer::{create_test_table, read_table_rows, PaimonSink};
use tachyon_sql::parse_sql;

const BROKER: &str = "localhost:9092";
const SQL: &str = "INSERT INTO orders_lake \
    SELECT order_id, window_start, window_end, COUNT(*) AS n, SUM(amount) AS amount \
    FROM orders \
    GROUP BY order_id, TUMBLE(event_time, INTERVAL '1' MINUTE)";

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn config_yaml(warehouse: &str, topic: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: window-recovery
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
    watermark:
      column: event_time
      lag: 1ms
output:
  name: orders_lake
  table: default.orders_window
  key: order_id
  bucket: 1
  sequence_field: window_end
deployment:
  partitions: 1
  commit_interval: 2s
"#
    ))
    .expect("config")
}

async fn fresh_topic(topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("admin");
    let _ = admin.delete_topics(&[topic], &Default::default()).await;
    admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &Default::default(),
        )
        .await
        .expect("topic");
}

async fn produce(producer: &FutureProducer, topic: &str, events: &[(i64, i64)]) {
    for (t, amount) in events {
        let payload = format!("{{\"order_id\":1,\"event_time\":{t},\"amount\":{amount}}}");
        producer
            .send(
                FutureRecord::to(topic).key("1").payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }
}

async fn read_windows(table: &paimon::table::Table) -> Vec<(i64, i64, i64, i64)> {
    let Ok(batches) = read_table_rows(table).await else {
        return vec![];
    };
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("order_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let starts = batch
            .column_by_name("window_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ns = batch
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let amounts = batch
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), starts.value(i), ns.value(i), amounts.value(i)));
        }
    }
    rows.sort();
    rows
}

async fn wait_rows(
    table: &paimon::table::Table,
    task: &mut Option<tokio::task::JoinHandle<Result<PipelineHandle, anyhow::Error>>>,
    ready: impl Fn(&[(i64, i64, i64, i64)]) -> bool,
) -> Vec<(i64, i64, i64, i64)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    while std::time::Instant::now() < deadline {
        if task.as_ref().is_some_and(|task| task.is_finished()) {
            match task.take().unwrap().await {
                Ok(Ok(_)) => panic!("el pipeline terminó"),
                Ok(Err(e)) => panic!("el pipeline falló: {e:#}"),
                Err(e) => panic!("tarea abortada: {e}"),
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        let rows = read_windows(table).await;
        if ready(&rows) {
            return rows;
        }
    }
    read_windows(table).await
}

fn start(
    warehouse: &str,
    topic: &str,
    table: &paimon::table::Table,
    identity: &str,
) -> (
    tokio::task::JoinHandle<Result<PipelineHandle, anyhow::Error>>,
    Arc<InstanceMetrics>,
) {
    let parsed = parse_sql(SQL).expect("sql");
    let window = parsed.window.expect("ventana");
    let select_sql = parsed.select_sql;
    let config = Arc::new(config_yaml(warehouse, topic));
    let options = RunOptions {
        commit_interval: Duration::from_secs(2),
        metrics_bind: Some("127.0.0.1:0".parse().unwrap()),
        group_id: identity.to_string(),
        commit_user: identity.to_string(),
    };
    let sink =
        PaimonSink::from_table(table.clone(), "order_id", 1, Some("window_end")).expect("sink");
    let codecs = HashMap::from([("orders".into(), PreparedInput::json(input_schema()))]);
    let metrics = Arc::new(InstanceMetrics::new());
    let metrics_task = metrics.clone();
    let task = tokio::spawn(async move {
        run_pipeline(
            &config,
            &select_sql,
            &options,
            sink,
            &codecs,
            &metrics_task,
            Some(&window),
        )
        .await
    });
    (task, metrics)
}

async fn fresh_table(warehouse: &str) -> paimon::table::Table {
    create_test_table(
        warehouse,
        "default",
        "orders_window",
        &[
            (
                "order_id",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
            (
                "window_start",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
            ("window_end", PDataType::BigInt(BigIntType::new())),
            ("n", PDataType::BigInt(BigIntType::new())),
            ("amount", PDataType::BigInt(BigIntType::new())),
        ],
        &["order_id", "window_start"],
        1,
        Some("window_end"),
    )
    .await
    .expect("tabla")
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_window_restart_does_not_double_count() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();

    let topic = format!("tachyon-window-crash-{}", std::process::id());
    let warehouse =
        std::env::temp_dir().join(format!("tachyon-window-crash-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("warehouse");
    let warehouse = warehouse.to_string_lossy().to_string();
    let table = fresh_table(&warehouse).await;
    fresh_topic(&topic).await;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("producer");
    let identity = format!("tachyon-window-crash-{}", std::process::id());

    produce(&producer, &topic, &[(10_000, 10), (20_000, 5), (70_000, 1)]).await;
    let (task, _) = start(&warehouse, &topic, &table, &identity);
    let mut task = Some(task);
    let rows = wait_rows(&table, &mut task, |rows| rows == [(1, 0, 2, 15)]).await;
    assert_eq!(
        rows,
        vec![(1, 0, 2, 15)],
        "la ventana cerrada antes del crash"
    );
    if let Some(task) = task.take() {
        task.abort();
        let _ = task.await;
    }

    let (task, metrics) = start(&warehouse, &topic, &table, &identity);
    let mut task = Some(task);
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        task.as_ref().is_some_and(|task| !task.is_finished()),
        "el segundo arranque murió"
    );
    assert_eq!(
        metrics.rows_read.load(Ordering::Relaxed),
        0,
        "el restart releyó eventos ya commiteados"
    );
    assert_eq!(read_windows(&table).await, vec![(1, 0, 2, 15)]);

    produce(&producer, &topic, &[(130_000, 7)]).await;
    let rows = wait_rows(&table, &mut task, |rows| rows.len() == 2).await;
    assert_eq!(
        rows,
        vec![(1, 0, 2, 15), (1, 60_000, 1, 1)],
        "la ventana de los 70s se cerró una sola vez"
    );
    assert_eq!(
        metrics.rows_read.load(Ordering::Relaxed),
        1,
        "el segundo arranque tenía que leer solo el evento nuevo"
    );
    if let Some(task) = task.take() {
        task.abort();
    }
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_window_crash_before_snapshot_replays_once() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();

    let topic = format!("tachyon-window-open-{}", std::process::id());
    let warehouse =
        std::env::temp_dir().join(format!("tachyon-window-open-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("warehouse");
    let warehouse = warehouse.to_string_lossy().to_string();
    let table = fresh_table(&warehouse).await;
    fresh_topic(&topic).await;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("producer");
    let identity = format!("tachyon-window-open-{}", std::process::id());

    // Los dos eventos quedan en la ventana abierta. No hay fila que commitear.
    produce(&producer, &topic, &[(10_000, 10), (20_000, 5)]).await;
    let (task, _) = start(&warehouse, &topic, &table, &identity);
    let mut task = Some(task);
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(
        task.as_ref().is_some_and(|task| !task.is_finished()),
        "el pipeline murió con la ventana abierta"
    );
    let open_rows = read_windows(&table).await;
    assert!(
        open_rows.is_empty(),
        "un epoch sin cierre no debe dejar fila: {open_rows:?}"
    );
    if let Some(task) = task.take() {
        task.abort();
        let _ = task.await;
    }

    let (task, metrics) = start(&warehouse, &topic, &table, &identity);
    let mut task = Some(task);
    produce(&producer, &topic, &[(70_000, 1)]).await;
    let rows = wait_rows(&table, &mut task, |rows| !rows.is_empty()).await;
    assert_eq!(
        rows,
        vec![(1, 0, 2, 15)],
        "el replay reconstruyó la ventana y la cerró una vez"
    );
    assert_eq!(
        metrics.rows_read.load(Ordering::Relaxed),
        3,
        "sin snapshot había que releer los tres eventos"
    );
    if let Some(task) = task.take() {
        task.abort();
    }
    let _ = std::fs::remove_dir_all(&warehouse);
}
