//! Ventana tumbling contra Redpanda: dos eventos del mismo minuto y uno del
//! siguiente. El tercero cierra la ventana y Paimon recibe una fila.

use std::collections::HashMap;
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
use tachyon_runtime::{run_pipeline, PreparedInput, RunOptions};
use tachyon_sink::writer::{create_test_table, read_table_rows, PaimonSink};
use tachyon_sql::parse_sql;

const BROKER: &str = "localhost:9092";
const TOPIC: &str = "tachyon-window-orders";

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn config_yaml(warehouse: &str, topic: &str, table: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: window-orders
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
  table: default.{table}
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
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", BROKER);
    let admin: AdminClient<DefaultClientContext> = cc.create().expect("admin");
    let _ = admin.delete_topics(&[topic], &Default::default()).await;
    admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &Default::default(),
        )
        .await
        .expect("topic");
}

async fn produce(topic: &str, events: &[(i64, i64)]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("producer");
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

fn read_windows(batches: &[arrow::array::RecordBatch]) -> Vec<(i64, i64, i64, i64, i64)> {
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
        let ends = batch
            .column_by_name("window_end")
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
            rows.push((
                ids.value(i),
                starts.value(i),
                ends.value(i),
                ns.value(i),
                amounts.value(i),
            ));
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_tumble_minute_closes_into_paimon() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    let warehouse = std::env::temp_dir().join(format!("tachyon-window-wh-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("warehouse");
    let warehouse = warehouse.to_string_lossy().to_string();

    let table = create_test_table(
        &warehouse,
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
    .expect("tabla");

    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", BROKER);
    let admin: AdminClient<DefaultClientContext> = cc.create().expect("admin");
    let _ = admin.delete_topics(&[TOPIC], &Default::default()).await;
    admin
        .create_topics(
            &[NewTopic::new(TOPIC, 1, TopicReplication::Fixed(1))],
            &Default::default(),
        )
        .await
        .expect("topic");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("producer");
    // 10s y 20s caen en el minuto [0, 60s). 70s abre el siguiente y cierra el primero.
    for (t, amount) in [(10_000i64, 10i64), (20_000, 5), (70_000, 1)] {
        let payload = format!("{{\"order_id\":1,\"event_time\":{t},\"amount\":{amount}}}");
        producer
            .send(
                FutureRecord::to(TOPIC).key("1").payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }

    let sql = "INSERT INTO orders_lake \
               SELECT order_id, window_start, window_end, COUNT(*) AS n, SUM(amount) AS amount \
               FROM orders \
               GROUP BY order_id, TUMBLE(event_time, INTERVAL '1' MINUTE)";
    let parsed = parse_sql(sql).expect("sql");
    let config = Arc::new(config_yaml(&warehouse, TOPIC, "orders_window"));
    let options = RunOptions {
        commit_interval: Duration::from_secs(2),
        metrics_bind: Some("127.0.0.1:0".parse().unwrap()),
        group_id: format!("tachyon-window-{}", std::process::id()),
        commit_user: format!("tachyon-window-{}", std::process::id()),
    };
    let sink =
        PaimonSink::from_table(table.clone(), "order_id", 1, Some("window_end")).expect("sink");
    let codecs = HashMap::from([("orders".to_string(), PreparedInput::json(input_schema()))]);
    let metrics = Arc::new(tachyon_metrics::InstanceMetrics::new());
    let window = parsed.window.expect("ventana");
    let select_sql = parsed.select_sql.clone();
    let run_task = tokio::spawn(async move {
        run_pipeline(
            &config,
            &select_sql,
            &options,
            sink,
            &codecs,
            &metrics,
            Some(&window),
        )
        .await
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut got: Vec<(i64, i64, i64, i64)> = vec![];
    while std::time::Instant::now() < deadline {
        if run_task.is_finished() {
            match run_task.await {
                Ok(Ok(_)) => panic!("el pipeline terminó"),
                Ok(Err(e)) => panic!("el pipeline falló: {e:#}"),
                Err(e) => panic!("tarea abortada: {e}"),
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        if let Ok(batches) = read_table_rows(&table).await {
            got = batches
                .iter()
                .flat_map(|batch| {
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
                    (0..batch.num_rows())
                        .map(|i| (ids.value(i), starts.value(i), ns.value(i), amounts.value(i)))
                        .collect::<Vec<_>>()
                })
                .collect();
            if !got.is_empty() {
                break;
            }
        }
    }
    run_task.abort();
    assert_eq!(
        got,
        vec![(1, 0, 2, 15)],
        "se esperaba la ventana [0, 60000) con dos eventos"
    );
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_hop_puts_one_event_in_two_windows() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();

    let topic = format!("tachyon-hop-{}", std::process::id());
    let warehouse = std::env::temp_dir().join(format!("tachyon-hop-wh-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("warehouse");
    let warehouse = warehouse.to_string_lossy().to_string();
    let table = create_test_table(
        &warehouse,
        "default",
        "orders_hop",
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
    .expect("tabla");
    fresh_topic(&topic).await;
    // 12s cae en [5s, 15s) y [10s, 20s). 20.001s sube el watermark por encima
    // de 20s (el lag es 1ms) y cierra esas dos. No entra en ninguna de las dos.
    produce(&topic, &[(12_000, 3), (20_001, 1)]).await;

    let sql = "INSERT INTO orders_lake \
               SELECT order_id, window_start, window_end, COUNT(*) AS n, SUM(amount) AS amount \
               FROM orders \
               GROUP BY order_id, HOP(event_time, INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
    let rows = run_until(&warehouse, &topic, "orders_hop", &table, sql, 2).await;
    assert_eq!(
        rows,
        vec![(1, 5_000, 15_000, 1, 3), (1, 10_000, 20_000, 1, 3)],
        "un evento de hop tiene que salir en dos ventanas"
    );
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_session_merges_inside_the_gap() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();

    let topic = format!("tachyon-session-{}", std::process::id());
    let warehouse = std::env::temp_dir().join(format!("tachyon-session-wh-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("warehouse");
    let warehouse = warehouse.to_string_lossy().to_string();
    let table = create_test_table(
        &warehouse,
        "default",
        "orders_session",
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
    .expect("tabla");
    fresh_topic(&topic).await;
    // 0 y 4s se fusionan. 9s cae justo en fin+gap y abre otra. 20s cierra las dos.
    produce(&topic, &[(0, 2), (4_000, 3), (9_000, 1), (20_000, 1)]).await;

    let sql = "INSERT INTO orders_lake \
               SELECT order_id, window_start, window_end, COUNT(*) AS n, SUM(amount) AS amount \
               FROM orders \
               GROUP BY order_id, SESSION(event_time, INTERVAL '5' SECOND)";
    let rows = run_until(&warehouse, &topic, "orders_session", &table, sql, 2).await;
    assert_eq!(
        rows,
        vec![(1, 0, 4_000, 2, 5), (1, 9_000, 9_000, 1, 1)],
        "la session de 0 y 4s se fusiona; la de 9s queda aparte"
    );
    let _ = std::fs::remove_dir_all(&warehouse);
}

async fn run_until(
    warehouse: &str,
    topic: &str,
    table_name: &str,
    table: &paimon::table::Table,
    sql: &str,
    want: usize,
) -> Vec<(i64, i64, i64, i64, i64)> {
    let parsed = parse_sql(sql).expect("sql");
    let window = parsed.window.expect("ventana");
    let select_sql = parsed.select_sql;
    let config = Arc::new(config_yaml(warehouse, topic, table_name));
    let options = RunOptions {
        commit_interval: Duration::from_secs(2),
        metrics_bind: Some("127.0.0.1:0".parse().unwrap()),
        group_id: format!("tachyon-{table_name}-{}", std::process::id()),
        commit_user: format!("tachyon-{table_name}-{}", std::process::id()),
    };
    let sink =
        PaimonSink::from_table(table.clone(), "order_id", 1, Some("window_end")).expect("sink");
    let codecs = HashMap::from([("orders".to_string(), PreparedInput::json(input_schema()))]);
    let metrics = Arc::new(tachyon_metrics::InstanceMetrics::new());
    let run_task = tokio::spawn(async move {
        run_pipeline(
            &config,
            &select_sql,
            &options,
            sink,
            &codecs,
            &metrics,
            Some(&window),
        )
        .await
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    let mut got = Vec::new();
    while std::time::Instant::now() < deadline {
        if run_task.is_finished() {
            match run_task.await {
                Ok(Ok(_)) => panic!("el pipeline terminó"),
                Ok(Err(e)) => panic!("el pipeline falló: {e:#}"),
                Err(e) => panic!("tarea abortada: {e}"),
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Ok(batches) = read_table_rows(table).await {
            got = read_windows(&batches);
            if got.len() >= want {
                break;
            }
        }
    }
    run_task.abort();
    got
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_two_consumers_count_each_partition_once() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    let topic = format!("tachyon-window-two-{}", std::process::id());
    let warehouse = std::env::temp_dir().join(format!("tachyon-window-two-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("warehouse");
    let warehouse = warehouse.to_string_lossy().to_string();
    let table = create_test_table(
        &warehouse,
        "default",
        "orders_two",
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
    .expect("tabla");

    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", BROKER);
    let admin: AdminClient<DefaultClientContext> = cc.create().expect("admin");
    let _ = admin.delete_topics(&[&topic], &Default::default()).await;
    admin
        .create_topics(
            &[NewTopic::new(&topic, 2, TopicReplication::Fixed(1))],
            &Default::default(),
        )
        .await
        .expect("topic");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("producer");
    // Primero solo lo que cae adentro del minuto. El cierre se produce
    // después, cuando las dos particiones ya entraron al operador: si el
    // de 70s llegara antes, el watermark dejaría tarde al otro.
    for (partition, order_id, t, amount) in [(0, 1, 10_000, 10), (1, 2, 20_000, 7)] {
        let payload = format!("{{\"order_id\":{order_id},\"event_time\":{t},\"amount\":{amount}}}");
        producer
            .send(
                FutureRecord::to(&topic)
                    .partition(partition)
                    .key(&order_id.to_string())
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }

    let sql = "INSERT INTO orders_lake \
               SELECT order_id, window_start, window_end, COUNT(*) AS n, SUM(amount) AS amount \
               FROM orders \
               GROUP BY order_id, TUMBLE(event_time, INTERVAL '1' MINUTE)";
    let parsed = parse_sql(sql).expect("sql");
    let config = Arc::new(
        serde_yaml::from_str::<PipelineConfig>(&format!(
            r#"
pipeline:
  name: window-two
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
  table: default.orders_two
  key: order_id
  bucket: 1
  sequence_field: window_end
deployment:
  partitions: 2
  consumers_per_topic: 2
  commit_interval: 2s
"#
        ))
        .expect("config"),
    );
    let options = RunOptions {
        commit_interval: Duration::from_secs(2),
        metrics_bind: Some("127.0.0.1:0".parse().unwrap()),
        group_id: format!("tachyon-two-{}", std::process::id()),
        commit_user: format!("tachyon-two-{}", std::process::id()),
    };
    let sink =
        PaimonSink::from_table(table.clone(), "order_id", 1, Some("window_end")).expect("sink");
    let codecs = HashMap::from([("orders".to_string(), PreparedInput::json(input_schema()))]);
    let metrics = Arc::new(tachyon_metrics::InstanceMetrics::new());
    let watch = metrics.clone();
    let window = parsed.window.expect("ventana");
    let select_sql = parsed.select_sql;
    let run_task = tokio::spawn(async move {
        run_pipeline(
            &config,
            &select_sql,
            &options,
            sink,
            &codecs,
            &metrics,
            Some(&window),
        )
        .await
    });
    let opened = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < opened {
        if run_task.is_finished() {
            match run_task.await {
                Ok(Ok(_)) => panic!("el pipeline terminó"),
                Ok(Err(e)) => panic!("el pipeline falló: {e:#}"),
                Err(e) => panic!("tarea abortada: {e}"),
            }
        }
        if watch.rows_read.load(std::sync::atomic::Ordering::Relaxed) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        watch.rows_read.load(std::sync::atomic::Ordering::Relaxed) >= 2,
        "no llegaron los dos eventos abiertos"
    );
    for (partition, order_id, t, amount) in [(0, 1, 70_000, 1), (1, 2, 70_000, 1)] {
        let payload = format!("{{\"order_id\":{order_id},\"event_time\":{t},\"amount\":{amount}}}");
        producer
            .send(
                FutureRecord::to(&topic)
                    .partition(partition)
                    .key(&order_id.to_string())
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .expect("produce cierre");
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut got = Vec::new();
    while std::time::Instant::now() < deadline {
        if run_task.is_finished() {
            match run_task.await {
                Ok(Ok(_)) => panic!("el pipeline terminó"),
                Ok(Err(e)) => panic!("el pipeline falló: {e:#}"),
                Err(e) => panic!("tarea abortada: {e}"),
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Ok(batches) = read_table_rows(&table).await {
            got = read_windows(&batches);
            if got.len() >= 2 {
                break;
            }
        }
    }
    run_task.abort();
    assert_eq!(
        got,
        vec![(1, 0, 60_000, 1, 10), (2, 0, 60_000, 1, 7)],
        "cada partición se agrega una vez con dos consumidores"
    );
    let _ = std::fs::remove_dir_all(&warehouse);
}
