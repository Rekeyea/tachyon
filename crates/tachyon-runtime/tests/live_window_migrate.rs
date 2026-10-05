//! La partición cambia de proceso. El nuevo restaura la ficha del último
//! snapshot y no vuelve a sumar lo que ese snapshot ya tenía abierto.
//!
//!   cargo test -p tachyon-runtime --test live_window_migrate -- --ignored --nocapture

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

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn config(warehouse: &str, topic: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: window-migrate
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
  table: default.orders_migrate
  key: order_id
  bucket: 1
  sequence_field: window_end
deployment:
  partitions: 1
  consumers_per_topic: 1
  commit_interval: 2s
"#
    ))
    .expect("config")
}

fn rows(batches: &[arrow::array::RecordBatch]) -> Vec<(i64, i64, i64, i64, i64)> {
    let mut out = Vec::new();
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
            out.push((
                ids.value(i),
                starts.value(i),
                ends.value(i),
                ns.value(i),
                amounts.value(i),
            ));
        }
    }
    out.sort();
    out
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
                FutureRecord::to(topic)
                    .partition(0)
                    .key("1")
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_partition_moves_to_another_commit_user_without_double_count() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    let topic = format!("tachyon-migrate-{}", std::process::id());
    let warehouse = std::env::temp_dir().join(format!("tachyon-migrate-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("warehouse");
    let warehouse = warehouse.to_string_lossy().to_string();
    let table = create_test_table(
        &warehouse,
        "default",
        "orders_migrate",
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

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("admin");
    let _ = admin.delete_topics(&[&topic], &Default::default()).await;
    admin
        .create_topics(
            &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
            &Default::default(),
        )
        .await
        .expect("topic");

    // 10s y 20s cierran con el de 70s. Ese de 70s queda abierto en el minuto siguiente.
    produce(&topic, &[(10_000, 10), (20_000, 5), (70_000, 1)]).await;

    let sql = "INSERT INTO orders_lake \
               SELECT order_id, window_start, window_end, COUNT(*) AS n, SUM(amount) AS amount \
               FROM orders \
               GROUP BY order_id, TUMBLE(event_time, INTERVAL '1' MINUTE)";
    let parsed = parse_sql(sql).expect("sql");
    let window = parsed.window.expect("ventana");
    let group = format!("tachyon-migrate-{}", std::process::id());

    let config_a = Arc::new(config(&warehouse, &topic));
    let options_a = RunOptions {
        commit_interval: Duration::from_secs(2),
        metrics_bind: None,
        group_id: group.clone(),
        commit_user: format!("{group}-a"),
    };
    let sink_a =
        PaimonSink::from_table(table.clone(), "order_id", 1, Some("window_end")).expect("sink");
    let codecs_a = HashMap::from([("orders".to_string(), PreparedInput::json(input_schema()))]);
    let metrics_a = Arc::new(tachyon_metrics::InstanceMetrics::new());
    let window_a = window.clone();
    let select_a = parsed.select_sql.clone();
    let task_a = tokio::spawn(async move {
        run_pipeline(
            &config_a,
            &select_a,
            &options_a,
            sink_a,
            &codecs_a,
            &metrics_a,
            Some(&window_a),
        )
        .await
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut got = Vec::new();
    while std::time::Instant::now() < deadline {
        if task_a.is_finished() {
            match task_a.await {
                Ok(Err(e)) => panic!("el primer proceso falló: {e:#}"),
                _ => panic!("el primer proceso terminó"),
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Ok(batches) = read_table_rows(&table).await {
            got = rows(&batches);
            if got.iter().any(|row| row.1 == 0) {
                break;
            }
        }
    }
    assert_eq!(
        got,
        vec![(1, 0, 60_000, 2, 15)],
        "el primer minuto no cerró: {got:?}"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    task_a.abort();
    tokio::time::sleep(Duration::from_secs(5)).await;

    let config_b = Arc::new(config(&warehouse, &topic));
    let options_b = RunOptions {
        commit_interval: Duration::from_secs(2),
        metrics_bind: None,
        group_id: group.clone(),
        commit_user: format!("{group}-b"),
    };
    let sink_b =
        PaimonSink::from_table(table.clone(), "order_id", 1, Some("window_end")).expect("sink");
    let codecs_b = HashMap::from([("orders".to_string(), PreparedInput::json(input_schema()))]);
    let metrics_b = Arc::new(tachyon_metrics::InstanceMetrics::new());
    let watch = metrics_b.clone();
    let window_b = window;
    let select_b = parsed.select_sql;
    let task_b = tokio::spawn(async move {
        run_pipeline(
            &config_b,
            &select_b,
            &options_b,
            sink_b,
            &codecs_b,
            &metrics_b,
            Some(&window_b),
        )
        .await
    });

    tokio::time::sleep(Duration::from_secs(6)).await;
    if task_b.is_finished() {
        match task_b.await {
            Ok(Err(e)) => panic!("el segundo proceso falló antes de leer: {e:#}"),
            _ => panic!("el segundo proceso terminó"),
        }
    }
    let replayed = watch.rows_read.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        replayed, 0,
        "el proceso nuevo releyó eventos que ya estaban en la ficha"
    );

    produce(&topic, &[(90_000, 3), (130_000, 1)]).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        if task_b.is_finished() {
            match task_b.await {
                Ok(Err(e)) => panic!("el segundo proceso falló: {e:#}"),
                _ => panic!("el segundo proceso terminó"),
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Ok(batches) = read_table_rows(&table).await {
            got = rows(&batches);
            if got.iter().any(|row| row.1 == 60_000) {
                break;
            }
        }
    }
    task_b.abort();
    assert_eq!(
        got,
        vec![(1, 0, 60_000, 2, 15), (1, 60_000, 120_000, 2, 4)],
        "la ventana abierta viajó con la partición y no se sumó dos veces"
    );
    let _ = std::fs::remove_dir_all(&warehouse);
}
