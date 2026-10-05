//! Una tabla Paimon se lee como un stream. El cursor es el snapshot: el
//! reinicio no vuelve a publicar esas filas, y el snapshot siguiente publica
//! solo las nuevas.
//!
//!   cargo test -p tachyon-runtime --test live_paimon_tail -- --ignored --nocapture

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use paimon::spec::{BigIntType, DataType as PDataType};
use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};
use tachyon_config::PipelineConfig;
use tachyon_runtime::Pipeline;
use tachyon_sink::writer::{create_test_table, PaimonSink};

const BROKER: &str = "localhost:9092";
const SQL: &str = "INSERT INTO paid_out SELECT order_id, amount FROM paid";

fn config(warehouse: &str, output: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: paimon-tail
connectors:
  redpanda:
    brokers: [{BROKER}]
  paimon:
    warehouse: {warehouse}
inputs:
  - name: paid
    table: default.paid_orders
    key: order_id
output:
  name: paid_out
  topic: {output}
  key: order_id
deployment:
  partitions: 1
  commit_interval: 1s
  consumers_per_topic: 1
"#
    ))
    .expect("yaml")
}

async fn recreate(topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("admin");
    let _ = admin.delete_topics(&[topic], &Default::default()).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &Default::default(),
        )
        .await
        .expect("topic");
}

fn batch(rows: &[(i64, i64)]) -> arrow::record_batch::RecordBatch {
    arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

async fn write_rows(table: &paimon::table::Table, rows: &[(i64, i64)]) {
    let mut sink = PaimonSink::from_table(table.clone(), "order_id", 1, None).expect("sink");
    sink.write(&batch(rows)).await.expect("write");
    sink.commit().await.expect("commit");
}

async fn kinds(table: &paimon::table::Table) -> String {
    let snapshots = table
        .snapshot_manager()
        .list_all()
        .await
        .expect("snapshots");
    snapshots
        .iter()
        .map(|snapshot| format!("{}={}", snapshot.id(), snapshot.commit_kind()))
        .collect::<Vec<_>>()
        .join(",")
}

fn read_committed(topic: &str, want: usize, wait: Duration) -> Vec<(String, String)> {
    let group = format!(
        "read-{}-{}",
        topic,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("reloj")
            .as_nanos()
    );
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .set("group.id", &group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .create()
        .expect("consumer");
    let mut list = TopicPartitionList::new();
    list.add_partition_offset(topic, 0, Offset::Beginning)
        .expect("offset");
    consumer.assign(&list).expect("assign");
    let deadline = std::time::Instant::now() + wait;
    let mut rows = Vec::new();
    while std::time::Instant::now() < deadline && rows.len() < want {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(message)) => {
                let key = message
                    .key()
                    .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                    .unwrap_or_default();
                let payload = message
                    .payload()
                    .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                    .unwrap_or_default();
                rows.push((key, payload));
            }
            Some(Err(err)) => panic!("leyendo {topic}: {err}"),
            None => {}
        }
    }
    rows
}

fn order_ids(rows: &[(String, String)]) -> Vec<i64> {
    let mut ids = Vec::new();
    for (key, payload) in rows {
        let value: serde_json::Value = serde_json::from_str(payload).expect(payload);
        let id = value
            .get("order_id")
            .and_then(|id| id.as_i64())
            .expect(payload);
        let amount = value.get("amount").and_then(|amount| amount.as_i64());
        assert_eq!(key, &id.to_string(), "{payload}");
        assert!(amount.is_some(), "{payload}");
        ids.push(id);
    }
    ids.sort();
    ids
}

fn died(
    result: Result<Result<tachyon_runtime::PipelineHandle, anyhow::Error>, tokio::task::JoinError>,
) -> String {
    match result {
        Ok(Err(err)) => format!("{err:#}"),
        Ok(Ok(_)) => "terminó sin error".to_string(),
        Err(err) => format!("cancelado: {err}"),
    }
}

fn start(
    cfg: PipelineConfig,
) -> tokio::task::JoinHandle<anyhow::Result<tachyon_runtime::PipelineHandle>> {
    tokio::spawn(async move {
        let pipeline = Pipeline::new(&cfg, SQL)?;
        pipeline.run(&HashMap::new()).await
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn live_a_paimon_table_publishes_each_snapshot_once() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let pid = std::process::id();
    let output = format!("tachyon-paimon-out-{pid}");
    let cursor_topic = format!("{output}-tachyon-cursor");
    recreate(&output).await;
    let _ = {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", BROKER)
            .create()
            .expect("admin");
        admin
            .delete_topics(&[&cursor_topic], &Default::default())
            .await
    };

    let warehouse = std::env::temp_dir().join(format!("tachyon-paimon-tail-{pid}"));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("warehouse");
    let warehouse = warehouse.to_string_lossy().to_string();
    let table = create_test_table(
        &warehouse,
        "default",
        "paid_orders",
        &[
            (
                "order_id",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
            (
                "amount",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
        ],
        &["order_id"],
        1,
        None,
    )
    .await
    .expect("tabla");
    write_rows(&table, &[(1, 10), (2, 20)]).await;
    let first_kinds = kinds(&table).await;

    let cfg = config(&warehouse, &output);
    let task = start(cfg.clone());
    let topic = output.clone();
    let got =
        tokio::task::spawn_blocking(move || read_committed(&topic, 2, Duration::from_secs(20)))
            .await
            .expect("lectura");
    if task.is_finished() {
        panic!("la cola terminó: {} kinds={first_kinds}", died(task.await));
    }
    assert_eq!(
        order_ids(&got),
        vec![1, 2],
        "kinds={first_kinds} filas={got:?}"
    );
    task.abort();
    let _ = task.await;

    tokio::time::sleep(Duration::from_secs(2)).await;
    let task = start(cfg.clone());
    tokio::time::sleep(Duration::from_secs(4)).await;
    if task.is_finished() {
        panic!("el reinicio terminó: {}", died(task.await));
    }
    let topic = output.clone();
    let again = tokio::task::spawn_blocking(move || {
        read_committed(&topic, usize::MAX, Duration::from_secs(3))
    })
    .await
    .expect("lectura");
    assert_eq!(
        order_ids(&again),
        vec![1, 2],
        "el reinicio volvió a publicar: {again:?}"
    );

    write_rows(&table, &[(3, 30)]).await;
    let second_kinds = kinds(&table).await;
    let topic = output.clone();
    let third =
        tokio::task::spawn_blocking(move || read_committed(&topic, 3, Duration::from_secs(20)))
            .await
            .expect("lectura");
    if task.is_finished() {
        panic!(
            "la cola terminó tras el segundo snapshot: {} kinds={second_kinds}",
            died(task.await)
        );
    }
    assert_eq!(
        order_ids(&third),
        vec![1, 2, 3],
        "kinds={second_kinds} filas={third:?}"
    );

    let cursor_topic_read = cursor_topic.clone();
    let cursor_rows = tokio::task::spawn_blocking(move || {
        read_committed(&cursor_topic_read, usize::MAX, Duration::from_secs(3))
    })
    .await
    .expect("cursor");
    let latest = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .expect("latest")
        .expect("hay snapshot");
    let last = cursor_rows
        .last()
        .map(|(_, payload)| payload.clone())
        .unwrap_or_default();
    assert_eq!(
        last,
        latest.to_string(),
        "cursor={cursor_rows:?} kinds={second_kinds}"
    );
    task.abort();
}
