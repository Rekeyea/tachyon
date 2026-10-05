//! Un pedido y un pago dentro de la hora dejan una fila. Un pago a las dos
//! horas no entra. El reinicio no vuelve a escribir el par ya commiteado.
//!
//!   cargo test -p tachyon-runtime --test live_interval_join -- --ignored --nocapture

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
use tachyon_runtime::join::run_join_pipeline;
use tachyon_runtime::{PreparedInput, RunOptions};
use tachyon_sink::writer::{create_test_table, read_table_rows, PaimonSink};
use tachyon_sql::parse_sql;

const BROKER: &str = "localhost:9092";
const SQL: &str = "INSERT INTO paid_orders \
    SELECT o.order_id, o.amount, p.payment_id \
    FROM orders AS o \
    JOIN payments AS p \
      ON o.order_id = p.order_id \
     AND p.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '1' HOUR";

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn payments_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("payment_id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
    ]))
}

fn config(warehouse: &str, orders: &str, payments: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: interval-join
connectors:
  redpanda:
    brokers: [{BROKER}]
  paimon:
    warehouse: {warehouse}
inputs:
  - name: orders
    topic: {orders}
    key: order_id
    schema: json/orders
    watermark:
      column: event_time
      lag: 1ms
  - name: payments
    topic: {payments}
    key: order_id
    schema: json/payments
    watermark:
      column: event_time
      lag: 1ms
output:
  name: paid_orders
  table: default.paid_orders
  key: order_id
  bucket: 1
  sequence_field: payment_id
deployment:
  partitions: 1
  commit_interval: 1s
  consumers_per_topic: 1
"#
    ))
    .expect("yaml")
}

async fn fresh(topic: &str) {
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

async fn produce(topic: &str, payload: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("producer");
    producer
        .send(
            FutureRecord::to(topic).key("1").payload(payload),
            Duration::from_secs(5),
        )
        .await
        .expect("produce");
}

fn rows(batches: &[arrow::array::RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut out = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("order_id")
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
        let payments = batch
            .column_by_name("payment_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            out.push((ids.value(i), amounts.value(i), payments.value(i)));
        }
    }
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn live_interval_join_emits_the_pair_once() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let pid = std::process::id();
    let orders = format!("tachyon-join-orders-{pid}");
    let payments = format!("tachyon-join-payments-{pid}");
    fresh(&orders).await;
    fresh(&payments).await;
    produce(&orders, r#"{"order_id":1,"event_time":0,"amount":10}"#).await;
    produce(
        &payments,
        r#"{"payment_id":7,"order_id":1,"event_time":1800000}"#,
    )
    .await;

    let warehouse = std::env::temp_dir().join(format!("tachyon-join-wh-{pid}"));
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
            (
                "payment_id",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
        ],
        &["order_id", "payment_id"],
        1,
        Some("payment_id"),
    )
    .await
    .expect("tabla");

    let cfg = config(&warehouse, &orders, &payments);
    let join = parse_sql(SQL).expect("sql").join.expect("join");
    let options = RunOptions {
        commit_interval: Duration::from_secs(1),
        metrics_bind: None,
        group_id: format!("tachyon-join-{pid}"),
        commit_user: format!("tachyon-join-{pid}"),
    };
    let codecs = HashMap::from([
        ("orders".to_string(), PreparedInput::json(orders_schema())),
        (
            "payments".to_string(),
            PreparedInput::json(payments_schema()),
        ),
    ]);
    let metrics = Arc::new(InstanceMetrics::new());
    let sink =
        PaimonSink::from_table(table.clone(), "order_id", 1, Some("payment_id")).expect("sink");
    let task = tokio::spawn({
        let cfg = cfg.clone();
        let join = join.clone();
        let options = options.clone();
        let codecs = codecs.clone();
        let metrics = metrics.clone();
        async move { run_join_pipeline(&cfg, &options, sink, &join, &codecs, &metrics).await }
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    let mut got = Vec::new();
    while std::time::Instant::now() < deadline {
        if task.is_finished() {
            panic!("el join terminó: {}", died(task.await));
        }
        got = rows(&read_table_rows(&table).await.expect("leer"));
        if got.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    task.abort();
    assert_eq!(
        got,
        vec![(1, 10, 7)],
        "el par dentro de la hora no quedó: {got:?}"
    );

    tokio::time::sleep(Duration::from_secs(2)).await;
    let metrics = Arc::new(InstanceMetrics::new());
    let sink =
        PaimonSink::from_table(table.clone(), "order_id", 1, Some("payment_id")).expect("sink");
    let task = tokio::spawn({
        let cfg = cfg.clone();
        let join = join.clone();
        let options = options.clone();
        let codecs = codecs.clone();
        let metrics = metrics.clone();
        async move { run_join_pipeline(&cfg, &options, sink, &join, &codecs, &metrics).await }
    });
    tokio::time::sleep(Duration::from_secs(4)).await;
    if task.is_finished() {
        panic!("el reinicio terminó: {}", died(task.await));
    }
    assert_eq!(
        metrics.rows_read.load(Ordering::Relaxed),
        0,
        "el reinicio releyó eventos ya commiteados"
    );
    produce(
        &payments,
        r#"{"payment_id":8,"order_id":1,"event_time":7200000}"#,
    )
    .await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    task.abort();
    let got = rows(&read_table_rows(&table).await.expect("leer"));
    assert_eq!(
        got,
        vec![(1, 10, 7)],
        "el pago a las dos horas entró: {got:?}"
    );
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
