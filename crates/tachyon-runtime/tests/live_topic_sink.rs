//! La salida de un pipeline puede ser un topic. El lote se ve recién cuando
//! la transacción commitea los offsets de entrada. Un abort antes del commit
//! no deja filas, y el reinicio las publica una vez.
//!
//!   cargo test -p tachyon-runtime --test live_topic_sink -- --ignored --nocapture

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema};
use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use tachyon_config::PipelineConfig;
use tachyon_metrics::InstanceMetrics;
use tachyon_runtime::{run_topic_pipeline, PreparedInput, RunOptions};

const BROKER: &str = "localhost:9092";
const SQL: &str = "SELECT order_id, amount FROM orders";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn config(input: &str, output: &str, commit_interval: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: topic-sink
connectors:
  redpanda:
    brokers: [{BROKER}]
inputs:
  - name: orders
    topic: {input}
    key: order_id
    schema: json/orders
output:
  name: orders_keyed
  topic: {output}
  key: order_id
deployment:
  partitions: 1
  commit_interval: {commit_interval}
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

async fn produce(topic: &str, rows: &[(i64, i64)]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("producer");
    for (id, amount) in rows {
        let payload = format!(r#"{{"order_id":{id},"amount":{amount}}}"#);
        let key = id.to_string();
        producer
            .send(
                FutureRecord::to(topic).key(&key).payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }
}

fn read_committed(topic: &str, wait: Duration) -> Vec<(String, String)> {
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
    while std::time::Instant::now() < deadline {
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

fn pipeline_died(
    result: Result<Result<tachyon_runtime::PipelineHandle, anyhow::Error>, tokio::task::JoinError>,
) -> String {
    match result {
        Ok(Err(err)) => format!("{err:#}"),
        Ok(Ok(_)) => "terminó sin error".to_string(),
        Err(err) => format!("cancelado: {err}"),
    }
}

fn options(id: &str) -> RunOptions {
    RunOptions {
        commit_interval: Duration::from_secs(1),
        metrics_bind: None,
        group_id: format!("tachyon-topic-{id}"),
        commit_user: format!("tachyon-topic-{id}-local"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn live_topic_publishes_once_and_a_restart_does_not_repeat() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let pid = std::process::id();
    let input = format!("tachyon-topic-in-{pid}");
    let output = format!("tachyon-topic-out-{pid}");
    recreate(&input).await;
    recreate(&output).await;
    produce(&input, &[(1, 10), (2, 20), (3, 30)]).await;

    let cfg = config(&input, &output, "1s");
    let mut opt = options(&pid.to_string());
    opt.commit_interval = Duration::from_secs(1);
    let codecs = HashMap::from([("orders".to_string(), PreparedInput::json(schema()))]);
    let metrics = Arc::new(InstanceMetrics::new());
    let task = tokio::spawn({
        let cfg = cfg.clone();
        let codecs = codecs.clone();
        let metrics = metrics.clone();
        let opt = opt.clone();
        let output = output.clone();
        async move { run_topic_pipeline(&cfg, SQL, &opt, &output, "order_id", &codecs, &metrics).await }
    });

    let topic = output.clone();
    let got = tokio::task::spawn_blocking(move || read_committed(&topic, Duration::from_secs(15)))
        .await
        .expect("lectura");
    if task.is_finished() {
        panic!("el pipeline terminó: {}", pipeline_died(task.await));
    }
    task.abort();
    assert_eq!(
        got.len(),
        3,
        "el topic de salida no recibió las tres filas: {got:?}"
    );

    tokio::time::sleep(Duration::from_secs(2)).await;
    let metrics = Arc::new(InstanceMetrics::new());
    let task = tokio::spawn({
        let cfg = cfg.clone();
        let codecs = codecs.clone();
        let metrics = metrics.clone();
        let opt = opt.clone();
        let output = output.clone();
        async move { run_topic_pipeline(&cfg, SQL, &opt, &output, "order_id", &codecs, &metrics).await }
    });
    tokio::time::sleep(Duration::from_secs(4)).await;
    if task.is_finished() {
        panic!("el reinicio terminó: {}", pipeline_died(task.await));
    }
    let topic = output.clone();
    let again = tokio::task::spawn_blocking(move || read_committed(&topic, Duration::from_secs(2)))
        .await
        .expect("lectura");
    task.abort();
    assert_eq!(again.len(), 3, "el reinicio volvió a publicar: {again:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn live_topic_hides_rows_until_the_transaction_commits() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let pid = std::process::id();
    let input = format!("tachyon-topic-open-in-{pid}");
    let output = format!("tachyon-topic-open-out-{pid}");
    recreate(&input).await;
    recreate(&output).await;
    produce(&input, &[(1, 10), (2, 20)]).await;

    let cfg = config(&input, &output, "30s");
    let mut opt = options(&format!("open-{pid}"));
    opt.commit_interval = Duration::from_secs(30);
    let codecs = HashMap::from([("orders".to_string(), PreparedInput::json(schema()))]);
    let metrics = Arc::new(InstanceMetrics::new());
    let task = tokio::spawn({
        let cfg = cfg.clone();
        let codecs = codecs.clone();
        let metrics = metrics.clone();
        let opt = opt.clone();
        let output = output.clone();
        async move { run_topic_pipeline(&cfg, SQL, &opt, &output, "order_id", &codecs, &metrics).await }
    });
    tokio::time::sleep(Duration::from_secs(3)).await;
    if task.is_finished() {
        panic!(
            "el pipeline cayó antes del abort: {}",
            pipeline_died(task.await)
        );
    }
    task.abort();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let topic = output.clone();
    let hidden =
        tokio::task::spawn_blocking(move || read_committed(&topic, Duration::from_secs(5)))
            .await
            .expect("lectura");
    assert!(
        hidden.is_empty(),
        "una transacción abierta se vio en el topic: {hidden:?}"
    );

    opt.commit_interval = Duration::from_secs(1);
    let metrics = Arc::new(InstanceMetrics::new());
    let out = output.clone();
    let task = tokio::spawn(async move {
        run_topic_pipeline(&cfg, SQL, &opt, &out, "order_id", &codecs, &metrics).await
    });
    let topic = output.clone();
    let got = tokio::task::spawn_blocking(move || read_committed(&topic, Duration::from_secs(15)))
        .await
        .expect("lectura");
    if task.is_finished() {
        panic!("el reinicio terminó: {}", pipeline_died(task.await));
    }
    task.abort();
    assert_eq!(
        got.len(),
        2,
        "el reinicio no publicó el lote una vez: {got:?}"
    );
}
