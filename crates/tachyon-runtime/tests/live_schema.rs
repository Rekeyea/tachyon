//! El topic Avro lleva el id del schema que salió del SELECT. El que lee no
//! tiene un archivo: arma el Arrow con el último schema del registry.
//!
//!   cargo test -p tachyon-runtime --test live_schema -- --ignored --nocapture

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
use tachyon_source::register_topic_schema;

const BROKER: &str = "localhost:9092";
const REGISTRY: &str = "http://127.0.0.1:8081";
const SQL: &str = "SELECT order_id, amount FROM orders";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn config(input: &str, output: &str, avro_out: bool, avro_in: bool) -> PipelineConfig {
    let input_format = if avro_in { "\n    format: avro" } else { "" };
    let output_format = if avro_out { "\n  format: avro" } else { "" };
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: schema-wire
connectors:
  redpanda:
    brokers: [{BROKER}]
  schema_registry:
    url: {REGISTRY}
inputs:
  - name: orders
    topic: {input}
    key: order_id{input_format}
output:
  name: orders_out
  topic: {output}
  key: order_id{output_format}
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

async fn produce(topic: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("producer");
    for (id, amount) in [(1, 10), (2, 20)] {
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

fn read_json(topic: &str, wait: Duration) -> Vec<serde_json::Value> {
    let group = format!(
        "read-schema-{}-{}",
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
        if let Some(Ok(message)) = consumer.poll(Duration::from_millis(200)) {
            if let Some(payload) = message.payload() {
                rows.push(serde_json::from_slice(payload).expect("json"));
            }
        }
    }
    rows
}

fn options(id: &str) -> RunOptions {
    RunOptions {
        commit_interval: Duration::from_secs(1),
        metrics_bind: None,
        group_id: format!("tachyon-schema-{id}"),
        commit_user: format!("tachyon-schema-{id}-local"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn live_the_reader_uses_the_schema_registered_by_the_select() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();
    let pid = std::process::id();
    let input = format!("tachyon-schema-in-{pid}");
    let avro = format!("tachyon-schema-avro-{pid}");
    let output = format!("tachyon-schema-out-{pid}");
    recreate(&input).await;
    recreate(&avro).await;
    recreate(&output).await;
    produce(&input).await;

    let writer = config(&input, &avro, true, false);
    let writer_opt = options(&format!("w-{pid}"));
    let writer_codecs = HashMap::from([("orders".to_string(), PreparedInput::json(schema()))]);
    let writer_metrics = Arc::new(InstanceMetrics::new());
    let writer_task = tokio::spawn({
        let writer = writer.clone();
        let writer_opt = writer_opt.clone();
        let writer_codecs = writer_codecs.clone();
        let writer_metrics = writer_metrics.clone();
        let avro = avro.clone();
        async move {
            run_topic_pipeline(
                &writer,
                SQL,
                &writer_opt,
                &avro,
                "order_id",
                &writer_codecs,
                &writer_metrics,
            )
            .await
        }
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while writer_metrics.commits.load(std::sync::atomic::Ordering::Relaxed) < 1 {
        if writer_task.is_finished() {
            panic!("el publicador terminó: {}", pipeline_died(writer_task.await));
        }
        if std::time::Instant::now() > deadline {
            panic!("el publicador no registró ni commiteó");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let reader_codecs = HashMap::from([(
        "orders".to_string(),
        PreparedInput::from_registry(REGISTRY, &avro).expect("schema del registry"),
    )]);
    assert_eq!(reader_codecs["orders"].schema.fields().len(), 2);

    let reader = config(&avro, &output, false, true);
    let reader_opt = options(&format!("r-{pid}"));
    let reader_metrics = Arc::new(InstanceMetrics::new());
    let reader_task = tokio::spawn({
        let reader = reader.clone();
        let reader_opt = reader_opt.clone();
        let reader_codecs = reader_codecs.clone();
        let reader_metrics = reader_metrics.clone();
        let output = output.clone();
        async move {
            run_topic_pipeline(
                &reader,
                SQL,
                &reader_opt,
                &output,
                "order_id",
                &reader_codecs,
                &reader_metrics,
            )
            .await
        }
    });

    let topic = output.clone();
    let rows = tokio::task::spawn_blocking(move || read_json(&topic, Duration::from_secs(15)))
        .await
        .expect("lectura");
    reader_task.abort();
    writer_task.abort();
    let mut ids: Vec<i64> = rows
        .iter()
        .map(|row| row["order_id"].as_i64().unwrap_or_default())
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2], "el lector no reconstruyó las filas: {rows:?}");

    let drifted = r#"{"type":"record","name":"tachyon","fields":[{"name":"order_id","type":"long"},{"name":"amount","type":"long"},{"name":"extra","type":"long"}]}"#;
    let err = register_topic_schema(REGISTRY, &avro, drifted).expect_err("schema incompatible");
    let text = format!("{err:#}");
    assert!(
        text.contains("409") || text.to_lowercase().contains("incompat"),
        "el registry aceptó un schema que el lector no puede leer: {text}"
    );
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
