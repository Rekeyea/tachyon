//! UNION ALL concatena dos topics del mismo esquema. Una fila filtrada no
//! sale, las que quedan salen una vez, y un reinicio no las vuelve a publicar.
//!
//!   cargo test -p tachyon-runtime --test live_union -- --ignored --nocapture

use std::collections::{BTreeMap, HashMap};
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
use tachyon_runtime::Pipeline;

const BROKER: &str = "localhost:9092";
const SQL: &str = "INSERT INTO orders_out \
    SELECT order_id, amount FROM web WHERE amount > 15 \
    UNION ALL \
    SELECT order_id, amount FROM app";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn config(name: &str, web: &str, app: &str, output: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: {name}
connectors:
  redpanda:
    brokers: [{BROKER}]
inputs:
  - name: web
    topic: {web}
    key: order_id
    schema: json/web
  - name: app
    topic: {app}
    key: order_id
    schema: json/app
output:
  name: orders_out
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

fn tally(rows: &[(String, String)]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for (_, payload) in rows {
        let value: serde_json::Value = serde_json::from_str(payload)
            .unwrap_or_else(|err| panic!("payload no es json: {payload} ({err})"));
        let canon = value.to_string();
        *counts.entry(canon).or_insert(0) += 1;
    }
    counts
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn live_union_keeps_both_sides_once_and_a_restart_does_not_repeat() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let pid = std::process::id();
    let web = format!("tachyon-union-web-{pid}");
    let app = format!("tachyon-union-app-{pid}");
    let output = format!("tachyon-union-out-{pid}");
    recreate(&web).await;
    recreate(&app).await;
    recreate(&output).await;
    produce(&web, &[(1, 10), (2, 20)]).await;
    produce(&app, &[(3, 30)]).await;

    let cfg = config(&format!("union-{pid}"), &web, &app, &output);
    let pipeline = Pipeline::new(&cfg, SQL).expect("plan");
    let codecs = HashMap::from([
        (
            "web".to_string(),
            tachyon_runtime::PreparedInput::json(schema()),
        ),
        (
            "app".to_string(),
            tachyon_runtime::PreparedInput::json(schema()),
        ),
    ]);
    let task = tokio::spawn(async move { pipeline.run(&codecs).await });

    let topic = output.clone();
    let got = tokio::task::spawn_blocking(move || read_committed(&topic, Duration::from_secs(20)))
        .await
        .expect("lectura");
    if task.is_finished() {
        panic!("el pipeline terminó: {}", pipeline_died(task.await));
    }
    task.abort();
    let mut expected = BTreeMap::new();
    expected.insert(
        serde_json::json!({"order_id": 2, "amount": 20}).to_string(),
        1,
    );
    expected.insert(
        serde_json::json!({"order_id": 3, "amount": 30}).to_string(),
        1,
    );
    assert_eq!(
        tally(&got),
        expected,
        "salen las dos ramas y cae la fila filtrada: {got:?}"
    );

    tokio::time::sleep(Duration::from_secs(3)).await;
    let pipeline = Pipeline::new(&cfg, SQL).expect("plan");
    let codecs = HashMap::from([
        (
            "web".to_string(),
            tachyon_runtime::PreparedInput::json(schema()),
        ),
        (
            "app".to_string(),
            tachyon_runtime::PreparedInput::json(schema()),
        ),
    ]);
    let task = tokio::spawn(async move { pipeline.run(&codecs).await });
    tokio::time::sleep(Duration::from_secs(4)).await;
    if task.is_finished() {
        panic!("el reinicio terminó: {}", pipeline_died(task.await));
    }
    produce(&app, &[(4, 40)]).await;
    let topic = output.clone();
    let again =
        tokio::task::spawn_blocking(move || read_committed(&topic, Duration::from_secs(20)))
            .await
            .expect("lectura");
    if task.is_finished() {
        panic!("el reinicio murió: {}", pipeline_died(task.await));
    }
    task.abort();
    expected.insert(
        serde_json::json!({"order_id": 4, "amount": 40}).to_string(),
        1,
    );
    assert_eq!(
        tally(&again),
        expected,
        "el reinicio no republica y sigue con la fila nueva: {again:?}"
    );
}
