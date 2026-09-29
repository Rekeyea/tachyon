//! Un stream se enriquece con una dimensión chica de Paimon antes de DataFusion.
//!
//!   cargo test -p tachyon-runtime --test live_lookup -- --ignored --nocapture

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use paimon::spec::{BigIntType, DataType as PDataType, VarCharType};
use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use tachyon_config::PipelineConfig;
use tachyon_runtime::Pipeline;
use tachyon_sink::writer::{create_test_table, PaimonSink};

const BROKER: &str = "localhost:9092";
const SQL: &str = "INSERT INTO orders_out \
    SELECT o.order_id, c.name AS customer_name \
    FROM orders o \
    LEFT JOIN customers c ON o.customer_id = c.customer_id \
    WHERE c.country = 'AR'";

fn fact_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
    ]))
}

fn config(input: &str, output: &str, warehouse: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: lookup-{input}
connectors:
  redpanda:
    brokers: [{BROKER}]
  paimon:
    warehouse: {warehouse}
inputs:
  - name: orders
    topic: {input}
    key: order_id
    schema: json/orders
dimensions:
  - name: customers
    table: default.customers
    key: customer_id
output:
  name: orders_out
  topic: {output}
  key: order_id
deployment:
  partitions: 1
  commit_interval: 1s
  consumers_per_topic: 1
  resources:
    memory: 256Mi
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
    for (order_id, customer_id) in rows {
        let payload = format!(r#"{{"order_id":{order_id},"customer_id":{customer_id}}}"#);
        let key = order_id.to_string();
        producer
            .send(
                FutureRecord::to(topic).key(&key).payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }
}

fn read_committed(topic: &str, wait: Duration, until: i64) -> Vec<(i64, String)> {
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
                let payload = message
                    .payload()
                    .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                    .unwrap_or_default();
                let value: serde_json::Value = serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null);
                let order_id = value.get("order_id").and_then(|item| item.as_i64()).unwrap_or(-1);
                let name = value
                    .get("customer_name")
                    .and_then(|item| item.as_str())
                    .unwrap_or("")
                    .to_string();
                rows.push((order_id, name));
            }
            Some(Err(err)) => panic!("leyendo {topic}: {err}"),
            None => {
                if rows.iter().any(|row| row.0 == until) {
                    break;
                }
            }
        }
    }
    rows
}

fn dim_rows(rows: &[(i64, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("country", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| Some(row.1.to_string()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| Some(row.2.to_string()))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("filas")
}

struct Stop(Option<tokio::task::JoinHandle<anyhow::Result<tachyon_runtime::PipelineHandle>>>);

impl Drop for Stop {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
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

async fn died(stop: &mut Stop) -> String {
    let Some(task) = stop.0.take() else {
        return "sin tarea".to_string();
    };
    pipeline_died(task.await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_lookup_keeps_the_country_and_sees_the_next_snapshot() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();
    let pid = std::process::id();
    let input = format!("tachyon-lookup-in-{pid}");
    let output = format!("tachyon-lookup-out-{pid}");
    let dir = std::env::temp_dir().join(format!("tachyon-lookup-live-{pid}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("warehouse");
    let warehouse = dir.to_string_lossy().to_string();

    let table = create_test_table(
        &warehouse,
        "default",
        "customers",
        &[
            (
                "customer_id",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
            ("name", PDataType::VarChar(VarCharType::string_type())),
            ("country", PDataType::VarChar(VarCharType::string_type())),
        ],
        &["customer_id"],
        1,
        None,
    )
    .await
    .expect("tabla");
    let mut sink = PaimonSink::from_table(table.clone(), "customer_id", 1, None).expect("sink");
    sink.write(&dim_rows(&[(1, "ana", "AR"), (2, "bea", "UY")]))
        .await
        .expect("write");
    sink.commit().await.expect("commit");

    recreate(&input).await;
    recreate(&output).await;
    produce(&input, &[(10, 1), (11, 9), (12, 2)]).await;

    let cfg = config(&input, &output, &warehouse);
    let pipeline = Pipeline::new(&cfg, SQL).expect("plan");
    let codecs = HashMap::from([(
        "orders".to_string(),
        tachyon_runtime::PreparedInput::json(fact_schema()),
    )]);
    let task = tokio::spawn(async move { pipeline.run(&codecs).await });
    let mut stop = Stop(Some(task));

    let topic = output.clone();
    let first = tokio::task::spawn_blocking(move || read_committed(&topic, Duration::from_secs(20), 10))
        .await
        .expect("lectura");
    if stop.0.as_ref().is_some_and(|task| task.is_finished()) {
        panic!("el pipeline terminó: {}", died(&mut stop).await);
    }
    assert_eq!(
        first,
        vec![(10, "ana".to_string())],
        "solo el cliente de AR sale; el miss y UY no"
    );

    let mut sink = PaimonSink::from_table(table, "customer_id", 1, None).expect("sink");
    sink.write(&dim_rows(&[(1, "eva", "AR")]))
        .await
        .expect("update");
    sink.commit().await.expect("commit");
    produce(&input, &[(13, 1)]).await;

    let topic = output.clone();
    let second = tokio::task::spawn_blocking(move || read_committed(&topic, Duration::from_secs(20), 13))
        .await
        .expect("lectura");
    if stop.0.as_ref().is_some_and(|task| task.is_finished()) {
        panic!("el pipeline terminó: {}", died(&mut stop).await);
    }
    assert_eq!(
        second,
        vec![(10, "ana".to_string()), (13, "eva".to_string())],
        "el snapshot nuevo cambia el nombre y no repite la fila ya publicada"
    );
    drop(stop);
    let _ = std::fs::remove_dir_all(&dir);
}
