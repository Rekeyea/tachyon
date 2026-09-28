//! Exactly-once por stream: el checkpoint en Paimon (snapshot + offsets) es la
//! fuente de verdad del progreso, no los offsets del consumer group.
//!
//! Se fuerza el desfase entre Kafka y el checkpoint en ambos sentidos:
//! 1. **Kafka adelantado** (lo que pasaba al commitear la posición del
//!    consumer, que incluye registros en vuelo): la instancia reiniciada debe
//!    hacer `seek` al checkpoint y no perder nada.
//! 2. **Kafka atrasado** (crash entre el commit de Paimon y el de Kafka): la
//!    instancia reiniciada debe saltear lo ya escrito y no re-procesar nada.
//!
//! En ambos casos la segunda instancia lee **exactamente** los eventos
//! posteriores al checkpoint (`rows_read`) y la tabla queda completa.
//!
//! Requiere Redpanda local en localhost:9092.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use paimon::spec::{BigIntType, DataType as PDataType, DoubleType, VarCharType};
use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use tachyon_config::PipelineConfig;
use tachyon_metrics::InstanceMetrics;
use tachyon_runtime::{run_pipeline, PreparedInput, RunOptions};
use tachyon_sink::writer::{create_test_table, read_table_rows, PaimonSink};

const BROKER: &str = "localhost:9092";
const DB: &str = "default";
const TABLE: &str = "orders_lake";
const PARTITIONS: i32 = 4;
const LOTE: i64 = 500;

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("source_version", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
    ]))
}

async fn recreate_topic(topic: &str) {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", BROKER);
    let admin: AdminClient<DefaultClientContext> = cc.create().expect("admin client");
    let _ = admin.delete_topics(&[topic], &Default::default()).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let new_topic = NewTopic::new(topic, PARTITIONS, TopicReplication::Fixed(1));
    admin
        .create_topics(&[new_topic], &Default::default())
        .await
        .expect("creando topic");
}

async fn produce_range(producer: &FutureProducer, topic: &str, ids: std::ops::RangeInclusive<i64>) {
    for id in ids {
        let payload = format!(
            "{{\"order_id\":{id},\"status\":\"paid\",\"source_version\":{id},\"amount\":{id}.0}}"
        );
        let key = id.to_string();
        producer
            .send(FutureRecord::to(topic).key(&key).payload(&payload), Duration::from_secs(5))
            .await
            .expect("produciendo order");
    }
}

fn config_yaml(warehouse: &str, topic: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: exactly-once
connectors:
  redpanda:
    brokers: [{BROKER}]
  paimon:
    warehouse: {warehouse}
inputs:
  - name: orders
    topic: {topic}
    key: order_id
    schema: json/orders
output:
  name: orders_lake
  table: {DB}.{TABLE}
  key: order_id
  bucket: 1
  sequence_field: source_version
deployment:
  partitions: {PARTITIONS}
  commit_interval: 500ms
"#
    ))
    .expect("config de test válida")
}

async fn order_ids(table: &paimon::table::Table) -> Vec<i64> {
    let mut ids: Vec<i64> = read_table_rows(table)
        .await
        .unwrap_or_default()
        .iter()
        .flat_map(|b| {
            b.column_by_name("order_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort();
    ids
}

type Instance = (
    tokio::task::JoinHandle<anyhow::Result<tachyon_runtime::PipelineHandle>>,
    Arc<InstanceMetrics>,
);

fn start_instance(config: &PipelineConfig, table: &paimon::table::Table, group_id: &str) -> Instance {
    let config = config.clone();
    let options = RunOptions {
        commit_interval: Duration::from_millis(500),
        metrics_bind: None,
        group_id: group_id.to_string(),
        commit_user: group_id.to_string(),
    };
    let sink = PaimonSink::from_table(table.clone(), "order_id", 1, Some("source_version"))
        .expect("abriendo sink");
    let schemas = HashMap::from([("orders".to_string(), PreparedInput::json(orders_schema()))]);
    let metrics = Arc::new(InstanceMetrics::new());
    let task = tokio::spawn({
        let metrics = metrics.clone();
        async move {
            run_pipeline(
                &config,
                "SELECT order_id, status, source_version, amount FROM orders",
                &options,
                sink,
                &schemas,
                &metrics,
            )
            .await
        }
    });
    (task, metrics)
}

/// Espera a que la tabla tenga `expected` filas (falla si la instancia muere).
async fn wait_rows(instance: &Instance, table: &paimon::table::Table, expected: usize) -> Vec<i64> {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut ids = vec![];
    while std::time::Instant::now() < deadline {
        assert!(!instance.0.is_finished(), "la instancia terminó antes de tiempo");
        tokio::time::sleep(Duration::from_millis(500)).await;
        ids = order_ids(table).await;
        if ids.len() >= expected {
            break;
        }
    }
    ids
}

/// Pisa los offsets del consumer group del input (grupo vacío: las
/// instancias ya se fueron). `offset_for(p)` da el offset por partición.
fn force_group_offsets(group_id: &str, topic: &str, offset_for: impl Fn(i32) -> i64) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .set("group.id", format!("{group_id}-orders"))
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer admin");
    let mut tpl = TopicPartitionList::new();
    for p in 0..PARTITIONS {
        tpl.add_partition_offset(topic, p, Offset::Offset(offset_for(p)))
            .expect("tpl");
    }
    consumer.assign(&tpl).expect("assign");
    consumer.commit(&tpl, CommitMode::Sync).expect("forzando offsets del grupo");
}

fn high_watermark(topic: &str, partition: i32) -> i64 {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .set("group.id", "tachyon-watermarks")
        .create()
        .expect("consumer watermarks");
    consumer
        .fetch_watermarks(topic, partition, Duration::from_secs(5))
        .expect("watermarks")
        .1
}

enum KafkaDrift {
    /// El grupo quedó en el final del topic (por delante del checkpoint).
    Ahead,
    /// El grupo quedó en el inicio del topic (por detrás del checkpoint).
    Behind,
}

async fn run_scenario(name: &str, drift: KafkaDrift) {
    let run_id = format!("{name}-{}", std::process::id());
    let topic = format!("tachyon-eos-{run_id}");
    let group_id = format!("tachyon-eos-{run_id}");
    let warehouse = std::env::temp_dir().join(format!("tachyon-eos-warehouse-{run_id}"));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("warehouse");
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
    .expect("tabla Paimon");
    let config = config_yaml(&warehouse, &topic);

    recreate_topic(&topic).await;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("producer");

    // --- Lote 1 -> instancia 1 -> checkpoint -> parar ---
    produce_range(&producer, &topic, 1..=LOTE).await;
    let inst1 = start_instance(&config, &table, &group_id);
    let ids = wait_rows(&inst1, &table, LOTE as usize).await;
    assert_eq!(ids.len(), LOTE as usize, "el lote 1 debe llegar a Paimon");
    inst1.0.abort();
    // El writer hace su checkpoint final y los consumers dejan el grupo.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let committed = order_ids(&table).await.len() as u64;
    assert_eq!(committed, LOTE as u64);

    // --- Lote 2 + desfase forzado de los offsets del grupo ---
    produce_range(&producer, &topic, LOTE + 1..=2 * LOTE).await;
    match drift {
        KafkaDrift::Ahead => force_group_offsets(&group_id, &topic, |p| high_watermark(&topic, p)),
        KafkaDrift::Behind => force_group_offsets(&group_id, &topic, |_| 0),
    }

    // --- Instancia 2: debe arrancar exactamente en el checkpoint ---
    let inst2 = start_instance(&config, &table, &group_id);
    let ids = wait_rows(&inst2, &table, 2 * LOTE as usize).await;
    // Margen para que un re-proceso de más (si lo hubiera) se vea en rows_read.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let rows_read = inst2.1.rows_read.load(Ordering::Relaxed);
    inst2.0.abort();

    assert_eq!(
        ids,
        (1..=2 * LOTE).collect::<Vec<_>>(),
        "la tabla debe tener los dos lotes completos (sin pérdida)"
    );
    assert_eq!(
        rows_read,
        LOTE as u64,
        "la instancia 2 debe leer exactamente el lote 2 (ni re-proceso ni hueco)"
    );
    let offsets_dir = std::path::Path::new(&warehouse)
        .join(format!("{DB}.db"))
        .join(TABLE)
        .join("tachyon-offsets")
        .join(&group_id);
    assert!(offsets_dir.is_dir(), "falta el directorio de offsets de checkpoint: {offsets_dir:?}");

    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_exactly_once_kafka_offsets_ahead_of_checkpoint() {
    run_scenario("ahead", KafkaDrift::Ahead).await;
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_exactly_once_kafka_offsets_behind_checkpoint() {
    run_scenario("behind", KafkaDrift::Behind).await;
}
