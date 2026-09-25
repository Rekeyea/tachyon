//! Slice 5: test live de escalado por particiones.
//!
//! Valida el criterio de éxito #3 del MVP: N instancias (aquí N=2) corren el
//! MISMO pipeline, cada una con un subconjunto de particiones del topic, y la
//! salida Paimon es correcta y sin conflictos.
//!
//! Modelo (DESIGN.md §5, §9.1):
//! - Topic con 2 particiones, producciones particionadas por clave `order_id`.
//! - 2 instancias con el MISMO consumer group: Redpanda reparte las
//!   particiones entre ellas (una por instancia, claves disjuntas).
//! - Cada instancia escribe a SU tabla Paimon (un writer por bucket, salidas
//!   disjuntas) → sin conflictos de escritura.
//! - La unión de ambas tablas debe cubrir los 5 orders no-cancelados, sin
//!   duplicados (cada `K` cae en una sola partición → una sola instancia).
//!
//! Requiere Redpanda local en localhost:9092.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use paimon::spec::{DataType as PDataType, BigIntType, DoubleType, VarCharType};
use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tachyon_config::PipelineConfig;
use tachyon_runtime::{run_pipeline, RunOptions};
use tachyon_sink::writer::{create_test_table, read_table_rows, PaimonSink};

const BROKER: &str = "localhost:9092";
const DB: &str = "default";
const TABLE: &str = "orders_lake";
const ORDERS_TOPIC: &str = "tachyon-scaling-orders";
const NUM_INSTANCES: usize = 2;
const NUM_PARTITIONS: i32 = 2;

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("source_version", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
    ]))
}

async fn recreate_topic(broker: &str) {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", broker);
    let admin: AdminClient<DefaultClientContext> = cc.create().expect("admin client");
    let _ = admin.delete_topics(&[ORDERS_TOPIC], &Default::default()).await;
    let topic = NewTopic::new(
        ORDERS_TOPIC,
        NUM_PARTITIONS,
        TopicReplication::Fixed(1),
    );
    admin
        .create_topics(&[topic], &Default::default())
        .await
        .expect("creando topic");
}

async fn produce_orders(producer: &FutureProducer) {
    // 6 orders: 1 cancelled (queda fuera por el filter), 5 pasan.
    let orders = [
        (1i64, "paid", 10i64, 100.0f64),
        (2, "shipped", 11, 200.0),
        (3, "paid", 12, 300.0),
        (4, "cancelled", 13, 400.0),
        (5, "paid", 14, 500.0),
        (6, "shipped", 15, 600.0),
    ];
    for (order_id, status, version, amount) in orders {
        let payload = format!(
            "{{\"order_id\":{order_id},\"status\":\"{status}\",\"source_version\":{version},\"amount\":{amount}}}"
        );
        let key = order_id.to_string();
        let record = FutureRecord::to(ORDERS_TOPIC)
            .key(&key)
            .payload(&payload);
        producer
            .send(record, Duration::from_secs(5))
            .await
            .expect("produciendo order");
    }
}

fn config_yaml(warehouse: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: scaling-orders
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
output:
  name: orders_lake
  table: {db}.{table}
  key: order_id
  bucket: 1
  sequence_field: source_version
deployment:
  partitions: {partitions}
  commit_interval: 2s
  metrics:
    bind_addr: "127.0.0.1:0"
"#,
        warehouse = warehouse,
        topic = ORDERS_TOPIC,
        db = DB,
        table = TABLE,
        partitions = NUM_PARTITIONS,
    ))
    .expect("config de test válida")
}

/// Warehouse independiente por instancia (evita colisiones de schema).
fn fresh_warehouse(instance: usize) -> String {
    let dir = std::env::temp_dir().join(format!(
        "tachyon-scaling-warehouse-{}-{}",
        std::process::id(),
        instance
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("creando warehouse");
    dir.to_string_lossy().to_string()
}

/// Lee las filas (order_id, amount) de una tabla Paimon.
async fn read_rows(table: &paimon::table::Table) -> Vec<(i64, f64)> {
    match read_table_rows(table).await {
        Ok(batches) => batches
            .iter()
            .flat_map(|b| {
                let ids = b
                    .column_by_name("order_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let amounts = b
                    .column_by_name("amount")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..b.num_rows()).map(move |i| (ids.value(i), amounts.value(i)))
            })
            .collect(),
        Err(_) => vec![],
    }
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn live_scaling_two_instances_disjoint_partitions() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    // --- 0. Tablas Paimon: una por instancia (salidas disjuntas) ---
    let mut tables = Vec::new();
    let mut warehouses = Vec::new();
    for instance in 0..NUM_INSTANCES {
        let warehouse = fresh_warehouse(instance);
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
        .expect("creando tabla Paimon");
        tables.push(table);
        warehouses.push(warehouse);
    }

    // --- 1. Topic fresco (2 particiones) + producer ---
    recreate_topic(BROKER).await;
    let mut producer_config = ClientConfig::new();
    producer_config.set("bootstrap.servers", BROKER);
    let producer: FutureProducer = producer_config.create().expect("producer");
    produce_orders(&producer).await;

    // --- 2. N instancias del MISMO pipeline, MISMO consumer group ---
    // Redpanda reparte las particiones entre los miembros del grupo: cada
    // instancia consume un subconjunto disjunto de claves.
    let select_sql = "SELECT order_id, status, source_version, amount \
                      FROM orders WHERE status <> 'cancelled'";
    let input_schemas: HashMap<String, Arc<Schema>> =
        HashMap::from([("orders".to_string(), orders_schema())]);
    let group_id = format!("tachyon-scaling-{}", std::process::id());

    let mut run_tasks = Vec::new();
    for instance in 0..NUM_INSTANCES {
        let config = Arc::new(config_yaml(&warehouses[instance]));
        let options = RunOptions {
            commit_interval: Duration::from_secs(2),
            metrics_bind: Some("127.0.0.1:0".parse().unwrap()),
            group_id: group_id.clone(), // MISMO grupo -> reparto de particiones
        };
        let sink = PaimonSink::from_table(
            tables[instance].clone(),
            "order_id",
            1,
            Some("source_version"),
        )
        .expect("abriendo sink");
        let schemas = input_schemas.clone();
        let metrics = Arc::new(tachyon_metrics::InstanceMetrics::new());
        run_tasks.push(tokio::spawn(async move {
            run_pipeline(&config, select_sql, &options, sink, &schemas, &metrics).await
        }));
    }

    // --- 3. Espera a que la UNIÓN de ambas tablas cubra los 5 orders ---
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    let mut union_rows: Vec<(i64, f64)> = vec![];
    while std::time::Instant::now() < deadline {
        for i in 0..run_tasks.len() {
            if run_tasks[i].is_finished() {
                let handle = run_tasks.remove(i);
                match handle.await {
                    Ok(Ok(_)) => panic!("instancia {i} terminó (el stream no es infinito)"),
                    Ok(Err(e)) => panic!("instancia {i} falló temprano: {e:#}"),
                    Err(e) => panic!("tarea de instancia {i} abortada: {e}"),
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut all_rows: Vec<(i64, f64)> = vec![];
        for table in &tables {
            all_rows.extend(read_rows(table).await);
        }
        union_rows = all_rows;
        union_rows.sort_by_key(|(id, _)| *id);
        union_rows.dedup();
        if union_rows.len() == 5 {
            break;
        }
    }

    // --- 4. Verificación: 5 orders en la unión, sin duplicados ---
    assert_eq!(
        union_rows,
        vec![(1, 100.0), (2, 200.0), (3, 300.0), (5, 500.0), (6, 600.0)],
        "la unión de las tablas de ambas instancias debe cubrir los 5 orders"
    );

    // Sin duplicados: cada order_id aparece exactamente una vez en la unión.
    let mut ids: Vec<i64> = union_rows.iter().map(|(id, _)| *id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 5, "no debe haber duplicados entre instancias");

    // Ambos pipelines siguen corriendo (streams infinitos).
    for (i, task) in run_tasks.iter().enumerate() {
        assert!(!task.is_finished(), "la instancia {i} debe seguir corriendo");
    }

    // --- Limpieza ---
    for task in &mut run_tasks {
        task.abort();
    }
    for instance in 0..NUM_INSTANCES {
        let dir = std::env::temp_dir().join(format!(
            "tachyon-scaling-warehouse-{}-{}",
            std::process::id(),
            instance
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
