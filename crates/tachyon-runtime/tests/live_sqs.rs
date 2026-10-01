//! SQS (floCi) como input -> tabla Paimon: at-least-once + dedup.
//!
//! N consumidores compiten por la cola (long polling). Cada mensaje llega al
//! writer con su receipt handle; el commit lo borra de la cola solo cuando el
//! snapshot de Paimon se commitea (visibility timeout > commit_interval deja
//! margen para el re-entrega). Un reinicio con la cola vacía no repite nada,
//! y si un re-entrega ocurriera la PK + sequence_field de Paimon lo deduplica.
//!
//! Requiere floCi en localhost:4566:
//!   docker run -p 4566:4566 floci/floci:latest
//!
//!   cargo test -p tachyon-runtime --test live_sqs -- --ignored --nocapture

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use paimon::spec::{DataType as PDataType, BigIntType, DoubleType, VarCharType};
use tachyon_config::PipelineConfig;
use tachyon_runtime::{run_pipeline, PreparedInput, RunOptions};
use tachyon_sink::writer::{create_test_table, read_table_rows, PaimonSink};

const ENDPOINT: &str = "http://localhost:4566";
const REGION: &str = "us-east-1";
const DB: &str = "default";
const TABLE: &str = "orders_lake";

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("source_version", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
    ]))
}

async fn sqs_client() -> aws_sdk_sqs::Client {
    // floCi acepta cualquier credencial.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    }
    let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_types::region::Region::new(REGION.to_string()))
        .endpoint_url(ENDPOINT.to_string())
        .load()
        .await;
    aws_sdk_sqs::Client::new(&sdk)
}

async fn create_queue(client: &aws_sdk_sqs::Client, name: &str) -> String {
    let resp = client
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .expect("create_queue");
    resp.queue_url().expect("queue_url").to_string()
}

async fn send_orders(
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
    orders: &[(i64, &str, i64, f64)],
) {
    for (order_id, status, version, amount) in orders {
        let payload = format!(
            r#"{{"order_id":{order_id},"status":"{status}","source_version":{version},"amount":{amount}}}"#
        );
        client
            .send_message()
            .queue_url(queue_url)
            .message_body(payload)
            .send()
            .await
            .expect("send_message");
    }
}

async fn queue_message_count(client: &aws_sdk_sqs::Client, queue_url: &str) -> usize {
    let resp = client
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .expect("receive_message");
    resp.messages().len()
}

fn config_yaml(warehouse: &str, queue: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: sqs-{pid}
connectors:
  paimon:
    warehouse: {warehouse}
    catalog: local
  sqs:
    region: {region}
    endpoint: {endpoint}
inputs:
  - name: orders
    sqs: {queue}
    key: order_id
    schema: json/orders
    receive_wait: 2s
output:
  name: orders_lake
  table: {db}.{table}
  key: order_id
  bucket: 1
  sequence_field: source_version
deployment:
  partitions: 1
  consumers_per_topic: 2
  commit_interval: 1s
"#,
        pid = std::process::id(),
        warehouse = warehouse,
        region = REGION,
        endpoint = ENDPOINT,
        queue = queue,
        db = DB,
        table = TABLE,
    ))
    .expect("config de test válida")
}

async fn read_rows(table: &paimon::table::Table) -> Vec<(i64, f64)> {
    let mut rows: Vec<(i64, f64)> = vec![];
    if let Ok(batches) = read_table_rows(table).await {
        rows = batches
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
            .collect();
        rows.sort_by_key(|(id, _)| *id);
    }
    rows
}

async fn wait_rows(
    table: &paimon::table::Table,
    expected: usize,
    deadline: Duration,
) -> Vec<(i64, f64)> {
    let t0 = std::time::Instant::now();
    loop {
        let rows = read_rows(table).await;
        if rows.len() == expected {
            return rows;
        }
        assert!(
            t0.elapsed() < deadline,
            "esperando {expected} filas, hay {rows:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn run_options() -> RunOptions {
    RunOptions {
        commit_interval: Duration::from_secs(1),
        metrics_bind: None,
        group_id: format!("tachyon-sqs-{}", std::process::id()),
        commit_user: format!("tachyon-sqs-{}", std::process::id()),
    }
}

const SQL: &str = "SELECT order_id, status, source_version, amount \
                   FROM orders WHERE status <> 'cancelled'";

async fn start_pipeline(
    config: &Arc<PipelineConfig>,
    table: &paimon::table::Table,
    options: &RunOptions,
) -> tokio::task::JoinHandle<Result<tachyon_runtime::PipelineHandle, anyhow::Error>> {
    let table = table.clone();
    let config = (*config).clone();
    let options = options.clone();
    let metrics = Arc::new(tachyon_metrics::InstanceMetrics::new());
    let input_schemas =
        HashMap::from([("orders".to_string(), PreparedInput::json(orders_schema()))]);
    tokio::spawn(async move {
        let sink = PaimonSink::from_table(table, "order_id", 1, Some("source_version"))
            .expect("abriendo sink");
        run_pipeline(&config, SQL, &options, sink, &input_schemas, &metrics, None).await
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requiere floCi en localhost:4566"]
async fn live_sqs_at_least_once_and_resume() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();

    // --- 0. Warehouse + tabla Paimon frescos ---
    let warehouse = std::env::temp_dir().join(format!(
        "tachyon-sqs-wh-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("creando warehouse");
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
    .expect("creando tabla Paimon");

    // --- 1. Cola fresca + 6 orders (1 cancelled) ---
    let client = sqs_client().await;
    let queue = format!("tachyon-sqs-{}", std::process::id());
    let queue_url = create_queue(&client, &queue).await;
    let orders = [
        (1i64, "paid", 10i64, 100.0f64),
        (2, "shipped", 11, 200.0),
        (3, "paid", 12, 300.0),
        (4, "cancelled", 13, 400.0),
        (5, "paid", 14, 500.0),
        (6, "shipped", 15, 600.0),
    ];
    send_orders(&client, &queue_url, &orders).await;

    // --- 2. Pipeline: sqs -> Paimon (2 consumidores compitiendo) ---
    let config = Arc::new(config_yaml(&warehouse, &queue));
    let options = run_options();
    let run_task = start_pipeline(&config, &table, &options).await;

    // --- 3. Las 5 orders no-canceladas salen una sola vez ---
    let rows = wait_rows(&table, 5, Duration::from_secs(60)).await;
    assert_eq!(
        rows,
        vec![(1, 100.0), (2, 200.0), (3, 300.0), (5, 500.0), (6, 600.0)],
        "sin duplicados: {rows:?}"
    );
    // Un commit más de margen: la cola queda vacía (los handles se borran
    // tras el commit, incluso el de la fila filtrada).
    tokio::time::sleep(Duration::from_secs(3)).await;
    let rows = read_rows(&table).await;
    assert_eq!(rows.len(), 5, "sin duplicados: {rows:?}");
    assert_eq!(
        queue_message_count(&client, &queue_url).await,
        0,
        "la cola queda vacía tras el commit"
    );

    // --- 4. Reinicio: cola vacía, nada se repite ---
    run_task.abort();
    let run_task = start_pipeline(&config, &table, &options).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let rows = read_rows(&table).await;
    assert_eq!(rows.len(), 5, "el reinicio no repite: {rows:?}");

    // --- 5. Un mensaje nuevo sigue llegando ---
    send_orders(&client, &queue_url, &[(7, "paid", 16, 700.0)]).await;
    let rows = wait_rows(&table, 6, Duration::from_secs(30)).await;
    assert_eq!(
        rows,
        vec![
            (1, 100.0),
            (2, 200.0),
            (3, 300.0),
            (5, 500.0),
            (6, 600.0),
            (7, 700.0)
        ],
        "el mensaje nuevo sale: {rows:?}"
    );

    run_task.abort();
    let _ = std::fs::remove_dir_all(&warehouse);
}
