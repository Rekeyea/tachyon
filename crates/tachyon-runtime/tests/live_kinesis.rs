//! Kinesis (floCi) como input -> tabla Paimon: exactly-once y resume.
//!
//! Los registros puestos en el stream se consumen una sola vez: el filter se
//! aplica, la posición (shard -> seq) se commitea junto al snapshot de
//! Paimon y un reinicio reanuda desde el checkpoint (AFTER_SEQUENCE_NUMBER)
//! sin releer.
//!
//! Requiere floCi en localhost:4566:
//!   docker run -p 4566:4566 floci/floci:latest
//!
//!   cargo test -p tachyon-runtime --test live_kinesis -- --ignored --nocapture

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use paimon::spec::{BigIntType, DataType as PDataType, DoubleType, VarCharType};
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

async fn kinesis_client() -> aws_sdk_kinesis::Client {
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
    aws_sdk_kinesis::Client::new(&sdk)
}

async fn recreate_stream(client: &aws_sdk_kinesis::Client, stream: &str) {
    let _ = client.delete_stream().stream_name(stream).send().await;
    client
        .create_stream()
        .stream_name(stream)
        .shard_count(1)
        .send()
        .await
        .expect("create_stream");
    // El mock simula una creación asíncrona: esperar ACTIVE evita la carrera
    // entre CreateStream y el primer GetRecords/PutRecords.
    let t0 = std::time::Instant::now();
    loop {
        match client.describe_stream().stream_name(stream).send().await {
            Ok(r) => {
                if r.stream_description()
                    .map(|d| d.stream_status().to_string())
                    == Some("ACTIVE".to_string())
                {
                    return;
                }
            }
            Err(_) => {}
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "el stream no llega a ACTIVE"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn put_orders(
    client: &aws_sdk_kinesis::Client,
    stream: &str,
    orders: &[(i64, &str, i64, f64)],
) {
    let mut req = client.put_records().stream_name(stream);
    for (order_id, status, version, amount) in orders {
        let payload = format!(
            r#"{{"order_id":{order_id},"status":"{status}","source_version":{version},"amount":{amount}}}"#
        );
        let entry = aws_sdk_kinesis::types::PutRecordsRequestEntry::builder()
            .data(aws_sdk_kinesis::primitives::Blob::from(
                payload.into_bytes(),
            ))
            .partition_key(order_id.to_string())
            .build()
            .expect("entry");
        req = req.records(entry);
    }
    let resp = req.send().await.expect("put_records");
    assert_eq!(
        resp.failed_record_count(),
        Some(0),
        "put_records con fallos"
    );
}

fn config_yaml(warehouse: &str, stream: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: kinesis-{pid}
connectors:
  paimon:
    warehouse: {warehouse}
    catalog: local
  kinesis:
    region: {region}
    endpoint: {endpoint}
inputs:
  - name: orders
    kinesis: {stream}
    key: order_id
    schema: json/orders
    start_from: trim_horizon
output:
  name: orders_lake
  table: {db}.{table}
  key: order_id
  bucket: 1
  sequence_field: source_version
deployment:
  partitions: 1
  commit_interval: 1s
"#,
        pid = std::process::id(),
        warehouse = warehouse,
        region = REGION,
        endpoint = ENDPOINT,
        stream = stream,
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
        group_id: format!("tachyon-kinesis-{}", std::process::id()),
        commit_user: format!("tachyon-kinesis-{}", std::process::id()),
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
async fn live_kinesis_exactly_once_and_resume() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    // --- 0. Warehouse + tabla Paimon frescos ---
    let warehouse = std::env::temp_dir().join(format!("tachyon-kinesis-wh-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("creando warehouse");
    let warehouse = warehouse.to_string_lossy().to_string();

    let table = create_test_table(
        &warehouse,
        DB,
        TABLE,
        &[
            (
                "order_id",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
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

    // --- 1. Stream fresco + 6 orders (1 cancelled) ---
    let client = kinesis_client().await;
    let stream = format!("tachyon-kinesis-{}", std::process::id());
    recreate_stream(&client, &stream).await;
    let orders = [
        (1i64, "paid", 10i64, 100.0f64),
        (2, "shipped", 11, 200.0),
        (3, "paid", 12, 300.0),
        (4, "cancelled", 13, 400.0),
        (5, "paid", 14, 500.0),
        (6, "shipped", 15, 600.0),
    ];
    put_orders(&client, &stream, &orders).await;

    // --- 2. Pipeline: kinesis -> Paimon ---
    let config = Arc::new(config_yaml(&warehouse, &stream));
    let options = run_options();
    let run_task = start_pipeline(&config, &table, &options).await;

    // --- 3. Las 5 orders no-canceladas salen una sola vez ---
    let rows = wait_rows(&table, 5, Duration::from_secs(60)).await;
    assert_eq!(
        rows,
        vec![(1, 100.0), (2, 200.0), (3, 300.0), (5, 500.0), (6, 600.0)],
        "exactly-once: {rows:?}"
    );
    // Un commit más de margen: sin duplicados.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let rows = read_rows(&table).await;
    assert_eq!(rows.len(), 5, "sin duplicados: {rows:?}");

    // --- 4. Reinicio: reanuda del checkpoint, no releer ---
    run_task.abort();
    let run_task = start_pipeline(&config, &table, &options).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let rows = read_rows(&table).await;
    assert_eq!(rows.len(), 5, "el reinicio no repite: {rows:?}");

    // --- 5. Un registro nuevo sigue llegando ---
    put_orders(&client, &stream, &[(7, "paid", 16, 700.0)]).await;
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
        "el registro nuevo sale: {rows:?}"
    );

    run_task.abort();
    let _ = std::fs::remove_dir_all(&warehouse);
}
