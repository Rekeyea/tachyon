//! Kinesis (floCi) como input -> Kinesis (floCi) como output: at-least-once.
//!
//! El filter se aplica y cada order no-cancelada se publica una sola vez en el
//! stream de salida (el registro es durable al publicarse; no hay transacción
//! que grupe salida y offsets). Un reinicio repubica desde el inicio: el
//! consumidor dedup con la columna de secuencia.
//!
//! Requiere floCi en localhost:4566:
//!   docker run -p 4566:4566 floci/floci:latest
//!
//!   cargo test -p tachyon-runtime --test live_kinesis_sink -- --ignored --nocapture

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema};
use tachyon_config::PipelineConfig;
use tachyon_runtime::{Pipeline, PreparedInput};

const ENDPOINT: &str = "http://localhost:4566";
const REGION: &str = "us-east-1";

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
    // entre CreateStream y el primer PutRecords/GetRecords.
    let t0 = Instant::now();
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

/// Lee todo el stream de salida (TrimHorizon, paginado) y devuelve
/// `(order_id, amount)` por registro.
async fn read_output(client: &aws_sdk_kinesis::Client, stream: &str) -> Vec<(i64, f64)> {
    let desc = client
        .describe_stream()
        .stream_name(stream)
        .send()
        .await
        .expect("describe_stream");
    let shards = desc
        .stream_description()
        .expect("stream_description")
        .shards()
        .to_vec();
    let mut out = Vec::new();
    for shard in shards {
        let shard_id = shard.shard_id().to_string();
        let mut iterator = client
            .get_shard_iterator()
            .stream_name(stream)
            .shard_id(&shard_id)
            .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon)
            .send()
            .await
            .expect("get_shard_iterator")
            .shard_iterator()
            .unwrap()
            .to_string();
        loop {
            let resp = client
                .get_records()
                .shard_iterator(&iterator)
                .limit(100)
                .send()
                .await
                .expect("get_records");
            let records = resp.records();
            if records.is_empty() {
                break;
            }
            for record in records {
                let body = String::from_utf8(record.data().as_ref().to_vec()).unwrap();
                let v: serde_json::Value = serde_json::from_str(&body).expect("JSON válido");
                out.push((
                    v["order_id"].as_i64().expect("order_id"),
                    v["amount"].as_f64().expect("amount"),
                ));
            }
            let Some(next) = resp.next_shard_iterator() else {
                break;
            };
            iterator = next.to_string();
        }
    }
    out
}

fn config_yaml(in_stream: &str, out_stream: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: kinesis-out-{pid}
connectors:
  kinesis:
    region: {region}
    endpoint: {endpoint}
inputs:
  - name: orders
    kinesis: {in_stream}
    key: order_id
    schema: json/orders
    start_from: trim_horizon
output:
  name: orders_out
  kinesis: {out_stream}
  key: order_id
deployment:
  partitions: 1
  commit_interval: 1s
"#,
        pid = std::process::id(),
        region = REGION,
        endpoint = ENDPOINT,
        in_stream = in_stream,
        out_stream = out_stream,
    ))
    .expect("config de test válida")
}

const SQL: &str = "INSERT INTO orders_out \
                   SELECT order_id, status, source_version, amount \
                   FROM orders WHERE status <> 'cancelled'";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requiere floCi en localhost:4566"]
async fn live_kinesis_sink_at_least_once() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    // --- 1. Streams frescos + 6 orders (1 cancelled) en el input ---
    let client = kinesis_client().await;
    let in_stream = format!("tachyon-in-{}", std::process::id());
    let out_stream = format!("tachyon-out-{}", std::process::id());
    recreate_stream(&client, &in_stream).await;
    recreate_stream(&client, &out_stream).await;
    let orders = [
        (1i64, "paid", 10i64, 100.0f64),
        (2, "shipped", 11, 200.0),
        (3, "paid", 12, 300.0),
        (4, "cancelled", 13, 400.0),
        (5, "paid", 14, 500.0),
        (6, "shipped", 15, 600.0),
    ];
    put_orders(&client, &in_stream, &orders).await;

    // --- 2. Pipeline: kinesis -> kinesis ---
    let config = config_yaml(&in_stream, &out_stream);
    let pipeline = Pipeline::new(&config, SQL).expect("compilando el pipeline");
    let input_codecs =
        HashMap::from([("orders".to_string(), PreparedInput::json(orders_schema()))]);
    let run_task = tokio::spawn(async move { pipeline.run(&input_codecs).await });

    // --- 3. Las 5 orders no-canceladas salen al stream de salida ---
    let t0 = Instant::now();
    let mut rows = Vec::new();
    loop {
        rows = read_output(&client, &out_stream).await;
        if rows.len() >= 5 {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(60),
            "esperando 5 orders de salida, hay {rows:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // El filter descarta la 4; el set único de ids es exactamente {1,2,3,5,6}.
    let mut unique: Vec<(i64, f64)> = rows;
    unique.sort_by_key(|(id, _)| *id);
    unique.dedup_by_key(|(id, _)| *id);
    assert_eq!(
        unique,
        vec![(1, 100.0), (2, 200.0), (3, 300.0), (5, 500.0), (6, 600.0)],
        "el filter aplica y salen las 5: {unique:?}"
    );
    // Un commit más de margen: sin duplicados en una sola ejecución.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let rows = read_output(&client, &out_stream).await;
    assert_eq!(rows.len(), 5, "sin duplicados: {rows:?}");

    // --- 4. Un registro nuevo sigue llegando ---
    put_orders(&client, &in_stream, &[(7, "paid", 16, 700.0)]).await;
    let t0 = Instant::now();
    loop {
        let rows = read_output(&client, &out_stream).await;
        if rows.iter().any(|(id, _)| *id == 7) {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "esperando el order 7 de salida"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    run_task.abort();
    let _ = client.delete_stream().stream_name(&in_stream).send().await;
    let _ = client.delete_stream().stream_name(&out_stream).send().await;
}
