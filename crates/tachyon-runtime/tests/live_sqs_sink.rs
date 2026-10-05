//! SQS (floCi) como input -> SQS (floCi) como output: at-least-once.
//!
//! El filter se aplica y cada order no-cancelada se publica una sola vez en la
//! cola de salida (`SendMessageBatch`; el registro es durable al publicarse).
//! Los mensajes del input se borran después de publicarse (`DeleteMessageBatch`
//! en el commit). Un reinicio repubica desde el inicio: el consumidor dedup con
//! la columna de secuencia.
//!
//! Requiere floCi en localhost:4566:
//!   docker run -p 4566:4566 floci/floci:latest
//!
//!   cargo test -p tachyon-runtime --test live_sqs_sink -- --ignored --nocapture

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

async fn recreate_queue(client: &aws_sdk_sqs::Client, queue: &str) -> String {
    if let Ok(existing) = client.get_queue_url().queue_name(queue).send().await {
        if let Some(url) = existing.queue_url() {
            let _ = client.delete_queue().queue_url(url).send().await;
        }
    }
    let resp = client
        .create_queue()
        .queue_name(queue)
        .send()
        .await
        .expect("create_queue");
    resp.queue_url().unwrap().to_string()
}

async fn put_orders(client: &aws_sdk_sqs::Client, url: &str, orders: &[(i64, &str, i64, f64)]) {
    for (order_id, status, version, amount) in orders {
        let payload = format!(
            r#"{{"order_id":{order_id},"status":"{status}","source_version":{version},"amount":{amount}}}"#
        );
        client
            .send_message()
            .queue_url(url)
            .message_body(payload)
            .send()
            .await
            .expect("send_message");
    }
}

/// Cuenta mensajes en la cola sin consumirlos (visible + en vuelo). `receive`
/// es destructivo (visibility timeout), así que para el conteo se usa
/// `get_queue_attributes`.
async fn count_output(client: &aws_sdk_sqs::Client, url: &str) -> usize {
    let resp = client
        .get_queue_attributes()
        .queue_url(url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
        .attribute_names(
            aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
        )
        .send()
        .await
        .expect("get_queue_attributes");
    let attrs = resp.attributes().cloned().unwrap_or_default();
    let get = |name: aws_sdk_sqs::types::QueueAttributeName| {
        attrs.get(&name).and_then(|v| v.parse().ok()).unwrap_or(0)
    };
    get(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
        + get(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
}

/// Drena la cola de salida (receive es destructivo) y devuelve
/// `(order_id, amount)` por mensaje. Se usa una sola vez para verificar el
/// filter y los montos.
async fn drain_output(client: &aws_sdk_sqs::Client, url: &str) -> Vec<(i64, f64)> {
    let mut out = Vec::new();
    loop {
        let resp = client
            .receive_message()
            .queue_url(url)
            .max_number_of_messages(10)
            .send()
            .await
            .expect("receive_message");
        let messages = resp.messages();
        if messages.is_empty() {
            break;
        }
        for message in messages {
            let body = message.body().unwrap();
            let v: serde_json::Value = serde_json::from_str(body).expect("JSON válido");
            out.push((
                v["order_id"].as_i64().expect("order_id"),
                v["amount"].as_f64().expect("amount"),
            ));
        }
    }
    out
}

fn config_yaml(in_queue: &str, out_queue: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: sqs-out-{pid}
connectors:
  sqs:
    region: {region}
    endpoint: {endpoint}
inputs:
  - name: orders
    sqs: {in_queue}
    key: order_id
    schema: json/orders
output:
  name: orders_out
  sqs: {out_queue}
  key: order_id
deployment:
  partitions: 1
  commit_interval: 1s
"#,
        pid = std::process::id(),
        region = REGION,
        endpoint = ENDPOINT,
        in_queue = in_queue,
        out_queue = out_queue,
    ))
    .expect("config de test válida")
}

const SQL: &str = "INSERT INTO orders_out \
                   SELECT order_id, status, source_version, amount \
                   FROM orders WHERE status <> 'cancelled'";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requiere floCi en localhost:4566"]
async fn live_sqs_sink_at_least_once() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    // --- 1. Colas frescas + 6 orders (1 cancelled) en el input ---
    let client = sqs_client().await;
    let in_queue = format!("tachyon-in-{}", std::process::id());
    let out_queue = format!("tachyon-out-{}", std::process::id());
    let in_url = recreate_queue(&client, &in_queue).await;
    let out_url = recreate_queue(&client, &out_queue).await;
    let orders = [
        (1i64, "paid", 10i64, 100.0f64),
        (2, "shipped", 11, 200.0),
        (3, "paid", 12, 300.0),
        (4, "cancelled", 13, 400.0),
        (5, "paid", 14, 500.0),
        (6, "shipped", 15, 600.0),
    ];
    put_orders(&client, &in_url, &orders).await;

    // --- 2. Pipeline: sqs -> sqs ---
    let config = config_yaml(&in_queue, &out_queue);
    let pipeline = Pipeline::new(&config, SQL).expect("compilando el pipeline");
    let input_codecs =
        HashMap::from([("orders".to_string(), PreparedInput::json(orders_schema()))]);
    let run_task = tokio::spawn(async move { pipeline.run(&input_codecs).await });

    // --- 3. Las 5 orders no-canceladas salen a la cola de salida ---
    let t0 = Instant::now();
    loop {
        let count = count_output(&client, &out_url).await;
        if count >= 5 {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(60),
            "esperando 5 orders de salida, hay {count}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // Se leen los payloads una vez (destructivo) para verificar el filter y
    // los montos. El filter descarta la 4: el set único es {1,2,3,5,6}.
    let rows = drain_output(&client, &out_url).await;
    let mut unique: Vec<(i64, f64)> = rows.clone();
    unique.sort_by_key(|(id, _)| *id);
    unique.dedup_by_key(|(id, _)| *id);
    assert_eq!(
        unique,
        vec![(1, 100.0), (2, 200.0), (3, 300.0), (5, 500.0), (6, 600.0)],
        "el filter aplica y salen las 5: {unique:?}"
    );
    assert_eq!(rows.len(), 5, "sin duplicados en la lectura: {rows:?}");

    // Un commit más de margen: el total de la cola sigue en 5 (sin duplicados).
    tokio::time::sleep(Duration::from_secs(3)).await;
    let count = count_output(&client, &out_url).await;
    assert_eq!(count, 5, "sin duplicados: {count}");

    // --- 4. Un registro nuevo sigue llegando ---
    put_orders(&client, &in_url, &[(7, "paid", 16, 700.0)]).await;
    let t0 = Instant::now();
    loop {
        let count = count_output(&client, &out_url).await;
        if count >= 6 {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "esperando el order 7 de salida, hay {count}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    run_task.abort();
    let _ = client.delete_queue().queue_url(&in_url).send().await;
    let _ = client.delete_queue().queue_url(&out_url).send().await;
}
