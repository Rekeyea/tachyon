//! Dos generadores, web y app, publican en el mismo topic. Un Tachyon los
//! agrega con una ventana tumbling de 1 segundo: COUNT y SUM por `order_id`.
//! La ventana lee un solo input, así que los dos streams comparten el topic.
//!
//!   cargo test -p tachyon-runtime --profile benchfast --test bench_window -- --ignored --nocapture --test-threads=1

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use paimon::spec::{BigIntType, DataType as PDataType};
use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tachyon_config::PipelineConfig;
use tachyon_metrics::InstanceMetrics;
use tachyon_runtime::{run_pipeline, PreparedInput, RunOptions, StatelessBudget};
use tachyon_sink::writer::{create_test_table, open_table, read_table_rows, PaimonSink};
use tachyon_sql::parse_sql;

const BROKER: &str = "localhost:9092";
const PARTITIONS: usize = 8;
const CPUS: &str = "8";
const KEYS_PER_STREAM: u64 = 256;
const ROWS_PER_STREAM: u64 = 1_000_000;
const WEB_BASE: i64 = 1;
const APP_BASE: i64 = 100_001;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("reloj")
        .as_millis() as i64
}

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn config_yaml(name: &str, warehouse: &str, topic: &str, table: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: {name}
connectors:
  redpanda:
    brokers: [{BROKER}]
  paimon:
    warehouse: {warehouse}
    catalog: local
inputs:
  - name: orders
    topic: {topic}
    key: order_id
    schema: json/orders
    watermark:
      column: event_time
      lag: 1ms
output:
  name: orders_lake
  table: default.{table}
  key: order_id
  bucket: {PARTITIONS}
  sequence_field: window_end
deployment:
  partitions: {PARTITIONS}
  commit_interval: 1s
  resources:
    cpu: "{CPUS}"
"#
    ))
    .expect("yaml")
}

struct Produced {
    rows: u64,
    max_event_ms: i64,
}

fn produce(topic: &str, base: i64, rows: u64, label: &str) -> Produced {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime del productor");
    rt.block_on(async {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", BROKER)
            .set("linger.ms", "5")
            .set("batch.size", "1048576")
            .set("acks", "1")
            .set("queue.buffering.max.messages", "1000000")
            .create()
            .expect("producer");
        let mut inflight = FuturesUnordered::new();
        let mut sent = 0u64;
        let mut max_event_ms = 0i64;
        for i in 0..rows {
            let order_id = base + (i % KEYS_PER_STREAM) as i64;
            let event_time = now_ms();
            max_event_ms = max_event_ms.max(event_time);
            let key = order_id.to_string();
            let payload =
                format!(r#"{{"order_id":{order_id},"event_time":{event_time},"amount":1}}"#);
            loop {
                match producer.send_result(FutureRecord::to(topic).key(&key).payload(&payload)) {
                    Ok(delivery) => {
                        inflight.push(delivery);
                        break;
                    }
                    Err((
                        KafkaError::MessageProduction(rdkafka::error::RDKafkaErrorCode::QueueFull),
                        _,
                    )) => {
                        if let Some(done) = inflight.next().await {
                            done.expect("delivery").expect("ack");
                            sent += 1;
                        } else {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    }
                    Err((err, _)) => panic!("{label} no pudo encolar: {err}"),
                }
            }
            if inflight.len() >= 2_000 {
                inflight
                    .next()
                    .await
                    .expect("inflight")
                    .expect("delivery")
                    .expect("ack");
                sent += 1;
            }
            if sent > 0 && sent % 200_000 == 0 {
                eprintln!("{label}: {sent} filas");
            }
        }
        while let Some(done) = inflight.next().await {
            done.expect("delivery").expect("ack");
            sent += 1;
        }
        eprintln!("{label}: {sent} filas publicadas");
        Produced {
            rows: sent,
            max_event_ms,
        }
    })
}

fn produce_rows(topic: &str, rows: &[(i64, i64)]) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime del productor");
    rt.block_on(async {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", BROKER)
            .create()
            .expect("producer");
        for (order_id, event_time) in rows {
            let key = order_id.to_string();
            let payload =
                format!(r#"{{"order_id":{order_id},"event_time":{event_time},"amount":1}}"#);
            producer
                .send(
                    FutureRecord::to(topic).key(&key).payload(&payload),
                    Duration::from_secs(30),
                )
                .await
                .expect("closer");
        }
    });
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
            &[NewTopic::new(
                topic,
                PARTITIONS as i32,
                TopicReplication::Fixed(1),
            )],
            &Default::default(),
        )
        .await
        .expect("topic");
}

struct WindowRow {
    order_id: i64,
    window_start: i64,
    window_end: i64,
    n: i64,
}

fn rows_of(batches: &[arrow::record_batch::RecordBatch]) -> Vec<WindowRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("order_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let starts = batch
            .column_by_name("window_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ends = batch
            .column_by_name("window_end")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ns = batch
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push(WindowRow {
                order_id: ids.value(i),
                window_start: starts.value(i),
                window_end: ends.value(i),
                n: ns.value(i),
            });
        }
    }
    rows
}

fn percentile(sorted: &[i64], q: f64) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[index.min(sorted.len() - 1)]
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

#[test]
#[ignore = "mide throughput contra Redpanda local; cargo test --profile benchfast"]
fn bench_window_two_streams() {
    let _ = tracing_subscriber::fmt().with_env_filter("warn").try_init();
    let pid = std::process::id();
    let topic = format!("tachyon-bench-orders-{pid}");
    let table_name = format!("orders_window_{pid}");
    let warehouse = std::env::temp_dir().join(format!("tachyon-bench-wh-{pid}"));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("warehouse");
    let _cleanup = WarehouseGuard(warehouse.clone());

    let config = config_yaml(
        &format!("bench-window-{pid}"),
        &warehouse.to_string_lossy(),
        &topic,
        &table_name,
    );
    let budget = StatelessBudget::resolve(&config).expect("presupuesto");
    eprintln!(
        "presupuesto: cpus={} consumidores={} decode={} batch={} particiones={}",
        budget.cpus,
        budget.consumers_per_topic,
        budget.decode_parallelism,
        budget.batch_size,
        PARTITIONS
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(budget.worker_threads())
        .max_blocking_threads(budget.max_blocking_threads())
        .enable_all()
        .thread_name("tachyon-bench")
        .build()
        .expect("runtime");

    rt.block_on(async {
        recreate(&topic).await;
        let table = create_test_table(
            &warehouse.to_string_lossy(),
            "default",
            &table_name,
            &[
                ("order_id", PDataType::BigInt(BigIntType::with_nullable(false))),
                (
                    "window_start",
                    PDataType::BigInt(BigIntType::with_nullable(false)),
                ),
                ("window_end", PDataType::BigInt(BigIntType::new())),
                ("n", PDataType::BigInt(BigIntType::new())),
                ("amount", PDataType::BigInt(BigIntType::new())),
            ],
            &["order_id", "window_start"],
            PARTITIONS as i32,
            Some("window_end"),
        )
        .await
        .expect("tabla");

        eprintln!("publicando web y app, {ROWS_PER_STREAM} filas cada uno");
        let topic_web = topic.clone();
        let topic_app = topic.clone();
        let (web, app) = tokio::task::spawn_blocking(move || {
            let web_topic = topic_web.clone();
            let app_topic = topic_app.clone();
            let web = std::thread::spawn(move || produce(&web_topic, WEB_BASE, ROWS_PER_STREAM, "web"));
            let app = std::thread::spawn(move || produce(&app_topic, APP_BASE, ROWS_PER_STREAM, "app"));
            (web.join().expect("web"), app.join().expect("app"))
        })
        .await
        .expect("productores");
        let backlog = web.rows + app.rows;
        let max_event = web.max_event_ms.max(app.max_event_ms);
        eprintln!(
            "backlog listo: {} filas (web {}, app {}), max event {}",
            backlog, web.rows, app.rows, max_event
        );

        let sql = "INSERT INTO orders_lake \
            SELECT order_id, window_start, window_end, COUNT(*) AS n, SUM(amount) AS amount \
            FROM orders \
            GROUP BY order_id, TUMBLE(event_time, INTERVAL '1' SECOND)";
        let parsed = parse_sql(sql).expect("sql");
        let window = parsed.window.expect("ventana");
        let select_sql = parsed.select_sql.clone();
        let options = RunOptions {
            commit_interval: Duration::from_secs(1),
            metrics_bind: None,
            group_id: format!("tachyon-bench-window-{pid}"),
            commit_user: format!("tachyon-bench-window-{pid}-local"),
        };
        let sink = PaimonSink::from_table(table, "order_id", PARTITIONS as i32, Some("window_end"))
            .expect("sink");
        let codecs = std::collections::HashMap::from([(
            "orders".to_string(),
            PreparedInput::json(input_schema()),
        )]);
        let metrics = Arc::new(InstanceMetrics::new());
        let metrics_task = metrics.clone();
        let started = Instant::now();
        let task = tokio::spawn(async move {
            run_pipeline(
                &config,
                &select_sql,
                &options,
                sink,
                &codecs,
                &metrics_task,
                Some(&window),
            )
            .await
        });

        let mut first_row: Option<Duration> = None;
        let mut caught: Option<Duration> = None;
        let mut rows_at_first = 0u64;
        let mut last_print = Instant::now();
        while started.elapsed() < Duration::from_secs(180) {
            if task.is_finished() {
                panic!("el pipeline murió: {}", pipeline_died(task.await));
            }
            let rows = metrics.rows_read.load(Ordering::Relaxed);
            if rows > 0 && first_row.is_none() {
                first_row = Some(started.elapsed());
                rows_at_first = rows;
            }
            if last_print.elapsed() >= Duration::from_secs(1) {
                eprintln!(
                    "drenando: {} / {} filas en {:.1}s",
                    rows,
                    backlog,
                    started.elapsed().as_secs_f64()
                );
                last_print = Instant::now();
            }
            if rows >= backlog {
                caught = Some(started.elapsed());
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let caught = caught.unwrap_or_else(|| {
            panic!(
                "no drenó el backlog en 180s: {} / {}",
                metrics.rows_read.load(Ordering::Relaxed),
                backlog
            );
        });
        let first = first_row.expect("nunca leyó");
        let steady_rows = backlog.saturating_sub(rows_at_first);
        let steady_secs = (caught - first).as_secs_f64().max(0.001);
        let steady = if steady_rows == 0 {
            backlog as f64 / caught.as_secs_f64().max(0.001)
        } else {
            steady_rows as f64 / steady_secs
        };
        let snap_read = metrics.rows_read.load(Ordering::Relaxed);
        let snap_written = metrics.rows_written.load(Ordering::Relaxed);
        let snap_commits = metrics.commits.load(Ordering::Relaxed);
        let snap_source_ns = metrics.source_next_ns.load(Ordering::Relaxed);
        let snap_write_ns = metrics.write_ns.load(Ordering::Relaxed);
        let snap_commit_ns = metrics.commit_ns.load(Ordering::Relaxed);
        let snap_wait_ns = metrics.send_wait_ns.load(Ordering::Relaxed);

        let mut closers = Vec::new();
        // Un evento por clave, un poco por delante del reloj, cierra las
        // ventanas del backlog. El burst empieza después de que ese tiempo
        // ya pasó, así que no queda marcado como tarde.
        let closer_time = now_ms() + 1_500;
        for id in 0..KEYS_PER_STREAM {
            closers.push((WEB_BASE + id as i64, closer_time));
            closers.push((APP_BASE + id as i64, closer_time));
        }
        let topic_closer = topic.clone();
        let with_closers = backlog + closers.len() as u64;
        tokio::task::spawn_blocking(move || produce_rows(&topic_closer, &closers))
            .await
            .expect("closers");
        let close_deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < close_deadline {
            if task.is_finished() {
                panic!("el pipeline murió: {}", pipeline_died(task.await));
            }
            if metrics.rows_read.load(Ordering::Relaxed) >= with_closers {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        let reader = open_table(&warehouse.to_string_lossy(), "default", &table_name)
            .await
            .expect("abrir tabla");
        let closed = rows_of(&read_table_rows(&reader).await.expect("leer"));
        let summed: i64 = closed.iter().map(|row| row.n).sum();
        let web_rows = closed.iter().filter(|row| row.order_id < APP_BASE).count();
        let app_rows = closed.iter().filter(|row| row.order_id >= APP_BASE).count();

        let burst_start = now_ms();
        let burst_topic = topic.clone();
        let burst = tokio::task::spawn_blocking(move || {
            let ends = Instant::now() + Duration::from_secs(5);
            let mut sent = 0u64;
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(async {
                let producer: FutureProducer = ClientConfig::new()
                    .set("bootstrap.servers", BROKER)
                    .set("linger.ms", "5")
                    .set("acks", "1")
                    .create()
                    .expect("producer");
                let mut i = 0u64;
                while Instant::now() < ends {
                    let order_id = if i % 2 == 0 {
                        WEB_BASE + (i % KEYS_PER_STREAM) as i64
                    } else {
                        APP_BASE + (i % KEYS_PER_STREAM) as i64
                    };
                    let event_time = now_ms();
                    let key = order_id.to_string();
                    let payload = format!(
                        r#"{{"order_id":{order_id},"event_time":{event_time},"amount":1}}"#
                    );
                    producer
                        .send(
                            FutureRecord::to(&burst_topic).key(&key).payload(&payload),
                            Duration::from_secs(10),
                        )
                        .await
                        .expect("burst");
                    sent += 1;
                    i += 1;
                }
                sent
            })
        });

        let mut seen: BTreeMap<(i64, i64), i64> = BTreeMap::new();
        let burst_deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < burst_deadline {
            if task.is_finished() {
                panic!("el pipeline murió: {}", pipeline_died(task.await));
            }
            if let Ok(batches) = read_table_rows(&reader).await {
                let seen_at = now_ms();
                for row in rows_of(&batches) {
                    if row.window_start < burst_start {
                        continue;
                    }
                    seen.entry((row.order_id, row.window_start)).or_insert(seen_at);
                }
            }
            if burst.is_finished() && seen.len() > KEYS_PER_STREAM as usize {
                tokio::time::sleep(Duration::from_secs(2)).await;
                if let Ok(batches) = read_table_rows(&reader).await {
                    let seen_at = now_ms();
                    for row in rows_of(&batches) {
                        if row.window_start < burst_start {
                            continue;
                        }
                        seen.entry((row.order_id, row.window_start)).or_insert(seen_at);
                    }
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        let burst_rows = burst.await.expect("burst");
        let mut latencies: Vec<i64> = seen
            .iter()
            .map(|((_, start), seen_at)| seen_at - (start + 1_000))
            .filter(|latency| *latency >= 0)
            .collect();
        latencies.sort_unstable();

        task.abort();
        let _ = task.await;

        eprintln!();
        eprintln!("query: {sql}");
        eprintln!(
            "streams: web {WEB_BASE}..{} y app {APP_BASE}..{}, topic {topic}, particiones {PARTITIONS}",
            WEB_BASE + KEYS_PER_STREAM as i64 - 1,
            APP_BASE + KEYS_PER_STREAM as i64 - 1
        );
        eprintln!(
            "arranque hasta la primera fila: {:.2}s",
            first.as_secs_f64()
        );
        eprintln!(
            "throughput de entrada: {:.0} filas/s ({:.0} por CPU de las {}), de {:.2}s entre la primera fila y el backlog",
            steady,
            steady / budget.cpus as f64,
            budget.cpus,
            steady_secs
        );
        eprintln!(
            "en ese tramo el sink escribió {} filas de ventana en {} commits",
            snap_written, snap_commits
        );
        eprintln!(
            "tiempos medios del drenado: next {:.1} µs/fila, espera al writer {:.1} µs/fila, write {:.1} µs/fila de salida, commit {:.1} ms",
            snap_source_ns as f64 / snap_read.max(1) as f64 / 1_000.0,
            snap_wait_ns as f64 / snap_read.max(1) as f64 / 1_000.0,
            snap_write_ns as f64 / snap_written.max(1) as f64 / 1_000.0,
            snap_commit_ns as f64 / snap_commits.max(1) as f64 / 1_000_000.0
        );
        eprintln!(
            "ventanas cerradas del backlog: {} filas (web {web_rows}, app {app_rows}), SUM(n)={summed}, backlog={backlog}",
            closed.len()
        );
        if latencies.is_empty() {
            eprintln!(
                "latencia: el burst de {burst_rows} filas no dejó ventanas visibles con window_end posterior al arranque"
            );
        } else {
            eprintln!(
                "latencia de la ventana (aparece en Paimon − window_end), {} ventanas, burst {burst_rows} filas en 5s: p50 {} ms, p99 {} ms, max {} ms",
                latencies.len(),
                percentile(&latencies, 0.50),
                percentile(&latencies, 0.99),
                latencies[latencies.len() - 1]
            );
        }
    });
}

struct WarehouseGuard(std::path::PathBuf);

impl Drop for WarehouseGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
