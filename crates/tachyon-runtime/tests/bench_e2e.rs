//! Benchmark **end-to-end sostenido** (broker real + Paimon).
//!
//! Mide el throughput real del pipeline completo (Redpanda -> DataFusion ->
//! Paimon) drenando un topic pre-cargado a máxima tasa. Al pre-producir todo el
//! dato ANTES de arrancar el pipeline, el broker alimenta al ritmo que el
//! pipeline pide (sin contención del producer), así que el número medido es el
//! techo de consumo del pipeline.
//!
//! La medición es en estado estacionario: se muestrea `rows_read` en dos puntos
//! del drenado (20% y 80% del total) y se calcula `delta / delta_t`, excluyendo
//! el arranque (conexión/subscribe) y la cola.
//!
//! Ejecutar (release):
//! ```
//! cargo test --release -p tachyon-runtime --test bench_e2e -- --ignored --nocapture
//! ```
//!
//! Para el análisis de **core pinning**, ver `bench_pinning.sh` (arranca el
//! binario `tachyon` con `taskset` sobre N P-cores y mide el drenado desde
//! fuera).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema};
use paimon::spec::{DataType as PDataType, BigIntType, DoubleType, VarCharType};
use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use tachyon_config::PipelineConfig;
use tachyon_runtime::{run_pipeline, RunOptions};
use tachyon_sink::writer::{create_test_table, PaimonSink};

const BROKER: &str = "localhost:9092";
const DB: &str = "default";
const TABLE: &str = "bench_lake";
const TOPIC: &str = "tachyon-bench-e2e";
/// Eventos pre-producidos. Lo suficientemente grande para que el drenado dure
/// varios segundos (medición estable) sin saturar el broker.
const EVENTS: i64 = 10_000_000;
/// Particiones del topic (overridable con `BENCH_PARTITIONS`). Con 1 partición
/// solo hay 1 consumidor activo; con N particiones se pueden usar N
/// consumidores paralelos (ver `BENCH_CONSUMERS`).
fn partitions() -> i32 {
    std::env::var("BENCH_PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}
/// Consumidores paralelos por topic (overridable con `BENCH_CONSUMERS`).
/// Default: `min(partitions, 4)` (mismo default opinionated del runtime).
fn consumers() -> usize {
    std::env::var("BENCH_CONSUMERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| partitions().max(1) as usize)
        .min(4)
        .max(1)
}

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("source_version", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
    ]))
}

/// CPU time (user+system) del proceso, en segundos.
fn cpu_seconds() -> f64 {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage.ru_utime.tv_sec as f64
            + usage.ru_utime.tv_usec as f64 / 1e6
            + usage.ru_stime.tv_sec as f64
            + usage.ru_stime.tv_usec as f64 / 1e6
    }
}

fn rss_mb() -> f64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(v) = line.strip_prefix("VmRSS:") {
                return v.trim().trim_end_matches(" kB").parse::<f64>().unwrap_or(0.0) / 1024.0;
            }
        }
    }
    0.0
}

/// Pinea el proceso a los `n` P-cores físicos (sin HT): 0, 2, 4, …, 14.
/// (i9-12900K: P-cores 0-15 con HT, E-cores 16-23. Los físicos son los pares.)
fn pin_to_cores(n: usize) {
    let p_cores = [0u32, 2, 4, 6, 8, 10, 12, 14];
    let take = n.min(p_cores.len());
    let mut mask: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    for &c in p_cores.iter().take(take) {
        unsafe { libc::CPU_SET(c as usize, &mut mask) };
    }
    let size = std::mem::size_of::<libc::cpu_set_t>();
    let rc = unsafe { libc::sched_setaffinity(0, size, &mask) };
    if rc != 0 {
        eprintln!("sched_setaffinity falló: {}", std::io::Error::last_os_error());
    } else {
        println!(
            "  pinned a {take} P-cores: {:?}",
            p_cores.iter().take(take).collect::<Vec<_>>()
        );
    }
}

/// Crea el topic (limpio) con `partitions()` particiones.
async fn ensure_topic() {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", BROKER);
    let admin: AdminClient<DefaultClientContext> = cc.create().expect("admin");
    let _ = admin.delete_topics(&[TOPIC], &Default::default()).await;
    admin
        .create_topics(&[NewTopic::new(TOPIC, partitions(), TopicReplication::Fixed(1))], &Default::default())
        .await
        .expect("creando topic");
}

/// Envía un mensaje drenando el buffer si está lleno (`send` nunca bloquea).
/// Reintenta hasta que quepa; devuelve `false` si no entra tras muchos intentos.
fn send_msg(producer: &BaseProducer, payload: &str, key: &str) -> bool {
    for _ in 0..2000 {
        let record = BaseRecord::to(TOPIC).payload(payload).key(key);
        match producer.send(record) {
            Ok(_) => return true,
            Err(_) => producer.poll(Duration::from_millis(10)),
        }
    }
    false
}

/// Pre-produce `EVENTS` eventos JSON al topic (producer síncrono, en lote).
fn pre_produce() {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", BROKER);
    cc.set("queue.buffering.max.messages", "1000000");
    cc.set("message.send.max.retries", "10");
    // Lotes grandes al broker: con el default (batch.size=16KB, sin linger)
    // cada fetch del consumer traía solo unas pocas cientos de filas y el
    // throughput quedaba acotado por la latencia de red. `linger.ms` agrupa
    // en batches de 1MB → cada round trip del consumer amortiza miles de filas.
    cc.set("batch.size", "1048576");
    cc.set("linger.ms", "5");
    let producer: BaseProducer = cc.create().expect("producer");
    let t0 = Instant::now();
    let mut ok = 0i64;
    let mut err = 0i64;
    for i in 0..EVENTS {
        let payload = format!(
            "{{\"order_id\":{},\"status\":\"paid\",\"source_version\":{},\"amount\":100.0}}",
            i, i
        );
        let key = i.to_string();
        // Particionado por clave (hash): el producer elige la partición.
        if send_msg(&producer, &payload, &key) {
            ok += 1;
        } else {
            err += 1;
            if err <= 5 {
                eprintln!("no se pudo enviar el evento {i} (buffer saturado)");
            }
        }
    }
    // Espera a que todo llegue al broker (flush bloqueante).
    let _ = producer.flush(Duration::from_secs(300));
    let dt = t0.elapsed().as_secs_f64();
    println!(
        "  pre-producción: {ok}/{EVENTS} eventos en {dt:.1}s ({:.0} ev/s){}",
        ok as f64 / dt,
        if err > 0 { format!(" ({err} errores)") } else { String::new() }
    );
    assert!(err == 0, "no deben haber errores de producción");
}

fn config_yaml(warehouse: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: bench-e2e
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
  consumers_per_topic: {consumers}
  commit_interval: 2s
  metrics:
    bind_addr: "127.0.0.1:0"
"#,
        warehouse = warehouse,
        topic = TOPIC,
        db = DB,
        table = TABLE,
        partitions = partitions(),
        consumers = consumers(),
    ))
    .expect("config válida")
}

#[tokio::test]
#[ignore = "benchmark: cargo test --release -p tachyon-runtime --test bench_e2e -- --ignored"]
async fn bench_e2e_sustained_drain() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();

    // Core pinning (opcional): `BENCH_PIN_CORES=N` pinea a N P-cores.
    if let Some(n) = std::env::var("BENCH_PIN_CORES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        pin_to_cores(n);
    }

    // --- 0. Warehouse + tabla Paimon ---
    let warehouse = std::env::temp_dir().join(format!("tachyon-bench-e2e-wh-{}", std::process::id()));
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

    // --- 1. Topic + pre-producción ---
    ensure_topic().await;
    pre_produce();

    // --- 2. Arranca el pipeline ---
    let config = Arc::new(config_yaml(&warehouse));
    // `BENCH_COMMIT_INTERVAL` (segundos) permite aislar el commit de offsets:
    // un valor grande (p. ej. 3600) deshabilita el commit durante el drenado.
    let commit_secs = std::env::var("BENCH_COMMIT_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let options = RunOptions {
        commit_interval: Duration::from_secs(commit_secs),
        metrics_bind: Some("127.0.0.1:0".parse().unwrap()),
        group_id: format!("tachyon-bench-e2e-{}", std::process::id()),
    };
    let sink = PaimonSink::from_table(table, "order_id", 1, Some("source_version"))
        .expect("sink");
    let input_schemas: HashMap<String, Arc<Schema>> =
        HashMap::from([("orders".to_string(), orders_schema())]);
    let metrics = Arc::new(tachyon_metrics::InstanceMetrics::new());
    let select_sql = "SELECT order_id, status, source_version, amount FROM orders";
    let metrics_read = metrics.clone();
    let run_task = tokio::spawn(async move {
        run_pipeline(&config, select_sql, &options, sink, &input_schemas, &metrics).await
    });

    // --- 3. Medición en estado estacionario ---
    // Espera a que el pipeline pase el arranque (20% del total consumido) y
    // muestrea; luego espera al 80% y calcula la tasa entre ambos puntos.
    let total = EVENTS as u64;
    let read = |m: &tachyon_metrics::InstanceMetrics| m.rows_read.load(std::sync::atomic::Ordering::Relaxed);

    // Punto 0: 20% consumido.
    let deadline = Instant::now() + Duration::from_secs(300);
    let (t0, r0, cpu_t0) = loop {
        if run_task.is_finished() {
            let result = run_task.await.expect("join del task del pipeline");
            match result {
                Ok(_) => panic!("el pipeline terminó prematuramente durante el benchmark"),
                Err(e) => panic!("el pipeline murió durante el benchmark: {e:#}"),
            }
        }
        if read(&metrics_read) >= total * 20 / 100 {
            break (Instant::now(), read(&metrics_read), cpu_seconds());
        }
        assert!(Instant::now() < deadline, "timeout esperando el 20%");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    // Punto 1: 80% consumido.
    let (t1, r1, cpu_t1) = loop {
        if read(&metrics_read) >= total * 80 / 100 {
            break (Instant::now(), read(&metrics_read), cpu_seconds());
        }
        assert!(Instant::now() < deadline, "timeout esperando el 80%");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let dt = t1.duration_since(t0).as_secs_f64();
    let drows = r1 - r0;
    let rows_s = drows as f64 / dt;
    let cpu_dt = cpu_t1 - cpu_t0;
    let cpu_pct = if dt > 0.0 { cpu_dt / dt * 100.0 } else { 0.0 };
    let rows_cpu_s = if cpu_dt > 0.0 { drows as f64 / cpu_dt } else { 0.0 };
    let rss = rss_mb();

    println!(
        "\n=== E2E sostenido ({p} particiones, {c} consumidores/topic) ===\n  ventana: {dt:.2}s (20%->80%)\n  filas: {drows}\n  throughput: {rows_s:.0} rows/s\n  CPU: {cpu_dt:.2}s ({cpu_pct:.0}% del wall)\n  por CPU-s: {rows_cpu_s:.0} rows/CPU-s\n  RSS: {rss:.0} MB\n  (total pre-producido: {total})",
        p = partitions(),
        c = consumers()
    );

    // --- Limpieza ---
    run_task.abort();
    let _ = std::fs::remove_dir_all(&warehouse);
}
