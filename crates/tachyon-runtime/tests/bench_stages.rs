//! Benchmark de **descomposición por etapa** (sin broker, cómputo puro).
//!
//! Mide cada etapa del pipeline en aislamiento, en modo release, para responder
//! "¿dónde va el CPU?" y cuál es el techo de throughput por etapa:
//!
//! 1. **JSON decode** (`Decoder::decode`): payloads JSON -> `RecordBatch`.
//! 2. **Transform DataFusion** (`execute_query`): filter+project sobre una
//!    fuente in-memory (el plan es idéntico al de producción: `target_partitions=1`,
//!    `batch_size=1`, tabla `infinite`).
//! 3. **Paimon write+commit** (`PaimonSink`): escritura amortizada + commit LSM.
//!
//! El mínimo de estas tasas es el cuello de botella teórico del pipeline.
//! El E2E real (con broker) vive en `bench_e2e.rs`.
//!
//! Ejecutar (release, necesario para números realistas):
//! ```
//! cargo test --release -p tachyon-runtime --test bench_stages -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use paimon::spec::{DataType as PDataType, BigIntType, DoubleType, VarCharType};
use tachyon_runtime::{execute_query, InputSource, StreamTableFactory};
use tachyon_sink::writer::{create_test_table, PaimonSink};
use tachyon_source::decode::{DecodeFormat, Decoder};

const N: usize = 1_000_000; // filas por benchmark

/// CPU time (user + system) del proceso, en segundos, vía `getrusage`.
fn cpu_seconds() -> f64 {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        let ut = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1e6;
        let st = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1e6;
        ut + st
    }
}

/// RSS del proceso en MB, desde `/proc/self/status`.
fn rss_mb() -> f64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(v) = line.strip_prefix("VmRSS:") {
                let kb: f64 = v.trim().trim_end_matches(" kB").parse().unwrap_or(0.0);
                return kb / 1024.0;
            }
        }
    }
    0.0
}

/// Imprime un resumen de benchmark: rows/s, CPU, rows/CPU-s, RSS.
fn report(name: &str, rows: usize, wall: f64, cpu: f64, rss: f64) {
    let rows_s = rows as f64 / wall;
    let rows_cpu_s = if cpu > 0.0 { rows as f64 / cpu } else { f64::NAN };
    let cpu_pct = if wall > 0.0 { cpu / wall * 100.0 } else { 0.0 };
    println!(
        "\n=== {name} ===\n  filas:        {rows}\n  wall:         {wall:.3}s\n  CPU:          {cpu:.3}s ({cpu_pct:.0}% del wall)\n  throughput:   {rows_s:.0} rows/s\n  por CPU-s:    {rows_cpu_s:.0} rows/CPU-s\n  RSS:          {rss:.0} MB"
    );
}

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("source_version", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
    ]))
}

/// `PartitionStream` finita que emite una lista de `RecordBatch` (fuente
/// in-memory para el benchmark de DataFusion).
struct BatchSource {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl std::fmt::Debug for BatchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchSource")
            .field("batches", &self.batches.len())
            .finish()
    }
}

impl PartitionStream for BatchSource {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batches = self.batches.clone();
        let schema = self.schema.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches.into_iter().map(Ok)),
        ))
    }
}

/// Construye `n` `RecordBatch` de `chunk` filas cada uno con datos deterministas.
fn build_batches(n: usize, chunk: usize) -> Vec<RecordBatch> {
    let schema = orders_schema();
    let mut batches = Vec::new();
    for start in (0..n).step_by(chunk) {
        let end = (start + chunk).min(n);
        let ids: Vec<i64> = (start..end).map(|i| i as i64).collect();
        let status: Vec<&str> = vec!["paid"; end - start];
        let versions: Vec<i64> = (start..end).map(|i| i as i64).collect();
        let amounts: Vec<f64> = (start..end).map(|i| (i % 1000) as f64).collect();
        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(status)),
                    Arc::new(Int64Array::from(versions)),
                    Arc::new(Float64Array::from(amounts)),
                ],
            )
            .expect("batch válido"),
        );
    }
    batches
}

/// Etapa 1: decodificación JSON -> RecordBatch.
#[tokio::test]
#[ignore = "benchmark: cargo test --release -p tachyon-runtime --test bench_stages -- --ignored"]
async fn bench_json_decode() {
    let schema = orders_schema();
    let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
    // Pre-construir payloads (fuera del timing).
    let payloads: Vec<Vec<u8>> = (0..N)
        .map(|i| {
            format!(
                "{{\"order_id\":{i},\"status\":\"paid\",\"source_version\":{i},\"amount\":100.0}}"
            )
            .into_bytes()
        })
        .collect();
    let chunk = 1000;
    // Warmup.
    let _ = decoder.decode(&payloads[..chunk]).expect("warmup decode");
    // Timed.
    let t0 = Instant::now();
    let cpu0 = cpu_seconds();
    let mut rows = 0;
    for c in payloads.chunks(chunk) {
        rows += decoder.decode(c).expect("decode").num_rows();
    }
    let wall = t0.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu0;
    report("1. JSON decode (arrow-json)", rows, wall, cpu, rss_mb());
}

/// Etapa 2: transformación DataFusion (filter + project), plan de producción.
#[tokio::test]
#[ignore = "benchmark: cargo test --release -p tachyon-runtime --test bench_stages -- --ignored"]
async fn bench_datafusion_transform() {
    let schema = orders_schema();
    let chunk = 10_000;
    let batches = build_batches(N, chunk);
    let source = Arc::new(BatchSource {
        schema: schema.clone(),
        batches,
    });
    // Factory idéntica a la de producción (tabla infinite), pero con la fuente
    // in-memory finita (el stream termina cuando la fuente se agota).
    let factory: Box<StreamTableFactory> = Box::new(move |_name, schema| {
        let table = StreamingTable::try_new(schema, vec![source.clone()])
            .map_err(|e| anyhow::anyhow!("tabla: {e}"))?;
        Ok(Arc::new(table.with_infinite_table(true)))
    });
    let inputs = vec![InputSource {
        name: "orders".into(),
        schema: schema.clone(),
    }];
    let sql = "SELECT order_id, status, source_version, amount \
              FROM orders WHERE status <> 'cancelled'";
    // Warmup: planificar + correr un tramo (el plan se re-crea por llamada).
    {
        let s = execute_query(sql, &inputs, &factory).await.expect("warmup plan");
        let mut s = s;
        let mut n = 0;
        while n < chunk && s.next().await.is_some() {
            n += 1;
        }
    }
    // Timed: planificar + ejecutar + consumir todo.
    let t0 = Instant::now();
    let cpu0 = cpu_seconds();
    let mut stream = execute_query(sql, &inputs, &factory).await.expect("plan");
    let mut rows = 0;
    while let Some(b) = stream.next().await {
        rows += b.expect("batch").num_rows();
    }
    let wall = t0.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu0;
    report("2. DataFusion transform (filter+project)", rows, wall, cpu, rss_mb());
}

/// Etapa 3: escritura + commit a Paimon (LSM local).
#[tokio::test]
#[ignore = "benchmark: cargo test --release -p tachyon-runtime --test bench_stages -- --ignored"]
async fn bench_paimon_write_commit() {
    let warehouse = std::env::temp_dir().join(format!(
        "tachyon-bench-paimon-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(&warehouse).expect("warehouse");
    let warehouse = warehouse.to_string_lossy().to_string();

    let table = create_test_table(
        &warehouse,
        "default",
        "bench",
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

    let chunk = 10_000;
    let batches = build_batches(N, chunk);
    let mut sink = PaimonSink::from_table(table.clone(), "order_id", 1, Some("source_version"))
        .expect("sink");

    // Warmup: un write + commit pequeño.
    {
        let mut warm = PaimonSink::from_table(table, "order_id", 1, Some("source_version"))
            .expect("sink warm");
        warm.write(&batches[0]).await.expect("warm write");
        warm.commit().await.expect("warm commit");
    }

    // Timed: escribir todos los batches + commit final.
    let t0 = Instant::now();
    let cpu0 = cpu_seconds();
    let mut rows = 0;
    for b in &batches {
        sink.write(b).await.expect("write");
        rows += b.num_rows();
    }
    let write_wall = t0.elapsed().as_secs_f64();
    sink.commit().await.expect("commit");
    let wall = t0.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu0;
    report("3. Paimon write+commit (LSM local)", rows, wall, cpu, rss_mb());
    println!("  (write amortizado: {write_wall:.3}s -> {:.0} rows/s)", rows as f64 / write_wall);

    let _ = std::fs::remove_dir_all(&warehouse);
}
