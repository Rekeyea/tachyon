//! NEWS2 sobre una tabla Paimon: dos dispositivos de un paciente, un signo
//! que no llegó, y una fila ancha. El snapshot siguiente trae un evento tarde
//! y otro que cierra la ventana abierta. El reinicio no la escribe dos veces.
//!
//! No usa Redpanda. El warehouse es un directorio local.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use paimon::spec::{BigIntType, DataType as PDataType, DoubleType, VarCharType};
use tachyon_config::PipelineConfig;
use tachyon_runtime::Pipeline;
use tachyon_sink::writer::{create_test_table, read_table_rows, PaimonSink};

const SQL: &str = "INSERT INTO gdnews2_scores \
SELECT patient_id, window_start, window_end, \
MAX(CASE WHEN measurement_type = 'HEART_RATE' THEN value END) AS heart_rate_value, \
COALESCE(MAX(CASE WHEN measurement_type = 'HEART_RATE' THEN score END), 0) AS heart_rate_score, \
COALESCE(MAX(CASE WHEN measurement_type = 'TEMPERATURE' THEN score END), 0) AS temperature_score, \
COALESCE(MAX(CASE WHEN measurement_type = 'OXYGEN_SATURATION' THEN score END), 0) AS oxygen_saturation_score, \
heart_rate_score + temperature_score + oxygen_saturation_score AS news2_score \
FROM scored \
GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)";

fn varchar(nullable: bool) -> PDataType {
    PDataType::VarChar(VarCharType::with_nullable(nullable, VarCharType::MAX_LENGTH).unwrap())
}

fn config(warehouse: &str, name: &str) -> PipelineConfig {
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: {name}
connectors:
  paimon:
    warehouse: {warehouse}
inputs:
  - name: scored
    table: default.scored
    key: patient_id
    watermark:
      column: measurement_timestamp
      lag: 10s
output:
  name: gdnews2_scores
  table: default.gdnews2_scores
  key: patient_id
  bucket: 1
  sequence_field: window_end
deployment:
  partitions: 1
  commit_interval: 200ms
"#
    ))
    .expect("yaml")
}

struct Vital {
    event_id: i64,
    device_id: &'static str,
    measurement_type: &'static str,
    value: f64,
    score: i64,
    at: i64,
}

fn scored_batch(rows: &[Vital]) -> arrow::record_batch::RecordBatch {
    let patient = vec!["P0005"; rows.len()];
    arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Int64, false),
            Field::new("patient_id", DataType::Utf8, false),
            Field::new("device_id", DataType::Utf8, false),
            Field::new("measurement_type", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("score", DataType::Int64, false),
            Field::new("measurement_timestamp", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.event_id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(patient)),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.device_id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.measurement_type)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.value).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.score).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.at).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

async fn write_scored(table: &paimon::table::Table, rows: &[Vital]) {
    let mut sink =
        PaimonSink::from_table(table.clone(), "event_id", 2, Some("measurement_timestamp"))
            .expect("sink");
    sink.write(&scored_batch(rows)).await.expect("write");
    sink.commit().await.expect("commit");
}

fn start(
    cfg: PipelineConfig,
) -> tokio::task::JoinHandle<anyhow::Result<tachyon_runtime::PipelineHandle>> {
    tokio::spawn(async move {
        let pipeline = Pipeline::new(&cfg, SQL)?;
        pipeline.run(&HashMap::new()).await
    })
}

fn died(
    result: Result<Result<tachyon_runtime::PipelineHandle, anyhow::Error>, tokio::task::JoinError>,
) -> String {
    match result {
        Ok(Err(err)) => format!("{err:#}"),
        Ok(Ok(_)) => "terminó sin error".to_string(),
        Err(err) => format!("cancelado: {err}"),
    }
}

#[derive(Debug, PartialEq)]
struct News {
    patient: String,
    start: i64,
    end: i64,
    heart_rate: Option<f64>,
    heart_score: i64,
    temperature_score: i64,
    oxygen_score: i64,
    news2: i64,
}

fn news_rows(batches: &[arrow::record_batch::RecordBatch]) -> Vec<News> {
    let mut rows = Vec::new();
    for batch in batches {
        let patients = batch
            .column_by_name("patient_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let starts = col_i64(batch, "window_start");
        let ends = col_i64(batch, "window_end");
        let rates = batch
            .column_by_name("heart_rate_value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let heart = col_i64(batch, "heart_rate_score");
        let temperature = col_i64(batch, "temperature_score");
        let oxygen = col_i64(batch, "oxygen_saturation_score");
        let news2 = col_i64(batch, "news2_score");
        for row in 0..batch.num_rows() {
            rows.push(News {
                patient: patients.value(row).to_string(),
                start: starts.value(row),
                end: ends.value(row),
                heart_rate: if rates.is_null(row) {
                    None
                } else {
                    Some(rates.value(row))
                },
                heart_score: heart.value(row),
                temperature_score: temperature.value(row),
                oxygen_score: oxygen.value(row),
                news2: news2.value(row),
            });
        }
    }
    rows.sort_by_key(|row| row.start);
    rows
}

fn col_i64<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a Int64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("falta {name}"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{name} no es Int64"))
}

async fn wait_news(
    task: tokio::task::JoinHandle<anyhow::Result<tachyon_runtime::PipelineHandle>>,
    table: &paimon::table::Table,
    n: usize,
) -> (
    tokio::task::JoinHandle<anyhow::Result<tachyon_runtime::PipelineHandle>>,
    Vec<News>,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if task.is_finished() {
            panic!("el pipeline terminó: {}", died(task.await));
        }
        let batches = read_table_rows(table).await.expect("leyendo gdnews2");
        let rows = news_rows(&batches);
        if rows.len() >= n {
            return (task, rows);
        }
        if std::time::Instant::now() > deadline {
            task.abort();
            panic!("esperaba {n} filas y hay {rows:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn snapshot_id(table: &paimon::table::Table) -> Option<i64> {
    table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .expect("snapshot")
}

async fn fresh_pair(dir: &str) -> (paimon::table::Table, paimon::table::Table) {
    let scored = create_test_table(
        dir,
        "default",
        "scored",
        &[
            (
                "event_id",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
            ("patient_id", varchar(false)),
            ("device_id", varchar(false)),
            ("measurement_type", varchar(false)),
            ("value", PDataType::Double(DoubleType::with_nullable(false))),
            ("score", PDataType::BigInt(BigIntType::with_nullable(false))),
            (
                "measurement_timestamp",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
        ],
        &["event_id"],
        2,
        Some("measurement_timestamp"),
    )
    .await
    .expect("scored");
    let scores = create_test_table(
        dir,
        "default",
        "gdnews2_scores",
        &[
            ("patient_id", varchar(false)),
            (
                "window_start",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
            (
                "window_end",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
            (
                "heart_rate_value",
                PDataType::Double(DoubleType::with_nullable(true)),
            ),
            (
                "heart_rate_score",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
            (
                "temperature_score",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
            (
                "oxygen_saturation_score",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
            (
                "news2_score",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
        ],
        &["patient_id", "window_start"],
        1,
        Some("window_end"),
    )
    .await
    .expect("gdnews2");
    (scored, scores)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_devices_fold_into_one_news2_row_and_a_restart_does_not_double_it() {
    let dir = std::env::temp_dir().join(format!(
        "tachyon-news2-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let warehouse = dir.to_string_lossy().to_string();
    let (scored, scores) = fresh_pair(&warehouse).await;
    write_scored(
        &scored,
        &[
            Vital {
                event_id: 1,
                device_id: "ECG_A",
                measurement_type: "HEART_RATE",
                value: 120.0,
                score: 3,
                at: 70_000,
            },
            Vital {
                event_id: 2,
                device_id: "TEMP_B",
                measurement_type: "TEMPERATURE",
                value: 37.5,
                score: 1,
                at: 1_000,
            },
            Vital {
                event_id: 3,
                device_id: "ECG_A",
                measurement_type: "HEART_RATE",
                value: 80.0,
                score: 2,
                at: 5_000,
            },
        ],
    )
    .await;

    let cfg = config(&warehouse, "news2-fold");
    let task = start(cfg.clone());
    let (task, first) = wait_news(task, &scores, 1).await;
    assert_eq!(
        first,
        vec![News {
            patient: "P0005".into(),
            start: 0,
            end: 60_000,
            heart_rate: Some(80.0),
            heart_score: 2,
            temperature_score: 1,
            oxygen_score: 0,
            news2: 3,
        }],
        "dos dispositivos, el HR de los 70s queda en la ventana siguiente"
    );
    let committed = snapshot_id(&scores).await;
    task.abort();
    let _ = task.await;

    let task = start(cfg.clone());
    tokio::time::sleep(Duration::from_millis(800)).await;
    if task.is_finished() {
        panic!("el reinicio terminó: {}", died(task.await));
    }
    assert_eq!(
        snapshot_id(&scores).await,
        committed,
        "el reinicio no volvió a escribir la ventana"
    );
    assert_eq!(news_rows(&read_table_rows(&scores).await.unwrap()), first);

    write_scored(
        &scored,
        &[
            Vital {
                event_id: 4,
                device_id: "SPO2_B",
                measurement_type: "OXYGEN_SATURATION",
                value: 90.0,
                score: 9,
                at: 2_000,
            },
            Vital {
                event_id: 5,
                device_id: "ECG_A",
                measurement_type: "HEART_RATE",
                value: 70.0,
                score: 0,
                at: 130_000,
            },
        ],
    )
    .await;
    let (task, rows) = wait_news(task, &scores, 2).await;
    task.abort();
    let _ = task.await;
    assert_eq!(
        rows,
        vec![
            News {
                patient: "P0005".into(),
                start: 0,
                end: 60_000,
                heart_rate: Some(80.0),
                heart_score: 2,
                temperature_score: 1,
                oxygen_score: 0,
                news2: 3,
            },
            News {
                patient: "P0005".into(),
                start: 60_000,
                end: 120_000,
                heart_rate: Some(120.0),
                heart_score: 3,
                temperature_score: 0,
                oxygen_score: 0,
                news2: 3,
            },
        ],
        "el oxígeno de t=2000 llega tarde y no entra; el HR de los 70s cierra la segunda ventana"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_uncommitted_snapshot_is_reread_and_still_scores_once() {
    let dir = std::env::temp_dir().join(format!(
        "tachyon-news2-open-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let warehouse = dir.to_string_lossy().to_string();
    let (scored, scores) = fresh_pair(&warehouse).await;
    write_scored(
        &scored,
        &[
            Vital {
                event_id: 1,
                device_id: "TEMP_B",
                measurement_type: "TEMPERATURE",
                value: 37.5,
                score: 1,
                at: 1_000,
            },
            Vital {
                event_id: 2,
                device_id: "ECG_A",
                measurement_type: "HEART_RATE",
                value: 80.0,
                score: 2,
                at: 5_000,
            },
        ],
    )
    .await;

    let cfg = config(&warehouse, "news2-open");
    let task = start(cfg.clone());
    tokio::time::sleep(Duration::from_millis(800)).await;
    if task.is_finished() {
        panic!("el primer proceso terminó: {}", died(task.await));
    }
    assert_eq!(snapshot_id(&scores).await, None, "sin cierre no hay commit");
    task.abort();
    let _ = task.await;

    let task = start(cfg);
    write_scored(
        &scored,
        &[Vital {
            event_id: 3,
            device_id: "ECG_A",
            measurement_type: "HEART_RATE",
            value: 120.0,
            score: 3,
            at: 70_000,
        }],
    )
    .await;
    let (task, rows) = wait_news(task, &scores, 1).await;
    task.abort();
    let _ = task.await;
    assert_eq!(
        rows,
        vec![News {
            patient: "P0005".into(),
            start: 0,
            end: 60_000,
            heart_rate: Some(80.0),
            heart_score: 2,
            temperature_score: 1,
            oxygen_score: 0,
            news2: 3,
        }]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

const FILTER_SQL: &str = "INSERT INTO kept \
SELECT event_id, score FROM scored WHERE measurement_type = 'HEART_RATE'";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_filter_writes_paimon_without_a_broker() {
    let dir = std::env::temp_dir().join(format!(
        "tachyon-table-filter-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let warehouse = dir.to_string_lossy().to_string();
    let scored = create_test_table(
        &warehouse,
        "default",
        "scored",
        &[
            (
                "event_id",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
            ("measurement_type", varchar(false)),
            ("score", PDataType::BigInt(BigIntType::with_nullable(false))),
        ],
        &["event_id"],
        2,
        None,
    )
    .await
    .expect("scored");
    let kept = create_test_table(
        &warehouse,
        "default",
        "kept",
        &[
            (
                "event_id",
                PDataType::BigInt(BigIntType::with_nullable(false)),
            ),
            ("score", PDataType::BigInt(BigIntType::with_nullable(false))),
        ],
        &["event_id"],
        1,
        None,
    )
    .await
    .expect("kept");
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Int64, false),
            Field::new("measurement_type", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                "HEART_RATE",
                "TEMPERATURE",
                "HEART_RATE",
            ])),
            Arc::new(Int64Array::from(vec![2, 1, 3])),
        ],
    )
    .unwrap();
    let mut sink = PaimonSink::from_table(scored.clone(), "event_id", 2, None).unwrap();
    sink.write(&batch).await.unwrap();
    sink.commit().await.unwrap();

    let yaml = format!(
        r#"
pipeline:
  name: table-filter
connectors:
  paimon:
    warehouse: {warehouse}
inputs:
  - name: scored
    table: default.scored
    key: event_id
output:
  name: kept
  table: default.kept
  key: event_id
  bucket: 1
deployment:
  partitions: 1
  commit_interval: 200ms
"#
    );
    let cfg: PipelineConfig = serde_yaml::from_str(&yaml).unwrap();
    let task = tokio::spawn(async move {
        let pipeline = Pipeline::new(&cfg, FILTER_SQL)?;
        pipeline.run(&HashMap::new()).await
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let rows = loop {
        if task.is_finished() {
            panic!("el filtro terminó: {}", died(task.await));
        }
        let batches = read_table_rows(&kept).await.unwrap();
        let mut got = Vec::new();
        for batch in &batches {
            let ids = col_i64(batch, "event_id");
            let scores = col_i64(batch, "score");
            for row in 0..batch.num_rows() {
                got.push((ids.value(row), scores.value(row)));
            }
        }
        got.sort();
        if got.len() >= 2 {
            break got;
        }
        if std::time::Instant::now() > deadline {
            task.abort();
            panic!("esperaba el filtro y hay {got:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(rows, vec![(1, 2), (3, 3)]);
    let committed = snapshot_id(&kept).await;
    task.abort();
    let _ = task.await;

    let cfg: PipelineConfig = serde_yaml::from_str(&yaml).unwrap();
    let task = tokio::spawn(async move {
        let pipeline = Pipeline::new(&cfg, FILTER_SQL)?;
        pipeline.run(&HashMap::new()).await
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    if task.is_finished() {
        panic!("el reinicio terminó: {}", died(task.await));
    }
    assert_eq!(snapshot_id(&kept).await, committed);
    task.abort();
    let _ = task.await;
    let _ = std::fs::remove_dir_all(&dir);
}
