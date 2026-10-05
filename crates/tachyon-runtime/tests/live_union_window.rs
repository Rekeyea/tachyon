//! Ventana sobre las seis tablas de score. Cada tipo vive en su tabla Paimon.
//! La fila ancha cierra con el mínimo de los seis relojes. Un vital que no
//! llegó suma 0. El reinicio no la escribe dos veces y un evento tarde no
//! entra en la ventana ya cerrada.
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

const BASE_MS: i64 = 1_700_000_040_000;
const WINDOW_MS: i64 = 60_000;

const TYPES: &[(&str, &str)] = &[
    ("RESPIRATORY_RATE", "scores_respiratory_rate"),
    ("OXYGEN_SATURATION", "scores_oxygen_saturation"),
    ("BLOOD_PRESSURE_SYSTOLIC", "scores_blood_pressure_systolic"),
    ("HEART_RATE", "scores_heart_rate"),
    ("TEMPERATURE", "scores_temperature"),
    ("CONSCIOUSNESS", "scores_consciousness"),
];

const SQL: &str = "INSERT INTO news2_wide \
SELECT patient_id, window_start, window_end, \
MAX(CASE WHEN measurement_type = 'RESPIRATORY_RATE' THEN value END) AS respiratory_rate_value, \
MAX(CASE WHEN measurement_type = 'OXYGEN_SATURATION' THEN value END) AS oxygen_saturation_value, \
MAX(CASE WHEN measurement_type = 'BLOOD_PRESSURE_SYSTOLIC' THEN value END) AS blood_pressure_value, \
MAX(CASE WHEN measurement_type = 'HEART_RATE' THEN value END) AS heart_rate_value, \
MAX(CASE WHEN measurement_type = 'TEMPERATURE' THEN value END) AS temperature_value, \
MAX(CASE WHEN measurement_type = 'CONSCIOUSNESS' THEN value END) AS consciousness_value, \
COALESCE(MAX(CASE WHEN measurement_type = 'RESPIRATORY_RATE' THEN score END), 0) AS respiratory_rate_score, \
COALESCE(MAX(CASE WHEN measurement_type = 'OXYGEN_SATURATION' THEN score END), 0) AS oxygen_saturation_score, \
COALESCE(MAX(CASE WHEN measurement_type = 'BLOOD_PRESSURE_SYSTOLIC' THEN score END), 0) AS blood_pressure_score, \
COALESCE(MAX(CASE WHEN measurement_type = 'HEART_RATE' THEN score END), 0) AS heart_rate_score, \
COALESCE(MAX(CASE WHEN measurement_type = 'TEMPERATURE' THEN score END), 0) AS temperature_score, \
COALESCE(MAX(CASE WHEN measurement_type = 'CONSCIOUSNESS' THEN score END), 0) AS consciousness_score, \
respiratory_rate_score + oxygen_saturation_score + blood_pressure_score + heart_rate_score + temperature_score + consciousness_score AS news2_score \
FROM ( \
SELECT measurement_type, patient_id, value, score, measurement_timestamp FROM scores_respiratory_rate \
UNION ALL \
SELECT measurement_type, patient_id, value, score, measurement_timestamp FROM scores_oxygen_saturation \
UNION ALL \
SELECT measurement_type, patient_id, value, score, measurement_timestamp FROM scores_blood_pressure_systolic \
UNION ALL \
SELECT measurement_type, patient_id, value, score, measurement_timestamp FROM scores_heart_rate \
UNION ALL \
SELECT measurement_type, patient_id, value, score, measurement_timestamp FROM scores_temperature \
UNION ALL \
SELECT measurement_type, patient_id, value, score, measurement_timestamp FROM scores_consciousness \
) \
GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)";

fn varchar(nullable: bool) -> PDataType {
    PDataType::VarChar(VarCharType::with_nullable(nullable, VarCharType::MAX_LENGTH).unwrap())
}

fn config(warehouse: &str) -> PipelineConfig {
    let mut inputs = String::new();
    for (_, table) in TYPES {
        inputs.push_str(&format!(
            r#"
  - name: {table}
    table: delta.{table}
    key: patient_id
    watermark:
      column: measurement_timestamp
      lag: 10s
"#
        ));
    }
    serde_yaml::from_str(&format!(
        r#"
pipeline:
  name: news2-union
connectors:
  paimon:
    warehouse: {warehouse}
inputs:{inputs}
output:
  name: news2_wide
  table: delta.news2_wide
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

struct Score {
    event_id: i64,
    patient: &'static str,
    value: f64,
    score: i64,
    at: i64,
}

fn score_batch(measurement_type: &str, rows: &[Score]) -> arrow::record_batch::RecordBatch {
    let n = rows.len();
    arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Int64, false),
            Field::new("patient_id", DataType::Utf8, false),
            Field::new("measurement_type", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("score", DataType::Int64, false),
            Field::new("measurement_timestamp", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.event_id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.patient).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![measurement_type; n])),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.value).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.score).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(rows.iter().map(|row| row.at).collect::<Vec<_>>())),
        ],
    )
    .expect("batch")
}

async fn write_scores(table: &paimon::table::Table, measurement_type: &str, rows: &[Score]) {
    let mut sink = PaimonSink::from_table(table.clone(), "event_id", 1, Some("event_id"))
        .expect("sink");
    sink.write(&score_batch(measurement_type, rows))
        .await
        .expect("write");
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
struct Wide {
    patient: String,
    start: i64,
    end: i64,
    values: [Option<f64>; 6],
    scores: [i64; 6],
    news2: i64,
}

fn wide_rows(batches: &[arrow::record_batch::RecordBatch]) -> Vec<Wide> {
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
        let values: Vec<&Float64Array> = [
            "respiratory_rate_value",
            "oxygen_saturation_value",
            "blood_pressure_value",
            "heart_rate_value",
            "temperature_value",
            "consciousness_value",
        ]
        .into_iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("falta {name}"))
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap_or_else(|| panic!("{name} no es Float64"))
        })
        .collect();
        let scores: Vec<&Int64Array> = [
            "respiratory_rate_score",
            "oxygen_saturation_score",
            "blood_pressure_score",
            "heart_rate_score",
            "temperature_score",
            "consciousness_score",
        ]
        .into_iter()
        .map(|name| col_i64(batch, name))
        .collect();
        let news2 = col_i64(batch, "news2_score");
        for row in 0..batch.num_rows() {
            let mut got_values = [None; 6];
            let mut got_scores = [0; 6];
            for i in 0..6 {
                got_values[i] = if values[i].is_null(row) {
                    None
                } else {
                    Some(values[i].value(row))
                };
                got_scores[i] = scores[i].value(row);
            }
            rows.push(Wide {
                patient: patients.value(row).to_string(),
                start: starts.value(row),
                end: ends.value(row),
                values: got_values,
                scores: got_scores,
                news2: news2.value(row),
            });
        }
    }
    rows.sort_by(|a, b| (&a.patient, a.start).cmp(&(&b.patient, b.start)));
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

fn expected() -> Vec<Wide> {
    // Misma cuenta que bench/harness/news2.py con 2 pacientes y 2 minutos.
    // p0000 en el primer minuto no tiene oximetría.
    vec![
        wide("p0000", 0, [Some(8.0), None, Some(90.0), Some(40.0), Some(35.0), Some(1.0)], [3, 0, 3, 3, 3, 0]),
        wide("p0000", 1, [Some(10.0), Some(93.0), Some(100.0), Some(45.0), Some(35.5), Some(0.0)], [1, 2, 2, 1, 1, 3]),
        wide("p0001", 0, [Some(22.0), Some(97.0), Some(120.0), Some(100.0), Some(38.5), Some(2.0)], [2, 0, 0, 1, 1, 3]),
        wide("p0001", 1, [Some(26.0), Some(90.0), Some(220.0), Some(120.0), Some(39.25), Some(1.0)], [3, 3, 3, 2, 2, 0]),
    ]
}

fn wide(patient: &str, minute: i64, values: [Option<f64>; 6], scores: [i64; 6]) -> Wide {
    let start = BASE_MS + minute * WINDOW_MS;
    Wide {
        patient: patient.to_string(),
        start,
        end: start + WINDOW_MS,
        values,
        scores,
        news2: scores.iter().sum(),
    }
}

fn fixture(minute_offset: i64) -> Vec<Vec<Score>> {
    // Por tipo, en orden inverso de tiempo: el snapshot trae el minuto 1 antes
    // que el 0. La ventana los guarda igual.
    let rows = [
        // patient, minute, rr, o2, bp, hr, temp, conscious, scores...
        ("p0001", 1_i64, [26.0, 90.0, 220.0, 120.0, 39.25, 1.0], [3_i64, 3, 3, 2, 2, 0]),
        ("p0001", 0, [22.0, 97.0, 120.0, 100.0, 38.5, 2.0], [2, 0, 0, 1, 1, 3]),
        ("p0000", 1, [10.0, 93.0, 100.0, 45.0, 35.5, 0.0], [1, 2, 2, 1, 1, 3]),
        ("p0000", 0, [8.0, 91.0, 90.0, 40.0, 35.0, 1.0], [3, 3, 3, 3, 3, 0]),
    ];
    let mut by_type: Vec<Vec<Score>> = (0..6).map(|_| Vec::new()).collect();
    let mut event_id = 1_i64;
    for (patient, minute, values, scores) in rows {
        for typ in 0..6 {
            if patient == "p0000" && minute == 0 && typ == 1 {
                event_id += 1;
                continue;
            }
            by_type[typ].push(Score {
                event_id,
                patient,
                value: values[typ],
                score: scores[typ],
                at: BASE_MS + (minute + minute_offset) * WINDOW_MS + 1_000,
            });
            event_id += 1;
        }
    }
    by_type
}

fn closers() -> Vec<Score> {
    // Un evento por tipo, en la ventana que queda abierta. Su reloj cierra
    // los dos minutos de la medición.
    let values = [8.0, 91.0, 90.0, 40.0, 35.0, 1.0];
    let scores = [3_i64, 3, 3, 3, 3, 0];
    values
        .into_iter()
        .zip(scores)
        .enumerate()
        .map(|(typ, (value, score))| Score {
            event_id: 10_000 + typ as i64,
            patient: "p0000",
            value,
            score,
            at: BASE_MS + 2 * WINDOW_MS + 10_000,
        })
        .collect()
}

fn score_fields() -> Vec<(&'static str, PDataType)> {
    vec![
        ("event_id", PDataType::BigInt(BigIntType::with_nullable(false))),
        ("patient_id", varchar(false)),
        ("measurement_type", varchar(false)),
        ("value", PDataType::Double(DoubleType::with_nullable(false))),
        ("score", PDataType::BigInt(BigIntType::with_nullable(false))),
        (
            "measurement_timestamp",
            PDataType::BigInt(BigIntType::with_nullable(false)),
        ),
    ]
}

fn wide_fields() -> Vec<(&'static str, PDataType)> {
    let mut fields = vec![
        ("patient_id", varchar(false)),
        ("window_start", PDataType::BigInt(BigIntType::with_nullable(false))),
        ("window_end", PDataType::BigInt(BigIntType::with_nullable(false))),
    ];
    for name in [
        "respiratory_rate_value",
        "oxygen_saturation_value",
        "blood_pressure_value",
        "heart_rate_value",
        "temperature_value",
        "consciousness_value",
    ] {
        fields.push((name, PDataType::Double(DoubleType::with_nullable(true))));
    }
    for name in [
        "respiratory_rate_score",
        "oxygen_saturation_score",
        "blood_pressure_score",
        "heart_rate_score",
        "temperature_score",
        "consciousness_score",
        "news2_score",
    ] {
        fields.push((name, PDataType::BigInt(BigIntType::with_nullable(false))));
    }
    fields
}

async fn fresh(dir: &str) -> (Vec<paimon::table::Table>, paimon::table::Table) {
    let mut scores = Vec::new();
    let fields = score_fields();
    for (_, name) in TYPES {
        scores.push(
            create_test_table(dir, "delta", name, &fields, &["event_id"], 1, Some("event_id"))
                .await
                .unwrap_or_else(|err| panic!("creando {name}: {err:#}")),
        );
    }
    let wide = wide_fields();
    let news = create_test_table(
        dir,
        "delta",
        "news2_wide",
        &wide,
        &["patient_id", "window_start"],
        1,
        Some("window_end"),
    )
    .await
    .expect("news2_wide");
    (scores, news)
}

async fn snapshot_id(table: &paimon::table::Table) -> Option<i64> {
    table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .expect("snapshot")
}

async fn wait_wide(
    task: tokio::task::JoinHandle<anyhow::Result<tachyon_runtime::PipelineHandle>>,
    table: &paimon::table::Table,
    n: usize,
) -> (
    tokio::task::JoinHandle<anyhow::Result<tachyon_runtime::PipelineHandle>>,
    Vec<Wide>,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if task.is_finished() {
            panic!("el pipeline terminó: {}", died(task.await));
        }
        let batches = read_table_rows(table).await.expect("leyendo news2_wide");
        let rows = wide_rows(&batches);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn six_score_tables_fold_into_news2_and_a_restart_does_not_double_it() {
    let dir = std::env::temp_dir().join(format!(
        "tachyon-union-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let warehouse = dir.to_string_lossy().to_string();
    let (tables, wide) = fresh(&warehouse).await;
    let fixture = fixture(0);
    for (index, rows) in fixture.iter().enumerate() {
        write_scores(&tables[index], TYPES[index].0, rows).await;
    }

    let cfg = config(&warehouse);
    let task = start(cfg.clone());
    let hold_until = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < hold_until {
        if task.is_finished() {
            panic!("la unión terminó antes de cerrar: {}", died(task.await));
        }
        let rows = wide_rows(&read_table_rows(&wide).await.unwrap());
        assert!(
            rows.is_empty(),
            "sin el evento de cierre el watermark no pasa el fin de la ventana: {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for (index, closer) in closers().into_iter().enumerate() {
        write_scores(&tables[index], TYPES[index].0, &[closer]).await;
    }
    let (task, rows) = wait_wide(task, &wide, 4).await;
    assert_eq!(
        rows, expected(),
        "fila ancha: vital ausente en 0, news2 es la suma, el cierre no entra"
    );
    let committed = snapshot_id(&wide).await;
    task.abort();
    let _ = task.await;

    let task = start(cfg.clone());
    tokio::time::sleep(Duration::from_millis(800)).await;
    if task.is_finished() {
        panic!("el reinicio terminó: {}", died(task.await));
    }
    assert_eq!(
        snapshot_id(&wide).await,
        committed,
        "el reinicio no volvió a escribir la ventana"
    );
    assert_eq!(wide_rows(&read_table_rows(&wide).await.unwrap()), expected());

    // Oxígeno del primer minuto de p0000, después del cierre. Llega tarde.
    write_scores(
        &tables[1],
        "OXYGEN_SATURATION",
        &[Score {
            event_id: 99,
            patient: "p0000",
            value: 97.0,
            score: 0,
            at: BASE_MS + 1_000,
        }],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    if task.is_finished() {
        panic!("el evento tarde terminó el pipeline: {}", died(task.await));
    }
    assert_eq!(
        snapshot_id(&wide).await,
        committed,
        "un evento tarde no abre otro snapshot"
    );
    assert_eq!(wide_rows(&read_table_rows(&wide).await.unwrap()), expected());
    task.abort();
    let _ = task.await;
    let _ = std::fs::remove_dir_all(&dir);
}
