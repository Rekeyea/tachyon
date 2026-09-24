//! Slice 4: sink Paimon — escritura real + dedup por primary key/sequence.
//!
//! Valida el riesgo #1 del MVP contra una tabla local (filesystem warehouse;
//! en producción el warehouse vive en S3-compatible, p. ej. rustfs).
//!
//! Escenarios:
//! 1. write + commit: los datos aparecen en la tabla (lectura del snapshot).
//! 2. Actualización por clave: una segunda escritura de la misma PK con
//!    sequence mayor sobrescribe (merge engine deduplicate).
//! 3. Evento late: una escritura con sequence MENOR no sobrescribe la
//!    versión ya comprometida (orden por sequence.field, no por llegada).

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use paimon::spec::{DataType, BigIntType, DoubleType, VarCharType};
use tachyon_sink::writer::{create_test_table, read_table_rows, PaimonSink};

const DB: &str = "default";
const TABLE: &str = "orders_lake";

fn arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        ArrowField::new("order_id", ArrowDataType::Int64, false),
        ArrowField::new("status", ArrowDataType::Utf8, true),
        ArrowField::new("source_version", ArrowDataType::Int64, true),
        ArrowField::new("amount", ArrowDataType::Float64, true),
    ]))
}

fn batch(
    order_ids: Vec<i64>,
    statuses: Vec<&str>,
    versions: Vec<i64>,
    amounts: Vec<f64>,
) -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(Int64Array::from(order_ids)),
            Arc::new(StringArray::from(statuses)),
            Arc::new(Int64Array::from(versions)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .expect("construyendo RecordBatch")
}

/// Devuelve (order_id, amount) de cada fila, ordenado por order_id.
fn rows_sorted(batches: &[RecordBatch]) -> Vec<(i64, f64)> {
    let mut rows: Vec<(i64, f64)> = batches
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
    rows
}

/// Warehouse único por test (el nombre del test lo hace distinto aunque
/// ambos corran en el mismo milisegio): evita pisarse el schema-0.
async fn fresh_warehouse(test_name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "tachyon-sink-test-{test_name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("creando warehouse");
    dir.to_string_lossy().to_string()
}

#[tokio::test]
async fn write_commit_and_dedup_by_sequence() {
    let warehouse = fresh_warehouse("write_commit").await;

    // Tabla: PK order_id, sequence.field source_version, 1 bucket.
    let table = create_test_table(
        &warehouse,
        DB,
        TABLE,
        &[
            (
                "order_id".into(),
                DataType::BigInt(BigIntType::with_nullable(false)),
            ),
            ("status".into(), DataType::VarChar(VarCharType::string_type())),
            ("source_version".into(), DataType::BigInt(BigIntType::new())),
            ("amount".into(), DataType::Double(DoubleType::new())),
        ],
        &["order_id"],
        1,
        Some("source_version"),
    )
    .await
    .expect("creando tabla");

    let mut sink = PaimonSink::from_table(table.clone(), "order_id", 1, Some("source_version"))
        .expect("abriendo sink");

    // --- 1. Primer commit: 3 orders nuevos ---
    sink.write(&batch(
        vec![1, 2, 3],
        vec!["paid", "shipped", "paid"],
        vec![10, 11, 12],
        vec![100.0, 200.0, 300.0],
    ))
    .await
    .expect("write batch 1");
    sink.commit().await.expect("commit 1");

    let rows = rows_sorted(&read_table_rows(&table).await.expect("lectura 1"));
    assert_eq!(
        rows,
        vec![(1, 100.0), (2, 200.0), (3, 300.0)],
        "los 3 orders deben estar en la tabla"
    );

    // --- 2. Actualización: order 1 con sequence mayor + order 4 nuevo ---
    sink.write(&batch(
        vec![1, 4],
        vec!["refunded", "paid"],
        vec![13, 14],
        vec![50.0, 400.0],
    ))
    .await
    .expect("write batch 2");
    sink.commit().await.expect("commit 2");

    let rows = rows_sorted(&read_table_rows(&table).await.expect("lectura 2"));
    assert_eq!(
        rows,
        vec![(1, 50.0), (2, 200.0), (3, 300.0), (4, 400.0)],
        "order 1 actualizado (v13), order 4 agregado"
    );

    // --- 3. Evento late: order 1 con sequence MENOR (v9) no sobrescribe ---
    sink.write(&batch(
        vec![1],
        vec!["cancelled"],
        vec![9],
        vec![0.0],
    ))
    .await
    .expect("write batch late");
    sink.commit().await.expect("commit 3");

    let rows = rows_sorted(&read_table_rows(&table).await.expect("lectura 3"));
    assert_eq!(
        rows,
        vec![(1, 50.0), (2, 200.0), (3, 300.0), (4, 400.0)],
        "el evento late (v9 < v13) debe ignorarse: gana el sequence mayor"
    );

    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test]
async fn rejects_unknown_key_column() {
    let warehouse = fresh_warehouse("rejects_key").await;
    let table = create_test_table(
        &warehouse,
        DB,
        TABLE,
        &[
            (
                "order_id".into(),
                DataType::BigInt(BigIntType::with_nullable(false)),
            ),
            ("amount".into(), DataType::Double(DoubleType::new())),
        ],
        &["order_id"],
        1,
        None,
    )
    .await
    .expect("creando tabla");

    let result = PaimonSink::from_table(table, "no_existe", 1, None);
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("debe rechazar la clave inexistente"),
    };
    assert!(
        err.to_string().contains("no_existe"),
        "debe reportar la clave inválida: {err}"
    );
    let _ = std::fs::remove_dir_all(&warehouse);
}
