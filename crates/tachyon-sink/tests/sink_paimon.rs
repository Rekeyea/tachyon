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
use tachyon_core::CheckpointBody;
use tachyon_sink::writer::{create_test_table, read_table_rows, PaimonSink, Recovered};

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

fn offsets(entries: &[(i32, i64)]) -> tachyon_core::SourceOffsets {
    tachyon_core::SourceOffsets::from([(
        "orders".to_string(),
        entries.iter().copied().collect(),
    )])
}

/// Exactly-once: `commit_checkpoint` persiste los offsets atómicamente con el
/// snapshot y `recover` los devuelve tras un reinicio (mismo `commit_user`).
/// Un archivo de offsets sin snapshot (crash entre ambos pasos) no cuenta, y
/// otro `commit_user` no ve los checkpoints ajenos.
#[tokio::test]
async fn checkpoint_offsets_survive_restart() {
    let warehouse = fresh_warehouse("checkpoint").await;
    let table = create_test_table(
        &warehouse,
        DB,
        TABLE,
        &[
            ("order_id", DataType::BigInt(BigIntType::with_nullable(false))),
            ("status", DataType::VarChar(VarCharType::string_type())),
            ("source_version", DataType::BigInt(BigIntType::new())),
            ("amount", DataType::Double(DoubleType::new())),
        ],
        &["order_id"],
        1,
        Some("source_version"),
    )
    .await
    .expect("creando tabla");
    let open = |user: &str| {
        PaimonSink::from_table(table.clone(), "order_id", 1, Some("source_version"))
            .expect("abriendo sink")
            .with_commit_user(user)
            .expect("commit_user")
    };

    // --- Instancia fresca: sin checkpoint ---
    let mut sink = open("tachyon-eos");
    assert_eq!(sink.recover().await.expect("recover"), Recovered::None);

    // Sin datos escritos no hay checkpoint (los offsets no avanzan).
    assert_eq!(
        sink.commit_checkpoint(&CheckpointBody::Offsets(offsets(&[(0, 3)])))
            .await
            .expect("vacío"),
        None
    );

    // --- Dos checkpoints ---
    sink.write(&batch(vec![1, 2], vec!["paid", "paid"], vec![1, 2], vec![1.0, 2.0]))
        .await
        .expect("write 1");
    assert_eq!(
        sink.commit_checkpoint(&CheckpointBody::Offsets(offsets(&[(0, 2)])))
            .await
            .expect("ckpt 1"),
        Some(1)
    );
    sink.write(&batch(vec![3], vec!["paid"], vec![3], vec![3.0]))
        .await
        .expect("write 2");
    assert_eq!(
        sink.commit_checkpoint(&CheckpointBody::Offsets(offsets(&[(0, 2), (1, 1)])))
            .await
            .expect("ckpt 2"),
        Some(2)
    );

    // --- Crash entre offsets y snapshot: archivo del checkpoint 3 huérfano ---
    let orphan = std::path::Path::new(&warehouse)
        .join(format!("{DB}.db"))
        .join(TABLE)
        .join("tachyon-offsets/tachyon-eos/3.json");
    std::fs::write(&orphan, r#"{"orders":{"0":99}}"#).expect("offsets huérfanos");

    // --- Reinicio: recupera el checkpoint 2 y continúa en el 3 ---
    let mut sink = open("tachyon-eos");
    assert_eq!(
        sink.recover().await.expect("recover"),
        Recovered::PassThrough {
            identifier: 2,
            offsets: offsets(&[(0, 2), (1, 1)]),
        },
        "debe recuperar los offsets del último snapshot commiteado, no el huérfano"
    );
    sink.write(&batch(vec![4], vec!["paid"], vec![4], vec![4.0]))
        .await
        .expect("write 3");
    assert_eq!(
        sink.commit_checkpoint(&CheckpointBody::Offsets(offsets(&[(0, 3), (1, 1)])))
            .await
            .expect("ckpt 3"),
        Some(3),
        "el identifier sigue siendo monótono tras el reinicio"
    );
    let mut sink = open("tachyon-eos");
    assert_eq!(
        sink.recover().await.expect("recover"),
        Recovered::PassThrough {
            identifier: 3,
            offsets: offsets(&[(0, 3), (1, 1)]),
        }
    );

    // --- Otro commit_user no ve estos checkpoints ---
    let mut other = open("tachyon-eos-otra-instancia");
    assert_eq!(other.recover().await.expect("recover otro"), Recovered::None);

    let rows = rows_sorted(&read_table_rows(&table).await.expect("lectura"));
    assert_eq!(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    let _ = std::fs::remove_dir_all(&warehouse);
}

/// Identifiers de snapshot de un `commit_user`: `(snapshot_id, commit_identifier)`.
async fn committed_identifiers(table: &paimon::table::Table, commit_user: &str) -> Vec<(i64, i64)> {
    let snapshots = table.snapshot_manager();
    let Ok(Some(latest)) = snapshots.get_latest_snapshot_id().await else {
        return Vec::new();
    };
    let earliest = snapshots
        .earliest_snapshot_id()
        .await
        .ok()
        .flatten()
        .unwrap_or(latest);
    let mut out = Vec::new();
    for id in earliest..=latest {
        let Ok(snapshot) = snapshots.get_snapshot(id).await else {
            continue;
        };
        if snapshot.commit_user() == commit_user && snapshot.commit_identifier() != i64::MAX {
            out.push((id, snapshot.commit_identifier()));
        }
    }
    out
}

/// paimon 0.3: un epoch sin archivos no deja snapshot.
///
/// `commit_with_identifier([])` vuelve `Ok` y no llama a `try_commit`.
/// `write_arrow_batch` de 0 filas tampoco abre un writer, así que
/// `prepare_commit` sigue vacío. Un commit con una fila sí crea snapshot:
/// el negativo no es un falso negativo del catalog.
#[tokio::test]
async fn empty_commit_does_not_create_a_snapshot() {
    let warehouse = fresh_warehouse("empty_commit").await;
    let table = create_test_table(
        &warehouse,
        DB,
        "empty_commit",
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
    .expect("tabla");

    let user = "tachyon-empty";

    // --- 1. prepare sin escrituras + commit de mensajes vacíos ---
    let builder = table
        .new_write_builder()
        .with_commit_user(user)
        .expect("commit_user");
    let mut writer = builder.new_write().expect("writer");
    let committer = builder.new_commit();
    let messages = writer.prepare_commit().await.expect("prepare vacío");
    assert!(
        messages.is_empty(),
        "sin filas, prepare_commit no debe producir CommitMessage: {messages:?}"
    );
    committer
        .commit_with_identifier(messages, 1)
        .await
        .expect("commit vacío debe ser Ok");
    assert!(
        committed_identifiers(&table, user).await.is_empty(),
        "commit_with_identifier([]) no crea snapshot"
    );

    // --- 2. batch de 0 filas, el fallback del diseño ---
    let builder = table
        .new_write_builder()
        .with_commit_user(user)
        .expect("commit_user");
    let mut writer = builder.new_write().expect("writer");
    let committer = builder.new_commit();
    let empty = RecordBatch::new_empty(arrow_schema());
    writer
        .write_arrow_batch(&empty)
        .await
        .expect("write de 0 filas");
    let messages = writer.prepare_commit().await.expect("prepare de 0 filas");
    assert!(
        messages.is_empty(),
        "un RecordBatch de 0 filas tampoco produce CommitMessage: {messages:?}"
    );
    committer
        .commit_with_identifier(messages, 2)
        .await
        .expect("commit de 0 filas debe ser Ok");
    assert!(
        committed_identifiers(&table, user).await.is_empty(),
        "el batch vacío tampoco crea snapshot"
    );

    // --- 3. control: una fila sí deja snapshot con ese identifier ---
    let builder = table
        .new_write_builder()
        .with_commit_user(user)
        .expect("commit_user");
    let mut writer = builder.new_write().expect("writer");
    let committer = builder.new_commit();
    writer
        .write_arrow_batch(&batch(vec![1], vec!["paid"], vec![1], vec![10.0]))
        .await
        .expect("write de 1 fila");
    let messages = writer.prepare_commit().await.expect("prepare con fila");
    assert!(!messages.is_empty(), "una fila debe producir CommitMessage");
    committer
        .commit_with_identifier(messages, 7)
        .await
        .expect("commit con fila");
    let ids = committed_identifiers(&table, user).await;
    assert!(
        ids.iter().any(|(_, ident)| *ident == 7),
        "el commit con datos debe verse; snapshots={ids:?}"
    );
    assert!(
        ids.iter().all(|(_, ident)| *ident != 1 && *ident != 2),
        "los commits vacíos no deben aparecer; snapshots={ids:?}"
    );

    let _ = std::fs::remove_dir_all(&warehouse);
}

/// Un sidecar v1 se restaura con el mismo identifier del snapshot, y el JSON
/// no es un mapa de offsets pelado.
#[tokio::test]
async fn window_checkpoint_roundtrips_with_the_snapshot() {
    use std::collections::BTreeMap;
    use tachyon_core::{
        Accumulators, AggKind, AggSpec, AggState, KeyState, OperatorState, WindowCheckpointV1,
        WindowKind, WindowSpecId,
    };

    let warehouse = fresh_warehouse("window_ckpt").await;
    let table = create_test_table(
        &warehouse,
        DB,
        "window_ckpt",
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
    .expect("tabla");

    let mut sink = PaimonSink::from_table(table.clone(), "order_id", 1, Some("source_version"))
        .expect("sink")
        .with_commit_user("tachyon-window")
        .expect("commit_user");
    sink.write(&batch(vec![1], vec!["paid"], vec![1], vec![10.0]))
        .await
        .expect("write");

    let mut keys = BTreeMap::new();
    keys.insert(
        7i64.to_le_bytes().to_vec(),
        KeyState {
            windows: BTreeMap::from([(
                0,
                Accumulators {
                    slots: vec![AggState::Count(1)],
                    present: vec![true],
                },
            )]),
            sessions: vec![],
        },
    );
    let checkpoint = WindowCheckpointV1 {
        v: 1,
        commit_identifier: 0,
        consumers_per_topic: 1,
        applied: offsets(&[(0, 4)]),
        progress: BTreeMap::new(),
        instance_watermark_ms: Some(1_000),
        spec: WindowSpecId {
            input: "orders".into(),
            event_time: "event_time".into(),
            kind: WindowKind::Tumble,
            size_ms: 60_000,
            slide_ms: None,
            gap_ms: None,
            group_columns: vec!["order_id".into()],
            partial: false,
            aggs: vec![AggSpec {
                kind: AggKind::Count,
                input: None,
                alias: "n".into(),
            }],
        },
        state: OperatorState { keys },
    };
    assert_eq!(
        sink.commit_checkpoint(&CheckpointBody::Window(checkpoint.clone()))
            .await
            .expect("commit ventana"),
        Some(1)
    );

    let path = std::path::Path::new(&warehouse)
        .join(format!("{DB}.db"))
        .join("window_ckpt")
        .join("tachyon-offsets/tachyon-window/1.json");
    let text = std::fs::read_to_string(&path).expect("sidecar");
    assert!(text.contains("\"v\":1"), "{text}");

    let mut sink = PaimonSink::from_table(table, "order_id", 1, Some("source_version"))
        .expect("sink")
        .with_commit_user("tachyon-window")
        .expect("commit_user");
    match sink.recover().await.expect("recover") {
        Recovered::Window {
            identifier,
            checkpoint: got,
        } => {
            assert_eq!(identifier, 1);
            assert_eq!(got.commit_identifier, 1);
            assert_eq!(got.applied, checkpoint.applied);
            assert_eq!(got.state, checkpoint.state);
            assert_eq!(got.spec, checkpoint.spec);
        }
        other => panic!("se esperaba ventana, llegó {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&warehouse);
}
