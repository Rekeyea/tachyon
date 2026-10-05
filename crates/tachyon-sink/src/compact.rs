//! Compactación de archivos chicos, fuera del commit del pipeline.
//!
//! Paimon 0.3 no tiene un job de compactación: `TableCommit` publica `APPEND`,
//! o `OVERWRITE` si el commit borra archivos. Un `OVERWRITE` detiene la cola.
//! Acá el snapshot se arma a mano y sale con `commitKind = COMPACT`. El scan
//! incremental solo lee `APPEND`, y la cola ya avanza el cursor al verlo.
//!
//! El archivo nuevo se escribe en modo overwrite, así que sus sequence numbers
//! arrancan en cero, por debajo de un epoch que ya haya leído el máximo. Si
//! ese epoch commitea después, sus filas ganan el merge. El `commit_user` es
//! `tachyon-compact` y el identifier es `i64::MAX`, así que la recuperación
//! del pipeline sigue encontrando su propio sidecar.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use paimon::spec::{
    CommitKind, FileKind, Manifest, ManifestEntry, ManifestFileMeta, ManifestList, Snapshot,
};
use paimon::table::{CommitMessage, DataSplit, Table};

use crate::writer::read_table_rows;

const COMPACT_USER: &str = "tachyon-compact";
const ATTEMPTS: u32 = 3;

/// Resultado de una pasada. `snapshot_id` vacío: ningún bucket llegó a `min_files`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactOutcome {
    pub snapshot_id: Option<i64>,
    pub files_before: usize,
    pub files_after: usize,
}

/// Publica un snapshot `COMPACT` cuando algún bucket tiene al menos `min_files`
/// data files. Reintenta si el writer publicó un snapshot en el medio.
pub async fn compact_table(table: &Table, min_files: usize) -> Result<CompactOutcome> {
    if min_files < 2 {
        anyhow::bail!("la compactación arranca con 2 archivos en el bucket");
    }
    if !table.schema().partition_keys().is_empty() {
        anyhow::bail!("la compactación de este corte es para una tabla sin particiones");
    }

    for attempt in 0..ATTEMPTS {
        let Some(latest) = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .context("leyendo el snapshot vigente")?
        else {
            return Ok(CompactOutcome {
                snapshot_id: None,
                files_before: 0,
                files_after: 0,
            });
        };
        let files_before = active_files(table).await?;
        if !bucket_is_ready(&files_before, min_files) {
            return Ok(CompactOutcome {
                snapshot_id: None,
                files_before: files_before.len(),
                files_after: files_before.len(),
            });
        }

        let batches = read_table_rows(table)
            .await
            .context("leyendo las filas vigentes")?;
        let messages = write_merged(table, &batches).await?;
        if !batches.is_empty() && messages.iter().all(|message| message.new_files.is_empty()) {
            anyhow::bail!("la compactación leyó filas y no escribió un archivo");
        }

        let current = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .context("releyendo el snapshot")?;
        if current != Some(latest.id()) {
            tracing::info!(
                attempt,
                snapshot = latest.id(),
                "el writer avanzó; la compactación reintenta"
            );
            continue;
        }

        let files = active_files(table).await?;
        let snapshot_id = publish_compact(table, &latest, &files, &messages).await?;
        if snapshot_id.is_none() {
            tracing::info!(
                attempt,
                snapshot = latest.id(),
                "otro snapshot ocupó el id; la compactación reintenta"
            );
            continue;
        }
        let files_after = active_files(table).await?.len();
        tracing::info!(
            snapshot = snapshot_id.unwrap_or(latest.id()),
            files_before = files_before.len(),
            files_after,
            "snapshot COMPACT publicado"
        );
        return Ok(CompactOutcome {
            snapshot_id,
            files_before: files_before.len(),
            files_after,
        });
    }
    anyhow::bail!("la compactación no pudo publicar: el writer avanzó el snapshot")
}

fn bucket_is_ready(files: &[ActiveFile], min_files: usize) -> bool {
    let mut per_bucket: HashMap<i32, usize> = HashMap::new();
    for file in files {
        *per_bucket.entry(file.bucket).or_default() += 1;
    }
    per_bucket.values().any(|count| *count >= min_files)
}

struct ActiveFile {
    partition: Vec<u8>,
    bucket: i32,
    total_buckets: i32,
    file: paimon::spec::DataFileMeta,
}

async fn active_files(table: &Table) -> Result<Vec<ActiveFile>> {
    let plan = table
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .context("listando los data files")?;
    let mut files = Vec::new();
    for split in plan.splits() {
        files.extend(files_of(split));
    }
    Ok(files)
}

fn files_of(split: &DataSplit) -> Vec<ActiveFile> {
    split
        .data_files()
        .iter()
        .cloned()
        .map(|file| ActiveFile {
            partition: split.partition().to_serialized_bytes(),
            bucket: split.bucket(),
            total_buckets: split.total_buckets(),
            file,
        })
        .collect()
}

async fn write_merged(table: &Table, batches: &[RecordBatch]) -> Result<Vec<CommitMessage>> {
    let mut writer = table
        .new_write_builder()
        .with_commit_user(COMPACT_USER)
        .context("commit_user de la compactación")?
        .new_write()
        .context("abriendo el writer de la compactación")?
        .with_overwrite();
    if !batches.is_empty() {
        writer
            .write_arrow(batches)
            .await
            .map_err(|err| anyhow::anyhow!("escribiendo el archivo compacto: {err}"))?;
    }
    writer
        .prepare_commit()
        .await
        .map_err(|err| anyhow::anyhow!("cerrando el archivo compacto: {err}"))
}

/// `None` si otro commit ganó la carrera por el id.
async fn publish_compact(
    table: &Table,
    latest: &Snapshot,
    files: &[ActiveFile],
    messages: &[CommitMessage],
) -> Result<Option<i64>> {
    let total_buckets = files.first().map(|file| file.total_buckets).unwrap_or(1);
    let mut entries = Vec::new();
    for file in files {
        entries.push(ManifestEntry::new(
            FileKind::Delete,
            file.partition.clone(),
            file.bucket,
            file.total_buckets,
            file.file.clone(),
            2,
        ));
    }
    for message in messages {
        for file in &message.new_files {
            entries.push(ManifestEntry::new(
                FileKind::Add,
                message.partition.clone(),
                message.bucket,
                total_buckets,
                file.clone(),
                2,
            ));
        }
    }

    let manager = table.snapshot_manager();
    let file_io = manager.file_io();
    let manifest_dir = manager.manifest_dir();
    let base_metas = {
        let base_path = format!("{manifest_dir}/{}", latest.base_manifest_list());
        let previous_delta = format!("{manifest_dir}/{}", latest.delta_manifest_list());
        let mut metas = ManifestList::read(file_io, &base_path)
            .await
            .context("leyendo el manifest base")?;
        metas.extend(
            ManifestList::read(file_io, &previous_delta)
                .await
                .context("leyendo el manifest delta")?,
        );
        metas
    };
    // `BinaryTableStats` no es público. La tabla sin particiones ya guardó el
    // prefijo de aridad vacío en el manifest anterior; se reutiliza tal cual.
    let partition_stats = base_metas
        .first()
        .context("la compactación no encontró un manifest previo")?
        .partition_stats()
        .clone();

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("reloj")?
        .as_nanos();
    let delta_name = format!("manifest-compact-{}-{stamp}", std::process::id());
    let delta_path = format!("{manifest_dir}/{delta_name}");
    Manifest::write(file_io, &delta_path, &entries)
        .await
        .context("escribiendo el manifest de la compactación")?;
    let delta_size = file_io
        .get_status(&delta_path)
        .await
        .context("midiendo el manifest")?
        .size as i64;
    let added = messages
        .iter()
        .map(|message| message.new_files.len() as i64)
        .sum();
    let (min_bucket, max_bucket, min_level, max_level) = bucket_level_stats(files, messages);
    let delta_meta = ManifestFileMeta::new(
        delta_name,
        delta_size,
        added,
        files.len() as i64,
        partition_stats,
        table.schema().id(),
    )
    .with_bucket_level_stats(min_bucket, max_bucket, min_level, max_level);

    let list_id = format!("manifest-list-compact-{}-{stamp}", std::process::id());
    let base_list = format!("{list_id}-0");
    let delta_list = format!("{list_id}-1");
    ManifestList::write(file_io, &format!("{manifest_dir}/{base_list}"), &base_metas)
        .await
        .context("escribiendo la lista base")?;
    ManifestList::write(
        file_io,
        &format!("{manifest_dir}/{delta_list}"),
        &[delta_meta],
    )
    .await
    .context("escribiendo la lista delta")?;

    let added: i64 = messages
        .iter()
        .flat_map(|message| &message.new_files)
        .map(|file| file.row_count)
        .sum();
    let removed: i64 = files.iter().map(|file| file.file.row_count).sum();
    let delta_records = added - removed;
    let total_records = latest.total_record_count().unwrap_or(0) + delta_records;
    let snapshot_id = latest.id() + 1;
    let snapshot = Snapshot::builder()
        .version(3)
        .id(snapshot_id)
        .schema_id(table.schema().id())
        .base_manifest_list(base_list)
        .delta_manifest_list(delta_list)
        .commit_user(COMPACT_USER.to_string())
        .commit_identifier(i64::MAX)
        .commit_kind(CommitKind::COMPACT)
        .time_millis(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("reloj")?
                .as_millis() as u64,
        )
        .total_record_count(Some(total_records))
        .delta_record_count(Some(delta_records))
        .index_manifest(latest.index_manifest().map(str::to_string))
        .next_row_id(latest.next_row_id())
        .build();

    let published = manager
        .commit_snapshot(&snapshot)
        .await
        .context("publicando el snapshot COMPACT")?;
    if published {
        Ok(Some(snapshot_id))
    } else {
        Ok(None)
    }
}

fn bucket_level_stats(
    files: &[ActiveFile],
    messages: &[CommitMessage],
) -> (Option<i32>, Option<i32>, Option<i32>, Option<i32>) {
    let mut min_bucket: Option<i32> = None;
    let mut max_bucket: Option<i32> = None;
    let mut min_level: Option<i32> = None;
    let mut max_level: Option<i32> = None;
    let mut note = |bucket: i32, level: i32| {
        min_bucket = Some(min_bucket.map_or(bucket, |current| current.min(bucket)));
        max_bucket = Some(max_bucket.map_or(bucket, |current| current.max(bucket)));
        min_level = Some(min_level.map_or(level, |current| current.min(level)));
        max_level = Some(max_level.map_or(level, |current| current.max(level)));
    };
    for file in files {
        note(file.bucket, file.file.level);
    }
    for message in messages {
        for file in &message.new_files {
            note(message.bucket, file.level);
        }
    }
    (min_bucket, max_bucket, min_level, max_level)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{create_test_table, tail_appends, PaimonSink};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use paimon::spec::{BigIntType, DataType as PDataType, VarCharType};
    use std::sync::Arc;

    fn rows(pairs: &[(i64, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("customer_id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    pairs.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    pairs
                        .iter()
                        .map(|row| Some(row.1.to_string()))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("filas")
    }

    fn names(batches: &[RecordBatch]) -> Vec<(i64, String)> {
        let mut out = Vec::new();
        for batch in batches {
            let ids = batch
                .column_by_name("customer_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let labels = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                out.push((ids.value(row), labels.value(row).to_string()));
            }
        }
        out.sort();
        out
    }

    async fn warehouse() -> (std::path::PathBuf, Table) {
        let dir = std::env::temp_dir().join(format!(
            "tachyon-compact-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("reloj")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("warehouse");
        let table = create_test_table(
            &dir.to_string_lossy(),
            "default",
            "customers",
            &[
                (
                    "customer_id",
                    PDataType::BigInt(BigIntType::with_nullable(false)),
                ),
                ("name", PDataType::VarChar(VarCharType::string_type())),
            ],
            &["customer_id"],
            1,
            None,
        )
        .await
        .expect("tabla");
        (dir, table)
    }

    #[tokio::test]
    async fn one_file_stays_as_it_is() {
        let (dir, table) = warehouse().await;
        let mut sink = PaimonSink::from_table(table.clone(), "customer_id", 1, None).expect("sink");
        sink.write(&rows(&[(1, "ana")])).await.expect("write");
        sink.commit().await.expect("commit");
        let outcome = compact_table(&table, 2).await.expect("compact");
        assert_eq!(outcome.snapshot_id, None);
        assert_eq!(outcome.files_before, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_compact_snapshot_folds_the_bucket_and_a_later_write_wins() {
        let (dir, table) = warehouse().await;
        for batch in [
            rows(&[(1, "ana"), (2, "bea")]),
            rows(&[(3, "caro")]),
            rows(&[(1, "eva")]),
        ] {
            let mut sink =
                PaimonSink::from_table(table.clone(), "customer_id", 1, None).expect("sink");
            sink.write(&batch).await.expect("write");
            sink.commit().await.expect("commit");
        }
        let before = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .expect("id")
            .unwrap();
        let outcome = compact_table(&table, 2).await.expect("compact");
        let snapshot_id = outcome.snapshot_id.expect("publicó");
        assert!(
            outcome.files_before >= 2,
            "archivos antes: {}",
            outcome.files_before
        );
        assert_eq!(outcome.files_after, 1, "el bucket queda en un archivo");

        let snapshot = table
            .snapshot_manager()
            .get_snapshot(snapshot_id)
            .await
            .expect("snapshot");
        assert_eq!(snapshot.commit_kind(), &CommitKind::COMPACT);
        assert_eq!(snapshot.commit_user(), COMPACT_USER);
        assert_eq!(snapshot.commit_identifier(), i64::MAX);
        assert_eq!(
            names(&read_table_rows(&table).await.expect("lectura")),
            vec![
                (1, "eva".to_string()),
                (2, "bea".to_string()),
                (3, "caro".to_string()),
            ]
        );

        let tail = tail_appends(
            &table,
            before,
            &["customer_id".to_string(), "name".to_string()],
        )
        .await
        .expect("cola");
        assert_eq!(tail.through, snapshot_id);
        assert_eq!(tail.blocked_at, None);
        assert!(
            tail.batches.iter().all(|batch| batch.num_rows() == 0),
            "un COMPACT no reinyecta las filas"
        );

        let mut sink = PaimonSink::from_table(table.clone(), "customer_id", 1, None).expect("sink");
        sink.write(&rows(&[(1, "ina"), (4, "dora")]))
            .await
            .expect("write");
        sink.commit().await.expect("commit");
        assert_eq!(
            names(&read_table_rows(&table).await.expect("lectura")),
            vec![
                (1, "ina".to_string()),
                (2, "bea".to_string()),
                (3, "caro".to_string()),
                (4, "dora".to_string()),
            ]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
