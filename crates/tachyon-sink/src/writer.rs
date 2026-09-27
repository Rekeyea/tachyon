//! Writer de Paimon (Slice 4).
//!
//! Modelo de concurrencia de Paimon: **un writer por bucket**
//! (DESIGN.md §7.1). Como Tachyon no es distribuido, cada instancia posee
//! todos los buckets de su tabla; el writer interno de Paimon enruta cada
//! fila a su bucket por hash de la primary key. La restricción de un writer
//! por bucket se cumple porque solo existe un `TableWrite` por (tabla,
//! instancia): si una segunda instancia escribiera al mismo bucket, se
//! detectarían conflictos de snapshot en el commit.
//!
//! Sequence numbers: el writer de Paimon los extrae de la columna
//! `sequence.field` del batch (ver `table_write.rs`: `sequence_field_indices`
//! + scan lazy de máximos por bucket). Los números de secuencia llegan ya
//! ordenados por la partición de Redpanda (offset monótono por clave); si la
//! fuente tiene un campo de versión, se declara como `sequence.field` y los
//! eventos late/out-of-order se resuelven por ese número, no por llegada.
//!
//! Commit: two-phase (`prepare_commit` → `commit`). En el MVP el commit es
//! explícito (llamado por el runtime tras cada tramo de stream); en la Fase 1
//! se integra con el checkpoint (barrier → prepare, completado → commit).
//!
//! Exactly-once (`commit_checkpoint` / `recover`): el snapshot de Paimon es el
//! **único punto de commit**. Cada checkpoint `N` persiste primero los offsets
//! de fuente que cubre en `<tabla>/tachyon-offsets/<commit_user>/N.json` y luego
//! commitea en Paimon con `commit_identifier = N` bajo un `commit_user` estable.
//! El checkpoint `N` existe si y solo si hay un snapshot de ese `commit_user`
//! con identifier `N`: un archivo de offsets sin snapshot (crash entre ambos
//! pasos) se ignora y se sobrescribe en el siguiente intento. Al recuperar se
//! lee el último identifier commiteado y sus offsets, y el consumo se
//! re-posiciona exactamente ahí: sin pérdida ni duplicados.

use std::collections::HashMap;
use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use paimon::catalog::Identifier;
use paimon::{CatalogFactory, Options};
use tachyon_core::SourceOffsets;

/// Reintentos de un commit de Paimon con resultado incierto (error de I/O
/// tras el prepare). El reintento filtra identifiers ya commiteados, así que
/// es idempotente.
const COMMIT_RETRIES: usize = 3;

/// Sink de Paimon para la tabla de salida de un pipeline.
///
/// Mantiene el writer y el committer (mismo `WriteBuilder`: comparten
/// commit user, requisito de Paimon para commits válidos).
pub struct PaimonSink {
    table: paimon::table::Table,
    writer: paimon::table::TableWrite,
    committer: paimon::table::TableCommit,
    /// Columna clave de particionado (== state key == bucket key, §3.3).
    /// Se usa para validación de alineación; el enrutado por bucket lo hace
    /// el writer interno de Paimon.
    key_column: String,
    /// Número de buckets de la tabla (== deployment.partitions, §3.3).
    bucket: i32,
    /// Columna de sequence number, si la fuente la aporta.
    sequence_field: Option<String>,
    /// Identidad del writer en los snapshots de Paimon. Debe ser estable entre
    /// reinicios para que `recover` encuentre los checkpoints propios.
    commit_user: String,
    /// Identifier del próximo checkpoint (monótono por `commit_user`).
    next_identifier: i64,
}

impl PaimonSink {
    /// Abre el sink sobre una tabla existente.
    ///
    /// `warehouse`: ruta del warehouse (local en el MVP; S3-compatible vía
    /// rustfs en producción, ver DESIGN.md §7). `database`/`table`:
    /// identificadores Paimon. `key_column`/`bucket`/`sequence_field`:
    /// valores del config (`output.key`, `output.bucket`,
    /// `output.sequence.field`) — se validan contra el schema de la tabla.
    pub async fn open(
        warehouse: &str,
        database: &str,
        table: &str,
        key_column: &str,
        bucket: i32,
        sequence_field: Option<&str>,
    ) -> Result<Self> {
        let options = Options::from_map(
            [(String::from("warehouse"), warehouse.to_string())]
                .into_iter()
                .collect(),
        );
        let catalog = CatalogFactory::create(options)
            .await
            .context("creando catalog Paimon")?;
        let identifier = Identifier::new(database, table);
        let table = catalog
            .get_table(&identifier)
            .await
            .context("abriendo tabla Paimon")?;

        Self::from_table(table, key_column, bucket, sequence_field)
    }

    /// Variante que recibe la tabla ya abierta (usada por tests y por el
    /// runtime cuando el catalog es compartido).
    pub fn from_table(
        table: paimon::table::Table,
        key_column: &str,
        bucket: i32,
        sequence_field: Option<&str>,
    ) -> Result<Self> {
        // Validación de alineación (§3.3): la clave debe existir en el schema.
        let fields: Vec<String> = table
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        if !fields.iter().any(|f| f == key_column) {
            anyhow::bail!(
                "la clave de particionado '{key_column}' no existe en el schema de la tabla: {fields:?}"
            );
        }
        if let Some(seq) = sequence_field {
            if !fields.iter().any(|f| f == seq) {
                anyhow::bail!(
                    "la columna de sequence '{seq}' no existe en el schema de la tabla: {fields:?}"
                );
            }
        }

        let builder = table.new_write_builder();
        let commit_user = builder.commit_user().to_string();
        let writer = builder.new_write().context("new_write")?;
        let committer = builder.new_commit();

        Ok(PaimonSink {
            table,
            writer,
            committer,
            key_column: key_column.to_string(),
            bucket,
            sequence_field: sequence_field.map(|s| s.to_string()),
            commit_user,
            next_identifier: 1,
        })
    }

    /// Fija un `commit_user` estable (requisito del exactly-once: sin él, cada
    /// arranque tiene un UUID nuevo y no puede encontrar sus checkpoints).
    ///
    /// Re-crea el writer y el committer, así que debe llamarse antes de
    /// cualquier `write`.
    pub fn with_commit_user(mut self, commit_user: &str) -> Result<Self> {
        let builder = self
            .table
            .new_write_builder()
            .with_commit_user(commit_user)
            .with_context(|| format!("commit_user inválido: '{commit_user}'"))?;
        self.writer = builder.new_write().context("new_write")?;
        self.committer = builder.new_commit();
        self.commit_user = commit_user.to_string();
        Ok(self)
    }

    /// Recupera el último checkpoint commiteado por este `commit_user`.
    ///
    /// Devuelve los offsets de fuente de ese checkpoint (o `None` si nunca
    /// commiteó) y deja el sink listo para commitear el siguiente identifier.
    pub async fn recover(&mut self) -> Result<Option<SourceOffsets>> {
        let Some(identifier) = last_committed_identifier(&self.table, &self.commit_user).await?
        else {
            self.next_identifier = 1;
            return Ok(None);
        };
        let path = offsets_path(&self.table, &self.commit_user, identifier);
        let bytes = self
            .table
            .file_io()
            .new_input(&path)
            .context("abriendo offsets del checkpoint")?
            .read()
            .await
            .with_context(|| {
                format!("el checkpoint {identifier} está commiteado pero falta {path}")
            })?;
        let offsets: SourceOffsets = serde_json::from_slice(&bytes)
            .with_context(|| format!("offsets del checkpoint corruptos: {path}"))?;
        self.next_identifier = identifier + 1;
        Ok(Some(offsets))
    }

    /// Commitea todo lo escrito desde el último checkpoint junto con los
    /// offsets de fuente que cubre (exactly-once).
    ///
    /// Devuelve el identifier commiteado, o `None` si no había nada que
    /// commitear (sin archivos nuevos: los offsets no avanzan y los registros
    /// correspondientes se re-procesan al recuperar, sin producir salida).
    pub async fn commit_checkpoint(&mut self, offsets: &SourceOffsets) -> Result<Option<i64>> {
        let messages = self.writer.prepare_commit().await.context("prepare_commit")?;
        if messages.is_empty() {
            return Ok(None);
        }
        let identifier = self.next_identifier;

        // 1. Offsets primero: solo cuentan si el snapshot `identifier` existe.
        write_offsets(&self.table, &offsets_path(&self.table, &self.commit_user, identifier), offsets)
            .await?;

        // 2. Snapshot de Paimon: el punto de commit atómico.
        commit_with_retries(&self.committer, messages, identifier).await?;
        self.next_identifier = identifier + 1;
        Ok(Some(identifier))
    }

    /// Divide el sink en la mitad escritora (hot path) y la mitad commitera
    /// (background) para el commit desacoplado del runtime: el writer task
    /// rota el `TableWrite` en cada checkpoint (instantáneo) y sigue
    /// escribiendo; el flush+commit del epoch cerrado corre en el commit task.
    pub fn split(self) -> (PaimonWriterHalf, PaimonCommitterHalf) {
        (
            PaimonWriterHalf {
                table: self.table.clone(),
                commit_user: self.commit_user.clone(),
                active: self.writer,
                next_identifier: self.next_identifier,
            },
            PaimonCommitterHalf {
                table: self.table,
                commit_user: self.commit_user,
                committer: self.committer,
            },
        )
    }

    /// Escribe un `RecordBatch` de salida.
    ///
    /// El schema Arrow debe coincidir (orden y tipos) con el schema Paimon:
    /// BigInt→Int64, VarChar→Utf8, Double→Float64, etc. (ver SPIKE paimon-write).
    /// Si está declarado `sequence.field`, el batch debe incluir esa columna.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer
            .write_arrow_batch(batch)
            .await
            .context("write_arrow_batch")
    }

    /// Two-phase commit: prepara y aplica el commit de todo lo escrito desde
    /// el último commit. Devuelve los mensajes de commit preparados (útiles
    /// para el checkpoint de la Fase 1: prepare en la barrier, commit al
    /// completar).
    pub async fn commit(&mut self) -> Result<()> {
        let messages = self.writer.prepare_commit().await.context("prepare_commit")?;
        self.committer
            .commit(messages)
            .await
            .context("commit Paimon")?;
        Ok(())
    }

    /// Solo la fase prepare (Fase 1: barrier de checkpoint).
    pub async fn prepare(&mut self) -> Result<Vec<paimon::table::CommitMessage>> {
        self.writer
            .prepare_commit()
            .await
            .context("prepare_commit")
    }

    /// Solo la fase commit (Fase 1: completado del checkpoint).
    pub async fn finish_commit(&mut self, messages: Vec<paimon::table::CommitMessage>) -> Result<()> {
        self.committer
            .commit(messages)
            .await
            .context("commit Paimon")?;
        Ok(())
    }

    pub fn key_column(&self) -> &str {
        &self.key_column
    }

    pub fn bucket(&self) -> i32 {
        self.bucket
    }

    pub fn sequence_field(&self) -> Option<&str> {
        self.sequence_field.as_deref()
    }

    pub fn commit_user(&self) -> &str {
        &self.commit_user
    }
}

/// Commit de Paimon con reintentos: un error de I/O tras el prepare deja el
/// resultado incierto, así que el reintento filtra identifiers ya commiteados
/// (idempotente).
async fn commit_with_retries(
    committer: &paimon::table::TableCommit,
    messages: Vec<paimon::table::CommitMessage>,
    identifier: i64,
) -> Result<()> {
    let mut attempt = 0;
    loop {
        let result = if attempt == 0 {
            committer
                .commit_with_identifier(messages.clone(), identifier)
                .await
        } else {
            committer
                .filter_and_commit_with_identifier(messages.clone(), identifier)
                .await
        };
        match result {
            Ok(()) => return Ok(()),
            Err(e) if attempt + 1 < COMMIT_RETRIES => {
                tracing::warn!(error = %e, identifier, attempt, "commit Paimon incierto, reintentando");
                attempt += 1;
            }
            Err(e) => {
                return Err(anyhow::Error::new(e)
                    .context(format!("commit Paimon del checkpoint {identifier}")))
            }
        }
    }
}

/// Mitad escritora del sink (ver `PaimonSink::split`). Vive en el writer task
/// del runtime: escribe batches y rota el `TableWrite` en cada checkpoint.
///
/// La rotación es instantánea (swap por un writer fresco); el writer lleno se
/// va al commit task, que hace el flush+commit en background mientras acá se
/// sigue escribiendo el epoch siguiente. Esto es seguro porque los nombres de
/// archivo son UUID por writer (sin colisiones) y los commits se serializan
/// (un solo commit task, identifiers monótonos).
pub struct PaimonWriterHalf {
    table: paimon::table::Table,
    commit_user: String,
    active: paimon::table::TableWrite,
    /// Identifier del próximo checkpoint (monótono por `commit_user`).
    next_identifier: i64,
}

impl PaimonWriterHalf {
    /// Escribe un `RecordBatch` (mismas reglas de schema que `PaimonSink::write`).
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.active
            .write_arrow_batch(batch)
            .await
            .context("write_arrow_batch")
    }

    /// Cierra el epoch actual: devuelve `(identifier, writer lleno)` y deja
    /// un writer fresco listo para seguir escribiendo. O(1).
    pub fn rotate(&mut self) -> Result<(i64, paimon::table::TableWrite)> {
        let fresh = self
            .table
            .new_write_builder()
            .with_commit_user(&self.commit_user)
            .context("commit_user inválido")?
            .new_write()
            .context("new_write")?;
        let identifier = self.next_identifier;
        self.next_identifier += 1;
        Ok((identifier, std::mem::replace(&mut self.active, fresh)))
    }
}

/// Mitad commitera (ver `PaimonSink::split`). Vive en un task dedicado y
/// procesa los epochs EN ORDEN: prepare (flush de los buffers, lo lento),
/// archivo de offsets y snapshot de Paimon con reintentos.
pub struct PaimonCommitterHalf {
    table: paimon::table::Table,
    commit_user: String,
    committer: paimon::table::TableCommit,
}

impl PaimonCommitterHalf {
    /// Commitea un epoch completo: `prepare_commit` del writer rotado (flush,
    /// lo caro, corre acá para no bloquear al writer), luego offsets y
    /// snapshot atómico. Devuelve el identifier commiteado, o `None` si el
    /// epoch no produjo archivos nuevos.
    pub async fn commit_epoch(
        &mut self,
        mut writer: paimon::table::TableWrite,
        identifier: i64,
        offsets: &SourceOffsets,
    ) -> Result<Option<i64>> {
        let debug = std::env::var("TACHYON_DEBUG_COMMIT").is_ok();
        let t_prepare = std::time::Instant::now();
        let messages = writer.prepare_commit().await.context("prepare_commit")?;
        let d_prepare = t_prepare.elapsed();
        if messages.is_empty() {
            return Ok(None);
        }

        // 1. Offsets primero: solo cuentan si el snapshot `identifier` existe.
        let t_offsets = std::time::Instant::now();
        write_offsets(&self.table, &offsets_path(&self.table, &self.commit_user, identifier), offsets)
            .await?;
        let d_offsets = t_offsets.elapsed();

        // 2. Snapshot de Paimon: el punto de commit atómico.
        let t_commit = std::time::Instant::now();
        commit_with_retries(&self.committer, messages, identifier).await?;
        if debug {
            eprintln!(
                "[commit] id={identifier} prepare={:.0}ms offsets={:.0}ms commit={:.0}ms",
                d_prepare.as_secs_f64() * 1e3,
                d_offsets.as_secs_f64() * 1e3,
                t_commit.elapsed().as_secs_f64() * 1e3
            );
        }
        Ok(Some(identifier))
    }
}

/// Un epoch cerrado pendiente de commit: lo que el writer task entrega al
/// commit task en cada checkpoint (commit desacoplado, ver `PaimonSink::split`).
pub struct CheckpointEpoch {
    pub writer: paimon::table::TableWrite,
    pub identifier: i64,
    pub offsets: SourceOffsets,
}

/// Último identifier commiteado por este `commit_user` (recorre los
/// snapshots del más nuevo al más viejo). Ignora los commits batch
/// (`i64::MAX`), que no son checkpoints.
async fn last_committed_identifier(table: &paimon::table::Table, commit_user: &str) -> Result<Option<i64>> {
    let snapshots = table.snapshot_manager();
    let Some(latest) = snapshots
        .get_latest_snapshot_id()
        .await
        .context("leyendo el último snapshot")?
    else {
        return Ok(None);
    };
    let earliest = snapshots
        .earliest_snapshot_id()
        .await
        .context("leyendo el snapshot más viejo")?
        .unwrap_or(latest);
    for id in (earliest..=latest).rev() {
        let snapshot = snapshots
            .get_snapshot(id)
            .await
            .with_context(|| format!("leyendo snapshot {id}"))?;
        if snapshot.commit_user() == commit_user
            && snapshot.commit_identifier() != i64::MAX
        {
            return Ok(Some(snapshot.commit_identifier()));
        }
    }
    Ok(None)
}

fn offsets_path(table: &paimon::table::Table, commit_user: &str, identifier: i64) -> String {
    format!(
        "{}/tachyon-offsets/{commit_user}/{identifier}.json",
        table.location().trim_end_matches('/'),
    )
}

/// Escribe los offsets de un checkpoint (tmp + rename; si el storage no
/// soporta rename, escritura directa). Sobrescribe un archivo previo del
/// mismo identifier (intento anterior que no llegó a commitear).
async fn write_offsets(table: &paimon::table::Table, path: &str, offsets: &SourceOffsets) -> Result<()> {
    let file_io = table.file_io();
    let json = bytes::Bytes::from(serde_json::to_vec(offsets).context("serializando offsets")?);
    if let Some(dir) = path.rsplit_once('/').map(|(d, _)| d) {
        let _ = file_io.mkdirs(dir).await;
    }
    let tmp = format!("{path}.tmp");
    file_io
        .new_output(&tmp)
        .context("creando offsets tmp")?
        .write(json.clone())
        .await
        .context("escribiendo offsets tmp")?;
    if file_io.rename(&tmp, path).await.is_err() {
        let _ = file_io.delete_file(&tmp).await;
        file_io
            .new_output(path)
            .context("creando offsets")?
            .write(json)
            .await
            .context("escribiendo offsets")?;
    }
    Ok(())
}

/// Utilidad para tests/CLI: crea database + tabla con PK, bucket y
/// sequence.field a partir de una descripción de campos.
///
/// `fields`: (nombre, tipo Paimon) en el orden exacto que tendrán los
/// RecordBatch.
#[allow(dead_code)]
pub async fn create_test_table(
    warehouse: &str,
    database: &str,
    table: &str,
    fields: &[(&str, paimon::spec::DataType)],
    primary_key: &[&str],
    bucket: i32,
    sequence_field: Option<&str>,
) -> Result<paimon::table::Table> {
    let options = Options::from_map(
        [(String::from("warehouse"), warehouse.to_string())]
            .into_iter()
            .collect(),
    );
    let catalog = CatalogFactory::create(options)
        .await
        .context("creando catalog")?;
    catalog
        .create_database(database, true, HashMap::new())
        .await
        .context("creando database")?;

    let mut builder = paimon::spec::Schema::builder();
    for (name, dt) in fields {
        builder = builder.column(*name, dt.clone());
    }
    builder = builder.primary_key(primary_key.iter().copied());
    if let Some(seq) = sequence_field {
        builder = builder.option("sequence.field", seq);
    }
    let schema = builder
        .option("bucket", &bucket.to_string())
        // Compresión de los data files, overridable con
        // `TACHYON_TEST_FILE_COMPRESSION` (p. ej. "none", "lz4", "zstd").
        // Se deja en "none" en tests para medir el costo de CPU del pipeline
        // sin el encode de compresión; la compresión real es decisión del DDL
        // de la tabla en producción.
        .option(
            "file.compression",
            &std::env::var("TACHYON_TEST_FILE_COMPRESSION").unwrap_or_else(|_| "none".to_string()),
        )
        .build()
        .context("construyendo schema Paimon")?;

    let identifier = Identifier::new(database, table);
    catalog
        .create_table(&identifier, schema, false)
        .await
        .context("creando tabla")?;
    catalog
        .get_table(&identifier)
        .await
        .context("get_table")
}

/// Lee el contenido actual de una tabla Paimon (último snapshot).
/// Utilidad para tests y verificación post-commit.
pub async fn read_table_rows(
    table: &paimon::table::Table,
) -> Result<Vec<RecordBatch>> {
    let read_builder = table.new_read_builder();
    let scan = read_builder.new_scan();
    let plan = scan.plan().await.context("scan.plan")?;
    let read = read_builder.new_read().context("new_read")?;
    let stream = read
        .to_arrow(&plan.splits())
        .context("to_arrow")?;
    use futures::TryStreamExt;
    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .context("leyendo batches")?;
    Ok(batches)
}
