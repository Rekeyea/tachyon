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

use std::collections::HashMap;
use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use paimon::catalog::Identifier;
use paimon::{CatalogFactory, Options};

/// Sink de Paimon para la tabla de salida de un pipeline.
///
/// Mantiene el writer y el committer (mismo `WriteBuilder`: comparten
/// commit user, requisito de Paimon para commits válidos).
pub struct PaimonSink {
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
        let writer = builder.new_write().context("new_write")?;
        let committer = builder.new_commit();

        Ok(PaimonSink {
            writer,
            committer,
            key_column: key_column.to_string(),
            bucket,
            sequence_field: sequence_field.map(|s| s.to_string()),
        })
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
