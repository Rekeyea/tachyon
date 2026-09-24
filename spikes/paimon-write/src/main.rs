//! Slice 0.5 spike: escribir a una tabla Paimon local con `paimon` (paimon-rs).
//!
//! Objetivo: validar que `paimon` soporta ESCRITURA en Rust puro, en el caso
//! exacto que Tachyon necesita: tabla con primary key + bucket fijo, flujo
//! write-then-commit (two-phase), y verificación de que los archivos de datos
//! aparecen en el warehouse.
//!
//! Esto de-riska el riesgo #1 del MVP (ver MVP.md §6): si este spike corre,
//! el sink de Paimon del MVP es viable.
//!
//! La API usada está verificada contra la fuente de `paimon` 0.3.0
//! (catalog/mod.rs, spec/schema.rs, table/write_builder.rs, table/table_write.rs,
//! table/table_commit.rs) y sus tests.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use paimon::catalog::Identifier;
use paimon::{CatalogFactory, Options};
use paimon::spec::{DataType, BigIntType, DoubleType, VarCharType};

const WAREHOUSE: &str = "/tmp/tachyon-paimon-spike-warehouse";
const DB: &str = "default";
const TABLE: &str = "orders_lake";

#[tokio::main]
async fn main() -> Result<()> {
    // Warehouse limpio para el spike.
    let _ = std::fs::remove_dir_all(WAREHOUSE);
    std::fs::create_dir_all(WAREHOUSE).context("creando warehouse")?;

    // 1. Catalog de filesystem local.
    let options = Options::from_map(
        [("warehouse".to_string(), WAREHOUSE.to_string())]
            .into_iter()
            .collect(),
    );
    let catalog = CatalogFactory::create(options).await.context("creando catalog")?;

    // 1b. El FileSystemCatalog no crea la DB por defecto: se crea explícitamente.
    catalog
        .create_database(DB, true, std::collections::HashMap::new())
        .await
        .context("creando database")?;

    // 2. Crear la tabla (PK + bucket fijo, el caso de Tachyon).
    //    El schema Paimon (spec::Schema) es distinto del Arrow: se construye
    //    con SchemaBuilder y tipos Paimon, no Arrow.
    let schema = paimon::spec::Schema::builder()
        .column("order_id", DataType::BigInt(BigIntType::with_nullable(false)))
        .column("status", DataType::VarChar(VarCharType::string_type()))
        .column("source_version", DataType::BigInt(BigIntType::new()))
        .column("amount", DataType::Double(DoubleType::new()))
        .primary_key(["order_id"])
        .option("bucket", "1")
        .build()
        .context("construyendo schema Paimon")?;

    let identifier = Identifier::new(DB, TABLE);
    catalog
        .create_table(&identifier, schema, false)
        .await
        .context("creando tabla")?;

    let table = catalog.get_table(&identifier).await.context("get_table")?;

    // 3. Write builder -> writer + committer (mismo builder: comparten commit user).
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().context("new_write")?;
    let committer = builder.new_commit();

    // 4. Escribir un RecordBatch. El schema Arrow debe coincidir (orden y tipos)
    //    con el schema Paimon: BigInt->Int64, VarChar->Utf8, Double->Float64.
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("order_id", ArrowDataType::Int64, false),
        ArrowField::new("status", ArrowDataType::Utf8, true),
        ArrowField::new("source_version", ArrowDataType::Int64, true),
        ArrowField::new("amount", ArrowDataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["paid", "shipped", "paid"])),
            Arc::new(Int64Array::from(vec![10, 11, 12])),
            Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0])),
        ],
    )
    .context("construyendo RecordBatch")?;

    writer.write_arrow_batch(&batch).await.context("write_arrow_batch")?;

    // 5. prepare_commit -> commit (two-phase).
    let messages = writer.prepare_commit().await.context("prepare_commit")?;
    committer.commit(messages).await.context("commit")?;

    // 6. Verificación: los archivos de datos/snapshot deben existir en el warehouse.
    //    Paimon layout: <warehouse>/<db>.db/<table>/{snapshot,bucket-0,manifest,schema}
    let table_dir = std::path::Path::new(WAREHOUSE)
        .join(format!("{DB}.db"))
        .join(TABLE);
    let snapshot_path = table_dir.join("snapshot");
    let data_path = table_dir.join("bucket-0");
    let snapshot_files = if snapshot_path.is_dir() {
        std::fs::read_dir(&snapshot_path)
            .map(|rd| rd.count())
            .unwrap_or(0)
    } else {
        0
    };
    let data_files = if data_path.is_dir() {
        std::fs::read_dir(&data_path)
            .map(|rd| rd.count())
            .unwrap_or(0)
    } else {
        0
    };
    println!("OK: escritura + commit a Paimon completados en {WAREHOUSE}");
    println!("    snapshot/: {snapshot_files} archivo(s) (snapshot-1 + LATEST)");
    println!("    bucket-0/: {data_files} archivo(s) .parquet");
    assert!(
        snapshot_files >= 2,
        "snapshot/ no se creo: el commit no persistio nada"
    );
    assert!(data_files >= 1, "bucket-0/ no se creo: no hay archivos de datos");
    println!("PASS: el sink de Paimon en Rust puro es viable (riesgo #1 resuelto)");
    Ok(())
}
