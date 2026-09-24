//! Binario de Tachyon: carga `pipeline.sql` + `pipeline.yaml` y corre el pipeline.
//!
//! El schema Arrow de cada input se carga desde un archivo JSON (una línea por
//! campo: `{"name": "order_id", "type": "Int64", "nullable": false}`). El campo
//! `schema` de la config es la ruta al archivo (o `dir/name.json` si es solo el
//! nombre del archivo, se busca en `--schemas-dir`).

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Field, Schema};
use clap::Parser;
use serde::Deserialize;
use std::sync::Arc;
use tachyon_config::PipelineConfig;
use tachyon_runtime::Pipeline;

/// Tachyon: motor de ejecución de pipelines streaming sobre lakehouse.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Ruta al archivo de configuración (pipeline.yaml).
    #[arg(long, default_value = "pipeline.yaml")]
    config: PathBuf,

    /// Ruta al archivo SQL (pipeline.sql).
    #[arg(long, default_value = "pipeline.sql")]
    sql: PathBuf,

    /// Directorio donde buscar los schemas JSON de los inputs (si el campo
    /// `schema` de la config es solo un nombre de archivo).
    #[arg(long)]
    schemas_dir: Option<PathBuf>,
}

/// Un campo del schema Arrow (formato JSON simplificado).
#[derive(Debug, Deserialize)]
struct FieldDef {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    #[serde(default = "default_true")]
    nullable: bool,
}

fn default_true() -> bool {
    true
}

/// Convierte un nombre de tipo a `DataType` Arrow.
fn parse_type(s: &str) -> Result<DataType> {
    Ok(match s {
        "Int64" => DataType::Int64,
        "Int32" => DataType::Int32,
        "Float64" => DataType::Float64,
        "Float32" => DataType::Float32,
        "Utf8" | "String" => DataType::Utf8,
        "Boolean" => DataType::Boolean,
        other => anyhow::bail!("tipo Arrow no soportado: {other}"),
    })
}

/// Carga el schema Arrow de un input desde un archivo JSON.
fn load_schema(path: &PathBuf) -> Result<Arc<Schema>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("leyendo schema {}", path.display()))?;
    let fields: Vec<FieldDef> =
        serde_json::from_str(&content).with_context(|| "parseando schema JSON".to_string())?;
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            Ok(Field::new(
                &f.name,
                parse_type(&f.data_type)?,
                f.nullable,
            ))
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(Schema::new(arrow_fields)))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let config_str = std::fs::read_to_string(&args.config)
        .with_context(|| format!("leyendo config {}", args.config.display()))?;
    let config: PipelineConfig =
        serde_yaml::from_str(&config_str).with_context(|| "parseando pipeline.yaml".to_string())?;

    let sql_str = std::fs::read_to_string(&args.sql)
        .with_context(|| format!("leyendo SQL {}", args.sql.display()))?;

    // Cargar los schemas de los inputs.
    let mut input_schemas: HashMap<String, Arc<Schema>> = HashMap::new();
    for input in &config.inputs {
        let path = if input.schema.contains('/') || input.schema.ends_with(".json") {
            PathBuf::from(&input.schema)
        } else if let Some(dir) = &args.schemas_dir {
            dir.join(&input.schema)
        } else {
            anyhow::bail!(
                "el schema '{}' no es una ruta y no se especificó --schemas-dir",
                input.schema
            );
        };
        let schema = load_schema(&path)?;
        input_schemas.insert(input.name.clone(), schema);
    }

    let pipeline = Pipeline::new(&config, &sql_str)?;
    let handle = pipeline.run(&input_schemas).await?;

    if let Some(addr) = handle.metrics_addr {
        tracing::info!(%addr, "métricas disponibles en http://{addr}/metrics");
    }
    tracing::info!(
        pipeline = %config.pipeline.name,
        "pipeline corriendo (Ctrl+C para detener)"
    );

    // Esperar señal de shutdown (SIGINT/SIGTERM).
    tokio::signal::ctrl_c().await.context("esperando señal de shutdown")?;
    tracing::info!("shutdown solicitado");

    Ok(())
}
