//! El loop de ejecución del pipeline.
//!
//! MVP: compilación del plan + validación. El loop real (DataFusion + tokio)
//! llega en los Slices 2-5 (ver MVP.md §4).

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use tachyon_config::{validate_config, PipelineConfig};
use tachyon_core::{OutputDef, PartitionKey, PipelinePlan, StreamDef};
use tachyon_sink::writer::PaimonSink;
use tachyon_sql::parse_sql;

use crate::run::{run_pipeline, PipelineHandle, RunOptions};

/// Un pipeline de Tachyon (una instancia).
#[derive(Debug)]
pub struct Pipeline {
    plan: PipelinePlan,
    config: PipelineConfig,
    select_sql: String,
}

impl Pipeline {
    /// Compila el plan desde la config + SQL y valida:
    /// - el invariante de alineación (config).
    /// - la sintaxis y el binding lógico de la SQL.
    pub fn new(config: &PipelineConfig, sql: &str) -> Result<Self> {
        validate_config(config)?;
        let parsed = parse_sql(sql)?;

        // Los schemas de columnas de los inputs se pueblan en el Slice 2, al
        // cargar los schemas Avro. Aquí solo el nombre lógico + la clave.
        let inputs: Vec<StreamDef> = config
            .inputs
            .iter()
            .map(|i| StreamDef {
                name: i.name.clone(),
                columns: vec![],
            })
            .collect();

        let plan = PipelinePlan {
            pipeline_name: config.pipeline.name.clone(),
            inputs,
            output: OutputDef {
                name: config.output.name.clone(),
                table: config.output.table.clone(),
                bucket: config.output.bucket,
                sequence_field: config.output.sequence_field.clone(),
            },
            partition_key: PartitionKey {
                column: config.output.key.clone(),
            },
            partitions: config.deployment.partitions,
            query: parsed.query,
            sql_target: parsed.target,
            sql_source_tables: parsed.source_tables,
        };

        plan.validate_alignment()?;
        plan.validate_sql_binding()?;
        Ok(Pipeline {
            plan,
            config: config.clone(),
            select_sql: parsed.select_sql.clone(),
        })
    }

    /// Corre el pipeline end-to-end (Slice 5): consume Redpanda, transforma con
    /// DataFusion y escribe a Paimon, con commits periódicos + métricas.
    ///
    /// `input_schemas`: el schema Arrow de cada input (nombre lógico -> schema).
    /// En producción vienen del schema Avro/JSON del topic; en tests se
    /// proporcionan explícitamente.
    ///
    /// No retorna mientras el pipeline esté activo (el stream es infinito).
    pub async fn run(&self, input_schemas: &std::collections::HashMap<String, SchemaRef>) -> Result<PipelineHandle> {
        // Abrir el sink Paimon desde el warehouse de la config.
        let (db, table) = split_table_identifier(&self.config.output.table);
        let mut sink = PaimonSink::open(
            &self.config.connectors.paimon.warehouse,
            &db,
            &table,
            &self.config.output.key,
            self.config.output.bucket as i32,
            self.config.output.sequence_field.as_deref(),
        )
        .await
        .with_context(|| format!("abriendo sink Paimon para {}", self.config.output.table))?;

        let options = RunOptions::from_config(&self.config);
        run_pipeline(&self.config, &self.select_sql, &options, &mut sink, input_schemas)
            .await
    }
}

/// Divide un identificador `db.table` en (db, table).
fn split_table_identifier(id: &str) -> (String, String) {
    match id.split_once('.') {
        Some((db, table)) => (db.to_string(), table.to_string()),
        None => ("default".to_string(), id.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_config::PipelineConfig;

    fn valid_config_yaml() -> String {
        r#"
pipeline:
  name: orders-etl
  version: 1
connectors:
  redpanda:
    brokers: [localhost:9092]
  paimon:
    warehouse: ./warehouse
    catalog: local
inputs:
  - name: orders
    topic: orders-topic
    key: order_id
    schema: avro/orders-v1
output:
  name: orders_lake
  table: default.orders_lake
  key: order_id
  bucket: 4
  sequence_field: source_version
deployment:
  partitions: 4
"#
        .to_string()
    }

    const VALID_SQL: &str = "INSERT INTO orders_lake \
        SELECT order_id, SUM(amount) AS total \
        FROM orders WHERE status <> 'cancelled' GROUP BY order_id";

    fn load(yaml: &str) -> PipelineConfig {
        serde_yaml::from_str(yaml).expect("config de test debe ser válida")
    }

    #[test]
    fn valid_config_and_sql_produce_plan() {
        let cfg = load(&valid_config_yaml());
        let pipeline = Pipeline::new(&cfg, VALID_SQL).expect("pipeline válido");
        assert_eq!(pipeline.plan.pipeline_name, "orders-etl");
        assert_eq!(pipeline.plan.partitions, 4);
        assert_eq!(pipeline.plan.sql_target, "orders_lake");
        assert_eq!(pipeline.plan.sql_source_tables, vec!["orders"]);
    }

    #[test]
    fn broken_alignment_is_rejected() {
        let yaml = valid_config_yaml().replace("partitions: 4", "partitions: 8");
        let cfg = load(&yaml);
        let err = Pipeline::new(&cfg, VALID_SQL).unwrap_err();
        assert!(
            err.to_string().contains("alineación"),
            "error inesperado: {err}"
        );
    }

    #[test]
    fn misaligned_input_key_is_rejected() {
        let yaml = valid_config_yaml().replace("key: order_id\n    schema", "key: other_id\n    schema");
        let cfg = load(&yaml);
        let err = Pipeline::new(&cfg, VALID_SQL).unwrap_err();
        assert!(
            err.to_string().contains("no alineadas"),
            "error inesperado: {err}"
        );
    }

    #[test]
    fn wrong_sql_target_is_rejected() {
        let cfg = load(&valid_config_yaml());
        let sql = "INSERT INTO wrong_name SELECT order_id FROM orders";
        let err = Pipeline::new(&cfg, sql).unwrap_err();
        assert!(
            err.to_string().contains("target"),
            "error inesperado: {err}"
        );
    }

    #[test]
    fn unknown_source_table_is_rejected() {
        let cfg = load(&valid_config_yaml());
        let sql = "INSERT INTO orders_lake SELECT order_id FROM nonexistent";
        let err = Pipeline::new(&cfg, sql).unwrap_err();
        assert!(
            err.to_string().contains("no está definida en inputs"),
            "error inesperado: {err}"
        );
    }

    #[test]
    fn invalid_sql_syntax_is_rejected() {
        let cfg = load(&valid_config_yaml());
        let err = Pipeline::new(&cfg, "THIS IS NOT SQL").unwrap_err();
        assert!(
            err.to_string().contains("sintaxis"),
            "error inesperado: {err}"
        );
    }
}
