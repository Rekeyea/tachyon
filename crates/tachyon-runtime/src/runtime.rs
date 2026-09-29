//! El loop de ejecución del pipeline.
//!
//! MVP: compilación del plan + validación. El loop real (DataFusion + tokio)
//! llega en los Slices 2-5 (ver MVP.md §4).

use anyhow::{Context, Result};
use std::sync::Arc;
use tachyon_config::{validate_config, PipelineConfig};
use tachyon_core::{OutputDef, PartitionKey, PipelinePlan, StreamDef};
use tachyon_metrics::InstanceMetrics;
use tachyon_sink::writer::PaimonSink;
use tachyon_sql::{orient_lookup, parse_sql, LookupJoin, LookupShape, WindowShape};

use crate::join::run_join_pipeline;
use crate::run::{
    run_pipeline_with_lookup, run_topic_pipeline_with_lookup, PipelineHandle, PreparedInput,
    RunOptions,
};
use crate::table_stream::run_table_stream;

/// Un pipeline de Tachyon (una instancia).
#[derive(Debug)]
pub struct Pipeline {
    plan: PipelinePlan,
    config: PipelineConfig,
    select_sql: String,
    /// `Some` hasta que el operador de ventanas esté en el loop: el arranque
    /// se rechaza en vez de mandar `TUMBLE` a DataFusion.
    window: Option<WindowShape>,
    join: Option<tachyon_sql::IntervalJoin>,
    lookup: Option<LookupJoin>,
}

impl Pipeline {
    /// Compila el plan desde la config + SQL y valida:
    /// - el invariante de alineación (config).
    /// - la sintaxis y el binding lógico de la SQL.
    pub fn new(config: &PipelineConfig, sql: &str) -> Result<Self> {
        validate_config(config)?;
        let parsed = parse_sql(sql)?;
        let lookup = bind_lookup(config, parsed.lookup.as_ref())?;

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
                topic: config.output.topic.clone(),
                bucket: config.output.bucket,
                sequence_field: config.output.sequence_field.clone(),
            },
            partition_key: PartitionKey {
                column: config.output.key.clone(),
            },
            partitions: config.deployment.partitions,
            query: parsed.query,
            sql_target: parsed.target,
            sql_source_tables: match &lookup {
                Some(join) => vec![join.fact.clone()],
                None => parsed.source_tables,
            },
        };

        plan.validate_alignment()?;
        plan.validate_sql_binding()?;
        Ok(Pipeline {
            plan,
            config: config.clone(),
            select_sql: parsed.select_sql.clone(),
            window: parsed.window,
            join: parsed.join,
            lookup,
        })
    }

    /// Corre el pipeline end-to-end (Slice 5): consume Redpanda, transforma con
    /// DataFusion y escribe a Paimon, con commits periódicos + métricas.
    ///
    /// `input_codecs`: schema Arrow y formato del topic de cada input
    /// (nombre lógico -> codec). En tests se arman con `PreparedInput::json`.
    ///
    /// No retorna mientras el pipeline esté activo (el stream es infinito).
    pub async fn run(
        &self,
        input_codecs: &std::collections::HashMap<String, PreparedInput>,
    ) -> Result<PipelineHandle> {
        if self
            .config
            .inputs
            .iter()
            .any(|input| input.paimon_table().is_some())
        {
            let topic = self
                .config
                .output
                .topic
                .as_deref()
                .context("leer una tabla publica un topic")?;
            let options = RunOptions::from_config(&self.config);
            let metrics = Arc::new(InstanceMetrics::new());
            return run_table_stream(
                &self.config,
                &self.select_sql,
                &options,
                topic,
                &self.config.output.key,
                &metrics,
            )
            .await;
        }
        if let Some(topic) = &self.config.output.topic {
            if self.window.is_some() || self.join.is_some() {
                anyhow::bail!(
                    "una ventana o un join escriben en Paimon: el topic no guarda el estado abierto"
                );
            }
            let options = RunOptions::from_config(&self.config);
            let metrics = Arc::new(InstanceMetrics::new());
            return run_topic_pipeline_with_lookup(
                &self.config,
                &self.select_sql,
                &options,
                topic,
                &self.config.output.key,
                input_codecs,
                &metrics,
                self.lookup.as_ref(),
            )
            .await;
        }

        // Abrir el sink Paimon desde el warehouse de la config.
        let table_id = self
            .config
            .output
            .table
            .as_deref()
            .context("la salida no tiene tabla")?;
        let (db, table) = split_table_identifier(table_id);
        let warehouse = self
            .config
            .connectors
            .paimon
            .as_ref()
            .context("la salida a tabla requiere connectors.paimon")?
            .warehouse
            .as_str();
        let bucket = self
            .config
            .output
            .bucket
            .context("la salida a tabla requiere output.bucket")?;
        let sink = PaimonSink::open(
            warehouse,
            &db,
            &table,
            &self.config.output.key,
            bucket as i32,
            self.config.output.sequence_field.as_deref(),
        )
        .await
        .with_context(|| format!("abriendo sink Paimon para {table_id}"))?
        .align_rowkind(self.config.output.rowkind_field.as_deref())
        .context("rowkind de la tabla")?;

        let options = RunOptions::from_config(&self.config);
        let metrics = Arc::new(InstanceMetrics::new());
        if let Some(join) = &self.join {
            return run_join_pipeline(
                &self.config,
                &options,
                sink,
                join,
                input_codecs,
                &metrics,
            )
            .await;
        }
        run_pipeline_with_lookup(
            &self.config,
            &self.select_sql,
            &options,
            sink,
            input_codecs,
            &metrics,
            self.window.as_ref(),
            self.lookup.as_ref(),
        )
        .await
    }
}

/// Orienta el JOIN contra el YAML. La dimensión no es un input.
fn bind_lookup(
    config: &PipelineConfig,
    shape: Option<&LookupShape>,
) -> Result<Option<LookupJoin>> {
    let dimension_names: Vec<&str> = config
        .dimensions
        .iter()
        .map(|dimension| dimension.name.as_str())
        .collect();
    let input_names: Vec<&str> = config.inputs.iter().map(|input| input.name.as_str()).collect();
    if let Some(shape) = shape {
        let join = orient_lookup(shape, &dimension_names, &input_names)?;
        if config.inputs.len() != 1 {
            anyhow::bail!("el lookup enriquece un solo stream");
        }
        let Some(dimension) = config.dimensions.first() else {
            anyhow::bail!("el lookup une una dimensión");
        };
        if join.dimension != dimension.name {
            anyhow::bail!("la dimensión '{}' no está en el JOIN", dimension.name);
        }
        if dimension.key != join.dimension_key {
            anyhow::bail!(
                "la clave de '{}' es '{}' en el YAML y '{}' en el JOIN",
                dimension.name,
                dimension.key,
                join.dimension_key
            );
        }
        return Ok(Some(join));
    }
    if let Some(dimension) = config.dimensions.first() {
        anyhow::bail!("la dimensión '{}' no está en el JOIN", dimension.name);
    }
    Ok(None)
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

    fn lookup_yaml() -> String {
        r#"
pipeline:
  name: orders-lookup
connectors:
  redpanda:
    brokers: [localhost:9092]
  paimon:
    warehouse: ./warehouse
inputs:
  - name: orders
    topic: orders-topic
    key: order_id
    schema: orders.json
dimensions:
  - name: customers
    table: default.customers
    key: customer_id
output:
  name: orders_out
  topic: orders-out
  key: order_id
deployment:
  partitions: 1
  resources:
    memory: 256Mi
"#
        .to_string()
    }

    const LOOKUP_SQL: &str = "INSERT INTO orders_out \
        SELECT o.order_id, c.name AS customer_name \
        FROM orders o \
        LEFT JOIN customers c ON o.customer_id = c.customer_id";

    #[test]
    fn a_lookup_binds_only_the_fact() {
        let pipeline = Pipeline::new(&load(&lookup_yaml()), LOOKUP_SQL).expect("lookup");
        assert_eq!(pipeline.plan.sql_source_tables, vec!["orders"]);
        let lookup = pipeline.lookup.expect("lookup");
        assert_eq!(lookup.fact, "orders");
        assert_eq!(lookup.dimension, "customers");
        assert_eq!(lookup.dimension_key, "customer_id");
        assert_eq!(lookup.kind, tachyon_sql::LookupKind::Left);
    }

    #[test]
    fn a_dimension_key_must_match_the_join() {
        let yaml = lookup_yaml().replace("key: customer_id", "key: other_id");
        let err = Pipeline::new(&load(&yaml), LOOKUP_SQL).unwrap_err();
        assert!(err.to_string().contains("JOIN"), "{err}");
    }

    #[test]
    fn a_dimension_without_a_join_does_not_start() {
        let err = Pipeline::new(
            &load(&lookup_yaml()),
            "INSERT INTO orders_out SELECT order_id FROM orders",
        )
        .unwrap_err();
        assert!(err.to_string().contains("no está en el JOIN"), "{err}");
    }

    #[test]
    fn a_lookup_enriches_one_stream() {
        let yaml = lookup_yaml().replace(
            "    schema: orders.json\n",
            "    schema: orders.json\n  - name: payments\n    topic: payments\n    key: order_id\n    schema: payments.json\n",
        );
        let err = Pipeline::new(&load(&yaml), LOOKUP_SQL).unwrap_err();
        assert!(err.to_string().contains("un solo stream"), "{err}");
    }
}
