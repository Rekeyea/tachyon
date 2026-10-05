//! Lookup de una dimensión chica.
//!
//! El probe corre antes de DataFusion. El SQL que se planifica ya es un
//! SELECT de una sola tabla: el batch del stream llega con las columnas
//! pegadas. El offset del hecho avanza cuando la fuente emite, así que un
//! INNER que descarta la fila deja el offset listo para el próximo commit.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use tachyon_config::{parse_memory_bytes, PipelineConfig};
use tachyon_sink::dimension::DimensionCache;
use tachyon_source::stream::RedpandaPartitionStream;
use tachyon_sql::{rewrite_lookup, LookupJoin, LookupKind};

/// Dimensión cargada y el SELECT de una tabla que ve DataFusion.
pub(crate) struct PreparedLookup {
    pub sql: String,
    pub fact: String,
    pub fact_schema: SchemaRef,
    pub enriched_schema: SchemaRef,
    pub cache: Arc<tokio::sync::Mutex<DimensionCache>>,
    pub fact_key: String,
    pub keep_misses: bool,
    pub outputs: Vec<(String, String)>,
}

/// Abre la dimensión y arma el schema enriquecido. Si no entra en memoria, no arranca.
pub(crate) async fn prepare_lookup(
    config: &PipelineConfig,
    lookup: &LookupJoin,
    select_sql: &str,
    fact_schema: SchemaRef,
) -> Result<PreparedLookup> {
    let dim = config
        .dimensions
        .first()
        .context("el lookup une una dimensión")?;
    if dim.key != lookup.dimension_key {
        anyhow::bail!(
            "la clave de '{}' es '{}' en el YAML y '{}' en el JOIN",
            dim.name,
            dim.key,
            lookup.dimension_key
        );
    }
    let memory = config
        .deployment
        .resources
        .as_ref()
        .and_then(|resources| resources.memory.as_deref())
        .unwrap_or("");
    let budget = parse_memory_bytes(memory).context("presupuesto de la dimensión")?;
    let fact_columns: Vec<String> = fact_schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect();
    let rewritten = rewrite_lookup(select_sql, lookup, &fact_columns)?;
    let mut columns = vec![lookup.dimension_key.clone()];
    for column in &rewritten.columns {
        if !columns.iter().any(|existing| existing == &column.source) {
            columns.push(column.source.clone());
        }
    }
    let (database, table_name) = split_table(&dim.table);
    let catalog = config
        .connectors
        .paimon
        .as_ref()
        .context("una dimensión requiere connectors.paimon.warehouse")?
        .catalog_options();
    let cache = DimensionCache::load_with(
        &catalog,
        &database,
        &table_name,
        &dim.name,
        &dim.key,
        &columns,
        budget,
    )
    .await
    .context("cargando la dimensión")?;

    let fact_key = fact_schema
        .field_with_name(&lookup.fact_key)
        .map_err(|_| anyhow::anyhow!("el stream no tiene la columna '{}'", lookup.fact_key))?;
    match fact_key.data_type() {
        DataType::Int64 | DataType::Int32 | DataType::Utf8 => {}
        other => anyhow::bail!(
            "la clave '{}' es {other}; el lookup la lee como Int64, Int32 o Utf8",
            lookup.fact_key
        ),
    }
    let dim_key = cache.field(&lookup.dimension_key)?;
    if fact_key.data_type() != dim_key.data_type() {
        anyhow::bail!(
            "la clave '{}' es {} en el stream y {} en la dimensión",
            lookup.fact_key,
            fact_key.data_type(),
            dim_key.data_type()
        );
    }

    let keep_misses = lookup.kind == LookupKind::Left;
    let mut fields: Vec<Field> = fact_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    let mut outputs = Vec::with_capacity(rewritten.columns.len());
    for column in &rewritten.columns {
        let source = cache.field(&column.source)?;
        let nullable = keep_misses || source.is_nullable();
        fields.push(Field::new(
            &column.name,
            source.data_type().clone(),
            nullable,
        ));
        outputs.push((column.source.clone(), column.name.clone()));
    }

    Ok(PreparedLookup {
        sql: rewritten.sql,
        fact: lookup.fact.clone(),
        fact_schema,
        enriched_schema: Arc::new(Schema::new(fields)),
        cache: Arc::new(tokio::sync::Mutex::new(cache)),
        fact_key: lookup.fact_key.clone(),
        keep_misses,
        outputs,
    })
}

fn split_table(id: &str) -> (String, String) {
    match id.split_once('.') {
        Some((database, table)) => (database.to_string(), table.to_string()),
        None => ("default".to_string(), id.to_string()),
    }
}

/// Envuelve la fuente del hecho. DataFusion ve `schema` (el hecho más la dimensión).
pub(crate) struct LookupStream {
    inner: RedpandaPartitionStream,
    schema: SchemaRef,
    cache: Arc<tokio::sync::Mutex<DimensionCache>>,
    fact_key: String,
    keep_misses: bool,
    outputs: Vec<(String, String)>,
}

impl LookupStream {
    pub(crate) fn new(inner: RedpandaPartitionStream, prepared: &PreparedLookup) -> Self {
        Self {
            inner,
            schema: prepared.enriched_schema.clone(),
            cache: prepared.cache.clone(),
            fact_key: prepared.fact_key.clone(),
            keep_misses: prepared.keep_misses,
            outputs: prepared.outputs.clone(),
        }
    }
}

impl std::fmt::Debug for LookupStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LookupStream")
            .field("fact_key", &self.fact_key)
            .field("keep_misses", &self.keep_misses)
            .finish()
    }
}

impl PartitionStream for LookupStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let inner = self.inner.execute(ctx);
        let schema = self.schema.clone();
        let yielded = schema.clone();
        let cache = self.cache.clone();
        let fact_key = self.fact_key.clone();
        let keep_misses = self.keep_misses;
        let outputs = self.outputs.clone();
        let stream = futures::stream::unfold(inner, move |mut inner| {
            let cache = cache.clone();
            let schema = schema.clone();
            let fact_key = fact_key.clone();
            let outputs = outputs.clone();
            async move {
                loop {
                    let next = inner.next().await?;
                    let batch = match next {
                        Ok(batch) => batch,
                        Err(err) => return Some((Err(err), inner)),
                    };
                    let enriched = {
                        let mut held = cache.lock().await;
                        if let Err(err) = held.refresh().await {
                            return Some((Err(DataFusionError::Execution(err.to_string())), inner));
                        }
                        match held.enrich(&batch, &fact_key, keep_misses, &outputs, &schema) {
                            Ok(batch) => batch,
                            Err(err) => {
                                return Some((
                                    Err(DataFusionError::Execution(err.to_string())),
                                    inner,
                                ));
                            }
                        }
                    };
                    if enriched.num_rows() == 0 {
                        continue;
                    }
                    return Some((Ok(enriched), inner));
                }
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(yielded, stream))
    }
}
