//! Tabla Paimon adentro del loop.
//!
//! Un snapshot es la unidad de lectura. El watermark del snapshot anterior
//! descarta lo tarde; las filas desordenadas del mismo snapshot entran, y
//! recién después sube el watermark. El cursor es el id de ese snapshot y
//! viaja en el sidecar del commit. Sin filas de salida Paimon no crea
//! snapshot, así que el cursor no se persiste y un reinicio relee. Un
//! silencio no cierra ventanas: no hay flush por idle.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use tachyon_config::PipelineConfig;
use tachyon_core::{CheckpointBody, PartitionProgress, SourceOffsets, WindowCheckpointV1};
use tachyon_metrics::{start_observability, InstanceMetrics};
use tachyon_sink::writer::{
    open_table_with, projection_schema, publishes_rowkind, tail_next_append, PaimonSink, Recovered,
};
use tachyon_sql::WindowShape;

use crate::run::{same_window, validate_window, PipelineHandle, RunOptions};
use crate::window::{
    batch_from_closed, inputs_from_batch, schema_with_partition, spec_from_shape, KeyKind,
    WindowOperator, PARTITION_COLUMN,
};

const SNAPSHOT_OPERATORS: &[&str] = &[
    "DataSourceExec",
    "CooperativeExec",
    "FilterExec",
    "ProjectionExec",
    "CoalesceBatchesExec",
    "GlobalLimitExec",
    "LocalLimitExec",
];

enum Drive {
    Window {
        shape: WindowShape,
        operator: WindowOperator,
        key_kind: KeyKind,
        user_schema: SchemaRef,
    },
    /// Filter y project. El cursor va en un sidecar v0, partición 0.
    Project,
}

/// Lee la única tabla de entrada, un snapshot por vuelta, y escribe la tabla
/// de salida. La query es una ventana o un filter/project.
pub async fn run_table_pipeline(
    config: &PipelineConfig,
    select_sql: &str,
    window: Option<&WindowShape>,
    options: &RunOptions,
    metrics: &Arc<InstanceMetrics>,
) -> Result<PipelineHandle> {
    if config.inputs.len() != 1 {
        anyhow::bail!("leer una tabla Paimon es el único input");
    }
    if config.deployment.partitions != 1 {
        anyhow::bail!(
            "leer una tabla y escribir otra usa una sola partición: el snapshot se lee entero"
        );
    }
    let input = &config.inputs[0];
    let table_id = input
        .paimon_table()
        .context("el input no es una tabla Paimon")?
        .to_string();
    let output_id = config
        .output
        .table
        .as_deref()
        .context("la salida de la tabla es otra tabla")?
        .to_string();
    let catalog = config
        .connectors
        .paimon
        .as_ref()
        .context("leer una tabla requiere connectors.paimon")?
        .catalog_options();
    let (database, table_name) = split_table(&table_id)?;
    let source = open_table_with(&catalog, &database, &table_name)
        .await
        .with_context(|| format!("abriendo {table_id}"))?;
    if publishes_rowkind(&source)? {
        anyhow::bail!(
            "el source de tabla en el loop lee appends; una tabla con changelog no entra en este corte"
        );
    }
    let columns: Vec<String> = source
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect();
    if columns.is_empty() {
        anyhow::bail!("la tabla {table_id} no tiene columnas");
    }
    let user_schema = Arc::new(projection_schema(&source, &columns).context("schema de la tabla")?);

    let (out_db, out_table) = split_table(&output_id)?;
    let bucket = config
        .output
        .bucket
        .context("la salida a tabla requiere output.bucket")?;
    let mut sink = PaimonSink::open_with(
        catalog,
        &out_db,
        &out_table,
        &config.output.key,
        bucket as i32,
        config.output.sequence_field.as_deref(),
    )
    .await
    .with_context(|| format!("abriendo sink Paimon para {output_id}"))?
    .align_rowkind(config.output.rowkind_field.as_deref())
    .context("rowkind de la tabla")?
    .with_commit_user(&options.commit_user)
    .context("fijando el commit_user del sink")?;
    let recovered = sink
        .recover()
        .await
        .context("recuperando el último checkpoint")?;

    let (mut cursor, mut drive) = match window {
        Some(shape) => {
            if shape.source != input.name {
                anyhow::bail!("el FROM '{}' no es el input '{}'", shape.source, input.name);
            }
            let (lag_ms, idle_ms) =
                validate_window(config, shape, user_schema.as_ref(), &sink.field_names())?;
            let stamped = schema_with_partition(user_schema.clone());
            let _projected = plan_snapshot(&shape.rewrite_sql(), &input.name, stamped).await?;
            let key_kind = if shape.group_columns.len() == 1
                && user_schema
                    .field_with_name(&shape.group_columns[0])
                    .is_ok_and(|field| field.data_type() == &DataType::Int64)
            {
                KeyKind::I64
            } else {
                KeyKind::Bytes
            };
            let spec = spec_from_shape(shape);
            let (cursor, operator) = match recovered {
                Recovered::None => (0, WindowOperator::new(spec, lag_ms, idle_ms, key_kind)),
                Recovered::Window { checkpoint, .. } => {
                    if !same_window(&checkpoint.spec, &spec) {
                        anyhow::bail!(
                            "el checkpoint de '{}' no coincide con la ventana de esta query",
                            options.commit_user
                        );
                    }
                    if checkpoint.consumers_per_topic != 1 {
                        anyhow::bail!(
                            "el sidecar de la tabla tiene {} consumidores; este corte lee con uno",
                            checkpoint.consumers_per_topic
                        );
                    }
                    let cursor = checkpoint.source_snapshot.with_context(|| {
                        format!(
                            "el sidecar de '{}' no trae el snapshot de la tabla",
                            options.commit_user
                        )
                    })?;
                    let mut operator = WindowOperator::restore(
                        spec,
                        lag_ms,
                        idle_ms,
                        checkpoint.state,
                        checkpoint.instance_watermark_ms,
                        key_kind,
                    )
                    .map_err(|err| anyhow::anyhow!("restaurando el estado de ventana: {err}"))?;
                    if let Some(parts) = checkpoint.progress.get(&input.name) {
                        let maxes = parts
                            .iter()
                            .map(|(id, progress)| (*id, progress.max_event_time_ms))
                            .collect();
                        operator.activate_restored(&maxes, Instant::now());
                    }
                    (cursor, operator)
                }
                Recovered::PassThrough { identifier, .. }
                | Recovered::Positions { identifier, .. }
                | Recovered::Join { identifier, .. } => anyhow::bail!(
                    "checkpoint {identifier} de '{}' no es de esta ventana",
                    options.commit_user
                ),
            };
            (
                cursor,
                Drive::Window {
                    shape: shape.clone(),
                    operator,
                    key_kind,
                    user_schema,
                },
            )
        }
        None => {
            if input.watermark.is_some() {
                anyhow::bail!(
                    "input '{}': la query no es una ventana y la tabla no usa watermark",
                    input.name
                );
            }
            let projected = plan_snapshot(select_sql, &input.name, user_schema).await?;
            let names: Vec<String> = projected
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            let table_fields = sink.field_names();
            if names != table_fields {
                anyhow::bail!(
                    "la tabla tiene campos {table_fields:?} y el SELECT escribe {names:?}"
                );
            }
            let cursor = match recovered {
                Recovered::None => 0,
                Recovered::PassThrough { offsets, .. } => offsets
                    .get(&input.name)
                    .and_then(|parts| parts.get(&0))
                    .copied()
                    .with_context(|| {
                        format!(
                            "el sidecar de '{}' no trae el snapshot de la tabla",
                            options.commit_user
                        )
                    })?,
                Recovered::Window { identifier, .. }
                | Recovered::Positions { identifier, .. }
                | Recovered::Join { identifier, .. } => anyhow::bail!(
                    "checkpoint {identifier} de '{}' no es pass-through",
                    options.commit_user
                ),
            };
            (cursor, Drive::Project)
        }
    };

    start_observability(metrics, options.metrics_bind, options.otlp.as_ref())
        .await
        .context("arrancando observabilidad")?;
    tracing::info!(
        table = %table_id,
        output = %output_id,
        cursor,
        "tabla en el loop; un snapshot por vuelta"
    );

    let input_name = input.name.clone();
    loop {
        let tail = tail_next_append(&source, cursor, &columns)
            .await
            .context("leyendo el snapshot siguiente")?;
        if let Some(snapshot) = tail.blocked_at {
            anyhow::bail!(
                "el snapshot {snapshot} es OVERWRITE; el loop no publica un archivo reescrito"
            );
        }
        if tail.through == cursor {
            tokio::time::sleep(options.commit_interval).await;
            continue;
        }
        let read_rows: usize = tail.batches.iter().map(|batch| batch.num_rows()).sum();
        metrics.inc_rows_read(read_rows as u64);
        let stamped = match &drive {
            Drive::Window { .. } => stamp_partition(&tail.batches, 0)?,
            Drive::Project => tail.batches,
        };
        let sql = match &drive {
            Drive::Window { shape, .. } => shape.rewrite_sql(),
            Drive::Project => select_sql.to_string(),
        };
        let projected = project_batches(&sql, &input_name, stamped)
            .await
            .context("proyectando el snapshot")?;

        match &mut drive {
            Drive::Window {
                shape,
                operator,
                key_kind,
                user_schema,
            } => {
                let mut rows = Vec::new();
                for batch in &projected {
                    rows.extend(
                        inputs_from_batch(batch, shape, *key_kind)
                            .context("leyendo las filas de la ventana")?,
                    );
                }
                // El epoch aplica todo el snapshot contra el watermark anterior
                // y recién ahí lo sube. No hay tick: el idle no es fin de tabla.
                let closed = operator
                    .apply_epoch(&rows, Instant::now())
                    .map_err(|err| anyhow::anyhow!("aplicando el snapshot: {err}"))?;
                if !closed.is_empty() {
                    let batch = batch_from_closed(&closed, shape, user_schema.as_ref())
                        .context("armando la salida de la ventana")?;
                    sink.write(&batch).await.context("escribiendo la ventana")?;
                    let identifier = sink
                        .commit_checkpoint(&CheckpointBody::Window(window_checkpoint(
                            operator,
                            shape,
                            &input_name,
                            tail.through,
                        )))
                        .await
                        .context("checkpoint de la ventana")?;
                    let Some(identifier) = identifier else {
                        anyhow::bail!(
                            "la ventana cerró filas en el snapshot {} y Paimon no creó snapshot",
                            tail.through
                        );
                    };
                    metrics.inc_rows_written(closed.len() as u64);
                    metrics.inc_commits();
                    tracing::info!(
                        identifier,
                        snapshot = tail.through,
                        rows = closed.len(),
                        "ventana de tabla commiteada"
                    );
                }
            }
            Drive::Project => {
                let mut wrote = 0usize;
                for batch in &projected {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    sink.write(batch)
                        .await
                        .context("escribiendo la proyección")?;
                    wrote += batch.num_rows();
                }
                if wrote > 0 {
                    let mut offsets = SourceOffsets::new();
                    offsets.insert(input_name.clone(), BTreeMap::from([(0, tail.through)]));
                    let identifier = sink
                        .commit_checkpoint(&CheckpointBody::Offsets(offsets))
                        .await
                        .context("checkpoint de la proyección")?;
                    let Some(identifier) = identifier else {
                        anyhow::bail!(
                            "la proyección escribió filas del snapshot {} y Paimon no creó snapshot",
                            tail.through
                        );
                    };
                    metrics.inc_rows_written(wrote as u64);
                    metrics.inc_commits();
                    tracing::info!(
                        identifier,
                        snapshot = tail.through,
                        rows = wrote,
                        "proyección commiteada"
                    );
                }
            }
        }
        cursor = tail.through;
    }
}

fn window_checkpoint(
    operator: &WindowOperator,
    shape: &WindowShape,
    input_name: &str,
    snapshot: i64,
) -> WindowCheckpointV1 {
    let mut progress = BTreeMap::new();
    progress.insert(
        input_name.to_string(),
        operator
            .partition_progress()
            .into_iter()
            .map(|(id, max_event_time_ms)| (id, PartitionProgress { max_event_time_ms }))
            .collect(),
    );
    WindowCheckpointV1 {
        v: 1,
        commit_identifier: 0,
        consumers_per_topic: 1,
        applied: SourceOffsets::new(),
        progress,
        instance_watermark_ms: operator.instance_watermark_ms(),
        spec: spec_from_shape(shape),
        state: operator.freeze_state(),
        source_snapshot: Some(snapshot),
        source_snapshots: BTreeMap::new(),
    }
}

pub(crate) fn stamp_partition(batches: &[RecordBatch], partition: i32) -> Result<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| stamp_one(batch, partition))
        .collect()
}

fn stamp_one(batch: &RecordBatch, partition: i32) -> Result<RecordBatch> {
    if batch.schema().field_with_name(PARTITION_COLUMN).is_ok() {
        anyhow::bail!("'{PARTITION_COLUMN}' está reservado");
    }
    let mut fields: Vec<Arc<Field>> = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        PARTITION_COLUMN,
        DataType::Int32,
        false,
    )));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int32Array::from(vec![
        partition;
        batch.num_rows()
    ])));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .context("agregando la partición del snapshot")
}

pub(crate) fn session() -> SessionContext {
    SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(8192),
    )
}

pub(crate) async fn plan_snapshot(sql: &str, table: &str, schema: SchemaRef) -> Result<SchemaRef> {
    let ctx = session();
    let mem = MemTable::try_new(schema, vec![vec![]]).context("armando la tabla del snapshot")?;
    ctx.register_table(table, Arc::new(mem))
        .with_context(|| format!("registrando '{table}'"))?;
    let plan = ctx
        .sql(sql)
        .await
        .context("planificando el snapshot")?
        .create_physical_plan()
        .await
        .context("plan físico del snapshot")?;
    assert_snapshot_plan(&plan)?;
    Ok(plan.schema())
}

fn assert_snapshot_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    if !SNAPSHOT_OPERATORS.contains(&plan.name()) {
        anyhow::bail!(
            "la tabla en el loop solo filtra y proyecta; el plan usa '{}'",
            plan.name()
        );
    }
    if plan.output_partitioning().partition_count() != 1 {
        anyhow::bail!(
            "el plan del snapshot tiene {} particiones",
            plan.output_partitioning().partition_count()
        );
    }
    for child in plan.children() {
        assert_snapshot_plan(child)?;
    }
    Ok(())
}

pub(crate) async fn project_batches(
    sql: &str,
    table: &str,
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() || batches.iter().all(|batch| batch.num_rows() == 0) {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let ctx = session();
    let mem = MemTable::try_new(schema, vec![batches]).context("cargando el snapshot")?;
    ctx.register_table(table, Arc::new(mem))
        .with_context(|| format!("registrando '{table}'"))?;
    ctx.sql(sql)
        .await
        .context("ejecutando la proyección")?
        .collect()
        .await
        .context("leyendo la proyección")
}

pub(crate) fn split_table(id: &str) -> Result<(String, String)> {
    match id.split_once('.') {
        Some((database, table)) if !database.is_empty() && !table.is_empty() => {
            Ok((database.to_string(), table.to_string()))
        }
        None if !id.is_empty() => Ok(("default".to_string(), id.to_string())),
        _ => anyhow::bail!("identificador de tabla inválido: '{id}'"),
    }
}
