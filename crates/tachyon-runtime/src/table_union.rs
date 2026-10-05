//! Ventana sobre varias tablas Paimon.
//!
//! Cada tabla es una partición del operador. El watermark de la instancia es
//! el mínimo de esas particiones: una tabla que todavía no trajo filas no
//! deja cerrar. Las filas nuevas de una vuelta entran en un solo
//! `apply_epoch` (el cierre espera a que estén todas) y recién ahí sube el
//! mínimo. No hay flush por idle.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use tachyon_config::{parse_fixed_duration, PipelineConfig};
use tachyon_core::{CheckpointBody, PartitionProgress, WindowCheckpointV1};
use tachyon_metrics::{InstanceMetrics, MetricsServer};
use tachyon_sink::writer::{
    open_table_with, projection_schema, publishes_rowkind, tail_next_append, PaimonSink, Recovered,
};
use tachyon_sql::{UnionBranch, WindowShape, UNION_SOURCE};

use crate::run::{same_window, PipelineHandle, RunOptions};
use crate::table_pipeline::{plan_snapshot, project_batches, split_table, stamp_partition};
use crate::window::{
    batch_from_closed, inputs_from_batch, output_field_names, spec_from_shape, KeyKind,
    WindowOperator, PARTITION_COLUMN,
};

const BRANCH_FIELDS: &[&str] = &[
    "measurement_type",
    "patient_id",
    "value",
    "score",
    "measurement_timestamp",
    PARTITION_COLUMN,
];

struct Source {
    name: String,
    branch_sql: String,
    table: paimon::table::Table,
    columns: Vec<String>,
}

/// Lee las tablas de score y escribe la ventana ancha. Una partición.
pub async fn run_table_union(
    config: &PipelineConfig,
    shape: &WindowShape,
    branches: &[UnionBranch],
    options: &RunOptions,
    metrics: &Arc<InstanceMetrics>,
) -> Result<PipelineHandle> {
    if shape.source != UNION_SOURCE {
        anyhow::bail!("la ventana de varias tablas lee {UNION_SOURCE}");
    }
    if config.deployment.partitions != 1 {
        anyhow::bail!(
            "leer tablas y escribir otra usa una sola partición: el snapshot se lee entero"
        );
    }
    if branches.len() < 2 {
        anyhow::bail!("UNION ALL de tablas tiene al menos dos ramas");
    }
    if config.inputs.len() != branches.len() {
        anyhow::bail!(
            "la ventana une {} tablas y el YAML declara {}",
            branches.len(),
            config.inputs.len()
        );
    }
    let lag_ms = shared_lag(config, shape)?;
    let output_id = config
        .output
        .table
        .as_deref()
        .context("la ventana de las tablas escribe otra tabla")?
        .to_string();
    let catalog = config
        .connectors
        .paimon
        .as_ref()
        .context("leer una tabla requiere connectors.paimon")?
        .catalog_options();

    let mut sources = Vec::with_capacity(branches.len());
    let mut union_schema: Option<SchemaRef> = None;
    for (index, branch) in branches.iter().enumerate() {
        let input = config
            .inputs
            .iter()
            .find(|input| input.name == branch.source)
            .with_context(|| format!("la rama '{}' no es un input", branch.source))?;
        let table_id = input
            .paimon_table()
            .with_context(|| format!("'{}' no es una tabla Paimon", input.name))?
            .to_string();
        let (database, table_name) = split_table(&table_id)?;
        let table = open_table_with(&catalog, &database, &table_name)
            .await
            .with_context(|| format!("abriendo {table_id}"))?;
        if publishes_rowkind(&table)? {
            anyhow::bail!(
                "el source de tabla en el loop lee appends; '{table_id}' tiene changelog"
            );
        }
        let columns: Vec<String> = table
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        let user_schema = Arc::new(projection_schema(&table, &columns).context("schema")?);
        let stamped = stamp_partition(&[RecordBatch::new_empty(user_schema)], index as i32)?;
        let branch_sql = select_with_partition(&branch.sql)?;
        let projected = plan_snapshot(&branch_sql, &input.name, stamped[0].schema())
            .await
            .with_context(|| format!("planificando la rama '{}'", input.name))?;
        let names: Vec<String> = projected
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        if names.iter().map(String::as_str).collect::<Vec<_>>() != BRANCH_FIELDS {
            anyhow::bail!(
                "la rama '{}' escribe {names:?} y la unión espera {BRANCH_FIELDS:?}",
                input.name
            );
        }
        if let Some(previous) = &union_schema {
            if previous.fields() != projected.fields() {
                anyhow::bail!("las ramas de la unión no tienen el mismo schema");
            }
        } else {
            let patient = projected
                .field_with_name("patient_id")
                .context("falta patient_id")?;
            let patient_type = patient.data_type();
            if patient.is_nullable()
                || (patient_type != &DataType::Utf8 && patient_type != &DataType::Int64)
            {
                anyhow::bail!(
                    "patient_id es {}; hace falta Utf8 o Int64 no nulo",
                    patient.data_type()
                );
            }
            let time = projected
                .field_with_name(&shape.event_time)
                .with_context(|| format!("falta {}", shape.event_time))?;
            if !matches!(time.data_type(), DataType::Int64 | DataType::Timestamp(_, _)) {
                anyhow::bail!("event time {} no es Int64 ni Timestamp", time.data_type());
            }
        }
        union_schema = Some(projected);
        sources.push(Source {
            name: input.name.clone(),
            branch_sql,
            table,
            columns,
        });
    }
    let union_schema = union_schema.context("la unión no tiene ramas")?;
    let _planned = plan_snapshot(&shape.rewrite_sql(), UNION_SOURCE, union_schema.clone())
        .await
        .context("planificando la ventana de la unión")?;

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
    let expected = output_field_names(shape);
    if sink.field_names() != expected {
        anyhow::bail!(
            "la tabla tiene campos {:?} y la ventana escribe {expected:?}",
            sink.field_names()
        );
    }
    let recovered = sink
        .recover()
        .await
        .context("recuperando el último checkpoint")?;

    let key_kind = if shape.group_columns.len() == 1
        && union_schema
            .field_with_name(&shape.group_columns[0])
            .is_ok_and(|field| field.data_type() == &DataType::Int64)
    {
        KeyKind::I64
    } else {
        KeyKind::Bytes
    };
    let spec = spec_from_shape(shape);
    let backlog: HashSet<i32> = (0..sources.len() as i32).collect();
    let (mut cursors, mut operator) = match recovered {
        Recovered::None => {
            let mut operator = WindowOperator::new(spec, lag_ms, lag_ms, key_kind);
            operator.set_backlog(backlog.clone());
            (vec![0; sources.len()], operator)
        }
        Recovered::Window { checkpoint, .. } => {
            if !same_window(&checkpoint.spec, &spec) {
                anyhow::bail!(
                    "el checkpoint de '{}' no coincide con la ventana de esta query",
                    options.commit_user
                );
            }
            if checkpoint.consumers_per_topic != 1 {
                anyhow::bail!(
                    "el sidecar de la unión tiene {} consumidores; este corte lee con uno",
                    checkpoint.consumers_per_topic
                );
            }
            let mut cursors = Vec::with_capacity(sources.len());
            for source in &sources {
                let cursor = checkpoint.source_snapshots.get(&source.name).with_context(|| {
                    format!(
                        "el sidecar de '{}' no trae el snapshot de '{}'",
                        options.commit_user, source.name
                    )
                })?;
                cursors.push(*cursor);
            }
            let mut operator = WindowOperator::restore(
                spec,
                lag_ms,
                lag_ms,
                checkpoint.state,
                checkpoint.instance_watermark_ms,
                key_kind,
            )
            .map_err(|err| anyhow::anyhow!("restaurando el estado de ventana: {err}"))?;
            if let Some(parts) = checkpoint.progress.get(UNION_SOURCE) {
                let maxes = parts
                    .iter()
                    .map(|(id, progress)| (*id, progress.max_event_time_ms))
                    .collect();
                operator.activate_restored(&maxes, Instant::now());
            }
            operator.set_backlog(backlog);
            (cursors, operator)
        }
        Recovered::PassThrough { identifier, .. }
        | Recovered::Positions { identifier, .. }
        | Recovered::Join { identifier, .. } => anyhow::bail!(
            "checkpoint {identifier} de '{}' no es de esta ventana",
            options.commit_user
        ),
    };

    if let Some(bind) = options.metrics_bind {
        let server = MetricsServer::new(bind, metrics.clone());
        let addr = server.start().await.context("arrancando métricas")?;
        tracing::info!(%addr, "métricas disponibles");
    }
    tracing::info!(
        tables = sources.len(),
        output = %output_id,
        "unión de tablas en el loop"
    );

    let rewrite = shape.rewrite_sql();
    loop {
        let mut next = cursors.clone();
        let mut pieces: Vec<RecordBatch> = Vec::new();
        let mut moved = false;
        let mut read_rows = 0usize;
        for (index, source) in sources.iter().enumerate() {
            let tail = tail_next_append(&source.table, cursors[index], &source.columns)
                .await
                .with_context(|| format!("leyendo {}", source.name))?;
            if let Some(snapshot) = tail.blocked_at {
                anyhow::bail!(
                    "el snapshot {snapshot} de '{}' es OVERWRITE; el loop no lo publica",
                    source.name
                );
            }
            if tail.through == cursors[index] {
                continue;
            }
            moved = true;
            next[index] = tail.through;
            read_rows += tail.batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
            if tail.batches.is_empty() {
                continue;
            }
            let stamped = stamp_partition(&tail.batches, index as i32)?;
            let projected = project_batches(&source.branch_sql, &source.name, stamped)
                .await
                .with_context(|| format!("proyectando {}", source.name))?;
            pieces.extend(projected.into_iter().filter(|batch| batch.num_rows() > 0));
        }
        if !moved {
            tokio::time::sleep(options.commit_interval).await;
            continue;
        }
        metrics.inc_rows_read(read_rows as u64);
        if !pieces.is_empty() {
            let schema = pieces[0].schema();
            let combined = concat_batches(&schema, &pieces).context("concatenando las ramas")?;
            let projected = project_batches(&rewrite, UNION_SOURCE, vec![combined])
                .await
                .context("proyectando la ventana")?;
            let mut rows = Vec::new();
            for batch in &projected {
                rows.extend(
                    inputs_from_batch(batch, shape, key_kind)
                        .context("leyendo las filas de la ventana")?,
                );
            }
            let closed = operator
                .apply_epoch(&rows, Instant::now())
                .map_err(|err| anyhow::anyhow!("aplicando las tablas: {err}"))?;
            if !closed.is_empty() {
                let batch = batch_from_closed(&closed, shape, union_schema.as_ref())
                    .context("armando la salida de la ventana")?;
                sink.write(&batch).await.context("escribiendo la ventana")?;
                let identifier = sink
                    .commit_checkpoint(&CheckpointBody::Window(union_checkpoint(
                        &operator,
                        shape,
                        &sources,
                        &next,
                    )))
                    .await
                    .context("checkpoint de la ventana")?;
                let Some(identifier) = identifier else {
                    anyhow::bail!("la ventana cerró filas y Paimon no creó snapshot");
                };
                metrics.inc_rows_written(closed.len() as u64);
                metrics.inc_commits();
                tracing::info!(identifier, rows = closed.len(), "ventana de la unión commiteada");
            }
        }
        cursors = next;
    }
}

fn shared_lag(config: &PipelineConfig, shape: &WindowShape) -> Result<i64> {
    let mut lag_ms = None;
    for input in &config.inputs {
        let watermark = input.watermark.as_ref().with_context(|| {
            format!(
                "input '{}': la unión de tablas declara watermark",
                input.name
            )
        })?;
        if watermark.column != shape.event_time {
            anyhow::bail!(
                "watermark.column '{}' de '{}' no es la columna de la ventana '{}'",
                watermark.column,
                input.name,
                shape.event_time
            );
        }
        let lag = parse_fixed_duration(&watermark.lag)
            .map_err(|err| anyhow::anyhow!("watermark.lag de '{}': {err}", input.name))?;
        if let Some(previous) = lag_ms {
            if previous != lag {
                anyhow::bail!("el watermark de las tablas tiene que ser el mismo");
            }
        }
        lag_ms = Some(lag);
        if !shape.group_columns.iter().any(|col| col == &input.key) {
            anyhow::bail!(
                "GROUP BY de ventana no incluye la clave '{}'",
                input.key
            );
        }
    }
    lag_ms.context("la unión no tiene tablas")
}

fn select_with_partition(sql: &str) -> Result<String> {
    let upper = sql.to_ascii_uppercase();
    let Some(idx) = upper.rfind(" FROM ") else {
        anyhow::bail!("la rama no tiene FROM: {sql}");
    };
    Ok(format!(
        "{}, {PARTITION_COLUMN}{}",
        &sql[..idx],
        &sql[idx..]
    ))
}

fn union_checkpoint(
    operator: &WindowOperator,
    shape: &WindowShape,
    sources: &[Source],
    cursors: &[i64],
) -> WindowCheckpointV1 {
    let mut progress = BTreeMap::new();
    progress.insert(
        UNION_SOURCE.to_string(),
        operator
            .partition_progress()
            .into_iter()
            .map(|(id, max_event_time_ms)| (id, PartitionProgress { max_event_time_ms }))
            .collect(),
    );
    let mut source_snapshots = BTreeMap::new();
    for (source, cursor) in sources.iter().zip(cursors.iter()) {
        source_snapshots.insert(source.name.clone(), *cursor);
    }
    WindowCheckpointV1 {
        v: 1,
        commit_identifier: 0,
        consumers_per_topic: 1,
        applied: BTreeMap::new(),
        progress,
        instance_watermark_ms: operator.instance_watermark_ms(),
        spec: spec_from_shape(shape),
        state: operator.freeze_state(),
        source_snapshot: None,
        source_snapshots,
    }
}
