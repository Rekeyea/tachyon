//! Join por intervalo de dos streams que ya llegaron a este proceso.
//!
//! La igualdad es la clave de partición. El `BETWEEN` acota el tiempo, así que
//! un evento se puede tirar cuando el otro lado ya no puede matchearlo. Cada
//! par emite una fila. El buffer y los offsets aplicados entran en el mismo
//! snapshot de Paimon.

use anyhow::{Context, Result};
use arrow::array::{Array, Float64Array, Int64Array, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::sync::Arc;
use tachyon_config::{parse_fixed_duration, InputKind, PipelineConfig};
use tachyon_core::{
    CheckpointBody, JoinCell, JoinCheckpointV1, JoinColumnSpec, JoinEvent, JoinKeyState, JoinSpec,
    JoinState, SourceOffsets,
};
use tachyon_metrics::{InstanceMetrics, MetricsServer};
use tachyon_sink::writer::{PaimonSink, Recovered};
use tachyon_source::consumer::RdkafkaSource;
use tachyon_source::decode::Decoder;
use tachyon_source::record::SourceRecord;
use tachyon_sql::{IntervalJoin, JoinSelect};

use crate::budget::StatelessBudget;
use crate::run::{ensure_topic_partitions, source_client_config, AbortOnDrop, RunOptions};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    Left,
    Right,
}

/// Eventos abiertos de los dos lados, más el reloj para poder expirarlos.
pub struct JoinOperator {
    lower_ms: i64,
    upper_ms: i64,
    base_is_left: bool,
    left_lag_ms: i64,
    right_lag_ms: i64,
    left_clock: BTreeMap<i32, i64>,
    right_clock: BTreeMap<i32, i64>,
    keys: BTreeMap<Vec<u8>, JoinKeyState>,
}

impl JoinOperator {
    pub fn new(
        lower_ms: i64,
        upper_ms: i64,
        base_is_left: bool,
        left_lag_ms: i64,
        right_lag_ms: i64,
    ) -> Self {
        Self {
            lower_ms,
            upper_ms,
            base_is_left,
            left_lag_ms,
            right_lag_ms,
            left_clock: BTreeMap::new(),
            right_clock: BTreeMap::new(),
            keys: BTreeMap::new(),
        }
    }

    pub fn restore(&mut self, checkpoint: &JoinCheckpointV1) {
        self.keys = checkpoint.state.keys.clone();
        self.left_clock = checkpoint
            .progress
            .get(&checkpoint.spec.left)
            .cloned()
            .unwrap_or_default();
        self.right_clock = checkpoint
            .progress
            .get(&checkpoint.spec.right)
            .cloned()
            .unwrap_or_default();
    }

    /// Mete el evento, emite los pares nuevos y tira lo que ya no puede matchear.
    pub(crate) fn apply(&mut self, side: Side, event: Incoming) -> Vec<Vec<JoinCell>> {
        let clock = match side {
            Side::Left => &mut self.left_clock,
            Side::Right => &mut self.right_clock,
        };
        let slot = clock.entry(event.partition).or_insert(event.time_ms);
        *slot = (*slot).max(event.time_ms);

        let buf = self.keys.entry(event.key).or_default();
        let mut emitted = Vec::new();
        match side {
            Side::Left => {
                for other in &buf.right {
                    if pairs(
                        event.time_ms,
                        other.time_ms,
                        self.lower_ms,
                        self.upper_ms,
                        self.base_is_left,
                    ) {
                        emitted.push(zip(&event.values, &other.values));
                    }
                }
                buf.left.push(JoinEvent {
                    time_ms: event.time_ms,
                    values: event.values,
                });
            }
            Side::Right => {
                for other in &buf.left {
                    if pairs(
                        other.time_ms,
                        event.time_ms,
                        self.lower_ms,
                        self.upper_ms,
                        self.base_is_left,
                    ) {
                        emitted.push(zip(&other.values, &event.values));
                    }
                }
                buf.right.push(JoinEvent {
                    time_ms: event.time_ms,
                    values: event.values,
                });
            }
        }
        self.expire();
        emitted
    }

    pub fn checkpoint(&self, spec: &JoinSpec, applied: &SourceOffsets) -> JoinCheckpointV1 {
        let mut progress = BTreeMap::new();
        progress.insert(spec.left.clone(), self.left_clock.clone());
        progress.insert(spec.right.clone(), self.right_clock.clone());
        JoinCheckpointV1 {
            v: 2,
            commit_identifier: 0,
            applied: applied.clone(),
            spec: spec.clone(),
            progress,
            state: JoinState {
                keys: self.keys.clone(),
            },
        }
    }

    fn expire(&mut self) {
        let left_wm = watermark(&self.left_clock, self.left_lag_ms);
        let right_wm = watermark(&self.right_clock, self.right_lag_ms);
        for buf in self.keys.values_mut() {
            if let Some(right_wm) = right_wm {
                buf.left.retain(|event| {
                    !expired_left(
                        event.time_ms,
                        right_wm,
                        self.lower_ms,
                        self.upper_ms,
                        self.base_is_left,
                    )
                });
            }
            if let Some(left_wm) = left_wm {
                buf.right.retain(|event| {
                    !expired_right(
                        event.time_ms,
                        left_wm,
                        self.lower_ms,
                        self.upper_ms,
                        self.base_is_left,
                    )
                });
            }
        }
        self.keys
            .retain(|_, buf| !buf.left.is_empty() || !buf.right.is_empty());
    }
}

pub struct Incoming {
    pub key: Vec<u8>,
    pub time_ms: i64,
    pub partition: i32,
    pub values: Vec<JoinCell>,
}

fn pairs(left_t: i64, right_t: i64, lower: i64, upper: i64, base_is_left: bool) -> bool {
    if base_is_left {
        right_t >= left_t.saturating_add(lower) && right_t <= left_t.saturating_add(upper)
    } else {
        left_t >= right_t.saturating_add(lower) && left_t <= right_t.saturating_add(upper)
    }
}

fn expired_left(time: i64, right_wm: i64, lower: i64, upper: i64, base_is_left: bool) -> bool {
    if base_is_left {
        right_wm > time.saturating_add(upper)
    } else {
        right_wm > time.saturating_sub(lower)
    }
}

fn expired_right(time: i64, left_wm: i64, lower: i64, upper: i64, base_is_left: bool) -> bool {
    if base_is_left {
        left_wm > time.saturating_sub(lower)
    } else {
        left_wm > time.saturating_add(upper)
    }
}

fn watermark(clock: &BTreeMap<i32, i64>, lag_ms: i64) -> Option<i64> {
    clock
        .values()
        .copied()
        .min()
        .map(|max| max.saturating_sub(lag_ms))
}

fn zip(left: &[JoinCell], right: &[JoinCell]) -> Vec<JoinCell> {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            if !matches!(left, JoinCell::Null) {
                left.clone()
            } else {
                right.clone()
            }
        })
        .collect()
}

/// Corre el join hasta que el stream se corta. La salida es la tabla Paimon.
pub async fn run_join_pipeline(
    config: &PipelineConfig,
    options: &RunOptions,
    sink: PaimonSink,
    join: &IntervalJoin,
    input_codecs: &std::collections::HashMap<String, crate::run::PreparedInput>,
    metrics: &Arc<InstanceMetrics>,
) -> Result<crate::run::PipelineHandle> {
    if config
        .deployment
        .consumers_per_topic
        .is_some_and(|count| count != 1)
    {
        anyhow::bail!("el join usa un consumidor por stream");
    }
    if config.inputs.len() != 2 {
        anyhow::bail!("el join tiene dos inputs");
    }
    if config
        .inputs
        .iter()
        .any(|input| input.kind() != InputKind::Topic)
    {
        anyhow::bail!("el join lee topics; kinesis y sqs publican una tabla (pass-through)");
    }
    let left_lag = side_lag(config, &join.left, &join.left_time, &join.left_key)?;
    let right_lag = side_lag(config, &join.right, &join.right_time, &join.right_key)?;
    let budget = StatelessBudget::resolve(config).context("presupuesto del pipeline")?;
    let metrics_addr = if let Some(bind) = options.metrics_bind {
        Some(
            MetricsServer::new(bind, metrics.clone())
                .start()
                .await
                .context("arrancando métricas")?,
        )
    } else {
        None
    };

    let mut sink = sink
        .with_commit_user(&options.commit_user)
        .context("fijando el commit_user del sink")?;
    let recovered = sink
        .recover()
        .await
        .context("recuperando el último checkpoint")?;
    let restored = match recovered {
        Recovered::None => None,
        Recovered::Join { checkpoint, .. } => {
            if checkpoint.spec.left != join.left
                || checkpoint.spec.right != join.right
                || checkpoint.spec.lower_ms != join.lower_ms
                || checkpoint.spec.upper_ms != join.upper_ms
                || checkpoint.spec.base_is_left != join.base_is_left
                || checkpoint.spec.left_time != join.left_time
                || checkpoint.spec.right_time != join.right_time
            {
                anyhow::bail!("el checkpoint de join no coincide con esta query");
            }
            Some(checkpoint)
        }
        Recovered::PassThrough { identifier, .. } => {
            anyhow::bail!("checkpoint {identifier} es pass-through y el plan es un join")
        }
        Recovered::Positions { identifier, .. } => {
            anyhow::bail!(
                "checkpoint {identifier} es de inputs mixtos y el plan es un join; se rechaza"
            )
        }
        Recovered::Window { identifier, .. } => {
            anyhow::bail!("checkpoint {identifier} es de una ventana y el plan es un join")
        }
    };

    let brokers = config.redpanda()?.brokers.join(",");
    let left_input = input_def(config, &join.left)?;
    let right_input = input_def(config, &join.right)?;
    let left_topic = left_input.kafka_topic()?.to_string();
    let right_topic = right_input.kafka_topic()?.to_string();
    ensure_topic_partitions(&brokers, &left_topic, config.deployment.partitions).await?;
    ensure_topic_partitions(&brokers, &right_topic, config.deployment.partitions).await?;

    let mut operator = JoinOperator::new(
        join.lower_ms,
        join.upper_ms,
        join.base_is_left,
        left_lag,
        right_lag,
    );
    let mut applied = SourceOffsets::new();
    if let Some(checkpoint) = &restored {
        operator.restore(checkpoint);
        applied = checkpoint.applied.clone();
    }

    let left_prep = input_codecs
        .get(&join.left)
        .cloned()
        .with_context(|| format!("sin schema para '{}'", join.left))?;
    let right_prep = input_codecs
        .get(&join.right)
        .cloned()
        .with_context(|| format!("sin schema para '{}'", join.right))?;
    let spec = spec_from(join);
    let out_schema = output_schema(&left_prep.schema, &right_prep.schema, &join.columns)?;

    let (mut left_rx, _left_stop) = spawn_input(
        config,
        options,
        &budget,
        left_input,
        &applied,
        left_prep.format.clone(),
        left_prep.schema.clone(),
        true,
        join.columns.clone(),
        join.left_key.clone(),
        join.left_time.clone(),
    )?;
    let (mut right_rx, _right_stop) = spawn_input(
        config,
        options,
        &budget,
        right_input,
        &applied,
        right_prep.format.clone(),
        right_prep.schema.clone(),
        false,
        join.columns.clone(),
        join.right_key.clone(),
        join.right_time.clone(),
    )?;

    let mut tick = tokio::time::interval(options.commit_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    let mut pending: Vec<Vec<JoinCell>> = Vec::new();
    loop {
        tokio::select! {
            batch = left_rx.recv() => {
                match batch {
                    Some(batch) => pending.extend(ingest(&mut operator, &mut applied, Side::Left, &left_topic, batch?, metrics)?),
                    None => break,
                }
            }
            batch = right_rx.recv() => {
                match batch {
                    Some(batch) => pending.extend(ingest(&mut operator, &mut applied, Side::Right, &right_topic, batch?, metrics)?),
                    None => break,
                }
            }
            _ = tick.tick() => {
                flush(&mut sink, &operator, &spec, &applied, &out_schema, &mut pending, metrics).await?;
            }
        }
    }
    flush(
        &mut sink,
        &operator,
        &spec,
        &applied,
        &out_schema,
        &mut pending,
        metrics,
    )
    .await?;
    Ok(crate::run::PipelineHandle {
        metrics_addr,
        metrics: metrics.clone(),
    })
}

fn input_def<'a>(
    config: &'a PipelineConfig,
    name: &str,
) -> Result<&'a tachyon_config::schema::InputDef> {
    config
        .inputs
        .iter()
        .find(|input| input.name == name)
        .with_context(|| format!("el join nombra '{name}' y no está en inputs"))
}

fn side_lag(config: &PipelineConfig, name: &str, time_column: &str, key: &str) -> Result<i64> {
    let input = input_def(config, name)?;
    if input.key != key {
        anyhow::bail!(
            "la clave de '{name}' es '{}' y el join iguala '{key}'",
            input.key
        );
    }
    let watermark = input
        .watermark
        .as_ref()
        .with_context(|| format!("'{name}' necesita watermark para expirar el join"))?;
    if watermark.column != time_column {
        anyhow::bail!(
            "'{name}': el watermark es '{}' y el join usa '{time_column}'",
            watermark.column
        );
    }
    parse_fixed_duration(&watermark.lag)
        .map_err(|err| anyhow::anyhow!("watermark de '{name}': {err}"))
}

fn spec_from(join: &IntervalJoin) -> JoinSpec {
    JoinSpec {
        left: join.left.clone(),
        right: join.right.clone(),
        left_time: join.left_time.clone(),
        right_time: join.right_time.clone(),
        lower_ms: join.lower_ms,
        upper_ms: join.upper_ms,
        base_is_left: join.base_is_left,
        columns: join
            .columns
            .iter()
            .map(|col| JoinColumnSpec {
                side_left: col.side_left,
                column: col.column.clone(),
                alias: col.alias.clone(),
            })
            .collect(),
    }
}

struct DecodedBatch {
    rows: Vec<Incoming>,
    /// Próximo offset por partición, cubriendo este lote.
    next: BTreeMap<i32, i64>,
}

fn spawn_input(
    config: &PipelineConfig,
    options: &RunOptions,
    budget: &StatelessBudget,
    input: &tachyon_config::schema::InputDef,
    applied: &SourceOffsets,
    format: tachyon_source::DecodeFormat,
    schema: SchemaRef,
    side_left: bool,
    columns: Vec<JoinSelect>,
    key_column: String,
    time_column: String,
) -> Result<(
    tokio::sync::mpsc::UnboundedReceiver<Result<DecodedBatch>>,
    AbortOnDrop,
)> {
    let source_group = format!("{}-{}", options.group_id, input.name);
    let mut source_cc = source_client_config(config, &options.group_id, budget)?;
    source_cc.set("group.id", &source_group);
    let topic = input.kafka_topic()?.to_string();
    let mut source = RdkafkaSource::new(&source_cc, &topic)
        .with_context(|| format!("creando source para '{}'", input.name))?
        .with_max_batch(budget.batch_size);
    let resume = applied.get(&topic).cloned().unwrap_or_default();
    if !resume.is_empty() {
        source = source.with_resume_offsets(resume);
    }
    let source = Arc::new(source);
    let decoder = Decoder::new(schema, format);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let mut stream = source.record_stream();
        while let Some(item) = stream.next().await {
            let decoded = item.and_then(|records| {
                decode_records(
                    &decoder,
                    &records,
                    side_left,
                    &columns,
                    &key_column,
                    &time_column,
                )
            });
            if tx.send(decoded).is_err() {
                break;
            }
        }
    });
    Ok((rx, AbortOnDrop(task)))
}

fn decode_records(
    decoder: &Decoder,
    records: &[SourceRecord],
    side_left: bool,
    columns: &[JoinSelect],
    key_column: &str,
    time_column: &str,
) -> Result<DecodedBatch> {
    if records.is_empty() {
        return Ok(DecodedBatch {
            rows: Vec::new(),
            next: BTreeMap::new(),
        });
    }
    let mut payloads: Vec<Vec<u8>> = records.iter().map(|record| record.value.clone()).collect();
    let batch = decoder.decode(&mut payloads)?;
    let key_index = batch
        .schema()
        .index_of(key_column)
        .map_err(|_| anyhow::anyhow!("falta la clave '{key_column}'"))?;
    let time_index = batch
        .schema()
        .index_of(time_column)
        .map_err(|_| anyhow::anyhow!("falta el tiempo '{time_column}'"))?;
    let width = columns.len();
    let mut rows = Vec::new();
    let mut next = BTreeMap::new();
    for (row, record) in records.iter().enumerate() {
        let slot = next.entry(record.partition).or_insert(record.offset + 1);
        *slot = (*slot).max(record.offset + 1);
        let Some(key) = key_bytes(batch.column(key_index), row)? else {
            continue;
        };
        let Some(time_ms) = time_ms(batch.column(time_index), row)? else {
            continue;
        };
        let mut values = vec![JoinCell::Null; width];
        for (index, column) in columns.iter().enumerate() {
            if column.side_left != side_left {
                continue;
            }
            let col_index = batch
                .schema()
                .index_of(&column.column)
                .map_err(|_| anyhow::anyhow!("falta '{}'", column.column))?;
            values[index] = cell_at(batch.column(col_index), row)?;
        }
        rows.push(Incoming {
            key,
            time_ms,
            partition: record.partition,
            values,
        });
    }
    Ok(DecodedBatch { rows, next })
}

fn ingest(
    operator: &mut JoinOperator,
    applied: &mut SourceOffsets,
    side: Side,
    topic: &str,
    batch: DecodedBatch,
    metrics: &InstanceMetrics,
) -> Result<Vec<Vec<JoinCell>>> {
    metrics.inc_rows_read(batch.rows.len() as u64);
    let mut emitted = Vec::new();
    for row in batch.rows {
        emitted.extend(operator.apply(side, row));
    }
    let parts = applied.entry(topic.to_string()).or_default();
    for (partition, next) in batch.next {
        let slot = parts.entry(partition).or_insert(next);
        *slot = (*slot).max(next);
    }
    Ok(emitted)
}

fn key_bytes(column: &dyn Array, row: usize) -> Result<Option<Vec<u8>>> {
    if column.is_null(row) {
        return Ok(None);
    }
    match column.data_type() {
        DataType::Int64 => {
            let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Some(values.value(row).to_le_bytes().to_vec()))
        }
        DataType::Utf8 => {
            let values = column.as_any().downcast_ref::<StringArray>().unwrap();
            let text = values.value(row);
            let mut key = (text.len() as u32).to_le_bytes().to_vec();
            key.extend_from_slice(text.as_bytes());
            Ok(Some(key))
        }
        other => anyhow::bail!("clave de join con tipo {other}"),
    }
}

fn time_ms(column: &dyn Array, row: usize) -> Result<Option<i64>> {
    if column.is_null(row) {
        return Ok(None);
    }
    match column.data_type() {
        DataType::Int64 => Ok(Some(
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(Some(
            column
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .unwrap()
                .value(row),
        )),
        other => anyhow::bail!("tiempo de join con tipo {other}"),
    }
}

fn cell_at(column: &dyn Array, row: usize) -> Result<JoinCell> {
    if column.is_null(row) {
        return Ok(JoinCell::Null);
    }
    match column.data_type() {
        DataType::Int64 => Ok(JoinCell::I64(
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        )),
        DataType::Float64 => Ok(JoinCell::F64(
            column
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        )),
        DataType::Utf8 => Ok(JoinCell::Text(
            column
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        other => anyhow::bail!("columna de join con tipo {other}"),
    }
}

fn output_schema(left: &Schema, right: &Schema, columns: &[JoinSelect]) -> Result<SchemaRef> {
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        let schema = if column.side_left { left } else { right };
        let field = schema
            .field_with_name(&column.column)
            .map_err(|_| anyhow::anyhow!("falta la columna '{}'", column.column))?;
        fields.push(Field::new(
            &column.alias,
            field.data_type().clone(),
            field.is_nullable(),
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}

async fn flush(
    sink: &mut PaimonSink,
    operator: &JoinOperator,
    spec: &JoinSpec,
    applied: &SourceOffsets,
    schema: &SchemaRef,
    pending: &mut Vec<Vec<JoinCell>>,
    metrics: &InstanceMetrics,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let batch = batch_from_rows(schema, pending)?;
    let rows = batch.num_rows();
    sink.write(&batch).await.context("escribiendo el join")?;
    let body = CheckpointBody::Join(operator.checkpoint(spec, applied));
    let committed = sink
        .commit_checkpoint(&body)
        .await
        .context("commiteando el join")?;
    if committed.is_none() {
        anyhow::bail!("el join escribió filas y Paimon no creó snapshot");
    }
    metrics.inc_rows_written(rows as u64);
    metrics.inc_commits();
    pending.clear();
    tracing::info!(rows, "join por intervalo commiteado");
    Ok(())
}

fn batch_from_rows(schema: &SchemaRef, rows: &[Vec<JoinCell>]) -> Result<RecordBatch> {
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();
    for (index, field) in schema.fields().iter().enumerate() {
        columns.push(column_from_cells(field, rows, index)?);
    }
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|err| anyhow::anyhow!("batch de join: {err}"))
}

fn column_from_cells(
    field: &Field,
    rows: &[Vec<JoinCell>],
    index: usize,
) -> Result<Arc<dyn Array>> {
    match field.data_type() {
        DataType::Int64 => {
            let values: Int64Array = rows
                .iter()
                .map(|row| match &row[index] {
                    JoinCell::I64(value) => Some(*value),
                    JoinCell::Null => None,
                    other => unexpected(other),
                })
                .collect();
            Ok(Arc::new(values))
        }
        DataType::Float64 => {
            let values: Float64Array = rows
                .iter()
                .map(|row| match &row[index] {
                    JoinCell::F64(value) => Some(*value),
                    JoinCell::Null => None,
                    other => unexpected(other),
                })
                .collect();
            Ok(Arc::new(values))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in rows {
                match &row[index] {
                    JoinCell::Text(value) => builder.append_value(value),
                    JoinCell::Null => builder.append_null(),
                    other => unexpected(other),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        other => anyhow::bail!("columna '{}' con tipo {other}", field.name()),
    }
}

fn unexpected(cell: &JoinCell) -> ! {
    panic!("celda de join inesperada: {cell:?}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell_i64(value: i64) -> JoinCell {
        JoinCell::I64(value)
    }

    #[test]
    fn a_payment_inside_the_hour_matches_and_one_after_does_not() {
        let mut join = JoinOperator::new(0, 3_600_000, true, 1, 1);
        let left = Incoming {
            key: 1i64.to_le_bytes().to_vec(),
            time_ms: 0,
            partition: 0,
            values: vec![cell_i64(1), cell_i64(10), JoinCell::Null],
        };
        assert!(join.apply(Side::Left, left).is_empty());
        let inside = Incoming {
            key: 1i64.to_le_bytes().to_vec(),
            time_ms: 1_800_000,
            partition: 0,
            values: vec![JoinCell::Null, JoinCell::Null, cell_i64(7)],
        };
        let rows = join.apply(Side::Right, inside);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][2], cell_i64(7));
        let late = Incoming {
            key: 1i64.to_le_bytes().to_vec(),
            time_ms: 7_200_000,
            partition: 0,
            values: vec![JoinCell::Null, JoinCell::Null, cell_i64(8)],
        };
        assert!(join.apply(Side::Right, late).is_empty());
    }
}
