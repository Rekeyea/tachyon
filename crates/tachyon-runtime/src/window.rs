//! Operador de ventanas en memoria.
//!
//! El estado dueño es el mapa por clave (`OperatorState`): eso es lo que se
//! clona al sidecar. `by_end` es un árbol B de fines de ventana hacia esas
//! mismas claves. No se persiste. Al restaurar se reconstruye caminando el mapa.
//!
//! Cerrar es un `split_off`: salen las ventanas con fin `<= watermark` y el
//! resto no se recorre. Tumbling y hop anotan el fin una sola vez. Session lo
//! mueve cuando el intervalo se estira o se fusiona.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use tachyon_sql::{WindowAgg, WindowShape};

use tachyon_core::{
    Accumulators, AggKind, AggSpec, AggState, KeyState, OperatorState, SessionState, WindowKind,
    WindowSpecId,
};

/// Una fila ya proyectada. `key` o `event_time_ms` en `None` es un drop
/// determinista: no mueve el reloj ni abre ventana.
#[derive(Debug, Clone)]
pub struct WindowInput {
    pub key: Option<Vec<u8>>,
    pub event_time_ms: Option<i64>,
    pub partition: i32,
    pub offset: i64,
    /// Paralelo a `spec.aggs`. `COUNT(*)` ignora el valor.
    pub values: Vec<Option<Num>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Num {
    I64(i64),
    F64(f64),
}

/// Ventana que acaba de cerrarse. El llamador arma el `RecordBatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClosedWindow {
    pub key: Vec<u8>,
    pub window_start: i64,
    pub window_end: i64,
    pub acc: Accumulators,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFault {
    Overflow {
        key: Vec<u8>,
        sum: i128,
        partition: i32,
        offset: i64,
    },
    TypeMix {
        partition: i32,
        offset: i64,
    },
    NonFinite {
        partition: i32,
        offset: i64,
    },
    ValueCount {
        expected: usize,
        got: usize,
    },
}

impl std::fmt::Display for WindowFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowFault::Overflow {
                sum,
                partition,
                offset,
                ..
            } => write!(
                f,
                "SUM no entra en i64 ({sum}) en la partición {partition} offset {offset}"
            ),
            WindowFault::TypeMix { partition, offset } => {
                write!(f, "mezcla Int64 y Float64 en partición {partition} offset {offset}")
            }
            WindowFault::NonFinite { partition, offset } => {
                write!(f, "Float64 no finito en partición {partition} offset {offset}")
            }
            WindowFault::ValueCount { expected, got } => {
                write!(f, "la fila trae {got} medidas y el spec pide {expected}")
            }
        }
    }
}

impl std::error::Error for WindowFault {}

#[derive(Debug, Clone)]
struct PartitionClock {
    max_event_time_ms: Option<i64>,
    last_on_time: Option<Instant>,
}

pub const PARTITION_COLUMN: &str = "_tachyon_partition";

/// Schema que ve DataFusion: columnas del usuario más la partición de Kafka.
pub fn schema_with_partition(schema: SchemaRef) -> SchemaRef {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(PARTITION_COLUMN, DataType::Int32, false)));
    Arc::new(Schema::new(fields))
}

pub fn user_schema_without_partition(schema: &SchemaRef) -> SchemaRef {
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|field| field.name() != PARTITION_COLUMN)
        .cloned()
        .collect();
    Arc::new(Schema::new(fields))
}

type SchemaRef = Arc<Schema>;

/// Nombres de columna que tiene que tener la tabla, en ese orden.
pub fn output_field_names(shape: &WindowShape) -> Vec<String> {
    let mut names = shape.group_columns.clone();
    names.push("window_start".into());
    names.push("window_end".into());
    names.extend(shape.aggs.iter().map(|agg| agg.alias.clone()));
    names
}

pub fn spec_from_shape(shape: &WindowShape) -> WindowSpecId {
    WindowSpecId {
        input: shape.source.clone(),
        event_time: shape.event_time.clone(),
        kind: shape.kind,
        size_ms: shape.size_ms,
        slide_ms: shape.slide_ms,
        gap_ms: shape.gap_ms,
        group_columns: shape.group_columns.clone(),
        partial: false,
        aggs: shape
            .aggs
            .iter()
            .map(|agg| tachyon_core::AggSpec {
                kind: agg.kind,
                input: agg.input.clone(),
                alias: agg.alias.clone(),
            })
            .collect(),
    }
}

/// Filas del batch reescrito. La última columna es `_tachyon_partition`.
pub fn inputs_from_batch(batch: &RecordBatch, shape: &WindowShape) -> anyhow::Result<Vec<WindowInput>> {
    let schema = batch.schema();
    let partition_idx = schema
        .index_of(PARTITION_COLUMN)
        .map_err(|_| anyhow::anyhow!("el batch de ventana no trae {PARTITION_COLUMN}"))?;
    let partitions = batch
        .column(partition_idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow::anyhow!("{PARTITION_COLUMN} no es Int32"))?;
    let time_idx = schema
        .index_of(&shape.event_time)
        .map_err(|_| anyhow::anyhow!("falta la columna de tiempo '{}'", shape.event_time))?;
    let mut group_idx = Vec::new();
    for name in &shape.group_columns {
        group_idx.push(
            schema
                .index_of(name)
                .map_err(|_| anyhow::anyhow!("falta la clave '{name}'"))?,
        );
    }
    let mut measure_idx = Vec::new();
    for agg in &shape.aggs {
        measure_idx.push(match &agg.input {
            Some(name) => Some(
                schema
                    .index_of(name)
                    .map_err(|_| anyhow::anyhow!("falta la medida '{name}'"))?,
            ),
            None => None,
        });
    }
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let key = encode_group_key(batch, &group_idx, row)?;
        let event_time_ms = event_time_at(batch.column(time_idx).as_ref(), row)?;
        let mut values = Vec::with_capacity(shape.aggs.len());
        for (agg, idx) in shape.aggs.iter().zip(&measure_idx) {
            values.push(match idx {
                Some(i) => measure_at(batch.column(*i).as_ref(), row, agg)?,
                None => None,
            });
        }
        rows.push(WindowInput {
            key,
            event_time_ms,
            partition: partitions.value(row),
            offset: 0,
            values,
        });
    }
    Ok(rows)
}

pub fn batch_from_closed(
    closed: &[ClosedWindow],
    shape: &WindowShape,
    input: &Schema,
) -> anyhow::Result<RecordBatch> {
    let mut fields = Vec::new();
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();
    for name in &shape.group_columns {
        let field = input
            .field_with_name(name)
            .map_err(|_| anyhow::anyhow!("la clave '{name}' no está en el schema"))?;
        fields.push(field.clone());
        columns.push(group_column(closed, name, shape, field.data_type())?);
    }
    fields.push(Field::new("window_start", DataType::Int64, false));
    columns.push(Arc::new(Int64Array::from(
        closed.iter().map(|w| w.window_start).collect::<Vec<_>>(),
    )));
    fields.push(Field::new("window_end", DataType::Int64, false));
    columns.push(Arc::new(Int64Array::from(
        closed.iter().map(|w| w.window_end).collect::<Vec<_>>(),
    )));
    for (i, agg) in shape.aggs.iter().enumerate() {
        let (field, column) = agg_column(closed, i, agg)?;
        fields.push(field);
        columns.push(column);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| anyhow::anyhow!("batch de ventana: {e}"))
}

fn encode_group_key(
    batch: &RecordBatch,
    indexes: &[usize],
    row: usize,
) -> anyhow::Result<Option<Vec<u8>>> {
    let mut key = Vec::new();
    for &idx in indexes {
        let column = batch.column(idx);
        if column.is_null(row) {
            return Ok(None);
        }
        match column.data_type() {
            DataType::Int64 => {
                let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
                key.extend_from_slice(&values.value(row).to_le_bytes());
            }
            DataType::Utf8 => {
                let values = column.as_any().downcast_ref::<StringArray>().unwrap();
                let text = values.value(row);
                key.extend_from_slice(&(text.len() as u32).to_le_bytes());
                key.extend_from_slice(text.as_bytes());
            }
            other => anyhow::bail!("clave de ventana con tipo {other}, hace falta Int64 o Utf8"),
        }
    }
    Ok(Some(key))
}

fn event_time_at(column: &dyn Array, row: usize) -> anyhow::Result<Option<i64>> {
    if column.is_null(row) {
        return Ok(None);
    }
    let ms = match column.data_type() {
        DataType::Int64 => column.as_any().downcast_ref::<Int64Array>().unwrap().value(row),
        DataType::Timestamp(TimeUnit::Second, _) => {
            column.as_any().downcast_ref::<arrow::array::TimestampSecondArray>().unwrap().value(row)
                * 1_000
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => column
            .as_any()
            .downcast_ref::<arrow::array::TimestampMillisecondArray>()
            .unwrap()
            .value(row),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            column
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .unwrap()
                .value(row)
                / 1_000
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            column
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .unwrap()
                .value(row)
                / 1_000_000
        }
        other => anyhow::bail!("event time {other} no es Int64 ni Timestamp"),
    };
    Ok(Some(ms))
}

fn measure_at(column: &dyn Array, row: usize, agg: &WindowAgg) -> anyhow::Result<Option<Num>> {
    if column.is_null(row) {
        return Ok(None);
    }
    let _ = agg;
    match column.data_type() {
        DataType::Int64 => Ok(Some(Num::I64(
            column.as_any().downcast_ref::<Int64Array>().unwrap().value(row),
        ))),
        DataType::Float64 => Ok(Some(Num::F64(
            column.as_any().downcast_ref::<Float64Array>().unwrap().value(row),
        ))),
        _ => Ok(Some(Num::I64(1))),
    }
}

fn group_column(
    closed: &[ClosedWindow],
    name: &str,
    shape: &WindowShape,
    dtype: &DataType,
) -> anyhow::Result<Arc<dyn Array>> {
    let index = shape
        .group_columns
        .iter()
        .position(|col| col == name)
        .ok_or_else(|| anyhow::anyhow!("columna de grupo '{name}'"))?;
    match dtype {
        DataType::Int64 => {
            let mut values = Vec::with_capacity(closed.len());
            for window in closed {
                values.push(decode_i64_key(&window.key, index, &shape.group_columns)?);
            }
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for window in closed {
                builder.append_value(decode_utf8_key(&window.key, index, &shape.group_columns)?);
            }
            Ok(Arc::new(builder.finish()))
        }
        other => anyhow::bail!("salida de clave {other}"),
    }
}

fn decode_i64_key(key: &[u8], index: usize, columns: &[String]) -> anyhow::Result<i64> {
    let start = index * 8;
    let bytes: [u8; 8] = key
        .get(start..start + 8)
        .ok_or_else(|| anyhow::anyhow!("clave corta para {}", columns[index]))?
        .try_into()
        .unwrap();
    let _ = columns;
    Ok(i64::from_le_bytes(bytes))
}

fn decode_utf8_key(key: &[u8], index: usize, _columns: &[String]) -> anyhow::Result<String> {
    // v1 de una sola columna utf8 al inicio. Compuestas utf8 se leen en orden.
    let mut offset = 0;
    for _ in 0..index {
        if offset + 4 > key.len() {
            anyhow::bail!("clave utf8 corta");
        }
        let len = u32::from_le_bytes(key[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4 + len;
    }
    let len = u32::from_le_bytes(key[offset..offset + 4].try_into().unwrap()) as usize;
    let text = std::str::from_utf8(&key[offset + 4..offset + 4 + len])
        .map_err(|e| anyhow::anyhow!("clave utf8: {e}"))?;
    Ok(text.to_string())
}

fn agg_column(
    closed: &[ClosedWindow],
    index: usize,
    agg: &WindowAgg,
) -> anyhow::Result<(Field, Arc<dyn Array>)> {
    let slot = closed.first().map(|w| &w.acc.slots[index]);
    match (agg.kind, slot) {
        (AggKind::Count, _) => Ok((
            Field::new(&agg.alias, DataType::Int64, false),
            Arc::new(Int64Array::from(
                closed
                    .iter()
                    .map(|w| match w.acc.slots[index] {
                        AggState::Count(n) => n,
                        _ => 0,
                    })
                    .collect::<Vec<_>>(),
            )),
        )),
        (AggKind::Avg, _) | (_, Some(AggState::SumF64(_)) | Some(AggState::MinF64(_)) | Some(AggState::MaxF64(_))) => {
            let values = closed
                .iter()
                .map(|w| float_slot(&w.acc, index))
                .collect::<Vec<_>>();
            Ok((
                Field::new(&agg.alias, DataType::Float64, true),
                Arc::new(Float64Array::from(values)),
            ))
        }
        _ => {
            let values = closed
                .iter()
                .map(|w| int_slot(&w.acc, index))
                .collect::<Vec<_>>();
            Ok((
                Field::new(&agg.alias, DataType::Int64, true),
                Arc::new(Int64Array::from(values)),
            ))
        }
    }
}

fn int_slot(acc: &Accumulators, index: usize) -> Option<i64> {
    if acc.present.get(index) == Some(&false) {
        return None;
    }
    match acc.slots.get(index)? {
        AggState::SumI64(n) => i64::try_from(*n).ok(),
        AggState::MinI64(n) | AggState::MaxI64(n) => Some(*n),
        AggState::Count(n) => Some(*n),
        _ => None,
    }
}

fn float_slot(acc: &Accumulators, index: usize) -> Option<f64> {
    if acc.present.get(index) == Some(&false) {
        return None;
    }
    match acc.slots.get(index)? {
        AggState::SumF64(n) | AggState::MinF64(n) | AggState::MaxF64(n) => Some(*n),
        AggState::Avg { sum, count } if *count > 0 => Some(*sum / *count as f64),
        AggState::Avg { .. } => None,
        AggState::SumI64(n) => Some(*n as f64),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct WindowOperator {
    spec: WindowSpecId,
    lag_ms: i64,
    idle: std::time::Duration,
    keys: BTreeMap<Vec<u8>, KeyState>,
    /// Fin de disparo (el `end` de tumble/hop, `end + gap` de session) → claves.
    by_end: BTreeMap<i64, BTreeSet<Vec<u8>>>,
    partitions: BTreeMap<i32, PartitionClock>,
    /// Particiones que este proceso ya soltó. Sus filas no reabren ventanas:
    /// las vuelve a leer quien las tenga ahora.
    released: std::collections::HashSet<i32>,
    instance_watermark_ms: Option<i64>,
}

impl WindowOperator {
    pub fn new(spec: WindowSpecId, lag_ms: i64, idle_ms: i64) -> Self {
        Self {
            spec,
            lag_ms,
            idle: std::time::Duration::from_millis(idle_ms.max(0) as u64),
            keys: BTreeMap::new(),
            by_end: BTreeMap::new(),
            partitions: BTreeMap::new(),
            released: std::collections::HashSet::new(),
            instance_watermark_ms: None,
        }
    }

    /// Restaura el mapa del sidecar y rearma `by_end`.
    pub fn restore(
        spec: WindowSpecId,
        lag_ms: i64,
        idle_ms: i64,
        state: OperatorState,
        instance_watermark_ms: Option<i64>,
    ) -> Self {
        let mut op = Self::new(spec, lag_ms, idle_ms);
        op.keys = state.keys;
        op.instance_watermark_ms = instance_watermark_ms;
        op.reindex();
        op
    }

    pub fn instance_watermark_ms(&self) -> Option<i64> {
        self.instance_watermark_ms
    }

    pub fn spec(&self) -> &WindowSpecId {
        &self.spec
    }

    pub fn is_released(&self, partition: i32) -> bool {
        self.released.contains(&partition)
    }

    /// Suelta la partición sin emitir. Las ventanas abiertas las reconstruye
    /// quien la reciba, desde el último snapshot.
    pub fn release_partition(&mut self, partition: i32) {
        self.released.insert(partition);
        self.partitions.remove(&partition);
        self.keys.retain(|_, state| state.partition != partition);
        self.reindex();
    }

    /// Incorpora la ficha commiteada de una partición que este proceso no tenía.
    /// Puede cerrar ventanas si el watermark de esa partición ya las pasó.
    pub fn adopt_partition(
        &mut self,
        partition: i32,
        keys: BTreeMap<Vec<u8>, KeyState>,
        max_event_time_ms: Option<i64>,
        now: Instant,
    ) -> Vec<ClosedWindow> {
        self.released.remove(&partition);
        for (key, mut state) in keys {
            state.partition = partition;
            self.keys.insert(key, state);
        }
        self.partitions.insert(
            partition,
            PartitionClock {
                max_event_time_ms,
                last_on_time: Some(now),
            },
        );
        self.reindex();
        self.raise_and_close(now)
    }

    pub fn open_windows(&self) -> usize {
        self.keys
            .values()
            .map(|state| state.windows.len() + state.sessions.len())
            .sum()
    }

    /// Aplica el batch en orden. Si una fila falla, el operador vuelve al
    /// estado de antes del batch y no devuelve las ventanas que ese batch
    /// hubiera cerrado.
    pub fn apply(
        &mut self,
        rows: &[WindowInput],
        now: Instant,
    ) -> Result<Vec<ClosedWindow>, WindowFault> {
        let snapshot = self.clone();
        match self.apply_inner(rows, now) {
            Ok(closed) => Ok(closed),
            Err(err) => {
                *self = snapshot;
                Err(err)
            }
        }
    }

    /// Recalcula particiones idle. Puede cerrar ventanas si el mínimo sube.
    /// No cierra por el paso del tiempo de pared cuando todas están idle.
    pub fn on_tick(&mut self, now: Instant) -> Vec<ClosedWindow> {
        self.raise_and_close(now)
    }

    /// Máximo event time por partición, para el sidecar.
    pub fn partition_progress(&self) -> BTreeMap<i32, Option<i64>> {
        self.partitions
            .iter()
            .map(|(id, clock)| (*id, clock.max_event_time_ms))
            .collect()
    }

    /// Tras un restore las particiones del sidecar vuelven a contar como
    /// activas hasta que pase `idle` sin un evento a tiempo.
    pub fn activate_restored(&mut self, progress: &BTreeMap<i32, Option<i64>>, now: Instant) {
        for (id, max) in progress {
            self.partitions.insert(
                *id,
                PartitionClock {
                    max_event_time_ms: *max,
                    last_on_time: Some(now),
                },
            );
        }
    }

    /// El mapa por clave, listo para el sidecar. `by_end` no entra.
    pub fn freeze_state(&self) -> OperatorState {
        OperatorState {
            keys: self.keys.clone(),
        }
    }

    fn apply_inner(
        &mut self,
        rows: &[WindowInput],
        now: Instant,
    ) -> Result<Vec<ClosedWindow>, WindowFault> {
        let mut closed = Vec::new();
        for row in rows {
            closed.extend(self.apply_row(row, now)?);
        }
        Ok(closed)
    }

    fn apply_row(
        &mut self,
        row: &WindowInput,
        now: Instant,
    ) -> Result<Vec<ClosedWindow>, WindowFault> {
        let (Some(key), Some(t)) = (&row.key, row.event_time_ms) else {
            return Ok(Vec::new());
        };
        if self.released.contains(&row.partition) {
            return Ok(Vec::new());
        }
        if self
            .instance_watermark_ms
            .is_some_and(|watermark| t < watermark)
        {
            return Ok(Vec::new());
        }
        let clock = self.partitions.entry(row.partition).or_insert(PartitionClock {
            max_event_time_ms: None,
            last_on_time: None,
        });
        clock.max_event_time_ms = Some(clock.max_event_time_ms.map_or(t, |max| max.max(t)));
        clock.last_on_time = Some(now);
        self.assign(key, t, row)?;
        Ok(self.raise_and_close(now))
    }

    fn assign(&mut self, key: &Vec<u8>, t: i64, row: &WindowInput) -> Result<(), WindowFault> {
        match self.spec.kind {
            WindowKind::Tumble => {
                let start = align_down(t, self.spec.size_ms);
                self.touch_fixed(key, start, start + self.spec.size_ms, row)
            }
            WindowKind::Hop => {
                let slide = self.spec.slide_ms.expect("hop con slide");
                let size = self.spec.size_ms;
                let mut start = align_down(t, slide);
                let mut guard = 0;
                while start > t - size {
                    self.touch_fixed(key, start, start + size, row)?;
                    start -= slide;
                    guard += 1;
                    if guard > 10_000 {
                        break;
                    }
                }
                Ok(())
            }
            WindowKind::Session => {
                let gap = self.spec.gap_ms.expect("session con gap");
                self.touch_session(key, t, gap, row)
            }
        }
    }

    fn touch_fixed(
        &mut self,
        key: &Vec<u8>,
        start: i64,
        end: i64,
        row: &WindowInput,
    ) -> Result<(), WindowFault> {
        let state = self
            .keys
            .entry(key.clone())
            .or_insert_with(|| empty_key(row.partition));
        state.partition = row.partition;
        let is_new = !state.windows.contains_key(&start);
        let acc = state
            .windows
            .entry(start)
            .or_insert_with(|| fresh_acc(&self.spec.aggs));
        absorb(acc, &self.spec.aggs, &row.values, row)?;
        if is_new {
            self.by_end.entry(end).or_default().insert(key.clone());
        }
        Ok(())
    }

    fn touch_session(
        &mut self,
        key: &Vec<u8>,
        t: i64,
        gap: i64,
        row: &WindowInput,
    ) -> Result<(), WindowFault> {
        let aggs = self.spec.aggs.clone();
        let state = self
            .keys
            .entry(key.clone())
            .or_insert_with(|| empty_key(row.partition));
        state.partition = row.partition;
        let hit: Vec<usize> = state
            .sessions
            .iter()
            .enumerate()
            .filter(|(_, session)| intersects(session, t, gap))
            .map(|(index, _)| index)
            .collect();
        if hit.is_empty() {
            let mut acc = fresh_acc(&aggs);
            absorb(&mut acc, &aggs, &row.values, row)?;
            insert_sorted(
                &mut state.sessions,
                SessionState {
                    start_ms: t,
                    end_ms: t,
                    acc,
                },
            );
            self.by_end
                .entry(t.saturating_add(gap))
                .or_default()
                .insert(key.clone());
            return Ok(());
        }
        let mut removed = Vec::with_capacity(hit.len());
        for index in hit.into_iter().rev() {
            removed.push(state.sessions.remove(index));
        }
        removed.reverse();
        let mut merged = removed.remove(0);
        let mut old_fires = vec![merged.end_ms.saturating_add(gap)];
        for other in removed {
            old_fires.push(other.end_ms.saturating_add(gap));
            merge_acc(&mut merged.acc, &other.acc, row)?;
            merged.start_ms = merged.start_ms.min(other.start_ms);
            merged.end_ms = merged.end_ms.max(other.end_ms);
        }
        let mut contribution = fresh_acc(&aggs);
        absorb(&mut contribution, &aggs, &row.values, row)?;
        merge_acc(&mut merged.acc, &contribution, row)?;
        merged.start_ms = merged.start_ms.min(t);
        merged.end_ms = merged.end_ms.max(t);
        let new_fire = merged.end_ms.saturating_add(gap);
        insert_sorted(&mut state.sessions, merged);
        for fire in old_fires {
            unindex(&mut self.by_end, fire, key);
        }
        self.by_end.entry(new_fire).or_default().insert(key.clone());
        Ok(())
    }

    fn raise_and_close(&mut self, now: Instant) -> Vec<ClosedWindow> {
        let min = self
            .partitions
            .values()
            .filter(|clock| clock.is_active(now, self.idle))
            .filter_map(|clock| clock.max_event_time_ms)
            .map(|max| max.saturating_sub(self.lag_ms))
            .min();
        if let Some(min) = min {
            self.instance_watermark_ms = Some(match self.instance_watermark_ms {
                Some(current) => current.max(min),
                None => min,
            });
        }
        let Some(watermark) = self.instance_watermark_ms else {
            return Vec::new();
        };
        self.close_until(watermark)
    }

    fn close_until(&mut self, watermark: i64) -> Vec<ClosedWindow> {
        let later = if watermark == i64::MAX {
            BTreeMap::new()
        } else {
            self.by_end.split_off(&(watermark + 1))
        };
        let due = std::mem::replace(&mut self.by_end, later);
        let mut closed = Vec::new();
        for (fire_at, keys) in due {
            for key in keys {
                let Some(state) = self.keys.get_mut(&key) else {
                    continue;
                };
                match self.spec.kind {
                    WindowKind::Session => {
                        let gap = self.spec.gap_ms.unwrap_or(0);
                        if let Some(pos) = state
                            .sessions
                            .iter()
                            .position(|session| session.end_ms.saturating_add(gap) == fire_at)
                        {
                            let session = state.sessions.remove(pos);
                            closed.push(ClosedWindow {
                                key: key.clone(),
                                window_start: session.start_ms,
                                window_end: session.end_ms,
                                acc: session.acc,
                            });
                        }
                    }
                    WindowKind::Tumble | WindowKind::Hop => {
                        let start = fire_at.saturating_sub(self.spec.size_ms);
                        if let Some(acc) = state.windows.remove(&start) {
                            closed.push(ClosedWindow {
                                key: key.clone(),
                                window_start: start,
                                window_end: fire_at,
                                acc,
                            });
                        }
                    }
                }
                if state.windows.is_empty() && state.sessions.is_empty() {
                    self.keys.remove(&key);
                }
            }
        }
        closed
    }

    fn reindex(&mut self) {
        self.by_end.clear();
        let kind = self.spec.kind;
        let size = self.spec.size_ms;
        let gap = self.spec.gap_ms.unwrap_or(0);
        for (key, state) in &self.keys {
            match kind {
                WindowKind::Tumble | WindowKind::Hop => {
                    for start in state.windows.keys() {
                        self.by_end
                            .entry(start.saturating_add(size))
                            .or_default()
                            .insert(key.clone());
                    }
                }
                WindowKind::Session => {
                    for session in &state.sessions {
                        self.by_end
                            .entry(session.end_ms.saturating_add(gap))
                            .or_default()
                            .insert(key.clone());
                    }
                }
            }
        }
    }
}

impl PartitionClock {
    fn is_active(&self, now: Instant, idle: std::time::Duration) -> bool {
        match self.last_on_time {
            Some(seen) => now.saturating_duration_since(seen) < idle,
            None => false,
        }
    }
}

fn empty_key(partition: i32) -> KeyState {
    KeyState {
        partition,
        windows: BTreeMap::new(),
        sessions: Vec::new(),
    }
}

fn align_down(ts: i64, size: i64) -> i64 {
    ts - ts.rem_euclid(size)
}

fn intersects(session: &SessionState, t: i64, gap: i64) -> bool {
    t < session.end_ms.saturating_add(gap) && session.start_ms < t.saturating_add(gap)
}

fn unindex(by_end: &mut BTreeMap<i64, BTreeSet<Vec<u8>>>, fire: i64, key: &Vec<u8>) {
    let Some(keys) = by_end.get_mut(&fire) else {
        return;
    };
    keys.remove(key);
    if keys.is_empty() {
        by_end.remove(&fire);
    }
}

fn insert_sorted(sessions: &mut Vec<SessionState>, session: SessionState) {
    let pos = sessions
        .iter()
        .position(|existing| existing.start_ms > session.start_ms)
        .unwrap_or(sessions.len());
    sessions.insert(pos, session);
}

fn fresh_acc(aggs: &[AggSpec]) -> Accumulators {
    Accumulators {
        slots: aggs
            .iter()
            .map(|agg| match agg.kind {
                AggKind::Count => AggState::Count(0),
                AggKind::Sum => AggState::SumI64(0),
                AggKind::Min => AggState::MinI64(0),
                AggKind::Max => AggState::MaxI64(0),
                AggKind::Avg => AggState::Avg { sum: 0.0, count: 0 },
            })
            .collect(),
        present: aggs.iter().map(|agg| agg.kind == AggKind::Count).collect(),
    }
}

fn absorb(
    acc: &mut Accumulators,
    aggs: &[AggSpec],
    values: &[Option<Num>],
    row: &WindowInput,
) -> Result<(), WindowFault> {
    if values.len() != aggs.len() {
        return Err(WindowFault::ValueCount {
            expected: aggs.len(),
            got: values.len(),
        });
    }
    if acc.present.len() != acc.slots.len() {
        acc.present = vec![true; acc.slots.len()];
    }
    for (i, agg) in aggs.iter().enumerate() {
        match agg.kind {
            AggKind::Count => {
                let counts = agg.input.is_none() || values[i].is_some();
                if counts {
                    if let AggState::Count(n) = &mut acc.slots[i] {
                        *n = n.saturating_add(1);
                    }
                }
            }
            AggKind::Sum | AggKind::Min | AggKind::Max | AggKind::Avg => {
                let Some(num) = values[i] else {
                    continue;
                };
                if let Num::F64(v) = num {
                    if !v.is_finite() {
                        return Err(WindowFault::NonFinite {
                            partition: row.partition,
                            offset: row.offset,
                        });
                    }
                }
                apply_num(
                    &mut acc.slots[i],
                    &mut acc.present[i],
                    agg.kind,
                    num,
                    row,
                )?;
            }
        }
    }
    Ok(())
}

fn apply_num(
    slot: &mut AggState,
    present: &mut bool,
    kind: AggKind,
    num: Num,
    row: &WindowInput,
) -> Result<(), WindowFault> {
    if !*present {
        *slot = match (kind, num) {
            (AggKind::Sum, Num::I64(_)) => AggState::SumI64(0),
            (AggKind::Sum, Num::F64(_)) => AggState::SumF64(0.0),
            (AggKind::Min, Num::I64(_)) => AggState::MinI64(0),
            (AggKind::Min, Num::F64(_)) => AggState::MinF64(0.0),
            (AggKind::Max, Num::I64(_)) => AggState::MaxI64(0),
            (AggKind::Max, Num::F64(_)) => AggState::MaxF64(0.0),
            (AggKind::Avg, _) => AggState::Avg { sum: 0.0, count: 0 },
            _ => slot.clone(),
        };
    }
    match (slot, num) {
        (AggState::SumI64(sum), Num::I64(v)) => {
            *sum += i128::from(v);
            if *sum > i128::from(i64::MAX) || *sum < i128::from(i64::MIN) {
                return Err(WindowFault::Overflow {
                    key: row.key.clone().unwrap_or_default(),
                    sum: *sum,
                    partition: row.partition,
                    offset: row.offset,
                });
            }
            *present = true;
        }
        (AggState::SumF64(sum), Num::F64(v)) => {
            *sum += v;
            *present = true;
        }
        (AggState::MinI64(min), Num::I64(v)) => {
            if !*present || v < *min {
                *min = v;
            }
            *present = true;
        }
        (AggState::MinF64(min), Num::F64(v)) => {
            if !*present || v < *min {
                *min = v;
            }
            *present = true;
        }
        (AggState::MaxI64(max), Num::I64(v)) => {
            if !*present || v > *max {
                *max = v;
            }
            *present = true;
        }
        (AggState::MaxF64(max), Num::F64(v)) => {
            if !*present || v > *max {
                *max = v;
            }
            *present = true;
        }
        (AggState::Avg { sum, count }, Num::I64(v)) => {
            *sum += v as f64;
            *count += 1;
            *present = true;
        }
        (AggState::Avg { sum, count }, Num::F64(v)) => {
            *sum += v;
            *count += 1;
            *present = true;
        }
        _ => {
            return Err(WindowFault::TypeMix {
                partition: row.partition,
                offset: row.offset,
            })
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tachyon_core::{AggKind, AggSpec, AggState, WindowKind};

    fn spec(kind: WindowKind, size: i64, slide: Option<i64>, gap: Option<i64>) -> WindowSpecId {
        WindowSpecId {
            input: "orders".into(),
            event_time: "event_time".into(),
            kind,
            size_ms: size,
            slide_ms: slide,
            gap_ms: gap,
            group_columns: vec!["order_id".into()],
            partial: false,
            aggs: vec![
                AggSpec {
                    kind: AggKind::Count,
                    input: None,
                    alias: "n".into(),
                },
                AggSpec {
                    kind: AggKind::Sum,
                    input: Some("amount".into()),
                    alias: "amount".into(),
                },
            ],
        }
    }

    fn row(key: i64, t: i64, partition: i32, amount: i64) -> WindowInput {
        WindowInput {
            key: Some(key.to_le_bytes().to_vec()),
            event_time_ms: Some(t),
            partition,
            offset: t,
            values: vec![None, Some(Num::I64(amount))],
        }
    }

    fn count_of(window: &ClosedWindow) -> i64 {
        match window.acc.slots[0] {
            AggState::Count(n) => n,
            _ => panic!("count"),
        }
    }

    fn sum_of(window: &ClosedWindow) -> i128 {
        match window.acc.slots[1] {
            AggState::SumI64(n) => n,
            _ => panic!("sum"),
        }
    }

    #[test]
    fn tumble_one_second_and_one_minute_close_on_watermark() {
        let t0 = Instant::now();
        let mut second = WindowOperator::new(spec(WindowKind::Tumble, 1_000, None, None), 0, 60_000);
        let closed = second
            .apply(&[row(1, 1_500, 0, 10), row(1, 2_400, 0, 1)], t0)
            .unwrap();
        // El evento de 2400 ms sube el watermark a 2400 y cierra [1000, 2000).
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_start, 1_000);
        assert_eq!(closed[0].window_end, 2_000);
        assert_eq!(count_of(&closed[0]), 1);
        assert_eq!(sum_of(&closed[0]), 10);
        assert_eq!(second.open_windows(), 1);

        let mut minute = WindowOperator::new(spec(WindowKind::Tumble, 60_000, None, None), 0, 60_000);
        let closed = minute
            .apply(
                &[row(7, 30_000, 0, 4), row(7, 90_000, 0, 1)],
                t0,
            )
            .unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_start, 0);
        assert_eq!(closed[0].window_end, 60_000);
        assert_eq!(sum_of(&closed[0]), 4);
    }

    #[test]
    fn hop_puts_each_event_in_two_windows() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Hop, 10_000, Some(5_000), None), 0, 60_000);
        // t=12s cae en [5s,15s) y [10s,20s). Un evento a 20s cierra las dos.
        let closed = op
            .apply(&[row(1, 12_000, 0, 3), row(1, 20_000, 0, 1)], t0)
            .unwrap();
        let starts: Vec<i64> = closed.iter().map(|w| w.window_start).collect();
        assert_eq!(starts, vec![5_000, 10_000]);
        assert!(closed.iter().all(|w| sum_of(w) == 3));
    }

    #[test]
    fn session_merges_inside_the_gap_and_not_on_the_boundary() {
        let t0 = Instant::now();
        let mut merged = WindowOperator::new(spec(WindowKind::Session, 0, None, Some(5_000)), 0, 60_000);
        // 0 y 4 se fusionan. t=9 está justo en end+gap y no entra. El watermark
        // de ese evento cierra la sesión fusionada; t=20 cierra la otra.
        let closed = merged
            .apply(
                &[row(1, 0, 0, 2), row(1, 4_000, 0, 3), row(1, 9_000, 0, 1), row(1, 20_000, 0, 1)],
                t0,
            )
            .unwrap();
        assert_eq!(closed.len(), 2);
        assert_eq!(closed[0].window_start, 0);
        assert_eq!(closed[0].window_end, 4_000);
        assert_eq!(count_of(&closed[0]), 2);
        assert_eq!(sum_of(&closed[0]), 5);
        assert_eq!(closed[1].window_start, 9_000);
        assert_eq!(count_of(&closed[1]), 1);

        let mut apart = WindowOperator::new(spec(WindowKind::Session, 0, None, Some(5_000)), 0, 60_000);
        let closed = apart
            .apply(&[row(1, 0, 0, 1), row(1, 5_000, 0, 1), row(1, 30_000, 0, 1)], t0)
            .unwrap();
        assert_eq!(closed.len(), 2);
        assert_eq!(closed[0].window_end, 0);
        assert_eq!(closed[1].window_start, 5_000);
    }

    #[test]
    fn session_bridges_two_open_intervals() {
        let t0 = Instant::now();
        // lag de 5s: el evento a 10s no dispara la sesión de t=0 (fire a los 6s).
        let mut op = WindowOperator::new(spec(WindowKind::Session, 0, None, Some(6_000)), 5_000, 60_000);
        op.apply(&[row(1, 0, 0, 1), row(1, 10_000, 0, 1)], t0)
            .unwrap();
        assert_eq!(op.open_windows(), 2);
        let closed = op
            .apply(&[row(1, 5_000, 0, 1), row(1, 40_000, 0, 1)], t0)
            .unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_start, 0);
        assert_eq!(closed[0].window_end, 10_000);
        assert_eq!(count_of(&closed[0]), 3);
    }

    #[test]
    fn late_event_does_not_move_the_clock() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Tumble, 1_000, None, None), 0, 60_000);
        op.apply(&[row(1, 5_000, 0, 1)], t0).unwrap();
        assert_eq!(op.instance_watermark_ms(), Some(5_000));
        let closed = op.apply(&[row(1, 100, 0, 9)], t0).unwrap();
        assert!(closed.is_empty());
        assert_eq!(op.instance_watermark_ms(), Some(5_000));
        assert_eq!(op.open_windows(), 1);
    }

    #[test]
    fn an_idle_partition_releases_the_minimum() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Tumble, 1_000, None, None), 0, 1_000);
        op.apply(&[row(1, 500, 0, 4)], t0).unwrap();
        assert_eq!(op.open_windows(), 1);
        // La partición 1 llega después: al cumplirse el idle de la 0, el mínimo
        // sube al reloj de la 1 y cierra [0, 1000).
        op.apply(&[row(1, 10_000, 1, 1)], t0 + Duration::from_millis(500))
            .unwrap();
        assert_eq!(op.instance_watermark_ms(), Some(500));
        let closed = op.on_tick(t0 + Duration::from_millis(1_000));
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_end, 1_000);
        assert_eq!(sum_of(&closed[0]), 4);
    }

    #[test]
    fn all_idle_does_not_emit() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Tumble, 1_000, None, None), 0, 1_000);
        op.apply(&[row(1, 100, 0, 1)], t0).unwrap();
        assert_eq!(op.open_windows(), 1);
        let closed = op.on_tick(t0 + Duration::from_secs(30));
        assert!(closed.is_empty());
        assert_eq!(op.open_windows(), 1);
    }

    #[test]
    fn overflow_reverts_the_batch() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Tumble, 60_000, None, None), 0, 60_000);
        op.apply(&[row(1, 1_000, 0, 10)], t0).unwrap();
        let err = op
            .apply(&[row(1, 1_100, 0, i64::MAX)], t0)
            .unwrap_err();
        assert!(matches!(err, WindowFault::Overflow { .. }), "{err}");
        assert_eq!(op.open_windows(), 1);
        let closed = op.apply(&[row(1, 120_000, 0, 1)], t0).unwrap();
        assert_eq!(sum_of(&closed[0]), 10);
    }

    #[test]
    fn restore_rebuilds_the_end_index() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Tumble, 1_000, None, None), 0, 60_000);
        op.apply(&[row(1, 100, 0, 6)], t0).unwrap();
        let state = op.freeze_state();
        let watermark = op.instance_watermark_ms();
        let mut restored = WindowOperator::restore(
            spec(WindowKind::Tumble, 1_000, None, None),
            0,
            60_000,
            state,
            watermark,
        );
        assert_eq!(restored.open_windows(), 1);
        let closed = restored.apply(&[row(1, 2_000, 0, 1)], t0).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(sum_of(&closed[0]), 6);
        assert_eq!(closed[0].window_start, 0);
    }

    #[test]
    fn releasing_a_partition_drops_its_keys_and_does_not_reopen_them() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Tumble, 60_000, None, None), 0, 60_000);
        op.apply(&[row(1, 10_000, 0, 4), row(2, 10_000, 1, 9)], t0)
            .unwrap();
        assert_eq!(op.open_windows(), 2);
        op.release_partition(1);
        assert_eq!(op.open_windows(), 1);
        let closed = op
            .apply(&[row(2, 80_000, 1, 1), row(1, 80_000, 0, 3)], t0)
            .unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(sum_of(&closed[0]), 4);
        assert_eq!(op.open_windows(), 1);
    }

    #[test]
    fn adopting_a_partition_keeps_the_open_window() {
        let t0 = Instant::now();
        let spec_id = spec(WindowKind::Tumble, 60_000, None, None);
        let mut op = WindowOperator::new(spec_id, 0, 60_000);
        op.apply(&[row(1, 10_000, 0, 4)], t0).unwrap();
        let state = op.freeze_state();
        op.release_partition(0);
        assert_eq!(op.open_windows(), 0);
        let closed = op.adopt_partition(0, state.keys, Some(10_000), t0);
        assert!(closed.is_empty());
        assert_eq!(op.open_windows(), 1);
        let closed = op.apply(&[row(1, 20_000, 0, 3)], t0).unwrap();
        assert!(closed.is_empty());
        let closed = op.apply(&[row(1, 70_000, 0, 1)], t0).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(sum_of(&closed[0]), 7);
    }
}

fn merge_acc(dst: &mut Accumulators, src: &Accumulators, row: &WindowInput) -> Result<(), WindowFault> {
    if dst.present.len() != dst.slots.len() {
        dst.present = vec![true; dst.slots.len()];
    }
    let src_present: Vec<bool> = if src.present.len() == src.slots.len() {
        src.present.clone()
    } else {
        vec![true; src.slots.len()]
    };
    for i in 0..dst.slots.len() {
        if !src_present[i] {
            continue;
        }
        if !dst.present[i] {
            dst.slots[i] = src.slots[i].clone();
            dst.present[i] = true;
            continue;
        }
        match (&mut dst.slots[i], &src.slots[i]) {
            (AggState::Count(a), AggState::Count(b)) => *a = a.saturating_add(*b),
            (AggState::SumI64(a), AggState::SumI64(b)) => {
                *a += *b;
                if *a > i128::from(i64::MAX) || *a < i128::from(i64::MIN) {
                    return Err(WindowFault::Overflow {
                        key: row.key.clone().unwrap_or_default(),
                        sum: *a,
                        partition: row.partition,
                        offset: row.offset,
                    });
                }
            }
            (AggState::SumF64(a), AggState::SumF64(b)) => *a += *b,
            (AggState::MinI64(a), AggState::MinI64(b)) => *a = (*a).min(*b),
            (AggState::MinF64(a), AggState::MinF64(b)) => *a = a.min(*b),
            (AggState::MaxI64(a), AggState::MaxI64(b)) => *a = (*a).max(*b),
            (AggState::MaxF64(a), AggState::MaxF64(b)) => *a = a.max(*b),
            (
                AggState::Avg { sum, count },
                AggState::Avg {
                    sum: sb,
                    count: cb,
                },
            ) => {
                *sum += *sb;
                *count += *cb;
            }
            _ => {
                return Err(WindowFault::TypeMix {
                    partition: row.partition,
                    offset: row.offset,
                })
            }
        }
    }
    Ok(())
}
