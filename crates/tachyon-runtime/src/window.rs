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
use smallvec::SmallVec;
use tachyon_sql::{WindowAgg, WindowShape};

use tachyon_core::{
    Accumulators, AggKind, AggSpec, AggState, KeyState, OperatorState, SessionState, WindowKind,
    WindowSpecId,
};

/// Clave de grupo tipada. El caso común (una sola columna `Int64`) no aloca:
/// la clave es el valor mismo, sin encoding ni copias por fila.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WinKey {
    I64(i64),
    /// Encoding binario (Int64 little-endian, Utf8 como u32 LE + bytes) para
    /// claves compuestas o de texto. Mismo encoding de siempre: el sidecar
    /// no cambia de formato.
    Bytes(Vec<u8>),
}

/// Cómo se construye la clave de grupo. Sale del schema del input al
/// arrancar (una sola columna `Int64` -> `I64`); no se infiere por longitud
/// (un `Utf8` de 4 chars también mide 8 bytes codificado).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    I64,
    Bytes,
}

impl WinKey {
    /// El encoding del sidecar (idéntico al `encode_group_key` original).
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        match self {
            WinKey::I64(v) => v.to_le_bytes().to_vec(),
            WinKey::Bytes(b) => b.clone(),
        }
    }

    /// Inverso de `to_bytes` según el `KeyKind` del operador. Una clave de
    /// longitud distinta de 8 bajo `KeyKind::I64` es un checkpoint corrupto.
    fn from_bytes(bytes: Vec<u8>, kind: KeyKind) -> Result<Self, String> {
        match kind {
            KeyKind::I64 => {
                let raw: [u8; 8] = bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| format!("clave i64 de {} bytes (esperaba 8)", bytes.len()))?;
                Ok(WinKey::I64(i64::from_le_bytes(raw)))
            }
            KeyKind::Bytes => Ok(WinKey::Bytes(bytes)),
        }
    }
}

/// Mapa de claves del operador. Hash (foldhash vía hashbrown) en vez de
/// BTreeMap: el orden de iteración no es observable (el cierre ordena por
/// `by_end`; el sidecar se re-ordena al serializar).
type KeyMap = hashbrown::HashMap<WinKey, KeyState>;

/// Una fila ya proyectada. `key` o `event_time_ms` en `None` es un drop
/// determinista: no mueve el reloj ni abre ventana.
#[derive(Debug, Clone)]
pub struct WindowInput {
    pub key: Option<WinKey>,
    pub event_time_ms: Option<i64>,
    pub partition: i32,
    pub offset: i64,
    /// Paralelo a `spec.aggs`. `COUNT(*)` ignora el valor. Inline hasta 4
    /// agregados: sin alloc por fila.
    pub values: SmallVec<[Option<Num>; 4]>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Num {
    I64(i64),
    F64(f64),
}

/// Ventana que acaba de cerrarse. El llamador arma el `RecordBatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClosedWindow {
    pub key: WinKey,
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
                write!(
                    f,
                    "mezcla Int64 y Float64 en partición {partition} offset {offset}"
                )
            }
            WindowFault::NonFinite { partition, offset } => {
                write!(
                    f,
                    "Float64 no finito en partición {partition} offset {offset}"
                )
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
    fields.push(Arc::new(Field::new(
        PARTITION_COLUMN,
        DataType::Int32,
        false,
    )));
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
    names.extend(shape.derived.iter().map(|item| item.alias.clone()));
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
                project: agg.project.clone(),
            })
            .collect(),
    }
}

/// Filas del batch reescrito. La última columna es `_tachyon_partition`.
///
/// Extracción **columnar**: cada columna se downcastea una sola vez y el
/// loop de filas lee valores tipados. Con `KeyKind::I64` la clave se lee
/// directo del array (sin encoding ni alloc por fila).
pub fn inputs_from_batch(
    batch: &RecordBatch,
    shape: &WindowShape,
    key_kind: KeyKind,
) -> anyhow::Result<Vec<WindowInput>> {
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
    let time_col = TimeCol::new(batch.column(time_idx).as_ref())?;
    let mut group_cols = Vec::with_capacity(shape.group_columns.len());
    for name in &shape.group_columns {
        let idx = schema
            .index_of(name)
            .map_err(|_| anyhow::anyhow!("falta la clave '{name}'"))?;
        group_cols.push(GroupCol::new(batch.column(idx).as_ref(), name)?);
    }
    let mut measure_cols = Vec::with_capacity(shape.aggs.len());
    for agg in &shape.aggs {
        measure_cols.push(match &agg.input {
            Some(name) => {
                let idx = schema
                    .index_of(name)
                    .map_err(|_| anyhow::anyhow!("falta la medida '{name}'"))?;
                Some(MeasureCol::new(batch.column(idx).as_ref()))
            }
            None => None,
        });
    }
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let key = match key_kind {
            KeyKind::I64 => match &group_cols[0] {
                GroupCol::I64(values) => {
                    if values.is_null(row) {
                        None
                    } else {
                        Some(WinKey::I64(values.value(row)))
                    }
                }
                // El KeyKind sale del schema al arranque; no puede diferir.
                GroupCol::Str(_) => anyhow::bail!("KeyKind::I64 con clave Utf8"),
            },
            KeyKind::Bytes => encode_group_key(&group_cols, row),
        };
        let event_time_ms = time_col.at(row);
        let mut values = SmallVec::with_capacity(shape.aggs.len());
        for col in &measure_cols {
            values.push(match col {
                Some(col) => col.at(row),
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

/// Columna de event time ya downcasteada (una vez por batch).
enum TimeCol<'a> {
    I64(&'a Int64Array),
    Sec(&'a arrow::array::TimestampSecondArray),
    Ms(&'a arrow::array::TimestampMillisecondArray),
    Us(&'a arrow::array::TimestampMicrosecondArray),
    Ns(&'a arrow::array::TimestampNanosecondArray),
}

impl<'a> TimeCol<'a> {
    fn new(column: &'a dyn Array) -> anyhow::Result<Self> {
        Ok(match column.data_type() {
            DataType::Int64 => TimeCol::I64(column.as_any().downcast_ref::<Int64Array>().unwrap()),
            DataType::Timestamp(TimeUnit::Second, _) => TimeCol::Sec(
                column
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                    .unwrap(),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, _) => TimeCol::Ms(
                column
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .unwrap(),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => TimeCol::Us(
                column
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    .unwrap(),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => TimeCol::Ns(
                column
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .unwrap(),
            ),
            other => anyhow::bail!("event time {other} no es Int64 ni Timestamp"),
        })
    }

    /// Milisegundos de event time, o `None` (null -> drop determinista).
    fn at(&self, row: usize) -> Option<i64> {
        match self {
            TimeCol::I64(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row))
                }
            }
            TimeCol::Sec(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row) * 1_000)
                }
            }
            TimeCol::Ms(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row))
                }
            }
            TimeCol::Us(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row) / 1_000)
                }
            }
            TimeCol::Ns(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row) / 1_000_000)
                }
            }
        }
    }
}

/// Columna de grupo ya downcasteada (una vez por batch).
enum GroupCol<'a> {
    I64(&'a Int64Array),
    Str(&'a StringArray),
}

impl<'a> GroupCol<'a> {
    fn new(column: &'a dyn Array, name: &str) -> anyhow::Result<Self> {
        Ok(match column.data_type() {
            DataType::Int64 => GroupCol::I64(column.as_any().downcast_ref::<Int64Array>().unwrap()),
            DataType::Utf8 => GroupCol::Str(column.as_any().downcast_ref::<StringArray>().unwrap()),
            other => {
                anyhow::bail!("clave de ventana '{name}' con tipo {other}, hace falta Int64 o Utf8")
            }
        })
    }
}

/// Encoding binario de la clave compuesta (mismo formato de siempre).
/// `None` si alguna columna es null en la fila.
fn encode_group_key(cols: &[GroupCol], row: usize) -> Option<WinKey> {
    let mut key = Vec::new();
    for col in cols {
        match col {
            GroupCol::I64(values) => {
                if values.is_null(row) {
                    return None;
                }
                key.extend_from_slice(&values.value(row).to_le_bytes());
            }
            GroupCol::Str(values) => {
                if values.is_null(row) {
                    return None;
                }
                let text = values.value(row);
                key.extend_from_slice(&(text.len() as u32).to_le_bytes());
                key.extend_from_slice(text.as_bytes());
            }
        }
    }
    Some(WinKey::Bytes(key))
}

/// Columna de medida ya downcasteada (una vez por batch).
enum MeasureCol<'a> {
    I64(&'a Int64Array),
    F64(&'a Float64Array),
    /// Otro tipo: solo cuenta presencia (COUNT(col) sobre no numérica).
    Other(&'a dyn Array),
}

impl<'a> MeasureCol<'a> {
    fn new(column: &'a dyn Array) -> Self {
        match column.data_type() {
            DataType::Int64 => {
                MeasureCol::I64(column.as_any().downcast_ref::<Int64Array>().unwrap())
            }
            DataType::Float64 => {
                MeasureCol::F64(column.as_any().downcast_ref::<Float64Array>().unwrap())
            }
            _ => MeasureCol::Other(column),
        }
    }

    fn at(&self, row: usize) -> Option<Num> {
        match self {
            MeasureCol::I64(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(Num::I64(a.value(row)))
                }
            }
            MeasureCol::F64(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(Num::F64(a.value(row)))
                }
            }
            MeasureCol::Other(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(Num::I64(1))
                }
            }
        }
    }
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
    for item in &shape.derived {
        let (field, column) = derived_column(closed, shape, item)?;
        fields.push(field);
        columns.push(column);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| anyhow::anyhow!("batch de ventana: {e}"))
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
                values.push(match &window.key {
                    WinKey::I64(value) => *value,
                    WinKey::Bytes(key) => decode_i64_key(key, index, &shape.group_columns)?,
                });
            }
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for window in closed {
                match &window.key {
                    WinKey::Bytes(key) => {
                        builder.append_value(decode_utf8_key(key, index, &shape.group_columns)?)
                    }
                    WinKey::I64(_) => anyhow::bail!("clave i64 para la columna Utf8 '{name}'"),
                }
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
    let float = agg.kind == AggKind::Avg
        || closed.iter().any(|window| {
            matches!(
                window.acc.slots.get(index),
                Some(
                    AggState::SumF64(_)
                        | AggState::MinF64(_)
                        | AggState::MaxF64(_)
                        | AggState::Avg { .. }
                )
            )
        });
    if agg.kind == AggKind::Count {
        return Ok((
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
        ));
    }
    if float {
        let mut values = closed
            .iter()
            .map(|w| float_slot(&w.acc, index))
            .collect::<Vec<_>>();
        let nullable = fill_f64(&mut values, agg.fill.as_deref())?;
        return Ok((
            Field::new(&agg.alias, DataType::Float64, nullable),
            Arc::new(Float64Array::from(values)),
        ));
    }
    let mut values = closed
        .iter()
        .map(|w| int_slot(&w.acc, index))
        .collect::<Vec<_>>();
    let nullable = fill_i64(&mut values, agg.fill.as_deref())?;
    Ok((
        Field::new(&agg.alias, DataType::Int64, nullable),
        Arc::new(Int64Array::from(values)),
    ))
}

fn fill_i64(values: &mut [Option<i64>], fill: Option<&str>) -> anyhow::Result<bool> {
    let Some(fill) = fill else {
        return Ok(true);
    };
    if fill.contains(['.', 'e', 'E']) {
        anyhow::bail!("COALESCE({fill}) no entra en un agregado entero");
    }
    let parsed: i64 = fill
        .parse()
        .map_err(|_| anyhow::anyhow!("COALESCE '{fill}' no es un entero"))?;
    for value in values.iter_mut() {
        if value.is_none() {
            *value = Some(parsed);
        }
    }
    Ok(false)
}

fn fill_f64(values: &mut [Option<f64>], fill: Option<&str>) -> anyhow::Result<bool> {
    let Some(fill) = fill else {
        return Ok(true);
    };
    let parsed: f64 = fill
        .parse()
        .map_err(|_| anyhow::anyhow!("COALESCE '{fill}' no es un número"))?;
    if !parsed.is_finite() {
        anyhow::bail!("COALESCE '{fill}' no es finito");
    }
    for value in values.iter_mut() {
        if value.is_none() {
            *value = Some(parsed);
        }
    }
    Ok(false)
}

fn derived_column(
    closed: &[ClosedWindow],
    shape: &WindowShape,
    item: &tachyon_sql::WindowDerived,
) -> anyhow::Result<(Field, Arc<dyn Array>)> {
    let indexes: Vec<usize> = item
        .terms
        .iter()
        .map(|term| {
            shape
                .aggs
                .iter()
                .position(|agg| agg.alias == *term)
                .ok_or_else(|| anyhow::anyhow!("'{}' no es un agregado", term))
        })
        .collect::<anyhow::Result<_>>()?;
    let float = indexes.iter().any(|&index| {
        shape.aggs[index].kind == AggKind::Avg
            || closed.iter().any(|window| {
                matches!(
                    window.acc.slots.get(index),
                    Some(
                        AggState::SumF64(_)
                            | AggState::MinF64(_)
                            | AggState::MaxF64(_)
                            | AggState::Avg { .. }
                    )
                )
            })
            || shape.aggs[index]
                .fill
                .as_deref()
                .is_some_and(|fill| fill.contains(['.', 'e', 'E']))
    });
    if float {
        let mut values = Vec::with_capacity(closed.len());
        for window in closed {
            values.push(sum_terms_f64(window, shape, &indexes)?);
        }
        let nullable = values.iter().any(Option::is_none);
        return Ok((
            Field::new(&item.alias, DataType::Float64, nullable),
            Arc::new(Float64Array::from(values)),
        ));
    }
    let mut values = Vec::with_capacity(closed.len());
    for window in closed {
        values.push(sum_terms_i64(window, shape, &indexes)?);
    }
    let nullable = values.iter().any(Option::is_none);
    Ok((
        Field::new(&item.alias, DataType::Int64, nullable),
        Arc::new(Int64Array::from(values)),
    ))
}

fn filled_i64(
    window: &ClosedWindow,
    shape: &WindowShape,
    index: usize,
) -> anyhow::Result<Option<i64>> {
    if let Some(value) = int_slot(&window.acc, index) {
        return Ok(Some(value));
    }
    match shape.aggs[index].fill.as_deref() {
        None => Ok(None),
        Some(fill) => {
            if fill.contains(['.', 'e', 'E']) {
                anyhow::bail!("COALESCE({fill}) no entra en un agregado entero");
            }
            fill.parse::<i64>()
                .map(Some)
                .map_err(|_| anyhow::anyhow!("COALESCE '{fill}' no es un entero"))
        }
    }
}

fn filled_f64(
    window: &ClosedWindow,
    shape: &WindowShape,
    index: usize,
) -> anyhow::Result<Option<f64>> {
    if let Some(value) = float_slot(&window.acc, index) {
        return Ok(Some(value));
    }
    if int_slot(&window.acc, index).is_some() {
        return Ok(int_slot(&window.acc, index).map(|value| value as f64));
    }
    match shape.aggs[index].fill.as_deref() {
        None => Ok(None),
        Some(fill) => fill
            .parse::<f64>()
            .map(Some)
            .map_err(|_| anyhow::anyhow!("COALESCE '{fill}' no es un número")),
    }
}

fn sum_terms_i64(
    window: &ClosedWindow,
    shape: &WindowShape,
    indexes: &[usize],
) -> anyhow::Result<Option<i64>> {
    let mut total: i64 = 0;
    for index in indexes {
        let Some(value) = filled_i64(window, shape, *index)? else {
            return Ok(None);
        };
        total = total
            .checked_add(value)
            .ok_or_else(|| anyhow::anyhow!("la suma de la ventana no entra en i64"))?;
    }
    Ok(Some(total))
}

fn sum_terms_f64(
    window: &ClosedWindow,
    shape: &WindowShape,
    indexes: &[usize],
) -> anyhow::Result<Option<f64>> {
    let mut total = 0.0;
    for index in indexes {
        let Some(value) = filled_f64(window, shape, *index)? else {
            return Ok(None);
        };
        total += value;
    }
    if !total.is_finite() {
        anyhow::bail!("la suma de la ventana no es finita");
    }
    Ok(Some(total))
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

/// El mínimo de las particiones de un operador (ver `local_min`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalMin {
    /// Una partición con backlog todavía no trajo eventos: nada puede cerrar.
    Hold,
    /// Ninguna partición activa: este operador no frena a nadie.
    Idle,
    At(i64),
}

#[derive(Debug)]
pub struct WindowOperator {
    spec: WindowSpecId,
    lag_ms: i64,
    idle: std::time::Duration,
    key_kind: KeyKind,
    keys: KeyMap,
    /// Fin de disparo (el `end` de tumble/hop, `end + gap` de session) → claves.
    by_end: BTreeMap<i64, BTreeSet<WinKey>>,
    partitions: BTreeMap<i32, PartitionClock>,
    /// Particiones que este proceso ya soltó. Sus filas no reabren ventanas:
    /// las vuelve a leer quien las tenga ahora.
    released: std::collections::HashSet<i32>,
    instance_watermark_ms: Option<i64>,
    /// Particiones con log sin leer o sin aplicar (ver `set_backlog`).
    backlog: std::collections::HashSet<i32>,
    /// El mayor |SUM| entero que llegó a tener una ventana. Con la suma de
    /// |valores| de un batch acota lo que puede sumar cualquier ventana, y
    /// deja saber antes de aplicar si el batch puede desbordar i64.
    max_abs_sum: u128,
    /// Partición que define hoy el mínimo del watermark. Si sube otra, el
    /// mínimo no cambia: no hace falta recalcularlo por cada fila.
    min_holder: Option<i32>,
    /// Techo del watermark propio (ver `set_cap`). `None` = sin techo.
    cap: Option<i64>,
    /// Falla de un batch aplicado sin undo (ver `apply`). El estado quedó a
    /// medio aplicar: toda llamada siguiente devuelve esta falla.
    poisoned: Option<WindowFault>,
    /// Un snapshot se aplica entero contra el watermark anterior. El mínimo
    /// sube una sola vez, al terminar el snapshot.
    defer_close: bool,
}

/// Rollback de un batch fallido: los originales de las claves tocadas y los
/// movimientos del índice por fin. Cuesta O(claves tocadas por el batch), no
/// O(estado total) como el clone completo anterior.
#[derive(Default)]
struct BatchUndo {
    /// `false`: el batch va sin undo (ver `apply`) y nada de lo de abajo
    /// salvo `closed` se registra.
    track: bool,
    watermark: Option<i64>,
    clocks: BTreeMap<i32, PartitionClock>,
    /// Original de cada clave tocada (`None` = no existía antes del batch).
    saved: hashbrown::HashMap<WinKey, Option<KeyState>>,
    /// Entradas agregadas a `by_end` durante el batch.
    inserted_ends: Vec<(i64, WinKey)>,
    /// Entradas quitadas de `by_end` durante el batch (merge de sesiones).
    removed_ends: Vec<(i64, WinKey)>,
    /// Ventanas cerradas durante el batch (su KeyState ya se restaura vía
    /// `saved`; acá solo hace falta re-indexarlas).
    closed: Vec<ClosedWindow>,
}

impl BatchUndo {
    /// Guarda el original de `key` la primera vez que el batch la toca.
    fn save_existing(&mut self, keys: &KeyMap, key: &WinKey) {
        if self.track && !self.saved.contains_key(key) {
            self.saved.insert(key.clone(), keys.get(key).cloned());
        }
    }
}

/// La entrada de `key` en el mapa, creándola vacía si no existía. La primera
/// vez que el batch toca la clave guarda su original para el rollback.
fn state_for<'a>(
    keys: &'a mut KeyMap,
    saved: &mut hashbrown::HashMap<WinKey, Option<KeyState>>,
    track: bool,
    key: &WinKey,
    partition: i32,
) -> &'a mut KeyState {
    if track && !saved.contains_key(key) {
        saved.insert(key.clone(), keys.get(key).cloned());
    }
    keys.entry(key.clone())
        .or_insert_with(|| empty_key(partition))
}

impl WindowOperator {
    pub fn new(spec: WindowSpecId, lag_ms: i64, idle_ms: i64, key_kind: KeyKind) -> Self {
        Self {
            spec,
            lag_ms,
            idle: std::time::Duration::from_millis(idle_ms.max(0) as u64),
            key_kind,
            keys: KeyMap::new(),
            by_end: BTreeMap::new(),
            partitions: BTreeMap::new(),
            released: std::collections::HashSet::new(),
            instance_watermark_ms: None,
            backlog: std::collections::HashSet::new(),
            max_abs_sum: 0,
            min_holder: None,
            cap: None,
            poisoned: None,
            defer_close: false,
        }
    }

    /// Particiones de este proceso que todavía tienen log por leer o por
    /// aplicar. Ninguna queda idle por reloj: frenan el watermark con su
    /// event time aunque haga rato que no traen un evento. Una que todavía
    /// no trajo ninguno lo frena del todo. Sin esto, drenar un backlog con
    /// particiones a distinto ritmo (o una que cambia de consumidor) dejaba
    /// atrás a las lentas y sus eventos se descartaban por tardíos.
    pub fn set_backlog(&mut self, backlog: std::collections::HashSet<i32>) {
        if backlog != self.backlog {
            self.min_holder = None;
        }
        self.backlog = backlog;
    }

    /// Restaura el mapa del sidecar y rearma `by_end`. Las claves se decodifican
    /// según `key_kind` (el encoding del sidecar no cambió).
    pub fn restore(
        spec: WindowSpecId,
        lag_ms: i64,
        idle_ms: i64,
        state: OperatorState,
        instance_watermark_ms: Option<i64>,
        key_kind: KeyKind,
    ) -> Result<Self, String> {
        let mut keys = KeyMap::with_capacity(state.keys.len());
        for (key, value) in state.keys {
            keys.insert(WinKey::from_bytes(key, key_kind)?, value);
        }
        let mut op = Self::new(spec, lag_ms, idle_ms, key_kind);
        op.keys = keys;
        op.instance_watermark_ms = instance_watermark_ms;
        op.reindex();
        Ok(op)
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
    /// La falla que dejó al operador envenenado, si la hay.
    pub(crate) fn poison(&self) -> Option<&WindowFault> {
        self.poisoned.as_ref()
    }

    pub(crate) fn poison_with(&mut self, fault: WindowFault) {
        self.poisoned.get_or_insert(fault);
    }

    pub fn release_partition(&mut self, partition: i32) {
        self.released.insert(partition);
        self.partitions.remove(&partition);
        self.keys.retain(|_, state| state.partition != partition);
        self.reindex();
    }

    /// Incorpora la ficha commiteada de una partición que este proceso no tenía.
    /// Puede cerrar ventanas si el watermark de esa partición ya las pasó.
    /// Las claves se decodifican según el `key_kind` del operador; una ficha
    /// con claves de otra forma es un error (estado de otro pipeline).
    pub fn adopt_partition(
        &mut self,
        partition: i32,
        keys: BTreeMap<Vec<u8>, KeyState>,
        max_event_time_ms: Option<i64>,
        now: Instant,
    ) -> Result<Vec<ClosedWindow>, String> {
        if let Some(fault) = &self.poisoned {
            return Err(fault.to_string());
        }
        self.released.remove(&partition);
        for (key, mut state) in keys {
            state.partition = partition;
            self.keys
                .insert(WinKey::from_bytes(key, self.key_kind)?, state);
        }
        self.partitions.insert(
            partition,
            PartitionClock {
                max_event_time_ms,
                last_on_time: Some(now),
            },
        );
        self.reindex();
        let mut undo = BatchUndo::default();
        self.raise_and_close(now, &mut undo);
        Ok(undo.closed)
    }

    pub fn open_windows(&self) -> usize {
        self.keys
            .values()
            .map(|state| state.windows.len() + state.sessions.len())
            .sum()
    }

    /// Aplica el batch en orden. Si una fila falla, el operador vuelve al
    /// estado de antes del batch y no devuelve las ventanas que ese batch
    /// hubiera cerrado. El rollback es un undo log sobre las claves tocadas
    /// (antes: un clone del estado completo por batch).
    ///
    /// Guardar los originales cuesta un clone de cada clave tocada (su mapa
    /// de ventanas entero) por batch: era la mitad del tiempo del operador.
    /// Si antes de aplicar se puede probar que ninguna fila del batch puede
    /// fallar (cantidad de valores, `f64` finitos y ningún SUM que pueda
    /// salirse de i64), el batch va sin undo. La única falla que queda
    /// posible ahí (un tipo que no coincide con el del acumulador) deja el
    /// operador envenenado: esa falla vuelve en cada llamada siguiente.
    /// Aplica el snapshot entero y recién después mueve el watermark.
    ///
    /// Una fila vieja que viene después de una nueva, dentro del mismo
    /// snapshot, se agrega. Una fila anterior al watermark del snapshot
    /// previo se descarta.
    pub fn apply_epoch(
        &mut self,
        rows: &[WindowInput],
        now: Instant,
    ) -> Result<Vec<ClosedWindow>, WindowFault> {
        self.defer_close = true;
        let applied = self.apply(rows, now);
        self.defer_close = false;
        let mut closed = applied?;
        let mut undo = BatchUndo::default();
        self.raise_and_close(now, &mut undo);
        closed.extend(undo.closed);
        Ok(closed)
    }

    pub fn apply(
        &mut self,
        rows: &[WindowInput],
        now: Instant,
    ) -> Result<Vec<ClosedWindow>, WindowFault> {
        if let Some(fault) = &self.poisoned {
            return Err(fault.clone());
        }
        if self.cannot_fail(rows) {
            let mut undo = BatchUndo::default();
            return match self.apply_inner(rows, now, &mut undo) {
                Ok(()) => Ok(undo.closed),
                Err(fault) => {
                    self.poisoned = Some(fault.clone());
                    Err(fault)
                }
            };
        }
        let mut undo = BatchUndo {
            track: true,
            watermark: self.instance_watermark_ms,
            clocks: self.partitions.clone(),
            ..Default::default()
        };
        match self.apply_inner(rows, now, &mut undo) {
            Ok(()) => Ok(undo.closed),
            Err(err) => {
                self.rollback(undo);
                Err(err)
            }
        }
    }

    /// Ninguna fila de `rows` puede devolver una falla al aplicarse. SESSION
    /// fusiona sesiones (una suma puede juntar dos ventanas): siempre con undo.
    fn cannot_fail(&self, rows: &[WindowInput]) -> bool {
        if !matches!(self.spec.kind, WindowKind::Tumble | WindowKind::Hop) {
            return false;
        }
        let aggs = &self.spec.aggs;
        let mut abs_total: u128 = 0;
        for row in rows {
            if row.values.len() != aggs.len() {
                return false;
            }
            for (agg, value) in aggs.iter().zip(row.values.iter()) {
                match value {
                    Some(Num::F64(v)) if !v.is_finite() => return false,
                    Some(Num::I64(v)) if agg.kind == AggKind::Sum => {
                        abs_total += u128::from(v.unsigned_abs());
                    }
                    _ => {}
                }
            }
        }
        // Una ventana de tumble/hop recibe cada fila a lo sumo una vez.
        self.max_abs_sum + abs_total <= i64::MAX as u128
    }

    /// Deshace un batch fallido: restaura los originales de las claves
    /// tocadas y revierte los movimientos del índice por fin. Las entradas
    /// que no se pueden reconstruir exactamente quedan como entradas
    /// "stale" en `by_end`, que son inertes: el cierre las salta porque la
    /// clave ya no tiene la ventana correspondiente.
    fn rollback(&mut self, undo: BatchUndo) {
        self.min_holder = None;
        self.instance_watermark_ms = undo.watermark;
        self.partitions = undo.clocks;
        for (end, key) in undo.inserted_ends {
            unindex(&mut self.by_end, end, &key);
        }
        for (end, key) in undo.removed_ends {
            self.by_end.entry(end).or_default().insert(key);
        }
        for (key, original) in undo.saved {
            match original {
                Some(state) => {
                    self.keys.insert(key, state);
                }
                None => {
                    self.keys.remove(&key);
                }
            }
        }
        // Las ventanas cerradas durante el batch vuelven al índice. Su
        // KeyState ya se restauró arriba (toda clave cerrada pasó por
        // `save_existing` antes de removerla).
        let gap = self.spec.gap_ms.unwrap_or(0);
        for closed in undo.closed {
            let fire = match self.spec.kind {
                WindowKind::Session => closed.window_end.saturating_add(gap),
                WindowKind::Tumble | WindowKind::Hop => closed.window_end,
            };
            self.by_end.entry(fire).or_default().insert(closed.key);
        }
    }

    /// Recalcula particiones idle. Puede cerrar ventanas si el mínimo sube.
    /// No cierra por el paso del tiempo de pared cuando todas están idle.
    pub fn on_tick(&mut self, now: Instant) -> Vec<ClosedWindow> {
        if self.poisoned.is_some() {
            return Vec::new();
        }
        let mut undo = BatchUndo::default();
        self.raise_and_close(now, &mut undo);
        undo.closed
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

    /// El mapa por clave, listo para el sidecar. `by_end` no entra. El
    /// encoding de las claves es el de siempre: el formato del sidecar no
    /// cambia.
    pub fn freeze_state(&self) -> OperatorState {
        OperatorState {
            keys: self
                .keys
                .iter()
                .map(|(key, state)| (key.to_bytes(), state.clone()))
                .collect(),
        }
    }

    fn apply_inner(
        &mut self,
        rows: &[WindowInput],
        now: Instant,
        undo: &mut BatchUndo,
    ) -> Result<(), WindowFault> {
        for row in rows {
            self.apply_row(row, now, undo)?;
        }
        Ok(())
    }

    fn apply_row(
        &mut self,
        row: &WindowInput,
        now: Instant,
        undo: &mut BatchUndo,
    ) -> Result<(), WindowFault> {
        let (Some(key), Some(t)) = (&row.key, row.event_time_ms) else {
            return Ok(());
        };
        if self.released.contains(&row.partition) {
            return Ok(());
        }
        if self
            .instance_watermark_ms
            .is_some_and(|watermark| t < watermark)
        {
            return Ok(());
        }
        let clock = self
            .partitions
            .entry(row.partition)
            .or_insert(PartitionClock {
                max_event_time_ms: None,
                last_on_time: None,
            });
        let prev_max = clock.max_event_time_ms;
        let raised = prev_max.is_none_or(|max| t > max);
        clock.max_event_time_ms = Some(prev_max.map_or(t, |max| max.max(t)));
        clock.last_on_time = Some(now);
        self.assign(key, t, row, undo)?;
        if self.defer_close {
            return Ok(());
        }
        // Solo un máximo que sube puede subir el watermark (el mínimo sobre
        // las particiones activas es monótono con los máximos, y el de
        // instancia es `max` consigo mismo). Si esta fila no movió el de su
        // partición, no hay nada nuevo que cerrar: se evita el split_off
        // por fila. Re-activar una partición idle solo puede BAJAR el
        // mínimo, y el watermark no retrocede.
        //
        // Además, solo la partición que hoy define el mínimo puede subirlo.
        // Si subió otra, el mínimo es el mismo, salvo que la que lo definía
        // haya quedado idle (entonces se recalcula).
        if raised && self.min_may_move(row.partition, now) {
            self.raise_and_close(now, undo);
        }
        Ok(())
    }

    fn assign(
        &mut self,
        key: &WinKey,
        t: i64,
        row: &WindowInput,
        undo: &mut BatchUndo,
    ) -> Result<(), WindowFault> {
        match self.spec.kind {
            WindowKind::Tumble => {
                let start = align_down(t, self.spec.size_ms);
                self.touch_fixed(key, start, start + self.spec.size_ms, row, undo)
            }
            WindowKind::Hop => {
                let slide = self.spec.slide_ms.expect("hop con slide");
                let size = self.spec.size_ms;
                let mut start = align_down(t, slide);
                let mut guard = 0;
                while start > t - size {
                    self.touch_fixed(key, start, start + size, row, undo)?;
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
                self.touch_session(key, t, gap, row, undo)
            }
        }
    }

    fn touch_fixed(
        &mut self,
        key: &WinKey,
        start: i64,
        end: i64,
        row: &WindowInput,
        undo: &mut BatchUndo,
    ) -> Result<(), WindowFault> {
        let state = state_for(
            &mut self.keys,
            &mut undo.saved,
            undo.track,
            key,
            row.partition,
        );
        state.partition = row.partition;
        let is_new = !state.windows.contains_key(&start);
        let acc = state
            .windows
            .entry(start)
            .or_insert_with(|| fresh_acc(&self.spec.aggs));
        absorb(acc, &self.spec.aggs, &row.values, row)?;
        for slot in &acc.slots {
            if let AggState::SumI64(sum) = slot {
                self.max_abs_sum = self.max_abs_sum.max(sum.unsigned_abs());
            }
        }
        if is_new {
            self.by_end.entry(end).or_default().insert(key.clone());
            if undo.track {
                undo.inserted_ends.push((end, key.clone()));
            }
        }
        Ok(())
    }

    fn touch_session(
        &mut self,
        key: &WinKey,
        t: i64,
        gap: i64,
        row: &WindowInput,
        undo: &mut BatchUndo,
    ) -> Result<(), WindowFault> {
        let aggs = &self.spec.aggs;
        let state = state_for(
            &mut self.keys,
            &mut undo.saved,
            undo.track,
            key,
            row.partition,
        );
        state.partition = row.partition;
        let hit: Vec<usize> = state
            .sessions
            .iter()
            .enumerate()
            .filter(|(_, session)| intersects(session, t, gap))
            .map(|(index, _)| index)
            .collect();
        if hit.is_empty() {
            let mut acc = fresh_acc(aggs);
            absorb(&mut acc, aggs, &row.values, row)?;
            insert_sorted(
                &mut state.sessions,
                SessionState {
                    start_ms: t,
                    end_ms: t,
                    acc,
                },
            );
            let fire = t.saturating_add(gap);
            self.by_end.entry(fire).or_default().insert(key.clone());
            undo.inserted_ends.push((fire, key.clone()));
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
        let mut contribution = fresh_acc(aggs);
        absorb(&mut contribution, aggs, &row.values, row)?;
        merge_acc(&mut merged.acc, &contribution, row)?;
        merged.start_ms = merged.start_ms.min(t);
        merged.end_ms = merged.end_ms.max(t);
        let new_fire = merged.end_ms.saturating_add(gap);
        insert_sorted(&mut state.sessions, merged);
        for fire in old_fires {
            unindex(&mut self.by_end, fire, key);
            undo.removed_ends.push((fire, key.clone()));
        }
        self.by_end.entry(new_fire).or_default().insert(key.clone());
        undo.inserted_ends.push((new_fire, key.clone()));
        Ok(())
    }

    fn min_may_move(&self, raised: i32, now: Instant) -> bool {
        let Some(holder) = self.min_holder else {
            return true;
        };
        if holder == raised {
            return true;
        }
        !self.backlog.contains(&holder)
            && self
                .partitions
                .get(&holder)
                .is_none_or(|clock| !clock.is_active(now, self.idle))
    }

    /// El mínimo de este operador sobre sus particiones activas o con
    /// backlog (ya restado el lag). Recuerda qué partición lo define.
    pub(crate) fn local_min(&mut self, now: Instant) -> LocalMin {
        let unseen_backlog = self.backlog.iter().any(|partition| {
            !self.released.contains(partition)
                && self
                    .partitions
                    .get(partition)
                    .is_none_or(|clock| clock.max_event_time_ms.is_none())
        });
        if unseen_backlog {
            self.min_holder = None;
            return LocalMin::Hold;
        }
        let min = self
            .partitions
            .iter()
            .filter(|(id, clock)| self.backlog.contains(id) || clock.is_active(now, self.idle))
            .filter_map(|(id, clock)| clock.max_event_time_ms.map(|max| (max, *id)))
            .map(|(max, id)| (max.saturating_sub(self.lag_ms), id))
            .min();
        self.min_holder = min.map(|(_, id)| id);
        match min {
            Some((value, _)) => LocalMin::At(value),
            None => LocalMin::Idle,
        }
    }

    /// Techo para el watermark propio: el de la instancia cuando este
    /// operador es un shard (ver `ShardedWindow`).
    pub(crate) fn set_cap(&mut self, cap: Option<i64>) {
        self.cap = cap;
    }

    /// Sube el watermark a `watermark` (el de la instancia) y cierra lo vencido.
    pub(crate) fn raise_to(&mut self, watermark: i64) -> Vec<ClosedWindow> {
        if self.poisoned.is_some() {
            return Vec::new();
        }
        self.instance_watermark_ms = Some(
            self.instance_watermark_ms
                .map_or(watermark, |w| w.max(watermark)),
        );
        let mut undo = BatchUndo::default();
        self.close_until(self.instance_watermark_ms.expect("watermark"), &mut undo);
        undo.closed
    }

    fn raise_and_close(&mut self, now: Instant, undo: &mut BatchUndo) {
        let min = match self.local_min(now) {
            LocalMin::At(value) => Some(match self.cap {
                Some(cap) => value.min(cap),
                None => value,
            }),
            LocalMin::Hold | LocalMin::Idle => None,
        };
        if let Some(min) = min {
            self.instance_watermark_ms = Some(match self.instance_watermark_ms {
                Some(current) => current.max(min),
                None => min,
            });
        }
        let Some(watermark) = self.instance_watermark_ms else {
            return;
        };
        self.close_until(watermark, undo)
    }

    fn close_until(&mut self, watermark: i64, undo: &mut BatchUndo) {
        // Nada vencido: O(1) sin tocar el árbol. Antes se hacía un
        // `split_off` por fila aunque no hubiera nada que cerrar.
        if watermark != i64::MAX
            && self
                .by_end
                .first_key_value()
                .is_none_or(|(end, _)| *end > watermark)
        {
            return;
        }
        let later = if watermark == i64::MAX {
            BTreeMap::new()
        } else {
            self.by_end.split_off(&(watermark + 1))
        };
        let due = std::mem::replace(&mut self.by_end, later);
        for (fire_at, keys) in due {
            for key in keys {
                // Toda clave que el cierre remueve guarda su original: el
                // rollback la restaura con sus ventanas incluidas.
                undo.save_existing(&self.keys, &key);
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
                            undo.closed.push(ClosedWindow {
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
                            undo.closed.push(ClosedWindow {
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
    }

    fn reindex(&mut self) {
        self.by_end.clear();
        // Estado que viene de un sidecar o de una ficha: su SUM más grande
        // entra en la cota de `cannot_fail`.
        for state in self.keys.values() {
            let accs = state
                .windows
                .values()
                .chain(state.sessions.iter().map(|session| &session.acc));
            for acc in accs {
                for slot in &acc.slots {
                    if let AggState::SumI64(sum) = slot {
                        self.max_abs_sum = self.max_abs_sum.max(sum.unsigned_abs());
                    }
                }
            }
        }
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

fn unindex(by_end: &mut BTreeMap<i64, BTreeSet<WinKey>>, fire: i64, key: &WinKey) {
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
                apply_num(&mut acc.slots[i], &mut acc.present[i], agg.kind, num, row)?;
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
                    key: row.key.as_ref().map(WinKey::to_bytes).unwrap_or_default(),
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
                    project: None,
                },
                AggSpec {
                    kind: AggKind::Sum,
                    input: Some("amount".into()),
                    alias: "amount".into(),
                    project: None,
                },
            ],
        }
    }

    fn row(key: i64, t: i64, partition: i32, amount: i64) -> WindowInput {
        WindowInput {
            key: Some(WinKey::I64(key)),
            event_time_ms: Some(t),
            partition,
            offset: t,
            values: vec![None, Some(Num::I64(amount))].into(),
        }
    }

    #[test]
    fn an_epoch_keeps_an_older_event_that_arrives_after_a_newer_one() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(
            spec(WindowKind::Tumble, 60_000, None, None),
            10_000,
            60_000,
            KeyKind::I64,
        );
        // 70s subiría el watermark a 60s si se aplicara fila a fila, y el
        // evento de 1s quedaría tarde. En el mismo snapshot entran los dos.
        let closed = op
            .apply_epoch(&[row(1, 70_000, 0, 3), row(1, 1_000, 0, 2)], t0)
            .unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_start, 0);
        assert_eq!(sum_of(&closed[0]), 2);
        // El de 70s sigue abierto. Un event time anterior al watermark nuevo se tira.
        let closed = op
            .apply_epoch(&[row(1, 2_000, 0, 9), row(1, 130_000, 0, 1)], t0)
            .unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_start, 60_000);
        assert_eq!(sum_of(&closed[0]), 3);
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
        let mut second = WindowOperator::new(
            spec(WindowKind::Tumble, 1_000, None, None),
            0,
            60_000,
            KeyKind::I64,
        );
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

        let mut minute = WindowOperator::new(
            spec(WindowKind::Tumble, 60_000, None, None),
            0,
            60_000,
            KeyKind::I64,
        );
        let closed = minute
            .apply(&[row(7, 30_000, 0, 4), row(7, 90_000, 0, 1)], t0)
            .unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_start, 0);
        assert_eq!(closed[0].window_end, 60_000);
        assert_eq!(sum_of(&closed[0]), 4);
    }

    #[test]
    fn hop_puts_each_event_in_two_windows() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(
            spec(WindowKind::Hop, 10_000, Some(5_000), None),
            0,
            60_000,
            KeyKind::I64,
        );
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
        let mut merged = WindowOperator::new(
            spec(WindowKind::Session, 0, None, Some(5_000)),
            0,
            60_000,
            KeyKind::I64,
        );
        // 0 y 4 se fusionan. t=9 está justo en end+gap y no entra. El watermark
        // de ese evento cierra la sesión fusionada; t=20 cierra la otra.
        let closed = merged
            .apply(
                &[
                    row(1, 0, 0, 2),
                    row(1, 4_000, 0, 3),
                    row(1, 9_000, 0, 1),
                    row(1, 20_000, 0, 1),
                ],
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

        let mut apart = WindowOperator::new(
            spec(WindowKind::Session, 0, None, Some(5_000)),
            0,
            60_000,
            KeyKind::I64,
        );
        let closed = apart
            .apply(
                &[row(1, 0, 0, 1), row(1, 5_000, 0, 1), row(1, 30_000, 0, 1)],
                t0,
            )
            .unwrap();
        assert_eq!(closed.len(), 2);
        assert_eq!(closed[0].window_end, 0);
        assert_eq!(closed[1].window_start, 5_000);
    }

    #[test]
    fn session_bridges_two_open_intervals() {
        let t0 = Instant::now();
        // lag de 5s: el evento a 10s no dispara la sesión de t=0 (fire a los 6s).
        let mut op = WindowOperator::new(
            spec(WindowKind::Session, 0, None, Some(6_000)),
            5_000,
            60_000,
            KeyKind::I64,
        );
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
        let mut op = WindowOperator::new(
            spec(WindowKind::Tumble, 1_000, None, None),
            0,
            60_000,
            KeyKind::I64,
        );
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
        let mut op = WindowOperator::new(
            spec(WindowKind::Tumble, 1_000, None, None),
            0,
            1_000,
            KeyKind::I64,
        );
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
    fn a_partition_with_backlog_is_never_idle() {
        // Drenando un backlog: la 0 va adelantada, la 1 atrasada y sin traer
        // nada hace más que `idle`. Sigue teniendo log sin leer, así que
        // frena el watermark y su evento de 1500 cae en su ventana.
        let t0 = Instant::now();
        let mut op = WindowOperator::new(
            spec(WindowKind::Tumble, 1_000, None, None),
            0,
            1_000,
            KeyKind::I64,
        );
        op.set_backlog([0, 1].into_iter().collect());
        op.apply(&[row(1, 1_200, 1, 1)], t0).unwrap();
        op.apply(&[row(2, 30_000, 0, 1)], t0 + Duration::from_millis(100))
            .unwrap();
        let later = t0 + Duration::from_secs(5);
        assert!(op.on_tick(later).is_empty());
        assert_eq!(op.instance_watermark_ms(), Some(1_200));
        let closed = op
            .apply(&[row(1, 1_500, 1, 1), row(1, 2_100, 1, 1)], later)
            .unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_end, 2_000);
        assert_eq!(
            sum_of(&closed[0]),
            2,
            "el 1500 de la partición lenta no se pierde"
        );
    }

    #[test]
    fn an_unseen_partition_with_backlog_holds_the_watermark() {
        // La 1 está asignada pero su consumidor todavía no trajo nada (p. ej.
        // la partición cambió de dueño en un rebalance): nada cierra.
        let t0 = Instant::now();
        let mut op = WindowOperator::new(
            spec(WindowKind::Tumble, 1_000, None, None),
            0,
            1_000,
            KeyKind::I64,
        );
        op.set_backlog([0, 1].into_iter().collect());
        let closed = op
            .apply(&[row(1, 500, 0, 1), row(1, 9_000, 0, 1)], t0)
            .unwrap();
        assert!(closed.is_empty());
        assert_eq!(op.instance_watermark_ms(), None);
        let closed = op.apply(&[row(2, 700, 1, 1)], t0).unwrap();
        assert!(
            closed.is_empty(),
            "la 1 va por 700: [0, 1000) sigue abierta"
        );
        let closed = op.apply(&[row(2, 9_500, 1, 1)], t0).unwrap();
        assert_eq!(
            closed.len(),
            2,
            "con las dos en 9000+ cierran las dos claves de [0, 1000)"
        );
    }

    #[test]
    fn a_caught_up_partition_goes_idle_again() {
        // Sin backlog vuelve la regla de siempre: la 0 lleva `idle` sin
        // eventos a tiempo y deja de frenar.
        let t0 = Instant::now();
        let mut op = WindowOperator::new(
            spec(WindowKind::Tumble, 1_000, None, None),
            0,
            1_000,
            KeyKind::I64,
        );
        op.set_backlog([0].into_iter().collect());
        op.apply(&[row(1, 500, 0, 4)], t0).unwrap();
        op.apply(&[row(1, 10_000, 1, 1)], t0 + Duration::from_millis(500))
            .unwrap();
        let at = t0 + Duration::from_millis(1_200);
        assert!(op.on_tick(at).is_empty(), "con backlog la 0 frena");
        op.set_backlog(Default::default());
        let closed = op.on_tick(at);
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_end, 1_000);
    }

    #[test]
    fn all_idle_does_not_emit() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(
            spec(WindowKind::Tumble, 1_000, None, None),
            0,
            1_000,
            KeyKind::I64,
        );
        op.apply(&[row(1, 100, 0, 1)], t0).unwrap();
        assert_eq!(op.open_windows(), 1);
        let closed = op.on_tick(t0 + Duration::from_secs(30));
        assert!(closed.is_empty());
        assert_eq!(op.open_windows(), 1);
    }

    #[test]
    fn a_fault_without_undo_poisons_the_operator() {
        // Un SUM entero que después recibe un f64: el batch pasa el
        // pre-chequeo (sin undo) y falla en el acumulador. El operador no
        // sigue con un estado a medias: la falla vuelve en cada llamada.
        let t0 = Instant::now();
        let mut op = WindowOperator::new(
            spec(WindowKind::Tumble, 60_000, None, None),
            0,
            60_000,
            KeyKind::I64,
        );
        op.apply(&[row(1, 1_000, 0, 10)], t0).unwrap();
        let mut mixed = row(1, 1_100, 0, 0);
        mixed.values[1] = Some(Num::F64(1.5));
        let err = op.apply(&[mixed], t0).unwrap_err();
        assert!(matches!(err, WindowFault::TypeMix { .. }), "{err}");
        let again = op.apply(&[row(2, 1_200, 0, 1)], t0).unwrap_err();
        assert_eq!(again, err);
        assert!(op.on_tick(t0 + Duration::from_secs(120)).is_empty());
    }

    #[test]
    fn overflow_reverts_the_batch() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(
            spec(WindowKind::Tumble, 60_000, None, None),
            0,
            60_000,
            KeyKind::I64,
        );
        op.apply(&[row(1, 1_000, 0, 10)], t0).unwrap();
        let err = op.apply(&[row(1, 1_100, 0, i64::MAX)], t0).unwrap_err();
        assert!(matches!(err, WindowFault::Overflow { .. }), "{err}");
        assert_eq!(op.open_windows(), 1);
        let closed = op.apply(&[row(1, 120_000, 0, 1)], t0).unwrap();
        assert_eq!(sum_of(&closed[0]), 10);
    }

    #[test]
    fn restore_rebuilds_the_end_index() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(
            spec(WindowKind::Tumble, 1_000, None, None),
            0,
            60_000,
            KeyKind::I64,
        );
        op.apply(&[row(1, 100, 0, 6)], t0).unwrap();
        let state = op.freeze_state();
        let watermark = op.instance_watermark_ms();
        let mut restored = WindowOperator::restore(
            spec(WindowKind::Tumble, 1_000, None, None),
            0,
            60_000,
            state,
            watermark,
            KeyKind::I64,
        )
        .expect("restore");
        assert_eq!(restored.open_windows(), 1);
        let closed = restored.apply(&[row(1, 2_000, 0, 1)], t0).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(sum_of(&closed[0]), 6);
        assert_eq!(closed[0].window_start, 0);
    }

    #[test]
    fn releasing_a_partition_drops_its_keys_and_does_not_reopen_them() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(
            spec(WindowKind::Tumble, 60_000, None, None),
            0,
            60_000,
            KeyKind::I64,
        );
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
        let mut op = WindowOperator::new(spec_id, 0, 60_000, KeyKind::I64);
        op.apply(&[row(1, 10_000, 0, 4)], t0).unwrap();
        let state = op.freeze_state();
        op.release_partition(0);
        assert_eq!(op.open_windows(), 0);
        let closed = op
            .adopt_partition(0, state.keys, Some(10_000), t0)
            .expect("adopt");
        assert!(closed.is_empty());
        assert_eq!(op.open_windows(), 1);
        let closed = op.apply(&[row(1, 20_000, 0, 3)], t0).unwrap();
        assert!(closed.is_empty());
        let closed = op.apply(&[row(1, 70_000, 0, 1)], t0).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(sum_of(&closed[0]), 7);
    }
}

fn merge_acc(
    dst: &mut Accumulators,
    src: &Accumulators,
    row: &WindowInput,
) -> Result<(), WindowFault> {
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
                        key: row.key.as_ref().map(WinKey::to_bytes).unwrap_or_default(),
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
            (AggState::Avg { sum, count }, AggState::Avg { sum: sb, count: cb }) => {
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
