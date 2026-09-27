//! Decodificación de payloads de Redpanda a `RecordBatch` de Arrow.
//!
//! Soporta dos formatos:
//! - **JSON** (vía `arrow-json`): cada mensaje es un objeto JSON.
//! - **Avro** (vía `apache-avro`): cada mensaje está codificado Avro con un
//!   schema conocido.
//!
//! El decoder toma un lote de payloads (ya agrupados por partición) y produce
//! un único `RecordBatch` cuyo schema coincide con el de la tabla de entrada.

use std::collections::HashMap;
use std::io::BufReader;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use apache_avro::Schema as AvroSchema;
use apache_avro::types::Value as AvroValue;

/// El formato de decodificación del payload.
#[derive(Debug, Clone)]
pub enum DecodeFormat {
    /// JSON (un objeto por mensaje).
    Json,
    /// Avro (con el schema Avro).
    Avro(Arc<AvroSchema>),
}

/// Decodifica los payloads de un lote de registros a un `RecordBatch`.
#[derive(Clone)]
pub struct Decoder {
    schema: SchemaRef,
    format: DecodeFormat,
    /// JSON: el schema admite el fast path SIMD (solo tipos planos soportados).
    fast_json: bool,
}

/// Tipos Arrow que el fast path JSON (SIMD) sabe materializar.
fn fast_json_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int64 | DataType::Float64 | DataType::Utf8 | DataType::Boolean
    )
}

impl Decoder {
    pub fn new(schema: SchemaRef, format: DecodeFormat) -> Self {
        let fast_json = matches!(format, DecodeFormat::Json)
            && schema.fields().iter().all(|f| fast_json_type(f.data_type()));
        Self {
            schema,
            format,
            fast_json,
        }
    }

    /// El schema Arrow de salida.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Decodifica los `values` (en orden) a un único `RecordBatch`.
    ///
    /// El fast path JSON **muta los buffers in-place** (simd-json parsea
    /// sobre el propio slice), así que los `values` no son reutilizables
    /// después del decode.
    pub fn decode(&self, values: &mut [Vec<u8>]) -> Result<RecordBatch> {
        match &self.format {
            DecodeFormat::Json if self.fast_json => self.decode_json_simd(values),
            DecodeFormat::Json => self.decode_json_arrow(values),
            DecodeFormat::Avro(schema) => self.decode_avro(values, schema),
        }
    }

    /// JSON fast path: parseo SIMD **in-place** (simd-json) directo a columnas
    /// Arrow. Sin buffer NDJSON intermedio, sin `concat_batches`, sin DOM
    /// persistente: una pasada por registro, una copia por string (la propia
    /// materialización columnar). Es el cuello de botella del pipeline, así
    /// que el costo por fila importa más que la generalidad.
    fn decode_json_simd(&self, values: &mut [Vec<u8>]) -> Result<RecordBatch> {
        use simd_json::BorrowedValue;

        let n = values.len();
        let col_index: HashMap<&str, usize> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().as_str(), i))
            .collect();
        let mut cols: Vec<JsonCol> = self
            .schema
            .fields()
            .iter()
            .map(|f| JsonCol::new(f.data_type(), n))
            .collect();

        for (row, buf) in values.iter_mut().enumerate() {
            let value: BorrowedValue = simd_json::to_borrowed_value(buf.as_mut_slice())
                .with_context(|| format!("parseando JSON (fila {row})"))?;
            let BorrowedValue::Object(obj) = &value else {
                anyhow::bail!("se esperaba un objeto JSON por mensaje (fila {row})");
            };
            for (key, val) in obj.iter() {
                if let Some(&ci) = col_index.get(key.as_ref()) {
                    let field = self.schema.fields()[ci].name().clone();
                    cols[ci]
                        .push(val)
                        .with_context(|| format!("decodificando la columna '{field}' (fila {row})"))?;
                }
            }
            // Campos ausentes en el mensaje -> null (padding alineado por fila).
            for col in cols.iter_mut() {
                if col.len() == row {
                    col.push_null();
                }
            }
        }

        let arrays: Vec<ArrayRef> = cols.into_iter().map(JsonCol::build).collect();
        RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| anyhow::anyhow!("construyendo RecordBatch JSON: {e}"))
    }

    /// JSON fallback: concatena los mensajes en NDJSON y lo decodifica con
    /// `arrow-json`. Cubre los tipos que el fast path SIMD no soporta.
    ///
    /// `batch_size = values.len()`: un solo batch por lote, así que el caso
    /// típico devuelve el batch tal cual sin `concat_batches` (que copiaba
    /// todas las columnas de nuevo por cada lote decodificado).
    fn decode_json_arrow(&self, values: &[Vec<u8>]) -> Result<RecordBatch> {
        let mut buf = Vec::with_capacity(values.iter().map(|v| v.len() + 1).sum());
        for v in values {
            buf.extend_from_slice(v);
            buf.push(b'\n');
        }
        let reader = arrow_json::ReaderBuilder::new(self.schema.clone())
            .with_batch_size(values.len().max(1024))
            .build(BufReader::new(buf.as_slice()))
            .context("decodificando JSON")?;
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("leyendo batches JSON")?;
        if batches.len() == 1 {
            Ok(batches.into_iter().next().expect("un batch"))
        } else {
            concat_batches(&self.schema, &batches).context("concatenando batches JSON")
        }
    }

    /// Avro: decodifica cada mensaje y construye las columnas Arrow.
    fn decode_avro(&self, values: &[Vec<u8>], schema: &Arc<AvroSchema>) -> Result<RecordBatch> {
        let mut records: Vec<AvroValue> = Vec::new();
        for v in values {
            let reader = apache_avro::Reader::with_schema(schema, BufReader::new(v.as_slice()))
                .map_err(|e| anyhow::anyhow!("leyendo mensaje Avro: {e}"))?;
            for record in reader {
                records.push(
                    record
                        .map_err(|e| anyhow::anyhow!("decodificando registro Avro: {e}"))?,
                );
            }
        }
        build_avro_batch(&self.schema, &records)
    }
}

/// Columna en construcción del fast path JSON: un `Vec<Option<T>>` por tipo
/// soportado. Los valores faltantes se rellenan con null al final de cada
/// fila (ver `decode_json_simd`).
enum JsonCol {
    I64(Vec<Option<i64>>),
    F64(Vec<Option<f64>>),
    Str(Vec<Option<String>>),
    Bool(Vec<Option<bool>>),
}

impl JsonCol {
    fn new(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Int64 => JsonCol::I64(Vec::with_capacity(capacity)),
            DataType::Float64 => JsonCol::F64(Vec::with_capacity(capacity)),
            DataType::Utf8 => JsonCol::Str(Vec::with_capacity(capacity)),
            DataType::Boolean => JsonCol::Bool(Vec::with_capacity(capacity)),
            other => unreachable!("tipo no soportado por el fast path JSON: {other:?}"),
        }
    }

    fn len(&self) -> usize {
        match self {
            JsonCol::I64(v) => v.len(),
            JsonCol::F64(v) => v.len(),
            JsonCol::Str(v) => v.len(),
            JsonCol::Bool(v) => v.len(),
        }
    }

    fn push_null(&mut self) {
        match self {
            JsonCol::I64(v) => v.push(None),
            JsonCol::F64(v) => v.push(None),
            JsonCol::Str(v) => v.push(None),
            JsonCol::Bool(v) => v.push(None),
        }
    }

    /// Pushea un valor JSON a la columna, con coerciones compatibles con
    /// arrow-json: enteros -> Float64, y JSON null -> null de Arrow.
    fn push(&mut self, value: &simd_json::BorrowedValue) -> Result<()> {
        use simd_json::{BorrowedValue, StaticNode};
        if matches!(value, BorrowedValue::Static(StaticNode::Null)) {
            self.push_null();
            return Ok(());
        }
        match (self, value) {
            (JsonCol::I64(v), BorrowedValue::Static(StaticNode::I64(n))) => v.push(Some(*n)),
            (JsonCol::I64(v), BorrowedValue::Static(StaticNode::U64(n))) => v.push(Some(
                i64::try_from(*n).context("entero JSON fuera de rango para Int64")?,
            )),
            (JsonCol::F64(v), BorrowedValue::Static(StaticNode::F64(f))) => v.push(Some(*f)),
            (JsonCol::F64(v), BorrowedValue::Static(StaticNode::I64(n))) => v.push(Some(*n as f64)),
            (JsonCol::F64(v), BorrowedValue::Static(StaticNode::U64(n))) => v.push(Some(*n as f64)),
            (JsonCol::Str(v), BorrowedValue::String(s)) => v.push(Some(s.to_string())),
            (JsonCol::Bool(v), BorrowedValue::Static(StaticNode::Bool(b))) => v.push(Some(*b)),
            (col, other) => {
                let tipo = match col {
                    JsonCol::I64(_) => "Int64",
                    JsonCol::F64(_) => "Float64",
                    JsonCol::Str(_) => "Utf8",
                    JsonCol::Bool(_) => "Boolean",
                };
                anyhow::bail!("valor JSON incompatible con la columna {tipo}: {other:?}");
            }
        }
        Ok(())
    }

    fn build(self) -> ArrayRef {
        match self {
            JsonCol::I64(v) => Arc::new(Int64Array::from(v)),
            JsonCol::F64(v) => Arc::new(Float64Array::from(v)),
            JsonCol::Str(v) => Arc::new(StringArray::from(v)),
            JsonCol::Bool(v) => Arc::new(BooleanArray::from(v)),
        }
    }
}

/// Construye un `RecordBatch` a partir de registros Avro, campo a campo.
fn build_avro_batch(schema: &SchemaRef, records: &[AvroValue]) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name().to_string();
        let values: Vec<Option<&AvroValue>> =
            records.iter().map(|r| get_record_field(r, &name)).collect();
        let array = build_array(&field.data_type(), &values)
            .with_context(|| format!("construyendo columna '{}'", field.name()))?;
        columns.push(array);
    }
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| anyhow::anyhow!("construyendo RecordBatch Avro: {e}"))
}

/// Extrae un campo de un `Value::Record` (representado como `Vec<(name, value)>`).
fn get_record_field<'a>(value: &'a AvroValue, name: &str) -> Option<&'a AvroValue> {
    match value {
        AvroValue::Record(fields) => fields.iter().find(|(n, _)| n == name).map(|(_, v)| v),
        _ => None,
    }
}

/// Construye una columna Arrow a partir de valores Avro según el tipo Arrow.
fn build_array(data_type: &DataType, values: &[Option<&AvroValue>]) -> Result<ArrayRef> {
    match data_type {
        DataType::Int64 => {
            let arr: Int64Array = values.iter().map(|v| v.and_then(extract_i64)).collect();
            Ok(Arc::new(arr))
        }
        DataType::Float64 => {
            let arr: Float64Array = values.iter().map(|v| v.and_then(extract_f64)).collect();
            Ok(Arc::new(arr))
        }
        DataType::Utf8 => {
            let arr: StringArray = values.iter().map(|v| v.and_then(extract_str)).collect();
            Ok(Arc::new(arr))
        }
        DataType::Boolean => {
            let arr: BooleanArray = values.iter().map(|v| v.and_then(extract_bool)).collect();
            Ok(Arc::new(arr))
        }
        other => Err(anyhow::anyhow!(
            "tipo no soportado en el decoder Avro: {other:?}"
        )),
    }
}

fn extract_i64(v: &AvroValue) -> Option<i64> {
    match v {
        AvroValue::Long(n) => Some(*n),
        AvroValue::Int(n) => Some(*n as i64),
        _ => None,
    }
}

fn extract_f64(v: &AvroValue) -> Option<f64> {
    match v {
        AvroValue::Double(f) => Some(*f),
        AvroValue::Float(f) => Some(*f as f64),
        _ => None,
    }
}

fn extract_str(v: &AvroValue) -> Option<&str> {
    match v {
        AvroValue::String(s) => Some(s.as_str()),
        _ => None,
    }
}

fn extract_bool(v: &AvroValue) -> Option<bool> {
    match v {
        AvroValue::Boolean(b) => Some(*b),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn orders_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
        ]))
    }

    #[test]
    fn decodes_json_to_record_batch() {
        let schema = orders_schema();
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
        let mut values = vec![
            br#"{"order_id":1,"status":"paid","amount":100.0}"#.to_vec(),
            br#"{"order_id":2,"status":"shipped","amount":200.0}"#.to_vec(),
        ];
        let batch = decoder.decode(&mut values).expect("decode JSON");
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("columna order_id");
        assert_eq!(ids.values(), &[1, 2]);
        let amounts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("columna amount");
        assert_eq!(amounts.values(), &[100.0, 200.0]);
    }

    #[test]
    fn decodes_json_with_missing_field_as_null() {
        let schema = orders_schema();
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
        // El segundo mensaje no trae `amount` -> debe ser null.
        let mut values = vec![
            br#"{"order_id":1,"status":"paid","amount":10.0}"#.to_vec(),
            br#"{"order_id":2,"status":"paid"}"#.to_vec(),
        ];
        let batch = decoder.decode(&mut values).expect("decode JSON");
        let amounts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("columna amount");
        assert_eq!(amounts.null_count(), 1);
        assert!(amounts.is_null(1));
    }

    #[test]
    fn fast_json_coerces_int_to_float_and_handles_explicit_null() {
        let schema = orders_schema();
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
        assert!(decoder.fast_json, "el schema plano debe usar el fast path");
        // amount entero (sin punto) -> Float64; status null explícito -> null.
        let mut values = vec![
            br#"{"order_id":1,"status":null,"amount":100}"#.to_vec(),
            br#"{"order_id":2,"status":"paid","amount":2.5}"#.to_vec(),
        ];
        let batch = decoder.decode(&mut values).expect("decode JSON");
        let amounts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("columna amount");
        assert_eq!(amounts.values(), &[100.0, 2.5], "entero coerciona a float");
        let statuses = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("columna status");
        assert!(statuses.is_null(0), "null explícito -> null de Arrow");
        assert_eq!(statuses.value(1), "paid");
    }

    #[test]
    fn fast_json_ignores_unknown_fields() {
        let schema = orders_schema();
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
        let mut values =
            vec![br#"{"order_id":7,"status":"paid","amount":1.0,"extra":"ignorame"}"#.to_vec()];
        let batch = decoder.decode(&mut values).expect("decode JSON");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3, "el campo extra no entra al batch");
    }

    #[test]
    fn fast_json_rejects_non_object() {
        let schema = orders_schema();
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
        let mut values = vec![br#"[1,2,3]"#.to_vec()];
        assert!(decoder.decode(&mut values).is_err(), "un array no es un registro");
    }

    #[test]
    fn json_fallback_arrow_json_para_tipos_no_soportados() {
        // Int32 no lo cubre el fast path -> cae al decoder arrow-json.
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
        assert!(!decoder.fast_json, "Int32 debe caer al fallback");
        let mut values = vec![br#"{"n":5,"s":"x"}"#.to_vec()];
        let batch = decoder.decode(&mut values).expect("decode fallback");
        assert_eq!(batch.num_rows(), 1);
    }

    /// Codifica un `Value` Avro a bytes (contenedor Avro con header) para el test.
    fn encode_avro(schema: &AvroSchema, value: &AvroValue) -> Vec<u8> {
        let mut writer = apache_avro::Writer::new(schema, Vec::new());
        writer.append_value_ref(value).expect("append Avro");
        writer.into_inner().expect("into_inner Avro")
    }

    #[test]
    fn decodes_avro_to_record_batch() {
        let schema = orders_schema();
        let avro_schema: Arc<AvroSchema> = Arc::new(
            AvroSchema::parse_str(
                r#"{
                    "type": "record",
                    "name": "Order",
                    "fields": [
                        {"name": "order_id", "type": "long"},
                        {"name": "status", "type": "string"},
                        {"name": "amount", "type": "double"}
                    ]
                }"#,
            )
            .expect("schema Avro válido"),
        );
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Avro(avro_schema.clone()));

        let value1 = AvroValue::Record(vec![
            ("order_id".to_string(), AvroValue::Long(1)),
            ("status".to_string(), AvroValue::String("paid".to_string())),
            ("amount".to_string(), AvroValue::Double(100.0)),
        ]);
        let value2 = AvroValue::Record(vec![
            ("order_id".to_string(), AvroValue::Long(2)),
            ("status".to_string(), AvroValue::String("shipped".to_string())),
            ("amount".to_string(), AvroValue::Double(200.0)),
        ]);
        let encoded = vec![
            encode_avro(avro_schema.as_ref(), &value1),
            encode_avro(avro_schema.as_ref(), &value2),
        ];

        let batch = decoder.decode(&mut encoded.clone()).expect("decode Avro");
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("columna order_id");
        assert_eq!(ids.values(), &[1, 2]);
        let statuses = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("columna status");
        assert_eq!(statuses.value(0), "paid");
    }
}
