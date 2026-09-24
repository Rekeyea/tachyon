//! Decodificación de payloads de Redpanda a `RecordBatch` de Arrow.
//!
//! Soporta dos formatos:
//! - **JSON** (vía `arrow-json`): cada mensaje es un objeto JSON.
//! - **Avro** (vía `apache-avro`): cada mensaje está codificado Avro con un
//!   schema conocido.
//!
//! El decoder toma un lote de payloads (ya agrupados por partición) y produce
//! un único `RecordBatch` cuyo schema coincide con el de la tabla de entrada.

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
}

impl Decoder {
    pub fn new(schema: SchemaRef, format: DecodeFormat) -> Self {
        Self { schema, format }
    }

    /// El schema Arrow de salida.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Decodifica los `values` (en orden) a un único `RecordBatch`.
    pub fn decode(&self, values: &[Vec<u8>]) -> Result<RecordBatch> {
        match &self.format {
            DecodeFormat::Json => self.decode_json(values),
            DecodeFormat::Avro(schema) => self.decode_avro(values, schema),
        }
    }

    /// JSON: concatena los mensajes en NDJSON y lo decodifica con `arrow-json`.
    fn decode_json(&self, values: &[Vec<u8>]) -> Result<RecordBatch> {
        let mut buf = Vec::new();
        for v in values {
            buf.extend_from_slice(v);
            buf.push(b'\n');
        }
        let reader = arrow_json::ReaderBuilder::new(self.schema.clone())
            .build(BufReader::new(buf.as_slice()))
            .context("decodificando JSON")?;
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("leyendo batches JSON")?;
        concat_batches(&self.schema, &batches).context("concatenando batches JSON")
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
        let values = vec![
            br#"{"order_id":1,"status":"paid","amount":100.0}"#.to_vec(),
            br#"{"order_id":2,"status":"shipped","amount":200.0}"#.to_vec(),
        ];
        let batch = decoder.decode(&values).expect("decode JSON");
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
        let values = vec![
            br#"{"order_id":1,"status":"paid","amount":10.0}"#.to_vec(),
            br#"{"order_id":2,"status":"paid"}"#.to_vec(),
        ];
        let batch = decoder.decode(&values).expect("decode JSON");
        let amounts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("columna amount");
        assert_eq!(amounts.null_count(), 1);
        assert!(amounts.is_null(1));
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

        let batch = decoder.decode(&encoded).expect("decode Avro");
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
