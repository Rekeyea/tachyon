//! Decodificación de payloads de Redpanda a `RecordBatch` de Arrow.
//!
//! Soporta dos formatos:
//! - **JSON** (vía `arrow-json`): cada mensaje es un objeto JSON.
//! - **Avro** (vía `apache-avro`): cada mensaje es un datum Avro del schema
//!   conocido (lo que viaja en un topic) o un object container. Se materializa
//!   a Arrow una sola vez; el resto del pipeline no vuelve a ver Avro.
//!
//! El decoder toma un lote de payloads (ya agrupados por partición) y produce
//! un único `RecordBatch` cuyo schema coincide con el de la tabla de entrada.

use std::io::{BufReader, Cursor};
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
    /// Avro de un `.avsc`: datum crudo o object container, un solo schema.
    Avro(Arc<AvroSchema>),
    /// Avro del registry: envelope `0x00` + id. El Arrow es el último schema.
    Registry(Arc<crate::schema_wire::SchemaCache>),
}

/// Header de un object container Avro (`Obj` + versión 1).
const AVRO_CONTAINER_MAGIC: &[u8] = b"Obj\x01";

/// Parsea un `.avsc`. El schema queda en el decoder; cada mensaje se proyecta
/// a las columnas Arrow y no se conserva como valor Avro.
pub fn parse_avro_schema(avsc: &str) -> Result<Arc<AvroSchema>> {
    AvroSchema::parse_str(avsc)
        .map(Arc::new)
        .map_err(|e| anyhow::anyhow!("schema Avro inválido: {e}"))
}

/// Decodifica los payloads de un lote de registros a un `RecordBatch`.
#[derive(Clone)]
pub struct Decoder {
    schema: SchemaRef,
    format: DecodeFormat,
    /// JSON con un schema de tipos planos: decoder directo a buffers Arrow.
    flat: Option<crate::json_flat::FlatJson>,
}

impl Decoder {
    pub fn new(schema: SchemaRef, format: DecodeFormat) -> Self {
        let flat = (matches!(format, DecodeFormat::Json)
            && schema
                .fields()
                .iter()
                .all(|f| crate::json_flat::supported(f.data_type())))
        .then(|| crate::json_flat::FlatJson::new(schema.clone()));
        Self {
            schema,
            format,
            flat,
        }
    }

    /// El schema Arrow de salida.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Decodifica los `values` (en orden) a un único `RecordBatch`.
    ///
    /// Los `values` no se modifican; la firma `&mut` se mantiene para los
    /// callers (el decoder anterior parseaba in-place).
    pub fn decode(&self, values: &mut [Vec<u8>]) -> Result<RecordBatch> {
        let payloads: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
        self.decode_payloads(&payloads)
    }

    /// Decodifica payloads prestados (el stream no los copia a un buffer
    /// propio antes del decode).
    pub fn decode_payloads(&self, values: &[&[u8]]) -> Result<RecordBatch> {
        match &self.format {
            DecodeFormat::Json if self.flat.is_some() => {
                self.flat.as_ref().expect("decoder plano").decode(values)
            }
            DecodeFormat::Json => self.decode_json_arrow(values),
            DecodeFormat::Avro(schema) => self.decode_avro(values, schema),
            DecodeFormat::Registry(cache) => self.decode_registry(values, cache),
        }
    }

    fn decode_registry(
        &self,
        values: &[&[u8]],
        cache: &crate::schema_wire::SchemaCache,
    ) -> Result<RecordBatch> {
        let mut records = Vec::with_capacity(values.len());
        for (row, payload) in values.iter().enumerate() {
            let id = crate::schema_wire::envelope_id(payload).with_context(|| {
                format!("el mensaje {row} no trae el id de schema (0x00 + id)")
            })?;
            let writer = cache.writer(id)?;
            let value = crate::schema_wire::decode_envelope(payload, &writer, &cache.reader)
                .with_context(|| format!("mensaje {row}, schema {id}"))?;
            records.push(value);
        }
        build_avro_batch(&self.schema, &records)
    }

    /// JSON fallback: concatena los mensajes en NDJSON y lo decodifica con
    /// `arrow-json`. Cubre los tipos que el fast path SIMD no soporta.
    ///
    /// `batch_size = values.len()`: un solo batch por lote, así que el caso
    /// típico devuelve el batch tal cual sin `concat_batches` (que copiaba
    /// todas las columnas de nuevo por cada lote decodificado).
    fn decode_json_arrow(&self, values: &[&[u8]]) -> Result<RecordBatch> {
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
    ///
    /// Un mensaje de topic es un datum (binario del schema, sin header). Un
    /// object container (`Obj\x01`, lo que escribe `apache_avro::Writer`)
    /// también se acepta: es el formato de los tests y de un productor que
    /// empaqueta el registro.
    fn decode_avro(&self, values: &[&[u8]], schema: &Arc<AvroSchema>) -> Result<RecordBatch> {
        let mut records: Vec<AvroValue> = Vec::new();
        for (row, v) in values.iter().enumerate() {
            if v.starts_with(AVRO_CONTAINER_MAGIC) {
                let reader = apache_avro::Reader::with_schema(schema, BufReader::new(*v))
                    .map_err(|e| anyhow::anyhow!("leyendo container Avro (fila {row}): {e}"))?;
                for record in reader {
                    records.push(record.map_err(|e| {
                        anyhow::anyhow!("decodificando container Avro (fila {row}): {e}")
                    })?);
                }
            } else {
                let mut cursor = Cursor::new(*v);
                let value = apache_avro::from_avro_datum(schema, &mut cursor, None)
                    .with_context(|| format!("decodificando datum Avro (fila {row})"))?;
                records.push(value);
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
        DataType::Int32 => {
            let arr: arrow::array::Int32Array =
                values.iter().map(|v| v.and_then(extract_i32)).collect();
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

fn peel(value: &AvroValue) -> &AvroValue {
    match value {
        AvroValue::Union(_, inner) => peel(inner),
        other => other,
    }
}

fn extract_i64(v: &AvroValue) -> Option<i64> {
    match peel(v) {
        AvroValue::Long(n) => Some(*n),
        AvroValue::Int(n) => Some(*n as i64),
        _ => None,
    }
}

fn extract_i32(v: &AvroValue) -> Option<i32> {
    match peel(v) {
        AvroValue::Int(n) => Some(*n),
        _ => None,
    }
}

fn extract_f64(v: &AvroValue) -> Option<f64> {
    match peel(v) {
        AvroValue::Double(f) => Some(*f),
        AvroValue::Float(f) => Some(*f as f64),
        _ => None,
    }
}

fn extract_str(v: &AvroValue) -> Option<&str> {
    match peel(v) {
        AvroValue::String(s) => Some(s.as_str()),
        _ => None,
    }
}

fn extract_bool(v: &AvroValue) -> Option<bool> {
    match peel(v) {
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
        assert!(decoder.flat.is_some(), "el schema plano debe usar el decoder plano");
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
        assert!(decoder.flat.is_none(), "Int32 debe caer al fallback");
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

    #[test]
    fn decodes_a_raw_avro_datum() {
        let schema = orders_schema();
        let avro_schema = parse_avro_schema(
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
        .expect("schema Avro");
        let decoder = Decoder::new(schema, DecodeFormat::Avro(avro_schema.clone()));
        let datum = apache_avro::to_avro_datum(
            avro_schema.as_ref(),
            AvroValue::Record(vec![
                ("order_id".to_string(), AvroValue::Long(7)),
                ("status".to_string(), AvroValue::String("paid".to_string())),
                ("amount".to_string(), AvroValue::Double(12.5)),
            ]),
        )
        .expect("datum");
        assert!(
            !datum.starts_with(b"Obj"),
            "un datum de topic no lleva header de container"
        );
        let mut values = vec![datum];
        let batch = decoder.decode(&mut values).expect("decode datum");
        assert_eq!(batch.num_rows(), 1);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("order_id");
        assert_eq!(ids.value(0), 7);
    }
}
