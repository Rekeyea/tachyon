//! Schema del topic en el registry de Redpanda.
//!
//! El Avro sale de las columnas del `SELECT`. Se registra en `{topic}-value`
//! y cada mensaje es `0x00` + id (i32 big-endian) + datum. Quien lee pide el
//! último schema para el Arrow del SQL y decodifica cada mensaje con su id.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use apache_avro::types::Value as AvroValue;
use apache_avro::Schema as AvroSchema;
use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

const CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

/// `{topic}-value`, el subject del payload en el registry de Confluent.
pub fn subject_name(topic: &str) -> String {
    format!("{topic}-value")
}

/// JSON del record Avro que describe `schema`. El nombre del record es fijo:
/// la identidad del schema son las columnas, no el topic.
pub fn avro_json_from_arrow(schema: &Schema) -> Result<String> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let ty = avro_type(field.data_type())
            .with_context(|| format!("columna '{}'", field.name()))?;
        let spec = if field.is_nullable() {
            serde_json::json!({
                "name": field.name(),
                "type": ["null", ty],
                "default": null,
            })
        } else {
            serde_json::json!({
                "name": field.name(),
                "type": ty,
            })
        };
        fields.push(spec);
    }
    let record = serde_json::json!({
        "type": "record",
        "name": "tachyon",
        "fields": fields,
    });
    Ok(record.to_string())
}

pub fn arrow_from_avro(schema: &AvroSchema) -> Result<SchemaRef> {
    let AvroSchema::Record(record) = schema else {
        anyhow::bail!("el schema del topic no es un record Avro");
    };
    let mut fields = Vec::with_capacity(record.fields.len());
    for field in &record.fields {
        let (nullable, data_type) = arrow_type(&field.schema)?;
        fields.push(Field::new(&field.name, data_type, nullable));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// Registra el schema del topic. Uno incompatible con el que ya está publicado
/// vuelve como error y no se le asigna id.
pub fn register_topic_schema(url: &str, topic: &str, avsc: &str) -> Result<i32> {
    let subject = subject_name(topic);
    put_compatibility(url, &subject)?;
    let body = serde_json::json!({
        "schemaType": "AVRO",
        "schema": avsc,
    });
    let response = post_json(&format!("{}/subjects/{subject}/versions", trim_url(url)), &body)
        .with_context(|| format!("registrando {subject}"))?;
    response
        .get("id")
        .and_then(|id| id.as_i64())
        .and_then(|id| i32::try_from(id).ok())
        .context("el registry no devolvió un id")
}

/// Último schema del subject, con su id.
pub fn latest_topic_schema(url: &str, topic: &str) -> Result<(i32, String)> {
    let subject = subject_name(topic);
    let response = get_json(&format!(
        "{}/subjects/{subject}/versions/latest",
        trim_url(url)
    ))
    .with_context(|| format!("leyendo el último schema de {subject}"))?;
    let id = response
        .get("id")
        .and_then(|id| id.as_i64())
        .and_then(|id| i32::try_from(id).ok())
        .context("el registry no devolvió un id")?;
    let avsc = response
        .get("schema")
        .and_then(|schema| schema.as_str())
        .context("el registry no devolvió el schema")?
        .to_string();
    Ok((id, avsc))
}

pub fn schema_by_id(url: &str, id: i32) -> Result<String> {
    let response = get_json(&format!("{}/schemas/ids/{id}", trim_url(url)))
        .with_context(|| format!("leyendo el schema {id}"))?;
    response
        .get("schema")
        .and_then(|schema| schema.as_str())
        .context("el registry no devolvió el schema")
        .map(str::to_string)
}

/// Un mensaje del topic: clave de Kafka y envelope Avro.
pub fn encode_envelopes(
    batch: &RecordBatch,
    key: &str,
    schema: &AvroSchema,
    id: i32,
) -> Result<Vec<(String, Vec<u8>)>> {
    let key_index = batch
        .schema()
        .index_of(key)
        .map_err(|_| anyhow::anyhow!("la salida no tiene la clave '{key}'"))?;
    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut fields = Vec::with_capacity(batch.num_columns());
        for (index, column) in batch.columns().iter().enumerate() {
            let name = batch.schema().field(index).name().clone();
            let nullable = batch.schema().field(index).is_nullable();
            fields.push((name, avro_value(column, row, nullable)?));
        }
        let key_text = key_text(batch.column(key_index), row, key)?;
        let datum = apache_avro::to_avro_datum(schema, AvroValue::Record(fields))
            .map_err(|err| anyhow::anyhow!("codificando Avro: {err}"))?;
        let mut payload = Vec::with_capacity(5 + datum.len());
        payload.push(0);
        payload.extend_from_slice(&id.to_be_bytes());
        payload.extend_from_slice(&datum);
        out.push((key_text, payload));
    }
    Ok(out)
}

/// Lee el id del envelope. `None` si el mensaje no trae el prefijo.
pub fn envelope_id(payload: &[u8]) -> Option<i32> {
    if payload.first() != Some(&0) || payload.len() < 5 {
        return None;
    }
    Some(i32::from_be_bytes(payload[1..5].try_into().expect("4 bytes")))
}

pub fn decode_envelope(
    payload: &[u8],
    writer: &AvroSchema,
    reader: &AvroSchema,
) -> Result<AvroValue> {
    let datum = payload.get(5..).context("envelope Avro truncado")?;
    let mut cursor = Cursor::new(datum);
    apache_avro::from_avro_datum(writer, &mut cursor, Some(reader))
        .map_err(|err| anyhow::anyhow!("decodificando Avro: {err}"))
}

/// Schemas de writer ya resueltos, compartidos por los hilos de decode.
#[derive(Debug)]
pub struct SchemaCache {
    url: String,
    pub reader: Arc<AvroSchema>,
    by_id: Mutex<HashMap<i32, Arc<AvroSchema>>>,
}

impl SchemaCache {
    pub fn new(url: String, reader_id: i32, reader: Arc<AvroSchema>) -> Self {
        let mut by_id = HashMap::new();
        by_id.insert(reader_id, reader.clone());
        Self {
            url,
            reader,
            by_id: Mutex::new(by_id),
        }
    }

    pub fn writer(&self, id: i32) -> Result<Arc<AvroSchema>> {
        if let Some(schema) = self.by_id.lock().expect("cache de schemas").get(&id) {
            return Ok(schema.clone());
        }
        let avsc = schema_by_id(&self.url, id)?;
        let schema = AvroSchema::parse_str(&avsc)
            .map(Arc::new)
            .map_err(|err| anyhow::anyhow!("schema {id} inválido: {err}"))?;
        self.by_id
            .lock()
            .expect("cache de schemas")
            .insert(id, schema.clone());
        Ok(schema)
    }
}

fn avro_type(data_type: &DataType) -> Result<serde_json::Value> {
    let name = match data_type {
        DataType::Int64 => "long",
        DataType::Int32 => "int",
        DataType::Float64 => "double",
        DataType::Utf8 => "string",
        DataType::Boolean => "boolean",
        other => anyhow::bail!("el topic Avro no publica columnas {other}"),
    };
    Ok(serde_json::Value::String(name.to_string()))
}

fn arrow_type(schema: &AvroSchema) -> Result<(bool, DataType)> {
    match schema {
        AvroSchema::Union(union) => {
            let concrete: Vec<&AvroSchema> = union
                .variants()
                .iter()
                .filter(|variant| !matches!(variant, AvroSchema::Null))
                .collect();
            if concrete.len() != 1 || !union.is_nullable() {
                anyhow::bail!("el union Avro tiene que ser null más un tipo");
            }
            let (_, data_type) = arrow_type(concrete[0])?;
            Ok((true, data_type))
        }
        AvroSchema::Long => Ok((false, DataType::Int64)),
        AvroSchema::Int => Ok((false, DataType::Int32)),
        AvroSchema::Double => Ok((false, DataType::Float64)),
        AvroSchema::String => Ok((false, DataType::Utf8)),
        AvroSchema::Boolean => Ok((false, DataType::Boolean)),
        other => anyhow::bail!("tipo Avro no soportado: {other:?}"),
    }
}

fn avro_value(column: &dyn Array, row: usize, nullable: bool) -> Result<AvroValue> {
    if column.is_null(row) {
        if !nullable {
            anyhow::bail!("valor null en una columna que no lo admite");
        }
        return Ok(AvroValue::Union(0, Box::new(AvroValue::Null)));
    }
    let value = match column.data_type() {
        DataType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("columna Int64")?;
            AvroValue::Long(array.value(row))
        }
        DataType::Int32 => {
            let array = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("columna Int32")?;
            AvroValue::Int(array.value(row))
        }
        DataType::Float64 => {
            let array = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("columna Float64")?;
            AvroValue::Double(array.value(row))
        }
        DataType::Utf8 => {
            let array = column
                .as_any()
                .downcast_ref::<StringArray>()
                .context("columna Utf8")?;
            AvroValue::String(array.value(row).to_string())
        }
        DataType::Boolean => {
            let array = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("columna Boolean")?;
            AvroValue::Boolean(array.value(row))
        }
        other => anyhow::bail!("el topic Avro no publica columnas {other}"),
    };
    if nullable {
        Ok(AvroValue::Union(1, Box::new(value)))
    } else {
        Ok(value)
    }
}

fn key_text(column: &dyn Array, row: usize, key: &str) -> Result<String> {
    if column.is_null(row) {
        anyhow::bail!("la clave '{key}' es null en la fila {row}");
    }
    match column.data_type() {
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(array.value(row).to_string())
        }
        DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(array.value(row).to_string())
        }
        DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(array.value(row).to_string())
        }
        other => anyhow::bail!("la clave '{key}' es {other}; hace falta Int64, Int32 o Utf8"),
    }
}

fn trim_url(url: &str) -> &str {
    url.trim().trim_end_matches('/')
}

fn put_compatibility(url: &str, subject: &str) -> Result<()> {
    let body = serde_json::json!({"compatibility": "BACKWARD"});
    let endpoint = format!("{}/config/{subject}", trim_url(url));
    let response = ureq::put(&endpoint)
        .set("Content-Type", CONTENT_TYPE)
        .send_json(body);
    explain(response).with_context(|| format!("fijando BACKWARD en {subject}"))?;
    Ok(())
}

fn post_json(url: &str, body: &serde_json::Value) -> Result<serde_json::Value> {
    let response = ureq::post(url)
        .set("Content-Type", CONTENT_TYPE)
        .send_json(body.clone());
    explain(response)
}

fn get_json(url: &str) -> Result<serde_json::Value> {
    explain(ureq::get(url).call())
}

fn explain(response: Result<ureq::Response, ureq::Error>) -> Result<serde_json::Value> {
    match response {
        Ok(response) => response
            .into_json()
            .context("leyendo la respuesta del schema registry"),
        Err(ureq::Error::Status(code, response)) => {
            let body = response.into_string().unwrap_or_default();
            anyhow::bail!("schema registry respondió {code}: {body}");
        }
        Err(err) => anyhow::bail!("schema registry: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    fn sample() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, true),
        ]))
    }

    #[test]
    fn an_arrow_schema_roundtrips_through_avro() {
        let arrow = sample();
        let avsc = avro_json_from_arrow(&arrow).unwrap();
        let avro = AvroSchema::parse_str(&avsc).unwrap();
        let back = arrow_from_avro(&avro).unwrap();
        assert_eq!(back.fields().len(), 2);
        assert!(!back.field(0).is_nullable());
        assert!(back.field(1).is_nullable());
        assert_eq!(back.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn an_envelope_decodes_with_its_id() {
        let arrow = sample();
        let avsc = avro_json_from_arrow(&arrow).unwrap();
        let avro = AvroSchema::parse_str(&avsc).unwrap();
        let batch = RecordBatch::try_new(
            arrow,
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![Some(4)])),
            ],
        )
        .unwrap();
        let records = encode_envelopes(&batch, "order_id", &avro, 11).unwrap();
        assert_eq!(records[0].0, "7");
        assert_eq!(envelope_id(&records[0].1), Some(11));
        let value = decode_envelope(&records[0].1, &avro, &avro).unwrap();
        match value {
            AvroValue::Record(fields) => {
                assert_eq!(fields[0].1, AvroValue::Long(7));
            }
            other => panic!("record esperado, llegó {other:?}"),
        }
    }
}
