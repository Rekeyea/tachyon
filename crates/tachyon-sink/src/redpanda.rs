//! Sink a un topic de Redpanda.
//!
//! Cada epoch es una transacción: los registros de salida y los offsets de
//! entrada se commitean juntos. Un crash antes del commit deja la transacción
//! abortada. El proceso que lee con `isolation.level=read_committed` no ve
//! ese lote, y el reinicio lo vuelve a publicar desde los offsets commiteados.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};

/// Productor transaccional de un topic. El payload de cada fila es un objeto
/// JSON. La clave del mensaje es la columna `key`, en texto canónico, para
/// que el particionador de librdkafka ubique la misma clave en la misma
/// partición en todos los pipelines.
pub struct RedpandaSink {
    producer: FutureProducer,
    topic: String,
    key: String,
    /// Hay una transacción abierta. `Drop` la aborta.
    open: Mutex<bool>,
}

impl RedpandaSink {
    pub async fn open(
        brokers: &str,
        topic: &str,
        key: &str,
        transactional_id: &str,
        transaction_timeout_ms: u64,
    ) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("transactional.id", transactional_id)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("transaction.timeout.ms", transaction_timeout_ms.to_string())
            .create()
            .context("creando el productor transaccional")?;
        let init = producer.clone();
        tokio::task::spawn_blocking(move || {
            init.init_transactions(Duration::from_secs(30))
                .map_err(|e| anyhow::anyhow!("init_transactions: {e}"))
        })
        .await
        .context("init_transactions")??;
        Ok(Self {
            producer,
            topic: topic.to_string(),
            key: key.to_string(),
            open: Mutex::new(false),
        })
    }

    pub async fn begin(&self) -> Result<()> {
        let producer = self.producer.clone();
        tokio::task::spawn_blocking(move || {
            producer
                .begin_transaction()
                .map_err(|e| anyhow::anyhow!("begin_transaction: {e}"))
        })
        .await
        .context("begin_transaction")??;
        *self.open.lock().expect("lock de la transacción") = true;
        Ok(())
    }

    /// Publica el batch dentro de la transacción abierta. Devuelve la cantidad
    /// de filas. Una clave null es un error: iría a una partición arbitraria.
    pub async fn write(&self, batch: &RecordBatch) -> Result<usize> {
        let records = records_from_batch(batch, &self.key)?;
        self.write_records(&records).await
    }

    /// Publica pares `(clave, payload)` ya codificados en el topic de salida.
    /// Lo usa el camino Avro, que arma el envelope antes de llegar acá.
    pub async fn write_records(&self, records: &[(String, Vec<u8>)]) -> Result<usize> {
        self.write_to(&self.topic, records).await
    }

    /// Publica en otro topic de la misma transacción. El cursor de una tabla
    /// va acá, después de las filas y antes del commit.
    pub async fn write_to(&self, topic: &str, records: &[(String, Vec<u8>)]) -> Result<usize> {
        let topic = topic.to_string();
        for (key, payload) in records {
            self.producer
                .send(
                    FutureRecord::to(&topic).key(key).payload(payload),
                    Duration::from_secs(30),
                )
                .await
                .map_err(|(e, _)| anyhow::anyhow!("publicando en '{topic}': {e}"))?;
        }
        Ok(records.len())
    }

    /// Incluye los offsets de entrada (el próximo a leer) en la transacción.
    pub fn send_input_offsets(
        &self,
        group: *const rdkafka_sys::rd_kafka_consumer_group_metadata_t,
        input_topic: &str,
        offsets: &BTreeMap<i32, i64>,
    ) -> Result<()> {
        let mut list = TopicPartitionList::new();
        for (&partition, &next) in offsets {
            list.add_partition_offset(input_topic, partition, Offset::Offset(next))
                .with_context(|| format!("armando offset {input_topic}[{partition}]={next}"))?;
        }
        let err = unsafe {
            rdkafka_sys::rd_kafka_send_offsets_to_transaction(
                self.producer.client().native_ptr(),
                list.ptr(),
                group,
                30_000,
            )
        };
        if !err.is_null() {
            let code = unsafe { rdkafka_sys::rd_kafka_error_code(err) };
            let msg = unsafe { std::ffi::CStr::from_ptr(rdkafka_sys::rd_kafka_error_string(err)) }
                .to_string_lossy()
                .into_owned();
            unsafe { rdkafka_sys::rd_kafka_error_destroy(err) };
            if code != rdkafka_sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                anyhow::bail!("send_offsets_to_transaction: {msg}");
            }
        }
        Ok(())
    }

    pub async fn commit(&self) -> Result<()> {
        let producer = self.producer.clone();
        tokio::task::spawn_blocking(move || {
            producer
                .commit_transaction(Duration::from_secs(30))
                .map_err(|e| anyhow::anyhow!("commit_transaction: {e}"))
        })
        .await
        .context("commit_transaction")??;
        *self.open.lock().expect("lock de la transacción") = false;
        Ok(())
    }

    pub fn abort(&self) -> Result<()> {
        let open = {
            let mut guard = self.open.lock().expect("lock de la transacción");
            let was = *guard;
            *guard = false;
            was
        };
        if !open {
            return Ok(());
        }
        self.producer
            .abort_transaction(Duration::from_secs(10))
            .map_err(|e| anyhow::anyhow!("abort_transaction: {e}"))
    }
}

impl Drop for RedpandaSink {
    fn drop(&mut self) {
        let _ = self.abort();
    }
}

/// Una fila del batch, como `(clave del mensaje, JSON)`.
pub fn records_from_batch(batch: &RecordBatch, key: &str) -> Result<Vec<(String, Vec<u8>)>> {
    let key_index = batch
        .schema()
        .index_of(key)
        .map_err(|_| anyhow::anyhow!("la salida no tiene la clave '{key}'"))?;
    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut object = serde_json::Map::with_capacity(batch.num_columns());
        for (index, column) in batch.columns().iter().enumerate() {
            let name = batch.schema().field(index).name().clone();
            object.insert(name, json_value(column, row)?);
        }
        let key_value = json_value(batch.column(key_index), row)?;
        let key_text = match key_value {
            serde_json::Value::String(text) => text,
            serde_json::Value::Number(number) => number.to_string(),
            serde_json::Value::Null => {
                anyhow::bail!("la clave '{key}' es null en la fila {row}")
            }
            other => anyhow::bail!("la clave '{key}' no es un número ni un texto: {other}"),
        };
        let payload = serde_json::to_vec(&object).context("serializando la fila")?;
        out.push((key_text, payload));
    }
    Ok(out)
}

fn json_value(column: &dyn Array, row: usize) -> Result<serde_json::Value> {
    if column.is_null(row) {
        return Ok(serde_json::Value::Null);
    }
    let value = match column.data_type() {
        DataType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("columna Int64")?;
            serde_json::Value::Number(array.value(row).into())
        }
        DataType::Int32 => {
            let array = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("columna Int32")?;
            serde_json::Value::Number(array.value(row).into())
        }
        DataType::Float64 => {
            let array = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("columna Float64")?;
            let number = array.value(row);
            let number = serde_json::Number::from_f64(number)
                .with_context(|| format!("Float64 no finito: {number}"))?;
            serde_json::Value::Number(number)
        }
        DataType::Utf8 => {
            let array = column
                .as_any()
                .downcast_ref::<StringArray>()
                .context("columna Utf8")?;
            serde_json::Value::String(array.value(row).to_string())
        }
        DataType::Boolean => {
            let array = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("columna Boolean")?;
            serde_json::Value::Bool(array.value(row))
        }
        other => anyhow::bail!("la salida a un topic no publica columnas {other}"),
    };
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn a_row_keeps_the_key_as_text_and_the_payload_as_json() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("order_id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("amount", DataType::Float64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![7, 8])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
            ],
        )
        .unwrap();
        let records = records_from_batch(&batch, "order_id").unwrap();
        assert_eq!(records[0].0, "7");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&records[0].1).unwrap(),
            serde_json::json!({"order_id": 7, "name": "a", "amount": 1.5})
        );
        assert_eq!(records[1].0, "8");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&records[1].1).unwrap(),
            serde_json::json!({"order_id": 8, "name": null, "amount": null})
        );
    }

    #[test]
    fn a_null_key_is_rejected() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "order_id",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let err = records_from_batch(&batch, "order_id").unwrap_err();
        assert!(err.to_string().contains("null"), "{err}");
    }
}
