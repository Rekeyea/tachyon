//! Sink a una cola de SQS (aws-sdk-sqs).
//!
//! At-least-once: el mensaje es durable al publicarse (`SendMessageBatch`).
//! SQS no tiene transacciones, así que no hay un commit que grupe salida y
//! offsets de entrada (como el de Redpanda). Un reinicio vuelve a leer desde
//! la posición de arranque y repubica: la cola de salida puede tener
//! duplicados, y el consumidor dedup con la columna de secuencia (el payload
//! JSON la incluye).

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use aws_sdk_sqs::Client;

use crate::aws;
use crate::redpanda::records_from_batch;

/// Tope de mensajes por `SendMessageBatch` (límite de la API).
const MAX_MESSAGES: usize = 10;
/// Reintentos de los mensajes que fallan.
const MAX_RETRIES: u32 = 3;

/// Productor de una cola. El body de cada mensaje es el objeto JSON de la
/// fila (incluye la columna `key`); el id de entrada es secuencial dentro de
/// cada lote, solo para reintentar los que fallan.
pub struct SqsSink {
    client: Client,
    queue_url: String,
    key: String,
}

impl SqsSink {
    pub async fn open(
        region: &str,
        endpoint: Option<&str>,
        profile: Option<&str>,
        queue: &str,
        key: &str,
    ) -> Result<Self> {
        let sdk = aws::sdk_config(region, endpoint, profile)
            .await
            .with_context(|| format!("cargando credenciales para sqs ({region})"))?;
        let client = Client::new(&sdk);
        let queue_url = aws::queue_url(&client, queue).await?;
        Ok(Self {
            client,
            queue_url,
            key: key.to_string(),
        })
    }

    /// Publica el batch. Devuelve la cantidad de filas.
    pub async fn write(&self, batch: &RecordBatch) -> Result<usize> {
        let records = records_from_batch(batch, &self.key)?;
        self.write_records(&records).await
    }

    /// Publica pares `(clave, payload)` ya codificados. La clave viaja en el
    /// JSON; SQS no tiene partición por clave.
    pub async fn write_records(&self, records: &[(String, Vec<u8>)]) -> Result<usize> {
        let mut offset = 0;
        while offset < records.len() {
            let end = (offset + MAX_MESSAGES).min(records.len());
            self.send_chunk(&records[offset..end]).await?;
            offset = end;
        }
        Ok(records.len())
    }

    /// `SendMessageBatch` de un lote; reintenta solo los mensajes que fallan.
    async fn send_chunk(&self, chunk: &[(String, Vec<u8>)]) -> Result<()> {
        let mut pending: Vec<(String, Vec<u8>)> = chunk.to_vec();
        let mut retries = 0;
        while !pending.is_empty() {
            let resp = self
                .client
                .send_message_batch()
                .queue_url(&self.queue_url)
                .set_entries(Some(
                    pending
                        .iter()
                        .enumerate()
                        .map(|(index, (_key, data))| -> anyhow::Result<_> {
                            let body = String::from_utf8(data.clone())
                                .context("el payload JSON no es UTF-8")?;
                            Ok(aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                                .id(index.to_string())
                                .message_body(body)
                                .build()?)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                ))
                .send()
                .await
                .with_context(|| format!("SendMessageBatch ({})", self.queue_url))?;
            let failed: Vec<(String, Vec<u8>)> = resp
                .failed()
                .iter()
                .map(|failed| failed.id())
                .filter_map(|id| id.parse::<usize>().ok())
                .filter(|index| *index < pending.len())
                .map(|index| pending[index].clone())
                .collect();
            if failed.is_empty() {
                return Ok(());
            }
            retries += 1;
            if retries > MAX_RETRIES {
                anyhow::bail!(
                    "SendMessageBatch ({}) falló en {} mensajes tras {MAX_RETRIES} reintentos",
                    self.queue_url,
                    failed.len()
                );
            }
            pending = failed;
        }
        Ok(())
    }

    /// El mensaje es durable al publicarse: no hay nada que commitear.
    pub async fn commit(&self) -> Result<()> {
        Ok(())
    }

    pub async fn abort(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    const ENDPOINT: &str = "http://localhost:4566";
    const REGION: &str = "us-east-1";

    async fn sdk_client() -> Client {
        // floCi acepta cualquier credencial.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        }
        let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_types::region::Region::new(REGION.to_string()))
            .endpoint_url(ENDPOINT.to_string())
            .load()
            .await;
        Client::new(&sdk)
    }

    async fn queue_url(client: &Client, queue: &str) -> String {
        let resp = client
            .get_queue_url()
            .queue_name(queue)
            .send()
            .await
            .expect("get_queue_url");
        resp.queue_url().unwrap().to_string()
    }

    fn orders_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(StringArray::from(vec!["paid", "shipped", "paid"])),
                Arc::new(arrow::array::Float64Array::from(vec![100.0, 200.0, 300.0])),
            ],
        )
        .expect("batch")
    }

    #[tokio::test]
    #[ignore = "requiere floCi en localhost:4566"]
    async fn sqs_sink_writes_and_reads_back() {
        let client = sdk_client().await;
        let queue = format!("tachyon-sink-sqs-{}", std::process::id());
        client
            .create_queue()
            .queue_name(&queue)
            .send()
            .await
            .expect("create_queue");

        let sink = SqsSink::open(REGION, Some(ENDPOINT), None, &queue, "order_id")
            .await
            .expect("open");
        let rows = sink.write(&orders_batch()).await.expect("write");
        assert_eq!(rows, 3);

        let url = queue_url(&client, &queue).await;
        let resp = client
            .receive_message()
            .queue_url(&url)
            .max_number_of_messages(10)
            .send()
            .await
            .expect("receive_message");
        let messages = resp.messages();
        assert_eq!(messages.len(), 3, "los 3 mensajes salen");
        for message in messages {
            let body = message.body().unwrap();
            let v: serde_json::Value = serde_json::from_str(body).expect("JSON válido");
            assert!(v.get("order_id").is_some(), "lleva la clave: {body}");
            assert!(v.get("status").is_some(), "lleva las columnas: {body}");
            assert!(v.get("amount").is_some(), "lleva las columnas: {body}");
        }
        let _ = client.delete_queue().queue_url(&url).send().await;
    }

    #[tokio::test]
    #[ignore = "requiere floCi en localhost:4566"]
    async fn sqs_sink_chunks_over_10_messages() {
        let client = sdk_client().await;
        let queue = format!("tachyon-sink-sqs-big-{}", std::process::id());
        client
            .create_queue()
            .queue_name(&queue)
            .send()
            .await
            .expect("create_queue");

        let sink = SqsSink::open(REGION, Some(ENDPOINT), None, &queue, "order_id")
            .await
            .expect("open");
        // 11 filas fuerza dos SendMessageBatch (10 + 1).
        let schema = Arc::new(Schema::new(vec![Field::new(
            "order_id",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from((0..11i64).collect::<Vec<_>>()))],
        )
        .expect("batch");
        let rows = sink.write(&batch).await.expect("write");
        assert_eq!(rows, 11);

        let url = queue_url(&client, &queue).await;
        let mut count = 0;
        // Receive es destructivo (visibility): se juntan los lotes de a 10.
        loop {
            let resp = client
                .receive_message()
                .queue_url(&url)
                .max_number_of_messages(10)
                .send()
                .await
                .expect("receive_message");
            let messages = resp.messages();
            count += messages.len();
            if messages.is_empty() {
                break;
            }
        }
        assert_eq!(count, 11, "salen los 11: {count}");
        let _ = client.delete_queue().queue_url(&url).send().await;
    }
}
