//! Sink a un stream de Kinesis (aws-sdk-kinesis).
//!
//! At-least-once: el registro es durable al publicarse (`PutRecords`).
//! Kinesis no tiene transacciones, así que no hay un commit que grupe
//! salida y offsets de entrada (como el de Redpanda). Un reinicio vuelve a
//! leer desde la posición de arranque y repubica: el stream de salida puede
//! tener duplicados, y el consumidor dedup con la columna de secuencia
//! (el payload JSON la incluye).

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::Client;

use crate::aws;
use crate::redpanda::records_from_batch;

/// Tope de registros por `PutRecords` (límite de la API).
const MAX_RECORDS: usize = 500;
/// Reintentos de los registros que fallan (el SDK no expone backoff).
const MAX_RETRIES: u32 = 3;

/// Productor de un stream. El payload de cada fila es un objeto JSON y la
/// clave de partición es la columna `key`, en texto canónico: dos pipelines
/// que escriben la misma clave caen en el mismo shard.
pub struct KinesisSink {
    client: Client,
    stream: String,
    key: String,
}

impl KinesisSink {
    pub async fn open(
        region: &str,
        endpoint: Option<&str>,
        profile: Option<&str>,
        stream: &str,
        key: &str,
    ) -> Result<Self> {
        let sdk = aws::sdk_config(region, endpoint, profile)
            .await
            .with_context(|| format!("cargando credenciales para kinesis ({region})"))?;
        Ok(Self {
            client: Client::new(&sdk),
            stream: stream.to_string(),
            key: key.to_string(),
        })
    }

    /// Publica el batch. Devuelve la cantidad de filas. Una clave null es un
    /// error: iría a un shard arbitrario.
    pub async fn write(&self, batch: &RecordBatch) -> Result<usize> {
        let records = records_from_batch(batch, &self.key)?;
        self.write_records(&records).await
    }

    /// Publica pares `(clave, payload)` ya codificados. Lo usa el camino Avro.
    pub async fn write_records(&self, records: &[(String, Vec<u8>)]) -> Result<usize> {
        let mut offset = 0;
        while offset < records.len() {
            let end = (offset + MAX_RECORDS).min(records.len());
            self.put_chunk(&records[offset..end]).await?;
            offset = end;
        }
        Ok(records.len())
    }

    /// `PutRecords` de un lote; reintenta solo los registros que fallan.
    async fn put_chunk(&self, chunk: &[(String, Vec<u8>)]) -> Result<()> {
        let mut pending: Vec<(String, Vec<u8>)> = chunk.to_vec();
        let mut retries = 0;
        while !pending.is_empty() {
            let resp = self
                .client
                .put_records()
                .stream_name(&self.stream)
                .set_records(Some(
                    pending
                        .iter()
                        .map(|(key, data)| -> anyhow::Result<_> {
                            Ok(aws_sdk_kinesis::types::PutRecordsRequestEntry::builder()
                                .partition_key(key)
                                .data(Blob::from(data.clone()))
                                .build()?)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                ))
                .send()
                .await
                .with_context(|| format!("PutRecords ({})", self.stream))?;
            let failed: Vec<(String, Vec<u8>)> = resp
                .records()
                .iter()
                .zip(pending.iter())
                .filter(|(record, _)| record.error_code().is_some())
                .map(|(_, (key, data))| (key.clone(), data.clone()))
                .collect();
            if failed.is_empty() {
                return Ok(());
            }
            retries += 1;
            if retries > MAX_RETRIES {
                anyhow::bail!(
                    "PutRecords ({}) falló en {} registros tras {MAX_RETRIES} reintentos",
                    self.stream,
                    failed.len()
                );
            }
            pending = failed;
        }
        Ok(())
    }

    /// El registro es durable al publicarse: no hay nada que commitear.
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
    use std::time::Duration;

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

    async fn recreate_stream(client: &Client, stream: &str) {
        let _ = client.delete_stream().stream_name(stream).send().await;
        client
            .create_stream()
            .stream_name(stream)
            .shard_count(1)
            .send()
            .await
            .expect("create_stream");
        // El mock simula una creación asíncrona: esperar ACTIVE evita la
        // carrera entre CreateStream y el primer PutRecords/GetRecords.
        let t0 = std::time::Instant::now();
        loop {
            match client.describe_stream().stream_name(stream).send().await {
                Ok(r) => {
                    if r.stream_description().map(|d| d.stream_status().to_string())
                        == Some("ACTIVE".to_string())
                    {
                        return;
                    }
                }
                Err(_) => {}
            }
            assert!(
                t0.elapsed() < Duration::from_secs(30),
                "el stream no llega a ACTIVE"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn read_all(client: &Client, stream: &str) -> Vec<String> {
        let desc = client
            .describe_stream()
            .stream_name(stream)
            .send()
            .await
            .expect("describe_stream");
        let shards = desc
            .stream_description()
            .expect("stream_description")
            .shards()
            .to_vec();
        let mut out = Vec::new();
        for shard in shards {
            let shard_id = shard.shard_id().to_string();
            let mut iterator = client
                .get_shard_iterator()
                .stream_name(stream)
                .shard_id(&shard_id)
                .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon)
                .send()
                .await
                .expect("get_shard_iterator")
                .shard_iterator()
                .unwrap()
                .to_string();
            // GetRecords devuelve como mucho 100 registros: se avanza con el
            // next_shard_iterator hasta agotar el shard.
            loop {
                let resp = client
                    .get_records()
                    .shard_iterator(&iterator)
                    .limit(100)
                    .send()
                    .await
                    .expect("get_records");
                let records = resp.records();
                if records.is_empty() {
                    break;
                }
                for record in records {
                    out.push(String::from_utf8(record.data().as_ref().to_vec()).unwrap());
                }
                let Some(next) = resp.next_shard_iterator() else {
                    break;
                };
                iterator = next.to_string();
            }
        }
        out
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
    async fn kinesis_sink_writes_and_reads_back() {
        let client = sdk_client().await;
        let stream = format!("tachyon-sink-kinesis-{}", std::process::id());
        recreate_stream(&client, &stream).await;

        let sink = KinesisSink::open(REGION, Some(ENDPOINT), None, &stream, "order_id")
            .await
            .expect("open");
        let rows = sink.write(&orders_batch()).await.expect("write");
        assert_eq!(rows, 3);

        let payloads = read_all(&client, &stream).await;
        assert_eq!(payloads.len(), 3, "los 3 registros salen: {payloads:?}");
        for payload in &payloads {
            let v: serde_json::Value = serde_json::from_str(payload).expect("JSON válido");
            assert!(v.get("order_id").is_some(), "lleva la clave: {payload}");
            assert!(v.get("status").is_some(), "lleva las columnas: {payload}");
            assert!(v.get("amount").is_some(), "lleva las columnas: {payload}");
        }
        let _ = client.delete_stream().stream_name(&stream).send().await;
    }

    #[tokio::test]
    #[ignore = "requiere floCi en localhost:4566"]
    async fn kinesis_sink_chunks_over_500_records() {
        let client = sdk_client().await;
        let stream = format!("tachyon-sink-kinesis-big-{}", std::process::id());
        recreate_stream(&client, &stream).await;

        let sink = KinesisSink::open(REGION, Some(ENDPOINT), None, &stream, "order_id")
            .await
            .expect("open");
        // 501 filas fuerza dos PutRecords (500 + 1).
        let schema = Arc::new(Schema::new(vec![Field::new(
            "order_id",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from((0..501i64).collect::<Vec<_>>()))],
        )
        .expect("batch");
        let rows = sink.write(&batch).await.expect("write");
        assert_eq!(rows, 501);

        let payloads = read_all(&client, &stream).await;
        assert_eq!(payloads.len(), 501, "salen las 501: {}", payloads.len());
        let _ = client.delete_stream().stream_name(&stream).send().await;
    }
}
