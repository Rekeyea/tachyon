//! Integración de la fuente de Redpanda con la ejecución de DataFusion.
//!
//! `RedpandaPartitionStream` implementa el trait `PartitionStream` de
//! DataFusion: es la fuente de streaming que, combinada con `StreamingTable`,
//! se ejecuta en modo `Unbounded`. Cada instancia corresponde a una partición.
//!
//! El flujo: `RecordStream` (crudo) -> batch por `batch_size` -> `Decoder` ->
//! `RecordBatch`. El `RecordStream` lo provee una factory (el consumidor
//! rdkafka en producción, una fuente in-memory en tests).

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;

use crate::consumer::RecordStream;
use crate::decode::Decoder;

/// Fuente de streaming de Redpanda para una partición, integrada con DataFusion.
pub struct RedpandaPartitionStream {
    partition: i32,
    schema: SchemaRef,
    decoder: Decoder,
    batch_size: usize,
    /// Límite de tiempo para acumular un batch (flush time-based).
    max_batch_delay: Duration,
    /// Crea un `RecordStream` para esta partición. Un stream solo se consume una
    /// vez, por eso es una factory (cada `execute()` obtiene uno fresco).
    make_stream: Arc<dyn Fn() -> RecordStream + Send + Sync>,
}

impl RedpandaPartitionStream {
    pub fn new(
        partition: i32,
        schema: SchemaRef,
        decoder: Decoder,
        batch_size: usize,
        make_stream: Arc<dyn Fn() -> RecordStream + Send + Sync>,
    ) -> Self {
        Self {
            partition,
            schema,
            decoder,
            batch_size,
            max_batch_delay: Duration::from_secs(1),
            make_stream,
        }
    }

    /// Límite de tiempo para que un batch parcial se emita aunque no alcance
    /// `batch_size` (streams de bajo tráfico). Default: 1s.
    pub fn with_max_batch_delay(mut self, delay: Duration) -> Self {
        self.max_batch_delay = delay;
        self
    }

    /// La partición que sirve esta fuente.
    pub fn partition(&self) -> i32 {
        self.partition
    }
}

impl std::fmt::Debug for RedpandaPartitionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedpandaPartitionStream")
            .field("partition", &self.partition)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl PartitionStream for RedpandaPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = self.schema.clone();
        let decoder = self.decoder.clone();
        let batch_size = self.batch_size;
        let max_batch_delay = self.max_batch_delay;
        let records = (self.make_stream)();

        // Batching por tamaño O tiempo: un batch se emite cuando (a) alcanza
        // `batch_size`, (b) se agota `max_batch_delay` desde el primer record
        // del batch (streams de bajo tráfico: sin esto un batch parcial
        // nunca se emite), o (c) el stream de records termina (flush de cola).
        let batches = futures::stream::unfold(
            (records, Vec::<Vec<u8>>::new(), None::<Instant>),
            move |(mut records, mut acc, mut since_first)| {
                let decoder = decoder.clone();
                let batch_size = batch_size;
                let max_batch_delay = max_batch_delay;
                async move {
                    loop {
                        // (a) Batch lleno: emitirlo.
                        if acc.len() >= batch_size {
                            let values = std::mem::take(&mut acc);
                            let result = decoder
                                .decode(&values)
                                .map_err(|e| DataFusionError::Execution(e.to_string()));
                            return Some((result, (records, acc, None)));
                        }
                        // (b) Límite de tiempo agotado: emitir el batch parcial.
                        if let Some(t0) = since_first {
                            if t0.elapsed() >= max_batch_delay {
                                let values = std::mem::take(&mut acc);
                                tracing::debug!(
                                    rows = values.len(),
                                    "emitiendo batch parcial (flush time-based)"
                                );
                                let result = decoder
                                    .decode(&values)
                                    .map_err(|e| DataFusionError::Execution(e.to_string()));
                                return Some((result, (records, acc, None)));
                            }
                        }
                        // Esperar el siguiente record (con timeout corto para
                        // poder re-verificar el límite de tiempo).
                        match tokio::time::timeout(Duration::from_millis(100), records.next())
                            .await
                        {
                            Ok(Some(Ok(record))) => {
                                if since_first.is_none() {
                                    since_first = Some(Instant::now());
                                }
                                acc.push(record.value);
                            }
                            Ok(Some(Err(e))) => {
                                return Some((
                                    Err(DataFusionError::Execution(e.to_string())),
                                    (records, acc, None),
                                ));
                            }
                            Ok(None) => {
                                // (c) Stream terminado: flush de cola.
                                if acc.is_empty() {
                                    return None;
                                }
                                let values = std::mem::take(&mut acc);
                                let result = decoder
                                    .decode(&values)
                                    .map_err(|e| DataFusionError::Execution(e.to_string()));
                                return Some((result, (records, acc, None)));
                            }
                            Err(_) => continue, // timeout: re-verificar condiciones
                        }
                    }
                }
            },
        );

        Box::pin(RecordBatchStreamAdapter::new(schema, batches))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::in_memory_stream;
    use crate::decode::{DecodeFormat, Decoder};
    use crate::record::SourceRecord;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::streaming::StreamingTable;
    use datafusion::prelude::SessionContext;

    fn orders_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
        ]))
    }

    fn json_records() -> Vec<SourceRecord> {
        vec![
            SourceRecord::new(0, 0, None, br#"{"order_id":1,"status":"paid"}"#.to_vec()),
            SourceRecord::new(0, 1, None, br#"{"order_id":2,"status":"paid"}"#.to_vec()),
            SourceRecord::new(0, 2, None, br#"{"order_id":3,"status":"paid"}"#.to_vec()),
        ]
    }

    #[tokio::test]
    async fn partition_stream_yields_decoded_batches() {
        let schema = orders_schema();
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
        let records = json_records();
        let make_stream = Arc::new(move || in_memory_stream(records.clone()));

        let ps = RedpandaPartitionStream::new(0, schema.clone(), decoder, 100, make_stream);
        let table = StreamingTable::try_new(schema, vec![Arc::new(ps)]).expect("tabla");
        let ctx = SessionContext::new();
        ctx.register_table("orders", Arc::new(table)).expect("register");

        let df = ctx.sql("SELECT order_id, status FROM orders").await.expect("plan");
        let batches = df.collect().await.expect("collect");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "se esperaban 3 filas");

        // Verifica el contenido del primer batch.
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("columna order_id");
        assert_eq!(ids.values(), &[1, 2, 3]);
    }

    #[tokio::test]
    async fn partition_stream_flushes_partial_batch_by_time() {
        // Un batch parcial (1 record < batch_size=100) debe emitirse por el
        // límite de tiempo: el stream emite un record y se queda callado
        // (sin terminar), así que solo el flush time-based lo emite.
        let schema = orders_schema();
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
        let record = json_records().into_iter().next().unwrap();
        let make_stream: Arc<dyn Fn() -> crate::consumer::RecordStream + Send + Sync> =
            Arc::new(move || {
                Box::pin(
                    futures::stream::iter(std::iter::once(Ok(record.clone())))
                        .chain(futures::stream::pending()),
                )
            });

        let ps = RedpandaPartitionStream::new(0, schema.clone(), decoder, 100, make_stream)
            .with_max_batch_delay(std::time::Duration::from_millis(50));
        let table = StreamingTable::try_new(schema, vec![Arc::new(ps)]).expect("tabla");
        let ctx = SessionContext::new();
        ctx.register_table("orders", Arc::new(table)).expect("register");

        let df = ctx
            .sql("SELECT order_id FROM orders LIMIT 1")
            .await
            .expect("plan");
        let batches = tokio::time::timeout(std::time::Duration::from_secs(10), df.collect())
            .await
            .expect("el flush time-based debe emitir el batch")
            .expect("collect");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "el batch parcial debe emitirse por tiempo");
    }

    #[tokio::test]
    async fn partition_stream_batches_by_size() {
        let schema = orders_schema();
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
        let records = json_records();
        let make_stream = Arc::new(move || in_memory_stream(records.clone()));

        // batch_size = 2 -> los 3 records se reparten en lotes (2 + 1).
        let ps = RedpandaPartitionStream::new(0, schema.clone(), decoder, 2, make_stream);
        let table = StreamingTable::try_new(schema, vec![Arc::new(ps)]).expect("tabla");
        let ctx = SessionContext::new();
        ctx.register_table("orders", Arc::new(table)).expect("register");

        let df = ctx.sql("SELECT order_id FROM orders").await.expect("plan");
        let batches = df.collect().await.expect("collect");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "el batcheo no debe perder ni duplicar filas");
    }
}
