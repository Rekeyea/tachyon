//! Integración de la fuente de Redpanda con la ejecución de DataFusion.
//!
//! `RedpandaPartitionStream` implementa el trait `PartitionStream` de
//! DataFusion: es la fuente de streaming que, combinada con `StreamingTable`,
//! se ejecuta en modo `Unbounded`. Cada instancia corresponde a una partición.
//!
//! El flujo: `RecordStream` (crudo) -> batch por `batch_size` -> `Decoder` ->
//! `RecordBatch`. El `RecordStream` lo provee una factory (el consumidor
//! rdkafka en producción, una fuente in-memory en tests).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;

use crate::consumer::RecordStream;
use crate::decode::Decoder;
use futures::stream::FuturesOrdered;

/// Progreso de una fuente: partición -> próximo offset a consumir (último
/// offset emitido + 1).
///
/// `RedpandaPartitionStream` lo avanza **en el momento en que emite** cada
/// `RecordBatch` hacia DataFusion. Con un plan de pass-through (filter /
/// project, sin buffering entre operadores; ver `tachyon-runtime`), cuando el
/// runtime recibe un batch de salida, todo lo emitido hasta ahí ya produjo su
/// salida: un snapshot del tracker en ese instante son exactamente los offsets
/// que cubre ese batch. Es la base del exactly-once.
#[derive(Debug, Clone, Default)]
pub struct OffsetTracker(Arc<Mutex<BTreeMap<i32, i64>>>);

impl OffsetTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Avanza el progreso con los offsets de un batch emitido
    /// (partición -> próximo offset).
    fn advance(&self, emitted: &BTreeMap<i32, i64>) {
        let mut progress = self.0.lock().expect("lock del tracker");
        for (&partition, &next) in emitted {
            let entry = progress.entry(partition).or_insert(next);
            *entry = (*entry).max(next);
        }
    }

    /// Copia del progreso actual.
    pub fn snapshot(&self) -> BTreeMap<i32, i64> {
        self.0.lock().expect("lock del tracker").clone()
    }
}

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
    /// Progreso de offsets emitidos (exactly-once). `None` = sin tracking.
    tracker: Option<OffsetTracker>,
    /// Lotes decodificados en paralelo en el pool de blocking (el parseo es
    /// CPU-bound). La emisión preserva el orden de despacho, así que el
    /// tracking de offsets (exactly-once) es idéntico al decode inline.
    /// Default: 1 (sin paralelismo; lo deriva el runtime).
    decode_parallelism: usize,
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
            tracker: None,
            decode_parallelism: 1,
        }
    }

    /// Lotes a decodificar en paralelo (ver `decode_parallelism`).
    pub fn with_decode_parallelism(mut self, parallelism: usize) -> Self {
        self.decode_parallelism = parallelism.max(1);
        self
    }

    /// Publica en `tracker` los offsets de cada batch al emitirlo.
    pub fn with_offset_tracker(mut self, tracker: OffsetTracker) -> Self {
        self.tracker = Some(tracker);
        self
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
        let tracker = self.tracker.clone();
        let decode_parallelism = self.decode_parallelism;
        let records = (self.make_stream)();

        // Batching por tamaño O tiempo: un lote se despacha a decodificar
        // cuando (a) alcanza `batch_size`, (b) se agota `max_batch_delay`
        // desde su primer record (streams de bajo tráfico), o (c) el stream
        // de records termina (flush de cola).
        //
        // Decode paralelo: hasta `decode_parallelism` lotes se decodifican a
        // la vez en el pool de blocking (el parseo SIMD es CPU-bound). La
        // emisión respeta el orden de despacho (FuturesOrdered), así que los
        // offsets publicados en el tracker avanzan en el mismo orden en que
        // los registros se acumularon: el invariante de exactly-once es
        // idéntico al decode inline (el plan sigue siendo pass-through de
        // 1 partición).
        //
        // `offsets` acompaña a cada lote despachado: partición -> próximo
        // offset de los registros acumulados. Se publica al emitir el batch.
        let batches = async_stream::stream! {
            let mut records = records;
            let mut acc: Vec<Vec<u8>> = Vec::new();
            let mut offsets = BTreeMap::<i32, i64>::new();
            let mut since_first: Option<Instant> = None;
            let mut in_flight = FuturesOrdered::<
                tokio::task::JoinHandle<(BTreeMap<i32, i64>, DecodedLot)>,
            >::new();

            loop {
                let mut emit = false;
                if acc.len() >= batch_size {
                    // (a) Lote lleno: despachar. Con el pool lleno, drenar el
                    // más viejo antes de seguir acumulando.
                    spawn_decode(&decoder, &mut acc, &mut offsets, &mut in_flight);
                    since_first = None;
                    emit = in_flight.len() >= decode_parallelism;
                } else if !acc.is_empty()
                    && since_first.is_some_and(|t0| t0.elapsed() >= max_batch_delay)
                {
                    // (b) Límite de tiempo agotado: despachar el parcial y
                    // drenar ya (latencia acotada por `max_batch_delay`).
                    tracing::debug!(rows = acc.len(), "emitiendo batch parcial (flush time-based)");
                    spawn_decode(&decoder, &mut acc, &mut offsets, &mut in_flight);
                    since_first = None;
                    emit = true;
                } else {
                    // Esperar el siguiente lote (con timeout corto para
                    // re-verificar el límite de tiempo).
                    match tokio::time::timeout(Duration::from_millis(100), records.next()).await {
                        Ok(Some(Ok(lot))) => {
                            if since_first.is_none() {
                                since_first = Some(Instant::now());
                            }
                            for record in lot {
                                let next = offsets.entry(record.partition).or_insert(0);
                                *next = (*next).max(record.offset + 1);
                                acc.push(record.value);
                            }
                        }
                        Ok(Some(Err(e))) => {
                            yield Err(DataFusionError::Execution(e.to_string()));
                        }
                        Ok(None) => {
                            // (c) Stream terminado: flush de cola y drenado.
                            if !acc.is_empty() {
                                spawn_decode(&decoder, &mut acc, &mut offsets, &mut in_flight);
                            }
                            if in_flight.is_empty() {
                                return;
                            }
                            emit = true;
                        }
                        Err(_) => {
                            // Timeout sin datos: si no hay nada que acumular,
                            // drenar un decode en vuelo para hacer progreso.
                            emit = acc.is_empty() && !in_flight.is_empty();
                        }
                    }
                }

                if emit {
                    match in_flight.next().await {
                        Some(Ok((offs, result))) => {
                            if let Some(t) = &tracker {
                                t.advance(&offs);
                            }
                            match result {
                                Ok(batch) => yield Ok(batch),
                                Err(e) => yield Err(e),
                            }
                        }
                        Some(Err(join)) => {
                            yield Err(DataFusionError::Execution(format!(
                                "el task de decode terminó con error: {join}"
                            )));
                        }
                        None => {}
                    }
                }
            }
        };

        Box::pin(RecordBatchStreamAdapter::new(schema, batches))
    }
}

/// El resultado de decodificar un lote (offsets que cubre + batch o error).
type DecodedLot = Result<RecordBatch, DataFusionError>;

/// Despacha el decode de un lote al pool de blocking de tokio (el parseo es
/// CPU-bound puro). El lote viaja con sus offsets, que se publican en el
/// tracker cuando el batch decodificado se emite (en orden de despacho).
fn spawn_decode(
    decoder: &Decoder,
    acc: &mut Vec<Vec<u8>>,
    offsets: &mut BTreeMap<i32, i64>,
    in_flight: &mut FuturesOrdered<tokio::task::JoinHandle<(BTreeMap<i32, i64>, DecodedLot)>>,
) {
    let mut values = std::mem::take(acc);
    let offs = std::mem::take(offsets);
    let decoder = decoder.clone();
    in_flight.push_back(tokio::task::spawn_blocking(move || {
        let result = decoder
            .decode(&mut values)
            .map_err(|e| DataFusionError::Execution(e.to_string()));
        (offs, result)
    }));
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
                    futures::stream::iter(std::iter::once(Ok(vec![record.clone()]))).chain(
                        futures::stream::pending(),
                    ),
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

    #[tokio::test]
    async fn parallel_decode_preserves_order_and_offsets() {
        // 3 lotes de 2 records (order_id 1..=6) con batch_size=2 y
        // paralelismo=4: cada lote llena un batch, los 3 se decodifican en
        // paralelo pero deben EMITIRSE en orden, y el tracker debe terminar
        // en el próximo offset de la partición.
        let schema = orders_schema();
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
        let make_stream: Arc<dyn Fn() -> crate::consumer::RecordStream + Send + Sync> =
            Arc::new(move || {
                let lots: Vec<anyhow::Result<Vec<SourceRecord>>> = (0..3i64)
                    .map(|lot| {
                        Ok((1..=2i64)
                            .map(|i| {
                                let id = lot * 2 + i;
                                SourceRecord::new(
                                    0,
                                    id - 1,
                                    None,
                                    format!(r#"{{"order_id":{id},"status":"paid"}}"#).into_bytes(),
                                )
                            })
                            .collect())
                    })
                    .collect();
                Box::pin(futures::stream::iter(lots))
            });
        let tracker = OffsetTracker::new();
        let ps = RedpandaPartitionStream::new(0, schema, decoder, 2, make_stream)
            .with_offset_tracker(tracker.clone())
            .with_decode_parallelism(4);

        let mut stream = ps.execute(Arc::new(TaskContext::default()));
        let mut ids: Vec<i64> = Vec::new();
        let mut batches = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch sin error");
            assert_eq!(batch.num_rows(), 2, "un batch por lote");
            batches += 1;
            ids.extend_from_slice(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("order_id")
                    .values(),
            );
        }
        assert_eq!(batches, 3, "un batch por cada lote de 2 records");
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6], "los batches se emiten en orden");
        assert_eq!(tracker.snapshot(), BTreeMap::from([(0, 6)]));
    }

    #[tokio::test]
    async fn offset_tracker_advances_when_batches_are_emitted() {
        let schema = orders_schema();
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
        let records = vec![
            SourceRecord::new(0, 7, None, br#"{"order_id":1,"status":"paid"}"#.to_vec()),
            SourceRecord::new(1, 3, None, br#"{"order_id":2,"status":"paid"}"#.to_vec()),
            SourceRecord::new(0, 8, None, br#"{"order_id":3,"status":"paid"}"#.to_vec()),
        ];
        let make_stream = Arc::new(move || in_memory_stream(records.clone()));
        let tracker = OffsetTracker::new();
        let ps = RedpandaPartitionStream::new(0, schema, decoder, 100, make_stream)
            .with_offset_tracker(tracker.clone());

        assert!(tracker.snapshot().is_empty(), "nada emitido todavía");
        let mut stream = ps.execute(Arc::new(TaskContext::default()));
        let batch = stream.next().await.expect("un batch").expect("sin error");
        assert_eq!(batch.num_rows(), 3);
        // Próximo offset por partición = último emitido + 1.
        assert_eq!(tracker.snapshot(), BTreeMap::from([(0, 9), (1, 4)]));
    }
}
