//! Consumo de Redpanda por consumer group (rdkafka).
//!
//! Este módulo contiene la parte que **depende de un broker**: el consumidor
//! rdkafka. Produce un `RecordStream` (stream de `SourceRecord`) que el
//! `PartitionStream` (ver `stream.rs`) decodifica a `RecordBatch`.
//!
//! El resto del conector (decoder + `PartitionStream`) se testea con una fuente
//! in-memory que produce los mismos `SourceRecord`, sin broker.
//!
//! Nota: el consumidor rdkafka se valida contra un broker real (Slice 2, test
//! con Redpanda local). Aquí se garantiza que compila y que el wiring es
//! correcto; el poll loop se ejercita en el test con broker.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use futures::stream;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::ClientConfig;
use rdkafka::Message;

use crate::record::SourceRecord;

/// Un stream de registros crudos (la abstracción que decoupla el broker).
pub type RecordStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<SourceRecord>> + Send + 'static>>;

/// Consumidor de Redpanda por consumer group.
///
/// El consumer group se encarga del particionado por clave y la asignación de
/// particiones: al suscribirse al topic, el protocolo de grupo reparte las
/// particiones entre las instancias del mismo `group.id`.
///
/// El `BaseConsumer` vive en un `Arc<Mutex<Option<_>>>` para que el stream de
/// registros lo use en el poll y el runtime pueda recuperarlo después para el
/// commit de offsets (Slice 5: exactly-once en Fase 1, at-least-once +
/// idempotencia por sequence en el MVP).
pub struct RdkafkaSource {
    consumer: Arc<Mutex<Option<BaseConsumer>>>,
}

impl RdkafkaSource {
    /// Crea un consumidor y se suscribe al `topic` (consumer group).
    ///
    /// `config` debe incluir al menos `group.id` y `bootstrap.servers`.
    pub fn new(config: &ClientConfig, topic: &str) -> Result<Self> {
        let consumer: BaseConsumer = config.create().context("creando consumidor rdkafka")?;
        consumer
            .subscribe(&[topic])
            .context("suscriciendose al topic")?;
        Ok(Self {
            consumer: Arc::new(Mutex::new(Some(consumer))),
        })
    }

    /// Commitea los offsets consumidos (consumer group).
    ///
    /// En el MVP es at-least-once: si el proceso muere entre el commit de
    /// Paimon y este commit, los eventos se re-procesan a la reconexión; la
    /// idempotencia la da el sequence number en Paimon (dedup por clave).
    /// En la Fase 1 el commit de offsets es atómico con el commit de Paimon
    /// (ver DESIGN.md §9.2.4).
    pub fn commit(&self) -> Result<()> {
        let guard = self
            .consumer
            .lock()
            .map_err(|e| anyhow::anyhow!("lock de consumer: {e}"))?;
        match guard.as_ref() {
            Some(consumer) => consumer
                .commit_consumer_state(CommitMode::Async)
                .context("commit de offsets rdkafka"),
            None => {
                // El stream ya tomó el consumer y no lo devolvió (terminó o
                // falló): no hay offsets que commitar.
                Ok(())
            }
        }
    }

    /// Produce un stream de registros del consumidor (poll continuo).
    ///
    /// El poll loop corre en un **task dedicado** que posee el `BaseConsumer`
    /// y envía cada `SourceRecord` por un canal mpsc. El stream devuelto es el
    /// receiver: cancelarlo (p. ej. por un timeout de batching) NO afecta al
    /// producer ni al consumer, que sigue pollando y enviando. Esto hace el
    /// stream cancelation-safe (necesario para el batching time-based de
    /// `RedpandaPartitionStream`).
    ///
    /// El `poll` de rdkafka es bloqueante, así que el task corre en
    /// `spawn_blocking` para no bloquear el runtime async.
    pub fn record_stream(&self) -> RecordStream {
        let poll_timeout = Duration::from_secs(1);
        let state = self.consumer.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            // El consumer vive dentro del task de poll: se toma una sola vez
            // (no por poll) y el loop lo reutiliza en cada iteración.
            let consumer = state
                .lock()
                .expect("lock del consumer")
                .take()
                .expect("consumidor presente");
            let _ = tokio::task::spawn_blocking(move || {
                let mut consumer = consumer;
                loop {
                    // Si el receiver se soltó (el stream de datos terminó),
                    // salir: dejar el `BaseConsumer` en el suelo mantendría
                    // vivos los threads de fondo de librdkafka y el proceso
                    // no podría terminar.
                    if tx.is_closed() {
                        break;
                    }
                    match consumer.poll(poll_timeout) {
                        Some(Ok(message)) => {
                            // El `BorrowedMessage` de rdkafka no puede escapar
                            // del poll, así que se convierte a `SourceRecord`
                            // (owned) aquí.
                            let record = message_to_record(&message);
                            if tx.blocking_send(record).is_err() {
                                break; // receiver caído: nada más que hacer
                            }
                        }
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "error de poll rdkafka");
                            break;
                        }
                        None => continue, // timeout sin mensajes
                    }
                }
            })
            .await;
            // El task de poll terminó (error o shutdown): el receiver verá el
            // canal cerrado y el stream terminará.
        });

        Box::pin(ReceiverStream { rx })
    }
}

/// Stream de `SourceRecord` desde el canal mpsc del task de poll.
///
/// Implementa `Stream` directamente (sin macro) para evitar la dependencia
/// `async-stream`. Es cancelation-safe: soltar el stream no afecta al task
/// de poll, que sigue poseyendo el `BaseConsumer`.
struct ReceiverStream {
    rx: tokio::sync::mpsc::Receiver<SourceRecord>,
}

impl futures::Stream for ReceiverStream {
    type Item = Result<SourceRecord>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match std::pin::Pin::new(&mut self.rx).poll_recv(cx) {
            std::task::Poll::Ready(Some(record)) => {
                tracing::debug!(
                    partition = record.partition,
                    offset = record.offset,
                    "record consumido de Redpanda"
                );
                std::task::Poll::Ready(Some(Ok(record)))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None), // canal cerrado
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// Convierte un mensaje de rdkafka en un `SourceRecord`.
fn message_to_record(message: &impl Message) -> SourceRecord {
    SourceRecord::new(
        message.partition(),
        message.offset(),
        message.key().map(|k| k.to_vec()),
        message.payload().map(|p| p.to_vec()).unwrap_or_default(),
    )
}

/// Fuente in-memory de registros (para tests y para emular el broker).
///
/// Produce un `RecordStream` a partir de una lista de `SourceRecord`.
pub fn in_memory_stream(records: Vec<SourceRecord>) -> RecordStream {
    Box::pin(stream::iter(records.into_iter().map(Ok)))
}
