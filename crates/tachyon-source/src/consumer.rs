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

/// Un stream de **lotes** de registros crudos (la abstracción que decoupla el
/// broker). Cada elemento es un `Vec<SourceRecord>` (un lote drenado del buffer
/// interno de librdkafka). Emitir lotes en vez de un registro a la vez reduce el
/// overhead por mensaje (un salto de canal mpsc por lote, no por registro).
pub type RecordStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<Vec<SourceRecord>>> + Send + 'static>>;

/// Consumidor de Redpanda por consumer group.
///
/// El consumer group se encarga del particionado por clave y la asignación de
/// particiones: al suscribirse al topic, el protocolo de grupo reparte las
/// particiones entre las instancias del mismo `group.id`.
///
/// El `BaseConsumer` vive en un `Arc<Mutex<Option<_>>>` hasta que `record_stream()`
/// lo `take()`a y lo mueve al task de poll (que lo posee y lo reutiliza). Como
/// `BaseConsumer` no es `Sync`, no puede compartirse entre threads vía `Arc`; el
/// commit de offsets se delega al task de poll a través de un canal de comandos
/// (`commit_tx`): `commit()` envía un comando y el task (que posee el consumer)
/// lo ejecuta y responde. Sin esto, `commit()` vería el `Option` vacío y
/// silenciosamente no commitaría nada (bug de recuperación).
pub struct RdkafkaSource {
    consumer: Arc<Mutex<Option<BaseConsumer>>>,
    /// Canal de comandos de commit hacia el task de poll.
    commit_tx: tokio::sync::mpsc::UnboundedSender<CommitCommand>,
    /// Receiver del canal de commit (se mueve al task de poll en `record_stream`).
    commit_rx: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<CommitCommand>>>,
}

/// Comando de commit de offsets: el task de poll lo ejecuta sobre el
/// `BaseConsumer` que posee y responde con el resultado.
struct CommitCommand {
    reply: tokio::sync::oneshot::Sender<Result<(), String>>,
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
        let (commit_tx, commit_rx) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            consumer: Arc::new(Mutex::new(Some(consumer))),
            commit_tx,
            commit_rx: Mutex::new(Some(commit_rx)),
        })
    }

    /// Commitea los offsets consumidos (consumer group).
    ///
    /// El `BaseConsumer` vive en el task de poll (no es `Sync`, no puede
    /// compartirse), así que el commit se delega: se envía un comando por el
    /// canal y el task (que posee el consumer) lo ejecuta y responde. Se espera
    /// la respuesta con timeout para no colgar si el task de poll ya terminó.
    ///
    /// En el MVP es at-least-once: si el proceso muere entre el commit de
    /// Paimon y este commit, los eventos se re-procesan a la reconexión; la
    /// idempotencia la da el sequence number en Paimon (dedup por clave).
    /// En la Fase 1 el commit de offsets es atómico con el commit de Paimon
    /// (ver DESIGN.md §9.2.4).
    pub async fn commit(&self) -> Result<()> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.commit_tx
            .send(CommitCommand { reply: reply_tx })
            .map_err(|_| anyhow::anyhow!("task de poll no disponible para commit de offsets"))?;
        let result = tokio::time::timeout(Duration::from_secs(5), reply_rx)
            .await
            .map_err(|_| anyhow::anyhow!("timeout esperando el commit de offsets"))?
            .map_err(|_| anyhow::anyhow!("el task de poll terminó antes de commitar"))?;
        result.map_err(|e| anyhow::anyhow!(e))
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
        // Tope de registros por lote (acota la memoria por envío al mpsc). Se
        // drena todo el buffer de librdkafka por envío: cada envío que toca la
        // red es un round trip al broker, así que un lote grande amortiza el
        // round trip sobre muchos más registros (con el tope antiguo de 1000,
        // el loop hacía un round trip cada ~1000 filas y el throughput quedaba
        // acotado por la latencia de red, no por el cómputo).
        let max_batch = 65536usize;
        let state = self.consumer.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        // El receiver del canal de commit se mueve al task de poll (no es Clone).
        let commit_rx = self
            .commit_rx
            .lock()
            .expect("lock del canal de commit")
            .take()
            .expect("canal de commit presente");

        tokio::spawn(async move {
            // El consumer vive dentro del task de poll: se toma una sola vez
            // (no por poll) y el loop lo reutiliza en cada iteración.
            let consumer = state
                .lock()
                .expect("lock del consumer")
                .take()
                .expect("consumidor presente");
            let _ = tokio::task::spawn_blocking(move || {
                let consumer = consumer;
                let mut commit_rx = commit_rx;
                // Instrumentación de diagnóstico (solo con TACHYON_DEBUG_POLL=1):
                // permite ver si el task de poll arranca, recibe mensajes y
                // logra enviarlos por el canal mpsc.
                let debug_poll = std::env::var("TACHYON_DEBUG_POLL").is_ok();
                if debug_poll {
                    eprintln!("[poll] task iniciado");
                }
                let mut sent_batches = 0u64;
                let mut sent_rows = 0u64;
                let mut last_report = std::time::Instant::now();
                loop {
                    // Si el receiver se soltó (el stream de datos terminó),
                    // salir: dejar el `BaseConsumer` en el suelo mantendría
                    // vivos los threads de fondo de librdkafka y el proceso
                    // no podría terminar.
                    if tx.is_closed() {
                        break;
                    }
                    // Drenar comandos de commit pendientes. `CommitMode::Async`:
                    // el commit se encola y el ack llega en background; NO
                    // bloquea el poll (un Sync aquí paralizaba el consumo).
                    loop {
                        match commit_rx.try_recv() {
                            Ok(cmd) => {
                                let result = consumer
                                    .commit_consumer_state(CommitMode::Async)
                                    .map_err(|e| e.to_string());
                                let _ = cmd.reply.send(result);
                            }
                            Err(_) => break, // Empty o Disconnected
                        }
                    }
                    // Consumir un LOTE: el primer `poll` bloquea (espera hasta
                    // `poll_timeout` por el primer mensaje); luego se drena el
                    // buffer interno de librdkafka con `poll(0)` (no bloquea)
                    // hasta vaciarlo o alcanzar `max_batch`. Un solo envío al
                    // mpsc por lote (no por registro).
                    let mut batch: Vec<SourceRecord> = Vec::new();
                    match consumer.poll(poll_timeout) {
                        Some(Ok(message)) => {
                            batch.push(message_to_record(&message));
                            while batch.len() < max_batch {
                                match consumer.poll(Duration::from_millis(0)) {
                                    Some(Ok(m)) => batch.push(message_to_record(&m)),
                                    Some(Err(e)) => {
                                        tracing::warn!(error = %e, "error de poll rdkafka");
                                        break;
                                    }
                                    None => break, // buffer vacío
                                }
                            }
                        }
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "error de poll rdkafka");
                            if debug_poll {
                                eprintln!("[poll] ERROR en poll(timeout): {e} -> el task de poll TERMINA");
                            }
                            break;
                        }
                        None => continue, // timeout sin mensajes
                    }
                    if batch.is_empty() {
                        if debug_poll
                            && last_report.elapsed() > std::time::Duration::from_secs(2)
                        {
                            eprintln!(
                                "[poll] {sent_batches} lotes / {sent_rows} filas enviados hasta ahora (poll sin mensajes aún?)"
                            );
                            last_report = std::time::Instant::now();
                        }
                        continue;
                    }
                    let batch_len = batch.len();
                    if tx.blocking_send(batch).is_err() {
                        break; // receiver caído: nada más que hacer
                    }
                    sent_batches += 1;
                    sent_rows += batch_len as u64;
                    if debug_poll && last_report.elapsed() > std::time::Duration::from_secs(2) {
                        eprintln!(
                            "[poll] {sent_batches} lotes / {sent_rows} filas enviados (media {:.0} filas/lote)",
                            sent_rows as f64 / sent_batches as f64
                        );
                        last_report = std::time::Instant::now();
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

/// Stream de lotes de `SourceRecord` desde el canal mpsc del task de poll.
///
/// Implementa `Stream` directamente (sin macro) para evitar la dependencia
/// `async-stream`. Es cancelation-safe: soltar el stream no afecta al task
/// de poll, que sigue poseyendo el `BaseConsumer`.
struct ReceiverStream {
    rx: tokio::sync::mpsc::Receiver<Vec<SourceRecord>>,
}

impl futures::Stream for ReceiverStream {
    type Item = Result<Vec<SourceRecord>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match std::pin::Pin::new(&mut self.rx).poll_recv(cx) {
            std::task::Poll::Ready(Some(batch)) => {
                tracing::debug!(
                    rows = batch.len(),
                    "lote consumido de Redpanda"
                );
                std::task::Poll::Ready(Some(Ok(batch)))
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
/// Produce un `RecordStream` (lotes) a partir de una lista de `SourceRecord`.
/// Todos los registros van en un único lote (los tests no miden throughput).
pub fn in_memory_stream(records: Vec<SourceRecord>) -> RecordStream {
    Box::pin(stream::iter(std::iter::once(Ok(records))))
}
