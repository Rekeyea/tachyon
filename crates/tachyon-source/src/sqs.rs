//! Consumo de una cola SQS (aws-sdk-sqs).
//!
//! N consumidores compiten por la cola: cada uno hace `ReceiveMessage` con
//! long polling (`wait_time_seconds`) y recibe hasta 10 mensajes.
//!
//! La garantía es at-least-once: los mensajes se borran SOLO después de que
//! el pipeline los commiteó a Paimon (el runtime hace `DeleteMessage` en
//! lotes de 10 tras cada commit). Los duplicados los deduplica el sink
//! (PK + `sequence.field`). El visibility timeout debe superar el intervalo
//! de commit más un margen: un mensaje que vuelve a ser visible antes del
//! commit se consume dos veces.
//!
//! El receipt handle viaja en `SourceRecord.position`: el task de decode lo
//! extrae por batch (ver `stream.rs`) y el writer lo borra de SQS después
//! del commit.

use std::time::Duration;

use aws_sdk_sqs::Client;

use crate::consumer::{ReceiverStream, RecordStream};
use crate::record::SourceRecord;

/// Tope de mensajes por `ReceiveMessage` (límite de la API).
const MAX_MESSAGES: i32 = 10;
/// Tope del long polling (segundos; la API acepta 1..=20).
const MAX_WAIT_SECONDS: i32 = 20;
/// Tope del visibility timeout (segundos; la API admite hasta 12h).
const MAX_VISIBILITY_SECONDS: i32 = 43_200;

/// Fuente de una cola SQS: uno de N consumidores que compiten por la cola.
#[derive(Clone)]
pub struct SqsSource {
    client: Client,
    queue_url: String,
    /// Espera del long polling (1..=20 s).
    wait: Duration,
    /// Tope de mensajes por `ReceiveMessage` (la API limita a 10).
    max_messages: usize,
    /// Visibility timeout por receive. Debe superar el intervalo de commit
    /// más un margen (ver el encabezado del módulo).
    visibility_timeout: Option<Duration>,
}

impl SqsSource {
    pub fn new(client: Client, queue_url: &str, wait: Duration) -> Self {
        Self {
            client,
            queue_url: queue_url.to_string(),
            wait,
            max_messages: MAX_MESSAGES as usize,
            visibility_timeout: None,
        }
    }

    /// Visibility timeout por receive. Debe superar el intervalo de commit
    /// más un margen.
    pub fn with_visibility_timeout(mut self, timeout: Duration) -> Self {
        self.visibility_timeout = Some(timeout);
        self
    }

    /// Tope de mensajes por `ReceiveMessage` (la API limita a 10).
    pub fn with_max_messages(mut self, max_messages: usize) -> Self {
        self.max_messages = max_messages.max(1);
        self
    }

    /// URL de la cola (para el log y el commit de borrados).
    pub fn queue_url(&self) -> &str {
        &self.queue_url
    }

    /// Produce un stream de lotes del consumidor (long polling continuo).
    ///
    /// Cada llamada arranca un loop independiente: N llamadas = N
    /// consumidores compitiendo por la misma cola.
    pub fn record_stream(&self) -> RecordStream {
        let client = self.client.clone();
        let queue_url = self.queue_url.clone();
        let wait = self.wait;
        let max_messages = self.max_messages;
        let visibility_timeout = self.visibility_timeout;
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            sqs_poll_loop(
                client,
                queue_url,
                wait,
                max_messages,
                visibility_timeout,
                tx,
            )
            .await;
        });
        Box::pin(ReceiverStream { rx })
    }
}

async fn sqs_poll_loop(
    client: Client,
    queue_url: String,
    wait: Duration,
    max_messages: usize,
    visibility_timeout: Option<Duration>,
    tx: tokio::sync::mpsc::Sender<Result<Vec<SourceRecord>, String>>,
) {
    let wait_seconds = wait.as_secs().clamp(1, MAX_WAIT_SECONDS as u64) as i32;
    let max_messages = max_messages.min(MAX_MESSAGES as usize) as i32;
    let visibility =
        visibility_timeout.map(|t| t.as_secs().clamp(0, MAX_VISIBILITY_SECONDS as u64) as i32);
    tracing::info!(queue = %queue_url, wait_seconds, "consumo SQS arrancado");
    loop {
        if tx.is_closed() {
            break;
        }
        let mut req = client
            .receive_message()
            .queue_url(queue_url.clone())
            .max_number_of_messages(max_messages)
            .wait_time_seconds(wait_seconds);
        if let Some(v) = visibility {
            req = req.visibility_timeout(v);
        }
        match req.send().await {
            Ok(resp) => {
                let messages = resp.messages().to_vec();
                if messages.is_empty() {
                    continue; // long polling vacío: al loop
                }
                let mut lot: Vec<SourceRecord> = Vec::new();
                for msg in &messages {
                    let Some(receipt) = msg.receipt_handle() else {
                        continue;
                    };
                    lot.push(SourceRecord {
                        partition: 0,
                        offset: 0,
                        key: None,
                        value: msg
                            .body()
                            .map(|b| b.as_bytes().to_vec())
                            .unwrap_or_default(),
                        position: Some(receipt.to_string()),
                    });
                }
                if lot.is_empty() {
                    continue;
                }
                // `send` espera lugar en el canal (backpressure).
                if tx.send(Ok(lot)).await.is_err() {
                    return; // receiver caído
                }
            }
            Err(e) => {
                // Fallar en voz alta: saltar un receive perdería mensajes.
                let _ = tx
                    .send(Err(format!("ReceiveMessage ({queue_url}): {e}")))
                    .await;
                return;
            }
        }
    }
}
