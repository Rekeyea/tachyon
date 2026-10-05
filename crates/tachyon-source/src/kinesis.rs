//! Consumo de un stream Kinesis (aws-sdk-kinesis).
//!
//! Exactamente un consumidor por stream: todos los shards en paralelo. La
//! escala entra por los shards. Cada shard tiene un índice de partición
//! estable (se asigna en orden lexicográfico de shard id al arrancar; un
//! shard nuevo de split/merge toma el siguiente índice).
//!
//! Produce un `RecordStream` (lotes de `SourceRecord`) que el
//! `RedpandaPartitionStream` decodifica. La posición de cada lote (shard ->
//! último seq) se publica por `LotPositions` y se consolida en el
//! `PositionTracker` al emitir el batch (ver `stream.rs`).
//!
//! El polling corre en un task por shard. Si el canal hacia el decoder se
//! cierra, los tasks terminan.
//!
//! `GetRecords` sale por `KinesisReader` (CBOR) cuando hay credenciales. Si el
//! endpoint contesta JSON o 415, ese shard se queda en el cliente del SDK.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aws_sdk_kinesis::types::{Shard, ShardIteratorType};
use aws_sdk_kinesis::Client;
use tokio::task::JoinHandle;

use crate::consumer::{ReceiverStream, RecordStream};
use crate::kinesis_read::{GetRecordsPage, RawRecord, ReadError};
use crate::record::SourceRecord;

pub use crate::kinesis_read::KinesisReader;

/// Lotes en cola del poll hacia el decoder. Un lote es un shard: con 16
/// shards, 4 huecos volvían a serializar la vuelta.
const LOT_QUEUE: usize = 32;
/// Pacing del loop cuando una vuelta no trae registros (el SDK no expone
/// espera de servidor en `GetRecords`).
const IDLE_POLL: Duration = Duration::from_millis(200);
/// Re-`DescribeStream` para ver splits/merges nuevos (un hijo cuyo padre ya
/// cerró y no llegó por `child_shards` de `GetRecords`).
const RE_DESCRIBE: Duration = Duration::from_secs(30);
/// Tope de registros por `GetRecords` (el tope de la API es 10000).
const MAX_LIMIT: i32 = 10_000;

/// Punto de arranque de un stream sin checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartPosition {
    /// Desde el final del stream: solo registros nuevos.
    Latest,
    /// Desde el registro más antiguo disponible en cada shard.
    TrimHorizon,
}

/// Posiciones Kinesis de cada lote (shard -> último seq del lote), en el
/// mismo orden que los lotes (uno por lote, FIFO). Mismo contrato que
/// `LotRanges`.
#[derive(Debug, Clone, Default)]
pub struct LotPositions(Arc<Mutex<std::collections::VecDeque<Vec<(String, String)>>>>);

impl LotPositions {
    pub fn new() -> Self {
        Self::default()
    }

    fn push(&self, positions: Vec<(String, String)>) {
        self.0
            .lock()
            .expect("lock de posiciones")
            .push_back(positions);
    }

    /// Las posiciones del próximo lote recibido.
    pub fn pop(&self) -> Option<Vec<(String, String)>> {
        self.0.lock().expect("lock de posiciones").pop_front()
    }
}

/// Fuente de un stream Kinesis: un consumidor que lee todos los shards en
/// paralelo.
#[derive(Clone)]
pub struct KinesisSource {
    client: Client,
    stream_name: String,
    start_from: StartPosition,
    /// Seq del último registro consumido por shard (resume). Se mueve al
    /// task de poll en `record_stream`.
    resume: Arc<Mutex<Option<BTreeMap<String, String>>>>,
    /// Tope de registros por `GetRecords` (shard). Debe coincidir con el
    /// `batch_size` del stream.
    max_batch: usize,
    /// Canal de posiciones por lote (ver `LotPositions`).
    lot_positions: Arc<Mutex<Option<LotPositions>>>,
    /// `GetRecords` en CBOR. `None` deja todos los shards en el SDK.
    reader: Arc<Mutex<Option<KinesisReader>>>,
}

impl KinesisSource {
    pub fn new(client: Client, stream_name: &str, start_from: StartPosition) -> Self {
        Self {
            client,
            stream_name: stream_name.to_string(),
            start_from,
            resume: Arc::new(Mutex::new(None)),
            max_batch: 32_768,
            lot_positions: Arc::new(Mutex::new(None)),
            reader: Arc::new(Mutex::new(None)),
        }
    }

    /// Lee `GetRecords` en CBOR. Sin esto, cada shard usa el cliente del SDK.
    pub fn with_reader(self, reader: KinesisReader) -> Self {
        *self.reader.lock().expect("lock del lector") = Some(reader);
        self
    }

    /// El stream termina en un carril con `PositionTracker`: cada lote sale
    /// con sus posiciones. Hay que llamarlo antes de `record_stream`.
    pub fn with_lot_positions(self, positions: LotPositions) -> Self {
        *self.lot_positions.lock().expect("lock de posiciones") = Some(positions);
        self
    }

    /// Tope de registros por `GetRecords` (shard). Debe coincidir con el
    /// `batch_size` del stream.
    pub fn with_max_batch(mut self, max_batch: usize) -> Self {
        self.max_batch = max_batch.max(1);
        self
    }

    /// Re-anuda el consumo en los seq de un checkpoint (shard -> último seq
    /// consumido). El iterator arranca en `AFTER_SEQUENCE_NUMBER`. Hay que
    /// llamarlo antes de `record_stream`.
    pub fn with_resume_positions(self, positions: BTreeMap<String, String>) -> Self {
        *self.resume.lock().expect("lock de resume") = Some(positions);
        self
    }

    /// Produce un stream de lotes del consumidor (poll continuo).
    pub fn record_stream(&self) -> RecordStream {
        let client = self.client.clone();
        let stream_name = self.stream_name.clone();
        let start_from = self.start_from;
        let resume = self
            .resume
            .lock()
            .expect("lock de resume")
            .take()
            .unwrap_or_default();
        let lot_positions = self
            .lot_positions
            .lock()
            .expect("lock de posiciones")
            .take();
        let max_batch = self.max_batch;
        let reader = self.reader.lock().expect("lock del lector").take();
        let (tx, rx) = tokio::sync::mpsc::channel(LOT_QUEUE);
        tokio::spawn(async move {
            poll_loop(
                client,
                reader,
                stream_name,
                start_from,
                resume,
                lot_positions,
                max_batch,
                tx,
            )
            .await;
        });
        Box::pin(ReceiverStream { rx })
    }
}

/// `(tipo de iterator, seq)` para arrancar un shard.
///
/// Prioridad: checkpoint (`AFTER_SEQUENCE_NUMBER` del último seq consumido)
/// > hijo de split/merge (`TRIM_HORIZON`: datos nuevos, nunca consumidos)
/// > arranque del stream (`LATEST` o `TRIM_HORIZON` según `start_from`).
fn iterator_for(
    shard: &Shard,
    resume: &BTreeMap<String, String>,
    start_from: StartPosition,
) -> (ShardIteratorType, Option<String>) {
    if let Some(seq) = resume.get(shard.shard_id()).map(|seq| seq.to_string()) {
        return (ShardIteratorType::AfterSequenceNumber, Some(seq));
    }
    if shard.parent_shard_id().is_some() {
        return (ShardIteratorType::TrimHorizon, None);
    }
    match start_from {
        StartPosition::Latest => (ShardIteratorType::Latest, None),
        StartPosition::TrimHorizon => (ShardIteratorType::TrimHorizon, None),
    }
}

/// Describe el stream y devuelve el mapa shard id -> info (paginado por
/// `exclusive_start_shard_id`).
async fn describe_shards(client: &Client, stream: &str) -> Result<BTreeMap<String, Shard>, String> {
    let mut shards = BTreeMap::new();
    let mut start: Option<String> = None;
    loop {
        let mut req = client.describe_stream().stream_name(stream);
        if let Some(s) = &start {
            req = req.exclusive_start_shard_id(s.clone());
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("DescribeStream ({stream}): {e}"))?;
        let desc = resp
            .stream_description()
            .ok_or_else(|| format!("DescribeStream ({stream}) sin stream_description"))?;
        let last = desc
            .shards()
            .iter()
            .rev()
            .map(|s| s.shard_id().to_string())
            .next();
        for shard in desc.shards() {
            shards.insert(shard.shard_id().to_string(), shard.clone());
        }
        if !desc.has_more_shards() {
            break;
        }
        start = last;
    }
    Ok(shards)
}

/// Crea el iterator de arranque de un shard. Si el seq del checkpoint ya no
/// está disponible (retención), el shard arranca en `TRIM_HORIZON` (el más
/// antiguo disponible).
async fn get_iterator(
    client: &Client,
    stream: &str,
    shard_id: &str,
    (iterator_type, seq): (ShardIteratorType, Option<String>),
) -> Result<String, String> {
    let mut req = client
        .get_shard_iterator()
        .stream_name(stream)
        .shard_id(shard_id)
        .shard_iterator_type(iterator_type);
    if let Some(seq) = &seq {
        req = req.starting_sequence_number(seq);
    }
    match req.send().await {
        Ok(resp) => resp
            .shard_iterator()
            .map(|it| it.to_string())
            .ok_or_else(|| format!("GetShardIterator ({stream}/{shard_id}) sin iterator")),
        Err(e) if seq.is_some() => {
            tracing::warn!(
                stream,
                shard_id,
                error = %e,
                "el seq del checkpoint no está disponible; el shard arranca en TRIM_HORIZON"
            );
            client
                .get_shard_iterator()
                .stream_name(stream)
                .shard_id(shard_id)
                .shard_iterator_type(ShardIteratorType::TrimHorizon)
                .send()
                .await
                .map(|resp| {
                    resp.shard_iterator()
                        .map(|it| it.to_string())
                        .ok_or_else(|| {
                            format!("GetShardIterator ({stream}/{shard_id}) sin iterator")
                        })
                })
                .map_err(|e| format!("GetShardIterator ({stream}/{shard_id}): {e}"))?
        }
        Err(e) => Err(format!("GetShardIterator ({stream}/{shard_id}): {e}")),
    }
}

/// Nota de un task de shard hacia el coordinador. El lote en sí va directo
/// al canal del decoder.
enum Note {
    Closed {
        shard_id: String,
        children: Vec<String>,
    },
    /// El task ya publicó el error en el canal de lotes.
    Failed,
}

#[derive(Clone, Copy)]
enum Transport {
    Cbor,
    Sdk,
}

struct Fetched {
    page: GetRecordsPage,
    transport: Transport,
}

/// `GetRecords` ya despachado. Si el task del shard muere antes de esperarlo,
/// el `Drop` lo cancela.
struct Inflight(Option<JoinHandle<Result<Fetched, String>>>);

impl Inflight {
    fn into_handle(mut self) -> JoinHandle<Result<Fetched, String>> {
        self.0.take().expect("prefetch")
    }
}

impl Drop for Inflight {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

struct Opening {
    shard_id: String,
    partition: i32,
    iterator: String,
}

struct PollCtx {
    client: Client,
    reader: Option<KinesisReader>,
    stream_name: String,
    start_from: StartPosition,
    resume: BTreeMap<String, String>,
    lot_positions: Option<LotPositions>,
    limit: i32,
    tx: tokio::sync::mpsc::Sender<Result<Vec<SourceRecord>, String>>,
    note_tx: tokio::sync::mpsc::UnboundedSender<Note>,
    /// Push de posiciones y `send` del lote, en ese orden y sin intercalarse
    /// con otro shard. El permiso del canal se espera antes de tomarlo: un
    /// `send` bloqueado con el lock puesto deja a los otros shards afuera.
    handoff: Arc<tokio::sync::Mutex<()>>,
    shards: BTreeMap<String, Shard>,
    partition_of: BTreeMap<String, i32>,
    next_partition: i32,
    active: BTreeSet<String>,
    /// Shards que ya cerraron. Un re-describe no los vuelve a abrir: su
    /// iterator terminó y repetirlos desde `TRIM_HORIZON` releería.
    finished: BTreeSet<String>,
    tasks: BTreeMap<String, JoinHandle<()>>,
}

impl PollCtx {
    fn abort(&mut self) {
        for (_, handle) in std::mem::take(&mut self.tasks) {
            handle.abort();
        }
    }

    async fn fail(mut self, err: String) {
        self.abort();
        let _ = self.tx.send(Err(err)).await;
    }

    /// Iterator de arranque, sin spawnear. El arranque pide todos los
    /// iterators y después lanza los tasks juntos: si el task del primer
    /// shard corre durante el `GetShardIterator` del segundo, el slot LIFO
    /// se queda con ese shard y los demás no llegan a salir.
    async fn prepare_shard(&mut self, shard_id: &str) -> Result<Option<Opening>, String> {
        if self.active.contains(shard_id) || self.finished.contains(shard_id) {
            return Ok(None);
        }
        let spec = {
            let Some(shard) = self.shards.get(shard_id) else {
                return Ok(None);
            };
            iterator_for(shard, &self.resume, self.start_from)
        };
        let iterator = get_iterator(&self.client, &self.stream_name, shard_id, spec).await?;
        let partition = match self.partition_of.get(shard_id) {
            Some(partition) => *partition,
            None => {
                let partition = self.next_partition;
                self.next_partition += 1;
                self.partition_of.insert(shard_id.to_string(), partition);
                partition
            }
        };
        Ok(Some(Opening {
            shard_id: shard_id.to_string(),
            partition,
            iterator,
        }))
    }

    fn launch(&mut self, opening: Opening) {
        let Opening {
            shard_id,
            partition,
            iterator,
        } = opening;
        let transport = if self.reader.is_some() {
            Transport::Cbor
        } else {
            Transport::Sdk
        };
        let client = self.client.clone();
        let reader = self.reader.clone();
        let stream_name = self.stream_name.clone();
        let lot_positions = self.lot_positions.clone();
        let tx = self.tx.clone();
        let note_tx = self.note_tx.clone();
        let handoff = Arc::clone(&self.handoff);
        let limit = self.limit;
        let task_shard = shard_id.clone();
        let handle = tokio::spawn(async move {
            shard_loop(
                client,
                reader,
                stream_name,
                task_shard,
                partition,
                iterator,
                limit,
                transport,
                lot_positions,
                tx,
                note_tx,
                handoff,
            )
            .await;
        });
        self.active.insert(shard_id.clone());
        self.tasks.insert(shard_id, handle);
    }

    /// Arranca un shard que no esté activo ni cerrado. `Ok(true)` si quedó
    /// un task nuevo. Un hijo que el describe todavía no lista se deja para
    /// el próximo re-describe, igual que antes.
    async fn start_shard(&mut self, shard_id: &str) -> Result<bool, String> {
        match self.prepare_shard(shard_id).await? {
            Some(opening) => {
                self.launch(opening);
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

async fn poll_loop(
    client: Client,
    reader: Option<KinesisReader>,
    stream_name: String,
    start_from: StartPosition,
    resume: BTreeMap<String, String>,
    lot_positions: Option<LotPositions>,
    max_batch: usize,
    tx: tokio::sync::mpsc::Sender<Result<Vec<SourceRecord>, String>>,
) {
    let limit = (max_batch as i32).min(MAX_LIMIT);
    let (note_tx, mut note_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut ctx = PollCtx {
        client,
        reader,
        stream_name,
        start_from,
        resume,
        lot_positions,
        limit,
        tx,
        note_tx,
        handoff: Arc::new(tokio::sync::Mutex::new(())),
        shards: BTreeMap::new(),
        partition_of: BTreeMap::new(),
        next_partition: 0,
        active: BTreeSet::new(),
        finished: BTreeSet::new(),
        tasks: BTreeMap::new(),
    };
    ctx.shards = match describe_shards(&ctx.client, &ctx.stream_name).await {
        Ok(shards) => shards,
        Err(e) => return ctx.fail(e).await,
    };
    // Índices de partición estables: shards de arranque en orden
    // lexicográfico de id; los nuevos toman el siguiente índice.
    let mut known: Vec<String> = ctx.shards.keys().cloned().collect();
    known.sort();
    let mut openings = Vec::with_capacity(known.len());
    for shard_id in known {
        match ctx.prepare_shard(&shard_id).await {
            Ok(Some(opening)) => openings.push(opening),
            Ok(None) => {}
            Err(e) => return ctx.fail(e).await,
        }
    }
    for opening in openings {
        ctx.launch(opening);
    }
    tracing::info!(
        stream = %ctx.stream_name,
        shards = ctx.active.len(),
        "consumo Kinesis arrancado"
    );
    let mut last_describe = Instant::now();
    loop {
        if ctx.tx.is_closed() {
            ctx.abort();
            return;
        }
        let wait = RE_DESCRIBE.saturating_sub(last_describe.elapsed());
        tokio::select! {
            biased;
            note = note_rx.recv() => {
                match note {
                    Some(Note::Failed) => {
                        ctx.abort();
                        return;
                    }
                    Some(Note::Closed { shard_id, children }) => {
                        ctx.tasks.remove(&shard_id);
                        ctx.active.remove(&shard_id);
                        ctx.finished.insert(shard_id);
                        for child in children {
                            match ctx.start_shard(&child).await {
                                Ok(true) => tracing::info!(
                                    stream = %ctx.stream_name,
                                    shard = %child,
                                    "shard nuevo adoptado"
                                ),
                                Ok(false) => {}
                                Err(e) => return ctx.fail(e).await,
                            }
                        }
                    }
                    None => return,
                }
            }
            _ = tokio::time::sleep(wait) => {
                last_describe = Instant::now();
                match describe_shards(&ctx.client, &ctx.stream_name).await {
                    Ok(fresh) => {
                        ctx.shards = fresh;
                        let listed: Vec<(String, Option<String>)> = ctx
                            .shards
                            .iter()
                            .map(|(id, shard)| {
                                (id.clone(), shard.parent_shard_id().map(|parent| parent.to_string()))
                            })
                            .collect();
                        let newcomers: Vec<String> = listed
                            .into_iter()
                            .filter_map(|(id, parent)| {
                                let parent = parent?;
                                if ctx.active.contains(&id)
                                    || ctx.finished.contains(&id)
                                    || ctx.active.contains(&parent)
                                {
                                    None
                                } else {
                                    Some(id)
                                }
                            })
                            .collect();
                        for shard_id in newcomers {
                            match ctx.start_shard(&shard_id).await {
                                Ok(true) => tracing::info!(
                                    stream = %ctx.stream_name,
                                    shard = %shard_id,
                                    "shard nuevo adoptado"
                                ),
                                Ok(false) => {}
                                Err(e) => return ctx.fail(e).await,
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            stream = %ctx.stream_name,
                            error = %e,
                            "re-DescribeStream falló; se reintentará"
                        );
                    }
                }
            }
        }
    }
}

async fn shard_loop(
    client: Client,
    reader: Option<KinesisReader>,
    stream_name: String,
    shard_id: String,
    partition: i32,
    iterator: String,
    limit: i32,
    mut transport: Transport,
    lot_positions: Option<LotPositions>,
    tx: tokio::sync::mpsc::Sender<Result<Vec<SourceRecord>, String>>,
    note_tx: tokio::sync::mpsc::UnboundedSender<Note>,
    handoff: Arc<tokio::sync::Mutex<()>>,
) {
    let mut iterator = Some(iterator);
    let mut prefetch: Option<Inflight> = None;
    loop {
        // 2 workers y el slot LIFO: si este task se reanuda en cuanto su
        // prefetch termina, se queda con el worker y los otros shards no
        // llegan a tener un GetRecords en vuelo. Ceder una vez por página
        // los intercala. El prefetch de la vuelta anterior ya está spawneado.
        tokio::task::yield_now().await;
        if tx.is_closed() {
            return;
        }
        let Some(current) = iterator.clone() else {
            return;
        };
        let fetched = if let Some(inflight) = prefetch.take() {
            let handle = inflight.into_handle();
            match handle.await {
                Ok(result) => result,
                Err(_) => Err(format!(
                    "GetRecords ({stream_name}, shard {shard_id}) cancelado"
                )),
            }
        } else {
            fetch_page(
                &client,
                &reader,
                &current,
                limit,
                transport,
                &shard_id,
                &stream_name,
            )
            .await
        };
        let fetched = match fetched {
            Ok(fetched) => fetched,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                let _ = note_tx.send(Note::Failed);
                return;
            }
        };
        transport = fetched.transport;
        let page = fetched.page;
        let records = page.records;
        let next_iterator = page.next_iterator;
        let children = page.child_shards;
        if records.is_empty() {
            iterator = next_iterator.clone();
            if iterator.is_none() {
                let _ = note_tx.send(Note::Closed { shard_id, children });
                return;
            }
            tokio::time::sleep(IDLE_POLL).await;
            continue;
        }
        if let Some(next) = next_iterator.clone() {
            prefetch = Some(spawn_fetch(
                client.clone(),
                reader.clone(),
                next,
                limit,
                transport,
                shard_id.clone(),
                stream_name.clone(),
            ));
        }
        let (lot, positions) = lot_of(&shard_id, partition, records);
        // El prefetch de la página siguiente ya está en vuelo. Esperar cupo
        // sin el lock: si el canal está lleno, los otros shards siguen
        // pudiendo entregar lo suyo y arrancar su propia lectura.
        let Ok(permit) = tx.reserve().await else {
            return;
        };
        {
            let _guard = handoff.lock().await;
            if let Some(positions_out) = &lot_positions {
                positions_out.push(positions);
            }
            permit.send(Ok(lot));
        }
        if next_iterator.is_none() {
            let _ = note_tx.send(Note::Closed { shard_id, children });
            return;
        }
        iterator = next_iterator;
    }
}

fn lot_of(
    shard_id: &str,
    partition: i32,
    records: Vec<RawRecord>,
) -> (Vec<SourceRecord>, Vec<(String, String)>) {
    let mut lot = Vec::with_capacity(records.len());
    let mut positions = Vec::with_capacity(records.len());
    for record in records {
        positions.push((shard_id.to_string(), record.sequence.clone()));
        lot.push(SourceRecord {
            partition,
            offset: 0,
            key: None,
            value: record.data,
            position: Some(record.sequence),
        });
    }
    (lot, positions)
}

fn spawn_fetch(
    client: Client,
    reader: Option<KinesisReader>,
    iterator: String,
    limit: i32,
    transport: Transport,
    shard_id: String,
    stream_name: String,
) -> Inflight {
    Inflight(Some(tokio::spawn(async move {
        fetch_page(
            &client,
            &reader,
            &iterator,
            limit,
            transport,
            &shard_id,
            &stream_name,
        )
        .await
    })))
}

async fn fetch_page(
    client: &Client,
    reader: &Option<KinesisReader>,
    iterator: &str,
    limit: i32,
    transport: Transport,
    shard_id: &str,
    stream_name: &str,
) -> Result<Fetched, String> {
    match transport {
        Transport::Sdk => Ok(Fetched {
            page: sdk_page(client, iterator, limit, shard_id, stream_name).await?,
            transport,
        }),
        Transport::Cbor => {
            let Some(reader) = reader else {
                return Err(format!(
                    "GetRecords ({stream_name}, shard {shard_id}) sin lector"
                ));
            };
            match reader.get_records(iterator, limit).await {
                Ok(page) => Ok(Fetched {
                    page,
                    transport: Transport::Cbor,
                }),
                Err(ReadError::Fallback(reason)) => {
                    tracing::warn!(
                        stream = %stream_name,
                        shard_id,
                        reason = %reason,
                        "GetRecords CBOR no disponible; el shard sigue en el SDK"
                    );
                    Ok(Fetched {
                        page: sdk_page(client, iterator, limit, shard_id, stream_name).await?,
                        transport: Transport::Sdk,
                    })
                }
                Err(ReadError::Fatal(error)) => Err(format!(
                    "GetRecords ({stream_name}, shard {shard_id}): {error}"
                )),
            }
        }
    }
}

async fn sdk_page(
    client: &Client,
    iterator: &str,
    limit: i32,
    shard_id: &str,
    stream_name: &str,
) -> Result<GetRecordsPage, String> {
    let records = client
        .get_records()
        .shard_iterator(iterator)
        .limit(limit)
        .send()
        .await
        .map_err(|e| format!("GetRecords ({stream_name}, shard {shard_id}): {e}"))?;
    let mut page = GetRecordsPage {
        next_iterator: records.next_shard_iterator().map(|it| it.to_string()),
        records: Vec::new(),
        child_shards: records
            .child_shards()
            .iter()
            .map(|child| child.shard_id().to_string())
            .collect(),
    };
    for record in records.records() {
        page.records.push(RawRecord {
            sequence: record.sequence_number().to_string(),
            data: record.data().as_ref().to_vec(),
        });
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(id: &str, parent: Option<&str>) -> Shard {
        let mut builder = Shard::builder().shard_id(id);
        if let Some(parent) = parent {
            builder = builder.parent_shard_id(parent);
        }
        builder.build().unwrap()
    }

    #[test]
    fn a_checkpoint_wins_over_everything() {
        let shard = shard(
            "shardId-0000000000:00000000001",
            Some("shardId-0000000000:00000000000"),
        );
        let mut resume = BTreeMap::new();
        resume.insert(
            "shardId-0000000000:00000000001".to_string(),
            "42".to_string(),
        );
        let (it, seq) = iterator_for(&shard, &resume, StartPosition::Latest);
        assert_eq!(it, ShardIteratorType::AfterSequenceNumber);
        assert_eq!(seq.as_deref(), Some("42"));
    }

    #[test]
    fn a_child_without_checkpoint_starts_at_trim_horizon() {
        let shard = shard(
            "shardId-0000000000:00000000002",
            Some("shardId-0000000000:00000000001"),
        );
        let (it, seq) = iterator_for(&shard, &BTreeMap::new(), StartPosition::Latest);
        assert_eq!(it, ShardIteratorType::TrimHorizon);
        assert!(seq.is_none());
    }

    #[test]
    fn a_root_shard_follows_start_from() {
        let shard = shard("shardId-0000000000:00000000000", None);
        let (it, _) = iterator_for(&shard, &BTreeMap::new(), StartPosition::Latest);
        assert_eq!(it, ShardIteratorType::Latest);
        let (it, _) = iterator_for(&shard, &BTreeMap::new(), StartPosition::TrimHorizon);
        assert_eq!(it, ShardIteratorType::TrimHorizon);
    }

    #[test]
    fn lot_positions_are_fifo() {
        let lots = LotPositions::new();
        assert!(lots.pop().is_none());
        lots.push(vec![("a".into(), "1".into())]);
        lots.push(vec![("b".into(), "2".into())]);
        assert_eq!(lots.pop(), Some(vec![("a".to_string(), "1".to_string())]));
        assert_eq!(lots.pop(), Some(vec![("b".to_string(), "2".to_string())]));
        assert!(lots.pop().is_none());
    }
}
