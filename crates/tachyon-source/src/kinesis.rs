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
//! El polling corre en un task async (el SDK de AWS es async). Si el canal
//! hacia el decoder se cierra, el task termina.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_kinesis::types::{Shard, ShardIteratorType};
use aws_sdk_kinesis::Client;
use futures::StreamExt;

use crate::consumer::{RecordStream, ReceiverStream};
use crate::record::SourceRecord;

/// Lotes en cola del poll hacia el decoder. El decode ya pipelina y
/// `GetRecords` tiene latencia de red; 4 acota la memoria por envío.
const LOT_QUEUE: usize = 4;
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
        self.0.lock().expect("lock de posiciones").push_back(positions);
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
        }
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
        let (tx, rx) = tokio::sync::mpsc::channel(LOT_QUEUE);
        tokio::spawn(async move {
            poll_loop(
                client,
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
                        .ok_or_else(|| format!("GetShardIterator ({stream}/{shard_id}) sin iterator"))
                })
                .map_err(|e| format!("GetShardIterator ({stream}/{shard_id}): {e}"))?
        }
        Err(e) => Err(format!("GetShardIterator ({stream}/{shard_id}): {e}")),
    }
}

/// Un shard activo: su iterator. El índice de partición vive en
/// `partition_of` (estable por shard).
struct ShardCursor {
    iterator: Option<String>,
}

async fn poll_loop(
    client: Client,
    stream_name: String,
    start_from: StartPosition,
    resume: BTreeMap<String, String>,
    lot_positions: Option<LotPositions>,
    max_batch: usize,
    tx: tokio::sync::mpsc::Sender<Result<Vec<SourceRecord>, String>>,
) {
    let limit = (max_batch as i32).min(MAX_LIMIT);
    let mut shards = match describe_shards(&client, &stream_name).await {
        Ok(shards) => shards,
        Err(e) => {
            let _ = tx.send(Err(e)).await;
            return;
        }
    };
    // Índices de partición estables: shards de arranque en orden
    // lexicográfico de id; los nuevos toman el siguiente índice.
    let mut known: Vec<String> = shards.keys().cloned().collect();
    known.sort();
    let mut partition_of: BTreeMap<String, i32> = BTreeMap::new();
    let mut next_partition = 0i32;
    let mut cursors: BTreeMap<String, ShardCursor> = BTreeMap::new();
    for shard_id in &known {
        let (it, seq) = iterator_for(&shards[shard_id], &resume, start_from);
        match get_iterator(&client, &stream_name, shard_id, (it, seq)).await {
            Ok(iterator) => {
                partition_of.insert(shard_id.clone(), next_partition);
                next_partition += 1;
                cursors.insert(
                    shard_id.clone(),
                    ShardCursor {
                        iterator: Some(iterator),
                    },
                );
            }
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        }
    }
    tracing::info!(stream = %stream_name, shards = cursors.len(), "consumo Kinesis arrancado");
    let mut last_describe = std::time::Instant::now();
    loop {
        if tx.is_closed() {
            break;
        }
        // Refresco periódico: un hijo cuyo padre cerró puede no haber llegado
        // por `child_shards` (split tardío de un shard inactivo).
        if last_describe.elapsed() >= RE_DESCRIBE {
            last_describe = std::time::Instant::now();
            match describe_shards(&client, &stream_name).await {
                Ok(fresh) => {
                    shards = fresh;
                    adopt_new_shards(
                        &client,
                        &stream_name,
                        &shards,
                        &resume,
                        &start_from,
                        &mut cursors,
                        &mut partition_of,
                        &mut next_partition,
                        &tx,
                    )
                    .await;
                }
                Err(e) => {
                    tracing::warn!(stream = %stream_name, error = %e, "re-DescribeStream falló; se reintentará");
                }
            }
        }
        let active: Vec<(String, String)> = cursors
            .iter()
            .filter_map(|(id, c)| c.iterator.as_ref().map(|it| (id.clone(), it.clone())))
            .collect();
        if active.is_empty() {
            // Sin shards activos: espera el próximo re-describe (merges).
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        // Todos los shards en paralelo.
        let mut rounds = futures::stream::FuturesUnordered::new();
        for (shard_id, iterator) in &active {
            rounds.push(async {
                let res = client
                    .get_records()
                    .shard_iterator(iterator.clone())
                    .limit(limit)
                    .send()
                    .await;
                (shard_id.clone(), res)
            });
        }
        let mut lot: Vec<SourceRecord> = Vec::new();
        let mut positions: Vec<(String, String)> = Vec::new();
        let mut next_iterators: BTreeMap<String, Option<String>> = BTreeMap::new();
        let mut children: BTreeMap<String, Vec<String>> = BTreeMap::new();
        while let Some((shard_id, res)) = rounds.next().await {
            match res {
                Ok(records) => {
                    next_iterators.insert(
                        shard_id.clone(),
                        records.next_shard_iterator().map(|it| it.to_string()),
                    );
                    for child in records.child_shards() {
                        children
                            .entry(shard_id.clone())
                            .or_default()
                            .push(child.shard_id().to_string());
                    }
                    for rec in records.records() {
                        let seq = rec.sequence_number().to_string();
                        positions.push((shard_id.clone(), seq.clone()));
                        let value = rec.data().as_ref().to_vec();
                        let partition = partition_of.get(&shard_id).copied().unwrap_or(0);
                        lot.push(SourceRecord {
                            partition,
                            offset: 0,
                            key: None,
                            value,
                            position: Some(seq),
                        });
                    }
                }
                Err(e) => {
                    // Fallar en voz alta: saltar registros adelantaría la
                    // posición sobre datos no procesados.
                    let _ = tx
                        .send(Err(format!("GetRecords ({stream_name}, shard {shard_id}): {e}")))
                        .await;
                    return;
                }
            }
        }
        if !lot.is_empty() {
            if let Some(lp) = &lot_positions {
                lp.push(positions);
            }
            if tx.send(Ok(lot)).await.is_err() {
                break; // receiver caído
            }
        } else {
            // Sin registros esta vuelta: pacing (el SDK no espera en el
            // servidor por nosotros).
            tokio::time::sleep(IDLE_POLL).await;
        }
        // Iteradores agotados: adoptar los hijos (TRIM_HORIZON) y soltar el
        // padre.
        for (shard_id, next) in &next_iterators {
            if next.is_some() {
                if let Some(cursor) = cursors.get_mut(shard_id) {
                    cursor.iterator = next.clone();
                }
                continue;
            }
            let child_ids = children.get(shard_id).cloned().unwrap_or_default();
            for child in child_ids {
                adopt_shard(
                    &client,
                    &stream_name,
                    &shards,
                    &resume,
                    &start_from,
                    &child,
                    &mut cursors,
                    &mut partition_of,
                    &mut next_partition,
                    &tx,
                )
                .await;
            }
            cursors.remove(shard_id);
        }
    }
}

/// Adota un shard nuevo: crea su iterator (TRIM_HORIZON si es hijo, según
/// `start_from` si no) y lo agrega a los activos. `Ok` si quedó activo o ya
/// estaba; `Err` si no se pudo arrancar (el caller termina el stream).
async fn adopt_shard(
    client: &Client,
    stream_name: &str,
    shards: &BTreeMap<String, Shard>,
    resume: &BTreeMap<String, String>,
    start_from: &StartPosition,
    child: &str,
    cursors: &mut BTreeMap<String, ShardCursor>,
    partition_of: &mut BTreeMap<String, i32>,
    next_partition: &mut i32,
    tx: &tokio::sync::mpsc::Sender<Result<Vec<SourceRecord>, String>>,
) {
    if cursors.contains_key(child) {
        return;
    }
    let Some(shard) = shards.get(child) else {
        return;
    };
    let (it, seq) = iterator_for(shard, resume, *start_from);
    match get_iterator(client, stream_name, child, (it, seq)).await {
        Ok(iterator) => {
            partition_of.insert(child.to_string(), *next_partition);
            *next_partition += 1;
            cursors.insert(
                child.to_string(),
                ShardCursor {
                    iterator: Some(iterator),
                },
            );
            tracing::info!(stream = %stream_name, shard = %child, "shard nuevo adoptado");
        }
        Err(e) => {
            let _ = tx.send(Err(e)).await;
        }
    }
}

/// Shards del último describe que no seguimos y cuyo padre ya no está activo
/// (split tardío de un shard inactivo): se adoptan.
async fn adopt_new_shards(
    client: &Client,
    stream_name: &str,
    shards: &BTreeMap<String, Shard>,
    resume: &BTreeMap<String, String>,
    start_from: &StartPosition,
    cursors: &mut BTreeMap<String, ShardCursor>,
    partition_of: &mut BTreeMap<String, i32>,
    next_partition: &mut i32,
    tx: &tokio::sync::mpsc::Sender<Result<Vec<SourceRecord>, String>>,
) {
    for (shard_id, shard) in shards {
        if cursors.contains_key(shard_id) {
            continue;
        }
        let Some(parent) = shard.parent_shard_id() else {
            continue; // un root nuevo no aparece salvo al crear el stream
        };
        if cursors.contains_key(parent) {
            continue; // el padre sigue activo: el hijo aún no tiene datos
        }
        adopt_shard(
            client,
            stream_name,
            shards,
            resume,
            start_from,
            shard_id,
            cursors,
            partition_of,
            next_partition,
            tx,
        )
        .await;
    }
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
        resume.insert("shardId-0000000000:00000000001".to_string(), "42".to_string());
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
