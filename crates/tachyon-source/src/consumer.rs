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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use futures::stream;
use rdkafka::ClientConfig;
use rdkafka_sys as rdsys;

use crate::handoff::{CommittedObs, PartitionAction, WindowHandoff};
use crate::record::SourceRecord;
use tachyon_core::OffsetRange;

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
/// El `NativeConsumer` vive en un `Arc<Mutex<Option<_>>>` hasta que `record_stream()`
/// lo `take()`a y lo mueve al task de poll (que lo posee y lo reutiliza). Como
/// el handle nativo no es `Sync`, no puede compartirse entre threads vía `Arc`; el
/// commit de offsets se delega al task de poll a través de un canal de comandos
/// (`commit_tx`): `commit()` envía un comando y el task (que posee el consumer)
/// lo ejecuta y responde. Sin esto, `commit()` vería el `Option` vacío y
/// silenciosamente no commitaría nada (bug de recuperación).
pub struct RdkafkaSource {
    consumer: Arc<Mutex<Option<NativeConsumer>>>,
    /// Offsets del último checkpoint (partición -> próximo offset). Se mueve
    /// al task de poll en `record_stream`.
    resume: Mutex<Option<BTreeMap<i32, i64>>>,
    /// Canal de comandos de commit hacia el task de poll.
    commit_tx: tokio::sync::mpsc::UnboundedSender<PollCommand>,
    /// Receiver del canal de commit (se mueve al task de poll en `record_stream`).
    commit_rx: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<PollCommand>>>,
    /// Tope de registros por envío al canal. Igual al `batch_size` del
    /// decoder: un lote del broker = un `RecordBatch`.
    max_batch: usize,
    /// Handoff de ventana. `None` en el pass-through: el rebalance sigue
    /// asignando en el momento. El `u64` es el id de este consumidor.
    handoff: Mutex<Option<(Arc<WindowHandoff>, u64)>>,
    /// Cursor compartido con los otros consumidores del topic (pass-through).
    cursor: Mutex<Option<CopiedCursor>>,
    /// Carril propio: los rangos de cada lote (ver `LotRanges`).
    lot_ranges: Mutex<Option<LotRanges>>,
}

/// Hasta dónde copió cada partición algún consumidor de este proceso
/// (partición -> próximo offset). Lo comparten los N consumidores de un
/// topic en el pass-through.
///
/// Los consumidores del mismo proceso entran al grupo de a uno y cada
/// entrada rebalancea. Sin esto, la partición que cambiaba de dueño se
/// releía desde el offset commiteado (al arrancar, desde el principio):
/// filas copiadas dos veces, el doble de decode y de escritura, y una tasa
/// que dependía de cuántos rebalanceos hubo. El dueño nuevo arranca donde
/// dejó el anterior. El anterior publica lo copiado después de cada poll y
/// al revocar, antes de volver a entrar al grupo, así que el dueño nuevo
/// nunca ve un valor viejo.
#[derive(Debug, Clone, Default)]
pub struct CopiedCursor(Arc<Mutex<CursorState>>);

#[derive(Debug, Default)]
struct CursorState {
    /// Próximo offset después de lo copiado.
    copied: HashMap<i32, i64>,
    /// Dónde empezó a leer la partición el primer consumidor del proceso.
    origin: HashMap<i32, i64>,
}

impl CopiedCursor {
    pub fn new() -> Self {
        Self::default()
    }

    fn get(&self, partition: i32) -> Option<i64> {
        self.0.lock().expect("lock del cursor").copied.get(&partition).copied()
    }

    /// Primer offset que este proceso leyó de `partition` (ver `Frontier`).
    pub fn origin(&self, partition: i32) -> Option<i64> {
        self.0.lock().expect("lock del cursor").origin.get(&partition).copied()
    }

    fn set_origin(&self, partition: i32, start: i64) {
        self.0
            .lock()
            .expect("lock del cursor")
            .origin
            .entry(partition)
            .or_insert(start);
    }

    /// Avanza con los registros copiados (el cursor nunca retrocede).
    fn publish(&self, records: &[SourceRecord]) {
        let mut high: Vec<(i32, i64)> = Vec::new();
        for record in records {
            match high.iter_mut().find(|(p, _)| *p == record.partition) {
                Some((_, next)) => *next = (*next).max(record.offset + 1),
                None => high.push((record.partition, record.offset + 1)),
            }
        }
        if high.is_empty() {
            return;
        }
        let mut state = self.0.lock().expect("lock del cursor");
        for (partition, next) in high {
            let slot = state.copied.entry(partition).or_insert(next);
            *slot = (*slot).max(next);
        }
    }
}

/// Rangos de offsets de cada lote que manda un consumidor, en el mismo orden
/// que los lotes (uno por lote, FIFO). Solo tiene sentido cuando un stream
/// lee de un único consumidor: el carril de `RedpandaPartitionStream`.
#[derive(Debug, Clone, Default)]
pub struct LotRanges(Arc<Mutex<std::collections::VecDeque<Vec<OffsetRange>>>>);

impl LotRanges {
    pub fn new() -> Self {
        Self::default()
    }

    fn push(&self, ranges: Vec<OffsetRange>) {
        self.0.lock().expect("lock de rangos").push_back(ranges);
    }

    /// Los rangos del próximo lote recibido.
    pub fn pop(&self) -> Option<Vec<OffsetRange>> {
        self.0.lock().expect("lock de rangos").pop_front()
    }
}

/// Comando hacia el task de poll, que es el dueño del `NativeConsumer`.
enum PollCommand {
    /// Offsets a commitear (partición -> próximo offset). Son los del
    /// checkpoint, NO la posición del consumer (que va por delante de lo que
    /// llegó a Paimon: commitearla perdería los registros en vuelo).
    Commit {
        offsets: BTreeMap<i32, i64>,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Copia del metadata del grupo, para `send_offsets_to_transaction`.
    GroupMetadata {
        reply: tokio::sync::oneshot::Sender<Result<GroupMetadata, String>>,
    },
}

/// Copia del membership del consumer group. Se puede usar desde otro hilo
/// para incluir los offsets de entrada en una transacción del productor.
pub struct GroupMetadata(*mut rdsys::rd_kafka_consumer_group_metadata_t);

unsafe impl Send for GroupMetadata {}

impl GroupMetadata {
    pub fn as_ptr(&self) -> *const rdsys::rd_kafka_consumer_group_metadata_t {
        self.0
    }
}

impl Drop for GroupMetadata {
    fn drop(&mut self) {
        unsafe { rdsys::rd_kafka_consumer_group_metadata_destroy(self.0) }
    }
}

/// Consumidor nativo de librdkafka (FFI directo).
///
/// Se usa el FFI en vez de `BaseConsumer` de rdkafka-rust por dos motivos:
///
/// 1. **Throughput:** el consumo por lotes (`rd_kafka_conf_set_consume_cb` +
///    `rd_kafka_poll`, ver el task de poll) elimina el overhead por mensaje
///    del poll individual (evento + `BorrowedMessage` por mensaje,
///    ~1.7µs/msg). Es la diferencia entre ~600K rows/s y varios M rows/s por
///    consumidor.
/// 2. **Rebalance correcto en modo mensaje:** rdkafka-rust no registra un
///    `rebalance_cb` nativo (maneja el rebalance vía eventos). Servir la cola
///    del consumidor en modo mensaje sin `rebalance_cb` hace que librdkafka
///    ejecute `assign(NULL)` ("forced unassign") al procesar el op de
///    rebalance: el consumidor nunca recibe particiones. Aquí sí registramos
///    el callback con el comportamiento estándar (eager: assign/revoke).
///
/// El `Send` manual es seguro: el handle lo usa un solo thread a la vez (el
/// task de poll lo posee; librdkafka garantiza que sus APIs son thread-safe
/// mientras no haya uso concurrente del mismo handle sin sincronización — el
/// `Mutex<Option<_>>` del `RdkafkaSource` y el `take()` único lo garantizan).
struct NativeConsumer {
    rk: *mut rdsys::rd_kafka_t,
    /// Handle de topic cacheado (para `rd_kafka_seek`).
    rkt: *mut rdsys::rd_kafka_topic_t,
    /// Nombre del topic (para commits y armado de listas de offsets).
    topic: CString,
    /// Estado de drenado por lotes compartido con el callback nativo de
    /// consumo (librdkafka lo recibe como `opaque` de la conf). Va en `Box`
    /// para que su dirección sea estable: el puntero se registra en la conf
    /// antes de `rd_kafka_new` y el struct se mueve después (al task de
    /// poll). Se dropea después del cuerpo de `Drop::drop` (orden de campos),
    /// así que cualquier callback durante `consumer_close` aún lo encuentra
    /// vivo.
    drain: Box<DrainState>,
}

unsafe impl Send for NativeConsumer {}

impl NativeConsumer {
    /// Crea el consumidor nativo y lo suscribe al topic (consumer group).
    fn new(config: &ClientConfig, topic: &str) -> Result<Self> {
        // Aplica todas las propiedades del ClientConfig (group.id,
        // bootstrap.servers, etc.) a la config nativa.
        let conf = config
            .create_native_config()
            .context("creando config nativa de rdkafka")?;
        let topic_c = CString::new(topic).context("nombre de topic inválido")?;
        // Estado de drenado: se registra como `opaque` de la conf (el callback
        // de consumo lo recibe por mensaje). Los handles rk/rkt se parchan
        // abajo, una vez creados.
        let mut drain = Box::new(DrainState {
            rk: std::ptr::null_mut(),
            rkt: std::ptr::null_mut(),
            topic: topic_c.clone(),
            resume: ResumeFilter::new(BTreeMap::new()),
            cursor: None,
            verified: HashSet::new(),
            expect: HashMap::new(),
            lot_ranges: None,
            chain: HashMap::new(),
            ranged: 0,
            staged: Vec::new(),
            batch: Vec::new(),
            max_batch: usize::MAX,
            fatal: None,
            handoff: None,
            consumer_id: 0,
            assigned: Vec::new(),
            at_end: HashSet::new(),
            assign_gen: 0,
            serviced_gen: 0,
            live: HashSet::new(),
            local_admitted: BTreeMap::new(),
        });
        unsafe {
            rdsys::rd_kafka_conf_set_rebalance_cb(conf.ptr(), Some(native_rebalance_cb));
            rdsys::rd_kafka_conf_set_offset_commit_cb(conf.ptr(), Some(native_commit_cb));
            rdsys::rd_kafka_conf_set_log_cb(conf.ptr(), Some(native_log_cb));
            rdsys::rd_kafka_conf_set_error_cb(conf.ptr(), Some(native_error_cb));
            // Consumo por callback: `rd_kafka_poll` despacha cada mensaje a
            // `native_consume_cb` sin locks de cola (ver el task de poll).
            rdsys::rd_kafka_conf_set_consume_cb(conf.ptr(), Some(native_consume_cb));
            rdsys::rd_kafka_conf_set_opaque(
                conf.ptr(),
                &mut *drain as *mut DrainState as *mut std::ffi::c_void,
            );
        }
        let mut errbuf = [0 as std::ffi::c_char; 512];
        let rk = unsafe {
            rdsys::rd_kafka_new(
                rdsys::rd_kafka_type_t::RD_KAFKA_CONSUMER,
                conf.ptr(),
                errbuf.as_mut_ptr(),
                errbuf.len(),
            )
        };
        if rk.is_null() {
            let msg = unsafe { CStr::from_ptr(errbuf.as_ptr()) }
                .to_string_lossy()
                .into_owned();
            return Err(anyhow::anyhow!("creando consumidor nativo: {msg}"));
        }
        // rd_kafka_new tomó ownership de la config (no destruir dos veces).
        std::mem::forget(conf);
        drain.rk = rk;

        let rkt_result = (|| -> Result<*mut rdsys::rd_kafka_topic_t> {
            // Redirige la cola principal a la cola del consumidor: los replies
            // de commit (y cualquier evento de control) se sirven en el mismo
            // drenado por lotes del task de poll. Sin esto, los replies se
            // acumularían sin ser servidos en la cola principal.
            let err = unsafe { rdsys::rd_kafka_poll_set_consumer(rk) };
            if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                return Err(anyhow::anyhow!("poll_set_consumer: {}", err_str(err)));
            }
            let rkt =
                unsafe { rdsys::rd_kafka_topic_new(rk, topic_c.as_ptr(), std::ptr::null_mut()) };
            if rkt.is_null() {
                return Err(anyhow::anyhow!("rd_kafka_topic_new devolvió null"));
            }
            // Suscripción: lista de topics (partición no aplica -> UA = -1).
            let topics = unsafe { rdsys::rd_kafka_topic_partition_list_new(1) };
            unsafe {
                rdsys::rd_kafka_topic_partition_list_add(topics, topic_c.as_ptr(), -1);
            }
            let err = unsafe { rdsys::rd_kafka_subscribe(rk, topics) };
            unsafe { rdsys::rd_kafka_topic_partition_list_destroy(topics) };
            if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                unsafe { rdsys::rd_kafka_topic_destroy(rkt) };
                return Err(anyhow::anyhow!("subscribe: {}", err_str(err)));
            }
            Ok(rkt)
        })();
        match rkt_result {
            Ok(rkt) => {
                drain.rkt = rkt;
                Ok(Self {
                    rk,
                    rkt,
                    topic: topic_c,
                    drain,
                })
            }
            Err(e) => {
                unsafe { rdsys::rd_kafka_destroy(rk) };
                Err(e)
            }
        }
    }

    /// Commitea offsets (async) en el consumer group. Debe llamarse desde el
    /// thread dueño del handle (el task de poll).
    fn commit(&self, offsets: &BTreeMap<i32, i64>) -> Result<(), String> {
        if offsets.is_empty() {
            return Ok(());
        }
        unsafe {
            let tpl = rdsys::rd_kafka_topic_partition_list_new(offsets.len() as i32);
            for (&partition, &next) in offsets {
                let elem =
                    rdsys::rd_kafka_topic_partition_list_add(tpl, self.topic.as_ptr(), partition);
                (*elem).offset = next;
            }
            // async=1: fire-and-forget; el reply llega a la cola del
            // consumidor y se sirve (noop) en el drenado por lotes.
            let err = rdsys::rd_kafka_commit(self.rk, tpl, 1);
            rdsys::rd_kafka_topic_partition_list_destroy(tpl);
            if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                return Err(format!("commit de offsets: {}", err_str(err)));
            }
        }
        Ok(())
    }

}

/// `(low, high)` watermark de una partición (FFI directo). Función libre (no
/// método) para poder llamarla desde el callback nativo, que no tiene
/// referencia al `NativeConsumer` (solo al `DrainState` vía opaque).
fn ffi_watermarks(
    rk: *mut rdsys::rd_kafka_t,
    topic: &CStr,
    partition: i32,
    timeout_ms: i32,
) -> Result<(i64, i64), String> {
    let (mut low, mut high) = (0i64, 0i64);
    let err = unsafe {
        rdsys::rd_kafka_query_watermark_offsets(
            rk,
            topic.as_ptr(),
            partition,
            &mut low,
            &mut high,
            timeout_ms,
        )
    };
    if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        return Err(format!("watermarks (partición {partition}): {}", err_str(err)));
    }
    Ok((low, high))
}

/// Seek a un offset absoluto de una partición (FFI directo).
fn ffi_seek(
    rkt: *mut rdsys::rd_kafka_topic_t,
    partition: i32,
    offset: i64,
    timeout_ms: i32,
) -> Result<(), String> {
    let err = unsafe { rdsys::rd_kafka_seek(rkt, partition, offset, timeout_ms) };
    if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        return Err(format!("seek (partición {partition}, offset {offset}): {}", err_str(err)));
    }
    Ok(())
}

/// `Ok(false)` si el fetch de la partición todavía no arrancó (típico justo
/// después de un assign). El caller reintenta en el próximo poll.
fn ffi_seek_or_wait(
    rkt: *mut rdsys::rd_kafka_topic_t,
    partition: i32,
    offset: i64,
) -> Result<bool, String> {
    let err = unsafe { rdsys::rd_kafka_seek(rkt, partition, offset, 10_000) };
    if err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        return Ok(true);
    }
    if err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__STATE
        || err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS
    {
        return Ok(false);
    }
    Err(format!(
        "seek (partición {partition}, offset {offset}): {}",
        err_str(err)
    ))
}

/// Offset commiteado en el grupo, o `None` si el broker no tiene uno.
/// Bloquea. Solo se llama entre polls.
fn ffi_committed_offset(
    rk: *mut rdsys::rd_kafka_t,
    topic: &CStr,
    partition: i32,
) -> Result<Option<i64>, String> {
    unsafe {
        let tpl = rdsys::rd_kafka_topic_partition_list_new(1);
        if tpl.is_null() {
            return Err("rd_kafka_topic_partition_list_new devolvió null".into());
        }
        let elem = rdsys::rd_kafka_topic_partition_list_add(tpl, topic.as_ptr(), partition);
        if elem.is_null() {
            rdsys::rd_kafka_topic_partition_list_destroy(tpl);
            return Err("rd_kafka_topic_partition_list_add devolvió null".into());
        }
        (*elem).offset = rdsys::RD_KAFKA_OFFSET_INVALID as i64;
        let err = rdsys::rd_kafka_committed(rk, tpl, 10_000);
        if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            let msg = err_str(err);
            rdsys::rd_kafka_topic_partition_list_destroy(tpl);
            return Err(format!("committed (partición {partition}): {msg}"));
        }
        let stored = (*(*tpl).elems).offset;
        let perr = (*(*tpl).elems).err;
        rdsys::rd_kafka_topic_partition_list_destroy(tpl);
        if perr != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR && stored < 0 {
            return Ok(None);
        }
        if stored >= 0 {
            Ok(Some(stored))
        } else {
            Ok(None)
        }
    }
}

/// Asignación actual del consumidor (FFI directo).
fn ffi_assignment(rk: *mut rdsys::rd_kafka_t) -> Result<Vec<i32>, String> {
    unsafe {
        let mut tpl: *mut rdsys::rd_kafka_topic_partition_list_t = std::ptr::null_mut();
        let err = rdsys::rd_kafka_assignment(rk, &mut tpl);
        if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            return Err(format!("assignment: {}", err_str(err)));
        }
        let ids = partition_ids(tpl);
        rdsys::rd_kafka_topic_partition_list_destroy(tpl);
        Ok(ids)
    }
}

/// Lotes que el hilo de poll puede tener en cola hacia su carril.
const LOT_QUEUE: usize = 4;
/// Lotes en cola cuando varios consumidores alimentan un solo stream.
const SHARED_LOT_QUEUE: usize = 16;

/// Cuánto espera el hilo de poll a que el decoder libere lugar antes de
/// pausar las particiones y volver a pollear. Muy por debajo del
/// `max.poll.interval.ms` default (300s): un sink trabado no saca al miembro
/// del grupo, y un rebalance espera como mucho esto a que se lo sirva.
const BACKPRESSURE_PAUSE_AFTER: Duration = Duration::from_secs(10);
/// Tramo de espera: entre tramos se sirven los commits pendientes, así el
/// commit task nunca espera más que esto a este hilo.
const BACKPRESSURE_SLICE: Duration = Duration::from_millis(50);

enum Room {
    Sent,
    Closed,
    Stalled(Vec<SourceRecord>),
}

/// Manda `batch` en cuanto el canal tenga lugar, esperando en tramos cortos
/// hasta `BACKPRESSURE_PAUSE_AFTER`. Corre en el hilo de poll (blocking).
fn wait_for_room(
    tx: &tokio::sync::mpsc::Sender<std::result::Result<Vec<SourceRecord>, String>>,
    batch: Vec<SourceRecord>,
    consumer: &NativeConsumer,
    commit_rx: &mut tokio::sync::mpsc::UnboundedReceiver<PollCommand>,
) -> Room {
    if tx.capacity() > 0 {
        // Un solo emisor (este hilo): con capacidad, `try_send` no falla por lleno.
        return match tx.try_send(Ok(batch)) {
            Ok(()) => Room::Sent,
            Err(_) => Room::Closed,
        };
    }
    let handle = tokio::runtime::Handle::current();
    let started = std::time::Instant::now();
    while started.elapsed() < BACKPRESSURE_PAUSE_AFTER {
        match handle.block_on(tokio::time::timeout(BACKPRESSURE_SLICE, tx.reserve())) {
            Ok(Ok(permit)) => {
                permit.send(Ok(batch));
                return Room::Sent;
            }
            Ok(Err(_)) => return Room::Closed,
            Err(_) => serve_poll_commands(consumer, commit_rx),
        }
    }
    Room::Stalled(batch)
}

/// Sirve los comandos que el runtime le manda al hilo de poll (commit de
/// offsets, metadata del grupo). No bloquea: el commit es async.
fn serve_poll_commands(
    consumer: &NativeConsumer,
    commit_rx: &mut tokio::sync::mpsc::UnboundedReceiver<PollCommand>,
) {
    loop {
        match commit_rx.try_recv() {
            Ok(PollCommand::Commit { offsets, reply }) => {
                let result = consumer.commit(&offsets);
                let _ = reply.send(result);
            }
            Ok(PollCommand::GroupMetadata { reply }) => {
                let ptr = unsafe { rdsys::rd_kafka_consumer_group_metadata(consumer.rk) };
                let result = if ptr.is_null() {
                    Err("el consumidor todavía no entró al grupo".to_string())
                } else {
                    Ok(GroupMetadata(ptr))
                };
                let _ = reply.send(result);
            }
            Err(_) => break, // Empty o Disconnected
        }
    }
}

/// Pausa el fetch de toda la asignación: el canal hacia el decoder está
/// lleno y acumular más lotes solo infla la memoria. El poll sigue vivo
/// (watchdog de max.poll.interval, rebalance, replies de commit) mientras
/// tanto. Fallar acá no es fatal: en el peor caso el buffer de librdkafka
/// crece hasta su propio tope (`queued.max.messages.kbytes`).
fn pause_for_backpressure(rk: *mut rdsys::rd_kafka_t, drain: &DrainState) {
    tracing::debug!("backpressure: el canal siguió lleno, se pausan las particiones");
    match ffi_assignment(rk) {
        Ok(parts) => {
            if let Err(e) = ffi_set_paused(rk, &drain.topic, &parts, true) {
                tracing::warn!(error = %e, "backpressure: no se pudieron pausar las particiones");
            }
        }
        Err(e) => tracing::warn!(error = %e, "backpressure: no se pudo leer la asignación"),
    }
}

/// Reanuda el fetch tras el backpressure. Con handoff solo se reanudan las
/// particiones ya admitidas (`live`): las demás siguen pausadas hasta que el
/// gate las habilite (reanudarlas copiaría mensajes que el callback
/// descarta, y el seek de admisión los relee).
fn resume_after_backpressure(rk: *mut rdsys::rd_kafka_t, drain: &DrainState) {
    let parts: Vec<i32> = if drain.handoff.is_some() {
        drain.live.iter().copied().collect()
    } else {
        match ffi_assignment(rk) {
            Ok(parts) => parts,
            Err(e) => {
                tracing::warn!(error = %e, "backpressure: no se pudo leer la asignación");
                return;
            }
        }
    };
    if let Err(e) = ffi_set_paused(rk, &drain.topic, &parts, false) {
        tracing::warn!(error = %e, "backpressure: no se pudieron reanudar las particiones");
    }
}

/// Pausa o reanuda fetch. Es local: no va al broker.
fn ffi_set_paused(
    rk: *mut rdsys::rd_kafka_t,
    topic: &CStr,
    partitions: &[i32],
    pause: bool,
) -> Result<(), String> {
    if partitions.is_empty() {
        return Ok(());
    }
    unsafe {
        let tpl = rdsys::rd_kafka_topic_partition_list_new(partitions.len() as i32);
        if tpl.is_null() {
            return Err("rd_kafka_topic_partition_list_new devolvió null".into());
        }
        for &partition in partitions {
            if rdsys::rd_kafka_topic_partition_list_add(tpl, topic.as_ptr(), partition).is_null() {
                rdsys::rd_kafka_topic_partition_list_destroy(tpl);
                return Err("rd_kafka_topic_partition_list_add devolvió null".into());
            }
        }
        let err = if pause {
            rdsys::rd_kafka_pause_partitions(rk, tpl)
        } else {
            rdsys::rd_kafka_resume_partitions(rk, tpl)
        };
        rdsys::rd_kafka_topic_partition_list_destroy(tpl);
        if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            return Err(format!(
                "{} particiones: {}",
                if pause { "pausar" } else { "reanudar" },
                err_str(err)
            ));
        }
    }
    Ok(())
}

/// Un miembro estático no abandona el grupo en `consumer_close`. Este
/// unsubscribe corre antes, con el hilo de grupo libre, y el poll siguiente
/// sirve el revoke. `assign(NULL)` desde ese callback sí manda LeaveGroup
/// porque el cierre todavía no marcó TERMINATE.
fn leave_static_member(rk: *mut rdsys::rd_kafka_t) {
    tracing::info!("miembro estático: unsubscribe para soltar el grupo");
    unsafe {
        let err = rdsys::rd_kafka_unsubscribe(rk);
        if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            tracing::warn!(error = %err_str(err), "unsubscribe antes de cerrar");
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            rdsys::rd_kafka_poll(rk, 100);
            let mid = rdsys::rd_kafka_memberid(rk);
            if mid.is_null() {
                rdsys::rd_kafka_poll(rk, 200);
                tracing::info!("miembro estático salió del grupo");
                return;
            }
            let empty = *mid == 0;
            rdsys::rd_kafka_mem_free(rk, mid.cast());
            if empty {
                // El id se borra al enviar LeaveGroup. Este poll espera el ack.
                rdsys::rd_kafka_poll(rk, 500);
                tracing::info!("miembro estático salió del grupo");
                return;
            }
        }
    }
    tracing::warn!("el miembro estático no confirmó la salida del grupo");
}

/// Consulta de arranque y seek del handoff. `Some` es un error fatal: el
/// poll sale sin seguir admitiendo. `None` si no hay handoff o si esta
/// vuelta no cambió nada.
fn service_handoff(rk: *mut rdsys::rd_kafka_t, drain: &mut DrainState) -> Option<String> {
    if drain.handoff.is_none() {
        return None;
    }
    drain.flush_local_admitted();
    if drain.assign_gen != drain.serviced_gen {
        drain.live.clear();
        drain.serviced_gen = drain.assign_gen;
    }
    let assigned = drain.assigned.clone();
    for partition in assigned {
        if drain.live.contains(&partition) {
            continue;
        }
        let handoff = drain.handoff.clone().expect("handoff presente");
        let action = match handoff.classify(drain.consumer_id, partition) {
            Ok(action) => action,
            Err(fatal) => return Some(fatal),
        };
        let offset = match action {
            PartitionAction::Hold => continue,
            PartitionAction::Query => match startup_query(rk, drain, &handoff, partition) {
                Ok(Some(offset)) => Some(offset),
                Ok(None) => continue,
                Err(fatal) => return Some(fatal),
            },
            PartitionAction::Ready(offset) => offset,
        };
        if let Some(offset) = offset {
            drain.resume.arm(partition, offset);
            match ffi_seek_or_wait(drain.rkt, partition, offset) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(fatal) => return Some(fatal),
            }
        }
        if let Err(fatal) = ffi_set_paused(rk, &drain.topic, &[partition], false) {
            return Some(fatal);
        }
        tracing::info!(partition, ?offset, "handoff: la partición vuelve a leer");
        drain.live.insert(partition);
    }
    None
}

/// `Ok(None)` si hay que seguir esperando (el fatal ya quedó en el gate, o
/// la consulta no sembró). `Ok(Some)` es el offset de resume.
fn startup_query(
    rk: *mut rdsys::rd_kafka_t,
    drain: &DrainState,
    handoff: &WindowHandoff,
    partition: i32,
) -> Result<Option<i64>, String> {
    if let Some(offset) = handoff.request_adopt(partition)? {
        return Ok(Some(offset));
    }
    handoff.note_no_ticket(partition)?;
    let committed = ffi_committed_offset(rk, &drain.topic, partition)?;
    match handoff.observe_committed(partition, committed)? {
        CommittedObs::Ready(offset) => Ok(Some(offset)),
        CommittedObs::NeedLow => {
            let (low, _) = ffi_watermarks(rk, &drain.topic, partition, 10_000)?;
            Ok(Some(handoff.seed_low(partition, low)))
        }
    }
}

impl Drop for NativeConsumer {
    fn drop(&mut self) {
        unsafe {
            // Sale del grupo limpiamente (leave group + revoke final) antes de
            // destruir el handle; sin esto, el grupo tarda `session.timeout`
            // en notar la baja (rebalance lento en restarts/escalado).
            rdsys::rd_kafka_consumer_close(self.rk);
            rdsys::rd_kafka_topic_destroy(self.rkt);
            rdsys::rd_kafka_destroy(self.rk);
            // El campo `drain` (estado de drenado referenciado por el opaque
            // de la conf) se dropea DESPUÉS de este cuerpo (orden de campos):
            // los callbacks que puedan dispararse durante el cierre lo
            // encuentran vivo.
        }
    }
}

/// Callback de rebalance (eager). En el pass-through asigna o desasigna y
/// nada más. En la ventana copia solo los ids: el offset de la lista no es
/// el commit del grupo. No llama a `rd_kafka_committed` ni a watermarks.
unsafe extern "C" fn native_rebalance_cb(
    rk: *mut rdsys::rd_kafka_t,
    err: rdsys::rd_kafka_resp_err_t,
    partitions: *mut rdsys::rd_kafka_topic_partition_list_t,
    opaque: *mut std::ffi::c_void,
) {
    let state = &mut *(opaque as *mut DrainState);
    if state.handoff.is_none() {
        // Lo copiado con la asignación que termina lleva sus rangos; la
        // próxima empieza cadenas nuevas.
        state.stage_ranges();
        state.chain.clear();
        state.verified.clear();
        state.expect.clear();
        if err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
            let n = if partitions.is_null() { 0 } else { (*partitions).cnt };
            tracing::info!(partitions = n, "rebalance: asignación de particiones");
            position_assignment(state, partitions);
            rdsys::rd_kafka_assign(rk, partitions);
        } else {
            tracing::info!("rebalance: revocación de particiones");
            // Lo copiado en este poll queda publicado antes de volver al
            // grupo: el dueño nuevo arranca después.
            if let Some(cursor) = &state.cursor {
                cursor.publish(&state.batch);
            }
            rdsys::rd_kafka_assign(rk, std::ptr::null_mut());
        }
        return;
    }
    let ids = partition_ids(partitions);
    if err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
        tracing::info!(partitions = ids.len(), "rebalance: asignación de particiones");
        state.at_end.clear();
        rdsys::rd_kafka_assign(rk, partitions);
        // Pausa local hasta que el poll, entre llamadas, decida el seek.
        // Los fetches de esta asignación todavía no están en la cola.
        let _ = ffi_set_paused(rk, &state.topic, &ids, true);
        if let Some(handoff) = &state.handoff {
            handoff.note_assignment(state.consumer_id, &ids);
        }
        state.assigned = ids;
        state.live.clear();
        state.assign_gen = state.assign_gen.wrapping_add(1);
    } else {
        tracing::info!(partitions = ids.len(), "rebalance: revocación de particiones");
        // Publica lo ya copiado en este poll antes de soltar el dueño.
        state.flush_local_admitted();
        if let Some(handoff) = &state.handoff {
            handoff.note_revoke(&ids);
            handoff.note_revoking(state.consumer_id);
        }
        state.assigned.clear();
        state.live.clear();
        state.assign_gen = state.assign_gen.wrapping_add(1);
        rdsys::rd_kafka_assign(rk, std::ptr::null_mut());
    }
}

/// Pass-through: una partición asignada que nadie del proceso copió todavía
/// arranca en el checkpoint, y recién si no hay en el offset del grupo. Antes el re-posicionamiento esperaba al
/// primer mensaje (`ResumeFilter`): si el offset del grupo quedó adelante del
/// checkpoint (el commit en Kafka va después del snapshot; un crash en el
/// medio lo deja así) y apuntaba al final del topic, no llegaba ningún
/// mensaje, nada disparaba el seek y ese tramo se perdía sin error.
unsafe fn position_assignment(
    state: &DrainState,
    partitions: *mut rdsys::rd_kafka_topic_partition_list_t,
) {
    if partitions.is_null() {
        return;
    }
    let list = &mut *partitions;
    if list.cnt <= 0 || list.elems.is_null() {
        return;
    }
    for i in 0..list.cnt as usize {
        let elem = &mut *list.elems.add(i);
        // Lo ya copiado en el proceso lo resuelve `align_with_cursor` (seek
        // al primer mensaje: ahí siempre hay mensajes, el dueño anterior los
        // estaba leyendo). Acá solo el checkpoint.
        let copied = state.cursor.as_ref().and_then(|cursor| cursor.get(elem.partition));
        if copied.is_none() {
            if let Some(start) = state.resume.pending_for(elem.partition) {
                elem.offset = start;
            }
        }
    }
}

/// Ids de la lista de assign/revoke. El campo `offset` no se lee.
unsafe fn partition_ids(partitions: *const rdsys::rd_kafka_topic_partition_list_t) -> Vec<i32> {
    if partitions.is_null() {
        return Vec::new();
    }
    let list = &*partitions;
    if list.cnt <= 0 || list.elems.is_null() {
        return Vec::new();
    }
    let mut ids = Vec::with_capacity(list.cnt as usize);
    for i in 0..list.cnt {
        ids.push((*list.elems.add(i as usize)).partition);
    }
    ids
}

/// Callback de reply de commits async: los commits son fire-and-forget; solo
/// se loguea el fallo (la fuente de verdad del progreso es el checkpoint).
unsafe extern "C" fn native_commit_cb(
    _rk: *mut rdsys::rd_kafka_t,
    err: rdsys::rd_kafka_resp_err_t,
    _offsets: *mut rdsys::rd_kafka_topic_partition_list_t,
    _opaque: *mut std::ffi::c_void,
) {
    if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        tracing::warn!(error = %err_str(err), "falló un commit async de offsets");
    }
}

/// Reenvía los logs internos de librdkafka a `tracing`.
unsafe extern "C" fn native_log_cb(
    _rk: *const rdsys::rd_kafka_t,
    level: i32,
    fac: *const std::ffi::c_char,
    buf: *const std::ffi::c_char,
) {
    let fac = if fac.is_null() {
        "".into()
    } else {
        CStr::from_ptr(fac).to_string_lossy()
    };
    let buf = if buf.is_null() {
        "".into()
    } else {
        CStr::from_ptr(buf).to_string_lossy()
    };
    match level {
        0..=3 => tracing::error!(target: "librdkafka", %fac, "{buf}"),
        4 => tracing::warn!(target: "librdkafka", %fac, "{buf}"),
        5..=6 => tracing::info!(target: "librdkafka", %fac, "{buf}"),
        _ => tracing::debug!(target: "librdkafka", %fac, "{buf}"),
    }
}

/// Errores de cliente de librdkafka (llegan por la cola principal, que está
/// redirigida a la del consumidor: corre en el hilo de poll). El fin de
/// partición no es un error. Un error fatal deja al cliente inservible
/// (p. ej. FENCED_INSTANCE_ID de un miembro estático): antes solo se
/// logueaba y el pipeline quedaba vivo sin consumir esas particiones. Ahora
/// termina el stream con el motivo.
unsafe extern "C" fn native_error_cb(
    rk: *mut rdsys::rd_kafka_t,
    err: i32,
    reason: *const std::ffi::c_char,
    opaque: *mut std::ffi::c_void,
) {
    if err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF as i32 {
        return;
    }
    let reason = if reason.is_null() {
        String::new()
    } else {
        CStr::from_ptr(reason).to_string_lossy().into_owned()
    };
    if err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__FATAL as i32 {
        let mut buf = [0 as std::ffi::c_char; 512];
        let code = rdsys::rd_kafka_fatal_error(rk, buf.as_mut_ptr(), buf.len());
        let detail = CStr::from_ptr(buf.as_ptr()).to_string_lossy().into_owned();
        let msg = format!("error fatal de librdkafka ({}): {detail}", err_str(code));
        tracing::error!("{msg}");
        if !opaque.is_null() {
            let state = &mut *(opaque as *mut DrainState);
            if state.fatal.is_none() {
                state.fatal = Some(msg);
            }
            rdsys::rd_kafka_yield(rk);
        }
        return;
    }
    tracing::warn!(target: "librdkafka", code = err, "{reason}");
}

/// Mensaje de error legible para un código de librdkafka.
fn err_str(err: rdsys::rd_kafka_resp_err_t) -> String {
    unsafe { CStr::from_ptr(rdsys::rd_kafka_err2str(err)) }
        .to_string_lossy()
        .into_owned()
}

impl RdkafkaSource {
    /// Crea un consumidor y se suscribe al `topic` (consumer group).
    ///
    /// `config` debe incluir al menos `group.id` y `bootstrap.servers`.
    pub fn new(config: &ClientConfig, topic: &str) -> Result<Self> {
        let consumer = NativeConsumer::new(config, topic)?;
        let (commit_tx, commit_rx) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            consumer: Arc::new(Mutex::new(Some(consumer))),
            resume: Mutex::new(None),
            commit_tx,
            commit_rx: Mutex::new(Some(commit_rx)),
            max_batch: 32_768,
            handoff: Mutex::new(None),
            cursor: Mutex::new(None),
            lot_ranges: Mutex::new(None),
        })
    }

    /// Este consumidor alimenta un carril propio: cada lote sale con sus
    /// rangos de offsets encadenados. Requiere `with_copied_cursor`.
    pub fn with_lot_ranges(self, ranges: LotRanges) -> Self {
        *self.lot_ranges.lock().expect("lock de rangos") = Some(ranges);
        self
    }

    /// El consumidor termina en un stream compartido (sin carril): nadie
    /// leería sus rangos. Hay que llamarlo antes de `record_stream`.
    pub fn drop_lot_ranges(&self) {
        self.lot_ranges.lock().expect("lock de rangos").take();
    }

    /// Comparte el cursor de lo copiado con los otros consumidores del topic
    /// en este proceso (ver `CopiedCursor`). Hay que llamarlo antes de
    /// `record_stream`. El camino de ventana usa el handoff, no esto.
    pub fn with_copied_cursor(self, cursor: CopiedCursor) -> Self {
        *self.cursor.lock().expect("lock del cursor") = Some(cursor);
        self
    }

    /// Comparte el handoff de ventana con los otros consumidores del input
    /// y con el operador. Hay que llamarlo antes de `record_stream`.
    pub fn with_window_handoff(self, handoff: Arc<WindowHandoff>) -> Self {
        let id = handoff.register_consumer();
        *self.handoff.lock().expect("lock del handoff") = Some((handoff, id));
        self
    }

    /// Tope de registros por lote del poll. Debe coincidir con el `batch_size`
    /// del stream para que un envío sea un batch decodificado.
    pub fn with_max_batch(mut self, max_batch: usize) -> Self {
        self.max_batch = max_batch.max(1);
        self
    }

    /// Re-posiciona el consumo en los offsets de un checkpoint (partición ->
    /// próximo offset). Debe llamarse antes de `record_stream`.
    ///
    /// El task de poll descarta los registros ya incluidos en el checkpoint
    /// (offset menor, p. ej. si el commit de Kafka quedó atrás) y hace `seek`
    /// si el consumer arranca más adelante (commit de Kafka adelantado): cada
    /// registro posterior al checkpoint se entrega exactamente una vez.
    pub fn with_resume_offsets(self, offsets: BTreeMap<i32, i64>) -> Self {
        *self.resume.lock().expect("lock de resume") = Some(offsets);
        self
    }

    /// Commitea en el consumer group los offsets de un checkpoint.
    ///
    /// Es informativo (lag, herramientas de Kafka) y acelera el arranque: la
    /// fuente de verdad del progreso es el checkpoint en Paimon, así que un
    /// fallo aquí no rompe el exactly-once.
    ///
    /// El `NativeConsumer` vive en el task de poll (no es `Sync`, no puede
    /// compartirse), así que el commit se delega: se envía un comando por el
    /// canal y el task (que posee el consumer) lo ejecuta y responde. Se espera
    /// la respuesta con timeout para no colgar si el task de poll ya terminó.
    pub async fn commit_offsets(&self, offsets: &BTreeMap<i32, i64>) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.commit_tx
            .send(PollCommand::Commit {
                offsets: offsets.clone(),
                reply: reply_tx,
            })
            .map_err(|_| anyhow::anyhow!("task de poll no disponible para commit de offsets"))?;
        let result = tokio::time::timeout(Duration::from_secs(5), reply_rx)
            .await
            .map_err(|_| anyhow::anyhow!("timeout esperando el commit de offsets"))?
            .map_err(|_| anyhow::anyhow!("el task de poll terminó antes de commitar"))?;
        result.map_err(|e| anyhow::anyhow!(e))
    }

    /// Metadata del grupo de este consumidor. El sink a un topic lo adjunta
    /// a la transacción que publica el lote, para que los offsets de entrada
    /// se commiteen junto con esos registros.
    pub async fn group_metadata(&self) -> Result<GroupMetadata> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.commit_tx
            .send(PollCommand::GroupMetadata { reply: reply_tx })
            .map_err(|_| anyhow::anyhow!("task de poll no disponible para el metadata del grupo"))?;
        let result = tokio::time::timeout(Duration::from_secs(10), reply_rx)
            .await
            .map_err(|_| anyhow::anyhow!("timeout esperando el metadata del grupo"))?
            .map_err(|_| anyhow::anyhow!("el task de poll terminó antes del metadata"))?;
        result.map_err(|e| anyhow::anyhow!(e))
    }

    /// Produce un stream de registros del consumidor (poll continuo).
    ///
    /// El poll loop corre en un **task dedicado** que posee el
    /// `NativeConsumer` y envía lotes de `SourceRecord` por un canal mpsc. El stream devuelto es el
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
        // El default (32768) coincide con el `batch_size` del presupuesto
        // stateless: un envío = un batch decodificado.
        let max_batch = self.max_batch;
        let state = self.consumer.clone();
        // Lotes en cola por consumidor. Cada registro lleva su payload en el
        // heap: 16 lotes de 32K eran ~90 MB por consumidor. Con un carril por
        // consumidor el pipelining ya está en el decode, y el prefetch de
        // librdkafka absorbe el jitter del broker: con 4 alcanza.
        // Un stream compartido (ventana, varios consumidores en un select)
        // necesita la cola profunda: el que lo lee atiende a todos.
        let lanes = self.lot_ranges.lock().expect("lock de rangos").is_some();
        let (tx, rx) = tokio::sync::mpsc::channel(if lanes { LOT_QUEUE } else { SHARED_LOT_QUEUE });
        // El receiver del canal de commit se mueve al task de poll (no es Clone).
        let commit_rx = self
            .commit_rx
            .lock()
            .expect("lock del canal de commit")
            .take()
            .expect("canal de commit presente");
        let resume = ResumeFilter::new(
            self.resume.lock().expect("lock de resume").take().unwrap_or_default(),
        );
        let handoff = self.handoff.lock().expect("lock del handoff").take();
        let cursor = self.cursor.lock().expect("lock del cursor").take();
        let lot_ranges = self.lot_ranges.lock().expect("lock de rangos").take();

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
                let mut commit_rx = commit_rx;
                // Instrumentación de diagnóstico (solo con TACHYON_DEBUG_POLL=1):
                // permite ver si el task de poll arranca, recibe mensajes y
                // logra enviarlos por el canal mpsc.
                let debug_poll = std::env::var("TACHYON_DEBUG_POLL").is_ok();
                tracing::info!("hilo de poll arrancado");
                let mut sent_batches = 0u64;
                let mut sent_rows = 0u64;
                let mut last_report = std::time::Instant::now();

                // El estado de drenado vive en el consumer (lo referencia el
                // opaque de la conf): se configura acá con el filtro de resume
                // y el tope de lote reales.
                {
                    let drain = &mut *consumer.drain;
                    drain.resume = resume;
                    drain.max_batch = max_batch;
                    drain.batch = Vec::with_capacity(max_batch);
                    if let Some((handoff, id)) = handoff {
                        drain.handoff = Some(handoff);
                        drain.consumer_id = id;
                    } else {
                        drain.cursor = cursor;
                        drain.lot_ranges = lot_ranges;
                    }
                }

                // Consumo por lotes con UN FFI por lote: `rd_kafka_poll` sirve
                // la cola vía `poll_cb` (cb_type CALLBACK): espera al primer
                // op (hasta `poll_timeout`), mueve TODOS los ops pendientes a
                // una cola local con una sola adquisición del lock y despacha
                // cada mensaje al `consume_cb` de la conf, sin locks por
                // mensaje (ese era el costo del `consumer_poll` individual,
                // ~1.2µs/msg). El callback acumula en `drain.batch`; al llegar
                // a `max_batch` hace `rd_kafka_yield` y los ops restantes
                // vuelven a la cabeza de la cola para la próxima llamada (no
                // se pierden). Los ops de control se sirven inline en la misma
                // pasada: rebalance -> `native_rebalance_cb` (assign/revoke),
                // replies de commit -> `native_commit_cb` (noop). Y
                // `app_poll_start/app_polled` mantienen sano el watchdog de
                // max.poll.interval.
                //
                // Por qué no las otras APIs de librdkafka 2.12.1:
                // - `rd_kafka_consume_batch_queue` (array de mensajes): rota
                //   para consumer groups (devuelve 0 mensajes siempre; solo
                //   está probada upstream con el consumidor simple, ver
                //   tests/0022) y deja el watchdog colgado (el close no
                //   termina).
                // - `rd_kafka_consume_callback_queue`: despacha con el
                //   trampoline de consumo SIN pasar por `poll_cb`, así que un
                //   op de control (el REBALANCE del join inicial, o un reply
                //   de commit con cb NULL) cae en `rd_kafka_message_get` y
                //   aborta el proceso (assert "unhandled optype", verificado).
                //   Solo es segura con colas de partición del consumidor
                //   simple.
                // - rdkafka-rust (`BaseConsumer::poll`): un evento +
                //   BorrowedMessage por mensaje (~1.7µs/msg) y sin
                //   rebalance_cb nativo (en modo mensaje: "forced unassign").
                //
                // Nota: en este modo los errores a nivel consumidor
                // (CONSUMER_ERR, p. ej. max.poll.exceeded) se loguean vía
                // `native_log_cb` en vez de terminar el stream (con
                // `consumer_poll` llegaban como mensaje de error -> fatal).
                // Los errores a nivel mensaje/partición siguen llegando como
                // FETCH con err -> fatal. Los commits async fallidos se
                // loguean en `native_commit_cb` (la fuente de verdad del
                // progreso es el checkpoint en Paimon).
                //
                // Backpressure SIN dejar de pollear: si el canal hacia el
                // decoder está lleno, el lote queda retenido en `pending` y
                // las particiones se pausan, pero el loop sigue llamando a
                // `rd_kafka_poll` (watchdog de max.poll.interval, rebalance y
                // replies de commit). Con `blocking_send` acá, un sink
                // frenado paraba el poll y el broker terminaba expulsando al
                // consumidor del grupo (rebalance en el peor momento: con el
                // sistema ya cargado). El canal es de un solo emisor (este
                // task), así que `capacity() > 0` garantiza que `try_send`
                // no falla por lleno.
                let mut pending: Option<Vec<SourceRecord>> = None;
                let mut bp_paused = false;
                loop {
                    // Si el receiver se soltó (el stream de datos terminó),
                    // salir: dejar el consumidor en el suelo mantendría
                    // vivos los threads de fondo de librdkafka y el proceso
                    // no podría terminar.
                    if tx.is_closed() {
                        break;
                    }
                    // Drenar comandos de commit pendientes. Async:
                    // el commit se encola y el ack llega en background; NO
                    // bloquea el poll (un Sync aquí paralizaba el consumo).
                    serve_poll_commands(&consumer, &mut commit_rx);
                    // Entre polls: la consulta de arranque y el seek del
                    // handoff. Dentro del callback de rebalance deadlockearían.
                    {
                        let rk = consumer.rk;
                        let drain = &mut *consumer.drain;
                        if let Some(fatal) = service_handoff(rk, drain) {
                            let _ = tx.blocking_send(Err(fatal));
                            break;
                        }
                        // Un error fatal del callback (también de un poll
                        // corto de backpressure) se entrega y termina.
                        if let Some(e) = drain.fatal.take() {
                            let _ = tx.blocking_send(Err(e));
                            break;
                        }
                    }
                    // Hay un lote retenido: solo se suelta cuando el canal
                    // tiene lugar. Mientras tanto, particiones pausadas y
                    // polls cortos para seguir sirviendo el grupo.
                    if pending.is_some() {
                        if tx.capacity() == 0 {
                            if !bp_paused {
                                pause_for_backpressure(consumer.rk, &consumer.drain);
                                bp_paused = true;
                            }
                            let _ = unsafe { rdsys::rd_kafka_poll(consumer.rk, 50) };
                            continue;
                        }
                        let lot = pending.take().expect("lote retenido presente");
                        let lot_len = lot.len();
                        if tx.try_send(Ok(lot)).is_err() {
                            break; // receiver caído: nada más que hacer
                        }
                        sent_batches += 1;
                        sent_rows += lot_len as u64;
                        if bp_paused {
                            resume_after_backpressure(consumer.rk, &consumer.drain);
                            bp_paused = false;
                        }
                    }
                    // Una llamada = un lote: espera datos (hasta
                    // `poll_timeout`) y drena la cola entera (o hasta
                    // `max_batch` vía yield del callback).
                    let _ops_served = unsafe {
                        rdsys::rd_kafka_poll(consumer.rk, poll_timeout.as_millis() as i32)
                    };
                    let drain = &mut *consumer.drain;
                    if let Some(e) = drain.fatal.take() {
                        let _ = tx.blocking_send(Err(e));
                        break;
                    }
                    drain.stage_ranges();
                    let batch = std::mem::replace(&mut drain.batch, Vec::with_capacity(max_batch));
                    drain.ranged = 0;
                    if let Some(cursor) = &drain.cursor {
                        cursor.publish(&batch);
                    }
                    if !batch.is_empty() {
                        if let Some(ranges) = &drain.lot_ranges {
                            // Antes de mandar el lote: el stream saca los
                            // rangos cuando lo recibe.
                            ranges.push(std::mem::take(&mut drain.staged));
                        }
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
                    // Canal lleno: primero se espera lugar sin pollear. El
                    // fetch de librdkafka sigue en su hilo hasta
                    // `queued.max.messages.kbytes` (memoria acotada) y el
                    // heartbeat también, así que un decoder un poco más
                    // lento no le cuesta nada al grupo. Pausar acá, en
                    // cambio, descarta lo pre-fetcheado (librdkafka sube la
                    // versión de la partición) y el resume lo vuelve a pedir
                    // al broker: con backpressure frecuente el consumo
                    // quedaba esperando la red la mayor parte del tiempo.
                    let batch = match wait_for_room(&tx, batch, &consumer, &mut commit_rx) {
                        Room::Sent => {
                            sent_batches += 1;
                            sent_rows += batch_len as u64;
                            None
                        }
                        Room::Closed => break, // receiver caído: nada más que hacer
                        Room::Stalled(batch) => Some(batch),
                    };
                    if let Some(batch) = batch {
                        // El canal siguió lleno más de `BACKPRESSURE_PAUSE_AFTER`:
                        // el lote se retiene y la próxima vuelta pausa las
                        // particiones y vuelve a pollear (watchdog de
                        // max.poll.interval, rebalance y replies de commit).
                        pending = Some(batch);
                    }
                    if debug_poll && last_report.elapsed() > std::time::Duration::from_secs(2) {
                        eprintln!(
                            "[poll] {sent_batches} lotes / {sent_rows} filas enviados (media {:.0} filas/lote)",
                            sent_rows as f64 / sent_batches as f64
                        );
                        last_report = std::time::Instant::now();
                    }
                }
                // group.instance.id (KIP-345) no manda LeaveGroup dentro de
                // consumer_close. Sin esta salida la partición sigue tomada
                // hasta session.timeout.ms y el proceso nuevo no la adopta.
                if consumer.drain.handoff.is_some() {
                    leave_static_member(consumer.rk);
                }
                // El `NativeConsumer` se destruye aquí (Drop): consumer_close
                // + destroy de topic y handle. El estado de drenado (`drain`)
                // se dropea después del cuerpo de Drop, así que cualquier
                // callback durante el cierre lo encuentra vivo.
            })
            .await;
            // El task de poll terminó (error o shutdown): el receiver verá el
            // canal cerrado y el stream terminará.
        });

        Box::pin(ReceiverStream { rx })
    }
}

/// Filtro de re-posicionamiento tras un checkpoint (ver `with_resume_offsets`).
///
/// Por partición con checkpoint (`next` = próximo offset a entregar): descarta
/// offsets `< next` (ya están en Paimon). Si el primer registro llega con
/// offset `> next` (el consumer group arrancó más adelante), hace `seek` a
/// `next` y descarta todo hasta ver exactamente `next` (los mensajes que ya
/// estaban en el buffer antes del seek son posteriores y se re-entregan en
/// orden después). Si la retención del topic ya borró `next`, el destino del
/// seek es el low watermark de la partición: esos registros no existen más en
/// el broker y se loguea el hueco.
struct ResumeFilter {
    /// Particiones aún no re-posicionadas: partición -> próximo offset.
    pending: HashMap<i32, i64>,
    /// Particiones sobre las que ya se hizo `seek`.
    seeked: HashSet<i32>,
}

impl ResumeFilter {
    fn new(offsets: BTreeMap<i32, i64>) -> Self {
        Self {
            pending: offsets.into_iter().collect(),
            seeked: HashSet::new(),
        }
    }

    /// `Ok(true)` si el mensaje debe entregarse. `Err` si no se pudo
    /// re-posicionar (seguir perdería datos en silencio).
    ///
    /// Recibe los handles FFI crudos (no el `NativeConsumer`): quien llama es
    /// el callback nativo, que solo tiene acceso al `DrainState`.
    fn admit(
        &mut self,
        rk: *mut rdsys::rd_kafka_t,
        rkt: *mut rdsys::rd_kafka_topic_t,
        topic: &CStr,
        partition: i32,
        offset: i64,
    ) -> Result<bool, String> {
        if self.pending.is_empty() {
            return Ok(true);
        }
        let Some(&next) = self.pending.get(&partition) else {
            return Ok(true); // partición sin checkpoint (o ya re-posicionada)
        };
        if offset < next {
            return Ok(false); // ya incluido en el checkpoint
        }
        if offset == next {
            self.pending.remove(&partition);
            return Ok(true);
        }
        if !self.seeked.insert(partition) {
            return Ok(false); // pre-seek en el buffer: llega de nuevo tras `next`
        }
        // offset > next: el consumer arrancó más adelante que el checkpoint.
        let timeout_ms = 10_000;
        let target = match ffi_watermarks(rk, topic, partition, timeout_ms) {
            Ok((low, _)) if low > next => {
                tracing::warn!(
                    partition,
                    checkpoint = next,
                    low_watermark = low,
                    "la retención borró offsets posteriores al checkpoint"
                );
                low
            }
            _ => next,
        };
        if target == offset {
            self.pending.remove(&partition);
            return Ok(true);
        }
        ffi_seek(rkt, partition, target, timeout_ms).map_err(|e| {
            format!("seek al checkpoint falló (partición {partition}, offset {target}): {e}")
        })?;
        tracing::info!(partition, from = offset, to = target, "seek al offset del checkpoint");
        self.pending.insert(partition, target);
        Ok(false)
    }

    /// Próximo offset a entregar de `partition` según el checkpoint, si
    /// todavía no se llegó.
    fn pending_for(&self, partition: i32) -> Option<i64> {
        self.pending.get(&partition).copied()
    }

    /// Otro consumidor del proceso ya entregó `partition` hasta `copied`: si
    /// el checkpoint pendiente es anterior, ya no hay que volver a él.
    fn covered(&mut self, partition: i32, copied: i64) {
        if self.pending.get(&partition).is_some_and(|&next| next <= copied) {
            self.pending.remove(&partition);
            self.seeked.remove(&partition);
        }
    }

    /// Fija el próximo offset a entregar. El seek anterior, si lo hubo, no
    /// cuenta: el handoff puede mover la partición después del arranque.
    fn arm(&mut self, partition: i32, next: i64) {
        self.pending.insert(partition, next);
        self.seeked.remove(&partition);
    }
}

/// Stream de lotes de `SourceRecord` desde el canal mpsc del task de poll.
///
/// Implementa `Stream` directamente (sin macro) para evitar la dependencia
/// `async-stream`. Es cancelation-safe: soltar el stream no afecta al task
/// de poll, que sigue poseyendo el `NativeConsumer`.
pub(crate) struct ReceiverStream {
    pub(crate) rx: tokio::sync::mpsc::Receiver<Result<Vec<SourceRecord>, String>>,
}

impl futures::Stream for ReceiverStream {
    type Item = Result<Vec<SourceRecord>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match std::pin::Pin::new(&mut self.rx).poll_recv(cx) {
            std::task::Poll::Ready(Some(Ok(batch))) => {
                tracing::debug!(
                    rows = batch.len(),
                    "lote consumido de Redpanda"
                );
                std::task::Poll::Ready(Some(Ok(batch)))
            }
            std::task::Poll::Ready(Some(Err(e))) => {
                std::task::Poll::Ready(Some(Err(anyhow::anyhow!(e))))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None), // canal cerrado
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// Extrae `(partition, offset, payload)` de un `rd_kafka_message_t` crudo.
/// La key NO se copia: el pipeline no la usa downstream (el particionado ya
/// lo hizo el broker).
///
/// Devuelve `Ok(None)` para señales sin datos (PARTITION_EOF: la partición se
/// alcanzó al día). `Err` para errores de fetch a nivel mensaje.
///
/// NO destruye el mensaje: en el modo callback (`consume_cb` de la conf) el
/// trampoline de librdkafka destruye el op al retornar el callback;
/// destruirlo aquí sería doble free.
///
/// # Safety
/// `rkm` debe ser un puntero válido a un mensaje vivo (dentro del callback o
/// antes de `rd_kafka_message_destroy`).
unsafe fn rkm_extract(
    rkm: *mut rdsys::rd_kafka_message_t,
) -> Result<Option<(i32, i64, Vec<u8>)>, String> {
    let msg = &*rkm;
    let err = msg.err as i32;
    if err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR as i32 {
        let value = if msg.payload.is_null() || msg.len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(msg.payload as *const u8, msg.len).to_vec()
        };
        Ok(Some((msg.partition, msg.offset, value)))
    } else if err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF as i32 {
        Ok(None)
    } else {
        let reason = rdsys::rd_kafka_message_errstr(rkm);
        let reason = if reason.is_null() {
            format!("error de fetch (código {err})")
        } else {
            CStr::from_ptr(reason).to_string_lossy().into_owned()
        };
        Err(format!(
            "error de fetch (partición {}, offset {}): {reason}",
            msg.partition, msg.offset
        ))
    }
}

/// Estado de drenado por lotes, compartido con librdkafka como `opaque` de la
/// conf (ver `NativeConsumer::new`). Solo lo toca el thread del task de poll:
/// dentro de `native_consume_cb` (durante `rd_kafka_poll`) y entre llamadas
/// (para retirar el lote), nunca a la vez.
struct DrainState {
    /// Handle del consumidor (para `rd_kafka_yield`).
    rk: *mut rdsys::rd_kafka_t,
    /// Handle del topic (para el `seek` del `ResumeFilter`).
    rkt: *mut rdsys::rd_kafka_topic_t,
    /// Nombre del topic (para los watermarks del `ResumeFilter`).
    topic: CString,
    /// Filtro de re-posicionamiento tras checkpoint.
    resume: ResumeFilter,
    /// Pass-through con varios consumidores: lo copiado en el proceso.
    cursor: Option<CopiedCursor>,
    /// Particiones de la asignación actual ya alineadas con el cursor.
    verified: HashSet<i32>,
    /// Seek al cursor en curso: los mensajes anteriores al destino son del
    /// buffer previo al seek y se descartan.
    expect: HashMap<i32, i64>,
    /// Carril propio: rangos de lo copiado (ver `LotRanges`).
    lot_ranges: Option<LotRanges>,
    /// Por partición de la asignación actual: dónde empieza el próximo rango.
    chain: HashMap<i32, i64>,
    /// Registros de `batch` que ya tienen rango.
    ranged: usize,
    /// Rangos del lote en construcción.
    staged: Vec<OffsetRange>,
    /// Lote en construcción (lo retira el task de poll tras cada `rd_kafka_poll`).
    batch: Vec<SourceRecord>,
    /// Tope del lote; al alcanzarlo el callback hace `rd_kafka_yield`.
    max_batch: usize,
    /// Error fatal pendiente de entregar al stream.
    fatal: Option<String>,
    /// `None` en el pass-through.
    handoff: Option<Arc<WindowHandoff>>,
    consumer_id: u64,
    /// Particiones de la última asignación. Vacía después de un revoke.
    assigned: Vec<i32>,
    /// Ventana: particiones que llegaron a PARTITION_EOF y no volvieron a
    /// recibir mensajes (se publica al handoff solo en los cambios).
    at_end: HashSet<i32>,
    assign_gen: u64,
    serviced_gen: u64,
    /// Particiones que este poll ya puede copiar sin consultar el gate.
    live: HashSet<i32>,
    /// High-water copiado en este poll, todavía no publicado al gate.
    local_admitted: BTreeMap<i32, i64>,
}

impl DrainState {
    /// El consumidor empezó a copiar `partition` en `start`: ahí arranca su
    /// cadena de rangos. El primero del proceso fija el origen.
    fn start_partition(&mut self, partition: i32, start: i64) {
        if let Some(cursor) = &self.cursor {
            cursor.set_origin(partition, start);
        }
        if self.lot_ranges.is_some() {
            self.chain.insert(partition, start);
        }
    }

    /// Rangos de los registros de `batch` que todavía no tienen uno. Cada
    /// rango empieza donde terminó el anterior de su partición.
    fn stage_ranges(&mut self) {
        if self.lot_ranges.is_none() {
            self.ranged = self.batch.len();
            return;
        }
        let mut high: Vec<(i32, i64, i64)> = Vec::new(); // partición, primero, próximo
        for record in &self.batch[self.ranged..] {
            match high.iter_mut().find(|(p, _, _)| *p == record.partition) {
                Some((_, _, next)) => *next = (*next).max(record.offset + 1),
                None => high.push((record.partition, record.offset, record.offset + 1)),
            }
        }
        for (partition, first, next) in high {
            let from = self.chain.get(&partition).copied().unwrap_or(first);
            self.staged.push(OffsetRange { partition, from, to: next });
            self.chain.insert(partition, next);
        }
        self.ranged = self.batch.len();
    }

    fn note_local(&mut self, partition: i32, next: i64) {
        let slot = self.local_admitted.entry(partition).or_insert(next);
        *slot = (*slot).max(next);
    }

    fn flush_local_admitted(&mut self) {
        let Some(handoff) = &self.handoff else {
            return;
        };
        if self.local_admitted.is_empty() {
            return;
        }
        let local = std::mem::take(&mut self.local_admitted);
        handoff.flush_admitted(self.consumer_id, &local);
    }
}

/// Callback nativo de consumo (`rd_kafka_conf_set_consume_cb`). librdkafka lo
/// invoca desde `rd_kafka_poll` por cada mensaje, con los ops ya movidos a
/// una cola local: no hay locks de cola por mensaje (~0.6µs/msg vs ~1.2µs/msg
/// del `consumer_poll` individual, que toma el lock y hace FFI por mensaje).
///
/// El mensaje es válido solo durante el callback (el trampoline destruye el
/// op al retornar). Al llenar el lote (o ante un error fatal) se llama
/// `rd_kafka_yield`: el serve se detiene y los ops restantes vuelven a la
/// cabeza de la cola (no se pierden; los sirve la próxima llamada).
unsafe extern "C" fn native_consume_cb(
    rkm: *mut rdsys::rd_kafka_message_t,
    opaque: *mut std::ffi::c_void,
) {
    let state = &mut *(opaque as *mut DrainState);
    if state.fatal.is_some() {
        return; // el stream ya está muriendo: descartar lo que siga
    }
    if let Some(handoff) = &state.handoff {
        // Fin del log / mensaje nuevo: el watermark de la ventana solo deja
        // idle a una partición sin nada pendiente (ver `WindowHandoff::backlog`).
        let msg = &*rkm;
        let partition = msg.partition;
        if msg.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF {
            if state.at_end.insert(partition) {
                handoff.note_end(partition, true);
            }
        } else if msg.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
            && state.at_end.remove(&partition)
        {
            handoff.note_end(partition, false);
        }
    }
    match rkm_extract(rkm) {
        Ok(Some((partition, offset, value))) => {
            if state.handoff.is_some() {
                // Después del revoke esta partición ya no es nuestra. Copiarla
                // y además dejar que el consumidor nuevo arranque en el
                // `applied` viejo la aplicaría dos veces. El mensaje se
                // destruye; el seek del dueño nuevo lo vuelve a leer.
                if state.assign_gen > 0 && !state.assigned.contains(&partition) {
                    return;
                }
                if !state.live.contains(&partition) {
                    let handoff = state.handoff.clone().expect("handoff presente");
                    match handoff.classify(state.consumer_id, partition) {
                        Ok(PartitionAction::Ready(_)) => {}
                        Ok(_) => return,
                        Err(e) => {
                            state.fatal = Some(e);
                            rdsys::rd_kafka_yield(state.rk);
                            return;
                        }
                    }
                }
            }
            if state.cursor.is_some() && !state.verified.contains(&partition) {
                match align_with_cursor(state, partition, offset) {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(e) => {
                        state.fatal = Some(e);
                        rdsys::rd_kafka_yield(state.rk);
                        return;
                    }
                }
            }
            match state.resume.admit(state.rk, state.rkt, &state.topic, partition, offset) {
                Ok(true) => {
                    if state.handoff.is_some() {
                        state.note_local(partition, offset + 1);
                    }
                    state
                        .batch
                        .push(SourceRecord::new(partition, offset, None, value));
                }
                Ok(false) => {}
                Err(e) => {
                    state.fatal = Some(e);
                    rdsys::rd_kafka_yield(state.rk);
                    return;
                }
            }
        }
        Ok(None) => {} // PARTITION_EOF: alcanzado el final
        Err(e) => {
            // Error a nivel mensaje (fetch fallido, etc.): fallar en voz
            // alta. Saltearlo adelantaría los offsets sobre datos no
            // procesados (pérdida silenciosa).
            state.fatal = Some(e);
            rdsys::rd_kafka_yield(state.rk);
            return;
        }
    }
    if state.batch.len() >= state.max_batch {
        rdsys::rd_kafka_yield(state.rk);
    }
}

/// Primer mensaje de `partition` desde la asignación: si otro consumidor del
/// proceso ya la copió hasta `c`, el consumo sigue en `c` (seek) en vez de
/// releer. `Ok(true)` entrega el mensaje; `Ok(false)` lo descarta.
///
/// Los offsets pueden saltear (markers de transacción, compaction), así que
/// después del seek se acepta el primer offset `>= c`. Un offset mayor que
/// `c` sin seek previo también vuelve a `c`: arrancar más adelante perdería
/// filas en silencio.
fn align_with_cursor(state: &mut DrainState, partition: i32, offset: i64) -> Result<bool, String> {
    if let Some(&target) = state.expect.get(&partition) {
        if offset < target {
            return Ok(false);
        }
        state.expect.remove(&partition);
        state.verified.insert(partition);
        state.start_partition(partition, target);
        return Ok(true);
    }
    let copied = state.cursor.as_ref().and_then(|cursor| cursor.get(partition));
    let Some(copied) = copied else {
        // Nadie del proceso la copió: se empieza en el checkpoint si hay
        // (el filtro de resume reposiciona ahí), si no en este mensaje.
        let start = state.resume.pending_for(partition).unwrap_or(offset);
        state.verified.insert(partition);
        state.start_partition(partition, start);
        return Ok(true);
    };
    // El checkpoint de arranque ya quedó atrás de lo copiado en el proceso.
    state.resume.covered(partition, copied);
    if offset == copied {
        state.verified.insert(partition);
        state.start_partition(partition, copied);
        return Ok(true);
    }
    ffi_seek(state.rkt, partition, copied, 10_000).map_err(|e| {
        format!("seek a lo ya copiado falló (partición {partition}, offset {copied}): {e}")
    })?;
    tracing::info!(partition, from = offset, to = copied, "la partición cambió de consumidor: sigue donde quedó");
    state.expect.insert(partition, copied);
    Ok(false)
}

/// Fuente in-memory de registros (para tests y para emular el broker).
///
/// Produce un `RecordStream` (lotes) a partir de una lista de `SourceRecord`.
/// Todos los registros van en un único lote (los tests no miden throughput).
pub fn in_memory_stream(records: Vec<SourceRecord>) -> RecordStream {
    Box::pin(stream::iter(std::iter::once(Ok(records))))
}
