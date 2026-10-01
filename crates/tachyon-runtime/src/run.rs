//! Loop de ejecución end-to-end (Slice 5).
//!
//! Orquesta el pipeline completo:
//! 1. Por cada input de la config: `RdkafkaSource` → `Decoder` →
//!    `RedpandaPartitionStream` → `StreamingTable` (factory de producción).
//! 2. `execute_query(select_sql, inputs, factory)` → stream de salida.
//! 3. Consume el stream y, por cada `RecordBatch`, `PaimonSink::write()`.
//! 4. Período de commit (configurable): checkpoint exactly-once
//!    (`PaimonSink::commit_checkpoint`) + commit informativo en Kafka.
//! 5. Métricas: rows leídas/escritas, commits, expuestas por HTTP.
//!
//! Exactly-once por stream de entrada:
//! - Cada `RedpandaPartitionStream` publica en un `OffsetTracker` los offsets
//!   de cada batch al emitirlo. El plan se valida como pass-through
//!   (`ensure_passthrough`: filter/project/limit, 1 partición, pull-based), así
//!   que al recibir un batch de salida, el snapshot de los trackers son
//!   exactamente los offsets de fuente que ese batch (y los anteriores) cubren.
//! - El writer acumula esos offsets y en cada checkpoint los commitea en el
//!   mismo paso atómico que el snapshot de Paimon (identifier monótono bajo un
//!   `commit_user` estable). Nunca se commitea la posición del consumer, que
//!   va por delante de lo escrito (registros en vuelo en los canales).
//! - `UNION ALL` no snapshotéa todos los trackers juntos: el batch sale de una
//!   rama y los offsets que lo acompañan son solo los de esa rama.
//! - Al arrancar, `PaimonSink::recover` devuelve los offsets del último
//!   checkpoint y cada fuente se re-posiciona ahí (skip de lo ya escrito +
//!   seek si el consumer group quedó adelante).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::StreamExt;
use rdkafka::consumer::Consumer;
use rdkafka::ClientConfig;
use tachyon_config::{
    parse_fixed_duration, InputKind, PayloadFormat, PipelineConfig, StartFrom,
};
use tachyon_core::{
    CheckpointBody, InputPosition, InputPositions, SourceOffsets, kafka_offsets, merge_positions,
};
use tachyon_metrics::{InstanceMetrics, MetricsServer};
use tachyon_sink::kinesis::KinesisSink;
use tachyon_sink::redpanda::RedpandaSink;
use tachyon_sink::sqs::SqsSink;
use tachyon_sink::writer::{CheckpointEpoch, PaimonSink, PartitionTickets, Recovered};
use tachyon_source::consumer::{CopiedCursor, LotRanges, RdkafkaSource};
use tachyon_source::kinesis::{KinesisSource, LotPositions, StartPosition};
use tachyon_source::sqs::SqsSource;
use tachyon_source::stream::{OffsetTracker, PositionTracker, ReceiptsOut, RedpandaPartitionStream};
use tachyon_source::{StateRequest, WindowHandoff};
use tachyon_source::decode::{parse_avro_schema, DecodeFormat, Decoder};
use tachyon_source::{
    arrow_from_avro, avro_json_from_arrow, encode_envelopes, latest_topic_schema,
    register_topic_schema, SchemaCache,
};
use tachyon_sql::{LookupJoin, UnionBranch, WindowShape};

use crate::budget::StatelessBudget;
use crate::execute::{
    ensure_passthrough, ensure_passthrough_lanes, plan_query, InputSource, StreamTableFactory,
};
use crate::lookup::{prepare_lookup, LookupStream, PreparedLookup};
use crate::union::{UnionFeed, UnionSource};
use crate::window::{
    batch_from_closed, inputs_from_batch, output_field_names, schema_with_partition,
    spec_from_shape, user_schema_without_partition, KeyKind, PARTITION_COLUMN,
};
use crate::window_shards::ShardedWindow;

/// Qué offsets cubre un batch que llega al writer.
pub(crate) enum Progress {
    /// Todo lo que las fuentes emitieron hasta este batch (un solo stream).
    Snapshot(SourceOffsets),
    /// Los tramos exactos que cubre el batch de un carril.
    Ranges(Vec<tachyon_core::OffsetRange>),
    /// Progreso de un input en una pipeline mixta (topic/kinesis/sqs).
    Input { name: String, progress: InputProgress },
}

/// Progreso que publica un input por batch.
pub(crate) enum InputProgress {
    /// Topic: próximo offset por partición del topic de la rama.
    Offsets(SourceOffsets),
    /// Kinesis: último sequence number emitido por shard.
    Positions(BTreeMap<String, String>),
    /// SQS: receipt handles de los mensajes del batch.
    Receipts { queue: String, handles: Vec<String> },
}

/// Dónde publica cada input su progreso (nombre lógico -> mecanismo).
#[derive(Clone)]
pub(crate) enum InputProgressSource {
    Offsets { topic: String, tracker: OffsetTracker },
    Positions(PositionTracker),
    Receipts { queue: String, out: ReceiptsOut },
}

/// Salida de un carril hacia el writer: rangos de topic o receipts SQS.
#[derive(Clone)]
enum LaneOutput {
    Ranges(tachyon_source::LaneRanges),
    Receipts { queue: String, out: ReceiptsOut },
}

/// Carril de receipts SQS fresco (uno por batch emitido, FIFO).
fn fresh_receipts() -> ReceiptsOut {
    Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()))
}

/// Fuente de un input para la factory (una por nombre lógico).
#[derive(Clone)]
enum InputSources {
    Topic {
        sources: Vec<Arc<RdkafkaSource>>,
        tracker: OffsetTracker,
        format: DecodeFormat,
        lane_lots: Vec<LotRanges>,
    },
    Kinesis {
        source: Arc<KinesisSource>,
        tracker: PositionTracker,
        format: DecodeFormat,
    },
    Sqs {
        sources: Vec<Arc<SqsSource>>,
        format: DecodeFormat,
        /// Carril de receipts del modo fusionado (select_all). En carriles
        /// cada lane tiene el suyo (ver `LaneOutput::Receipts`).
        receipts: ReceiptsOut,
    },
}

/// Espera del long polling SQS cuando la config no fija `receive_wait`.
const SQS_DEFAULT_WAIT: Duration = Duration::from_secs(20);

/// Fusiona `update` en `offsets` (máximo por partición: el progreso nunca
/// retrocede).
pub(crate) struct AbortOnDrop(pub(crate) tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn same_window(stored: &tachyon_core::WindowSpecId, current: &tachyon_core::WindowSpecId) -> bool {
    stored.kind == current.kind
        && stored.size_ms == current.size_ms
        && stored.slide_ms == current.slide_ms
        && stored.gap_ms == current.gap_ms
        && stored.group_columns == current.group_columns
}

fn release_lost_partitions(
    handoff: &Option<Arc<WindowHandoff>>,
    operator: &mut ShardedWindow,
    applied: &mut SourceOffsets,
) {
    let Some(handoff) = handoff else {
        return;
    };
    let topic = handoff.topic().to_string();
    for partition in handoff.take_released() {
        tracing::info!(
            %topic,
            partition,
            "la partición dejó el proceso; sus ventanas abiertas se releen desde el último snapshot"
        );
        operator.release_partition(partition);
        if let Some(parts) = applied.get_mut(&topic) {
            parts.remove(&partition);
        }
        handoff.forget_partition(partition);
    }
}

/// Schema del decoder y schema del stream interno. Con lookup el decoder ve
/// el hecho; la tabla de DataFusion ya trae las columnas de la dimensión.
fn decode_plan(
    table_schema: &SchemaRef,
    lookup: &Option<PreparedLookup>,
    name: &str,
) -> (SchemaRef, SchemaRef, bool) {
    if let Some(prepared) = lookup.as_ref().filter(|item| item.fact == name) {
        return (
            prepared.fact_schema.clone(),
            prepared.fact_schema.clone(),
            false,
        );
    }
    if table_schema
        .fields()
        .last()
        .is_some_and(|field| field.name() == PARTITION_COLUMN)
    {
        return (
            user_schema_without_partition(table_schema),
            table_schema.clone(),
            true,
        );
    }
    (table_schema.clone(), table_schema.clone(), false)
}

fn as_partition(
    inner: RedpandaPartitionStream,
    lookup: &Option<PreparedLookup>,
    name: &str,
) -> Arc<dyn PartitionStream> {
    if let Some(prepared) = lookup.as_ref().filter(|item| item.fact == name) {
        Arc::new(LookupStream::new(inner, prepared))
    } else {
        Arc::new(inner)
    }
}

fn input_table_schema(
    prepared_schema: SchemaRef,
    input_name: &str,
    window: bool,
    lookup: &Option<PreparedLookup>,
) -> Result<SchemaRef> {
    if window {
        if prepared_schema.field_with_name(PARTITION_COLUMN).is_ok() {
            anyhow::bail!("'{PARTITION_COLUMN}' está reservado");
        }
        return Ok(schema_with_partition(prepared_schema));
    }
    if let Some(prepared) = lookup
        .as_ref()
        .filter(|item| item.fact == input_name)
    {
        return Ok(prepared.enriched_schema.clone());
    }
    Ok(prepared_schema)
}

pub(crate) fn merge_offsets(offsets: &mut SourceOffsets, update: &SourceOffsets) {
    for (topic, partitions) in update {
        let entry = offsets.entry(topic.clone()).or_default();
        for (&partition, &next) in partitions {
            let current = entry.entry(partition).or_insert(next);
            *current = (*current).max(next);
        }
    }
}

/// Resultado de ejecutar el pipeline (lo que el binario/tests necesitan).
pub struct PipelineHandle {
    /// La dirección real del endpoint de métricas (si está habilitado).
    pub metrics_addr: Option<std::net::SocketAddr>,
    /// Las métricas de la instancia (para inspección en tests).
    pub metrics: Arc<InstanceMetrics>,
}

/// Opciones de ejecución del pipeline (decoupladas de la config para tests).
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Intervalo de commit del sink.
    pub commit_interval: Duration,
    /// Puerto/dirección del endpoint de métricas (None = deshabilitado).
    pub metrics_bind: Option<std::net::SocketAddr>,
    /// ID de consumer group (debe ser único por instancia; en producción se
    /// deriva del nombre del pipeline).
    pub group_id: String,
    /// Identidad del writer en los snapshots de Paimon (exactly-once). Debe
    /// ser estable entre reinicios de la misma instancia y distinta entre
    /// instancias que escriben la misma tabla. Default:
    /// `{group_id}-{TACHYON_INSTANCE_ID|POD_NAME|HOSTNAME}`.
    pub commit_user: String,
}

impl RunOptions {
    pub fn from_config(config: &PipelineConfig) -> Self {
        let commit_interval = parse_duration(&config.deployment.commit_interval);
        let metrics_bind = config
            .deployment
            .metrics
            .as_ref()
            .map(|m| m.bind_addr.parse().expect("bind_addr inválido"));
        let group_id = format!("tachyon-{}", config.pipeline.name);
        // Varias instancias del mismo pipeline escriben la misma tabla: cada
        // una necesita su propio `commit_user` (sus checkpoints son propios)
        // y el mismo valor después de reiniciar. En Kubernetes `HOSTNAME` es
        // el nombre del pod, estable entre reinicios del contenedor.
        let instance = instance_name();
        let commit_user = commit_user_from(&group_id, &instance);
        RunOptions {
            commit_interval,
            metrics_bind,
            commit_user,
            group_id,
        }
    }
}

/// Parsa una duración simple ("10s", "500ms", "2m") a `Duration`.
fn parse_duration(s: &str) -> Duration {
    let s = s.trim();
    if let Some(ms) = s.strip_suffix("ms") {
        Duration::from_millis(ms.parse().unwrap_or(10_000))
    } else if let Some(sec) = s.strip_suffix('s') {
        Duration::from_secs(sec.parse().unwrap_or(10))
    } else if let Some(min) = s.strip_suffix('m') {
        Duration::from_secs(min.parse().unwrap_or(10) * 60)
    } else {
        Duration::from_secs(s.parse().unwrap_or(10))
    }
}

/// `TACHYON_INSTANCE_ID`, si no `POD_NAME`, si no `HOSTNAME`.
fn instance_name() -> String {
    ["TACHYON_INSTANCE_ID", "POD_NAME", "HOSTNAME"]
        .into_iter()
        .find_map(|key| {
            std::env::var(key)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
        .unwrap_or_else(|| "local".to_string())
}

/// El broker tiene que aceptar este timeout (`group.max.session.timeout.ms`).
/// Solo el camino de ventana lo setea: el miembro estático no debe caerse
/// del grupo en un commit largo.
fn window_session_timeout_ms(commit_interval: Duration) -> u64 {
    (commit_interval.as_millis() as u64)
        .saturating_add(30_000)
        .max(45_000)
}

/// Un count distinto cambia los `group.instance.id` y la asignación.
fn ensure_window_consumers(stored: u32, now: usize) -> Result<()> {
    if stored as usize != now {
        anyhow::bail!(
            "consumers_per_topic cambió ({stored} → {now}); los group.instance.id cambian y la asignación también"
        );
    }
    Ok(())
}

/// `{group}-{instance}` apto para el `commit_user` de Paimon: estable por
/// instancia, sin caracteres que el path del checkpoint no tolera.
fn commit_user_from(group_id: &str, instance: &str) -> String {
    let raw = format!("{group_id}-{instance}");
    let mut out: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if out.len() > 128 {
        out.truncate(128);
    }
    if out.is_empty() {
        "tachyon".to_string()
    } else {
        out
    }
}

/// Construye el `ClientConfig` rdkafka para un input, a partir de la config
/// del pipeline.
pub(crate) fn source_client_config(
    config: &PipelineConfig,
    group_id: &str,
    budget: &StatelessBudget,
) -> Result<ClientConfig> {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", config.redpanda()?.brokers.join(","));
    cc.set("group.id", group_id);
    cc.set("auto.offset.reset", "earliest");
    cc.set("enable.auto.commit", "false");
    // Un topic escrito con transacción esconde el lote hasta el commit. Los
    // mensajes sin transacción siguen llegando.
    cc.set("isolation.level", "read_committed");
    cc.set("session.timeout.ms", "10000");
    // Los N consumidores del proceso entran al grupo de a uno y cada entrada
    // rebalancea. Un miembro se entera del rebalance en su próximo
    // heartbeat: con el default (3s) el arranque quedaba frenado 3s en la
    // mitad de las corridas (el broker ya había armado la generación con
    // los primeros). 500 ms lo acota sin cargar al coordinador.
    cc.set("heartbeat.interval.ms", "500");
    // El broker acumula hasta `fetch.min.bytes` antes de responder. Con el
    // default de 1 byte cada round trip trae poquísimas filas y el throughput
    // queda acotado por la latencia de red. Los valores salen del presupuesto
    // (512 KiB / 1s), los mismos para cualquier pin.
    cc.set("fetch.min.bytes", budget.fetch_min_bytes.to_string());
    cc.set("fetch.max.bytes", budget.fetch_max_bytes.to_string());
    cc.set(
        "max.partition.fetch.bytes",
        budget.max_partition_fetch_bytes.to_string(),
    );
    cc.set("fetch.wait.max.ms", budget.fetch_wait_max_ms.to_string());
    // Prefetch: la cola local absorbe el jitter del broker para que el
    // decode no vea el round trip. El default (100K mensajes / 64 MiB) se
    // queda corto cuando cada batch son 32K filas.
    cc.set("queued.min.messages", "1000000");
    cc.set(
        "queued.max.messages.kbytes",
        budget.queued_max_kbytes.to_string(),
    );
    if let Some(sec) = &config.redpanda()?.security {
        if let Some(mech) = &sec.sasl_mechanism {
            cc.set("sasl.mechanism", mech);
        }
        if let Some(user) = &sec.username {
            cc.set("sasl.username", user);
        }
        if let Some(pass) = &sec.password {
            cc.set("sasl.password", pass);
        }
    }
    Ok(cc)
}

/// Schema Arrow de un input y la codificación de los bytes del topic.
///
/// DataFusion y Paimon ven `schema`. `format` solo decide cómo se decodifica
/// cada mensaje, una vez, en el borde.
#[derive(Clone)]
pub struct PreparedInput {
    pub schema: SchemaRef,
    pub format: DecodeFormat,
}

impl PreparedInput {
    pub fn json(schema: SchemaRef) -> Self {
        Self {
            schema,
            format: DecodeFormat::Json,
        }
    }

    /// `avsc` es el texto de un `.avsc`. El schema queda en el decoder.
    pub fn from_avsc(schema: SchemaRef, avsc: &str) -> Result<Self> {
        Ok(Self {
            schema,
            format: DecodeFormat::Avro(parse_avro_schema(avsc)?),
        })
    }

    /// El Arrow y el decoder salen del último schema de `{topic}-value`.
    pub fn from_registry(url: &str, topic: &str) -> Result<Self> {
        let (id, avsc) = latest_topic_schema(url, topic)?;
        let avro = parse_avro_schema(&avsc)?;
        let arrow = arrow_from_avro(&avro)?;
        Ok(Self {
            schema: arrow,
            format: DecodeFormat::Registry(Arc::new(SchemaCache::new(
                url.to_string(),
                id,
                avro,
            ))),
        })
    }
}

fn format_matches(declared: PayloadFormat, format: &DecodeFormat) -> bool {
    matches!(
        (declared, format),
        (PayloadFormat::Json, DecodeFormat::Json)
            | (PayloadFormat::Avro, DecodeFormat::Avro(_))
            | (PayloadFormat::Avro, DecodeFormat::Registry(_))
    )
}

/// Corre el pipeline end-to-end.
///
/// No retorna mientras el pipeline esté activo (el stream de salida es
/// infinito). En tests se lanza en una tarea y se cancela; en producción
/// corre hasta recibir una señal de shutdown.
///
/// `metrics` es pre-creada por el caller para que pueda leerse **mientras el
/// pipeline corre** (tests de recuperación, supervisor, admin API).
pub async fn run_pipeline(
    config: &PipelineConfig,
    select_sql: &str,
    options: &RunOptions,
    // La tabla Paimon ya abierta (para no depender del catalog en el loop).
    // Se pasa por valor: el writer task la posee (write + commit en background).
    sink: PaimonSink,
    // Codecs de los inputs (nombre lógico -> schema Arrow + formato del topic).
    input_codecs: &std::collections::HashMap<String, PreparedInput>,
    // Métricas de la instancia (pre-creadas por el caller).
    metrics: &Arc<InstanceMetrics>,
    window: Option<&WindowShape>,
) -> Result<PipelineHandle> {
    run_pipeline_with_lookup(
        config,
        select_sql,
        options,
        sink,
        input_codecs,
        metrics,
        window,
        None,
        None,
    )
    .await
}

pub(crate) async fn run_pipeline_with_lookup(
    config: &PipelineConfig,
    select_sql: &str,
    options: &RunOptions,
    sink: PaimonSink,
    input_codecs: &std::collections::HashMap<String, PreparedInput>,
    metrics: &Arc<InstanceMetrics>,
    window: Option<&WindowShape>,
    lookup: Option<&LookupJoin>,
    union_branches: Option<&[UnionBranch]>,
) -> Result<PipelineHandle> {
    if union_branches.is_some() && (window.is_some() || lookup.is_some()) {
        anyhow::bail!("UNION ALL no se mezcla con una ventana, un join o un lookup");
    }
    // Exactly-once no se desactiva: los offsets se commitean con el snapshot
    // de Paimon y un plan que retiene filas entre batches se rechaza abajo.
    // El paralelismo sale del pin (ver `StatelessBudget`).
    let budget = StatelessBudget::resolve(config).context("presupuesto del pipeline")?;
    tracing::info!(
        cpus = budget.cpus,
        batch_size = budget.batch_size,
        consumers_per_topic = budget.consumers_per_topic,
        decode_parallelism = budget.decode_parallelism,
        commit_user = %options.commit_user,
        "exactly-once activo (pass-through); el paralelismo sale del pin salvo override del yaml"
    );

    // --- Métricas (endpoint HTTP) ---
    let metrics_addr = if let Some(bind) = options.metrics_bind {
        let server = MetricsServer::new(bind, metrics.clone());
        Some(server.start().await.context("arrancando métricas")?)
    } else {
        None
    };

    // --- Recuperación: último checkpoint exactly-once de esta instancia ---
    let mut sink = sink
        .with_commit_user(&options.commit_user)
        .context("fijando el commit_user del sink")?;
    let recovered = sink.recover().await.context("recuperando el último checkpoint")?;
    let (resume, resume_positions, restored_window) = match (&recovered, window) {
        (Recovered::None, _) => (None, None, None),
        (Recovered::PassThrough { offsets, .. }, None) => (Some(offsets.clone()), None, None),
        (Recovered::Positions { positions, .. }, None) => (
            Some(kafka_offsets(positions)),
            Some(positions.clone()),
            None,
        ),
        (Recovered::Positions { identifier, .. }, Some(_)) => {
            anyhow::bail!(
                "checkpoint {identifier} de '{}' es de inputs mixtos y el plan es de ventana; se rechaza",
                options.commit_user
            );
        }
        (Recovered::PassThrough { identifier, .. }, Some(_)) => {
            anyhow::bail!(
                "checkpoint {identifier} de '{}' es pass-through y el plan es de ventana; se rechaza",
                options.commit_user
            );
        }
        (Recovered::Window { identifier, .. }, None) => {
            anyhow::bail!(
                "checkpoint {identifier} tiene estado de ventana y el plan es pass-through; se rechaza"
            );
        }
        (Recovered::Join { identifier, .. }, _) => {
            anyhow::bail!(
                "checkpoint {identifier} es de un join y el plan no lo es; se rechaza"
            );
        }
        (Recovered::Window { checkpoint, .. }, Some(shape)) => {
            if checkpoint.spec.kind != shape.kind
                || checkpoint.spec.size_ms != shape.size_ms
                || checkpoint.spec.slide_ms != shape.slide_ms
                || checkpoint.spec.gap_ms != shape.gap_ms
                || checkpoint.spec.group_columns != shape.group_columns
            {
                anyhow::bail!(
                    "el checkpoint de '{}' no coincide con la ventana de esta query",
                    options.commit_user
                );
            }
            (Some(checkpoint.applied.clone()), None, Some(checkpoint.clone()))
        }
    };
    let resume = resume;
    if let Some(offsets) = &resume {
        tracing::info!(?offsets, "reanudando desde el último checkpoint");
    }

    // --- Fuentes Redpanda: N consumidores por input (mismo consumer group) ---
    // Cada input tiene su propio consumer group (independiente por topic) para
    // que el rebalanceo de uno no afecte a los demás. Dentro de cada grupo, N
    // instancias librdkafka reparten las particiones y hacen fetch en paralelo
    // Por topic: el source que commitea en Kafka (informativo) y el tracker de
    // offsets emitidos.
    let mut commit_sources: Vec<(String, Arc<RdkafkaSource>)> = Vec::new();
    let mut trackers: Vec<(String, OffsetTracker)> = Vec::new();
    let mut inputs: Vec<InputSource> = Vec::new();
    let mut input_topics: Vec<(String, String)> = Vec::new();
    let mut progress_sources: Vec<(String, InputProgressSource)> = Vec::new();
    let mut sqs_deleters: Vec<(aws_sdk_sqs::Client, String)> = Vec::new();
    let mut name_to_sources: std::collections::HashMap<String, InputSources> =
        std::collections::HashMap::new();
    let mut window_handoff: Option<Arc<WindowHandoff>> = None;
    let (adopt_tx, adopt_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut adopt_rx = if window.is_some() { Some(adopt_rx) } else { None };
    // El client de Redpanda solo se necesita si hay algún input de topic;
    // una pipeline de kinesis/sqs no declara connectors.redpanda.
    let cc = if config
        .inputs
        .iter()
        .any(|input| input.kind() == InputKind::Topic)
    {
        Some(source_client_config(config, &options.group_id, &budget)?)
    } else {
        None
    };
    // Ventana y pass-through usan el mismo presupuesto: min(particiones, CPUs)
    // salvo override. El handoff evita aplicar dos veces una partición que
    // cambia de consumidor dentro del proceso.
    let n_consumers = budget.consumers_per_topic;
    if let Some(checkpoint) = &restored_window {
        ensure_window_consumers(checkpoint.consumers_per_topic, n_consumers)?;
    }
    let session_timeout_ms = window_session_timeout_ms(options.commit_interval);
    let prepared_lookup = match lookup {
        Some(join) if window.is_none() => {
            let codec = input_codecs.get(&join.fact).cloned().with_context(|| {
                format!("sin schema para el input '{}'", join.fact)
            })?;
            Some(prepare_lookup(config, join, select_sql, codec.schema).await?)
        }
        _ => None,
    };
    if let Some(prepared) = &prepared_lookup {
        let fact_kind = config
            .inputs
            .iter()
            .find(|input| input.name == prepared.fact)
            .map(|input| input.kind());
        if fact_kind != Some(InputKind::Topic) {
            anyhow::bail!(
                "el lookup enriquece un input de topic; '{}' es kinesis o sqs",
                prepared.fact
            );
        }
    }
    let planned_sql = if let Some(shape) = window {
        shape.rewrite_sql()
    } else if let Some(prepared) = &prepared_lookup {
        prepared.sql.clone()
    } else {
        select_sql.to_string()
    };

    // Carriles: con un solo topic en pass-through, cada consumidor alimenta
    // su propia partición de DataFusion (decode + filtro/proyección en
    // paralelo, como los subtasks de Flink) y el writer junta los rangos de
    // offsets de cada carril en una frontera exacta (ver `Frontier`). Un
    // único stream ordenado que fundía los N consumidores era el techo del
    // pipeline y la fuente de su variabilidad.
    let use_lanes = union_branches.is_none()
        && prepared_lookup.is_none()
        && config.inputs.len() == 1
        && n_consumers > 1
        && matches!(config.inputs[0].kind(), InputKind::Topic | InputKind::Sqs);
    let mut lane_outs: Vec<LaneOutput> = Vec::new();
    // Ventana: cada carril lleva su propio tracker (lo que emitió); el
    // handoff impide que un carril se adelante a otro en una partición.
    let lane_trackers: Vec<OffsetTracker> = if use_lanes && window.is_some() {
        (0..n_consumers).map(|_| OffsetTracker::new()).collect()
    } else {
        Vec::new()
    };
    let mut lane_cursor: Option<(String, CopiedCursor)> = None;

    for input_def in &config.inputs {
        let prepared = input_codecs
            .get(&input_def.name)
            .cloned()
            .with_context(|| format!("sin schema para el input '{}'", input_def.name))?;
        if !format_matches(input_def.format, &prepared.format) {
            anyhow::bail!(
                "el input '{}' declara format {:?} pero el codec cargado no coincide",
                input_def.name,
                input_def.format
            );
        }
        tracing::info!(
            input = %input_def.name,
            format = ?input_def.format,
            "codec del input"
        );
        let schema = input_table_schema(
            prepared.schema.clone(),
            &input_def.name,
            window.is_some(),
            &prepared_lookup,
        )?;
        match input_def.kind() {
            InputKind::Topic => {
                let topic = input_def.kafka_topic()?.to_string();
                input_topics.push((input_def.name.clone(), topic.clone()));

                let source_group = format!("{}-{}", options.group_id, input_def.name);
                let cc = cc
                    .as_ref()
                    .expect("un input de topic requiere connectors.redpanda");
                let mut source_cc = cc.clone();
                source_cc.set("group.id", &source_group);
                let handoff = if window.is_some() {
                    source_cc.set("session.timeout.ms", session_timeout_ms.to_string());
                    // Fin del log por partición: el watermark no deja idle a una
                    // partición que todavía tiene backlog.
                    source_cc.set("enable.partition.eof", "true");
                    let topic_applied = restored_window
                        .as_ref()
                        .and_then(|checkpoint| checkpoint.applied.get(&topic))
                        .cloned()
                        .unwrap_or_default();
                    let gate = match &restored_window {
                        Some(checkpoint) => Arc::new(WindowHandoff::restored(
                            &source_group,
                            &topic,
                            &options.commit_user,
                            checkpoint.commit_identifier,
                            topic_applied,
                        )),
                        None => Arc::new(WindowHandoff::starting(
                            &source_group,
                            &topic,
                            &options.commit_user,
                        )),
                    };
                    gate.bind_state_bus(adopt_tx.clone());
                    window_handoff = Some(gate.clone());
                    // El id estático incluye el commit_user: dos procesos en la
                    // misma máquina no se pisan el membership, y un restart del
                    // mismo proceso conserva el id.
                    let ids: Vec<String> = (0..n_consumers)
                        .map(|index| format!("{}-{index}", options.commit_user))
                        .collect();
                    tracing::info!(
                        input = %input_def.name,
                        group = %source_group,
                        session_timeout_ms,
                        instances = ?ids,
                        "ventana: un operador y N consumidores"
                    );
                    Some(gate)
                } else {
                    None
                };
                // Cada consumidor recibe el mapa completo del checkpoint: el
                // filtro de resume ignora las particiones que no le asigna el
                // grupo.
                let topic_resume = resume
                    .as_ref()
                    .and_then(|r| r.get(&topic))
                    .cloned()
                    .unwrap_or_default();
                // Pass-through: los N consumidores del topic comparten lo
                // copiado, así un rebalance entre ellos no relee (ver
                // `CopiedCursor`).
                let copied = CopiedCursor::new();
                let lane_lots: Vec<LotRanges> = if use_lanes {
                    lane_cursor = Some((topic.clone(), copied.clone()));
                    (0..n_consumers).map(|_| LotRanges::new()).collect()
                } else {
                    Vec::new()
                };
                let input_sources: Vec<Arc<RdkafkaSource>> = (0..n_consumers)
                    .map(|index| -> Result<Arc<RdkafkaSource>> {
                        let mut consumer_cc = source_cc.clone();
                        if window.is_some() {
                            // KIP-345: un id distinto por miembro. No va en el
                            // config compartido, porque el segundo join con el
                            // mismo id echa al primero.
                            consumer_cc.set(
                                "group.instance.id",
                                format!("{}-{index}", options.commit_user),
                            );
                        }
                        let mut source = RdkafkaSource::new(&consumer_cc, &topic)
                            .with_context(|| format!("creando source para '{}'", input_def.name))?
                            .with_max_batch(budget.batch_size)
                            .with_resume_offsets(topic_resume.clone());
                        if let Some(gate) = &handoff {
                            source = source.with_window_handoff(gate.clone());
                        } else {
                            source = source.with_copied_cursor(copied.clone());
                            if let Some(lots) = lane_lots.get(index) {
                                source = source.with_lot_ranges(lots.clone());
                            }
                        }
                        Ok(Arc::new(source))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let tracker = OffsetTracker::new();
                commit_sources.push((topic.clone(), input_sources[0].clone()));
                trackers.push((topic.clone(), tracker.clone()));
                if use_lanes {
                    // En ventana no se leen (el carril usa su tracker), pero el
                    // zip de la factory recorre uno por consumidor.
                    lane_outs = (0..n_consumers)
                        .map(|_| LaneOutput::Ranges(Default::default()))
                        .collect();
                }
                name_to_sources.insert(
                    input_def.name.clone(),
                    InputSources::Topic {
                        sources: input_sources,
                        tracker: tracker.clone(),
                        format: prepared.format,
                        lane_lots,
                    },
                );
                progress_sources.push((
                    input_def.name.clone(),
                    InputProgressSource::Offsets {
                        topic: topic.clone(),
                        tracker,
                    },
                ));
            }
            InputKind::Kinesis => {
                if window.is_some() {
                    anyhow::bail!(
                        "una ventana lee un topic; el input '{}' es kinesis",
                        input_def.name
                    );
                }
                let kinesis_cfg = config
                    .connectors
                    .kinesis
                    .as_ref()
                    .context("kinesis requiere connectors.kinesis.region")?;
                let client = crate::aws::kinesis_client(kinesis_cfg).await?;
                let stream_name = input_def
                    .kinesis_stream()
                    .expect("la validación garantiza un stream")
                    .to_string();
                let start_from = match input_def.start_from {
                    Some(StartFrom::TrimHorizon) => StartPosition::TrimHorizon,
                    _ => StartPosition::Latest,
                };
                let mut source =
                    KinesisSource::new(client, &stream_name, start_from)
                        .with_max_batch(budget.batch_size);
                if let Some(resume_seq) = resume_positions
                    .as_ref()
                    .and_then(|positions| positions.get(&input_def.name))
                    .and_then(|position| match position {
                        InputPosition::Kinesis(map) => Some(map.clone()),
                        _ => None,
                    })
                {
                    source = source.with_resume_positions(resume_seq);
                }
                let tracker = PositionTracker::new();
                name_to_sources.insert(
                    input_def.name.clone(),
                    InputSources::Kinesis {
                        source: Arc::new(source),
                        tracker: tracker.clone(),
                        format: prepared.format,
                    },
                );
                progress_sources.push((
                    input_def.name.clone(),
                    InputProgressSource::Positions(tracker),
                ));
            }
            InputKind::Sqs => {
                if window.is_some() {
                    anyhow::bail!(
                        "una ventana lee un topic; el input '{}' es sqs",
                        input_def.name
                    );
                }
                let sqs_cfg = config
                    .connectors
                    .sqs
                    .as_ref()
                    .context("sqs requiere connectors.sqs.region")?;
                let client = crate::aws::sqs_client(sqs_cfg).await?;
                let queue = crate::aws::queue_url(
                    &client,
                    input_def.sqs_queue().expect("la validación garantiza una cola"),
                )
                .await?;
                let wait = input_def
                    .receive_wait
                    .as_deref()
                    .map(parse_fixed_duration)
                    .transpose()?
                    .map(|ms| Duration::from_millis(ms as u64))
                    .unwrap_or(SQS_DEFAULT_WAIT);
                // El visibility timeout supera el intervalo de commit: un
                // mensaje que vuelve a ser visible antes del commit se consume
                // dos veces y el sink lo deduplica (PK + sequence.field).
                let visibility = options.commit_interval + Duration::from_secs(60);
                let source = SqsSource::new(client.clone(), &queue, wait)
                    .with_visibility_timeout(visibility);
                let sources: Vec<Arc<SqsSource>> =
                    (0..n_consumers).map(|_| Arc::new(source.clone())).collect();
                let receipts = fresh_receipts();
                if use_lanes {
                    lane_outs = (0..n_consumers)
                        .map(|_| LaneOutput::Receipts {
                            queue: queue.clone(),
                            out: fresh_receipts(),
                        })
                        .collect();
                }
                name_to_sources.insert(
                    input_def.name.clone(),
                    InputSources::Sqs {
                        sources,
                        format: prepared.format,
                        receipts: receipts.clone(),
                    },
                );
                progress_sources.push((
                    input_def.name.clone(),
                    InputProgressSource::Receipts {
                        queue: queue.clone(),
                        out: receipts,
                    },
                ));
                sqs_deleters.push((client, queue));
            }
            InputKind::Table => {
                anyhow::bail!(
                    "el input '{}' es una tabla Paimon y no entra al loop de streaming",
                    input_def.name
                );
            }
        }
        inputs.push(InputSource {
            name: input_def.name.clone(),
            schema: schema.clone(),
        });
    }

    // --- Factory de producción: cablea cada input a su StreamingTable ---
    // Mapa nombre lógico -> sources (N consumidores por input, mismo grupo).
    // `batch_size` es la unidad de flujo end-to-end: un lote drenado del
    // broker se decodifica a un único `RecordBatch`.
    let batch_size = budget.batch_size;
    let decode_parallelism = budget.decode_parallelism;
    let lanes_on = Arc::new(std::sync::atomic::AtomicBool::new(use_lanes));
    let factory_lanes = lanes_on.clone();
    let factory_outs = lane_outs.clone();
    let factory_trackers = lane_trackers.clone();
    let factory: Box<StreamTableFactory> = Box::new(move |name, schema| {
        let entry = name_to_sources
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("source no encontrado para '{name}'"))?;
        let (decode_schema, inner_schema, row_partitions) =
            decode_plan(&schema, &prepared_lookup, name);
        match entry {
            InputSources::Topic {
                sources,
                tracker,
                format,
                lane_lots,
            } => {
                let decoder = Decoder::new(decode_schema, format);
                if factory_lanes.load(std::sync::atomic::Ordering::SeqCst) {
                    // Una partición por consumidor. Dos decodes en vuelo por
                    // carril: mientras uno decodifica, el loop junta el lote
                    // siguiente.
                    let per_lane = (decode_parallelism / sources.len().max(1)).max(2);
                    let lane_trackers = factory_trackers.clone();
                    let partitions: Vec<Arc<dyn datafusion::physical_plan::streaming::PartitionStream>> =
                        sources
                            .iter()
                            .zip(lane_lots.iter())
                            .zip(factory_outs.iter())
                            .enumerate()
                            .map(|(index, ((source, lots), out))| {
                                let source = source.clone();
                                let make_stream: Arc<dyn Fn() -> tachyon_source::consumer::RecordStream + Send + Sync> =
                                    Arc::new(move || source.record_stream());
                                let lane = RedpandaPartitionStream::new(
                                    index as i32,
                                    inner_schema.clone(),
                                    decoder.clone(),
                                    batch_size,
                                    make_stream,
                                )
                                .with_decode_parallelism(per_lane)
                                .with_row_partitions(row_partitions);
                                let lane = match lane_trackers.get(index) {
                                    Some(tracker) => lane.with_offset_tracker(tracker.clone()),
                                    None => {
                                        let LaneOutput::Ranges(out) = out else {
                                            unreachable!("un carril de topic publica rangos")
                                        };
                                        lane.with_lane(lots.clone(), out.clone())
                                    }
                                };
                                Arc::new(lane) as Arc<dyn datafusion::physical_plan::streaming::PartitionStream>
                            })
                            .collect();
                    let table = datafusion::catalog::streaming::StreamingTable::try_new(schema, partitions)
                        .map_err(|e| anyhow::anyhow!("creando StreamingTable: {e}"))?;
                    return Ok(Arc::new(table.with_infinite_table(true)));
                }
                for source in &sources {
                    source.drop_lot_ranges();
                }
                // Un `record_stream()` por consumidor, fusionados con
                // `select_all`: el grupo reparte las particiones entre ellos y
                // cada instancia drena su propio buffer de fetch en paralelo.
                // La factory se invoca una vez por ejecución, así que cada
                // consumidor se `take()`a una vez.
                let make_stream = Arc::new(move || {
                    let streams: Vec<tachyon_source::consumer::RecordStream> = sources
                        .iter()
                        .map(|s| s.record_stream())
                        .collect();
                    // Coerción explícita a `RecordStream` (el `SelectAll`
                    // concreto no se coercea solo a `Pin<Box<dyn Stream>>` en
                    // posición de retorno).
                    let merged: tachyon_source::consumer::RecordStream =
                        Box::pin(futures::stream::select_all(streams));
                    merged
                });
                let inner = RedpandaPartitionStream::new(
                    0,
                    inner_schema,
                    decoder,
                    batch_size,
                    make_stream,
                )
                .with_offset_tracker(tracker)
                .with_decode_parallelism(decode_parallelism)
                .with_row_partitions(row_partitions);
                let ps = as_partition(inner, &prepared_lookup, name);
                let table = datafusion::catalog::streaming::StreamingTable::try_new(
                    schema,
                    vec![ps],
                )
                .map_err(|e| anyhow::anyhow!("creando StreamingTable: {e}"))?;
                Ok(Arc::new(table.with_infinite_table(true)))
            }
            InputSources::Kinesis {
                source,
                tracker,
                format,
            } => {
                let decoder = Decoder::new(decode_schema, format);
                // Un consumidor por stream: el poll loop lee todos los shards
                // en paralelo y las posiciones llegan por lote en
                // `LotPositions` (ver `tachyon-source::kinesis`).
                let positions = LotPositions::new();
                let owned = (*source).clone().with_lot_positions(positions.clone());
                let make_stream: Arc<dyn Fn() -> tachyon_source::consumer::RecordStream + Send + Sync> =
                    Arc::new(move || owned.record_stream());
                let inner = RedpandaPartitionStream::new(
                    0,
                    inner_schema,
                    decoder,
                    batch_size,
                    make_stream,
                )
                .with_position_tracker(tracker, positions)
                .with_decode_parallelism(decode_parallelism)
                .with_row_partitions(row_partitions);
                let table = datafusion::catalog::streaming::StreamingTable::try_new(
                    schema,
                    vec![Arc::new(inner)],
                )
                .map_err(|e| anyhow::anyhow!("creando StreamingTable: {e}"))?;
                Ok(Arc::new(table.with_infinite_table(true)))
            }
            InputSources::Sqs {
                sources,
                format,
                receipts,
                ..
            } => {
                let decoder = Decoder::new(decode_schema, format);
                if factory_lanes.load(std::sync::atomic::Ordering::SeqCst) {
                    // Un consumidor por carril; cada carril publica sus
                    // receipts en su propio `ReceiptsOut`.
                    let per_lane = (decode_parallelism / sources.len().max(1)).max(2);
                    let partitions: Vec<Arc<dyn datafusion::physical_plan::streaming::PartitionStream>> =
                        sources
                            .iter()
                            .enumerate()
                            .map(|(index, source)| {
                                let source = source.clone();
                                let make_stream: Arc<dyn Fn() -> tachyon_source::consumer::RecordStream + Send + Sync> =
                                    Arc::new(move || source.record_stream());
                                let lane = RedpandaPartitionStream::new(
                                    index as i32,
                                    inner_schema.clone(),
                                    decoder.clone(),
                                    batch_size,
                                    make_stream,
                                )
                                .with_decode_parallelism(per_lane)
                                .with_row_partitions(row_partitions);
                                let lane = match &factory_outs[index] {
                                    LaneOutput::Receipts { out, .. } => {
                                        lane.with_receipts(out.clone())
                                    }
                                    LaneOutput::Ranges(_) => {
                                        unreachable!("un carril SQS publica receipts")
                                    }
                                };
                                Arc::new(lane) as Arc<dyn datafusion::physical_plan::streaming::PartitionStream>
                            })
                            .collect();
                    let table = datafusion::catalog::streaming::StreamingTable::try_new(schema, partitions)
                        .map_err(|e| anyhow::anyhow!("creando StreamingTable: {e}"))?;
                    return Ok(Arc::new(table.with_infinite_table(true)));
                }
                // N consumidores compitiendo, fusionados con `select_all`: los
                // receipts viajan con los registros (`position`) y el decode
                // los extrae por batch.
                let make_stream = Arc::new(move || {
                    let streams: Vec<tachyon_source::consumer::RecordStream> = sources
                        .iter()
                        .map(|s| s.record_stream())
                        .collect();
                    let merged: tachyon_source::consumer::RecordStream =
                        Box::pin(futures::stream::select_all(streams));
                    merged
                });
                let inner = RedpandaPartitionStream::new(
                    0,
                    inner_schema,
                    decoder,
                    batch_size,
                    make_stream,
                )
                .with_receipts(receipts)
                .with_decode_parallelism(decode_parallelism)
                .with_row_partitions(row_partitions);
                let table = datafusion::catalog::streaming::StreamingTable::try_new(
                    schema,
                    vec![Arc::new(inner)],
                )
                .map_err(|e| anyhow::anyhow!("creando StreamingTable: {e}"))?;
                Ok(Arc::new(table.with_infinite_table(true)))
            }
        }
    });

    // --- Ejecuta la transformación (validada como pass-through) ---
    // UNION ALL planifica cada rama aparte. El SQL completo iría a un
    // UnionExec, que puede leer una rama por delante de lo publicado.
    let mut union_feed = None;
    let mut stream = None;
    let mut lane_streams: Vec<datafusion::physical_plan::SendableRecordBatchStream> = Vec::new();
    if let Some(branches) = union_branches {
        let sources = assemble_union_sources(branches, &inputs, &progress_sources)?;
        union_feed = Some(
            UnionFeed::start(&sources, &factory)
                .await
                .context("armando UNION ALL")?,
        );
    } else {
        let (mut plan, mut task_ctx) = plan_query(&planned_sql, &inputs, &factory)
            .await
            .context("planificando la transformación")?;
        if use_lanes {
            match ensure_passthrough_lanes(&plan, n_consumers) {
                Ok(()) => {
                    tracing::info!(lanes = n_consumers, "un carril por consumidor");
                    for index in 0..n_consumers {
                        lane_streams.push(
                            plan.execute(index, task_ctx.clone())
                                .context("ejecutando un carril de la transformación")?,
                        );
                    }
                }
                Err(e) => {
                    // P. ej. un LIMIT: es global, no se reparte en carriles.
                    tracing::info!(motivo = %e, "el plan no se reparte en carriles: un solo stream");
                    lanes_on.store(false, std::sync::atomic::Ordering::SeqCst);
                    (plan, task_ctx) = plan_query(&planned_sql, &inputs, &factory)
                        .await
                        .context("planificando la transformación")?;
                }
            }
        }
        if lane_streams.is_empty() {
            ensure_passthrough(&plan).context("la transformación no admite exactly-once")?;
            stream = Some(
                datafusion::physical_plan::execute_stream(plan, task_ctx)
                    .context("ejecutando la transformación")?,
            );
        }
    }

    if let Some(shape) = window {
        let window_lanes: Vec<(crate::execute::TransformOutput, OffsetTracker)> =
            if lane_streams.is_empty() {
                let stream = stream.take().context("la ventana lee un solo stream")?;
                let tracker = trackers
                    .first()
                    .map(|(_, tracker)| tracker.clone())
                    .context("la ventana lee un topic")?;
                vec![(stream, tracker)]
            } else {
                std::mem::take(&mut lane_streams)
                    .into_iter()
                    .zip(lane_trackers.iter().cloned())
                    .collect()
            };
        let user_schema = input_codecs
            .get(&shape.source)
            .with_context(|| format!("sin schema para '{}'", shape.source))?
            .schema
            .clone();
        let (lag_ms, idle_ms) = validate_window(config, shape, user_schema.as_ref(), &sink.field_names())?;
        let ticket_table = sink.tickets();
        return drive_window(
            shape,
            lag_ms,
            idle_ms,
            user_schema,
            window_lanes,
            sink,
            trackers,
            commit_sources,
            metrics,
            options.commit_interval,
            restored_window,
            metrics_addr,
            window_handoff,
            n_consumers,
            adopt_rx.take().unwrap_or_else(|| {
                let (_, rx) = tokio::sync::mpsc::unbounded_channel();
                rx
            }),
            ticket_table,
        )
        .await;
    }

    // --- Writer task + commit task: decoupla el write y el commit de Paimon ---
    // El checkpoint de Paimon tiene dos fases con costos muy distintos
    // (medido): el prepare (flush+encode de los buffers, ~1.3s por epoch de
    // 260MB) y el commit del snapshot (manifest, ~10ms). Hacer el prepare en
    // el writer task lo dejaba sin escribir ~40% del tiempo a intervalos de
    // 2s. En su lugar, en cada tick el writer task ROTA el TableWrite (swap
    // O(1) por uno fresco) y manda el writer lleno al commit task, que hace
    // prepare + offsets + snapshot en background y EN ORDEN (un solo task,
    // identifiers monótonos por commit_user). El canal de epochs acotado (1
    // en cola + 1 commiteando) frena al writer si los commits no dan abasto
    // (backpressure, acota la memoria de datos no commiteados).
    let commit_interval = options.commit_interval;
    // Canal acotado por FILAS, no por batches: con batches de `batch_size`
    // filas, una capacidad fija en batches escala la memoria en vuelo sin
    // control (512 × 32K filas ≈ 16M filas). 1M filas absorbe el jitter del
    // commit de Paimon sin inflar el RSS de la instancia.
    let channel_batches = budget.channel_batches();
    let (batch_tx, mut batch_rx) =
        tokio::sync::mpsc::channel::<(arrow::array::RecordBatch, Progress)>(channel_batches);

    // Los buckets se reparten entre `sink_shards` writers: escritura y flush
    // en paralelo (ver `tachyon_sink::shard`).
    let (mut sink_writer, mut sink_committer) = sink
        .split_sharded(budget.sink_shards)
        .context("repartiendo el sink por bucket")?;
    let (epoch_tx, mut epoch_rx) = tokio::sync::mpsc::channel::<CheckpointEpoch>(1);
    let commit_metrics = metrics.clone();
    let commit_task: tokio::task::JoinHandle<Result<(), anyhow::Error>> =
        tokio::spawn(async move {
            // Commit task: procesa los epochs EN ORDEN (FIFO del canal).
            while let Some(epoch) = epoch_rx.recv().await {
                let t_commit = Instant::now();
                let kafka_offsets = epoch.body.applied_offsets();
                let result = sink_committer
                    .commit_epoch(epoch.writer, epoch.identifier, &epoch.body)
                    .await;
                commit_metrics.add_commit_ns(t_commit.elapsed().as_nanos() as u64);
                let Some(identifier) = result.context("checkpoint del sink")? else {
                    // Epoch sin archivos nuevos: los receipts del tramo se
                    // consumieron igual (sus filas, si las hubo, salieron
                    // filtradas), así que se borran de todos modos.
                    for (client, queue) in &sqs_deleters {
                        let handles = epoch.receipts.get(queue).cloned().unwrap_or_default();
                        delete_sqs_messages(client, queue, &handles).await;
                    }
                    continue;
                };
                tracing::debug!(identifier, "checkpoint commiteado");
                commit_metrics.inc_commits();
                // Commit informativo en Kafka (lag, arranque rápido): la
                // fuente de verdad del progreso es el checkpoint en Paimon,
                // así que un fallo acá no rompe el exactly-once.
                for (topic, source) in &commit_sources {
                    if let Some(partitions) = kafka_offsets.get(topic) {
                        if let Err(e) = source.commit_offsets(partitions).await {
                            tracing::warn!(error = %e, topic, "commit de offsets en Kafka");
                            commit_metrics.inc_errors();
                        }
                    }
                }
                // SQS: borrado SOLO después del commit (at-least-once). Un
                // fallo es un warn: el mensaje se reentrega y el sink
                // deduplica (PK + sequence.field).
                for (client, queue) in &sqs_deleters {
                    let handles = epoch.receipts.get(queue).cloned().unwrap_or_default();
                    delete_sqs_messages(client, queue, &handles).await;
                }
            }
            Ok(())
        });

    let writer_metrics = metrics.clone();
    // Pipeline mixta: el sidecar es v3 (posiciones por input) en vez de los
    // offsets crudos v0.
    let mixed = config
        .inputs
        .iter()
        .any(|input| input.kind() != InputKind::Topic);
    let writer: tokio::task::JoinHandle<Result<(), anyhow::Error>> = tokio::spawn(async move {
        // Offsets del próximo checkpoint. Arranca en el checkpoint recuperado
        // para arrastrar las particiones que no avanzan en esta ejecución.
        let mut offsets: SourceOffsets = resume.unwrap_or_default();
        // Posiciones por input (sidecar v3): arrancan en el checkpoint
        // recuperado. Solo se commitean cuando hay un input no-Kafka.
        let mut positions: InputPositions = resume_positions.unwrap_or_default();
        // Receipts SQS pendientes de borrado (se borran tras el commit).
        let mut pending_receipts: BTreeMap<String, Vec<String>> = BTreeMap::new();
        // Carriles: la frontera por partición (arranca en el checkpoint).
        let mut frontier = lane_cursor.map(|(topic, cursor)| {
            let start = offsets.get(&topic).cloned().unwrap_or_default();
            (topic, cursor, tachyon_core::Frontier::new(&start))
        });
        let mut commit_timer = tokio::time::interval(commit_interval);
        commit_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        commit_timer.tick().await; // primera tick inmediata
        let mut dirty = false;
        // Si el commit task murió, el canal se cierra y el handoff falla: se
        // propaga el error real al final (del JoinHandle).
        let mut handoff_error: Option<anyhow::Error> = None;
        loop {
            tokio::select! {
                maybe_batch = batch_rx.recv() => {
                    match maybe_batch {
                        Some((batch, batch_offsets)) => {
                            let t_write = Instant::now();
                            sink_writer.write(&batch)
                                .await
                                .context("escribiendo al sink Paimon")?;
                            writer_metrics.add_write_ns(t_write.elapsed().as_nanos() as u64);
                            match batch_offsets {
                                Progress::Snapshot(snapshot) => merge_offsets(&mut offsets, &snapshot),
                                Progress::Ranges(ranges) => {
                                    let (topic, cursor, frontier) = frontier
                                        .as_mut()
                                        .context("rangos de carril sin frontera")?;
                                    for range in ranges {
                                        frontier.cover(range, |p| cursor.origin(p));
                                    }
                                    offsets.insert(topic.clone(), frontier.offsets().clone());
                                }
                                Progress::Input { name, progress } => {
                                    match progress {
                                        InputProgress::Offsets(o) => {
                                            merge_offsets(&mut offsets, &o);
                                            if let Some((_, parts)) = o.iter().next() {
                                                let update = BTreeMap::from([(
                                                    name.clone(),
                                                    InputPosition::Kafka(parts.clone()),
                                                )]);
                                                merge_positions(&mut positions, &update);
                                            }
                                        }
                                        InputProgress::Positions(p) => {
                                            let update = BTreeMap::from([(
                                                name.clone(),
                                                InputPosition::Kinesis(p),
                                            )]);
                                            merge_positions(&mut positions, &update);
                                        }
                                        InputProgress::Receipts { queue, handles } => {
                                            if !handles.is_empty() {
                                                pending_receipts
                                                    .entry(queue)
                                                    .or_default()
                                                    .extend(handles);
                                            }
                                            if !positions.contains_key(&name) {
                                                positions.insert(name.clone(), InputPosition::Sqs);
                                            }
                                        }
                                    }
                                }
                            }
                            writer_metrics.inc_rows_written(batch.num_rows() as u64);
                            dirty = true;
                        }
                        None => break, // canal cerrado (fin del stream)
                    }
                }
                _ = commit_timer.tick() => {
                    if dirty {
                        // Rotación O(1): el epoch cerrado se commitea en
                        // background y se sigue escribiendo de inmediato.
                        let (identifier, full_writer) = sink_writer
                            .rotate()
                            .await
                            .context("rotando el writer del sink")?;
                        let body = if mixed {
                            CheckpointBody::Positions(positions.clone())
                        } else {
                            CheckpointBody::Offsets(offsets.clone())
                        };
                        let receipts = std::mem::take(&mut pending_receipts);
                        let epoch = CheckpointEpoch {
                            writer: full_writer,
                            identifier,
                            body,
                            receipts,
                        };
                        if epoch_tx.send(epoch).await.is_err() {
                            handoff_error = Some(anyhow::anyhow!(
                                "el task de commit terminó inesperadamente"
                            ));
                            break;
                        }
                        dirty = false;
                    }
                }
            }
        }
        // Checkpoint final (fin del stream).
        if dirty && handoff_error.is_none() {
            let (identifier, full_writer) = sink_writer
                .rotate()
                .await
                .context("rotando el writer del sink")?;
            let body = if mixed {
                CheckpointBody::Positions(positions.clone())
            } else {
                CheckpointBody::Offsets(offsets.clone())
            };
            let receipts = std::mem::take(&mut pending_receipts);
            let _ = epoch_tx
                .send(CheckpointEpoch {
                    writer: full_writer,
                    identifier,
                    body,
                    receipts,
                })
                .await;
        }
        drop(epoch_tx);
        // Esperar a que el commit task termine de commitear los epochs
        // pendientes; un error ahí rompe el exactly-once: propagar.
        let commit_result = commit_task.await.context("task de commit");
        match commit_result {
            Ok(Ok(())) => handoff_error.map_or(Ok(()), Err),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(e),
        }
    });

    // --- Carriles: un task por partición, cada uno manda sus batches con
    // los rangos de offsets (topic) o los receipts (SQS) que cubren ---
    if !lane_streams.is_empty() {
        let mut lanes = tokio::task::JoinSet::new();
        let lane_name = config.inputs[0].name.clone();
        for (mut lane, out) in lane_streams.into_iter().zip(lane_outs.into_iter()) {
            let tx = batch_tx.clone();
            let metrics = metrics.clone();
            let name = lane_name.clone();
            lanes.spawn(async move {
                loop {
                    let t_next = Instant::now();
                    let next = lane.next().await;
                    metrics.add_source_next_ns(t_next.elapsed().as_nanos() as u64);
                    let Some(batch) = next else { break };
                    let batch = batch.context("batch de salida")?;
                    // Con un plan pass-through, lo que el carril emitió hasta
                    // este batch ya salió en él o en uno anterior.
                    let progress = match &out {
                        LaneOutput::Ranges(out) => {
                            let ranges =
                                std::mem::take(&mut *out.lock().expect("lock de rangos del carril"));
                            Progress::Ranges(ranges)
                        }
                        LaneOutput::Receipts { queue, out } => {
                            let handles = out
                                .lock()
                                .expect("lock de receipts")
                                .pop_front()
                                .unwrap_or_default();
                            Progress::Input {
                                name: name.clone(),
                                progress: InputProgress::Receipts {
                                    queue: queue.clone(),
                                    handles,
                                },
                            }
                        }
                    };
                    metrics.inc_rows_read(batch.num_rows() as u64);
                    let t_send = Instant::now();
                    tx.send((batch, progress))
                        .await
                        .context("enviando batch al writer")?;
                    metrics.add_send_wait_ns(t_send.elapsed().as_nanos() as u64);
                }
                Ok::<(), anyhow::Error>(())
            });
        }
        while let Some(done) = lanes.join_next().await {
            done.context("un carril terminó con pánico")??;
        }
        drop(batch_tx);
        writer.await.context("task del writer")??;
        return Ok(PipelineHandle {
            metrics_addr,
            metrics: metrics.clone(),
        });
    }

    // --- Loop de consumo: tira de DataFusion y envía batches al writer ---
    loop {
        let t_next = Instant::now();
        let next = if let Some(feed) = union_feed.as_mut() {
            feed.next().await
        } else {
            let stream = stream.as_mut().context("el pipeline no tiene stream")?;
            match stream.next().await {
                Some(Ok(batch)) => {
                    // Con un plan pass-through, todo lo que la fuente emitió
                    // hasta ahora ya produjo su salida. El progreso sale del
                    // mecanismo del input (offsets, posiciones o receipts).
                    let progress = match progress_sources.first() {
                        Some((_, InputProgressSource::Offsets { .. })) => {
                            let batch_offsets: SourceOffsets = trackers
                                .iter()
                                .map(|(topic, tracker)| (topic.clone(), tracker.snapshot()))
                                .collect();
                            Progress::Snapshot(batch_offsets)
                        }
                        Some((name, InputProgressSource::Positions(tracker))) => {
                            Progress::Input {
                                name: name.clone(),
                                progress: InputProgress::Positions(tracker.snapshot()),
                            }
                        }
                        Some((name, InputProgressSource::Receipts { queue, out })) => {
                            let handles = out
                                .lock()
                                .expect("lock de receipts")
                                .pop_front()
                                .unwrap_or_default();
                            Progress::Input {
                                name: name.clone(),
                                progress: InputProgress::Receipts {
                                    queue: queue.clone(),
                                    handles,
                                },
                            }
                        }
                        None => Progress::Snapshot(SourceOffsets::new()),
                    };
                    Some(Ok((batch, progress)))
                }
                Some(Err(err)) => Some(Err(anyhow::anyhow!(err))),
                None => None,
            }
        };
        metrics.add_source_next_ns(t_next.elapsed().as_nanos() as u64);
        match next {
            Some(Ok((batch, batch_offsets))) => {
                let rows = batch.num_rows() as u64;
                tracing::debug!(rows, "batch de salida desde DataFusion");
                metrics.inc_rows_read(rows);
                // Backpressure limitada: bloquea solo si el writer no da abasto.
                let t_send = Instant::now();
                batch_tx
                    .send((batch, batch_offsets))
                    .await
                    .context("enviando batch al writer")?;
                metrics.add_send_wait_ns(t_send.elapsed().as_nanos() as u64);
            }
            Some(Err(err)) => return Err(err).context("batch de salida"),
            None => break, // stream terminó
        }
    }

    // Fin del stream: cerrar el canal y esperar el commit final del writer.
    drop(batch_tx);
    writer.await.context("task del writer")??;

    Ok(PipelineHandle {
        metrics_addr,
        metrics: metrics.clone(),
    })
}

/// Publica la salida en un topic. El commit es una transacción de Redpanda:
/// los registros y los offsets de entrada aparecen juntos. Un consumidor con
/// `read_committed` no ve un lote cuyo commit no terminó.
///
/// Un solo input y un solo consumidor: la transacción commitea el grupo de
/// ese miembro. Una ventana no entra por acá: su estado vive en el warehouse.
pub async fn run_topic_pipeline(
    config: &PipelineConfig,
    select_sql: &str,
    options: &RunOptions,
    topic: &str,
    key: &str,
    input_codecs: &std::collections::HashMap<String, PreparedInput>,
    metrics: &Arc<InstanceMetrics>,
) -> Result<PipelineHandle> {
    run_topic_pipeline_with_lookup(
        config,
        select_sql,
        options,
        topic,
        key,
        input_codecs,
        metrics,
        None,
        None,
    )
    .await
}

pub(crate) async fn run_topic_pipeline_with_lookup(
    config: &PipelineConfig,
    select_sql: &str,
    options: &RunOptions,
    topic: &str,
    key: &str,
    input_codecs: &std::collections::HashMap<String, PreparedInput>,
    metrics: &Arc<InstanceMetrics>,
    lookup: Option<&LookupJoin>,
    union_branches: Option<&[UnionBranch]>,
) -> Result<PipelineHandle> {
    if let Some(branches) = union_branches {
        if lookup.is_some() {
            anyhow::bail!("UNION ALL no se mezcla con una ventana, un join o un lookup");
        }
        return run_union_topic(
            config,
            branches,
            options,
            topic,
            key,
            input_codecs,
            metrics,
        )
        .await;
    }
    if config.inputs.len() != 1 {
        anyhow::bail!(
            "el sink a un topic tiene un solo input: la transacción commitea un consumer group"
        );
    }
    if config
        .deployment
        .consumers_per_topic
        .is_some_and(|count| count != 1)
    {
        anyhow::bail!(
            "el sink a un topic usa un consumidor: la transacción commitea los offsets de ese miembro"
        );
    }
    let budget = StatelessBudget::resolve(config).context("presupuesto del pipeline")?;
    tracing::info!(
        cpus = budget.cpus,
        batch_size = budget.batch_size,
        topic,
        transactional_id = %options.commit_user,
        "exactly-once activo (topic); el commit es la transacción de Redpanda"
    );

    let metrics_addr = if let Some(bind) = options.metrics_bind {
        let server = MetricsServer::new(bind, metrics.clone());
        Some(server.start().await.context("arrancando métricas")?)
    } else {
        None
    };

    let brokers = config.redpanda()?.brokers.join(",");
    ensure_topic_partitions(&brokers, topic, config.deployment.partitions).await?;

    let input_def = &config.inputs[0];
    let prepared = input_codecs
        .get(&input_def.name)
        .cloned()
        .with_context(|| format!("sin schema para el input '{}'", input_def.name))?;
    if !format_matches(input_def.format, &prepared.format) {
        anyhow::bail!(
            "el input '{}' declara format {:?} pero el codec cargado no coincide",
            input_def.name,
            input_def.format
        );
    }
    let source_group = format!("{}-{}", options.group_id, input_def.name);
    let mut source_cc = source_client_config(config, &options.group_id, &budget)?;
    source_cc.set("group.id", &source_group);
    let input_topic = input_def.kafka_topic()?.to_string();
    let source = Arc::new(
        RdkafkaSource::new(&source_cc, &input_topic)
            .with_context(|| format!("creando source para '{}'", input_def.name))?
            .with_max_batch(budget.batch_size),
    );
    let tracker = OffsetTracker::new();
    let stream_tracker = tracker.clone();
    let input_sources = vec![source.clone()];
    let decoder_format = prepared.format.clone();
    let batch_size = budget.batch_size;
    let decode_parallelism = budget.decode_parallelism;
    let input_name = input_def.name.clone();
    let prepared_lookup = match lookup {
        Some(join) => {
            if join.fact != input_def.name {
                anyhow::bail!("el lookup enriquece '{}'", join.fact);
            }
            Some(prepare_lookup(config, join, select_sql, prepared.schema.clone()).await?)
        }
        None => None,
    };
    let input_schema = input_table_schema(
        prepared.schema.clone(),
        &input_def.name,
        false,
        &prepared_lookup,
    )?;
    let planned_sql = prepared_lookup
        .as_ref()
        .map(|item| item.sql.clone())
        .unwrap_or_else(|| select_sql.to_string());
    let factory: Box<StreamTableFactory> = Box::new(move |name, schema| {
        if name != input_name {
            anyhow::bail!("source no encontrado para '{name}'");
        }
        let (decode_schema, inner_schema, _row_partitions) =
            decode_plan(&schema, &prepared_lookup, name);
        let decoder = Decoder::new(decode_schema, decoder_format.clone());
        let sources = input_sources.clone();
        let make_stream = Arc::new(move || {
            let merged: tachyon_source::consumer::RecordStream =
                Box::pin(futures::stream::select_all(
                    sources.iter().map(|s| s.record_stream()),
                ));
            merged
        });
        let inner = RedpandaPartitionStream::new(0, inner_schema, decoder, batch_size, make_stream)
            .with_offset_tracker(stream_tracker.clone())
            .with_decode_parallelism(decode_parallelism);
        let ps = as_partition(inner, &prepared_lookup, name);
        let table = datafusion::catalog::streaming::StreamingTable::try_new(schema, vec![ps])
            .map_err(|e| anyhow::anyhow!("creando StreamingTable: {e}"))?;
        Ok(Arc::new(table.with_infinite_table(true)))
    });
    let inputs = vec![InputSource {
        name: input_def.name.clone(),
        schema: input_schema,
    }];
    let (plan, task_ctx) = plan_query(&planned_sql, &inputs, &factory)
        .await
        .context("planificando la transformación")?;
    ensure_passthrough(&plan).context("la transformación no admite exactly-once")?;
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx)
        .context("ejecutando la transformación")?;
    let out_schema = stream.schema();
    let key_field = out_schema
        .field_with_name(key)
        .map_err(|_| anyhow::anyhow!("la salida no tiene la clave '{key}'"))?;
    match key_field.data_type() {
        arrow::datatypes::DataType::Int64
        | arrow::datatypes::DataType::Int32
        | arrow::datatypes::DataType::Utf8 => {}
        other => anyhow::bail!(
            "la clave '{key}' es {other}; el topic la publica como Int64, Int32 o Utf8"
        ),
    }

    let timeout_ms = (options.commit_interval.as_millis() as u64)
        .saturating_mul(4)
        .max(120_000);
    let wire = match config.output.format {
        PayloadFormat::Json => TopicWire::Json,
        PayloadFormat::Avro => {
            let url = config
                .connectors
                .schema_registry
                .as_ref()
                .context("format avro requiere connectors.schema_registry.url")?
                .url
                .clone();
            let avsc = avro_json_from_arrow(out_schema.as_ref())?;
            let schema = parse_avro_schema(&avsc)?;
            let id = register_topic_schema(&url, topic, &avsc)
                .context("registrando el schema de la salida")?;
            tracing::info!(topic, schema_id = id, "schema de salida registrado");
            TopicWire::Avro { schema, id }
        }
    };
    let sink = RedpandaSink::open(&brokers, topic, key, &options.commit_user, timeout_ms).await?;

    // El snapshot se toma cuando `next()` vuelve, antes de tirar del siguiente
    // lote: en un pass-through ese mapa es exactamente lo que esta salida cubre.
    let snap_tracker = tracker;
    let (batch_tx, mut batch_rx) =
        tokio::sync::mpsc::channel::<(Result<arrow::array::RecordBatch, datafusion::error::DataFusionError>, BTreeMap<i32, i64>)>(1);
    let stream_task = tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            let snap = snap_tracker.snapshot();
            if batch_tx.send((item, snap)).await.is_err() {
                break;
            }
        }
    });
    let _stop_stream = AbortOnDrop(stream_task);

    let mut tick = tokio::time::interval(options.commit_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    let mut offsets = SourceOffsets::new();
    let mut in_txn = false;
    loop {
        tokio::select! {
            batch = batch_rx.recv() => {
                match batch {
                    Some((batch, snap)) => {
                        let batch = batch.context("batch de salida")?;
                        if !in_txn {
                            sink.begin().await?;
                            in_txn = true;
                        }
                        let rows = match &wire {
                            TopicWire::Json => sink.write(&batch).await.context("publicando el batch")?,
                            TopicWire::Avro { schema, id } => {
                                let records = encode_envelopes(&batch, key, schema, *id)
                                    .context("codificando Avro")?;
                                sink.write_records(&records)
                                    .await
                                    .context("publicando el batch")?
                            }
                        };
                        let mut covered = SourceOffsets::new();
                        covered.insert(input_topic.clone(), snap);
                        merge_offsets(&mut offsets, &covered);
                        metrics.inc_rows_read(rows as u64);
                        metrics.inc_rows_written(rows as u64);
                    }
                    None => break,
                }
            }
            _ = tick.tick() => {
                if in_txn {
                    commit_topic_epoch(&sink, &source, &input_topic, &offsets).await?;
                    in_txn = false;
                    metrics.inc_commits();
                    tracing::info!(topic, "transacción de topic commiteada");
                }
            }
        }
    }
    if in_txn {
        commit_topic_epoch(&sink, &source, &input_topic, &offsets).await?;
        metrics.inc_commits();
    }
    Ok(PipelineHandle {
        metrics_addr,
        metrics: metrics.clone(),
    })
}

enum TopicWire {
    Json,
    Avro {
        schema: std::sync::Arc<apache_avro::Schema>,
        id: i32,
    },
}

async fn commit_topic_epoch(
    sink: &RedpandaSink,
    source: &RdkafkaSource,
    input_topic: &str,
    offsets: &SourceOffsets,
) -> Result<()> {
    let parts = offsets
        .get(input_topic)
        .cloned()
        .unwrap_or_default();
    if parts.is_empty() {
        anyhow::bail!("el epoch publicó filas y no tiene offsets de entrada");
    }
    let metadata = source.group_metadata().await.context("metadata del grupo")?;
    sink.send_input_offsets(metadata.as_ptr(), input_topic, &parts)
        .context("adjuntando offsets a la transacción")?;
    sink.commit().await?;
    Ok(())
}

/// Una transacción, un `send_offsets_to_transaction` por consumer group. Los
/// `GroupMetadata` viven hasta que `commit` vuelve: el puntero tiene que
/// seguir siendo válido. Una rama que todavía no emitió no se manda; el grupo
/// conserva el offset anterior.
async fn commit_topic_groups(
    sink: &RedpandaSink,
    groups: &[(String, Arc<RdkafkaSource>)],
    offsets: &SourceOffsets,
) -> Result<()> {
    let mut held = Vec::new();
    for (topic, source) in groups {
        let Some(parts) = offsets.get(topic) else {
            continue;
        };
        if parts.is_empty() {
            continue;
        }
        let metadata = source
            .group_metadata()
            .await
            .with_context(|| format!("metadata del grupo de '{topic}'"))?;
        held.push((metadata, topic.clone(), parts.clone()));
    }
    if held.is_empty() {
        anyhow::bail!("el epoch publicó filas y no tiene offsets de entrada");
    }
    for (metadata, topic, parts) in &held {
        sink.send_input_offsets(metadata.as_ptr(), topic, parts)
            .with_context(|| format!("adjuntando offsets de '{topic}'"))?;
    }
    sink.commit().await?;
    Ok(())
}

fn assemble_union_sources(
    branches: &[UnionBranch],
    inputs: &[InputSource],
    progress_sources: &[(String, InputProgressSource)],
) -> Result<Vec<UnionSource>> {
    let mut sources = Vec::with_capacity(branches.len());
    for branch in branches {
        let progress = progress_sources
            .iter()
            .find(|(name, _)| name == &branch.source)
            .map(|(_, progress)| progress.clone())
            .with_context(|| {
                format!("UNION ALL lee un input y '{}' no es un input", branch.source)
            })?;
        let schema = inputs
            .iter()
            .find(|input| input.name == branch.source)
            .with_context(|| format!("sin schema para '{}'", branch.source))?
            .schema
            .clone();
        sources.push(UnionSource {
            name: branch.source.clone(),
            sql: branch.sql.clone(),
            schema,
            progress,
        });
    }
    Ok(sources)
}

/// Borra los mensajes commiteados de la cola (lotes de 10, tope de la API).
/// Un fallo es un warn: el contrato es at-least-once y el sink deduplica
/// (PK + sequence.field).
async fn delete_sqs_messages(client: &aws_sdk_sqs::Client, queue: &str, handles: &[String]) {
    for chunk in handles.chunks(10) {
        let mut req = client.delete_message_batch().queue_url(queue.to_string());
        for (index, handle) in chunk.iter().enumerate() {
            let entry = aws_sdk_sqs::types::DeleteMessageBatchRequestEntry::builder()
                .id(format!("{index}"))
                .receipt_handle(handle.clone())
                .build()
                .expect("entry de borrado válida");
            req = req.entries(entry);
        }
        if let Err(e) = req.send().await {
            tracing::warn!(
                error = %e,
                queue,
                count = chunk.len(),
                "DeleteMessageBatch SQS falló; los mensajes se reentregan y el sink deduplica"
            );
        }
    }
}

/// Destino del output AWS.
pub(crate) enum AwsOutput {
    Kinesis { stream: String },
    Sqs { queue: String },
}

/// Source de entrada del pipeline AWS: de dónde tira el stream y qué persiste
/// el paso de commit. Kinesis no persiste nada: un reinicio vuelve a leer
/// desde la posición de arranque.
enum AwsStreamState {
    Topic {
        source: Arc<RdkafkaSource>,
        tracker: OffsetTracker,
    },
    Kinesis {
        source: Arc<KinesisSource>,
    },
    Sqs {
        source: Arc<SqsSource>,
        client: aws_sdk_sqs::Client,
        queue: String,
        receipts: ReceiptsOut,
    },
}

/// Progreso de entrada al emitir cada batch (at-least-once): los handles se
/// pueblan en lockstep con el batch y se borran solo después de publicado.
enum AwsProgress {
    Offsets(BTreeMap<i32, i64>),
    Receipts(Vec<String>),
    None,
}

/// Qué persistir en cada intervalo de commit.
enum AwsCommit {
    Topic { source: Arc<RdkafkaSource> },
    Sqs {
        client: aws_sdk_sqs::Client,
        queue: String,
    },
    Kinesis,
}

enum AwsSink {
    Kinesis(KinesisSink),
    Sqs(SqsSink),
}

impl AwsSink {
    /// Publica el batch. Devuelve la cantidad de filas.
    async fn write(&self, batch: &arrow::array::RecordBatch) -> Result<usize> {
        match self {
            AwsSink::Kinesis(sink) => sink.write(batch).await,
            AwsSink::Sqs(sink) => sink.write(batch).await,
        }
    }
}

/// Pipeline de salida AWS (Kinesis o SQS): at-least-once.
///
/// El registro es durable al publicarse (`PutRecords` / `SendMessageBatch`),
/// así que el commit del sink es un no-op. En cada intervalo de commit se
/// persiste el progreso de entrada con el mismo orden que el camino Paimon:
/// offsets al consumer group (topic) y borrado de receipts (SQS) solo después
/// de que el batch se publicó. Un reinicio vuelve a leer desde la posición de
/// arranque y repubica: la salida puede tener duplicados y el consumidor
/// dedup con la columna de secuencia (el payload JSON la incluye).
pub(crate) async fn run_aws_pipeline(
    config: &PipelineConfig,
    select_sql: &str,
    options: &RunOptions,
    input_codecs: &std::collections::HashMap<String, PreparedInput>,
    metrics: &Arc<InstanceMetrics>,
    lookup: Option<&LookupJoin>,
    output: AwsOutput,
) -> Result<PipelineHandle> {
    if config.inputs.len() != 1 {
        anyhow::bail!(
            "el sink a kinesis/sqs tiene un solo input: la posición de arranque es por input"
        );
    }
    if config
        .deployment
        .consumers_per_topic
        .is_some_and(|count| count != 1)
    {
        anyhow::bail!("el sink a kinesis/sqs usa un consumidor");
    }
    let budget = StatelessBudget::resolve(config).context("presupuesto del pipeline")?;
    tracing::info!(
        cpus = budget.cpus,
        batch_size = budget.batch_size,
        "at-least-once activo (kinesis/sqs); el registro es durable al publicarse"
    );

    let metrics_addr = if let Some(bind) = options.metrics_bind {
        let server = MetricsServer::new(bind, metrics.clone());
        Some(server.start().await.context("arrancando métricas")?)
    } else {
        None
    };

    let input_def = &config.inputs[0];
    let prepared = input_codecs
        .get(&input_def.name)
        .cloned()
        .with_context(|| format!("sin schema para el input '{}'", input_def.name))?;
    if !format_matches(input_def.format, &prepared.format) {
        anyhow::bail!(
            "el input '{}' declara format {:?} pero el codec cargado no coincide",
            input_def.name,
            input_def.format
        );
    }

    // --- Source de entrada: topic, Kinesis o SQS ---
    let state = match input_def.kind() {
        InputKind::Topic => {
            let topic = input_def.kafka_topic()?.to_string();
            let source_group = format!("{}-{}", options.group_id, input_def.name);
            let mut source_cc = source_client_config(config, &options.group_id, &budget)?;
            source_cc.set("group.id", &source_group);
            let source = Arc::new(
                RdkafkaSource::new(&source_cc, &topic)
                    .with_context(|| format!("creando source para '{}'", input_def.name))?
                    .with_max_batch(budget.batch_size),
            );
            AwsStreamState::Topic {
                source,
                tracker: OffsetTracker::new(),
            }
        }
        InputKind::Kinesis => {
            let kinesis_cfg = config
                .connectors
                .kinesis
                .as_ref()
                .context("kinesis requiere connectors.kinesis.region")?;
            let client = crate::aws::kinesis_client(kinesis_cfg).await?;
            let stream_name = input_def
                .kinesis_stream()
                .expect("la validación garantiza un stream")
                .to_string();
            let start_from = match input_def.start_from {
                Some(StartFrom::TrimHorizon) => StartPosition::TrimHorizon,
                _ => StartPosition::Latest,
            };
            let source = KinesisSource::new(client, &stream_name, start_from)
                .with_max_batch(budget.batch_size);
            AwsStreamState::Kinesis {
                source: Arc::new(source),
            }
        }
        InputKind::Sqs => {
            let sqs_cfg = config
                .connectors
                .sqs
                .as_ref()
                .context("sqs requiere connectors.sqs.region")?;
            let client = crate::aws::sqs_client(sqs_cfg).await?;
            let queue = crate::aws::queue_url(
                &client,
                input_def.sqs_queue().expect("la validación garantiza una cola"),
            )
            .await?;
            let wait = input_def
                .receive_wait
                .as_deref()
                .map(parse_fixed_duration)
                .transpose()?
                .map(|ms| Duration::from_millis(ms as u64))
                .unwrap_or(SQS_DEFAULT_WAIT);
            // El visibility timeout supera el intervalo de commit: un mensaje
            // que vuelve a ser visible antes del commit se consume dos veces y
            // el consumidor dedup.
            let visibility = options.commit_interval + Duration::from_secs(60);
            let source = SqsSource::new(client.clone(), &queue, wait)
                .with_visibility_timeout(visibility);
            AwsStreamState::Sqs {
                source: Arc::new(source),
                client,
                queue,
                receipts: fresh_receipts(),
            }
        }
        InputKind::Table => {
            anyhow::bail!(
                "el input '{}' es una tabla Paimon y no entra al loop de streaming",
                input_def.name
            );
        }
    };

    // Lockstep con los batches: el stream publica un lote de receipts por
    // batch emitido (filtro o no), así el pop va con el batch de salida.
    let snap_fn: Arc<dyn Fn() -> AwsProgress + Send + Sync> = match &state {
        AwsStreamState::Topic { tracker, .. } => {
            let tracker = tracker.clone();
            Arc::new(move || AwsProgress::Offsets(tracker.snapshot()))
        }
        AwsStreamState::Sqs { receipts, .. } => {
            let receipts = receipts.clone();
            Arc::new(move || {
                let handles = receipts.lock().unwrap().pop_front().unwrap_or_default();
                AwsProgress::Receipts(handles)
            })
        }
        AwsStreamState::Kinesis { .. } => Arc::new(|| AwsProgress::None),
    };

    // Qué persistir en cada commit (se clona antes de mover `state` a la
    // factory).
    let commit = match &state {
        AwsStreamState::Topic { source, .. } => AwsCommit::Topic {
            source: source.clone(),
        },
        AwsStreamState::Sqs { client, queue, .. } => AwsCommit::Sqs {
            client: client.clone(),
            queue: queue.clone(),
        },
        AwsStreamState::Kinesis { .. } => AwsCommit::Kinesis,
    };

    let prepared_lookup = match lookup {
        Some(join) => {
            if join.fact != input_def.name {
                anyhow::bail!("el lookup enriquece '{}'", join.fact);
            }
            Some(prepare_lookup(config, join, select_sql, prepared.schema.clone()).await?)
        }
        None => None,
    };
    let input_schema = input_table_schema(
        prepared.schema.clone(),
        &input_def.name,
        false,
        &prepared_lookup,
    )?;
    let planned_sql = prepared_lookup
        .as_ref()
        .map(|item| item.sql.clone())
        .unwrap_or_else(|| select_sql.to_string());
    let input_name = input_def.name.clone();
    let decoder_format = prepared.format.clone();
    let batch_size = budget.batch_size;
    let decode_parallelism = budget.decode_parallelism;
    let factory: Box<StreamTableFactory> = Box::new(move |name, schema| {
        if name != input_name {
            anyhow::bail!("source no encontrado para '{name}'");
        }
        let (decode_schema, inner_schema, _row_partitions) =
            decode_plan(&schema, &prepared_lookup, name);
        let decoder = Decoder::new(decode_schema, decoder_format.clone());
        let inner = match &state {
            AwsStreamState::Topic { source, tracker } => {
                let source = source.clone();
                let make_stream: Arc<
                    dyn Fn() -> tachyon_source::consumer::RecordStream + Send + Sync,
                > = Arc::new(move || source.record_stream());
                RedpandaPartitionStream::new(0, inner_schema, decoder, batch_size, make_stream)
                    .with_offset_tracker(tracker.clone())
                    .with_decode_parallelism(decode_parallelism)
            }
            AwsStreamState::Kinesis { source } => {
                let source = source.clone();
                let make_stream: Arc<
                    dyn Fn() -> tachyon_source::consumer::RecordStream + Send + Sync,
                > = Arc::new(move || source.record_stream());
                RedpandaPartitionStream::new(0, inner_schema, decoder, batch_size, make_stream)
                    .with_decode_parallelism(decode_parallelism)
            }
            AwsStreamState::Sqs { source, receipts, .. } => {
                let source = source.clone();
                let make_stream: Arc<
                    dyn Fn() -> tachyon_source::consumer::RecordStream + Send + Sync,
                > = Arc::new(move || source.record_stream());
                RedpandaPartitionStream::new(0, inner_schema, decoder, batch_size, make_stream)
                    .with_receipts(receipts.clone())
                    .with_decode_parallelism(decode_parallelism)
            }
        };
        let ps = as_partition(inner, &prepared_lookup, name);
        let table = datafusion::catalog::streaming::StreamingTable::try_new(schema, vec![ps])
            .map_err(|e| anyhow::anyhow!("creando StreamingTable: {e}"))?;
        Ok(Arc::new(table.with_infinite_table(true)))
    });
    let inputs = vec![InputSource {
        name: input_def.name.clone(),
        schema: input_schema,
    }];
    let (plan, task_ctx) = plan_query(&planned_sql, &inputs, &factory)
        .await
        .context("planificando la transformación")?;
    ensure_passthrough(&plan).context("la transformación no admite at-least-once")?;
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx)
        .context("ejecutando la transformación")?;
    let out_schema = stream.schema();
    let key = &config.output.key;
    let key_field = out_schema
        .field_with_name(key)
        .map_err(|_| anyhow::anyhow!("la salida no tiene la clave '{key}'"))?;
    match key_field.data_type() {
        arrow::datatypes::DataType::Int64
        | arrow::datatypes::DataType::Int32
        | arrow::datatypes::DataType::Utf8 => {}
        other => anyhow::bail!(
            "la clave '{key}' es {other}; kinesis/sqs la publican como Int64, Int32 o Utf8"
        ),
    }

    let sink = match &output {
        AwsOutput::Kinesis { stream } => {
            let kinesis_cfg = config
                .connectors
                .kinesis
                .as_ref()
                .context("kinesis requiere connectors.kinesis.region")?;
            AwsSink::Kinesis(
                KinesisSink::open(
                    &kinesis_cfg.region,
                    kinesis_cfg.endpoint.as_deref(),
                    kinesis_cfg.profile.as_deref(),
                    stream,
                    key,
                )
                .await?,
            )
        }
        AwsOutput::Sqs { queue } => {
            let sqs_cfg = config
                .connectors
                .sqs
                .as_ref()
                .context("sqs requiere connectors.sqs.region")?;
            AwsSink::Sqs(
                SqsSink::open(
                    &sqs_cfg.region,
                    sqs_cfg.endpoint.as_deref(),
                    sqs_cfg.profile.as_deref(),
                    queue,
                    key,
                )
                .await?,
            )
        }
    };

    let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<(
        Result<arrow::array::RecordBatch, datafusion::error::DataFusionError>,
        AwsProgress,
    )>(1);
    let stream_task = tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            let snap = snap_fn();
            if batch_tx.send((item, snap)).await.is_err() {
                break;
            }
        }
    });
    let _stop_stream = AbortOnDrop(stream_task);

    let mut tick = tokio::time::interval(options.commit_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    let mut topic_offsets: BTreeMap<i32, i64> = BTreeMap::new();
    // Receipts de los batches publicados y todavía no borrados (se borran en
    // el commit, después de que el batch salió).
    let mut pending_receipts: Vec<String> = Vec::new();
    loop {
        tokio::select! {
            batch = batch_rx.recv() => {
                match batch {
                    Some((batch, progress)) => {
                        let batch = batch.context("batch de salida")?;
                        let rows = sink.write(&batch).await.context("publicando el batch")?;
                        match progress {
                            AwsProgress::Offsets(snap) => {
                                for (part, off) in snap {
                                    topic_offsets
                                        .entry(part)
                                        .and_modify(|o| *o = (*o).max(off))
                                        .or_insert(off);
                                }
                            }
                            AwsProgress::Receipts(handles) => {
                                pending_receipts.extend(handles);
                            }
                            AwsProgress::None => {}
                        }
                        metrics.inc_rows_read(rows as u64);
                        metrics.inc_rows_written(rows as u64);
                    }
                    None => break,
                }
            }
            _ = tick.tick() => {
                commit_aws_epoch(&commit, &topic_offsets, &mut pending_receipts, metrics).await;
                metrics.inc_commits();
            }
        }
    }
    Ok(PipelineHandle {
        metrics_addr,
        metrics: metrics.clone(),
    })
}

/// Commit at-least-once: persiste el progreso de entrada después de que el
/// batch de salida se publicó. Offsets al consumer group (topic) y borrado de
/// receipts (SQS); Kinesis no persiste nada. Un fallo es un warn: el registro
/// ya es durable y un reinicio repubica (el consumidor dedup con la columna de
/// secuencia).
async fn commit_aws_epoch(
    commit: &AwsCommit,
    topic_offsets: &BTreeMap<i32, i64>,
    pending_receipts: &mut Vec<String>,
    metrics: &InstanceMetrics,
) {
    match commit {
        AwsCommit::Topic { source } => {
            if !topic_offsets.is_empty() {
                if let Err(e) = source.commit_offsets(topic_offsets).await {
                    tracing::warn!(error = %e, "commit de offsets al consumer group");
                    metrics.inc_errors();
                }
            }
        }
        AwsCommit::Sqs { client, queue } => {
            let handles = std::mem::take(pending_receipts);
            if !handles.is_empty() {
                delete_sqs_messages(client, queue, &handles).await;
            }
        }
        AwsCommit::Kinesis => {}
    }
}

/// `UNION ALL` hacia un topic. Un consumidor por rama: la transacción adjunta
/// el grupo de cada rama que ya publicó. No hay estado abierto, así que el
/// topic alcanza; la fuente de verdad de los offsets es la transacción.
async fn run_union_topic(
    config: &PipelineConfig,
    branches: &[UnionBranch],
    options: &RunOptions,
    topic: &str,
    key: &str,
    input_codecs: &std::collections::HashMap<String, PreparedInput>,
    metrics: &Arc<InstanceMetrics>,
) -> Result<PipelineHandle> {
    if config
        .deployment
        .consumers_per_topic
        .is_some_and(|count| count != 1)
    {
        anyhow::bail!(
            "el sink a un topic usa un consumidor: la transacción commitea los offsets de ese miembro"
        );
    }
    let budget = StatelessBudget::resolve(config).context("presupuesto del pipeline")?;
    tracing::info!(
        cpus = budget.cpus,
        branches = branches.len(),
        topic,
        transactional_id = %options.commit_user,
        "exactly-once activo (UNION ALL a topic); cada rama commitea su consumer group"
    );

    let metrics_addr = if let Some(bind) = options.metrics_bind {
        let server = MetricsServer::new(bind, metrics.clone());
        Some(server.start().await.context("arrancando métricas")?)
    } else {
        None
    };

    let brokers = config.redpanda()?.brokers.join(",");
    ensure_topic_partitions(&brokers, topic, config.deployment.partitions).await?;

    let mut groups: Vec<(String, Arc<RdkafkaSource>)> = Vec::new();
    let mut inputs: Vec<InputSource> = Vec::new();
    let mut progress_sources: Vec<(String, InputProgressSource)> = Vec::new();
    let mut name_to_sources: std::collections::HashMap<
        String,
        (Vec<Arc<RdkafkaSource>>, OffsetTracker, DecodeFormat),
    > = std::collections::HashMap::new();
    let cc = source_client_config(config, &options.group_id, &budget)?;

    for branch in branches {
        let input_def = config
            .inputs
            .iter()
            .find(|input| input.name == branch.source)
            .with_context(|| {
                format!("UNION ALL lee topics y '{}' no es un input", branch.source)
            })?;
        if input_def.paimon_table().is_some() {
            anyhow::bail!("UNION ALL lee topics");
        }
        let prepared = input_codecs
            .get(&input_def.name)
            .cloned()
            .with_context(|| format!("sin schema para el input '{}'", input_def.name))?;
        if !format_matches(input_def.format, &prepared.format) {
            anyhow::bail!(
                "el input '{}' declara format {:?} pero el codec cargado no coincide",
                input_def.name,
                input_def.format
            );
        }
        let input_topic = input_def.kafka_topic()?.to_string();
        let source_group = format!("{}-{}", options.group_id, input_def.name);
        let mut source_cc = cc.clone();
        source_cc.set("group.id", &source_group);
        let source = Arc::new(
            RdkafkaSource::new(&source_cc, &input_topic)
                .with_context(|| format!("creando source para '{}'", input_def.name))?
                .with_max_batch(budget.batch_size),
        );
        let tracker = OffsetTracker::new();
        groups.push((input_topic.clone(), source.clone()));
        progress_sources.push((
            input_def.name.clone(),
            InputProgressSource::Offsets {
                topic: input_topic.clone(),
                tracker: tracker.clone(),
            },
        ));
        name_to_sources.insert(
            input_def.name.clone(),
            (vec![source], tracker, prepared.format),
        );
        inputs.push(InputSource {
            name: input_def.name.clone(),
            schema: prepared.schema.clone(),
        });
    }

    let batch_size = budget.batch_size;
    let decode_parallelism = budget.decode_parallelism;
    let factory: Box<StreamTableFactory> = Box::new(move |name, schema| {
        let (input_sources, tracker, format) = name_to_sources
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("source no encontrado para '{name}'"))?;
        let decoder = Decoder::new(schema.clone(), format);
        let make_stream = Arc::new(move || {
            let merged: tachyon_source::consumer::RecordStream =
                Box::pin(futures::stream::select_all(
                    input_sources.iter().map(|source| source.record_stream()),
                ));
            merged
        });
        let inner = RedpandaPartitionStream::new(0, schema.clone(), decoder, batch_size, make_stream)
            .with_offset_tracker(tracker)
            .with_decode_parallelism(decode_parallelism);
        let table = datafusion::catalog::streaming::StreamingTable::try_new(
            schema,
            vec![Arc::new(inner)],
        )
        .map_err(|err| anyhow::anyhow!("creando StreamingTable: {err}"))?;
        Ok(Arc::new(table.with_infinite_table(true)))
    });

    let sources = assemble_union_sources(branches, &inputs, &progress_sources)?;
    let mut feed = UnionFeed::start(&sources, &factory)
        .await
        .context("armando UNION ALL")?;
    let out_schema = feed.schema();
    let key_field = out_schema
        .field_with_name(key)
        .map_err(|_| anyhow::anyhow!("la salida no tiene la clave '{key}'"))?;
    match key_field.data_type() {
        arrow::datatypes::DataType::Int64
        | arrow::datatypes::DataType::Int32
        | arrow::datatypes::DataType::Utf8 => {}
        other => anyhow::bail!(
            "la clave '{key}' es {other}; el topic la publica como Int64, Int32 o Utf8"
        ),
    }

    let timeout_ms = (options.commit_interval.as_millis() as u64)
        .saturating_mul(4)
        .max(120_000);
    let wire = match config.output.format {
        PayloadFormat::Json => TopicWire::Json,
        PayloadFormat::Avro => {
            let url = config
                .connectors
                .schema_registry
                .as_ref()
                .context("format avro requiere connectors.schema_registry.url")?
                .url
                .clone();
            let avsc = avro_json_from_arrow(out_schema.as_ref())?;
            let schema = parse_avro_schema(&avsc)?;
            let id = register_topic_schema(&url, topic, &avsc)
                .context("registrando el schema de la salida")?;
            tracing::info!(topic, schema_id = id, "schema de salida registrado");
            TopicWire::Avro { schema, id }
        }
    };
    let sink = RedpandaSink::open(&brokers, topic, key, &options.commit_user, timeout_ms).await?;

    let mut tick = tokio::time::interval(options.commit_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    let mut offsets = SourceOffsets::new();
    let mut in_txn = false;
    loop {
        tokio::select! {
            batch = feed.next() => {
                match batch {
                    Some(Ok((batch, progress))) => {
                        let Progress::Input {
                            progress: InputProgress::Offsets(covered),
                            ..
                        } = progress
                        else {
                            unreachable!("UNION ALL a topic solo trae offsets de topic");
                        };
                        if !in_txn {
                            sink.begin().await?;
                            in_txn = true;
                        }
                        let rows = match &wire {
                            TopicWire::Json => sink.write(&batch).await.context("publicando el batch")?,
                            TopicWire::Avro { schema, id } => {
                                let records = encode_envelopes(&batch, key, schema, *id)
                                    .context("codificando Avro")?;
                                sink.write_records(&records)
                                    .await
                                    .context("publicando el batch")?
                            }
                        };
                        merge_offsets(&mut offsets, &covered);
                        metrics.inc_rows_read(rows as u64);
                        metrics.inc_rows_written(rows as u64);
                    }
                    Some(Err(err)) => return Err(err).context("batch de salida"),
                    None => break,
                }
            }
            _ = tick.tick() => {
                if in_txn {
                    commit_topic_groups(&sink, &groups, &offsets).await?;
                    in_txn = false;
                    metrics.inc_commits();
                    tracing::info!(topic, "transacción de topic commiteada");
                }
            }
        }
    }
    if in_txn {
        commit_topic_groups(&sink, &groups, &offsets).await?;
        metrics.inc_commits();
    }
    Ok(PipelineHandle {
        metrics_addr,
        metrics: metrics.clone(),
    })
}

pub(crate) async fn ensure_topic_partitions(brokers: &str, topic: &str, expected: usize) -> Result<()> {
    let probe: rdkafka::consumer::BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("cliente para leer la metadata del topic")?;
    let metadata = probe
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .with_context(|| format!("metadata de '{topic}'"))?;
    let found = metadata
        .topics()
        .iter()
        .find(|item| item.name() == topic)
        .with_context(|| format!("el topic '{topic}' no está en la metadata"))?;
    if let Some(err) = found.error() {
        anyhow::bail!("el topic '{topic}' no se puede leer: {err:?}");
    }
    let partitions = found.partitions().len();
    if partitions != expected {
        anyhow::bail!(
            "el topic '{topic}' tiene {partitions} particiones y deployment.partitions es {expected}"
        );
    }
    Ok(())
}

fn validate_window(
    config: &PipelineConfig,
    shape: &WindowShape,
    schema: &arrow::datatypes::Schema,
    table_fields: &[String],
) -> Result<(i64, i64)> {
    if config.inputs.len() != 1 {
        anyhow::bail!("una query de ventana tiene un solo input");
    }
    let input = &config.inputs[0];
    if !shape.group_columns.iter().any(|col| col == &input.key) {
        anyhow::bail!(
            "GROUP BY de ventana no incluye la clave de particionado '{}'; v1 no tiene agregado parcial",
            input.key
        );
    }
    let watermark = input.watermark.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "el input '{}' alimenta una ventana y no declara watermark (column, lag)",
            input.name
        )
    })?;
    if watermark.column != shape.event_time {
        anyhow::bail!(
            "watermark.column '{}' no es la columna de la ventana '{}'",
            watermark.column,
            shape.event_time
        );
    }
    let lag_ms = parse_fixed_duration(&watermark.lag)
        .map_err(|e| anyhow::anyhow!("watermark.lag: {e}"))?;
    let idle_ms = match &watermark.idle {
        Some(idle) => parse_fixed_duration(idle).map_err(|e| anyhow::anyhow!("watermark.idle: {e}"))?,
        None => lag_ms,
    };
    let time = schema
        .field_with_name(&shape.event_time)
        .map_err(|_| anyhow::anyhow!("falta la columna de tiempo '{}'", shape.event_time))?;
    match time.data_type() {
        arrow::datatypes::DataType::Int64 | arrow::datatypes::DataType::Timestamp(_, _) => {}
        other => anyhow::bail!("event time {other} no es Int64 ni Timestamp"),
    }
    for col in &shape.group_columns {
        let field = schema
            .field_with_name(col)
            .map_err(|_| anyhow::anyhow!("falta la clave '{col}'"))?;
        if field.is_nullable() {
            anyhow::bail!("la clave '{col}' de la ventana no puede ser nullable");
        }
        match field.data_type() {
            arrow::datatypes::DataType::Int64 | arrow::datatypes::DataType::Utf8 => {}
            other => anyhow::bail!("la clave '{col}' es {other}; hace falta Int64 o Utf8"),
        }
    }
    if table_fields.iter().any(|name| name == PARTITION_COLUMN)
        || schema.field_with_name(PARTITION_COLUMN).is_ok()
    {
        anyhow::bail!("'{PARTITION_COLUMN}' está reservado");
    }
    let expected = output_field_names(shape);
    if table_fields != expected.as_slice() {
        anyhow::bail!(
            "la tabla tiene campos {table_fields:?} y la ventana escribe {expected:?}"
        );
    }
    Ok((lag_ms, idle_ms))
}

enum WindowMsg {
    Rows(arrow::array::RecordBatch),
    Barrier(tachyon_core::WindowCheckpointV1),
}

async fn drive_window(
    shape: &WindowShape,
    lag_ms: i64,
    idle_ms: i64,
    user_schema: arrow::datatypes::SchemaRef,
    lanes: Vec<(crate::execute::TransformOutput, OffsetTracker)>,
    sink: PaimonSink,
    trackers: Vec<(String, OffsetTracker)>,
    commit_sources: Vec<(String, Arc<RdkafkaSource>)>,
    metrics: &Arc<InstanceMetrics>,
    commit_interval: std::time::Duration,
    restored: Option<tachyon_core::WindowCheckpointV1>,
    metrics_addr: Option<std::net::SocketAddr>,
    handoff: Option<Arc<WindowHandoff>>,
    n_consumers: usize,
    mut adopt_rx: tokio::sync::mpsc::UnboundedReceiver<StateRequest>,
    ticket_table: PartitionTickets,
) -> Result<PipelineHandle> {
    let spec = spec_from_shape(shape);
    // Clave tipada: una sola columna de grupo Int64 (el caso común) evita el
    // encoding binario y las allocs por fila en el operador.
    let key_kind = if shape.group_columns.len() == 1
        && user_schema
            .field_with_name(&shape.group_columns[0])
            .is_ok_and(|property| property.data_type() == &arrow::datatypes::DataType::Int64)
    {
        KeyKind::I64
    } else {
        KeyKind::Bytes
    };
    // Un shard del operador por consumidor: cada uno es dueño de
    // `partición % shards` y aplica en paralelo (ver `ShardedWindow`).
    let shards = n_consumers.max(1);
    let mut operator = match &restored {
        Some(checkpoint) => {
            let mut op = ShardedWindow::restore(
                spec,
                lag_ms,
                idle_ms,
                checkpoint.state.clone(),
                checkpoint.instance_watermark_ms,
                key_kind,
                shards,
            )
            .map_err(|e| anyhow::anyhow!("restaurando el estado de ventana: {e}"))?;
            let topic = trackers.first().map(|(topic, _)| topic.clone());
            if let Some(topic) = topic {
                if let Some(parts) = checkpoint.progress.get(&topic) {
                    let maxes = parts
                        .iter()
                        .map(|(id, progress)| (*id, progress.max_event_time_ms))
                        .collect();
                    op.activate_restored(&maxes, Instant::now());
                }
            }
            op
        }
        None => ShardedWindow::new(spec, lag_ms, idle_ms, key_kind, shards),
    };
    let mut applied = restored
        .as_ref()
        .map(|checkpoint| checkpoint.applied.clone())
        .unwrap_or_default();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<WindowMsg>(8);
    let (mut sink_writer, mut sink_committer) = sink.split();
    let (epoch_tx, mut epoch_rx) = tokio::sync::mpsc::channel::<CheckpointEpoch>(1);
    let commit_metrics = metrics.clone();
    let commit_handoff = handoff.clone();
    let commit_task = tokio::spawn(async move {
        while let Some(epoch) = epoch_rx.recv().await {
            let kafka_offsets = epoch.body.applied_offsets();
            let result = sink_committer
                .commit_epoch(epoch.writer, epoch.identifier, &epoch.body)
                .await;
            let Some(identifier) = result.context("checkpoint de ventana")? else {
                continue;
            };
            if let Some(handoff) = &commit_handoff {
                handoff.mark_snapshot_committed();
            }
            commit_metrics.inc_commits();
            tracing::info!(identifier, "checkpoint de ventana commiteado");
            for (topic, source) in &commit_sources {
                if let Some(partitions) = kafka_offsets.get(topic) {
                    if let Err(e) = source.commit_offsets(partitions).await {
                        tracing::warn!(error = %e, topic, "commit de offsets en Kafka");
                    }
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let writer_metrics = metrics.clone();
    let writer = tokio::spawn(async move {
        let mut dirty = false;
        while let Some(msg) = rx.recv().await {
            match msg {
                WindowMsg::Rows(batch) => {
                    let t_write = Instant::now();
                    sink_writer.write(&batch).await.context("escribiendo ventana")?;
                    writer_metrics.add_write_ns(t_write.elapsed().as_nanos() as u64);
                    writer_metrics.inc_rows_written(batch.num_rows() as u64);
                    dirty = true;
                }
                WindowMsg::Barrier(checkpoint) => {
                    if !dirty {
                        continue;
                    }
                    let (identifier, full_writer) =
                        sink_writer.rotate().await.context("rotando el writer")?;
                    if epoch_tx
                        .send(CheckpointEpoch {
                            writer: full_writer,
                            identifier,
                            body: CheckpointBody::Window(checkpoint),
                            receipts: BTreeMap::new(),
                        })
                        .await
                        .is_err()
                    {
                        anyhow::bail!("el task de commit terminó inesperadamente");
                    }
                    dirty = false;
                }
            }
        }
        drop(epoch_tx);
        commit_task.await.context("task de commit")??;
        Ok::<(), anyhow::Error>(())
    });

    // El futuro de `stream.next()` vive en otro task. El select de acá
    // espera un canal, así que un tick no cancela el batch de DataFusion.
    let (batch_in_tx, mut batch_in_rx) = tokio::sync::mpsc::channel(n_consumers.max(1));
    // Si el pipeline se aborta, esta tarea tiene que morir: si no, el stream
    // no se suelta, el poll no ve el canal cerrado y el consumidor se queda
    // en el grupo hasta el session timeout.
    //
    // El paso de Arrow a filas del operador también corre acá: es trabajo
    // por fila y así no se suma al hilo del operador (pipeline de 2 etapas).
    //
    // Un task por carril (o uno solo). Cada uno pasa Arrow a filas del
    // operador y toma el snapshot de su tracker al mandar el batch: lo que
    // el carril emitió hasta ahí es justo lo que ese batch cubre. Antes el
    // snapshot se tomaba en el loop después de aplicar, y el task del stream
    // ya podía haber sacado el batch siguiente: el checkpoint declaraba
    // aplicado un batch que todavía no estaba en el estado.
    let mut stream_tasks = Vec::new();
    for (mut stream, tracker) in lanes {
        let convert_shape = shape.clone();
        let batch_in_tx = batch_in_tx.clone();
        let stream_task = tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                let item = item.map_err(anyhow::Error::from).and_then(|record| {
                    let rows = inputs_from_batch(&record, &convert_shape, key_kind)
                        .context("leyendo el batch de ventana")?;
                    Ok((record.num_rows(), rows, tracker.snapshot()))
                });
                if batch_in_tx.send(item).await.is_err() {
                    break;
                }
            }
        });
        stream_tasks.push(AbortOnDrop(stream_task));
    }
    drop(batch_in_tx);
    let _stop_streams = stream_tasks;
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    let mut last_commit = Instant::now();
    loop {
        enum Wake {
            Batch((usize, Vec<crate::window::WindowInput>, BTreeMap<i32, i64>)),
            Tick,
            Adopt(StateRequest),
            End,
        }
        release_lost_partitions(&handoff, &mut operator, &mut applied);
        let wake = tokio::select! {
            req = adopt_rx.recv() => match req {
                Some(req) => Wake::Adopt(req),
                None => Wake::End,
            },
            result = batch_in_rx.recv() => match result {
                Some(batch) => Wake::Batch(batch.context("batch de la ventana")?),
                None => Wake::End,
            },
            _ = tick.tick() => Wake::Tick,
        };
        match wake {
            Wake::End => break,
            Wake::Adopt(req) => {
                let topic = handoff
                    .as_ref()
                    .map(|gate| gate.topic().to_string())
                    .unwrap_or_default();
                let reply = match ticket_table.read(&topic, req.partition).await
                {
                    Ok(Some(ticket)) => {
                        if !same_window(operator.spec(), &ticket.spec) {
                            Err(format!(
                                "el estado de la partición {} no corresponde a esta ventana",
                                req.partition
                            ))
                        } else {
                            let offset = ticket.applied_offset;
                            match operator.adopt_partition(
                                ticket.partition,
                                ticket.keys,
                                ticket.max_event_time_ms,
                                Instant::now(),
                            ) {
                                Ok(closed) => {
                                    applied
                                        .entry(topic)
                                        .or_default()
                                        .insert(req.partition, offset);
                                    if let Some(gate) = &handoff {
                                        gate.note_adopted(req.partition, offset);
                                    }
                                    if !closed.is_empty() {
                                        let out =
                                            batch_from_closed(&closed, shape, user_schema.as_ref())
                                                .context("armando la salida adoptada")?;
                                        tx.send(WindowMsg::Rows(out))
                                            .await
                                            .context("enviando filas adoptadas")?;
                                    }
                                    tracing::info!(
                                        partition = req.partition,
                                        offset,
                                        "partición adoptada desde la ficha"
                                    );
                                    Ok(Some(offset))
                                }
                                Err(e) => Err(format!(
                                    "la ficha de la partición {} no se pudo adoptar: {e}",
                                    req.partition
                                )),
                            }
                        }
                    }
                    Ok(None) => Ok(None),
                    Err(e) => Err(e.to_string()),
                };
                let _ = req.ack.send(reply);
            }
            Wake::Batch((record_rows, rows, snap)) => {
                release_lost_partitions(&handoff, &mut operator, &mut applied);
                if let Some(gate) = &handoff {
                    operator.set_backlog(gate.backlog());
                }
                let closed = operator
                    .apply(rows, Instant::now())
                    .map_err(|e| anyhow::anyhow!(e))?;
                if let Some((topic, _)) = trackers.first() {
                    let entry = applied.entry(topic.clone()).or_default();
                    for (partition, next) in &snap {
                        if operator.is_released(*partition) {
                            continue;
                        }
                        let slot = entry.entry(*partition).or_insert(*next);
                        *slot = (*slot).max(*next);
                    }
                    if let Some(handoff) = &handoff {
                        if handoff.topic() == topic {
                            let published: BTreeMap<i32, i64> = snap
                                .iter()
                                .filter(|(partition, _)| !operator.is_released(**partition))
                                .map(|(partition, next)| (*partition, *next))
                                .collect();
                            handoff.publish_applied(&published);
                        }
                    }
                }
                metrics.inc_rows_read(record_rows as u64);
                if !closed.is_empty() {
                    let out = batch_from_closed(&closed, shape, user_schema.as_ref())
                        .context("armando la salida de la ventana")?;
                    tx.send(WindowMsg::Rows(out))
                        .await
                        .context("enviando filas de ventana")?;
                }
            }
            Wake::Tick => {
                release_lost_partitions(&handoff, &mut operator, &mut applied);
                if let Some(gate) = &handoff {
                    operator.set_backlog(gate.backlog());
                }
                let closed = operator.on_tick(Instant::now());
                if !closed.is_empty() {
                    let out = batch_from_closed(&closed, shape, user_schema.as_ref())?;
                    tx.send(WindowMsg::Rows(out)).await.context("enviando filas de ventana")?;
                }
            }
        }
        if last_commit.elapsed() >= commit_interval {
            let mut progress = BTreeMap::new();
            if let Some((topic, _)) = trackers.first() {
                progress.insert(
                    topic.clone(),
                    operator
                        .partition_progress()
                        .into_iter()
                        .map(|(id, max)| {
                            (
                                id,
                                tachyon_core::PartitionProgress {
                                    max_event_time_ms: max,
                                },
                            )
                        })
                        .collect(),
                );
            }
            if let Some(handoff) = &handoff {
                // El seed del low watermark vive en el gate. El primer
                // sidecar nombra la partición aunque ese lote no haya emitido.
                let seeded = handoff.applied_map();
                if let Some((topic, _)) = trackers.first() {
                    let entry = applied.entry(topic.clone()).or_default();
                    for (partition, next) in seeded {
                        let slot = entry.entry(partition).or_insert(next);
                        *slot = (*slot).max(next);
                    }
                }
            }
            tx.send(WindowMsg::Barrier(tachyon_core::WindowCheckpointV1 {
                v: 1,
                commit_identifier: 0,
                consumers_per_topic: n_consumers as u32,
                applied: applied.clone(),
                progress,
                instance_watermark_ms: operator.instance_watermark_ms(),
                spec: spec_from_shape(shape),
                state: operator.freeze_state(),
            }))
            .await
            .context("enviando la barrera de ventana")?;
            last_commit = Instant::now();
        }
    }
    drop(tx);
    writer.await.context("task de escritura de ventana")??;
    Ok(PipelineHandle {
        metrics_addr,
        metrics: metrics.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::{commit_user_from, ensure_window_consumers, window_session_timeout_ms};
    use std::time::Duration;

    #[test]
    fn commit_user_is_stable_and_sanitized() {
        assert_eq!(
            commit_user_from("tachyon-orders", "pod-a"),
            "tachyon-orders-pod-a"
        );
        assert_eq!(
            commit_user_from("tachyon-orders", "pod/a b"),
            "tachyon-orders-pod-a-b"
        );
        let long = "x".repeat(200);
        assert!(commit_user_from("g", &long).len() <= 128);
    }

    #[test]
    fn a_different_consumer_count_does_not_start() {
        assert!(ensure_window_consumers(1, 1).is_ok());
        let err = ensure_window_consumers(1, 2).unwrap_err().to_string();
        assert!(err.contains("cambió (1 → 2)"), "{err}");
    }

    #[test]
    fn window_session_timeout_covers_the_commit() {
        assert_eq!(window_session_timeout_ms(Duration::from_secs(2)), 45_000);
        assert_eq!(window_session_timeout_ms(Duration::from_secs(20)), 50_000);
    }
}
