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
//! - Al arrancar, `PaimonSink::recover` devuelve los offsets del último
//!   checkpoint y cada fuente se re-posiciona ahí (skip de lo ya escrito +
//!   seek si el consumer group quedó adelante).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use rdkafka::ClientConfig;
use tachyon_config::{parse_fixed_duration, PayloadFormat, PipelineConfig};
use tachyon_core::{CheckpointBody, SourceOffsets};
use tachyon_metrics::{InstanceMetrics, MetricsServer};
use tachyon_sink::writer::{CheckpointEpoch, PaimonSink, Recovered};
use tachyon_source::consumer::RdkafkaSource;
use tachyon_source::WindowHandoff;
use tachyon_source::decode::{parse_avro_schema, DecodeFormat, Decoder};
use tachyon_source::stream::{OffsetTracker, RedpandaPartitionStream};
use tachyon_sql::WindowShape;

use crate::budget::StatelessBudget;
use crate::execute::{ensure_passthrough, plan_query, InputSource, StreamTableFactory};
use crate::window::{
    batch_from_closed, inputs_from_batch, output_field_names, schema_with_partition,
    spec_from_shape, user_schema_without_partition, WindowOperator, PARTITION_COLUMN,
};

/// Fusiona `update` en `offsets` (máximo por partición: el progreso nunca
/// retrocede).
fn merge_offsets(offsets: &mut SourceOffsets, update: &SourceOffsets) {
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
fn source_client_config(
    config: &PipelineConfig,
    group_id: &str,
    budget: &StatelessBudget,
) -> ClientConfig {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", config.connectors.redpanda.brokers.join(","));
    cc.set("group.id", group_id);
    cc.set("auto.offset.reset", "earliest");
    cc.set("enable.auto.commit", "false");
    cc.set("session.timeout.ms", "10000");
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
    cc.set("queued.max.messages.kbytes", "262144");
    if let Some(sec) = &config.connectors.redpanda.security {
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
    cc
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
}

fn format_matches(declared: PayloadFormat, format: &DecodeFormat) -> bool {
    matches!(
        (declared, format),
        (PayloadFormat::Json, DecodeFormat::Json) | (PayloadFormat::Avro, DecodeFormat::Avro(_))
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
    let (resume, restored_window) = match (&recovered, window) {
        (Recovered::None, _) => (None, None),
        (Recovered::PassThrough { offsets, .. }, None) => (Some(offsets.clone()), None),
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
            (Some(checkpoint.applied.clone()), Some(checkpoint.clone()))
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
    let mut name_to_sources: std::collections::HashMap<
        String,
        (Vec<Arc<RdkafkaSource>>, OffsetTracker, DecodeFormat),
    > = std::collections::HashMap::new();
    let mut window_handoff: Option<Arc<WindowHandoff>> = None;
    let cc = source_client_config(config, &options.group_id, &budget);
    // Ventana y pass-through usan el mismo presupuesto: min(particiones, CPUs)
    // salvo override. El handoff evita aplicar dos veces una partición que
    // cambia de consumidor dentro del proceso.
    let n_consumers = budget.consumers_per_topic;
    if let Some(checkpoint) = &restored_window {
        ensure_window_consumers(checkpoint.consumers_per_topic, n_consumers)?;
    }
    let instance = instance_name();
    let session_timeout_ms = window_session_timeout_ms(options.commit_interval);
    let planned_sql = match window {
        Some(shape) => shape.rewrite_sql(),
        None => select_sql.to_string(),
    };

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
        let schema = if window.is_some() {
            if prepared.schema.field_with_name(PARTITION_COLUMN).is_ok() {
                anyhow::bail!("'{PARTITION_COLUMN}' está reservado");
            }
            schema_with_partition(prepared.schema.clone())
        } else {
            prepared.schema.clone()
        };

        let source_group = format!("{}-{}", options.group_id, input_def.name);
        let mut source_cc = cc.clone();
        source_cc.set("group.id", &source_group);
        let handoff = if window.is_some() {
            source_cc.set("session.timeout.ms", session_timeout_ms.to_string());
            let topic_applied = restored_window
                .as_ref()
                .and_then(|checkpoint| checkpoint.applied.get(&input_def.topic))
                .cloned()
                .unwrap_or_default();
            let gate = match &restored_window {
                Some(checkpoint) => Arc::new(WindowHandoff::restored(
                    &source_group,
                    &input_def.topic,
                    &options.commit_user,
                    checkpoint.commit_identifier,
                    topic_applied,
                )),
                None => Arc::new(WindowHandoff::starting(
                    &source_group,
                    &input_def.topic,
                    &options.commit_user,
                )),
            };
            window_handoff = Some(gate.clone());
            let ids: Vec<String> = (0..n_consumers)
                .map(|index| format!("{instance}-{index}"))
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
        // Cada consumidor recibe el mapa completo del checkpoint: el filtro de
        // resume ignora las particiones que no le asigna el grupo.
        let topic_resume = resume
            .as_ref()
            .and_then(|r| r.get(&input_def.topic))
            .cloned()
            .unwrap_or_default();
        let input_sources: Vec<Arc<RdkafkaSource>> = (0..n_consumers)
            .map(|index| -> Result<Arc<RdkafkaSource>> {
                let mut consumer_cc = source_cc.clone();
                if window.is_some() {
                    // KIP-345: un id distinto por miembro. No va en el config
                    // compartido, porque el segundo join con el mismo id echa
                    // al primero.
                    consumer_cc.set("group.instance.id", format!("{instance}-{index}"));
                }
                let mut source = RdkafkaSource::new(&consumer_cc, &input_def.topic)
                    .with_context(|| format!("creando source para '{}'", input_def.name))?
                    .with_max_batch(budget.batch_size)
                    .with_resume_offsets(topic_resume.clone());
                if let Some(gate) = &handoff {
                    source = source.with_window_handoff(gate.clone());
                }
                Ok(Arc::new(source))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let tracker = OffsetTracker::new();
        commit_sources.push((input_def.topic.clone(), input_sources[0].clone()));
        trackers.push((input_def.topic.clone(), tracker.clone()));
        name_to_sources.insert(
            input_def.name.clone(),
            (input_sources, tracker, prepared.format),
        );
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
    let factory: Box<StreamTableFactory> = Box::new(move |name, schema| {
        let (input_sources, tracker, format) = name_to_sources
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("source no encontrado para '{name}'"))?;
        let (decode_schema, row_partitions) = if schema
            .fields()
            .last()
            .is_some_and(|field| field.name() == PARTITION_COLUMN)
        {
            (user_schema_without_partition(&schema), true)
        } else {
            (schema.clone(), false)
        };
        let decoder = Decoder::new(decode_schema, format);
        // Un `record_stream()` por consumidor, fusionados con `select_all`:
        // el grupo reparte las particiones entre ellos y cada instancia
        // drena su propio buffer de fetch en paralelo. La factory se invoca
        // una vez por ejecución, así que cada consumidor se `take()`a una vez.
        let make_stream = Arc::new(move || {
            let streams: Vec<tachyon_source::consumer::RecordStream> = input_sources
                .iter()
                .map(|s| s.record_stream())
                .collect();
            // Coerción explícita a `RecordStream` (el `SelectAll` concreto no
            // se coercea solo a `Pin<Box<dyn Stream>>` en posición de retorno).
            let merged: tachyon_source::consumer::RecordStream =
                Box::pin(futures::stream::select_all(streams));
            merged
        });
        let ps = Arc::new(
            RedpandaPartitionStream::new(0, schema.clone(), decoder.clone(), batch_size, make_stream)
                .with_offset_tracker(tracker)
                .with_decode_parallelism(decode_parallelism)
                .with_row_partitions(row_partitions),
        );
        let table = datafusion::catalog::streaming::StreamingTable::try_new(
            schema,
            vec![ps],
        )
        .map_err(|e| anyhow::anyhow!("creando StreamingTable: {e}"))?;
        Ok(Arc::new(table.with_infinite_table(true)))
    });

    // --- Ejecuta la transformación (validada como pass-through) ---
    let (plan, task_ctx) = plan_query(&planned_sql, &inputs, &factory)
        .await
        .context("planificando la transformación")?;
    ensure_passthrough(&plan).context("la transformación no admite exactly-once")?;
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx)
        .context("ejecutando la transformación")?;

    if let Some(shape) = window {
        let user_schema = input_codecs
            .get(&shape.source)
            .with_context(|| format!("sin schema para '{}'", shape.source))?
            .schema
            .clone();
        let (lag_ms, idle_ms) = validate_window(config, shape, user_schema.as_ref(), &sink.field_names())?;
        return drive_window(
            shape,
            lag_ms,
            idle_ms,
            user_schema,
            stream,
            sink,
            trackers,
            commit_sources,
            metrics,
            options.commit_interval,
            restored_window,
            metrics_addr,
            window_handoff,
            n_consumers,
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
        tokio::sync::mpsc::channel::<(arrow::array::RecordBatch, SourceOffsets)>(channel_batches);

    let (mut sink_writer, mut sink_committer) = sink.split();
    let (epoch_tx, mut epoch_rx) = tokio::sync::mpsc::channel::<CheckpointEpoch>(1);
    let commit_metrics = metrics.clone();
    let commit_task: tokio::task::JoinHandle<Result<(), anyhow::Error>> =
        tokio::spawn(async move {
            // Commit task: procesa los epochs EN ORDEN (FIFO del canal).
            while let Some(epoch) = epoch_rx.recv().await {
                let t_commit = Instant::now();
                let kafka_offsets = epoch.body.applied_offsets().clone();
                let result = sink_committer
                    .commit_epoch(epoch.writer, epoch.identifier, &epoch.body)
                    .await;
                commit_metrics.add_commit_ns(t_commit.elapsed().as_nanos() as u64);
                let Some(identifier) = result.context("checkpoint del sink")? else {
                    continue; // epoch sin archivos nuevos
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
            }
            Ok(())
        });

    let writer_metrics = metrics.clone();
    let writer: tokio::task::JoinHandle<Result<(), anyhow::Error>> = tokio::spawn(async move {
        // Offsets del próximo checkpoint. Arranca en el checkpoint recuperado
        // para arrastrar las particiones que no avanzan en esta ejecución.
        let mut offsets: SourceOffsets = resume.unwrap_or_default();
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
                            merge_offsets(&mut offsets, &batch_offsets);
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
                            .context("rotando el writer del sink")?;
                        let epoch = CheckpointEpoch {
                            writer: full_writer,
                            identifier,
                            body: CheckpointBody::Offsets(offsets.clone()),
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
                .context("rotando el writer del sink")?;
            let _ = epoch_tx
                .send(CheckpointEpoch {
                    writer: full_writer,
                    identifier,
                    body: CheckpointBody::Offsets(offsets.clone()),
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

    // --- Loop de consumo: tira de DataFusion y envía batches al writer ---
    loop {
        let t_next = Instant::now();
        let next = stream.next().await;
        metrics.add_source_next_ns(t_next.elapsed().as_nanos() as u64);
        match next {
            Some(Ok(batch)) => {
                let rows = batch.num_rows() as u64;
                tracing::debug!(rows, "batch de salida desde DataFusion");
                metrics.inc_rows_read(rows);
                // Offsets que cubre este batch: con un plan pass-through, todo
                // lo que la fuente emitió hasta ahora ya produjo su salida.
                let batch_offsets: SourceOffsets = trackers
                    .iter()
                    .map(|(topic, t)| (topic.clone(), t.snapshot()))
                    .collect();
                // Backpressure limitada: bloquea solo si el writer no da abasto.
                let t_send = Instant::now();
                batch_tx
                    .send((batch, batch_offsets))
                    .await
                    .context("enviando batch al writer")?;
                metrics.add_send_wait_ns(t_send.elapsed().as_nanos() as u64);
            }
            Some(Err(e)) => return Err(e).context("batch de salida"),
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
    mut stream: crate::execute::TransformOutput,
    sink: PaimonSink,
    trackers: Vec<(String, OffsetTracker)>,
    commit_sources: Vec<(String, Arc<RdkafkaSource>)>,
    metrics: &Arc<InstanceMetrics>,
    commit_interval: std::time::Duration,
    restored: Option<tachyon_core::WindowCheckpointV1>,
    metrics_addr: Option<std::net::SocketAddr>,
    handoff: Option<Arc<WindowHandoff>>,
    n_consumers: usize,
) -> Result<PipelineHandle> {
    let spec = spec_from_shape(shape);
    let mut operator = match &restored {
        Some(checkpoint) => {
            let mut op = WindowOperator::restore(
                spec,
                lag_ms,
                idle_ms,
                checkpoint.state.clone(),
                checkpoint.instance_watermark_ms,
            );
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
        None => WindowOperator::new(spec, lag_ms, idle_ms),
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
            let kafka_offsets = epoch.body.applied_offsets().clone();
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
                    let (identifier, full_writer) = sink_writer.rotate().context("rotando el writer")?;
                    if epoch_tx
                        .send(CheckpointEpoch {
                            writer: full_writer,
                            identifier,
                            body: CheckpointBody::Window(checkpoint),
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
    let (batch_in_tx, mut batch_in_rx) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            if batch_in_tx.send(item).await.is_err() {
                break;
            }
        }
    });
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    let mut last_commit = Instant::now();
    loop {
        enum Wake {
            Batch(arrow::array::RecordBatch),
            Tick,
            End,
        }
        let wake = tokio::select! {
            result = batch_in_rx.recv() => match result {
                Some(batch) => Wake::Batch(batch.context("batch de la ventana")?),
                None => Wake::End,
            },
            _ = tick.tick() => Wake::Tick,
        };
        match wake {
            Wake::End => break,
            Wake::Batch(record) => {
                let rows = inputs_from_batch(&record, shape).context("leyendo el batch de ventana")?;
                let closed = operator
                    .apply(&rows, Instant::now())
                    .map_err(|e| anyhow::anyhow!(e))?;
                for (topic, tracker) in &trackers {
                    let snap = tracker.snapshot();
                    let entry = applied.entry(topic.clone()).or_default();
                    for (partition, next) in &snap {
                        let slot = entry.entry(*partition).or_insert(*next);
                        *slot = (*slot).max(*next);
                    }
                    if let Some(handoff) = &handoff {
                        if handoff.topic() == topic {
                            handoff.publish_applied(&snap);
                        }
                    }
                }
                metrics.inc_rows_read(record.num_rows() as u64);
                if !closed.is_empty() {
                    let out = batch_from_closed(&closed, shape, user_schema.as_ref())
                        .context("armando la salida de la ventana")?;
                    tx.send(WindowMsg::Rows(out))
                        .await
                        .context("enviando filas de ventana")?;
                }
            }
            Wake::Tick => {
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
