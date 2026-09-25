//! Loop de ejecución end-to-end (Slice 5).
//!
//! Orquesta el pipeline completo:
//! 1. Por cada input de la config: `RdkafkaSource` → `Decoder` →
//!    `RedpandaPartitionStream` → `StreamingTable` (factory de producción).
//! 2. `execute_query(select_sql, inputs, factory)` → stream de salida.
//! 3. Consume el stream y, por cada `RecordBatch`, `PaimonSink::write()`.
//! 4. Período de commit (configurable): `sink.commit()` + commit de offsets
//!    de Redpanda (at-least-once + idempotencia por sequence en el MVP).
//! 5. Métricas: rows leídas/escritas, commits, expuestas por HTTP.
//!
//! El commit de offsets es at-least-once en el MVP: si el proceso muere entre
//! el commit de Paimon y el commit de offsets, los eventos se re-procesan a la
//! reconexión; la idempotencia la da el sequence number en Paimon (dedup por
//! clave). En la Fase 1 el commit de offsets es atómico con el commit de
//! Paimon (ver DESIGN.md §9.2.4).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use rdkafka::ClientConfig;
use tachyon_config::PipelineConfig;
use tachyon_metrics::{InstanceMetrics, MetricsServer};
use tachyon_sink::writer::PaimonSink;
use tachyon_source::consumer::RdkafkaSource;
use tachyon_source::decode::{DecodeFormat, Decoder};
use tachyon_source::stream::RedpandaPartitionStream;

use crate::execute::{execute_query, InputSource, StreamTableFactory};

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
}

impl RunOptions {
    pub fn from_config(config: &PipelineConfig) -> Self {
        let commit_interval = parse_duration(&config.deployment.commit_interval);
        let metrics_bind = config
            .deployment
            .metrics
            .as_ref()
            .map(|m| m.bind_addr.parse().expect("bind_addr inválido"));
        RunOptions {
            commit_interval,
            metrics_bind,
            group_id: format!("tachyon-{}", config.pipeline.name),
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

/// Construye el `ClientConfig` rdkafka para un input, a partir de la config
/// del pipeline.
fn source_client_config(config: &PipelineConfig, group_id: &str) -> ClientConfig {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", config.connectors.redpanda.brokers.join(","));
    cc.set("group.id", group_id);
    cc.set("auto.offset.reset", "earliest");
    cc.set("enable.auto.commit", "false");
    cc.set("session.timeout.ms", "10000");
    // Tuning de fetch: el broker acumula hasta `fetch.min.bytes` antes de
    // responder (con el default de 1 byte cada round trip trae poquísimas
    // filas y el throughput queda acotado por la latencia de red).
    cc.set("fetch.min.bytes", "524288");
    cc.set("fetch.wait.max.ms", "1000");
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

/// Número de consumidores paralelos por topic (instancias librdkafka en el
/// mismo consumer group). Un solo cliente se satura en ~200K rows/s por
/// serialización de round trips de fetch; el protocolo de grupo reparte las
/// particiones entre los N clientes y cada uno hace fetch en paralelo.
/// Default opinionated: `min(partitions, 4)` (óptimo observado en benchmarks;
/// más consumidores que particiones no tiene sentido porque una partición solo
/// puede estar asignada a un consumidor).
fn consumers_per_topic(config: &PipelineConfig) -> usize {
    config
        .deployment
        .consumers_per_topic
        .unwrap_or_else(|| config.deployment.partitions.min(4).max(1))
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
    // Schemas de los inputs (nombre lógico -> schema Arrow). En producción
    // vienen del schema Avro/JSON del topic; en tests se proporcionan.
    input_schemas: &std::collections::HashMap<String, SchemaRef>,
    // Métricas de la instancia (pre-creadas por el caller).
    metrics: &Arc<InstanceMetrics>,
) -> Result<PipelineHandle> {
    // --- Métricas (endpoint HTTP) ---
    let metrics_addr = if let Some(bind) = options.metrics_bind {
        let server = MetricsServer::new(bind, metrics.clone());
        Some(server.start().await.context("arrancando métricas")?)
    } else {
        None
    };

    // --- Fuentes Redpanda: N consumidores por input (mismo consumer group) ---
    // Cada input tiene su propio consumer group (independiente por topic) para
    // que el rebalanceo de uno no afecte a los demás. Dentro de cada grupo, N
    // instancias librdkafka reparten las particiones y hacen fetch en paralelo
    // (un solo cliente se satura en ~200K rows/s; ver consumers_per_topic).
    let mut sources: Vec<Arc<RdkafkaSource>> = Vec::new();
    let mut inputs: Vec<InputSource> = Vec::new();
    let mut name_to_sources: std::collections::HashMap<String, Vec<Arc<RdkafkaSource>>> =
        std::collections::HashMap::new();
    let cc = source_client_config(config, &options.group_id);
    let n_consumers = consumers_per_topic(config);

    for input_def in &config.inputs {
        let schema = input_schemas
            .get(&input_def.name)
            .cloned()
            .with_context(|| format!("sin schema para el input '{}'", input_def.name))?;

        let source_group = format!("{}-{}", options.group_id, input_def.name);
        let mut source_cc = cc.clone();
        source_cc.set("group.id", &source_group);
        let input_sources: Vec<Arc<RdkafkaSource>> = (0..n_consumers)
            .map(|_| -> Result<Arc<RdkafkaSource>> {
                Ok(Arc::new(
                    RdkafkaSource::new(&source_cc, &input_def.topic)
                        .with_context(|| format!("creando source para '{}'", input_def.name))?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for s in &input_sources {
            sources.push(s.clone());
        }
        name_to_sources.insert(input_def.name.clone(), input_sources);
        inputs.push(InputSource {
            name: input_def.name.clone(),
            schema: schema.clone(),
        });
    }

    // --- Factory de producción: cablea cada input a su StreamingTable ---
    // Mapa nombre lógico -> sources (N consumidores por input, mismo grupo).
    let batch_size: usize = 100;
    let factory: Box<StreamTableFactory> = Box::new(move |name, schema| {
        let input_sources = name_to_sources
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("source no encontrado para '{name}'"))?;
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
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
        let ps = Arc::new(RedpandaPartitionStream::new(
            0,
            schema.clone(),
            decoder.clone(),
            batch_size,
            make_stream,
        ));
        let table = datafusion::catalog::streaming::StreamingTable::try_new(
            schema,
            vec![ps],
        )
        .map_err(|e| anyhow::anyhow!("creando StreamingTable: {e}"))?;
        Ok(Arc::new(table.with_infinite_table(true)))
    });

    // --- Ejecuta la transformación ---
    let mut stream = execute_query(select_sql, &inputs, &factory)
        .await
        .context("ejecutando la transformación")?;

    // --- Writer task: decoupla el write+commit de Paimon del consumo ---
    // El commit de Paimon (LSM flush a disco) bloquea. Si se hiciera en el loop
    // de consumo, detendría `stream.next()` y backpresionaría toda la cadena
    // (mpsc lleno -> poll task bloqueado en `blocking_send` -> el comando de
    // commit de offsets no se drena -> timeout). Con un writer task dedicado,
    // el loop de consumo sigue tirando de DataFusion (y por tanto del source y
    // del poll task) mientras el writer escribe y commita en background. El
    // canal acotado da backpressure limitada: el loop solo se frena si el
    // writer no da abasto (el canal se llena).
    let commit_interval = options.commit_interval;
    let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel(512);

    let writer_metrics = metrics.clone();
    let writer: tokio::task::JoinHandle<Result<(), anyhow::Error>> = tokio::spawn(async move {
        let mut sink = sink;
        let mut commit_timer = tokio::time::interval(commit_interval);
        commit_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        commit_timer.tick().await; // primera tick inmediata
        let mut dirty = false;
        loop {
            tokio::select! {
                maybe_batch = batch_rx.recv() => {
                    match maybe_batch {
                        Some(batch) => {
                            sink.write(&batch)
                                .await
                                .context("escribiendo al sink Paimon")?;
                            writer_metrics.inc_rows_written(batch.num_rows() as u64);
                            dirty = true;
                        }
                        None => break, // canal cerrado (fin del stream)
                    }
                }
                _ = commit_timer.tick() => {
                    if dirty {
                        sink.commit().await.context("commit del sink")?;
                        writer_metrics.inc_commits();
                        // Commit de offsets (at-least-once).
                        for source in &sources {
                            if let Err(e) = source.commit().await {
                                tracing::warn!(error = %e, "commit de offsets");
                                writer_metrics.inc_errors();
                            }
                        }
                        dirty = false;
                    }
                }
            }
        }
        // Commit final (fin del stream).
        sink.commit().await.context("commit final del sink")?;
        writer_metrics.inc_commits();
        for source in &sources {
            let _ = source.commit().await;
        }
        Ok(())
    });

    // --- Loop de consumo: tira de DataFusion y envía batches al writer ---
    loop {
        match stream.next().await {
            Some(Ok(batch)) => {
                let rows = batch.num_rows() as u64;
                tracing::debug!(rows, "batch de salida desde DataFusion");
                metrics.inc_rows_read(rows);
                // Backpressure limitada: bloquea solo si el writer no da abasto.
                batch_tx.send(batch).await.context("enviando batch al writer")?;
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
