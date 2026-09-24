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

/// Corre el pipeline end-to-end.
///
/// No retorna mientras el pipeline esté activo (el stream de salida es
/// infinito). En tests se lanza en una tarea y se cancela; en producción
/// corre hasta recibir una señal de shutdown.
pub async fn run_pipeline(
    config: &PipelineConfig,
    select_sql: &str,
    options: &RunOptions,
    // La tabla Paimon ya abierta (para no depender del catalog en el loop).
    sink: &mut PaimonSink,
    // Schemas de los inputs (nombre lógico -> schema Arrow). En producción
    // vienen del schema Avro/JSON del topic; en tests se proporcionan.
    input_schemas: &std::collections::HashMap<String, SchemaRef>,
) -> Result<PipelineHandle> {
    let metrics = Arc::new(InstanceMetrics::new());

    // --- Métricas (endpoint HTTP) ---
    let metrics_addr = if let Some(bind) = options.metrics_bind {
        let server = MetricsServer::new(bind, metrics.clone());
        Some(server.start().await.context("arrancando métricas")?)
    } else {
        None
    };

    // --- Fuentes Redpanda: una por input ---
    // Cada input tiene su propio consumer group (independiente por topic) para
    // que el rebalanceo de uno no afecte a los demás.
    let mut sources: Vec<Arc<RdkafkaSource>> = Vec::new();
    let mut inputs: Vec<InputSource> = Vec::new();
    let cc = source_client_config(config, &options.group_id);

    for input_def in &config.inputs {
        let schema = input_schemas
            .get(&input_def.name)
            .cloned()
            .with_context(|| format!("sin schema para el input '{}'", input_def.name))?;

        let source_group = format!("{}-{}", options.group_id, input_def.name);
        let mut source_cc = cc.clone();
        source_cc.set("group.id", &source_group);
        let source = Arc::new(
            RdkafkaSource::new(&source_cc, &input_def.topic)
                .with_context(|| format!("creando source para '{}'", input_def.name))?,
        );
        sources.push(source.clone());
        inputs.push(InputSource {
            name: input_def.name.clone(),
            schema: schema.clone(),
        });
    }

    // --- Factory de producción: cablea cada input a su StreamingTable ---
    // Mapa nombre lógico -> source (1:1 con los inputs de la config).
    let batch_size: usize = 100;
    let name_to_source: std::collections::HashMap<String, Arc<RdkafkaSource>> = config
        .inputs
        .iter()
        .zip(sources.iter())
        .map(|(def, src)| (def.name.clone(), src.clone()))
        .collect();
    let factory: Box<StreamTableFactory> = Box::new(move |name, schema| {
        let source = name_to_source
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("source no encontrado para '{name}'"))?;
        let decoder = Decoder::new(schema.clone(), DecodeFormat::Json);
        let make_stream = Arc::new(move || source.record_stream());
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

    // --- Loop: consume el stream y escribe al sink ---
    // El commit es time-driven (no batch-driven): con `select!` el timer
    // dispara el commit cada `commit_interval` aunque no llegue ningún batch
    // (streams de bajo tráfico). Si el commit dependiera de la llegada de
    // batches, un stream silencioso retendría los datos en el writer sin
    // commitar indefinidamente.
    let commit_interval = options.commit_interval;
    let mut commit_timer = tokio::time::interval(commit_interval);
    commit_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // La primera tick es inmediata: se consume para que el primer commit real
    // ocurra a los `commit_interval`.
    commit_timer.tick().await;
    let mut dirty = false;

    loop {
        tokio::select! {
            maybe_batch = stream.next() => {
                match maybe_batch {
                    Some(Ok(batch)) => {
                        let rows = batch.num_rows() as u64;
                        tracing::debug!(rows, "batch de salida desde DataFusion");
                        metrics.inc_rows_read(rows);

                        sink.write(&batch)
                            .await
                            .context("escribiendo al sink Paimon")?;
                        metrics.inc_rows_written(rows);
                        dirty = true;
                    }
                    Some(Err(e)) => return Err(e).context("batch de salida"),
                    None => break, // stream terminó
                }
            }
            _ = commit_timer.tick() => {
                if dirty {
                    sink.commit().await.context("commit del sink")?;
                    metrics.inc_commits();
                    // Commit de offsets (at-least-once).
                    for source in &sources {
                        if let Err(e) = source.commit() {
                            tracing::warn!(error = %e, "commit de offsets");
                            metrics.inc_errors();
                        }
                    }
                    dirty = false;
                }
            }
        }
    }

    // El stream terminó (en producción es infinito; en tests puede terminar).
    // Commit final para no perder datos.
    sink.commit().await.context("commit final del sink")?;
    metrics.inc_commits();
    for source in &sources {
        let _ = source.commit();
    }

    Ok(PipelineHandle {
        metrics_addr,
        metrics,
    })
}
