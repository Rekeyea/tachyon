//! Cola de una tabla Paimon.
//!
//! El cursor es el id del snapshot. Las filas nuevas y ese id se publican en
//! la misma transacción: los registros en el topic de salida y uno en
//! `{topic}-tachyon-cursor`, con clave `commit_user` y el id en decimal. Un
//! crash antes del commit no deja ni las filas ni el cursor.
//!
//! Si la tabla tiene changelog, cada mensaje lleva `rowkind` (`+I`, `-U`,
//! `+U`, `-D`) adelante de las columnas del `SELECT`.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};
use tachyon_config::{PayloadFormat, PipelineConfig};
use tachyon_metrics::{start_observability, InstanceMetrics};
use tachyon_sink::redpanda::RedpandaSink;
use tachyon_sink::writer::{open_table_with, stream_schema, tail_appends};
use tachyon_source::{
    avro_json_from_arrow, encode_envelopes, parse_avro_schema, register_topic_schema,
};
use tachyon_sql::{table_select, TableColumns};

use crate::run::{ensure_topic_partitions, PipelineHandle, RunOptions};

/// Lee los snapshots nuevos de la única tabla de entrada y los publica en
/// `topic`. El `SELECT` nombra columnas; no filtra ni agrega.
pub async fn run_table_stream(
    config: &PipelineConfig,
    select_sql: &str,
    options: &RunOptions,
    topic: &str,
    key: &str,
    metrics: &std::sync::Arc<InstanceMetrics>,
) -> Result<PipelineHandle> {
    if config.inputs.len() != 1 {
        anyhow::bail!("leer una tabla Paimon es el único input");
    }
    if config
        .deployment
        .consumers_per_topic
        .is_some_and(|count| count != 1)
    {
        anyhow::bail!("leer una tabla usa un cursor, no varios consumidores");
    }
    let input = &config.inputs[0];
    let table_id = input
        .paimon_table()
        .context("el input no es una tabla Paimon")?
        .to_string();
    let parsed = table_select(select_sql)?;
    if parsed.source != input.name {
        anyhow::bail!(
            "el FROM '{}' no es el input '{}'",
            parsed.source,
            input.name
        );
    }

    let (database, table_name) = split_table(&table_id)?;
    let catalog = config
        .connectors
        .paimon
        .as_ref()
        .context("leer una tabla requiere connectors.paimon")?
        .catalog_options();
    let table = open_table_with(&catalog, &database, &table_name)
        .await
        .with_context(|| format!("abriendo {table_id}"))?;
    let columns = match parsed.columns {
        TableColumns::Star => table
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect(),
        TableColumns::Names(names) => names,
    };
    if !columns.iter().any(|name| name == key) {
        anyhow::bail!("la clave '{key}' no está en el SELECT");
    }
    let schema = stream_schema(&table, &columns).context("schema de la cola")?;
    let key_field = schema
        .field_with_name(key)
        .map_err(|_| anyhow::anyhow!("la salida no tiene la clave '{key}'"))?;
    match key_field.data_type() {
        DataType::Int64 | DataType::Int32 | DataType::Utf8 => {}
        other => anyhow::bail!(
            "la clave '{key}' es {other}; el topic la publica como Int64, Int32 o Utf8"
        ),
    }

    start_observability(metrics, options.metrics_bind, options.otlp.as_ref())
        .await
        .context("arrancando observabilidad")?;

    let brokers = config.redpanda()?.brokers.join(",");
    ensure_topic_partitions(&brokers, topic, config.deployment.partitions).await?;
    let cursor_topic = format!("{topic}-tachyon-cursor");
    ensure_cursor_topic(&brokers, topic, &cursor_topic).await?;

    let timeout_ms = (options.commit_interval.as_millis() as u64)
        .saturating_mul(4)
        .max(120_000);
    let wire = match config.output.format {
        PayloadFormat::Json => {
            for field in schema.fields() {
                match field.data_type() {
                    DataType::Int64
                    | DataType::Int32
                    | DataType::Float64
                    | DataType::Utf8
                    | DataType::Boolean => {}
                    other => anyhow::bail!(
                        "la columna '{}' es {other}; JSON publica Int64, Int32, Float64, Utf8 o Boolean",
                        field.name()
                    ),
                }
            }
            TopicWire::Json
        }
        PayloadFormat::Avro => {
            let url = config
                .connectors
                .schema_registry
                .as_ref()
                .context("format avro requiere connectors.schema_registry.url")?
                .url
                .clone();
            let avsc = avro_json_from_arrow(&schema)?;
            let avro = parse_avro_schema(&avsc)?;
            let id = register_topic_schema(&url, topic, &avsc)
                .context("registrando el schema de la salida")?;
            tracing::info!(topic, schema_id = id, "schema de salida registrado");
            TopicWire::Avro { schema: avro, id }
        }
    };
    // init_transactions cerca al productor anterior. El cursor se lee después,
    // cuando su transacción abierta ya no es visible.
    let sink = RedpandaSink::open(&brokers, topic, key, &options.commit_user, timeout_ms).await?;
    let mut cursor = {
        let brokers = brokers.clone();
        let cursor_topic = cursor_topic.clone();
        let commit_user = options.commit_user.clone();
        tokio::task::spawn_blocking(move || read_cursor(&brokers, &cursor_topic, &commit_user))
            .await
            .context("leyendo el cursor")??
    };
    tracing::info!(
        table = %table_id,
        topic,
        cursor,
        cursor_topic = %cursor_topic,
        "cola de tabla activa; el commit es la transacción de Redpanda"
    );

    loop {
        let tail = tail_appends(&table, cursor, &columns)
            .await
            .context("leyendo snapshots nuevos")?;
        if tail.through == cursor {
            if let Some(snapshot) = tail.blocked_at {
                anyhow::bail!(
                    "el snapshot {snapshot} es OVERWRITE; la cola no publica un archivo reescrito"
                );
            }
            tokio::time::sleep(options.commit_interval).await;
            continue;
        }
        for batch in &tail.batches {
            if !same_shape(batch.schema().as_ref(), &schema) {
                anyhow::bail!(
                    "el batch de la tabla tiene columnas {:?} y la proyección es {:?}",
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| format!("{}:{:?}", field.name(), field.data_type()))
                        .collect::<Vec<_>>(),
                    schema
                        .fields()
                        .iter()
                        .map(|field| format!("{}:{:?}", field.name(), field.data_type()))
                        .collect::<Vec<_>>()
                );
            }
        }
        sink.begin().await?;
        let mut rows = 0usize;
        for batch in &tail.batches {
            rows += publish(&sink, &wire, key, batch).await?;
        }
        let payload = tail.through.to_string().into_bytes();
        sink.write_to(&cursor_topic, &[(options.commit_user.clone(), payload)])
            .await
            .context("publicando el cursor")?;
        sink.commit().await?;
        cursor = tail.through;
        metrics.inc_rows_read(rows as u64);
        metrics.inc_rows_written(rows as u64);
        metrics.inc_commits();
        tracing::info!(snapshot = cursor, rows, topic, "tabla publicada");
        if let Some(snapshot) = tail.blocked_at {
            anyhow::bail!(
                "el snapshot {snapshot} es OVERWRITE; se publicó hasta el snapshot {cursor} y la cola se detiene"
            );
        }
    }
}

enum TopicWire {
    Json,
    Avro {
        schema: std::sync::Arc<apache_avro::Schema>,
        id: i32,
    },
}

async fn publish(
    sink: &RedpandaSink,
    wire: &TopicWire,
    key: &str,
    batch: &RecordBatch,
) -> Result<usize> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    match wire {
        TopicWire::Json => sink.write(batch).await.context("publicando el batch"),
        TopicWire::Avro { schema, id } => {
            let records = encode_envelopes(batch, key, schema, *id).context("codificando Avro")?;
            sink.write_records(&records)
                .await
                .context("publicando el batch")
        }
    }
}

fn same_shape(batch: &Schema, expected: &Schema) -> bool {
    batch.fields().len() == expected.fields().len()
        && batch
            .fields()
            .iter()
            .zip(expected.fields())
            .all(|(left, right)| {
                left.name() == right.name()
                    && left.data_type() == right.data_type()
                    && left.is_nullable() == right.is_nullable()
            })
}

fn split_table(id: &str) -> Result<(String, String)> {
    match id.split_once('.') {
        Some((database, table)) if !database.is_empty() && !table.is_empty() => {
            Ok((database.to_string(), table.to_string()))
        }
        None if !id.is_empty() => Ok(("default".to_string(), id.to_string())),
        _ => anyhow::bail!("identificador de tabla inválido: '{id}'"),
    }
}

async fn ensure_cursor_topic(brokers: &str, output: &str, cursor: &str) -> Result<()> {
    let replication = replication_of(brokers, output)?;
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("admin del cursor")?;
    let created = admin
        .create_topics(
            &[NewTopic::new(
                cursor,
                1,
                TopicReplication::Fixed(replication),
            )],
            &AdminOptions::new(),
        )
        .await
        .context("creando el topic del cursor")?;
    for item in created {
        if let Err((name, err)) = item {
            let text = err.to_string().to_ascii_lowercase();
            if !text.contains("already exists") {
                anyhow::bail!("creando '{name}': {err}");
            }
        }
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last = None;
    while Instant::now() < deadline {
        match ensure_topic_partitions(brokers, cursor, 1).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                last = Some(err);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("el topic del cursor no apareció")))
}

fn replication_of(brokers: &str, topic: &str) -> Result<i32> {
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("cliente para la replicación del topic")?;
    let metadata = probe
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .with_context(|| format!("metadata de '{topic}'"))?;
    let found = metadata
        .topics()
        .iter()
        .find(|item| item.name() == topic)
        .with_context(|| format!("el topic '{topic}' no está en la metadata"))?;
    let partition = found
        .partitions()
        .first()
        .with_context(|| format!("el topic '{topic}' no tiene particiones"))?;
    let replicas = partition.replicas().len();
    if replicas == 0 {
        anyhow::bail!("el topic '{topic}' no tiene réplicas");
    }
    i32::try_from(replicas).context("factor de replicación")
}

fn read_cursor(brokers: &str, topic: &str, commit_user: &str) -> Result<i64> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set(
            "group.id",
            format!(
                "tachyon-cursor-{}-{}",
                commit_user,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("reloj")
                    .as_nanos()
            ),
        )
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .create()
        .context("consumidor del cursor")?;
    let (_, high) = watermarks(&consumer, topic)?;
    if high == 0 {
        return Ok(0);
    }
    let mut list = TopicPartitionList::new();
    list.add_partition_offset(topic, 0, Offset::Beginning)
        .context("assign del cursor")?;
    consumer.assign(&list).context("assign del cursor")?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut cursor = None;
    while Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(message)) => {
                let key = message
                    .key()
                    .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                    .unwrap_or_default();
                if key == commit_user {
                    let payload = message.payload().context("el cursor no tiene payload")?;
                    let text = std::str::from_utf8(payload)
                        .with_context(|| format!("cursor de '{topic}' no es utf-8"))?;
                    cursor = Some(text.parse::<i64>().with_context(|| {
                        format!("cursor '{text}' de '{topic}' no es un snapshot")
                    })?);
                }
            }
            Some(Err(err)) => anyhow::bail!("leyendo '{topic}': {err}"),
            None => {
                if next_offset(&consumer, topic)? >= high {
                    return Ok(cursor.unwrap_or(0));
                }
            }
        }
    }
    anyhow::bail!("no se leyó el cursor de '{topic}' hasta el offset {high}")
}

fn watermarks(consumer: &BaseConsumer, topic: &str) -> Result<(i64, i64)> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last = None;
    while Instant::now() < deadline {
        match consumer.fetch_watermarks(topic, 0, Duration::from_secs(2)) {
            Ok(marks) => return Ok(marks),
            Err(err) => {
                last = Some(err);
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
    Err(anyhow::anyhow!(
        "watermarks de '{topic}': {}",
        last.map(|err| err.to_string()).unwrap_or_default()
    ))
}

fn next_offset(consumer: &BaseConsumer, topic: &str) -> Result<i64> {
    let list = consumer.position().context("posición del cursor")?;
    let Some(element) = list
        .elements()
        .into_iter()
        .find(|element| element.topic() == topic && element.partition() == 0)
    else {
        return Ok(-1);
    };
    match element.offset() {
        Offset::Offset(offset) => Ok(offset),
        Offset::Beginning | Offset::Stored | Offset::Invalid => Ok(-1),
        other => anyhow::bail!("posición del cursor inesperada: {other:?}"),
    }
}
