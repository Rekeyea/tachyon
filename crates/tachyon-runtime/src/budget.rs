//! Presupuesto de un pipeline stateless.
//!
//! Exactly-once no es una perilla: el runtime siempre commitea los offsets en
//! el mismo paso que el snapshot de Paimon, y rechaza al arrancar un plan que
//! no sea pass-through. Lo que sí se deriva solo es el paralelismo, para que
//! el throughput siga a los CPUs pineados sin tocar el yaml.
//!
//! El pin es el que ya aplicó el operador (`taskset`, cpuset, `cpu.max` de
//! cgroup). `std::thread::available_parallelism` lo lee (afinidad y quota).
//! `deployment.resources.cpu` lo achica, no lo agranda. Tachyon no llama a
//! `sched_setaffinity`: si el proceso puede usar 10 cores y el yaml dice 4,
//! se arman 4 hilos de decode y el SO los reparte dentro del pin.
//!
//! Con N CPUs y sin overrides:
//! - `decode_parallelism = N` (un hilo de decode por CPU; es la etapa que
//!   escala en un ETL stateless, y el orden de emisión se preserva);
//! - `consumers_per_topic = min(partitions, N)` (un fetch por CPU, nunca más
//!   particiones que las que el grupo puede asignar);
//! - `batch_size = 32768` (amortiza el overhead por batch; no depende de N).
//!
//! El writer de Paimon es un solo task. A partir de unos pocos cores su costo
//! queda diluido y la tasa por CPU pineado se estabiliza; en 1 CPU el writer
//! comparte ese único core con el decode.

use anyhow::{Context, Result};
use tachyon_config::{parse_cpu_cores, PipelineConfig};

/// Knobs resueltos de un pipeline stateless.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatelessBudget {
    /// CPUs de los que sale el paralelismo.
    pub cpus: usize,
    /// Filas por `RecordBatch` de punta a punta.
    pub batch_size: usize,
    /// Clientes librdkafka por topic.
    pub consumers_per_topic: usize,
    /// Lotes decodificados a la vez, en orden.
    pub decode_parallelism: usize,
    /// `fetch.min.bytes` de librdkafka.
    pub fetch_min_bytes: u32,
    /// `fetch.max.bytes` de librdkafka.
    pub fetch_max_bytes: u32,
    /// `max.partition.fetch.bytes` de librdkafka.
    pub max_partition_fetch_bytes: u32,
    /// `fetch.wait.max.ms` de librdkafka.
    pub fetch_wait_max_ms: u32,
    /// Filas en vuelo entre la transformación y el writer.
    pub channel_rows: usize,
    /// Writers de Paimon en paralelo (buckets repartidos por `bucket % N`).
    /// Cada uno escribe y hace el flush de sus buckets en su propio hilo.
    pub sink_shards: usize,
    /// `queued.max.messages.kbytes` de librdkafka: el prefetch local de UN
    /// consumidor (no es por partición). Acota la memoria del source a
    /// `consumers_per_topic` veces esto.
    pub queued_max_kbytes: u32,
}

/// Overrides explícitos del yaml. `None` = seguir la fórmula del pin.
#[derive(Debug, Clone, Copy, Default)]
pub struct BudgetOverrides {
    pub batch_size: Option<usize>,
    pub consumers_per_topic: Option<usize>,
    pub decode_parallelism: Option<usize>,
}

impl StatelessBudget {
    pub const BATCH_SIZE: usize = 32_768;
    /// El broker no responde el fetch hasta juntar esto (o agotar
    /// `FETCH_WAIT_MAX_MS`). 512 KiB amortiza el round trip.
    pub const FETCH_MIN_BYTES: u32 = 524_288;
    /// Tope de un fetch. Grande para que un round trip traiga varios batches
    /// de `BATCH_SIZE` y el core pineado no se quede esperando la red.
    pub const FETCH_MAX_BYTES: u32 = 64 * 1024 * 1024;
    /// Tope por partición dentro de ese fetch. 4 MiB son ~50K filas JSON
    /// chicas: un solo viaje llena más de un batch.
    pub const MAX_PARTITION_FETCH_BYTES: u32 = 4 * 1024 * 1024;
    /// Con carga alta el broker responde apenas junta `FETCH_MIN_BYTES`;
    /// este tope solo actúa con poco tráfico. Con 1s, a baja carga cada fetch
    /// esperaba 1s entero y eso se sumaba a la latencia de cada fila.
    pub const FETCH_WAIT_MAX_MS: u32 = 100;
    pub const CHANNEL_ROWS: usize = 1_000_000;
    /// 64 MiB de prefetch por consumidor: a 1M filas/s de ~100 bytes son
    /// ~0.6s de colchón por cliente, de sobra para el round trip del fetch.
    /// Con 256 MiB, 4 consumidores inflaban el RSS a ~2.7 GB sin mover la tasa.
    pub const QUEUED_MAX_KBYTES: u32 = 65_536;

    /// Fórmula pura. `cpus` y `partitions` se tratan como >= 1.
    pub fn derive(cpus: usize, partitions: usize, overrides: BudgetOverrides) -> Result<Self> {
        let cpus = cpus.max(1);
        let partitions = partitions.max(1);
        Ok(Self {
            cpus,
            batch_size: positive("batch_size", overrides.batch_size, Self::BATCH_SIZE)?,
            consumers_per_topic: positive(
                "consumers_per_topic",
                overrides.consumers_per_topic,
                partitions.min(cpus),
            )?,
            decode_parallelism: positive(
                "decode_parallelism",
                overrides.decode_parallelism,
                cpus,
            )?,
            fetch_min_bytes: Self::FETCH_MIN_BYTES,
            fetch_max_bytes: Self::FETCH_MAX_BYTES,
            max_partition_fetch_bytes: Self::MAX_PARTITION_FETCH_BYTES,
            fetch_wait_max_ms: Self::FETCH_WAIT_MAX_MS,
            channel_rows: Self::CHANNEL_ROWS,
            queued_max_kbytes: Self::QUEUED_MAX_KBYTES,
            // Un writer por CPU (el router lo acota a los buckets): el flush
            // del epoch (sort + parquet + zstd) era un core entero.
            sink_shards: cpus,
        })
    }

    /// Presupuesto de una config. Lee el pin del proceso y, si está,
    /// `deployment.resources.cpu` como tope.
    pub fn resolve(config: &PipelineConfig) -> Result<Self> {
        let declared = match config
            .deployment
            .resources
            .as_ref()
            .and_then(|r| r.cpu.as_deref())
        {
            Some(raw) => Some(
                parse_cpu_cores(raw)
                    .map_err(|e| anyhow::anyhow!(e))
                    .with_context(|| format!("deployment.resources.cpu inválido: '{raw}'"))?,
            ),
            None => None,
        };
        let detected = detected_cpus();
        let cpus = match declared {
            Some(n) => n.min(detected).max(1),
            None => detected,
        };
        Self::derive(
            cpus,
            config.deployment.partitions,
            BudgetOverrides {
                batch_size: config.deployment.batch_size,
                consumers_per_topic: config.deployment.consumers_per_topic,
                decode_parallelism: config.deployment.decode_parallelism,
            },
        )
    }

    /// Workers de Tokio. El trabajo de CPU está en el pool de blocking
    /// (decode + poll); acá solo hace falta mover batches y el writer. Con el
    /// default de Tokio (un worker por core de la máquina) el proceso pineado
    /// a N cores igual arrancaba decenas de hilos y el throughput dejaba de
    /// ser función del pin.
    pub fn worker_threads(&self) -> usize {
        self.cpus.min(2).max(1)
    }

    /// Tope del pool de `spawn_blocking`. Los poll de librdkafka ocupan un
    /// hilo cada uno durante toda la vida del pipeline, y el decode ocupa
    /// hasta `decode_parallelism`. El +2 es margen para I/O del sink. El
    /// default de Tokio (512) sobre-suscribe cualquier pin.
    pub fn max_blocking_threads(&self) -> usize {
        // Cada shard del sink ocupa un hilo mientras vive y su flush otro
        // por epoch: 2 por shard.
        self.consumers_per_topic
            .saturating_add(self.decode_parallelism)
            .saturating_add(self.sink_shards.saturating_mul(2))
            .saturating_add(2)
            .max(1)
    }

    /// Capacidad del canal writer, en batches, equivalente a `channel_rows`.
    pub fn channel_batches(&self) -> usize {
        (self.channel_rows / self.batch_size.max(1)).clamp(8, 512)
    }
}

fn positive(name: &str, value: Option<usize>, default: usize) -> Result<usize> {
    match value {
        None => Ok(default),
        Some(0) => anyhow::bail!("{name} debe ser >= 1"),
        Some(n) => Ok(n),
    }
}

/// CPUs que el proceso puede usar ahora: afinidad (`taskset`, cpuset) y
/// quota de cgroup, vía `available_parallelism`.
pub fn detected_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(partitions: usize, extra: &str) -> PipelineConfig {
        serde_yaml::from_str(&format!(
            r#"
pipeline:
  name: t
connectors:
  redpanda:
    brokers: ["localhost:9092"]
  paimon:
    warehouse: ./w
inputs:
  - name: orders
    topic: t
    key: order_id
    schema: s
output:
  name: orders_lake
  table: default.t
  key: order_id
  bucket: {partitions}
deployment:
  partitions: {partitions}
{extra}
"#
        ))
        .expect("config de test")
    }

    #[test]
    fn one_cpu_is_one_decode_and_one_consumer() {
        let b = StatelessBudget::derive(1, 8, BudgetOverrides::default()).unwrap();
        assert_eq!(b.decode_parallelism, 1);
        assert_eq!(b.consumers_per_topic, 1);
        assert_eq!(b.batch_size, StatelessBudget::BATCH_SIZE);
        assert_eq!(b.worker_threads(), 1);
        assert_eq!(b.fetch_min_bytes, 524_288);
    }

    #[test]
    fn each_pinned_cpu_adds_a_decode_thread() {
        let b2 = StatelessBudget::derive(2, 16, BudgetOverrides::default()).unwrap();
        let b4 = StatelessBudget::derive(4, 16, BudgetOverrides::default()).unwrap();
        let b8 = StatelessBudget::derive(8, 16, BudgetOverrides::default()).unwrap();
        assert_eq!(b2.decode_parallelism, 2);
        assert_eq!(b4.decode_parallelism, 4);
        assert_eq!(b8.decode_parallelism, 8);
        assert_eq!(b8.decode_parallelism - b4.decode_parallelism, 4);
        assert_eq!(b4.consumers_per_topic, 4);
        assert_eq!(b8.consumers_per_topic, 8);
        assert_eq!(b4.worker_threads(), 2);
        assert_eq!(b2.batch_size, b8.batch_size);
    }

    #[test]
    fn consumers_stop_at_the_partition_count() {
        let b = StatelessBudget::derive(8, 1, BudgetOverrides::default()).unwrap();
        assert_eq!(b.consumers_per_topic, 1);
        assert_eq!(b.decode_parallelism, 8);
    }

    #[test]
    fn overrides_replace_the_formula() {
        let b = StatelessBudget::derive(
            8,
            8,
            BudgetOverrides {
                batch_size: Some(128),
                consumers_per_topic: Some(2),
                decode_parallelism: Some(3),
            },
        )
        .unwrap();
        assert_eq!(b.batch_size, 128);
        assert_eq!(b.consumers_per_topic, 2);
        assert_eq!(b.decode_parallelism, 3);
        assert_eq!(b.cpus, 8);
    }

    #[test]
    fn zero_override_is_rejected() {
        let err = StatelessBudget::derive(
            4,
            4,
            BudgetOverrides {
                batch_size: Some(0),
                ..BudgetOverrides::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("batch_size"), "{err}");
    }

    #[test]
    fn declared_cpu_caps_the_detected_pin() {
        let detected = detected_cpus();
        let cfg = config(32, "  resources:\n    cpu: \"2\"\n");
        let b = StatelessBudget::resolve(&cfg).unwrap();
        assert_eq!(b.cpus, 2.min(detected));
        assert_eq!(b.decode_parallelism, b.cpus);
        assert_eq!(b.consumers_per_topic, b.cpus);
    }

    #[test]
    fn absent_cpu_uses_the_whole_pin() {
        let cfg = config(32, "");
        let b = StatelessBudget::resolve(&cfg).unwrap();
        let detected = detected_cpus();
        assert_eq!(b.cpus, detected);
        assert_eq!(b.decode_parallelism, detected);
        assert_eq!(b.consumers_per_topic, 32.min(detected));
        assert_eq!(b.batch_size, StatelessBudget::BATCH_SIZE);
    }

    #[test]
    fn yaml_overrides_survive_resolve() {
        let cfg = config(
            8,
            "  batch_size: 100\n  consumers_per_topic: 2\n  decode_parallelism: 3\n",
        );
        let b = StatelessBudget::resolve(&cfg).unwrap();
        assert_eq!(b.batch_size, 100);
        assert_eq!(b.consumers_per_topic, 2);
        assert_eq!(b.decode_parallelism, 3);
    }

    #[test]
    fn channel_stays_near_one_million_rows() {
        let b = StatelessBudget::derive(4, 4, BudgetOverrides::default()).unwrap();
        let rows = b.channel_batches() * b.batch_size;
        assert!(rows <= StatelessBudget::CHANNEL_ROWS);
        assert!(rows + b.batch_size > StatelessBudget::CHANNEL_ROWS || b.channel_batches() == 8);
    }
}
