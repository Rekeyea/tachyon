//! Schema de `pipeline.yaml` (ver DESIGN.md §3.2).

use serde::Deserialize;

/// La configuración completa de un pipeline.
#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    pub pipeline: PipelineMeta,
    pub connectors: Connectors,
    pub inputs: Vec<InputDef>,
    pub output: OutputConfig,
    pub deployment: Deployment,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineMeta {
    pub name: String,
    #[serde(default = "default_version")]
    pub version: u32,
}

fn default_version() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub struct Connectors {
    pub redpanda: RedpandaConfig,
    pub paimon: PaimonConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedpandaConfig {
    pub brokers: Vec<String>,
    #[serde(default)]
    pub security: Option<SecurityConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SecurityConfig {
    pub sasl_mechanism: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PaimonConfig {
    pub warehouse: String,
    #[serde(default = "default_catalog")]
    pub catalog: String,
    pub catalog_uri: Option<String>,
}

fn default_catalog() -> String {
    "local".to_string()
}

/// Codificación de los mensajes del topic. Adentro del pipeline el dato es
/// siempre un `RecordBatch` Arrow: el decoder corre una sola vez en el borde.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadFormat {
    /// Un objeto JSON por mensaje. Default.
    #[default]
    Json,
    /// Un datum Avro por mensaje (o un object container), con el schema de
    /// `avro_schema`.
    Avro,
}

/// Un stream de entrada (vincula nombre lógico -> topic físico).
#[derive(Debug, Clone, Deserialize)]
pub struct InputDef {
    pub name: String,
    pub topic: String,
    /// Clave de particionado (== state key == bucket key).
    pub key: String,
    /// Ruta al schema Arrow de las columnas que ve el SQL.
    pub schema: String,
    /// Codificación del payload. Default: `json`.
    #[serde(default)]
    pub format: PayloadFormat,
    /// Ruta al schema Avro (`.avsc`) de los mensajes. Obligatoria con
    /// `format: avro`. No se usa como formato interno: cada mensaje se
    /// decodifica a Arrow y el resto del pipeline no vuelve a ver Avro.
    #[serde(default)]
    pub avro_schema: Option<String>,
    #[serde(default)]
    pub watermark: Option<WatermarkConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WatermarkConfig {
    pub column: String,
    pub lag: String,
    /// Si la partición no trae un evento a tiempo durante este plazo, deja de
    /// frenar el watermark. Ausente: el mismo valor que `lag`.
    #[serde(default)]
    pub idle: Option<String>,
}

/// La salida (vincula nombre lógico -> tabla Paimon).
#[derive(Debug, Clone, Deserialize)]
pub struct OutputConfig {
    pub name: String,
    pub table: String,
    pub key: String,
    pub bucket: usize,
    pub sequence_field: Option<String>,
    pub rowkind_field: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Deployment {
    pub partitions: usize,
    #[serde(default)]
    pub resources: Option<Resources>,
    #[serde(default)]
    pub scaling: Option<Scaling>,
    #[serde(default)]
    pub state: Option<StateConfig>,
    /// Intervalo de commit del sink (p. ej. "10s"). Default: 10s.
    #[serde(default = "default_commit_interval")]
    pub commit_interval: String,
    /// Métricas de la instancia (endpoint HTTP).
    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
    /// Consumidores paralelos por topic (mismo consumer group). El protocolo de
    /// grupo reparte las particiones entre ellos y cada uno hace fetch en
    /// paralelo. Default: `min(partitions, cpus pineados)`. Un override queda
    /// fijo y deja de seguir al pin.
    #[serde(default)]
    pub consumers_per_topic: Option<usize>,
    /// Filas por batch decodificado (la unidad de flujo end-to-end: un lote
    /// drenado del broker = un `RecordBatch`). Batches grandes amortizan el
    /// overhead por batch del decoder, de DataFusion y del sink. Default:
    /// 32768. No depende del pin: es el tamaño que minimiza el overhead por
    /// fila sin inflar la latencia de un batch.
    #[serde(default)]
    pub batch_size: Option<usize>,
    /// Lotes decodificados en paralelo dentro del stream. El orden de emisión
    /// se preserva, así que el tracking de offsets (exactly-once) no cambia.
    /// Default: un hilo por CPU pineado (el decode es la etapa que escala).
    /// Un override queda fijo y deja de seguir al pin.
    #[serde(default)]
    pub decode_parallelism: Option<usize>,
}

fn default_commit_interval() -> String {
    "10s".to_string()
}

/// Configuración del endpoint de métricas.
#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    /// Dirección a escuchar (p. ej. "127.0.0.1:9090"). Default: "127.0.0.1:0"
    /// (puerto efímero, útil en tests).
    #[serde(default = "default_metrics_bind")]
    pub bind_addr: String,
}

fn default_metrics_bind() -> String {
    "127.0.0.1:0".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct Resources {
    /// CPUs del presupuesto. Cores (`"4"`, `"1.5"`) o millicores (`"2500m"`).
    /// Tope del pin detectado (afinidad del proceso o quota del cgroup): el
    /// runtime deriva consumidores y decode de `min(este valor, pin)`.
    /// Ausente: se usa el pin entero.
    pub cpu: Option<String>,
    pub memory: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Scaling {
    #[serde(default = "default_strategy")]
    pub strategy: String,
    #[serde(default = "default_min")]
    pub min: usize,
    #[serde(default = "default_max")]
    pub max: usize,
}

fn default_strategy() -> String {
    "partitions".to_string()
}
fn default_min() -> usize {
    1
}
fn default_max() -> usize {
    16
}

#[derive(Debug, Clone, Deserialize)]
pub struct StateConfig {
    /// redb es el único backend (opinionated); no hay opción que elegir.
    pub checkpoint_interval: Option<String>,
    pub checkpoint_storage: Option<String>,
    pub checkpoint_retention: Option<usize>,
}
