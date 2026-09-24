//! `tachyon-source`: conector fuente Redpanda.
//!
//! Implementa una fuente de streaming de DataFusion (`PartitionStream`) sobre
//! el cliente de Redpanda (rdkafka): consumer group, particionado por clave, y
//! decodificación a Arrow (JSON/Avro).
//!
//! Arquitectura (decouplada del broker para poder testear sin él):
//! - `record`: `SourceRecord`, la unidad cruda que emite la fuente.
//! - `consumer`: el consumidor rdkafka (depende de broker) + fuente in-memory.
//! - `decode`: `Decoder`, decodifica payloads a `RecordBatch` (JSON/Avro).
//! - `stream`: `RedpandaPartitionStream`, la integración con DataFusion.

pub mod consumer;
pub mod decode;
pub mod record;
pub mod stream;

pub use consumer::{in_memory_stream, RdkafkaSource, RecordStream};
pub use decode::{DecodeFormat, Decoder};
pub use record::SourceRecord;
pub use stream::RedpandaPartitionStream;
