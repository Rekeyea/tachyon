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
pub mod handoff;
mod json_flat;
pub mod kinesis;
pub mod record;
pub mod schema_wire;
pub mod sqs;
pub mod stream;

pub use consumer::{
    in_memory_stream, CopiedCursor, GroupMetadata, LotRanges, RdkafkaSource, RecordStream,
};
pub use handoff::{StateRequest, WindowHandoff};
pub use decode::{parse_avro_schema, DecodeFormat, Decoder};
pub use kinesis::{KinesisSource, LotPositions, StartPosition};
pub use schema_wire::{
    arrow_from_avro, avro_json_from_arrow, encode_envelopes, latest_topic_schema,
    register_topic_schema, SchemaCache,
};
pub use record::SourceRecord;
pub use sqs::SqsSource;
pub use stream::{LaneRanges, OffsetTracker, PositionTracker, ReceiptsOut, RedpandaPartitionStream};
