//! `tachyon-sink`: conectores de salida.
//!
//! Paimon (writer por bucket con sequence numbers y commit, ver DESIGN.md §7),
//! topic de Redpanda (productor transaccional) y Kinesis/SQS (at-least-once).

pub mod aws;
pub mod compact;
pub mod dimension;
pub mod kinesis;
pub mod redpanda;
pub mod shard;
pub mod sqs;
pub mod writer;
