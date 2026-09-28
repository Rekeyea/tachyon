//! `tachyon-core`: tipos base y plan de pipeline.
//!
//! Aquí viven los tipos que comparten todos los crates: la definición de
//! streams, la clave de particionado (el invariante de alineación) y el plan
//! de pipeline compilado desde `pipeline.sql` + `pipeline.yaml`.

pub mod checkpoint;
pub mod error;
pub mod plan;
pub mod types;

pub use checkpoint::{
    parse_checkpoint, partition_tickets, Accumulators, AggKind, AggSpec, AggState, CheckpointBody,
    JoinCell, JoinCheckpointV1, JoinColumnSpec, JoinEvent, JoinKeyState, JoinSpec, JoinState,
    KeyState, OperatorState, PartitionProgress, PartitionTicketV1, SessionState,
    WindowCheckpointV1, WindowKind, WindowSpecId, MAX_SIDECAR_BYTES,
};
pub use error::Error;
pub use plan::{OutputDef, PipelinePlan};
pub use types::{Column, PartitionKey, SourceOffsets, StreamDef};
