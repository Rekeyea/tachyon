//! `tachyon-config`: parsing y validación de `pipeline.yaml`.

pub mod schema;
pub mod validate;

pub use schema::{
    DimensionDef, InputDef, InputKind, KinesisConfig, PayloadFormat, PipelineConfig, SqsConfig,
    StartFrom,
};
pub use validate::{parse_cpu_cores, parse_fixed_duration, parse_memory_bytes, validate_config};
