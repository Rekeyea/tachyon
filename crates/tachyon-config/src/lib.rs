//! `tachyon-config`: parsing y validación de `pipeline.yaml`.

pub mod schema;
pub mod validate;

pub use schema::{DimensionDef, PayloadFormat, PipelineConfig};
pub use validate::{parse_cpu_cores, parse_fixed_duration, parse_memory_bytes, validate_config};
