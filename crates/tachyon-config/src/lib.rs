//! `tachyon-config`: parsing y validación de `pipeline.yaml`.

pub mod schema;
pub mod validate;

pub use schema::PipelineConfig;
pub use validate::validate_config;
