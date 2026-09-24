//! `tachyon-runtime`: runtime de ejecución de Tachyon.
//!
//! Cablea fuente (Redpanda) -> transformación (DataFusion) -> sink (Paimon) y
//! corre el loop del pipeline.

pub mod execute;
pub mod run;
pub mod runtime;

pub use execute::{execute_query, InputSource, StreamTable, StreamTableFactory, TransformOutput};
pub use run::{run_pipeline, PipelineHandle, RunOptions};
pub use runtime::Pipeline;
