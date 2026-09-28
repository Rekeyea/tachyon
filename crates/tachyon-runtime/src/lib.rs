//! `tachyon-runtime`: runtime de ejecución de Tachyon.
//!
//! Cablea fuente (Redpanda) -> transformación (DataFusion) -> sink (Paimon) y
//! corre el loop del pipeline.

pub mod budget;
pub mod execute;
pub mod run;
pub mod runtime;
pub mod window;

pub use budget::StatelessBudget;
pub use execute::{execute_query, InputSource, StreamTable, StreamTableFactory, TransformOutput};
pub use run::{run_pipeline, PipelineHandle, PreparedInput, RunOptions};
pub use runtime::Pipeline;
pub use window::{ClosedWindow, Num, WindowFault, WindowInput, WindowOperator};
