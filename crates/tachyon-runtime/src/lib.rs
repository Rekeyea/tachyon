//! `tachyon-runtime`: runtime de ejecución de Tachyon.
//!
//! Cablea fuente (Redpanda) -> transformación (DataFusion) -> sink (Paimon) y
//! corre el loop del pipeline.

mod aws;
pub mod budget;
pub mod convert;
pub mod execute;
pub mod join;
pub mod lookup;
pub mod run;
pub mod runtime;
pub mod table_pipeline;
pub mod table_stream;
mod table_union;
pub mod union;
pub mod window;
pub mod window_shards;

pub use budget::StatelessBudget;
pub use execute::{execute_query, InputSource, StreamTable, StreamTableFactory, TransformOutput};
pub use run::{run_pipeline, run_topic_pipeline, PipelineHandle, PreparedInput, RunOptions};
pub use runtime::Pipeline;
pub use table_stream::run_table_stream;
pub use window::{ClosedWindow, Num, WindowFault, WindowInput, WindowOperator};
