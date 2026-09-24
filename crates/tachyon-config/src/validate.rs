//! Validación del invariante de alineación (ver DESIGN.md §3.3).

use crate::schema::PipelineConfig;
use tachyon_core::Error;

/// Valida la config contra el invariante de alineación:
///
/// - `deployment.partitions == output.bucket`
/// - `inputs[*].key == output.key`
pub fn validate_config(cfg: &PipelineConfig) -> Result<(), Error> {
    if cfg.deployment.partitions != cfg.output.bucket {
        return Err(Error::Alignment {
            expected: cfg.deployment.partitions,
            found: cfg.output.bucket,
        });
    }

    for input in &cfg.inputs {
        if input.key != cfg.output.key {
            return Err(Error::KeyMismatch(format!(
                "input '{}' key '{}' != output key '{}'",
                input.name, input.key, cfg.output.key
            )));
        }
    }

    Ok(())
}
