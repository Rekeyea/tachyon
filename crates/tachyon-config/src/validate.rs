//! Validación del invariante de alineación (ver DESIGN.md §3.3).

use crate::schema::PipelineConfig;
use tachyon_core::Error;

/// Valida la config contra el invariante de alineación:
///
/// - `deployment.partitions == output.bucket`
/// - `inputs[*].key == output.key`
///
/// Y que los knobs opcionales de despliegue, si están, sean usables. El
/// presupuesto de CPUs (consumidores, decode) lo deriva el runtime; acá solo
/// se rechaza lo que no se puede interpretar.
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

    if let Some(cpu) = cfg
        .deployment
        .resources
        .as_ref()
        .and_then(|r| r.cpu.as_deref())
    {
        parse_cpu_cores(cpu)?;
    }
    for (name, value) in [
        ("deployment.batch_size", cfg.deployment.batch_size),
        (
            "deployment.consumers_per_topic",
            cfg.deployment.consumers_per_topic,
        ),
        (
            "deployment.decode_parallelism",
            cfg.deployment.decode_parallelism,
        ),
    ] {
        if value == Some(0) {
            return Err(Error::Config(format!("{name} debe ser >= 1")));
        }
    }

    Ok(())
}

/// Interpreta `deployment.resources.cpu` como cantidad de cores enteros.
///
/// Acepta cores (`"4"`, `"1.5"`) y millicores al estilo de Kubernetes
/// (`"500m"`, `"2500m"`). El resultado es el techo: un pipeline no corre en
/// una fracción de hilo, así que `500m` y `1.5` valen 1 y 2 cores. El runtime
/// usa este número como tope del pin detectado (afinidad / quota).
pub fn parse_cpu_cores(raw: &str) -> Result<usize, Error> {
    let s = raw.trim();
    let (number, millis) = if let Some(n) = s.strip_suffix('m') {
        (n, true)
    } else {
        (s, false)
    };
    let value: f64 = number.parse().map_err(|_| {
        Error::Config(format!(
            "deployment.resources.cpu '{raw}' no es un número de cores ni millicores"
        ))
    })?;
    if !value.is_finite() || value <= 0.0 {
        return Err(Error::Config(format!(
            "deployment.resources.cpu '{raw}' debe ser > 0"
        )));
    }
    let cores = if millis { value / 1000.0 } else { value };
    Ok((cores.ceil() as usize).max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_cores_and_millicores_round_up() {
        assert_eq!(parse_cpu_cores("4").unwrap(), 4);
        assert_eq!(parse_cpu_cores("1.5").unwrap(), 2);
        assert_eq!(parse_cpu_cores("500m").unwrap(), 1);
        assert_eq!(parse_cpu_cores("2500m").unwrap(), 3);
        assert!(parse_cpu_cores("0").is_err());
        assert!(parse_cpu_cores("abc").is_err());
    }
}
