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
        match input.format {
            crate::schema::PayloadFormat::Json => {
                if input.avro_schema.is_some() {
                    return Err(Error::Config(format!(
                        "input '{}': avro_schema solo se usa con format: avro",
                        input.name
                    )));
                }
            }
            crate::schema::PayloadFormat::Avro => {
                let missing = input
                    .avro_schema
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or("")
                    .is_empty();
                if missing {
                    return Err(Error::Config(format!(
                        "input '{}': format avro requiere avro_schema",
                        input.name
                    )));
                }
            }
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

/// Duración fija en milisegundos. Gramática `^[0-9]+(ms|s|m|h|d)$`, valor > 0.
///
/// `d` son 86_400_000 ms, no un día civil. No hay fallback: un sufijo
/// desconocido es error. `parse_duration` del runtime no sirve acá (`"5h"`
/// caería en 10 s).
pub fn parse_fixed_duration(raw: &str) -> Result<i64, Error> {
    let s = raw.trim();
    let (number, unit) = if let Some(n) = s.strip_suffix("ms") {
        (n, "ms")
    } else if let Some(n) = s.strip_suffix('s') {
        (n, "s")
    } else if let Some(n) = s.strip_suffix('m') {
        (n, "m")
    } else if let Some(n) = s.strip_suffix('h') {
        (n, "h")
    } else if let Some(n) = s.strip_suffix('d') {
        (n, "d")
    } else {
        return Err(Error::Config(format!(
            "duración '{raw}' debe ser un entero con sufijo ms, s, m, h o d"
        )));
    };
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::Config(format!(
            "duración '{raw}' debe ser un entero con sufijo ms, s, m, h o d"
        )));
    }
    let value: i64 = number.parse().map_err(|_| {
        Error::Config(format!("duración '{raw}' no entra en un entero"))
    })?;
    if value <= 0 {
        return Err(Error::Config(format!("duración '{raw}' debe ser > 0")));
    }
    let factor: i64 = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => unreachable!("sufijo ya filtrado"),
    };
    value.checked_mul(factor).ok_or_else(|| {
        Error::Config(format!("duración '{raw}' se pasa de i64 milisegundos"))
    })
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
    use crate::schema::PipelineConfig;

    #[test]
    fn cpu_cores_and_millicores_round_up() {
        assert_eq!(parse_cpu_cores("4").unwrap(), 4);
        assert_eq!(parse_cpu_cores("1.5").unwrap(), 2);
        assert_eq!(parse_cpu_cores("500m").unwrap(), 1);
        assert_eq!(parse_cpu_cores("2500m").unwrap(), 3);
        assert!(parse_cpu_cores("0").is_err());
        assert!(parse_cpu_cores("abc").is_err());
    }

    #[test]
    fn fixed_duration_has_no_fallback() {
        assert_eq!(parse_fixed_duration("5h").unwrap(), 18_000_000);
        assert_eq!(parse_fixed_duration("500ms").unwrap(), 500);
        assert_eq!(parse_fixed_duration("1d").unwrap(), 86_400_000);
        assert_eq!(parse_fixed_duration("1m").unwrap(), 60_000);
        assert!(parse_fixed_duration("5x").is_err());
        assert!(parse_fixed_duration("").is_err());
        assert!(parse_fixed_duration("0s").is_err());
    }

    fn pipeline(extra_input: &str) -> PipelineConfig {
        serde_yaml::from_str(&format!(
            r#"
pipeline:
  name: t
connectors:
  redpanda:
    brokers: ["localhost:9092"]
  paimon:
    warehouse: ./w
inputs:
  - name: orders
    topic: t
    key: order_id
    schema: orders.json
{extra_input}
output:
  name: orders_lake
  table: default.t
  key: order_id
  bucket: 1
deployment:
  partitions: 1
"#
        ))
        .expect("yaml de test")
    }

    #[test]
    fn json_is_the_default_and_rejects_an_avro_schema() {
        let ok = pipeline("");
        assert_eq!(ok.inputs[0].format, crate::schema::PayloadFormat::Json);
        assert!(validate_config(&ok).is_ok());

        let err = validate_config(&pipeline("    avro_schema: orders.avsc\n")).unwrap_err();
        assert!(err.to_string().contains("avro_schema"), "{err}");
    }

    #[test]
    fn avro_requires_its_schema_file() {
        let err = validate_config(&pipeline("    format: avro\n")).unwrap_err();
        assert!(err.to_string().contains("avro_schema"), "{err}");

        let ok = pipeline("    format: avro\n    avro_schema: orders.avsc\n");
        assert!(validate_config(&ok).is_ok());
    }
}
