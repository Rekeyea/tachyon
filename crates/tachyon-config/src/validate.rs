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
fn blank(value: Option<&str>) -> bool {
    value.map(str::trim).unwrap_or("").is_empty()
}

fn registry_url(cfg: &PipelineConfig) -> Option<&str> {
    cfg.connectors
        .schema_registry
        .as_ref()
        .map(|registry| registry.url.trim())
        .filter(|url| !url.is_empty())
}

pub fn validate_config(cfg: &PipelineConfig) -> Result<(), Error> {
    match (&cfg.output.table, &cfg.output.topic) {
        (Some(table), None) => {
            if table.trim().is_empty() {
                return Err(Error::Config(
                    "output.table está vacío".to_string(),
                ));
            }
            let Some(bucket) = cfg.output.bucket else {
                return Err(Error::Config(
                    "output.bucket es obligatorio cuando la salida es una tabla".to_string(),
                ));
            };
            if cfg.deployment.partitions != bucket {
                return Err(Error::Alignment {
                    expected: cfg.deployment.partitions,
                    found: bucket,
                });
            }
            let warehouse = cfg
                .connectors
                .paimon
                .as_ref()
                .map(|p| p.warehouse.trim())
                .unwrap_or("");
            if warehouse.is_empty() {
                return Err(Error::Config(
                    "una salida a tabla requiere connectors.paimon.warehouse".to_string(),
                ));
            }
        }
        (None, Some(topic)) => {
            if topic.trim().is_empty() {
                return Err(Error::Config("output.topic está vacío".to_string()));
            }
            if cfg.output.bucket.is_some() {
                return Err(Error::Config(
                    "output.bucket pertenece a la tabla; un topic no lo usa".to_string(),
                ));
            }
            if cfg.output.sequence_field.is_some() || cfg.output.rowkind_field.is_some() {
                return Err(Error::Config(
                    "sequence_field y rowkind_field pertenecen a la tabla Paimon"
                        .to_string(),
                ));
            }
        }
        (Some(_), Some(_)) => {
            return Err(Error::Config(
                "la salida tiene table y topic; hace falta uno solo".to_string(),
            ));
        }
        (None, None) => {
            return Err(Error::Config(
                "la salida necesita output.table o output.topic".to_string(),
            ));
        }
    }

    if cfg.output.format == crate::schema::PayloadFormat::Avro {
        if cfg.output.topic.is_none() {
            return Err(Error::Config(
                "format avro de la salida publica un topic".to_string(),
            ));
        }
        if registry_url(cfg).is_none() {
            return Err(Error::Config(
                "format avro requiere connectors.schema_registry.url".to_string(),
            ));
        }
    }

    let mut table_inputs = 0;
    for input in &cfg.inputs {
        if input.key != cfg.output.key {
            return Err(Error::KeyMismatch(format!(
                "input '{}' key '{}' != output key '{}'",
                input.name, input.key, cfg.output.key
            )));
        }
        if input.paimon_table().is_some() {
            table_inputs += 1;
            if input.kafka_topic().is_ok() {
                return Err(Error::Config(format!(
                    "input '{}' tiene table y topic; hace falta uno solo",
                    input.name
                )));
            }
            if input.schema.is_some() || input.avro_schema.is_some() {
                return Err(Error::Config(format!(
                    "input '{}': la tabla trae su schema",
                    input.name
                )));
            }
            if input.format != crate::schema::PayloadFormat::Json {
                return Err(Error::Config(format!(
                    "input '{}': la tabla no se decodifica con format",
                    input.name
                )));
            }
            if input.watermark.is_some() {
                return Err(Error::Config(format!(
                    "input '{}': una tabla no declara watermark",
                    input.name
                )));
            }
            continue;
        }
        if input.kafka_topic().is_err() {
            return Err(Error::Config(format!(
                "input '{}' necesita topic o table",
                input.name
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
                if blank(input.schema.as_deref()) {
                    return Err(Error::Config(format!(
                        "input '{}': format json requiere schema",
                        input.name
                    )));
                }
            }
            crate::schema::PayloadFormat::Avro => {
                let file = !blank(input.avro_schema.as_deref());
                let arrow = !blank(input.schema.as_deref());
                if file && arrow {
                    continue;
                }
                if file || arrow {
                    return Err(Error::Config(format!(
                        "input '{}': el avro de archivo usa schema y avro_schema juntos",
                        input.name
                    )));
                }
                if registry_url(cfg).is_none() {
                    return Err(Error::Config(format!(
                        "input '{}': format avro sin archivo requiere connectors.schema_registry.url",
                        input.name
                    )));
                }
            }
        }
    }

    if table_inputs > 0 {
        if cfg.inputs.len() != 1 {
            return Err(Error::Config(
                "leer una tabla Paimon es el único input".to_string(),
            ));
        }
        if cfg.output.topic.is_none() {
            return Err(Error::Config(
                "leer una tabla Paimon publica un topic".to_string(),
            ));
        }
        if cfg
            .deployment
            .consumers_per_topic
            .is_some_and(|count| count != 1)
        {
            return Err(Error::Config(
                "leer una tabla usa un cursor, no varios consumidores".to_string(),
            ));
        }
        let warehouse = cfg
            .connectors
            .paimon
            .as_ref()
            .map(|paimon| paimon.warehouse.trim())
            .unwrap_or("");
        if warehouse.is_empty() {
            return Err(Error::Config(
                "leer una tabla requiere connectors.paimon.warehouse".to_string(),
            ));
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
    fn avro_from_a_file_uses_both_schemas() {
        let err = validate_config(&pipeline("    format: avro\n")).unwrap_err();
        assert!(err.to_string().contains("avro_schema"), "{err}");

        let ok = pipeline("    format: avro\n    avro_schema: orders.avsc\n");
        assert!(validate_config(&ok).is_ok());
    }

    #[test]
    fn avro_from_the_registry_needs_no_schema_file() {
        let cfg: PipelineConfig = serde_yaml::from_str(
            r#"
pipeline:
  name: t
connectors:
  redpanda:
    brokers: ["localhost:9092"]
  schema_registry:
    url: http://localhost:8081
inputs:
  - name: orders
    topic: orders-by-customer
    key: order_id
    format: avro
output:
  name: orders_json
  topic: orders-json
  key: order_id
deployment:
  partitions: 1
"#,
        )
        .expect("yaml");
        assert!(validate_config(&cfg).is_ok(), "{cfg:?}");
    }

    #[test]
    fn a_topic_output_does_not_need_a_warehouse() {
        let cfg: PipelineConfig = serde_yaml::from_str(
            r#"
pipeline:
  name: t
connectors:
  redpanda:
    brokers: ["localhost:9092"]
inputs:
  - name: orders
    topic: orders
    key: order_id
    schema: orders.json
output:
  name: orders_keyed
  topic: orders-by-customer
  key: order_id
deployment:
  partitions: 4
"#,
        )
        .expect("yaml");
        assert!(validate_config(&cfg).is_ok());
    }

    #[test]
    fn table_and_topic_together_are_rejected() {
        let mut cfg = pipeline("");
        cfg.output.topic = Some("orders-by-customer".to_string());
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("table y topic"), "{err}");
    }

    #[test]
    fn a_paimon_input_publishes_a_topic() {
        let cfg: PipelineConfig = serde_yaml::from_str(
            r#"
pipeline:
  name: t
connectors:
  redpanda:
    brokers: ["localhost:9092"]
  paimon:
    warehouse: ./w
inputs:
  - name: paid
    table: default.paid_orders
    key: order_id
output:
  name: paid_out
  topic: paid-topic
  key: order_id
deployment:
  partitions: 1
"#,
        )
        .expect("yaml");
        assert!(validate_config(&cfg).is_ok(), "{cfg:?}");
    }

    #[test]
    fn a_paimon_input_rejects_a_schema_file_and_a_table_output() {
        let err = validate_config(&serde_yaml::from_str(
            r#"
pipeline:
  name: t
connectors:
  redpanda:
    brokers: ["localhost:9092"]
  paimon:
    warehouse: ./w
inputs:
  - name: paid
    table: default.paid_orders
    key: order_id
    schema: paid.json
output:
  name: paid_out
  topic: paid-topic
  key: order_id
deployment:
  partitions: 1
"#,
        )
        .expect("yaml"))
        .unwrap_err();
        assert!(err.to_string().contains("schema"), "{err}");

        let err = validate_config(&serde_yaml::from_str(
            r#"
pipeline:
  name: t
connectors:
  redpanda:
    brokers: ["localhost:9092"]
  paimon:
    warehouse: ./w
inputs:
  - name: paid
    table: default.paid_orders
    key: order_id
output:
  name: paid_out
  table: default.out
  key: order_id
  bucket: 1
deployment:
  partitions: 1
"#,
        )
        .expect("yaml"))
        .unwrap_err();
        assert!(err.to_string().contains("publica un topic"), "{err}");
    }
}
