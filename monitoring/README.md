# Observabilidad de Tachyon (OpenTelemetry + Grafana)

Stack local para ver las métricas de los pipelines: el binario exporta
métricas **OTLP** (HTTP) al OpenTelemetry Collector, el collector las publica
en su exporter **Prometheus**, **Prometheus** las scrapea y **Grafana** las
grafica con el dashboard "Tachyon" (provisionado automáticamente).

```
tachyon (binario)                otlp://4318      otel-collector  :8889      prometheus :9090      grafana :3000
  SdkMeterProvider ─────────────────────────────►  (exporter prometheus) ──►  (scrape) ──────────►  (dashboard)
```

## Arranque

```sh
cd monitoring
docker compose up -d
```

| Servicio        | URL                          |
| --------------- | ---------------------------- |
| Grafana         | http://localhost:3000 (admin/admin) |
| Prometheus      | http://localhost:9090        |
| OTLP/HTTP       | http://localhost:4318        |

Para apagarlo: `docker compose down`.

## Configurar el pipeline

En el YAML del pipeline, añade `otlp_endpoint` bajo `deployment.metrics`:

```yaml
deployment:
  metrics:
    bind_addr: 0.0.0.0:9090        # endpoint /metrics nativo (opcional)
    otlp_endpoint: http://otel-collector:4318
    otlp_interval: 10s             # default: 10s
    # service_name: mi-pipeline    # default: nombre del pipeline
```

- `otlp_endpoint`: URL base del collector. Si el pipeline corre en el host
  (fuera de docker), usa `http://localhost:4318`; si corre en la misma red de
  docker, `http://otel-collector:4318`.
- `service_name`: se exporta como `service.name` del recurso OTel; el
  exporter Prometheus del collector lo convierte en la label `exported_job`
  (para distinguir pipelines). La identidad de la instancia
  (`service.instance.id`) llega como la label `exported_instance`.
- `bind_addr` y `otlp_endpoint` son independientes: puedes usar solo el
  endpoint `/metrics` nativo (formato Prometheus, scrapeable directamente),
  solo OTLP, o ambos.

## Métricas

Vía OTLP (collector), los counters salen con sufijo `_total` y el gauge sin
sufijo:

| Métrica Prometheus              | Tipo    | Descripción                                  |
| ------------------------------- | ------- | -------------------------------------------- |
| `tachyon_rows_read_total`       | counter | Filas leídas de la fuente (acumulado)        |
| `tachyon_rows_written_total`    | counter | Filas escritas al sink (acumulado)           |
| `tachyon_commits_total`         | counter | Commits de sink completados (acumulado)      |
| `tachyon_consumer_lag`          | gauge   | Offsets no consumidos                        |
| `tachyon_errors_total`          | counter | Errores de ejecución (acumulado)             |
| `tachyon_stage_source_next_ns_total` | counter | Tiempo en `stream.next()` (ns, acumulado) |
| `tachyon_stage_send_wait_ns_total`   | counter | Tiempo bloqueado enviando al writer (ns)  |
| `tachyon_stage_write_ns_total`       | counter | Tiempo en `sink.write()` (ns)              |
| `tachyon_stage_commit_ns_total`      | counter | Tiempo en el checkpoint del sink (ns)      |

En OTel los nombres llevan puntos (`tachyon.rows.read`, etc.); el exporter
Prometheus del collector los traduce a guiones bajos y añade `_total` a los
counters. El endpoint `/metrics` nativo expone los mismos contadores sin el
sufijo (los nombres son estables para el harness de bench).

## Verificación rápida

```sh
# Series en Prometheus:
curl -s 'http://localhost:9090/api/v1/series?match[]=tachyon_*' | head

# Health de Grafana:
curl -s http://localhost:3000/api/health
```

## Archivos

- `docker-compose.yml`: los tres servicios.
- `otel-collector/config.yaml`: pipeline OTLP → exporter Prometheus (mapea
  `service.instance.id` → label `instance`).
- `prometheus/prometheus.yml`: scrape del collector (y del binario, opcional).
- `grafana/provisioning/`: datasource Prometheus + carpeta de dashboards.
- `grafana/dashboards/tachyon.json`: dashboard "Tachyon" (throughput, lag,
  desglose de etapas, errores, commits).
