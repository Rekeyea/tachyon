# Tachyon

Motor de ejecución de pipelines streaming sobre lakehouse: lee de Redpanda
(Kafka), Kinesis o SQS, aplica SQL (DataFusion + extensiones de streaming) y
escribe en Paimon, un topic o Kinesis/SQS. Estado alineado por clave: la clave
del input es la clave del estado y el bucket de salida.

## Instalación

### Binario precompilado (recomendado)

Descarga el tarball de tu plataforma desde [Releases](https://github.com/Rekeyea/tachyon/releases):

```sh
curl -LO https://github.com/Rekeyea/tachyon/releases/latest/download/tachyon-x86_64-unknown-linux-gnu.tar.gz
tar xzf tachyon-x86_64-unknown-linux-gnu.tar.gz
sudo install -m 755 tachyon /usr/local/bin/
```

Plataformas: `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`,
`x86_64-apple-darwin`, `aarch64-apple-darwin`.

### Docker

```sh
docker build -t tachyon .
docker run --rm -v $PWD/pipeline.yaml:/app/pipeline.yaml \
  -v $PWD/pipeline.sql:/app/pipeline.sql \
  -v $PWD/schemas:/app/schemas \
  tachyon --config /app/pipeline.yaml --sql /app/pipeline.sql --schemas-dir /app/schemas
```

### Compilar desde código

Requiere un compilador C y `cmake` (librdkafka se compila desde fuente):

```sh
cargo build --release --bin tachyon
# o
cargo install --path tachyon
```

### Desde crates.io

Publicado como `tachyon` (y sus crates `tachyon-*`):

```sh
cargo install tachyon
```

## Uso

El binario lee `pipeline.yaml` (config) y `pipeline.sql` (la consulta) y corre
el pipeline hasta que se le envía Ctrl+C:

```sh
tachyon --config pipeline.yaml --sql pipeline.sql --schemas-dir schemas
```

| Flag | Descripción |
| --- | --- |
| `--config` | Ruta al `pipeline.yaml` (default: `pipeline.yaml`) |
| `--sql` | Ruta al `pipeline.sql` (default: `pipeline.sql`) |
| `--schemas-dir` | Directorio donde buscar los schemas JSON de los inputs |
| `--compact` | Compacta la tabla de salida y termina (no corre el pipeline) |
| `--compact-min-files` | Archivos mínimos por bucket para compactar (default: 8) |
| `--version` | Versión |

### Ejemplo mínimo

`pipeline.yaml`:

```yaml
pipeline:
  name: orders-etl
  version: 1

connectors:
  redpanda:
    brokers: [localhost:9092]
  paimon:
    warehouse: ./warehouse
    catalog: local

inputs:
  - name: orders
    topic: orders-topic
    key: order_id            # == state key == bucket key
    schema: orders.json      # schema Arrow; se busca en --schemas-dir

output:
  name: orders_lake
  table: default.orders_lake
  key: order_id
  bucket: 4
  sequence_field: source_version

deployment:
  partitions: 4
```

`pipeline.sql`:

```sql
INSERT INTO orders_lake
SELECT order_id, status, source_version, event_time, SUM(amount) AS order_total
FROM orders
WHERE status <> 'cancelled'
GROUP BY order_id, status, source_version, event_time;
```

El schema Arrow de cada input es un JSON con una línea por campo:

```json
[
  {"name": "order_id", "type": "Utf8", "nullable": false},
  {"name": "amount", "type": "Float64", "nullable": true}
]
```

### Métricas

Si la config define un puerto de métricas, el binario expone
`http://<addr>/metrics` en formato Prometheus.

## Arquitectura

Workspace de 8 crates:

| Crate | Rol |
| --- | --- |
| `tachyon` | Binario (CLI + wiring) |
| `tachyon-core` | Tipos base y plan de pipeline |
| `tachyon-config` | Parsing y validación de `pipeline.yaml` |
| `tachyon-sql` | Parsing de `pipeline.sql` |
| `tachyon-source` | Fuentes: Redpanda/Kafka, Kinesis, SQS |
| `tachyon-sink` | Sinks: Paimon, topic, Kinesis, SQS |
| `tachyon-runtime` | Loop consume → transform → write, ventanas, joins |
| `tachyon-metrics` | Métricas y endpoint HTTP |

Ver `DESIGN.md` y `MVP.md` para el diseño completo.

## Licencia

Apache-2.0
