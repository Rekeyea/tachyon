# Tachyon — Plan del MVP (Fase 0)

> **Estado:** Plan de implementación
> **Depende de:** `DESIGN.md` (el diseño completo)
> **Fecha:** 2026-09-23

El MVP es un **pipeline ETL stateless** de Redpanda → Paimon, escalable por particiones. Es el entregable mínimo que valida la arquitectura: SQL + config, el conector de fuente, la transformación stateless, el sink de Paimon, y el escalado por particiones. El estado (redb), el checkpointing y el fan-in por co-partitioning llegan en fases posteriores.

---

## 1. Alcance

### En el MVP (in)

- **Binario Tachyon** que carga `pipeline.sql` + `pipeline.yaml` y corre el pipeline.
- **Parsing y validación** de la config y la SQL.
- **Validación del invariante de alineación** (`inputs[*].key == output.key`, `partitions == buckets`) en compile-time.
- **Conector fuente Redpanda** (`StreamTableExec`): consumer group, particionado por clave, decodificación a Arrow (Avro y JSON).
- **Transformación stateless** en SQL: filter, project, aggregate stateless, join stateless.
- **Conector sink Paimon**: writer por bucket, sequence numbers, commit.
- **Fault tolerance básico (stateless):** commit de offsets de Redpanda + commit de Paimon. At-least-once con escrituras idempotentes (Paimon deduplica por clave + sequence number) → efectivamente exactly-once en la salida.
- **Métricas básicas:** throughput (rows/s, MB/s), consumer lag, memoria, CPU.
- **Escalado por particiones:** N instancias, cada una con un subconjunto de particiones, escribiendo a buckets disjuntos.

### Fuera del MVP (out) — llegan en fases posteriores

- **Estado (redb)** y operadores stateful (Fase 1).
- **Checkpointing** (barriers, snapshot redb, unaligned) (Fase 1).
- **Watermarks, windowing (TUMBLE/HOP/SESSION), state TTL** (Fase 1).
- **Fan-in por co-partitioning** (Fase 3).
- **Lance, vector search** (Fase 4).
- **Stream-to-core pinning** (optimización, post-MVP).
- **Operator de Kubernetes** (post-MVP; el MVP se despliega como contenedor simple).

---

## 2. El pipeline del MVP (ejemplo concreto)

**`examples/orders-etl/pipeline.sql`:**
```sql
-- ETL stateless: filtra, proyecta y agrega por pedido
INSERT INTO orders_lake
SELECT
    order_id,
    status,
    source_version,
    event_time,
    SUM(amount) AS order_total
FROM orders
WHERE status <> 'cancelled'
GROUP BY order_id, status, source_version, event_time;
```

**`examples/orders-etl/pipeline.yaml`:**
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
    key: order_id
    schema: avro/orders-v1
output:
  name: orders_lake
  table: default.orders_lake
  key: order_id
  bucket: 4
  sequence.field: source_version
deployment:
  partitions: 4
```

**Nota:** en el MVP, `GROUP BY` sin watermark es una agregación stateless (agrega todo el batch recibido). Para agregaciones con ventana temporal (que requieren estado + watermark) se espera la Fase 1. El ejemplo muestra la forma; el MVP la ejecuta como agregación por clave sobre el batch.

---

## 3. Criterios de éxito (definition of done)

El MVP está completo cuando:

1. **End-to-end:** un pipeline stateless (Redpanda → Paimon) corre de punta a punta y produce la tabla Paimon correcta.
2. **Validación:** una config/SQL inválida (invariante de alineación roto) se rechaza con un error claro antes de correr.
3. **Escalado:** N instancias (N = 1, 2, 4) corren el mismo pipeline, cada una con un subconjunto de particiones, y la salida Paimon es correcta y sin conflictos.
4. **Recuperación:** al matar una instancia y re-iniciarla, re-consume desde el último offset commitado y no duplica ni pierde eventos en la salida (idempotencia por sequence number).
5. **Footprint:** las métricas (throughput, lag, memoria, CPU) se exponen (endpoint HTTP / OpenTelemetry) y son medibles por instancia.
6. **Sin JVM:** el binario es Rust estático, pequeño, con startup rápido.

---

## 4. Slices verticales (orden de construcción)

Cada slice es un corte vertical independiente y testeable. Se construyen en este orden; cada uno deja el proyecto en un estado compilable y testeable.

### Slice 1 — Config + SQL + validación
- Parsear `pipeline.yaml` (schema de config).
- Parsear `pipeline.sql` (extraer la query de DataFusion; ignorar por ahora las extensiones de streaming).
- Validar el invariante de alineación.
- **Test:** config + SQL válidos → plan validado. Inválidos → error claro.

### Slice 2 — Conector fuente Redpanda
- Implementar `StreamTableExec` sobre rdkafka / cliente nativo de Redpanda.
- Consumer group, particionado por clave, asignación de particiones.
- Decodificación Avro/JSON → `RecordBatch` de Arrow.
- **Test:** consumir de un topic de prueba (Redpanda local) y producir `RecordBatch` correctos.

### Slice 3 — Transformación stateless
- Conectar la fuente Redpanda a DataFusion.
- Correr la query stateless (filter, project, aggregate, join).
- **Test:** dado input de prueba, la salida transformada es correcta.

### Slice 4 — Conector sink Paimon
- Implementar el writer de Paimon (writer por bucket, sequence numbers, commit).
- Escribir la salida transformada a Paimon.
- **Test:** escribir a una tabla Paimon de prueba y verificar los datos.

### Slice 5 — End-to-end + métricas + escalado
- Cablear todo: Redpanda → transformación → Paimon.
- Commit de offsets + commit de Paimon (fault tolerance stateless).
- Métricas (throughput, lag, memoria, CPU) vía endpoint HTTP / OpenTelemetry.
- Escalado por particiones (N instancias).
- **Test:** pipeline end-to-end correcto; métricas expuestas; escalado por particiones funciona; recuperación ante fallo.

---

## 5. Arquitectura de código (workspace Rust)

```
tachyon/
├── Cargo.toml                 # workspace
├── crates/
│   ├── tachyon-core/          # tipos base, plan de pipeline, integración Arrow
│   │   └── src/{lib,plan,types,error}.rs
│   ├── tachyon-config/        # parsing + validación de pipeline.yaml
│   │   └── src/{lib,schema,validate}.rs
│   ├── tachyon-sql/           # parsing de pipeline.sql (DataFusion SQL)
│   │   └── src/{lib,parse}.rs
│   ├── tachyon-source/        # conector fuente Redpanda (StreamTableExec)
│   │   └── src/{lib,consumer,decode}.rs
│   ├── tachyon-sink/          # conector sink Paimon (writer por bucket)
│   │   └── src/{lib,writer}.rs
│   ├── tachyon-runtime/       # runtime de ejecución (loop: consume→transform→write)
│   │   └── src/{lib,runtime}.rs
│   └── tachyon-metrics/       # métricas (OpenTelemetry)
│       └── src/lib.rs
├── tachyon/                   # binario
│   └── src/main.rs            # CLI: carga config + SQL, corre el pipeline
└── examples/
    └── orders-etl/            # el pipeline del MVP
        ├── pipeline.sql
        └── pipeline.yaml
```

**Dependencias entre crates:**
```
tachyon (bin)
  └─► tachyon-runtime
        ├─► tachyon-source  (Redpanda → Arrow)
        ├─► tachyon-sink    (Arrow → Paimon)
        ├─► tachyon-sql     (plan de la query)
        ├─► tachyon-config  (config validada)
        ├─► tachyon-metrics
        └─► tachyon-core    (tipos, plan)
```

**Dependencias externas clave (MVP):**
- `datafusion` — motor de cómputo + `StreamTableExec`.
- `arrow` — formato in-memory.
- `rdkafka` (o `redpanda-rs`) — cliente de Redpanda.
- `paimon` (`paimon-rs`) — writer de Paimon.
- `apache-avro` — decodificación Avro.
- `serde` + `serde_yaml` — parsing de config.
- `tokio` — runtime async.
- `opentelemetry` + `opentelemetry-otlp` — métricas.
- `clap` — CLI.

---

## 6. Riesgos del MVP y mitigación

| Riesgo | Mitigación |
|---|---|
| `paimon-rs` inmaduro para escritura (solo lectura/vector search documentado) | Verificar capacidades de escritura de `paimon-rs` **antes** de empezar el Slice 4. Si no soporta escritura, evaluar `paimon` vía Flink/Spark como sink externo, o contribuir a `paimon-rs`. |
| Conector Redpanda en Rust (no existe off-the-shelf) | `rdkafka` es maduro; `StreamTableExec` es el patrón. Slice 2 es autocontenido y testeable. |
| Decodificación Avro → Arrow | `apache-avro` + `arrow` son estables; la conversión es el trabajo. |
| Escalado por particiones con writer por bucket | Alinear `partitions == buckets`; testear con N instancias en el Slice 5. |
| Exactly-once stateless | At-least-once + idempotencia por sequence number (Paimon deduplica). Suficiente para el MVP. |

**Gating:** el riesgo #1 (`paimon-rs` escritura) es el más importante. **Validarlo en el Slice 0.5** (un spike de 1 día: escribir a una tabla Paimon local con `paimon-rs`) antes de comprometer el resto del MVP.

---

## 7. Slice 0.5 — Spike de validación (hacer primero)

Antes de construir el MVP, un spike de ~1 día para de-riskar lo crítico:

1. **Escribir a Paimon con `paimon-rs`:** crear una tabla PK local y escribir records con el writer de `paimon-rs`. Confirmar que soporta escritura (no solo lectura/vector search).
2. **Consumir de Redpanda con `rdkafka` + `StreamTableExec`:** consumir un topic y producir `RecordBatch` de Arrow.
3. **Correr una query stateless de DataFusion sobre esos `RecordBatch`:** confirmar el wiring fuente→DataFusion.

Si el spike #1 falla (Paimon no soporta escritura en Rust), se re-evalúa el sink (ver Riesgos). El spike #2 y #3 validan el camino de datos.

**Salida del spike:** un `examples/spike/` con el código mínimo que demuestra los tres puntos, y una conclusión go/no-go para el sink de Paimon.
