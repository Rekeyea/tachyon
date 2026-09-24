# Tachyon — Motor de Ejecución de Pipelines Streaming sobre Lakehouse

> **Estado:** Borrador de diseño (v0.1)
> **Nombre de trabajo:** Tachyon (pendiente de confirmar)
> **Fecha:** 2026-09-23

---

## 1. Resumen (TL;DR)

Tachyon es un motor de ejecución de pipelines de streaming basado en lakehouse. El usuario escribe **toda la lógica de transformación en SQL** y proporciona **un archivo de configuración** que define los streams de entrada (temas de Redpanda), la salida (tabla Paimon) y los parámetros de despliegue. El motor compila la SQL a un plan de ejecución sobre **Apache DataFusion**, corre el pipeline como una instancia autocontenida (un binario Rust, sin JVM), y escribe el resultado en **Apache Paimon** como el formato de tabla analítica.

**Primitivas centrales:**

| Capa | Tecnología | Rol |
|---|---|---|
| Formato de tabla analítica | **Apache Paimon** | Lakehouse unificado streaming+batch, LSM-tree, changelog, CDC, ACID |
| Formato de archivo ML/vector | **Apache Lance** | Formato columnar para random access, vector search, ML (formato de archivo opcional de Paimon) |
| In-memory / intercambio | **Apache Arrow** | Formato columnar in-memory; cero copia entre capas |
| Motor de cómputo | **Apache DataFusion** | Planificador SQL + motor de ejecución vectorizado, multithread, columnar, con modo `Unbounded` (streaming) |
| Motor de eventos | **Redpanda** | Broker Kafka-compatible, C++, thread-per-core, baja latencia, footprint reducido |

**Propiedades que el motor debe cumplir:**

1. **SQL-first:** el usuario escribe solo SQL + un archivo de configuración. No escribe código de pipeline.
2. **Footprint medible:** cada instancia de pipeline expone métricas de CPU, memoria, I/O, throughput y latencia. El stack Rust/C++ (sin JVM) mantiene el footprint bajo.
3. **Una salida por pipeline:** cada instancia de Tachyon produce un único stream de salida (una tabla Paimon), aunque puede consumir múltiples streams de entrada.
4. **Escalado horizontal:** si un nodo no puede con el flujo, se escala horizontalmente particionando la entrada en Redpanda. Un solo writer por bucket de Paimon.
5. **Clave única de particionado:** una misma clave `K` a través de Redpanda (partition key), el estado del procesador (state key), el operador de fusión (merge key) y Paimon (bucket key).
6. **No distribuido:** cada instancia de Tachyon es autocontenida (un proceso). No hay coordinación entre instancias para un pipeline. Para soportar un stream demasiado grande, se particiona y cada partición se maneja por una instancia independiente (claves disjuntas). El fan-in se resuelve con co-partitioning de Redpanda, no con un shuffle cruza-nodo.

---

## 2. Objetivos y No-Objetivos

### Objetivos

- **O1.** SQL como única interface de transformación (DataFusion SQL + extensiones de streaming).
- **O2.** Un archivo de configuración que defina inputs (Redpanda), output (Paimon) y despliegue.
- **O3.** Footprint medible y bajo (Rust, sin JVM, embebible).
- **O4.** Una salida por pipeline; escalado horizontal por particiones de Redpanda.
- **O5.** Un solo writer por bucket de Paimon (corrección + sin conflictos de escritura).
- **O6.** Operadores stateless y stateful.
- **O7.** Correct semánticas de streaming: watermarks, windowing, estado, checkpointing, exactly-once.
- **O8.** Soporte de Paimon (analítico), Lance (ML/vector), Arrow (in-memory), DataFusion (cómputo), Redpanda (eventos).
- **O9.** **Configuración opinionated:** el usuario especifica solo lo esencial (identidad, I/O, clave de particionado). El resto tiene defaults. Un único backend de estado (`redb`), sin opciones alternativas.

### No-Objetivos (por ahora)

- **N1.** No es un motor de batch general (DataFusion ya cubre batch; Tachyon es el motor de streaming).
- **N2.** No reemplaza a Flink como plataforma de stream processing general; es un motor lean, embebible, por-pipeline.
- **N3.** No incluye un gestor de metadatos/catalog propio (usa el catalog de Paimon).
- **N4.** No hace serving/consulta ad-hoc sobre el lakehouse (eso lo hace un motor de consulta tipo DataFusion batch, StarRocks, Trino, etc.).
- **N5.** No es un sistema distribuido (no distribuye un pipeline sobre nodos coordinados como Flink). Cada instancia es autocontenida; el paralelismo es por partición de Redpanda entre instancias independientes con claves disjuntas.

---

## 3. Modelo de Usuario: SQL + Configuración

Esta es la decisión de diseño central. El usuario interactúa con Tachyon a través de **dos artefactos**:

1. **Un archivo SQL** (`pipeline.sql`) que contiene **toda la lógica de transformación**.
2. **Un archivo de configuración** (`pipeline.yaml`) que define **los streams de entrada, la salida y el despliegue**.

La SQL referencia **streams lógicos por nombre** (p. ej. `orders`, `items`, `orders_lake`). La configuración **vincula esos nombres lógicos a recursos físicos** (temas de Redpanda, tablas Paimon) y define el particionado, el despliegue y el escalado.

### 3.1 El archivo SQL

El usuario escribe solo la transformación. La SQL usa DataFusion SQL (estándar) más un conjunto pequeño de **extensiones de streaming** (watermarks, windowing, state TTL).

```sql
-- pipeline.sql
-- Lógica de transformación. Solo SQL.

-- (Opcional) Vista intermedia
CREATE STREAM VIEW enriched_orders AS
SELECT
    o.order_id,
    o.status,
    o.source_version,
    o.event_time,
    SUM(i.amount)               AS order_total,
    COUNT(i.item_id)            AS item_count
FROM orders AS o
JOIN items  AS i ON o.order_id = i.order_id
GROUP BY o.order_id, o.status, o.source_version, o.event_time;

-- Salida: inserción continua en el stream lógico de salida
INSERT INTO orders_lake
SELECT
    order_id,
    status,
    source_version,
    event_time,
    order_total,
    item_count
FROM enriched_orders;
```

**Extensiones de streaming que soporta la SQL** (sobre DataFusion SQL):

| Extensión | Sintaxis (propuesta) | Mapea a |
|---|---|---|
| Watermark | `WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND` (en el `CREATE SOURCE` lógico o en config) | Tracking de late data |
| Tumbling window | `TUMBLE(event_time, INTERVAL '1' MINUTE)` | Operador de agregación con ventana |
| Sliding window | `HOP(event_time, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)` | Operador de ventana deslizante |
| Session window | `SESSION(event_time, INTERVAL '30' MINUTE)` | Operador de ventana de sesión |
| State TTL | `STATE_TTL = INTERVAL '7' DAY` (opción de tabla/consulta) | Expiración de estado |
| Continuous query | `INSERT INTO <out> SELECT ...` (implícitamente continua) | Pipeline de streaming |

> **Nota de diseño:** DataFusion ya soporta funciones de ventana estándar (`OVER (PARTITION BY ... ORDER BY ...)`). Para **agregaciones con ventana temporal** (tumbling/sliding/session) se añaden funciones `TUMBLE`/`HOP`/`SESSION` que el planificador de Tachyon traduce a operadores de streaming custom. Esto es consistente con la sintaxis de Flink SQL, familiar para la comunidad.

### 3.2 El archivo de configuración

Define los streams de entrada, la salida y el despliegue. **Aquí vive el particionado y la alineación de claves.**

```yaml
# pipeline.yaml
pipeline:
  name: orders-enrichment
  version: 1

# --- Conexiones físicas ---
connectors:
  redpanda:
    brokers: [redpanda-0:9092, redpanda-1:9092]
    security:
      sasl_mechanism: SCRAM-SHA-256
      username: ${REDPANDA_USER}
      password: ${REDPANDA_PASSWORD}
  paimon:
    warehouse: s3://lakehouse/warehouse
    catalog: rest
    catalog_uri: http://paimon-catalog:8080

# --- Streams de entrada (vinculan nombre lógico -> tema físico) ---
inputs:
  - name: orders                # nombre lógico referenciado en la SQL
    topic: orders-topic
    key: order_id               # CLAVE de particionado (== state key == bucket key)
    schema: avro/orders-v1
    watermark:
      column: event_time
      lag: 5s
  - name: items
    topic: items-topic
    key: order_id               # misma clave -> alineado para co-ubicar en el join
    schema: avro/items-v1

# --- Salida (vincula nombre lógico -> tabla Paimon) ---
output:
  name: orders_lake             # nombre lógico referenciado en la SQL
  table: warehouse.dbo.orders_lake
  key: order_id                 # == partition key (alineada)
  bucket: 8                     # == número de particiones (alineado)
  sequence.field: source_version
  rowkind.field: op             # si el stream es CDC (+I/-U/+U/-D)

# --- Despliegue y escalado ---
deployment:
  partitions: 8                 # == buckets == writers (alineado)
  resources:
    cpu: "2"
    memory: 4Gi
  scaling:
    strategy: partitions        # escalar agregando particiones + buckets + nodos
    min: 1
    max: 16
  state:
    checkpoint_interval: 10s    # redb es el único backend (opinionated); no hay opción que elegir
    checkpoint_storage: s3://lakehouse/checkpoints/orders-enrichment
```

> **Opinionated defaults:** el usuario solo *debe* especificar lo esencial: `connectors` (Redpanda + Paimon), `inputs` (los streams de entrada), `output` (la tabla y la clave) y `deployment.partitions`. El resto (`resources`, `scaling`, `state.checkpoint_*`) tiene defaults sensatos y solo se sobreescribe si hace falta. No hay opciones de backend de estado, de formato de checkpoint ni de estrategia de asignación de particiones: Tachyon las decide.

### 3.3 La regla de alineación (invariante clave)

La configuración debe satisfacer el **invariante de alineación**:

```
inputs[*].key  ==  output.key  ==  state key  ==  Paimon bucket key
deployment.partitions  ==  output.bucket  ==  número de writers
```

Tachyon **valida este invariante en el compile-time** y rechaza el pipeline si no se cumple. Esto garantiza:
- Corrección del estado (co-ubicado por clave).
- Sin conflictos de escritura en Paimon (un writer por bucket).
- Escalado consistente (particiones, buckets y nodos escalan juntos).

### 3.4 Modelo de despliegue

- **Un pipeline = una unidad de despliegue.** Un binario Tachyon autocontenido que consume la entrada, procesa y escribe a Paimon.
- **Un nodo = una instancia de Tachyon** que posee un subconjunto de particiones del pipeline.
- **Escalar** = agregar nodos que toman particiones adicionales (rebalanceo de consumer group de Redpanda) + buckets adicionales de Paimon.
- Despliegue típico: contenedor por nodo, orquestado en Kubernetes (DaemonSet/Deployment o un operator propio).

### 3.5 Sintaxis de las extensiones de streaming

La SQL usa DataFusion SQL (estándar) más un conjunto de extensiones de streaming, con sintaxis **compatible con Flink SQL** (familiar para la comunidad). El schema y el binding físico de cada stream vienen del config; la SQL declara la semántica de streaming y la transformación.

**Extensiones:**

| Extensión | Sintaxis | Mapea a |
|---|---|---|
| Declarar stream lógico | `CREATE STREAM <name> (<cols>) WITH (...)` | Binding lógico (físico en config) |
| Watermark | `WATERMARK FOR <col> AS <expr>` | Tracking de late data |
| Tumbling window | `TABLE(TUMBLE(TABLE <s>, DESCRIPTOR(<t>), INTERVAL '<size>'))` | Agregación con ventana fija |
| Sliding window | `TABLE(HOP(TABLE <s>, DESCRIPTOR(<t>), INTERVAL '<slide>', INTERVAL '<size>'))` | Ventana deslizante |
| Session window | `TABLE(SESSION(TABLE <s>, DESCRIPTOR(<t>), INTERVAL '<gap>'))` | Ventana de sesión |
| State TTL | `STATE_TTL = INTERVAL '<dur>'` (propiedad de stream) | Expiración de estado |
| Continuous query | `INSERT INTO <out> SELECT ...` | Pipeline de streaming (implícitamente continuo) |

**Ejemplo completo:**

```sql
-- pipeline.sql

-- 1. Streams lógicos con semántica de streaming
CREATE STREAM orders (
    order_id       BIGINT,
    status         STRING,
    source_version BIGINT,
    event_time     TIMESTAMP
) WITH (
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 2. Transformación: agregación con tumbling window de 1 minuto
INSERT INTO orders_lake
SELECT
    window_start,
    window_end,
    order_id,
    SUM(amount)  AS order_total,
    COUNT(*)     AS event_count
FROM TABLE(
    TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, order_id;
```

**Notas de diseño:**
- **Event time vs processing time:** las ventanas operan sobre event time por defecto (si hay watermark). Para processing time se usa `PROCTIME()`.
- **`window_start` / `window_end`:** las Windowing TVFs (`TUMBLE`/`HOP`/`SESSION`) producen estas dos columnas, que se usan en el `GROUP BY`.
- **El planificador de Tachyon** traduce las Windowing TVFs a operadores de streaming custom (ver §6.2). Las funciones de ventana estándar de DataFusion (`OVER (...)`) siguen disponibles para casos no-temporales.
- **Validación en compile-time:** el schema de `CREATE STREAM` debe coincidir con el schema referenciado en el config; el invariante de alineación (§3.3) se valida también aquí.

---

## 4. Arquitectura

### 4.1 Vista de componentes

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          INSTANCIA TACHYON (1 pipeline)                 │
│                                                                          │
│  ┌─────────────┐     ┌──────────────────────────────────────────────┐   │
│  │  pipeline.sql│    │           CAPA DE CÓMPUTO (DataFusion)        │   │
│  │  (SQL)      │────►│  Planificador SQL + optimizadores             │   │
│  └─────────────┘     │  Motor de ejecución vectorizado (Arrow)       │   │
│                      │  Modo Unbounded (streaming)                   │   │
│  ┌─────────────┐     └──────────────────────────────────────────────┘   │
│  │ pipeline.yaml│                      │                               │
│  │  (config)   │────►  ┌────────────────────────────────────────────┐  │
│  └─────────────┘       │   CAPA DE SEMÁNTICAS DE STREAMING (lo que  │  │
│                        │   Tachyon construye sobre DataFusion)       │  │
│                        │  • Watermarks (late data)                   │  │
│                        │  • Estado + checkpointing/restauración      │  │
│                        │  • Windowing (tumbling/sliding/session)     │  │
│                        │  • Operadores stateful (agregación, join)   │  │
│                        │  • Backpressure                             │  │
│                        └────────────────────────────────────────────┘  │
│                                        │                               │
│              ┌─────────────────────────┼─────────────────────────┐     │
│              ▼                         ▼                         ▼     │
│   ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│   │ CONNECTOR FUENTE │      │ OPERADOR DE FUSIÓN│      │ CONNECTOR SINK   │
│   │ Redpanda (source)│      │ (key-aware)       │      │ Paimon (writer)  │
│   │ StreamTableExec  │      │ • union disjunto  │      │ • 1 writer/bucket│
│   │ • consumer group │      │   (1 entrada)     │      │ • sequence num   │
│   │ • offsets        │      │ • streaming shuffle│     │ • CDC rowkind    │
│   │ • Arrow IPC      │      │   (fan-in)        │      │ • commit/compact │
│   └──────────────────┘      └──────────────────┘      └──────────────────┘
│              │                         │                         │     │
└──────────────┼─────────────────────────┼─────────────────────────┼─────┘
               ▼                         ▼                         ▼
        ┌─────────────┐           (re-particion          ┌─────────────┐
        │  REDPANDA   │            por clave K)          │   PAIMON    │
        │  (broker)   │            ────────────────►     │  (warehouse)│
        │ topics/part.│                                  │  buckets    │
        └─────────────┘                                  └─────────────┘
```

### 4.2 Capas

**Capa de cómputo (se hereda de DataFusion):**
- Planificador SQL, optimizadores lógicos y físicos.
- Motor de ejecución vectorizado sobre Arrow, multithread.
- Modo `Unbounded` + `StreamTableExec` (fuentes de streaming).
- `SymmetricHashJoinExec` (streaming joins).
- Partitioning por core.

**Capa de semánticas de streaming (lo que Tachyon construye):**
- **Watermarks:** tracking de late data. Requiere convencer a los optimizadores de DataFusion de no dropear la columna de timestamp.
- **Estado + checkpointing:** estado en memoria (default) o KV embebido `redb` (puro Rust, single-file) para estado grande; snapshots Chandy–Lamport + restauración. Ver §6.4.
- **Windowing:** operadores custom para tumbling/sliding/session.
- **Operadores stateful:** agregaciones con estado, joins stateful.
- **Backpressure:** control de flujo entre operadores (DataFusion pull-based tiene poco control; se añade).

**Conectores:**
- **Fuente Redpanda:** implementa `StreamTableExec` sobre rdkafka/cliente nativo de Redpanda. Consume por consumer group, maneja offsets, decodifica a Arrow (Arrow IPC o decodificación Avro/JSON/Arrow).
- **Sink Paimon:** writer por bucket sobre `paimon-rs`. Gestiona sequence numbers, CDC rowkind, commits y compaction.

**Operador de fusión (key-aware):**
- **1 entrada alineada:** union disjunto (barato). Cada partición ya tiene claves disjuntas.
- **Fan-in (varias entradas):** merge local por co-partitioning de Redpanda (ver §5.4). Como todas las entradas están co-particionadas por `K`, la fusión es un union disjunto local, no un shuffle cruza-nodo.

### 4.3 Flujo de datos

1. El nodo consume particiones de Redpanda (consumer group). Cada partición → un subconjunto disjunto de claves `K`.
2. Los eventos se decodifican a `RecordBatch` de Arrow.
3. DataFusion ejecuta el plan (operadores stateless y stateful). El estado está co-ubicado por `K`.
4. El operador de fusión re-particiona por `K` (union disjunto o streaming shuffle) y enruta cada registro a su bucket (`hash(K) % num_buckets`).
5. Un writer por bucket escribe a Paimon, propagando el sequence number por clave.
6. Paimon aplica el merge engine (deduplicate/partial-update/aggregation) y compacta el LSM.

---

## 5. La Clave Única de Particionado

Principio fundamental: **una sola clave `K` a través de todo el pipeline.**

```
Redpanda partition key  ==  state key  ==  merge key  ==  Paimon bucket key
```

### 5.1 Por qué

- **Corrección del estado:** todos los eventos de una misma `K` caen en la misma partición → mismo nodo → mismo estado.
- **Sin conflictos de escritura:** cada `K` mapea a un bucket; un solo writer por bucket.
- **Escalado consistente:** particiones, buckets y nodos escalan juntos.

### 5.2 Alineación de conteos

```
num Redpanda partitions  ==  num Paimon buckets  ==  num writers
```

- Paimon: por defecto una tabla tiene **un solo bucket** (paralelismo único de lectura/escritura). Se configura `bucket = N` para N buckets. El bucket es la **unidad más pequeña de paralelismo** de Paimon.
- Redpanda: una partición se asigna a un solo consumidor a la vez. El número de particiones limita el paralelismo de consumo.
- Alineando ambos, cada partición ↔ bucket ↔ writer.

### 5.3 Enrutado por bucket (decoupling)

El operador de fusión calcula `hash(K) % num_buckets` y enruta cada registro al writer de ese bucket. Esto **desacopla** el particionado de Redpanda del bucketing de Paimon: no se depende de que ambos hash sean idénticos. El enrutado lo hace el operador de fusión.

### 5.4 Fan-in por co-partitioning (no shuffle cruza-nodo)

Como Tachyon no es distribuido, el fan-in (múltiples entradas → una salida) **no usa un streaming shuffle cruza-nodo**. Lo resuelve el **co-partitioning de Redpanda**:

- **Precondición:** todos los topics de entrada están particionados por la misma clave `K`, con el mismo número de particiones y hash consistente.
- Entonces, los eventos de una misma `K` caen en la **misma partición en todos los topics** (co-partitioning).
- Una instancia que consume la partición `i` de cada topic recibe todos los eventos de las mismas claves de todos los topics.
- La fusión es **local** (dentro de la instancia): un union disjunto, porque las claves ya están alineadas y disjuntas por partición.

**Costo del modelo no-distribuido:** las entradas deben llegar co-particionadas por `K`. Si un stream upstream no particiona por `K`, se necesita una etapa de re-particionado (un pipeline Tachyon o una operación de Redpanda) antes de que entre al pipeline.

---

## 6. Semánticas de Streaming (lo que se construye sobre DataFusion)

### 6.1 Lo que se hereda de DataFusion

| Se hereda | Detalle |
|---|---|
| Motor de ejecución | Vectorizado, multithread, columnar (Arrow) |
| Modo `Unbounded` | La mayoría de operadores físicos soporta ejecución no acotada |
| `StreamTableExec` | Fuente de streaming (Redpanda) |
| Streaming joins | `SymmetricHashJoinExec` sobre streams unbounded |
| Planificador SQL | + optimizadores |
| Partitioning | Por core (se extiende a particiones lógicas) |

### 6.2 Lo que se construye

| Se construye | Complejidad | Notas |
|---|---|---|
| Watermarks | Media | Convencer a los optimizadores de no dropear el timestamp |
| Estado + checkpointing | **Alta** | Estado en memoria (default) o `redb` + snapshots Chandy–Lamport + restauración. Lo más difícil. Ver §6.4. |
| Windowing (tumbling/sliding/session) | Media-Alta | Operadores custom |
| Operadores stateful (agregación, join) | Media-Alta | Algunos operadores de DataFusion son `PipelineBreaking` (memoria unbounded) y no sirven para streaming; hay que implementar los propios |
| Backpressure | Media | DataFusion pull-based tiene poco control; añadir buffers adaptativos |
| Merge local (fan-in) | **Baja** | Por co-partitioning de Redpanda (§5.4): union disjunto local, no shuffle cruza-nodo |

### 6.3 Prior art de referencia

- **Arroyo:** usa DataFusion para parsing SQL y plan lógico; runtime y conectores propios. Referencia para la capa de streaming.
- **Denormalized:** "DuckDB for streaming"; operadores custom para windowing y agregación stateful sobre DataFusion. Referencia para operadores.
- **StreamFusion:** acelerador Rust + Arrow/DataFusion para Flink SQL. Referencia para usar DataFusion como acelerador de cómputo.

### 6.4 Backend de estado

**Decisión opinionated: `redb` es el único backend de estado.** No hay opciones alternativas ni enfoque por niveles. Un solo backend → un solo mecanismo de checkpoint → mental model simple y configuración mínima.

**Por qué `redb`:**
- KV embebido, **puro Rust**, single-file, MVCC, B+trees copy-on-write, ACID.
- Rápido incluso para estado pequeño (CoW B+trees); no penaliza los workloads acotados.
- Checkpoint limpio: el CoW permite snapshot/copy consistente del archivo (Chandy–Lamport).
- Sin dependencia C, sin asociación a Flink (RocksDB), sin librería relacional (SQLite).

**Por qué no un enfoque por niveles (memoria → redb):**
- Un solo backend elimina una bifurcación de código (serialización en memoria vs. redb) y queda un solo mecanismo de checkpoint.
- `redb` es ligero y puro Rust; el overhead para estado pequeño es despreciable.
- Simplifica la configuración: no hay `state.backend` que elegir.

**Considerados y descartados:**
- **RocksDB:** más pesado, asociación a Flink.
- **SQLite:** relacional, no KV; resta naturalidad para el patrón de acceso keyed.
- **LMDB:** más rápido, pero binding C (no puro Rust).
- **Paimon como estado en el hot path:** no; Paimon está optimizado para escrituras de lakehouse, no para point read/update de alta frecuencia (LSM + compaction). Sirve para tablas de dimensión (lookup joins), no para estado por evento.

---

## 7. Integración con Paimon

### 7.1 Writer por bucket

- Modelo de concurrencia de Paimon: **un writer por bucket**.
- Tachyon mantiene **un writer por bucket** por nodo. El operador de fusión enruta cada `K` a su bucket.
- Múltiples nodos escriben a buckets **disjuntos** de la misma tabla → sin conflictos.

### 7.2 Orden y sequence numbers

- Paimon ordena actualizaciones dentro de un bucket por **sequence number**.
- Por defecto asigna sequence numbers internos. Si las actualizaciones pueden llegar out-of-order (late data, múltiples writers), se configura `sequence.field` a un campo de versión/timestamp de la fuente.
- **Tachyon propaga un sequence monotónico por clave** (offset de Redpanda dentro de la partición, o timestamp de fuente) y lo mapea a `sequence.field`.
- **CDC:** `rowkind.field` codifica `+I`/`-U`/`+U`/`-D`.
- **Múltiples writers que compiten:** `sequence.snapshot-ordering = true` ordena por commit snapshot ID (exige `write-only = true` + job de compaction dedicado).

### 7.3 Compaction

- El LSM de Paimon acumula sorted runs; se compactan para mantener la eficiencia de lectura.
- Opciones: compaction inline (el writer compacta) o **dedicated compaction job** (un job separado compacta). Para writers de alto throughput, el dedicated compaction job libera el writer de ese costo.
- Footprint: la compaction es una fuente de overhead de I/O; se mide y se puede separar.

### 7.4 Formato de archivo: Parquet vs Lance

- **Analytics SQL clásico → Parquet** (formato natural de Paimon).
- **ML / vector search / random access → Lance** (formato de archivo de Paimon optimizado para ello).
- Se decide **por tabla** según el caso de uso. El soporte de Lance como formato de archivo de Paimon debe verificarse por madurez (ver Riesgos).

---

## 8. Integración con Redpanda

### 8.1 Consumo por consumer group

- Cada pipeline usa un **consumer group** en Redpanda.
- El consumer group reparte particiones entre nodos (consumer assignment: range/round-robin/sticky).
- **Static group membership** (`group.instance.id`): un nodo mantiene sus particiones tras un restart sin rebuild del estado. Crucial para apps stateful.

### 8.2 Particionado por clave

- El producer particiona por `K` (default hash partitioner). Todos los eventos de una `K` → misma partición.
- **Caveat:** agregar particiones reshufflea el mapeo `K`→partición. Mitigación: pre-provisionar particiones a un número alto, o consistent hashing / virtual buckets.

### 8.3 Offsets y exactly-once

- Redpanda almacena offsets en `__consumer_offsets`.
- exactly-once: los offsets se commiten como parte del checkpoint (transacción de estado + offset). Al recuperarse, se restaura el estado y el offset del último checkpoint.

### 8.4 "Integración nativa"

- No existe un conector Arrow/DataFusion/Redpanda off-the-shelf. "Nativo" = **conector source/sink en Rust** (rdkafka o cliente nativo de Redpanda + Arrow IPC).
- Es trabajo de Tachyon, pero encaja con el stack Rust (sin JVM, sin serialización costosa).

---

## 9. Escalado y Fault Tolerance

### 9.1 Modelo de escalado horizontal

- **Unidad de escala: el nodo.** Un nodo posee un subconjunto de particiones (y por tanto un subconjunto de `K` y de buckets).
- **Escalar:** agregar nodos → rebalanceo del consumer group → nodos nuevos toman particiones → más buckets de Paimon.
- **Escalar arriba/abajo** dispara rebalances; usar **cooperative/incremental rebalancing** para evitar downtime.

### 9.2 Checkpointing y recuperación

**Mecanismo: checkpoint local por instancia, basado en barriers (Chandy–Lamport aplicado in-process) + checkpointing unaligned (el más rápido).** Como Tachyon no es distribuido (cada instancia es autocontenida), el checkpoint es **local**: no hay barrier cruza-nodo. La barrier fluye **dentro de la instancia** (entre las tareas de operador in-process) para llegar a un corte consistente. Cada instancia checkpointea de forma independiente. El criterio de diseño: el mecanismo más usado en la industria (Flink), aplicado a escala local, y dentro de ese, el más rápido.

#### 9.2.1 Coordinación: checkpoint barriers (Chandy–Lamport in-process)

1. Un **coordinador de checkpoint** (dentro de la instancia) dispara un checkpoint periódicamente (cada `state.checkpoint_interval`, default 10s) inyectando una **barrier** en el stream de entrada.
2. La barrier fluye a través de los operadores **dentro de la instancia** (in-process) junto con los datos.
3. Cada operador stateful, al recibir la barrier, **snapshot de su estado redb**.
4. La barrier llega al sink; el writer de Paimon hace un **two-phase commit** (prepare en la barrier, commit al completar el checkpoint).
5. Al completar (todos los snapshots + commit de Paimon), se **commit de los offsets de Redpanda** de forma atómica con el commit de Paimon.

Resultado: un snapshot **local** consistente (exactly-once) **sin detener el pipeline** (la barrier fluye mientras se sigue procesando). Cada instancia lo hace de forma independiente; no hay coordinación entre instancias.

#### 9.2.2 Lo rápido: checkpointing unaligned (Flink FLIP-76)

- **Aligned (Fase 1, más simple):** un operador espera a que la barrier llegue por **todos** sus inputs antes de snapshot (barrier alignment). Bajo backpressure, la barrier se demora por los datos in-flight en otros canales → checkpoints más lentos.
- **Unaligned (Fase 2, lo más rápido):** el operador snapshot en cuanto llega la **primera** barrier y también snapshot de los **datos in-flight** (buffers entre operadores) aún no procesados. Esto **desacopla la duración del checkpoint del backpressure** → checkpoints rápidos incluso bajo carga. Es el mecanismo más rápido de la industria (Flink 1.11+).

Recomendación: empezar con **aligned** en la Fase 1 y añadir **unaligned** en la Fase 2 cuando el throughput lo exija. El costo de unaligned es snapshotear los buffers in-flight (serializar los `RecordBatch` en vuelo).

#### 9.2.3 Snapshot de estado: redb CoW + reflink

- redb es CoW B+trees + MVCC, single-file. Un `ReadTransaction` es una vista consistente en un punto en el tiempo.
- Al llegar la barrier, el operador toma un snapshot consistente de su tabla redb y lo **persiste copiando el archivo redb**:
  - **reflink** (XFS/Btrfs): la copia es **casi instantánea** (copy-on-write a nivel de filesystem, sin copiar datos). Es el camino más rápido.
  - **Copia full en background** (otros filesystems): se hace async para no bloquear la propagación de la barrier.
- El CoW de redb garantiza que el snapshot sea consistente; el reflink lo hace barato. El "costo" del checkpoint se mueve fuera del camino crítico.

#### 9.2.4 Exactly-once (Redpanda + Paimon)

- El checkpoint incluye los **offsets de consumo de Redpanda** (los offsets de los eventos procesados hasta la barrier).
- El sink de Paimon usa **two-phase commit** (prepare en la barrier, commit al completar).
- El **commit de offsets es atómico con el commit de Paimon** (el mismo paso de completado del checkpoint).
- Al recuperarse: se restaura el estado + se re-consume desde el offset commitado → **sin eventos perdidos ni duplicados**.

#### 9.2.5 Recuperación ante fallo

1. **Restaurar** el último checkpoint redb completado (el snapshot CoW persistido).
2. **Re-suscribirse** a Redpanda desde el offset commitado.
3. **Replay** de eventos desde el offset; el estado es consistente con el offset (exactly-once).

#### 9.2.6 Migración de estado en rebalance

Al mover una partición entre nodos (escalado), el estado redb de esa partición debe **migrarse** (el snapshot CoW/reflink de la partición se transfiere al nodo destino y se restaura) o reconstruirse. El reflink hace esta migración barata (se transfiere el snapshot, no se re-copia el estado). Es la parte más dura del escalado; el reflink la mitiga.

#### 9.2.7 Layout de archivos de checkpoint

El `checkpoint_storage` (S3 o local) organiza los checkpoints de un pipeline en directorios por checkpoint-ID, siguiendo el modelo de Flink:

```
s3://lakehouse/checkpoints/<pipeline-name>/
├── checkpoint-10/
│   ├── _metadata.json          # metadatos del checkpoint
│   ├── offsets.json            # offsets de consumo de Redpanda por partición
│   ├── redb-part-0.db          # snapshot redb (CoW/reflink) de la partición 0
│   └── redb-part-1.db          # snapshot redb de la partición 1
├── checkpoint-11/
│   ├── _metadata.json
│   ├── offsets.json
│   ├── redb-part-0.db
│   └── redb-part-1.db
└── latest                      # puntero al último checkpoint completado (-> 11)
```

**`_metadata.json`:**
```json
{
  "checkpoint_id": 11,
  "timestamp": "2026-09-23T12:00:00Z",
  "pipeline": "orders-enrichment",
  "instance": "node-3",
  "partitions": [0, 1],
  "state_files": { "0": "redb-part-0.db", "1": "redb-part-1.db" },
  "paimon_commit": {
    "table": "warehouse.dbo.orders_lake",
    "snapshot_id": 42,
    "buckets": [0, 1]
  },
  "unaligned": false
}
```

**`offsets.json`:**
```json
{
  "orders-topic": { "0": 12345, "1": 67890 },
  "items-topic":  { "0": 23456, "1": 78901 }
}
```

**Recuperación:** al arrancar, leer `latest` → obtener el checkpoint-ID → leer `_metadata.json` → restaurar los snapshots redb + offsets + commit de Paimon.

**Retención y limpieza:**
- Se conservan los últimos N checkpoints (config `state.checkpoint_retention`, default 3).
- Un checkpoint es elegible para limpieza si: (a) es más viejo que los N más recientes, Y (b) no es el referenciado por `latest` ni está en uso para recuperación.
- La limpieza es async (background), usando el delete del filesystem o el lifecycle de S3.
- El puntero `latest` se actualiza de forma atómica al completar un checkpoint.

**reflink:** si el `checkpoint_storage` está en un filesystem con reflink (XFS/Btrfs), los snapshots `redb-part-*.db` son reflinks (casi instantáneos, sin copiar datos). En S3, cada snapshot es un objeto completo (el costo se mueve a la carga, no al checkpoint).

### 9.3 Los tres sub-problemas del escalado stateful

1. **Reshuffle al agregar particiones:** el mapeo `K`→partición cambia. Mitigación: pre-provisionar particiones, o consistent hashing / virtual buckets.
2. **Migración de estado en rebalance:** checkpoint + transferir + restaurar el estado de la partición.
3. **Keys calientes:** una `K` con mucho tráfico no paraleliza (una partición = un consumidor). Mitigación: keying de dos niveles (`K` + salt) o custom partitioner (partición dedicada para la key caliente).

> Para pipelines **stateless**, los tres desaparecen y el escalado es trivial.

### 9.4 Escalado vertical por afinidad de core (stream-to-core pinning)

Técnica adicional para escalar el paralelismo **en un solo nodo** (escalado vertical del hardware) en lugar de agregar nodos: **pinear cada stream/partición a un core de CPU dedicado** (thread affinity + NUMA locality). No es un disparate: es una técnica real y **Redpanda ya la usa** (arquitectura thread-per-core / TPC, donde cada partición se procesa en un thread pinned a un core físico, con datos core-affine).

**Beneficios:**
- **Localidad de cache:** el estado redb y los datos de cada stream se quedan en la L1/L2 del core; sin cache thrashing por migración de threads.
- **Localidad de NUMA:** pino el stream al core del nodo NUMA donde vive su memoria; evita el acceso cross-NUMA (~30-70% más lento).
- **Menor latencia:** sin migración de threads, sin overhead de context-switch, rendimiento predecible.
- **Afinidad de core end-to-end:** alineando la asignación de cores de Tachyon con el mapeo partición→core de Redpanda: partición en core X → procesamiento en core X → escritura Paimon en core X.

**Límites:**
- Acotado por el número de cores del nodo (no puedes correr 1000 streams en 64 cores).
- Acotado por la RAM del nodo (estado redb).
- Punto único de fallo (un nodo caído = todos sus streams pinean caen); mitigar con varios nodos para redundancia.
- Retornos decrecientes pasado cierto número de cores (ancho de banda de memoria, coherencia de cache).

**Reframe:** es **escalado vertical del paralelismo** — conseguir muchos streams en un solo nodo usando más cores, en vez de repartir entre nodos. Es **complementario** al escalado horizontal (más nodos): llenar un nodo (pinear streams a cores) hasta agotar cores/memoria, y después agregar nodos.

**Implementación:**
- Thread affinity en Rust: `task_affinity` o `sched_setaffinity` (Linux); pino la tarea de cada stream a un core.
- Awareness de NUMA: pino al core del nodo NUMA donde se allocó la memoria del estado.
- Core allocator: reparte streams/particiones a cores balanceando carga.
- Alineación con el TPC de Redpanda para afinidad end-to-end (caveat: el TPC de Redpanda es interno; si Tachyon es proceso separado, se alinea la asignación de cores con el mapeo esperado partición→core).

**Todo lo demás se mantiene:** clave única de particionado, no-distribuido, checkpointing local (ahora también por core), un writer por bucket. Es una capa de optimización sobre el diseño existente.

---

## 10. Footprint y Observabilidad

### 10.1 Footprint medible

- Stack Rust/C++ (DataFusion, `paimon-rs`, Redpanda) → sin JVM, footprint bajo, startup rápido.
- **Métricas por instancia de pipeline** (vía OpenTelemetry):
  - **Throughput:** rows/s, MB/s (por input y output).
  - **Latencia:** p50/p99 evento→escritura (event time → Paimon commit).
  - **Lag:** consumer lag (offsets) por partición.
  - **Memoria:** tamaño de estado, buffers, RecordBatches en vuelo.
  - **CPU:** por operador y por nodo.
  - **I/O:** lecturas/escrituras a Paimon, amplificación de compaction del LSM.
  - **Checkpointing:** duración, tamaño, frecuencia, lag entre checkpoints.
- **SLA de footprint:** objetivo de CPU/memoria por partición procesada (p. ej. X MB de estado por MB de throughput). Se define y se mide en benchmarks.

### 10.2 Observabilidad

- **Métricas:** OpenTelemetry (métricas, traces, logs).
- **Traces:** trace distribuido a través de los operadores (DataFusion soporta extensibilidad).
- **Admin API:** endpoint HTTP para consultar estado, lag, métricas, trigger checkpoint, reassign particiones.
- **Health:** liveness/readiness probes para K8s.

---

## 11. Despliegue

- **Un pipeline = un deployable.** Binario Tachyon + `pipeline.sql` + `pipeline.yaml`.
- **Contenedor por nodo.** Imagen Rust estática, pequeña.
- **Orquestación:** Kubernetes. Un Deployment/StatefulSet por pipeline; los nodos son réplicas. O un **operator** de Tachyon que gestiona el ciclo de vida de los pipelines (escalar, checkpoint, migrar estado).
- **Configuración:** `pipeline.yaml` montado como ConfigMap; secretos (credenciales Redpanda/Paimon) como Secret.
- **Escalar:** el operator (o HPA custom) agrega réplicas cuando el lag o el throughput lo exigen, manteniendo alineado particiones/buckets/nodos.

---

## 12. Soporte de Lance y Arrow

- **Arrow:** formato in-memory de intercambio. DataFusion y `paimon-rs` ya lo usan. Cero copia entre Redpanda (fuente) → DataFusion (cómputo) → Paimon (sink). El conector de Redpanda emite `RecordBatch` de Arrow.
- **Lance:** formato de archivo columnar para ML/vector. Se usa como **formato de archivo de Paimon** cuando la tabla de salida necesita random access, vector search o trabajo de ML. No compite con Paimon (que es la capa de tabla/lakehouse); es el formato físico opcional.
- **Vector search:** Paimon Rust soporta ANN vector search (índice Lumina) registrado como UDTF en DataFusion. Tachyon hereda esta capacidad para pipelines que producen embeddings/tablas vectoriales.

---

## 13. Decisiones de Diseño y Tradeoffs

| # | Decisión | Alternativa | Razón |
|---|---|---|---|
| D1 | SQL + config file (separación lógica/física) | Todo en SQL (Arroyo puro) | Separación limpia: SQL = lógica, config = I/O + despliegue. Fácil de validar el invariante de alineación. |
| D2 | DataFusion como motor de cómputo | Motor propio | Heredar el motor de ejecución (la parte más dura). Solo construir la capa de semánticas. |
| D3 | No usar Flink | Flink como orquestador | Footprint bajo (sin JVM), despliegue por-pipeline, embebible. Tradeoff: construir watermarks/estado/checkpointing. |
| D4 | Clave única de particionado | Particionado independiente por capa | Corrección + escalado + sin conflictos de escritura. |
| D5 | Un writer por bucket | Múltiples writers por bucket | Corrección + sin conflictos. Escala por buckets. |
| D6 | Redpanda como broker | Kafka, Pulsar, NATS | C++, thread-per-core, baja latencia, footprint menor, Kafka-compatible, Jepsen-verified. |
| D7 | Paimon como formato de tabla | Iceberg, Delta, Hudi | Streaming-first (LSM, changelog, CDC), `paimon-rs` nativo con DataFusion. |
| D8 | `redb` como único backend de estado (opinionated) | Enfoque por niveles (memoria/redb), RocksDB, SQLite | Un solo backend → un solo checkpoint → config mínima. Puro Rust, single-file, CoW, rápido incluso para estado pequeño. |
| D9 | Parquet (analytics) / Lance (ML) como formato de archivo | Solo Parquet | Cobrir ambos casos de uso analítico y ML/vector. |

---

## 14. Riesgos y Preguntas Abiertas

| # | Riesgo / Pregunta | Impacto | Mitigación |
|---|---|---|---|
| R1 | **Madurez de Paimon↔Lance** como formato de archivo | Alto si Lance es central | Verificar estado actual antes de hacer de Lance una dependencia. Empezar con Parquet. |
| R2 | **Checkpointing sobre DataFusion** (lo más difícil) | Alto | Referencia Arroyo/Denormalized. Empezar con stateless, añadir stateful gradualmente. |
| R3 | **Streaming shuffle (fan-in)** | Alto solo si hay fan-in | Diseñar para 1 entrada alineada cuando sea posible. El shuffle es el operador más caro. |
| R4 | **Migración de estado en rebalance** | Medio-Alto | Static group membership + checkpoint de partición. |
| R5 | **Backpressure en DataFusion pull-based** | Medio | Añadir buffers adaptativos entre operadores. |
| R6 | **Reshuffle al agregar particiones** | Medio | Pre-provisionar particiones / consistent hashing. |
| R7 | **Keys calientes** | Medio | Keying de dos niveles / custom partitioner. |
| R8 | **Conector Redpanda en Rust** (no existe off-the-shelf) | Medio | rdkafka + Arrow IPC. Encaja con el stack Rust. |
| R9 | **Watermarks y optimizadores de DataFusion** | Medio | Convencer a los optimizadores de no dropear el timestamp. |
| R10 | **Nombre del proyecto** (Tachyon) | Bajo | Confirmar (Tachyon fue el nombre original de Apache). |

---

## 15. Roadmap (fases)

### Fase 0 — Fundamentos (MVP stateless)
- Binario Tachyon: parsear `pipeline.sql` + `pipeline.yaml`, validar invariante de alineación.
- Conector fuente Redpanda (`StreamTableExec`), decodificación a Arrow.
- Conector sink Paimon (writer por bucket, sequence numbers).
- Operadores stateless (filter, project, map, join stateless).
- Footprint: métricas básicas (throughput, lag, memoria).
- **Entregable:** un pipeline stateless ETL de Redpanda → Paimon, escalable por particiones.

### Fase 1 — Semánticas de streaming (stateful)
- Watermarks.
- Estado `redb` (único backend) + checkpointing/restauración.
- Windowing (tumbling primero, luego sliding/session).
- Operadores stateful (agregación con estado, joins stateful).
- exactly-once (offsets + checkpoint).
- **Entregable:** pipelines stateful con garantías de exactly-once.

### Fase 2 — Escalado y fault tolerance robustos
- Migración de estado en rebalance.
- Cooperative rebalancing.
- Consistent hashing / virtual buckets (evitar reshuffle).
- Keys calientes (keying de dos niveles).
- Dedicated compaction job de Paimon.
- **Entregable:** escalado horizontal sin downtime, recuperación ante fallos.

### Fase 3 — Fan-in y streaming shuffle
- Operador de fusión con streaming shuffle por clave.
- Múltiples entradas con particionados distintos.
- **Entregable:** pipelines con fan-in correctos.

### Fase 4 — Lance, vector search, observabilidad completa
- Soporte de Lance como formato de archivo de Paimon.
- Vector search (Lumina) en pipelines.
- Observabilidad completa (traces, admin API, operator de K8s).
- **Entregable:** pipelines ML/vector + operación de producción.

---

## 16. Ejemplo Completo (end-to-end)

**`pipeline.sql`:**
```sql
CREATE STREAM VIEW order_sums AS
SELECT
    o.order_id,
    o.status,
    o.source_version,
    o.event_time,
    SUM(i.amount) AS order_total
FROM orders AS o
JOIN items  AS i ON o.order_id = i.order_id
GROUP BY o.order_id, o.status, o.source_version, o.event_time;

INSERT INTO orders_lake
SELECT order_id, status, source_version, event_time, order_total
FROM order_sums;
```

**`pipeline.yaml`:**
```yaml
pipeline:
  name: order-sums
  version: 1
connectors:
  redpanda:
    brokers: [redpanda-0:9092]
    security: { sasl_mechanism: SCRAM-SHA-256, username: ${RP_USER}, password: ${RP_PASS} }
  paimon:
    warehouse: s3://lakehouse/warehouse
    catalog: rest
    catalog_uri: http://paimon-catalog:8080
inputs:
  - { name: orders, topic: orders-topic, key: order_id, schema: avro/orders-v1, watermark: { column: event_time, lag: 5s } }
  - { name: items,  topic: items-topic,  key: order_id, schema: avro/items-v1 }
output:
  name: orders_lake
  table: warehouse.dbo.order_sums
  key: order_id
  bucket: 8
  sequence.field: source_version
deployment:
  partitions: 8
  resources: { cpu: "2", memory: 4Gi }
  scaling: { strategy: partitions, min: 1, max: 16 }
  state: { checkpoint_interval: 10s, checkpoint_storage: s3://lakehouse/checkpoints/order-sums }
```

**Validación en compile-time:** `inputs[*].key == output.key == order_id` ✓ · `deployment.partitions == output.bucket == 8` ✓ → pipeline válido.

---

## Apéndice A — Glosario

- **Bucket (Paimon):** unidad más pequeña de paralelismo de lectura/escritura en una tabla Paimon. Un writer por bucket.
- **Partition (Redpanda):** sub-tema ordenado; unidad de paralelismo de consumo. Una partición = un consumidor a la vez.
- **Clave única de particionado (`K`):** la clave que particiona Redpanda, el estado, la fusión y los buckets de Paimon.
- **Watermark:** indicador del avance del event time; se usa para late data y para cerrar ventanas.
- **Chandy–Lamport:** algoritmo de snapshot distribuido para checkpointing consistente.
- **LSM-tree:** Log-Structured Merge-Tree; estructura de almacenamiento de Paimon que apila sorted runs y los compacta.
- **Sequence number:** campo que ordena actualizaciones de una misma clave en Paimon.
- **Fan-in:** múltiples streams de entrada fusionados en uno.
- **Union disjunto:** fusión de streams cuyas claves son disjuntas (no requiere shuffle).
- **Streaming shuffle:** redistribución de datos por clave entre nodos, preservando orden, en un pipeline de streaming.
