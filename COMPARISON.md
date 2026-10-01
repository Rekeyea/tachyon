# Tachyon vs Apache Flink — comparación con números públicos

> **Estado:** Borrador v0.1
> **Fecha:** 2026-09-25
> **Propósito:** situar el throughput y footprint medidos de Tachyon frente a
> números públicos de Apache Flink, siendo honesto sobre qué es comparable y qué no.

---

## 0. Advertencia de comparabilidad (leer primero)

**Los números de Tachyon y los de Flink no son medidos bajo las mismas condiciones.**
Cualquier ratio directo (X veces más rápido) es una aproximación, no una afirmación
de benchmark. Las diferencias:

| Dimensión | Tachyon (medido) | Flink (público) |
|---|---|---|
| Workload | JSON decode + SQL transform (DataFusion) + write Paimon + commit | Varía por fuente: word count, ad-campaign join+window, passthrough, etc. |
| Topología | 1 nodo, pipeline autocontenido, 1 output | Cluster (10-40 nodos) o single-node según fuente |
| Broker | Redpanda (C++, thread-per-core) | Kafka (JVM) en la mayoría de fuentes |
| Sink | Paimon (lakehouse, LSM) | Redis / Kafka / sink de memoria |
| Garantía | at-least-once + idempotencia por sequence (MVP) | exactly-once (checkpointing) en muchas fuentes |
| Hardware | i9-12900K (24 hilos, 1 NUMA), Docker | Varía: E5530 (2015) hasta HPC modernos (2025) |

**Conclusión del draft:** los números públicos de Flink se usan como **orden de
magnitud de referencia**, no como rival directo. El valor real de Tachyon no es
"es N veces más rápido que Flink" sino "mismo orden de throughput con un footprint
y un modelo de despliegue radicalmente más simples (sin JVM, sin cluster, sin
coordinación)".

---

## 1. Números medidos de Tachyon (este repo, release, i9-12900K)

Workload: 10M eventos JSON (~62 bytes) pre-producidos en Redpanda (1-8 particiones),
drenados por el pipeline completo: `Redpanda -> decode JSON -> DataFusion SQL ->
Paimon write + commit`. Medición en estado estacionario (ventana 20%→80% del drenado).

| Config | rows/s | CPU | rows/CPU-s | RSS |
|---|---|---|---|---|
| 1 partición / 1 consumidor | 226K | 28% (1 core) | 813K | 236 MB |
| 8 particiones / 1 consumidor | 234K | 29% | 800K | 383 MB |
| 8 particiones / 2 consumidores | 499K | 62% | 805K | 459 MB |
| 8 particiones / 4 consumidores | **1,083,891** | 118% (CPU-bound) | 919K | 717 MB |

**Cifras de referencia por etapa** (bench_stages, release):
- Decode JSON: ~4.7M rows/s por core
- Transform DataFusion: ~175M rows/s por core (casi gratis, vectorizado)
- Write Paimon: ~427M rows/s por core (casi gratis)
- Commit Paimon: ~3.3M commits (bottleneck histórico, ya decoplado a writer task)

**Diagnóstico clave (histograma de gaps):** 98% del wall time se pasa esperando
que llegue data del broker, no procesando. Un lote de 57K filas tarda ~2ms en
procesarse pero ~265ms en llegar. El techo es la serialización de round trips de
fetch por cliente librdkafka (~200K rows/s por instancia), no el cómputo.

---

## 2. Números públicos de Flink (referencia)

### 2.1 Yahoo Streaming Benchmark (2015/2016) — Flink 0.10.1

Fuente: "Benchmarking Streaming Computation Engines: Storm, Flink and Spark
Streaming" (Chintapalli et al., IEEE IPDPSW 2016) + blog de Yahoo.

- **Workload:** ad-campaign — leer JSON de Kafka, parsear, filtrar, proyectar,
  join con Redis (dimensión), windowed count por campaña, escribir a Redis.
- **Topología:** 10 worker nodes, 5 Kafka nodes (5 particiones), 1 Redis, 3 ZK.
  Cada node: 2× Intel E5530 (16 cores), 24 GiB RAM, Gigabit Ethernet.
- **Config Flink:** `taskmanager.heap.mb: 15360` (15 GB heap por TaskManager),
  `taskmanager.numberOfTaskSlots: 16`. Sin checkpointing (at-most-once).
- **Resultado:** Flink procesó **50,000 – 170,000 events/s** en el cluster de 10
  nodos, con latencia p99 sub-segundo a través de ese rango. Storm 0.11 (sin
  acking) y Flink tuvieron curvas throughput/latencia muy similares; Spark
  Streaming tuvo mucha más latencia (micro-batching).

**Lectura:** ~17-50K events/s **por nodo** (170K/10, 50K/10) en hardware de 2015,
con un workload más pesado que el de Tachyon (join con Redis incluido) pero con
15 GB de heap por TaskManager y sin escribir a un lakehouse.

### 2.2 SProBench (2025) — Flink en HPC

Fuente: "SProBench: Stream Processing Benchmark for High Performance Computing
Infrastructure" (Kulkarni & Ghiasvand, arXiv 2504.02364, 2025).

- **Workload:** passthrough, CPU-intensive (parse + transform + threshold),
  memory-intensive (keyed + sliding window + average). Event JSON ~27+ bytes.
- **Topología:** cluster HPC (SLURM), scale-up (1-16 cores) y scale-out (multi-node).
- **Resultado:** Flink escala de forma casi lineal hasta ~8 de parallelism y se
  satura después. El benchmark genera hasta 40M events/s (pero eso es el
  generador, no el consumo de Flink). Con 16 threads, Flink alcanza el throughput
  más alto pero con latencia creciente y más GC.

**Lectura:** Flink moderno aprovecha bien los cores hasta ~8 de parallelism en
workloads CPU-bound, luego retornos decrecientes (GC + scheduling). Cifra absoluta
de throughput no está en el resumen (está en las figuras), pero el patrón de
escala (lineal hasta 8, luego plano) es comparable al de Tachyon.

### 2.3 Footprint de Flink (documentación oficial)

Fuente: "Set up TaskManager Memory" (docs oficiales de Flink, stable).

El modelo de memoria de un TaskManager es la suma de:
- Framework heap + Task heap (JVM Heap)
- Managed memory (off-heap, para RocksDB state / sorting / hash tables)
- Framework off-heap + Task off-heap (direct memory)
- Network memory (buffer de exchange entre tasks, fracción acotada de la memoria total)
- JVM metaspace + JVM overhead (thread stacks, code cache, GC)

Incluso en **local execution** (sin cluster), Flink reserva por defecto
128 MB de managed memory + 64 MB de network memory, y el heap es el que le des
tú. En producción, los deployments típicos arrancan en **1-4 GB por TaskManager**
y el JobManager añade otro proceso JVM. Esto es el **floor** de footprint:
aunque el pipeline no use estado, el JVM + el framework + el network buffer
ocupan RAM.

**Lectura:** el floor de Flink es un proceso JVM (tipicamente ≥1 GB) + JobManager
(separado) + ZooKeeper/K8s para coordinación. Tachyon no tiene ninguno de esos
tres: un solo binario Rust, sin JVM, sin proceso de coordinación, sin ZK.

---

## 3. Comparación (con las salvedades de §0)

### 3.1 Throughput por nodo

| | Tachyon (2026, medido) | Flink (público) |
|---|---|---|
| Throughput single-node | **1.08M rows/s** (8p/4c, i9-12900K) | ~17-50K events/s por nodo (2015, cluster de 10) |
| Workload | JSON + SQL + Paimon write+commit | JSON + filter + join Redis + window + Redis write |
| Hardware | i9-12900K (2021) | E5530 (2015) |
| Broker | Redpanda | Kafka |

**Lectura honesta:** no es comparable directamente (hardware de 2021 vs 2015,
workloads distintos, broker distinto). Pero el **orden de magnitud** es el mismo:
ambos sistemas procesan cientos de miles de events/s por nodo en workloads de
streaming reales. Tachyon no está "100x por encima" de Flink; está en el mismo
rango, con un workload que incluye write a lakehouse (que Flink no hacía en ese
benchmark).

### 3.2 Footprint

| | Tachyon | Flink |
|---|---|---|
| Runtime | Rust, sin JVM | JVM (TaskManager + JobManager) |
| Floor de RAM (sin estado) | ~236 MB (1p/1c) | ≥1 GB (TaskManager) + JobManager + ZK |
| Coordinación | Ninguna (autocontenido) | JobManager + ZooKeeper/K8s |
| Despliegue | 1 binario + SQL + YAML | Cluster (JM + N×TM + ZK) |
| Estado | redb (puro Rust, single-file) | RocksDB (managed memory) o heap |

**Lectura:** aquí sí hay una diferencia estructural clara y no depende del
hardware. Tachyon tiene un floor de footprint ~4-8x menor y un modelo de
despliegue sin procesos de coordinación. Para pipelines pequeños/medianos
(1-8 particiones), Tachyon corre en un solo binario donde Flink necesita un
cluster mínimo (JM + TM + ZK).

### 3.3 Escalado

| | Tachyon | Flink |
|---|---|---|
| Unidad de escala | Partición de Redpanda (nodo autocontenido) | Task slot / TaskManager |
| Coordinación | Ninguna (consumer group de Redpanda) | JobManager + rebalanceo |
| State migration | Reflink de snapshot redb (Fase 2) | Savepoint + restore |
| Techo observado | ~200K rows/s por cliente librdkafka; escala con N consumidores | Lineal hasta ~8 parallelism, luego GC/scheduling |

**Lectura:** ambos escalan de forma lineal hasta un punto y luego se saturan.
El cuello de Tachyon es el fetch por cliente (se soluciona con más consumidores,
ya implementado). El cuello de Flink es GC + scheduling (inherente al JVM).

---

## 4. Lo que Tachyon NO puede claimar (honestidad)

1. **No es "N veces más rápido que Flink".** Los workloads, hardware y brokers
   son distintos. Lo que sí se puede claimar: mismo orden de throughput con
   footprint y despliegue radicalmente más simples.
2. **No tiene exactamente-once todavía.** El MVP es at-least-once + idempotencia
   por sequence. Flink tiene exactly-once con checkpointing desde 2015. Esto se
   resuelve en la Fase 1 (checkpointing + commit atómico de offsets).
3. **No tiene el ecosistema de Flink.** Connectors, state backends, SQL completo,
   community. Tachyon es un motor lean por-pipeline, no una plataforma general.
4. **Los números de Flink de 2015 están desactualizados.** Flink 1.19/2.x es
   considerablemente más rápido que Flink 0.10.1. La comparación de throughput
   con el benchmark de Yahoo es una cota inferior, no el estado actual de Flink.

---

## 5. Nuevas capacidades (2026-10-xx)

Tachyon se extendió para cubrir escenarios que antes se atribuían a Flink:

### 5.1 Conversor de esquemas (`tachyon-convert`)

Un input con un esquema distinto al del SQL se mapea automáticamente:
- **Reordena columnas** (source → target)
- **Coerciona tipos** (Int32→Int64, Utf8→Int64 parsing, Boolean→Int64, Float32→Float64)
- Stateless: sin checkpoint ni estado adicional

Uso en config: `inputs[*].convert_to: combined_orders`. Dos topics con schemas
distintos (`web_orders` y `app_orders`) convergen a un schema base común antes
de entrar al SQL.

### 5.2 UNION ALL + ventanas (cross-service windowed queries)

Se relajó la restricción del parser que prohibía mezclar `UNION ALL` con
operadores de ventana. Ahora:
```sql
INSERT INTO summary
SELECT service, COUNT(*) AS events
FROM combined_streams
GROUP BY service
UNION ALL SELECT order_id, amount FROM app
```
Las ramas se unifican y la ventana (TUMBLE/HOP/SESSION) opera sobre el stream
combinado. Ideal para agregar logs/métricas de múltiples servicios en un solo query.

### 5.3 JOIN entre dos streams

El operador de interval join ya soporta `A JOIN B ON A.key = B.key AND ...`.
Con la extensión de esquemas, los dos streams pueden tener schemas distintos y
se normalizan antes del join. El patrón `ventana → Paimon → leer como stream`
ya está cableado (`table_stream.rs`).

### 5.4 Benchmarks de nueva funcionalidad

| workload | descripción | notas |
|---|---|---|
| `etl` | JSON → filter → Paimon (baseline) | 60M eventos, ~1.08M rows/s |
| `win` | tumbling window 1s por order_id | 60M eventos, watermark |
| `convert` | 2 topics schemas distintos → unified → Paimon | Conversor + UNION ALL |
| `multiwin` | 2 servicios → UNION ALL → session window → Paimon | Ventana multi-servicio |

## 6. Siguiente paso (para blindar la comparación)

Para que la comparación sea justa y defendible, falta un **benchmark head-to-head
controlado**: mismo workload, mismo hardware, mismo broker, midiendo Tachyon y
Flink en las mismas condiciones. Propuesta:

1. **Workload común:** JSON decode + filter + SQL transform + write a sink
   (Paimon para Tachyon, Kafka/Redis para Flink, o ambos a Paimon si Flink lo
   soporta).
2. **Hardware:** el mismo i9-12900K (o un VM estándar reproducible).
3. **Broker:** Redpanda para ambos (Flink tiene connector Kafka-compatible).
4. **Métricas:** rows/s, CPU-s, RSS, latencia p50/p99, floor de RAM.
5. **Nuevos workloads:** los benchmarks `convert` y `multiwin` miden el overhead
   del conversor de esquemas y la latencia de ventanas cruzando múltiples
   servicios (UNION ALL + window).
6. **Fase 1 primero:** el benchmark head-to-head tiene más sentido después de la
   Fase 1 (stateful + exactly-once), porque comparar un MVP at-least-once contra
   un Flink exactly-once sesga la comparación a favor de Tachyon en latencia y
   en contra en correctness.

---

## Apéndice — Fuentes

- Tachyon: `bench_e2e.rs`, `bench_stages.rs`, `raw_consume_spike.rs` (este repo).
- Flink Yahoo: Chintapalli et al., "Benchmarking Streaming Computation Engines:
  Storm, Flink and Spark Streaming", IEEE IPDPSW 2016.
  <https://github.com/yahoo/streaming-benchmarks>
- Flink SProBench: Kulkarni & Ghiasvand, "SProBench: Stream Processing Benchmark
  for HPC Infrastructure", arXiv 2504.02364, 2025.
- Flink memory: "Set up TaskManager Memory", docs oficiales de Apache Flink.
  <https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/memory/mem_setup_tm/>
