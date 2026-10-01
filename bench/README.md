# Tachyon vs Flink en la misma máquina

Benchmark reproducible de una instancia de Tachyon contra Flink 2.2.1 +
Paimon 1.4.2 (conectores Kafka 5.0.0-2.2 y Kinesis 6.0.1-2.0), con el mismo
input, la misma tabla de salida y el mismo presupuesto de CPU y memoria.

Tres fuentes de entrada: **Redpanda** (Kafka), **Kinesis** y **SQS** (los dos
AWS emulados por [floCi](https://github.com/floci/floci)). Flink no tiene
source de SQS: el escenario SQS se mide solo con Tachyon.

## Qué se compara

| | Tachyon | Flink |
|---|---|---|
| Proceso | un binario (`tachyon`) | JobManager + TaskManager en un contenedor |
| CPUs | cpuset 4-7 (4 CPUs) | cpuset 4-7 (4 CPUs), `parallelism = 4` |
| Memoria | tope de cgroup 4608 MiB | tope de cgroup 4608 MiB (JM 1 GiB + TM 3 GiB) |
| Garantía | exactly-once (Kinesis: shard→secuencia en el checkpoint; SQS: at-least-once + dedup por PK+secuencia; Kafka: offsets en el snapshot de Paimon) | exactly-once (checkpoints) |
| Commit | `commit_interval: 1s` | `execution.checkpointing.interval: 1s` |
| Tabla | la misma DDL (`flink/table-*.sql`), creada por Paimon Java | idem |
| Formato | parquet + zstd, `write-only` (sin compactar en el writer) | idem |

Los motores corren en el namespace de red de su fuente (Redpanda o floCi),
donde `localhost` es el broker/emulador. Redpanda queda en los CPUs 0-1 y el
harness (productores y medición) en 2-3 y 8-9: nada comparte núcleo con el
motor que se mide. Los motores corren de a uno.

Workloads (`flink/*.sql` y `tachyon/*`):

- **etl**: JSON `{order_id, status, source_version, amount, event_time}` →
  `WHERE status <> 'cancelled'` → tabla PK por `order_id`, 8 buckets,
  `sequence.field = source_version`. 60M eventos en Redpanda, 10M en Kinesis,
  500K en SQS (el preload de SQS lo limita floCi, ~1.3K msg/s), 10% filtrados.
- **win**: tumbling de 1s por `order_id` (10K claves), `COUNT(*)` y
  `SUM(amount)`, watermark sobre `event_time`. Solo Redpanda. 60M eventos con
  event time sintético (100K eventos por segundo de event time).

El input tiene 8 particiones/shards/consumidores y la clave cumple
`order_id % 8 == partición`. Cada productor es dueño de sus particiones y
escribe en orden: el event time es monótono dentro de cada partición (no hay
late data artificial).

## Cómo se mide

Todo desde afuera y del mismo modo para los dos motores (`harness/bench.py`):

- **Throughput** (`drain`): la fuente se precarga una vez (topic, stream o
  cola) y cada corrida la drena desde el principio con un grupo/consumidor
  nuevo. El progreso es el `totalRecordCount` del snapshot más reciente de
  Paimon para las tres fuentes: un snapshot solo existe cuando el commit
  exactly-once está visible. Como avanza a escalones (un commit por vez), la
  tasa es la pendiente de una recta ajustada a los instantes de salto entre
  el 20% y el 95% del backlog (régimen, sin el arranque; el arranque se
  reporta aparte como `first_commit_s`).
- **CPU y memoria**: `cpu.stat` y `memory.stat` (anon) del cgroup del
  contenedor del motor, sobre el mismo tramo.
- **Correctitud**: después de cada drenado se verifica la salida fila por
  fila contra la verdad calculada desde la fórmula del generador
  (`harness/etl_truth.py` para etl, `harness/truth_win.py` para win): cada
  evento no cancelado aparece una vez, sin duplicados de versión, sin filas
  extra y con los valores exactos. Una corrida que no verifica no cuenta.
- **Latencia** (`live`): productores a tasa fija con `event_time = reloj`,
  el harness sigue los snapshots de Paimon y, por cada data file nuevo, lee
  la columna de tiempo del parquet. Latencia de una fila = `timeMillis` del
  snapshot que la hace visible − `event_time` (etl) o − `window_end` (win).
  En Kinesis la tasa real queda acotada por floCi (~75K–240K rec/s según
  carga); en SQS, por su serialización (~780–1300 msg/s).

## Correr

```bash
# Un vez, por máquina:
bench/fetch-flink-jars.sh      # jars de Paimon, Kafka, Hadoop y Kinesis para Flink
docker build -t tachyon-bench-flink bench/flink
docker build -t tachyon-benchkit -f bench/Dockerfile.harness bench
bench/build-engine.sh          # binario benchfast + imagen tachyon-build + volumen tachyon-target
docker volume create tachyon-bench-wh

# Fuentes:
docker run -d --name tachyon-redpanda --cpuset-cpus 0-1 -p 8081:8081 -p 9092:9092 redpandadata/redpanda
docker run -d --name tachyon-floci -p 4566:4566 floci/floci

# Corridas (source: redpanda | kinesis | sqs):
bench/run.sh preload kinesis   # stream bench-etl, 10M eventos (KINESIS_EVENTS=)
bench/run.sh preload sqs       # cola bench-etl, 500K eventos (SQS_EVENTS=)
bench/run.sh drain tachyon etl kinesis
bench/run.sh live flink etl 100000 kinesis
REPS=3 bench/suite.sh          # todo: SOURCES="redpanda kinesis sqs" por defecto
python3 bench/report.py        # tablas de resumen
```

Una corrida suelta: `bench/run.sh drain tachyon etl` (Redpanda),
`bench/run.sh live flink win 1000000`. `CPUS`, `MEM`, `COMMIT`, `LAG_MS`,
`PARALLELISM`, `SHARDS`, `KINESIS_EVENTS` y `SQS_EVENTS` cambian el
escenario; `PROFILE=1` corre Tachyon bajo `perf`. En SQS el preload es
destructivo (cola): `drain` lo refresca justo antes de arrancar el motor.

## Resultados (2026-09-30, Linux x86_64, 24 CPUs, 4 por motor)

Tachyon `benchfast` contra Flink 2.2.1 + Paimon 1.4.2. Toda corrida de
throughput verificó su salida fila por fila; `python3 bench/report.py`
regenera las tablas desde `results/`.

### Kinesis (10M eventos, 16 shards)

Throughput (backlog drenado, exactly-once, salida verificada):

| motor | corridas | filas/s (mediana) | cores usados | filas/CPU-s | RSS anon pico |
|---|---|---|---|---|---|
| tachyon | 1 | 364K | 0.77 | 540K | 595 MB |
| flink | 1 | 460K | 2.37 | 180K | 1953 MB |
| **Tachyon / Flink** | | **0.79x** en filas/s | | **3.0x** en filas/CPU-s | |

Latencia de punta a punta (tasa objetivo 100K/s; real 75K Tachyon / 53K
Flink, acotada por floCi; `landed_fraction` 1.0 en ambos):

| motor | p50 | p99 | p99.9 | max | cores |
|---|---|---|---|---|---|
| tachyon | 681 ms | 1259 ms | 3097 ms | 3117 ms | 0.20 |
| flink | 780 ms | 4068 ms | 4209 ms | 4227 ms | 1.33 |
| **Flink / Tachyon** | **1.15x** | **3.23x** | | | |

### SQS (500K mensajes; solo Tachyon, Flink no tiene source de SQS)

Throughput: el drenado corre a ~776 filas/s, limitado por la tasa de
recepción de floCi (~860 msg/s) y no por el motor (0.05 cores, 403 MB).
Salida verificada: 450K/450K filas, sin duplicados ni valores erróneos.

Latencia de punta a punta (tasa 500/s, `landed_fraction` 1.0):

| motor | p50 | p99 | p99.9 | max | cores |
|---|---|---|---|---|---|
| tachyon | 553 ms | 1049 ms | 1066 ms | 1076 ms | 0.11 |

### Redpanda (máquina anterior, Apple M5)

Los números de Redpanda en `results/` corresponden a la máquina M5; en esta
máquina hay corridas sueltas (`results/2026-09-30.jsonl`). Resumen M5
(mediana de 3 corridas, motores alternados, salida verificada):

| workload | motor | filas/s | cores | filas/CPU-s | p50 (1M/s) | p99 (1M/s) |
|---|---|---|---|---|---|---|
| etl | tachyon | 5.42M | 2.78 | 2.00M | 720 ms | 1246 ms |
| etl | flink | 2.79M | 3.67 | 758K | 843 ms | 1778 ms |
| win | tachyon | 5.17M | 2.63 | 2.00M | 726 ms | 1475 ms |
| win | flink | 3.07M | 3.55 | 918K | 1180 ms | 1780 ms |

## Límites conocidos

- floCi emula Kinesis y SQS en un solo proceso: SQS queda acotado a
  ~780–1300 msg/s y Kinesis a ~75K–240K rec/s según la carga. Las tasas de
  SQS miden el emulador, no al motor; en AWS real el motor consume en
  paralelo por shard/cola.
- Flink no tiene source de SQS: la comparación de paridad solo existe para
  Kinesis (y Redpanda).
- El backlog de Kinesis (10M) es menor que el de Redpanda (60M) para que el
  preload y el drenado quepan en el tiempo de la suite; `KINESIS_EVENTS` lo
  escala.
- La imagen del motor importa el filesystem del host (el binario está
  linkado contra la glibc del host); por eso `build-engine.sh` se corre en
  la misma máquina que compila.
- Tachyon se mide con el perfil `benchfast` (`opt-level=3`, LTO thin). El
  perfil `release` (LTO fat, 1 unidad de codegen) no linkea en un VM de 16 GB.
- El sink de los dos es Paimon con `write-only`: la compactación queda fuera
  del camino caliente en ambos.
