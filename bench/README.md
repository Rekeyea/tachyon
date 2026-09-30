# Tachyon vs Flink en la misma máquina

Benchmark reproducible de una instancia de Tachyon contra Flink 2.2.1 +
Paimon 1.4.2 (conector Kafka 5.0.0-2.2), con el mismo input, la misma tabla
de salida y el mismo presupuesto de CPU y memoria.

## Qué se compara

| | Tachyon | Flink |
|---|---|---|
| Proceso | un binario (`tachyon`) | JobManager + TaskManager en un contenedor |
| CPUs | cpuset 4-7 (4 CPUs) | cpuset 4-7 (4 CPUs), `parallelism = 4` |
| Memoria | tope de cgroup 4608 MiB | tope de cgroup 4608 MiB (JM 1 GiB + TM 3 GiB) |
| Garantía | exactly-once (offsets en el snapshot de Paimon) | exactly-once (checkpoints) |
| Commit | `commit_interval: 1s` | `execution.checkpointing.interval: 1s` |
| Tabla | la misma DDL (`flink/table-*.sql`), creada por Paimon Java | idem |
| Formato | parquet + zstd, `write-only` (sin compactar en el writer) | idem |

Redpanda queda en los CPUs 0-1 y el harness (productores y medición) en
2-3 y 8-9: nada comparte núcleo con el motor que se mide. Los motores corren
de a uno.

Workloads (`flink/*.sql` y `tachyon/*`):

- **etl**: JSON `{order_id, status, source_version, amount, event_time}` →
  `WHERE status <> 'cancelled'` → tabla PK por `order_id`, 8 buckets,
  `sequence.field = source_version`. 60M eventos, 10% filtrados.
- **win**: tumbling de 1s por `order_id` (10K claves), `COUNT(*)` y
  `SUM(amount)`, watermark sobre `event_time`. 60M eventos con event time
  sintético (100K eventos por segundo de event time).

El topic tiene 8 particiones y la clave cumple `order_id % 8 == partición`.
Cada productor es dueño de sus particiones y escribe en orden: el event time
es monótono dentro de cada partición (no hay late data artificial).

## Cómo se mide

Todo desde afuera y del mismo modo para los dos motores (`harness/bench.py`):

- **Throughput** (`drain`): el topic se precarga una vez y cada corrida lo
  drena desde `earliest` con un consumer group nuevo. Los dos motores
  commitean offsets en Kafka recién cuando el snapshot de Paimon existe, así
  que el offset commiteado es progreso exactly-once visible. Como avanza a
  escalones (un checkpoint por vez), la tasa es la pendiente de una recta
  ajustada a los instantes de salto entre el 20% y el 95% del backlog (régimen,
  sin el arranque; el arranque se reporta aparte como `first_commit_s`).
- **CPU y memoria**: `cpu.stat` y `memory.stat` (anon) del cgroup del
  contenedor del motor, sobre el mismo tramo.
- **Correctitud**: después de cada drenado se verifica la salida. En etl,
  cada evento no cancelado aparece una vez. En win, cada `(clave, ventana)`
  se compara con la verdad recalculada desde la fórmula del generador
  (`harness/truth_win.py`). Una corrida que no verifica no cuenta.
- **Latencia** (`live`): productores a tasa fija con `event_time = reloj`,
  el harness sigue los snapshots de Paimon y, por cada data file nuevo, lee
  la columna de tiempo del parquet. Latencia de una fila = `timeMillis` del
  snapshot que la hace visible − `event_time` (etl) o − `window_end` (win).

## Correr

```bash
bench/fetch-flink-jars.sh      # jars de Paimon, Kafka y Hadoop para Flink
docker build -t tachyon-bench-flink bench/flink
docker build -t tachyon-benchkit -f bench/Dockerfile.harness bench
docker volume create tachyon-bench-wh
docker update --cpuset-cpus 0-1 tachyon-redpanda   # restaurar: --cpuset-cpus 0-9

bench/run.sh preload           # una vez
REPS=3 bench/suite.sh          # drain + live, motores alternados
python3 bench/report.py        # tablas de resumen
```

Una corrida suelta: `bench/run.sh drain tachyon etl`,
`bench/run.sh live flink win 1000000`. `CPUS`, `MEM`, `COMMIT`, `LAG_MS` y
`PARALLELISM` cambian el escenario; `PROFILE=1` corre Tachyon bajo `perf`.

## Resultados (2026-09-30, Apple M5, 4 CPUs por motor)

Tachyon `benchfast` contra Flink 2.2.1 + Paimon 1.4.2. Cada fila es la
mediana de 3 corridas de throughput o 2 de latencia, alternando motores; toda
corrida de throughput verificó su salida. `python3 bench/report.py` regenera
estas tablas desde `results/`.

### Throughput (backlog drenado, exactly-once, salida verificada)

| workload | cpus | commit | motor | corridas | filas/s (mediana) | min–max | CV entre corridas | cores usados | filas/CPU-s | RSS anon pico |
|---|---|---|---|---|---|---|---|---|---|---|
| etl | 4-7 | 1s | tachyon | 3 | 5.42M | 4.04M–5.45M | 0.13 | 2.78 | 2.00M | 2532 MB |
| etl | 4-7 | 1s | flink | 3 | 2.79M | 2.35M–3.11M | 0.11 | 3.67 | 758K | 2108 MB |
| | | | **Tachyon / Flink** | | **1.94x** (peor Tachyon / mejor Flink: 1.30x) | | | | | |
| win | 4-7 | 1s | tachyon | 3 | 5.17M | 3.43M–5.32M | 0.19 | 2.63 | 2.00M | 2945 MB |
| win | 4-7 | 1s | flink | 3 | 3.07M | 2.51M–3.23M | 0.10 | 3.55 | 918K | 2241 MB |
| | | | **Tachyon / Flink** | | **1.68x** (peor Tachyon / mejor Flink: 1.06x) | | | | | |

### Latencia de punta a punta (tasa fija; evento -> visible en Paimon)

| workload | cpus | commit | lag | tasa | motor | corridas | p50 | p99 | p99.9 | max | std | cores |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| etl | 4-7 | 1s | — | 1.00M/s | tachyon | 2 | 720 ms | 1246 ms | 1330 ms | 1372 ms | 293 ms | 0.61 |
| etl | 4-7 | 1s | — | 1.00M/s | flink | 2 | 843 ms | 1778 ms | 2029 ms | 2141 ms | 334 ms | 2.04 |
| | | | | | **Flink / Tachyon** | | **1.17x** | **1.43x** | | | | |
| etl | 4-7 | 250ms | — | 1.00M/s | tachyon | 2 | 260 ms | 414 ms | 452 ms | 800 ms | 80 ms | 0.62 |
| etl | 4-7 | 250ms | — | 1.00M/s | flink | 2 | 254 ms | 1412 ms | 1656 ms | 1729 ms | 267 ms | 2.25 |
| | | | | | **Flink / Tachyon** | | **0.98x** | **3.41x** | | | | |
| win | 4-7 | 1s | 200 | 1.00M/s | tachyon | 2 | 726 ms | 1475 ms | 1475 ms | 1475 ms | 237 ms | 0.32 |
| win | 4-7 | 1s | 200 | 1.00M/s | flink | 2 | 1180 ms | 1780 ms | 1780 ms | 1780 ms | 112 ms | 1.05 |
| | | | | | **Flink / Tachyon** | | **1.63x** | **1.21x** | | | | |
| win | 4-7 | 250ms | 200 | 1.00M/s | tachyon | 2 | 471 ms | 630 ms | 630 ms | 630 ms | 77 ms | 0.32 |
| win | 4-7 | 250ms | 200 | 1.00M/s | flink | 2 | 521 ms | 1570 ms | 1570 ms | 1570 ms | 198 ms | 1.28 |
| | | | | | **Flink / Tachyon** | | **1.11x** | **2.49x** | | | | |

Lectura: a igual intervalo de commit la latencia mediana está acotada por el
intervalo en los dos motores (≈ intervalo/2 + flush). La diferencia está en la
cola y en el costo: con commits cada 250 ms Tachyon tiene un p99 2.5–3.4x
menor usando 3.5–4x menos CPU.

## Límites conocidos

- La máquina es un Apple M5 (núcleos P y E) con Docker en OrbStack. El VM no
  controla en qué núcleo físico cae cada vCPU: por eso cada escenario se
  repite y se alternan los motores.
- Tachyon se mide con el perfil `benchfast` (`opt-level=3`, LTO thin). El
  perfil `release` (LTO fat, 1 unidad de codegen) no linkea en el VM de 16 GB.
- El sink de los dos es Paimon con `write-only`: la compactación queda fuera
  del camino caliente en ambos.
