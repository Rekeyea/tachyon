#!/usr/bin/env bash
# Suite completa Tachyon vs Flink (misma máquina, mismo input, mismos CPUs).
#   REPS=3 bench/suite.sh            # throughput + latencia
#   PHASES="drain" bench/suite.sh    # solo throughput
#   SOURCES="redpanda kinesis" bench/suite.sh
# Los resultados se agregan a bench/results/<fecha>.jsonl; `python3
# bench/report.py` los resume.
set -euo pipefail
cd "$(dirname "$0")"
REPS="${REPS:-3}"
PHASES="${PHASES:-drain live}"
SOURCES="${SOURCES:-redpanda kinesis sqs}"
export TACHYON_BIN="${TACHYON_BIN:-/work/target/release/tachyon}"

drain_kinds() { # <source>: workloads del drain (win solo Redpanda)
  case "$1" in
    redpanda) printf 'etl win\n' ;;
    *) printf 'etl\n' ;;
  esac
}

live_specs() { # <source>: "<kind> <rate>" por línea. Las tasas reflejan el
  # cuello de cada fuente: Redpanda produce 1M/s; Kinesis ~200K rec/s en
  # floCi; SQS ~1.3K msg/s (floCi serializa).
  case "$1" in
    redpanda) printf 'etl %s\nwin %s\n' "${LIVE_ETL_RATE:-1000000}" "${LIVE_WIN_RATE:-1000000}" ;;
    kinesis) printf 'etl %s\n' "${LIVE_KINESIS_RATE:-100000}" ;;
    sqs) printf 'etl %s\n' "${LIVE_SQS_RATE:-500}" ;;
  esac
}

# Alterna motores en cada repetición: una deriva de la máquina (térmica,
# page cache) no favorece a uno solo. Flink no tiene source de SQS.
for phase in $PHASES; do
  case "$phase" in
    drain)
      for source in $SOURCES; do
        for kind in $(drain_kinds "$source"); do
          for rep in $(seq "$REPS"); do
            for engine in flink tachyon; do
              [ "$source" = sqs ] && [ "$engine" = flink ] && continue
              echo ">> drain $engine $kind $source ($rep/$REPS)" >&2
              ./run.sh drain "$engine" "$kind" "$source" | tail -1 | cut -c1-160
            done
          done
        done
      done
      ;;
    live)
      for source in $SOURCES; do
        while read -r kind rate; do
          for rep in $(seq "$REPS"); do
            for engine in flink tachyon; do
              [ "$source" = sqs ] && [ "$engine" = flink ] && continue
              echo ">> live $engine $kind $source @ $rate/s ($rep/$REPS)" >&2
              LAG_MS="${LIVE_LAG_MS:-200}" ./run.sh live "$engine" "$kind" "$rate" "$source" | tail -1 | cut -c1-200
            done
          done
        done < <(live_specs "$source")
      done
      ;;
  esac
done
