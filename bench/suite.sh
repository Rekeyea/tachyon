#!/usr/bin/env bash
# Suite completa Tachyon vs Flink (misma máquina, mismo input, mismos CPUs).
#   REPS=3 bench/suite.sh            # throughput + latencia
#   PHASES="drain" bench/suite.sh    # solo throughput
# Los resultados se agregan a bench/results/<fecha>.jsonl; `python3
# bench/report.py` los resume.
set -euo pipefail
cd "$(dirname "$0")"
REPS="${REPS:-3}"
PHASES="${PHASES:-drain live}"
export TACHYON_BIN="${TACHYON_BIN:-/work/target/release/tachyon}"

# Alterna motores en cada repetición: una deriva de la máquina (térmica,
# page cache) no favorece a uno solo.
for phase in $PHASES; do
  case "$phase" in
    drain)
      for kind in etl win; do
        for rep in $(seq "$REPS"); do
          for engine in flink tachyon; do
            echo ">> drain $engine $kind ($rep/$REPS)" >&2
            ./run.sh drain "$engine" "$kind" | tail -1 | cut -c1-160
          done
        done
      done
      ;;
    live)
      for spec in "etl ${LIVE_ETL_RATE:-1000000}" "win ${LIVE_WIN_RATE:-1000000}"; do
        set -- $spec
        for rep in $(seq "$REPS"); do
          for engine in flink tachyon; do
            echo ">> live $engine $1 @ $2/s ($rep/$REPS)" >&2
            LAG_MS="${LIVE_LAG_MS:-200}" ./run.sh live "$engine" "$1" "$2" | tail -1 | cut -c1-200
          done
        done
      done
      ;;
  esac
done
