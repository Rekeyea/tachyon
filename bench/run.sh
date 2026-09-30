#!/usr/bin/env bash
# Benchmark Tachyon vs Flink en la misma máquina.
#
#   bench/run.sh preload                       # una vez: carga bench-etl y bench-win
#   bench/run.sh drain <tachyon|flink> <etl|win>
#   bench/run.sh live  <tachyon|flink> <etl|win> <rate>
#
# Variables: CPUS (cpuset del motor, default 4-7), MEM (tope, default 4608m),
# COMMIT (intervalo de commit/checkpoint, default 1s), LAG_MS (watermark, 1000),
# PARALLELISM (Flink, default = #CPUS), TACHYON_BIN (default benchfast),
# DURATION (live, 30s). Cada resultado se agrega a bench/results/<fecha>.jsonl.
set -euo pipefail
cd "$(dirname "$0")"
BENCH="$PWD"

CPUS="${CPUS:-4-7}"
MEM="${MEM:-4608m}"
COMMIT="${COMMIT:-1s}"
LAG_MS="${LAG_MS:-1000}"
DURATION="${DURATION:-30}"
TACHYON_BIN="${TACHYON_BIN:-/work/target/benchfast/tachyon}"
HARNESS_CPUS="${HARNESS_CPUS:-2-3,8-9}"
ncpus() { python3 -c "import sys;s=sys.argv[1];print(sum((int(b)-int(a)+1) if '-' in r else 1 for r in s.split(',') for a,b in [r.split('-') if '-' in r else (r,r)]))" "$1"; }
PARALLELISM="${PARALLELISM:-$(ncpus "$CPUS")}"
NET="container:tachyon-redpanda"
mkdir -p results .run
OUT="results/$(date +%Y-%m-%d).jsonl"

harness() {
  docker run --rm --network "$NET" --cpuset-cpus "$HARNESS_CPUS" \
    -v tachyon-bench-wh:/wh -v /sys/fs/cgroup:/cg:ro -v "$BENCH":/bench \
    tachyon-benchkit python harness/bench.py "$@"
}

render() { # render <plantilla> <salida>: reemplaza ${VAR} con el entorno
  python3 -c 'import os,re,sys; s=open(sys.argv[1]).read(); open(sys.argv[2],"w").write(re.sub(r"\$\{(\w+)\}", lambda m: os.environ[m.group(1)], s))' "$1" "$2"
}

reset_table() { # reset_table <engine> <kind>: tabla vacía con la DDL común
  local engine=$1 kind=$2
  docker run --rm -v tachyon-bench-wh:/wh tachyon-benchkit \
    sh -c "rm -rf /wh/$engine/default.db/${kind}_lake /wh/flink-checkpoints; mkdir -p /wh/$engine; chmod -R 777 /wh"
  WAREHOUSE="/wh/$engine" render "flink/table-$kind.sql" ".run/table-$engine-$kind.sql"
  docker run --rm --cpuset-cpus "$HARNESS_CPUS" -v tachyon-bench-wh:/wh -v "$BENCH":/bench \
    --entrypoint bash tachyon-bench-flink -c "bin/sql-client.sh -f /bench/.run/table-$engine-$kind.sql" \
    > ".run/ddl.log" 2>&1 || { cat .run/ddl.log; exit 1; }
  if grep -q "ERROR" .run/ddl.log; then cat .run/ddl.log; exit 1; fi
}

stop_engines() {
  docker stop -t 5 bench-flink bench-tachyon >/dev/null 2>&1 || true
  docker rm -f bench-flink bench-tachyon >/dev/null 2>&1 || true
}

start_engine() { # start_engine <engine> <kind> <topic>; exporta CID y GROUP
  local engine=$1 kind=$2 topic=$3
  export NAME="bench-$kind-$(date +%s)" TOPIC="$topic" COMMIT LAG_MS PARALLELISM
  stop_engines
  if [ "$engine" = flink ]; then
    export GROUP="flink-$NAME"
    render "flink/$kind.sql" ".run/job.sql"
    CID=$(docker run -d --name bench-flink --network "$NET" --cpuset-cpus "$CPUS" --memory "$MEM" \
      -v tachyon-bench-wh:/wh -v "$BENCH":/bench --entrypoint bash tachyon-bench-flink \
      -c 'bin/jobmanager.sh start && bin/taskmanager.sh start && sleep infinity')
    for _ in $(seq 60); do
      docker exec bench-flink curl -sf localhost:18081/overview 2>/dev/null | grep -q '"slots-total":[1-9]' && break
      sleep 0.5
    done
    docker exec bench-flink bin/sql-client.sh -f /bench/.run/job.sql > .run/submit.log 2>&1
    grep -q "Job ID" .run/submit.log || { cat .run/submit.log; exit 1; }
  else
    # Tachyon commitea en un grupo por input: tachyon-<pipeline>-<input>.
    local input; [ "$kind" = etl ] && input=orders || input=events
    export GROUP="tachyon-$NAME-$input"
    render "tachyon/$kind.yaml" ".run/pipeline.yaml"
    cp "tachyon/$kind.sql" .run/pipeline.sql
    # PROFILE=1: el mismo binario bajo `perf record` (imagen tachyon-prof).
    local image=tachyon-build entry=("$TACHYON_BIN") extra=()
    if [ -n "${PROFILE:-}" ]; then
      image=tachyon-prof extra=(--privileged)
      entry=(perf record -F 499 -g -o /bench/.run/perf.data -- "$TACHYON_BIN")
    fi
    # Diagnóstico: cualquier TACHYON_DEBUG_* del entorno pasa al contenedor.
    for v in $(env | grep -o '^TACHYON_DEBUG_[A-Z_]*' || true); do extra+=(-e "$v=1"); done
    CID=$(docker run -d --name bench-tachyon --network "$NET" --cpuset-cpus "$CPUS" --memory "$MEM" \
      ${extra[@]+"${extra[@]}"} -e RUST_LOG="${RUST_LOG:-warn}" -e TACHYON_INSTANCE_ID=bench \
      -v tachyon-bench-wh:/wh -v "$BENCH":/bench -v tachyon-target:/work/target:ro \
      --entrypoint "${entry[0]}" "$image" "${entry[@]:1}" \
      --config /bench/.run/pipeline.yaml --sql /bench/.run/pipeline.sql)
  fi
  export CID
}

meta() { # contexto de la corrida para el jsonl
  python3 -c 'import json,sys,os; r=json.loads(sys.stdin.read()); r.update({"cpus":os.environ["CPUS"],"mem":os.environ["MEM"],"commit":os.environ["COMMIT"],"parallelism":int(os.environ["PARALLELISM"]),"lag_ms":int(os.environ["LAG_MS"]),"tag":os.environ.get("TAG",""),"tachyon_bin":os.environ["TACHYON_BIN"],"ts":int(__import__("time").time())}); print(json.dumps(r))'
}
LAG_S=$(python3 -c "print(f'{${LAG_MS}/1000:.3f}')")
# Líneas extra de `deployment:` para experimentos (p. ej. "  decode_parallelism: 8").
TACHYON_EXTRA="${TACHYON_EXTRA:-}"
export CPUS MEM COMMIT PARALLELISM LAG_MS LAG_S TACHYON_EXTRA TACHYON_BIN

case "${1:-}" in
  preload)
    harness preload --topic bench-etl --kind etl --events "${ETL_EVENTS:-60000000}"
    harness preload --topic bench-win --kind win --events "${WIN_EVENTS:-60000000}"
    ;;
  drain)
    engine=$2 kind=$3
    stop_engines; reset_table "$engine" "$kind"
    start_engine "$engine" "$kind" "bench-$kind"
    harness drain --engine "$engine" --kind "$kind" --topic "bench-$kind" --group "$GROUP" \
      --container "$CID" --timeout "${TIMEOUT:-600}" \
      $( [ "$engine" = tachyon ] && echo --metrics http://127.0.0.1:9464/metrics ) | meta > .run/drain.json
    # Deja cerrar el último epoch y verifica la salida: una tasa sin salida
    # correcta no cuenta.
    sleep 3
    docker logs "$CID" > ".run/$engine-$kind.log" 2>&1 || true
    stop_engines
    table="/wh/$engine/default.db/${kind}_lake"
    if [ "$kind" = etl ]; then
      harness verify_etl "$table" bench-etl > .run/verify.json
    else
      harness truth_win bench-win "$table" > .run/verify.json
    fi
    python3 -c 'import json; r=json.load(open(".run/drain.json")); v=json.loads(open(".run/verify.json").read().strip().splitlines()[-1]); r["verify"]=v; print(json.dumps(r))' | tee -a "$OUT"
    ;;
  live)
    engine=$2 kind=$3 rate=$4
    topic="bench-live-$kind"
    stop_engines; reset_table "$engine" "$kind"
    harness mktopic --topic "$topic"
    start_engine "$engine" "$kind" "$topic"
    sleep "${WARMUP:-10}"
    harness live --engine "$engine" --kind "$kind" --topic "$topic" --container "$CID" \
      --table "/wh/$engine/default.db/${kind}_lake" --rate "$rate" --duration "$DURATION" \
      | meta | tee -a "$OUT"
    docker logs "$CID" > ".run/$engine-$kind-live.log" 2>&1 || true
    stop_engines
    ;;
  *) sed -n 2,12p "$0"; exit 1 ;;
esac
