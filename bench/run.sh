#!/usr/bin/env bash
# Benchmark Tachyon vs Flink en la misma máquina.
#
#   bench/run.sh preload                       # una vez: carga bench-etl y bench-win
#   bench/run.sh drain <tachyon|flink> <etl|win|news2>
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
SHARDS="${SHARDS:-16}"
KINESIS_EVENTS="${KINESIS_EVENTS:-60000000}"
SQS_EVENTS="${SQS_EVENTS:-500000}"
ncpus() { python3 -c "import sys;s=sys.argv[1];print(sum((int(b)-int(a)+1) if '-' in r else 1 for r in s.split(',') for a,b in [r.split('-') if '-' in r else (r,r)]))" "$1"; }
PARALLELISM="${PARALLELISM:-$(ncpus "$CPUS")}"
# NET se elige por fuente en cada rama: redpanda usa el netns de Redpanda;
# kinesis/sqs usan el de floCi (localhost:4566).
NET="container:tachyon-redpanda"
net_for() { [ "$1" = redpanda ] && echo "container:tachyon-redpanda" || echo "container:tachyon-floci"; }
mkdir -p results .run
OUT="results/$(date +%Y-%m-%d).jsonl"

harness() {
  docker run --rm --network "$NET" --cpuset-cpus "$HARNESS_CPUS" \
    -e NEWS2_PATIENTS="${NEWS2_PATIENTS:-250}" \
    -e NEWS2_MINUTES="${NEWS2_MINUTES:-2000}" \
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

start_engine() { # start_engine <engine> <kind> <source>; exporta CID y GROUP
  local engine=$1 kind=$2 source=$3
  export NAME="bench-$kind-$(date +%s)" COMMIT LAG_MS PARALLELISM
  stop_engines
  if [ "$engine" = flink ]; then
    export GROUP="flink-$NAME"
    local tpl="flink/$kind.sql"
    [ "$source" != redpanda ] && tpl="flink/$kind-$source.sql"
    render "$tpl" ".run/job.sql"
    CID=$(docker run -d --name bench-flink --network "$NET" --cpuset-cpus "$CPUS" --memory "$MEM" \
      -v tachyon-bench-wh:/wh -v "$BENCH":/bench \
      -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
      --entrypoint bash tachyon-bench-flink \
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
    local tpl="tachyon/$kind.yaml"
    [ "$source" != redpanda ] && tpl="tachyon/$kind-$source.yaml"
    render "$tpl" ".run/pipeline.yaml"
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
      -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
      -v tachyon-bench-wh:/wh -v "$BENCH":/bench -v tachyon-target:/work/target:ro \
      --entrypoint "${entry[0]}" "$image" "${entry[@]:1}" \
      --config /bench/.run/pipeline.yaml --sql /bench/.run/pipeline.sql)
  fi
  export CID
}

start_sink_engine() { # start_sink_engine <engine> <source>; exporta CID
  # Sink de Kinesis/SQS: input de AWS -> output de AWS (sin tabla Paimon).
  # Flink solo tiene sink de Kinesis; SQS se mide solo con Tachyon.
  local engine=$1 source=$2
  export NAME="bench-sink-$(date +%s)" COMMIT PARALLELISM
  stop_engines
  if [ "$engine" = flink ]; then
    render "flink/etl-kinesis-sink.sql" ".run/job.sql"
    CID=$(docker run -d --name bench-flink --network "$NET" --cpuset-cpus "$CPUS" --memory "$MEM" \
      -v tachyon-bench-wh:/wh -v "$BENCH":/bench \
      -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
      --entrypoint bash tachyon-bench-flink \
      -c 'bin/jobmanager.sh start && bin/taskmanager.sh start && sleep infinity')
    for _ in $(seq 60); do
      docker exec bench-flink curl -sf localhost:18081/overview 2>/dev/null | grep -q '"slots-total":[1-9]' && break
      sleep 0.5
    done
    docker exec bench-flink bin/sql-client.sh -f /bench/.run/job.sql > .run/submit.log 2>&1
    grep -q "Job ID" .run/submit.log || { cat .run/submit.log; exit 1; }
  else
    local tpl="tachyon/etl-$source-sink.yaml"
    render "$tpl" ".run/pipeline.yaml"
    cp "tachyon/etl-sink.sql" .run/pipeline.sql
    local image=tachyon-build entry=("$TACHYON_BIN") extra=()
    if [ -n "${PROFILE:-}" ]; then
      image=tachyon-prof extra=(--privileged)
      entry=(perf record -F 499 -g -o /bench/.run/perf.data -- "$TACHYON_BIN")
    fi
    for v in $(env | grep -o '^TACHYON_DEBUG_[A-Z_]*' || true); do extra+=(-e "$v=1"); done
    CID=$(docker run -d --name bench-tachyon --network "$NET" --cpuset-cpus "$CPUS" --memory "$MEM" \
      ${extra[@]+"${extra[@]}"} -e RUST_LOG="${RUST_LOG:-warn}" -e TACHYON_INSTANCE_ID=bench \
      -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
      -v tachyon-bench-wh:/wh -v "$BENCH":/bench -v tachyon-target:/work/target:ro \
      --entrypoint "${entry[0]}" "$image" "${entry[@]:1}" \
      --config /bench/.run/pipeline.yaml --sql /bench/.run/pipeline.sql)
  fi
  export CID
}

meta() { # contexto de la corrida para el jsonl
  python3 -c 'import json,sys,os; r=json.loads(sys.stdin.read()); r.update({"source":os.environ.get("SOURCE","redpanda"),"cpus":os.environ["CPUS"],"mem":os.environ["MEM"],"commit":os.environ["COMMIT"],"parallelism":int(os.environ["PARALLELISM"]),"lag_ms":int(os.environ["LAG_MS"]),"tag":os.environ.get("TAG",""),"tachyon_bin":os.environ["TACHYON_BIN"],"ts":int(__import__("time").time())}); print(json.dumps(r))'
}
LAG_S=$(python3 -c "print(f'{${LAG_MS}/1000:.3f}')")
# Líneas extra de `deployment:` para experimentos (p. ej. "  decode_parallelism: 8").
TACHYON_EXTRA="${TACHYON_EXTRA:-}"
export CPUS MEM COMMIT PARALLELISM LAG_MS LAG_S TACHYON_EXTRA TACHYON_BIN

drain_news2() {
  local engine=$1
  export LAG_MS=10000
  LAG_S=$(python3 -c "print(f'{${LAG_MS}/1000:.3f}')")
  export LAG_S SOURCE=redpanda
  export NEWS2_PATIENTS="${NEWS2_PATIENTS:-250}"
  export NEWS2_MINUTES="${NEWS2_MINUTES:-2000}"
  export TOPIC=bench-news2
  export NAME="bench-news2-$(date +%s)"
  export GROUP="flink-$NAME"
  NET="container:tachyon-redpanda"
  python3 harness/news2.py --render "$engine" .run
  harness preload --source redpanda --topic "$TOPIC" --kind news2 --events 0 \
    | tee .run/news2-preload.json
  local wide
  wide=$(python3 -c 'import json; print(json.load(open(".run/news2-preload.json"))["wide_rows"])')
  stop_engines
  docker run --rm -v tachyon-bench-wh:/wh tachyon-benchkit \
    sh -c "rm -rf /wh/$engine/delta.db /wh/flink-checkpoints; mkdir -p /wh/$engine; chmod -R 777 /wh"
  docker run --rm --cpuset-cpus "$HARNESS_CPUS" -v tachyon-bench-wh:/wh -v "$BENCH":/bench \
    --entrypoint bash tachyon-bench-flink -c "bin/sql-client.sh -f /bench/.run/ddl.sql" \
    > .run/ddl.log 2>&1 || { cat .run/ddl.log; exit 1; }
  if grep -q "ERROR" .run/ddl.log; then cat .run/ddl.log; exit 1; fi
  if [ "$engine" = flink ]; then
    CID=$(docker run -d --name bench-flink --network "$NET" --cpuset-cpus "$CPUS" --memory "$MEM" \
      -v tachyon-bench-wh:/wh -v "$BENCH":/bench \
      -v "$BENCH/flink/config-news2.yaml:/opt/flink/conf/config.yaml:ro" \
      --entrypoint bash tachyon-bench-flink \
      -c 'bin/jobmanager.sh start && bin/taskmanager.sh start && sleep infinity')
    for _ in $(seq 60); do
      docker exec bench-flink curl -sf localhost:18081/overview 2>/dev/null | grep -q '"slots-total":[1-9]' && break
      sleep 0.5
    done
    if ! docker exec bench-flink bin/sql-client.sh -f /bench/.run/job.sql > .run/submit.log 2>&1; then
      cat .run/submit.log
      exit 1
    fi
    grep -q "Job ID" .run/submit.log || { cat .run/submit.log; exit 1; }
  else
    CID=$(docker run -d --name bench-tachyon --network "$NET" --cpuset-cpus "$CPUS" --memory "$MEM" \
      -e RUST_LOG="${RUST_LOG:-warn}" -e TACHYON_INSTANCE_ID=bench \
      -v tachyon-bench-wh:/wh -v "$BENCH":/bench -v tachyon-target:/work/target:ro \
      --entrypoint bash tachyon-build /bench/.run/news2-tachyon.sh)
    # 1s basta para ver si el proceso murió al arrancar. Con 4s, en esta
    # escala el wide table a veces ya pasó el 20% y la medición no ve saltos.
    sleep 1
    docker ps --format '{{.Names}}' | grep -qx bench-tachyon || {
      echo "tachyon no quedó en pie" >&2
      docker logs bench-tachyon 2>&1 | tail -80 || true
      cat .run/news2/*.log 2>/dev/null || true
      exit 1
    }
  fi
  export CID
  local metrics=()
  if [ "$engine" = tachyon ]; then
    metrics=(--metrics http://127.0.0.1:9464/metrics)
  fi
  harness drain --engine "$engine" --kind news2 --source redpanda \
    --topic "$TOPIC" --table "/wh/$engine/delta.db/news2_wide" --group "$GROUP" \
    --container "$CID" --timeout "${TIMEOUT:-900}" --backlog "$wide" \
    ${metrics[@]+"${metrics[@]}"} | meta > .run/drain.json
  sleep 3
  docker logs "$CID" > ".run/$engine-news2.log" 2>&1 || true
  stop_engines
  harness truth_news2 --topic "$TOPIC" --warehouse "/wh/$engine" > .run/verify.json
  python3 -c 'import json; r=json.load(open(".run/drain.json")); v=json.loads(open(".run/verify.json").read().strip().splitlines()[-1]); r["verify"]=v; print(json.dumps(r))' | tee -a "$OUT"
}

case "${1:-}" in
  preload)
    source="${2:-redpanda}"
    export SOURCE="$source"
    NET=$(net_for "$source")
    case "$source" in
      redpanda)
        harness preload --source redpanda --topic bench-etl --kind etl --events "${ETL_EVENTS:-60000000}"
        harness preload --source redpanda --topic bench-win --kind win --events "${WIN_EVENTS:-60000000}"
        ;;
      kinesis)
        harness preload --source kinesis --stream bench-etl --shards "$SHARDS" --kind etl --events "$KINESIS_EVENTS"
        ;;
      sqs)
        harness preload --source sqs --queue bench-etl --kind etl --events "$SQS_EVENTS"
        ;;
    esac
    ;;
  drain)
    if [ "${3:-}" = news2 ]; then
      drain_news2 "$2"
      exit 0
    fi
    engine=$2 kind=$3 source="${4:-redpanda}"
    export SOURCE="$source"
    NET=$(net_for "$source")
    export TOPIC="bench-$kind" STREAM="bench-$kind" QUEUE="bench-$kind"
    case "$source" in
      redpanda) EVENTS=0 ;;
      kinesis) EVENTS="$KINESIS_EVENTS" ;;
      sqs) EVENTS="$SQS_EVENTS" ;;
    esac
    # SQS: cola destructiva -> precarga fresca justo antes del motor.
    [ "$source" = sqs ] && harness preload --source sqs --queue "$QUEUE" --kind "$kind" --events "$SQS_EVENTS"
    stop_engines; reset_table "$engine" "$kind"
    start_engine "$engine" "$kind" "$source"
    harness drain --engine "$engine" --kind "$kind" --source "$source" \
      --topic "$TOPIC" --stream "$STREAM" --queue "$QUEUE" --events "$EVENTS" \
      --table "/wh/$engine/default.db/${kind}_lake" --group "$GROUP" \
      --container "$CID" --timeout "${TIMEOUT:-600}" \
      $( [ "$engine" = tachyon ] && echo --metrics http://127.0.0.1:9464/metrics ) | meta > .run/drain.json
    # Deja cerrar el último epoch y verifica la salida: una tasa sin salida
    # correcta no cuenta.
    sleep 3
    docker logs "$CID" > ".run/$engine-$kind.log" 2>&1 || true
    stop_engines
    table="/wh/$engine/default.db/${kind}_lake"
    if [ "$kind" = etl ]; then
      if [ "$source" = redpanda ]; then
        harness verify_etl "$table" "$TOPIC" > .run/verify.json
      else
        harness etl_truth "$source" "$table" "$EVENTS" > .run/verify.json
      fi
    else
      harness truth_win "$TOPIC" "$table" > .run/verify.json
    fi
    python3 -c 'import json; r=json.load(open(".run/drain.json")); v=json.loads(open(".run/verify.json").read().strip().splitlines()[-1]); r["verify"]=v; print(json.dumps(r))' | tee -a "$OUT"
    ;;
  live)
    engine=$2 kind=$3 rate=$4 source="${5:-redpanda}"
    export SOURCE="$source"
    NET=$(net_for "$source")
    export TOPIC="bench-live-$kind" STREAM="bench-live-$kind" QUEUE="bench-live-$kind"
    stop_engines; reset_table "$engine" "$kind"
    [ "$source" = redpanda ] && harness mktopic --topic "$TOPIC"
    # Kinesis/SQS: el motor exige que el stream/queue exista al arrancar.
    case "$source" in
      kinesis) harness preload --source kinesis --stream "$STREAM" --shards "$SHARDS" --kind "$kind" --events 0 ;;
      sqs) harness preload --source sqs --queue "$QUEUE" --kind "$kind" --events 0 ;;
    esac
    start_engine "$engine" "$kind" "$source"
    sleep "${WARMUP:-10}"
    harness live --engine "$engine" --kind "$kind" --source "$source" \
      --topic "$TOPIC" --stream "$STREAM" --queue "$QUEUE" --shards "$SHARDS" \
      --container "$CID" --table "/wh/$engine/default.db/${kind}_lake" --rate "$rate" --duration "$DURATION" \
      | meta | tee -a "$OUT"
    docker logs "$CID" > ".run/$engine-$kind-live.log" 2>&1 || true
    stop_engines
    ;;
  sink)
    engine=$2 source="${3:-kinesis}"
    if [ "$engine" = flink ] && [ "$source" = sqs ]; then
      echo "Flink no tiene sink de SQS: la corrida sqs se mide solo con Tachyon" >&2
      exit 1
    fi
    export SOURCE="$source"
    NET=$(net_for "$source")
    case "$source" in
      kinesis) EVENTS="${SINK_KINESIS_EVENTS:-20000000}" ;;
      sqs) EVENTS="${SINK_SQS_EVENTS:-500000}" ;;
    esac
    export STREAM="bench-etl-sink" QUEUE="bench-etl-sink" \
           OUT_STREAM="bench-etl-sink-out" OUT_QUEUE="bench-etl-sink-out"
    # Input fresco: el meta guarda el base_ms exacto para la verificación.
    harness preload --source "$source" --stream "$STREAM" --queue "$QUEUE" \
      --shards "$SHARDS" --kind etl --events "$EVENTS"
    # Salida fresca.
    harness mksink --source "$source" --stream "$OUT_STREAM" --queue "$OUT_QUEUE" --shards "$SHARDS"
    start_sink_engine "$engine" "$source"
    harness drain_sink --engine "$engine" --kind etl --source "$source" \
      --container "$CID" --warmup "${WARMUP:-10}" --duration "${DURATION:-30}" \
      | meta > .run/drain_sink.json
    # Deja cerrar el último tramo y verifica la salida: una tasa sin salida
    # correcta no cuenta.
    sleep 3
    docker logs "$CID" > ".run/$engine-sink.log" 2>&1 || true
    stop_engines
    harness verify_sink --source "$source" --stream "$OUT_STREAM" --queue "$OUT_QUEUE" --events "$EVENTS" > .run/verify_sink.json
    python3 -c 'import json; d=json.load(open(".run/drain_sink.json")); v=json.loads(open(".run/verify_sink.json").read().strip().splitlines()[-1]); span=d["warmup_s"]+d["window_s"]; d["rows_s"]=round(v["output_records"]/span); d["rows_per_cpu_s"]=round(v["output_records"]/max(d["cpu_cores"]*span,1e-9)); d["verify"]=v; print(json.dumps(d))' | tee -a "$OUT"
    ;;
  live-sink)
    # Latencia del sink de Kinesis (punta a punta): produce a tasa fija y
    # sigue la salida. Solo Kinesis (Flink no tiene sink de SQS).
    engine=$2
    export SOURCE="kinesis"
    NET=$(net_for kinesis)
    export STREAM="bench-live-sink" OUT_STREAM="bench-live-sink-out"
    # Input y salida frescos (vacíos): el motor espera en el input vacío.
    harness preload --source kinesis --stream "$STREAM" --shards "$SHARDS" --kind etl --events 0
    harness mksink --source kinesis --stream "$OUT_STREAM" --shards "$SHARDS"
    start_sink_engine "$engine" "kinesis"
    harness live_sink --engine "$engine" --kind etl --source kinesis \
      --stream "$STREAM" --out-stream "$OUT_STREAM" --container "$CID" \
      --rate "${RATE:-5000}" --duration "${DURATION:-30}" --settle "${SETTLE:-8}" \
      | meta | tee -a "$OUT"
    docker logs "$CID" > ".run/$engine-sink-live.log" 2>&1 || true
    stop_engines
    ;;
  *) sed -n 2,12p "$0"; exit 1 ;;
esac
