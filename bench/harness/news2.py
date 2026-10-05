"""Input y verdad del pipeline NEWS2 que separa por medición.

La fórmula no lee la salida de ningún motor. Un evento entra en la ventana
`[start, start + 60s)` con `start = timestamp - timestamp % 60000`. El score
de cada valor es el CASE de la tesis. En la fila ancha, value es el máximo
del minuto y score es el máximo de esos scores; el vital que no llegó vale
0 y `news2_score` es la suma. La ventana cierra cuando el watermark (el
mínimo de los máximos por tipo, menos 10s) pasa su fin. El evento de cierre,
uno por tipo, deja la última ventana abierta.
"""
import json
import os
import sys

TYPES = (
    "RESPIRATORY_RATE",
    "OXYGEN_SATURATION",
    "BLOOD_PRESSURE_SYSTOLIC",
    "HEART_RATE",
    "TEMPERATURE",
    "CONSCIOUSNESS",
)

# Sufijo de tabla. El de presión sigue el nombre de la tesis (`blood_pressure`).
SUFFIX = {
    "RESPIRATORY_RATE": "respiratory_rate",
    "OXYGEN_SATURATION": "oxygen_saturation",
    "BLOOD_PRESSURE_SYSTOLIC": "blood_pressure_systolic",
    "HEART_RATE": "heart_rate",
    "TEMPERATURE": "temperature",
    "CONSCIOUSNESS": "consciousness",
}

VALUE_COLUMN = {
    "RESPIRATORY_RATE": "respiratory_rate_value",
    "OXYGEN_SATURATION": "oxygen_saturation_value",
    "BLOOD_PRESSURE_SYSTOLIC": "blood_pressure_value",
    "HEART_RATE": "heart_rate_value",
    "TEMPERATURE": "temperature_value",
    "CONSCIOUSNESS": "consciousness_value",
}

SCORE_COLUMN = {
    "RESPIRATORY_RATE": "respiratory_rate_score",
    "OXYGEN_SATURATION": "oxygen_saturation_score",
    "BLOOD_PRESSURE_SYSTOLIC": "blood_pressure_score",
    "HEART_RATE": "heart_rate_score",
    "TEMPERATURE": "temperature_score",
    "CONSCIOUSNESS": "consciousness_score",
}

WIDE_COLUMNS = (
    ["patient_id", "window_start", "window_end"]
    + [VALUE_COLUMN[typ] for typ in TYPES]
    + [SCORE_COLUMN[typ] for typ in TYPES]
    + ["news2_score"]
)

WINDOW_MS = 60_000
LAG_MS = 10_000
# Alineado a un minuto: 1_700_000_040_000 % 60_000 == 0.
BASE_MS = 1_700_000_040_000
TOPIC_PARTITIONS = 1

_RR = (8.0, 10.0, 16.0, 22.0, 26.0)
_O2 = (91.0, 93.0, 95.0, 97.0, 90.0)
_BP = (90.0, 100.0, 110.0, 120.0, 220.0)
_HR = (40.0, 45.0, 70.0, 100.0, 120.0)
_TEMP = (35.0, 35.5, 37.0, 38.5, 39.25)
_CONSCIOUS = (1.0, 0.0, 1.0, 2.0, 1.0)


def scale():
    patients = int(os.environ.get("NEWS2_PATIENTS", "80"))
    minutes = int(os.environ.get("NEWS2_MINUTES", "250"))
    if patients < 2 or minutes < 2:
        raise SystemExit("NEWS2 necesita al menos 2 pacientes y 2 minutos")
    return patients, minutes


def patient_id(patient):
    return f"p{patient:04d}"


def measurement_value(typ, patient, minute):
    band = (patient * 3 + minute) % 5
    table = {
        "RESPIRATORY_RATE": _RR,
        "OXYGEN_SATURATION": _O2,
        "BLOOD_PRESSURE_SYSTOLIC": _BP,
        "HEART_RATE": _HR,
        "TEMPERATURE": _TEMP,
        "CONSCIOUSNESS": _CONSCIOUS,
    }[typ]
    return table[band]


def vital_missing(typ, patient, minute):
    """Un paciente-minuto sin oximetría. La tabla del tipo no queda vacía."""
    return typ == "OXYGEN_SATURATION" and patient % 11 == 0 and minute % 7 == 0


def score(typ, value):
    """CASE de scoring.sql. None es el hueco que la tesis deja en NULL."""
    if typ == "RESPIRATORY_RATE":
        if value <= 8:
            return 3
        if value >= 25:
            return 3
        if 21 <= value <= 24:
            return 2
        if 9 <= value <= 11:
            return 1
        if 12 <= value <= 20:
            return 0
        return None
    if typ == "OXYGEN_SATURATION":
        if value <= 91:
            return 3
        if 92 <= value <= 93:
            return 2
        if 94 <= value <= 95:
            return 1
        if value >= 96:
            return 0
        return None
    if typ == "BLOOD_PRESSURE_SYSTOLIC":
        if value <= 90:
            return 3
        if value <= 100:
            return 2
        if value <= 110:
            return 1
        if 111 <= value <= 219:
            return 0
        if value >= 220:
            return 3
        return None
    if typ == "HEART_RATE":
        if value <= 40:
            return 3
        if value >= 131:
            return 3
        if 111 <= value <= 130:
            return 2
        if 41 <= value <= 50:
            return 1
        if 91 <= value <= 110:
            return 1
        if 51 <= value <= 90:
            return 0
        return None
    if typ == "TEMPERATURE":
        if value <= 35.0:
            return 3
        if value >= 39.1:
            return 2
        if 38.1 <= value <= 39.0:
            return 1
        if 35.1 <= value <= 36.0:
            return 1
        if 36.1 <= value <= 38.0:
            return 0
        return None
    if typ == "CONSCIOUSNESS":
        return 0 if value == 1 else 3
    raise KeyError(typ)


def iter_events(patients, minutes, base_ms=BASE_MS):
    """Eventos del topic, en orden de event time, y un cierre por tipo."""
    event_id = 1
    for minute in range(minutes):
        ts = base_ms + minute * WINDOW_MS + 1_000
        for patient in range(patients):
            for typ in TYPES:
                if vital_missing(typ, patient, minute):
                    continue
                value = measurement_value(typ, patient, minute)
                yield {
                    "event_id": event_id,
                    "patient_id": patient_id(patient),
                    "measurement_type": typ,
                    "value": value,
                    "measurement_timestamp": ts,
                }
                event_id += 1
    # Un cierre por paciente y tipo. Flink reparte el watermark por bucket;
    # un solo paciente no adelanta el reloj de los demás. La ventana del
    # cierre queda abierta (su fin es posterior al watermark).
    close_ts = base_ms + minutes * WINDOW_MS + LAG_MS
    for patient in range(patients):
        for typ in TYPES:
            yield {
                "event_id": event_id,
                "patient_id": patient_id(patient),
                "measurement_type": typ,
                "value": measurement_value(typ, patient, 0),
                "measurement_timestamp": close_ts,
            }
            event_id += 1


def payload(event):
    return event["patient_id"], (
        '{"event_id":%d,"patient_id":"%s","measurement_type":"%s","value":%s,"measurement_timestamp":%d}'
        % (
            event["event_id"],
            event["patient_id"],
            event["measurement_type"],
            _num(event["value"]),
            event["measurement_timestamp"],
        )
    )


def _num(value):
    if value == int(value):
        return str(int(value))
    return repr(value)


def fold(events, lag_ms=LAG_MS, window_ms=WINDOW_MS):
    """Filas anchas cerradas. `events` es iterable de dicts del topic."""
    per_type_max = {}
    bucket = {}
    for event in events:
        typ = event["measurement_type"]
        if typ not in SUFFIX:
            raise ValueError(f"tipo desconocido {typ}")
        value = float(event["value"])
        scored = score(typ, value)
        if scored is None:
            raise ValueError(f"{typ}={value} cae en un hueco del CASE")
        ts = int(event["measurement_timestamp"])
        patient = event["patient_id"]
        per_type_max[typ] = max(per_type_max.get(typ, ts), ts)
        start = ts - ts % window_ms
        key = (patient, start, typ)
        prev = bucket.get(key)
        # MAX(value) y MAX(score) van por separado, como el pivot de la tesis.
        if prev is None:
            bucket[key] = [value, scored]
        else:
            prev[0] = max(prev[0], value)
            prev[1] = max(prev[1], scored)
    if set(per_type_max) != set(TYPES):
        missing = sorted(set(TYPES) - set(per_type_max))
        raise ValueError(f"el input no trae {missing}")
    watermark = min(per_type_max.values()) - lag_ms
    rows = {}
    for (patient, start, typ), (value, scored) in bucket.items():
        end = start + window_ms
        if end > watermark:
            continue
        row = rows.get((patient, start))
        if row is None:
            row = {
                "patient_id": patient,
                "window_start": start,
                "window_end": end,
                "news2_score": 0,
            }
            for kind in TYPES:
                row[VALUE_COLUMN[kind]] = None
                row[SCORE_COLUMN[kind]] = 0
            rows[(patient, start)] = row
        row[VALUE_COLUMN[typ]] = value
        row[SCORE_COLUMN[typ]] = scored
    out = []
    for row in rows.values():
        row["news2_score"] = sum(row[SCORE_COLUMN[typ]] for typ in TYPES)
        out.append(row)
    out.sort(key=lambda row: (row["patient_id"], row["window_start"]))
    return out


def case_sql(typ, engine="flink"):
    """CASE de la tesis. Flink cita `value`; DataFusion lo deja pelado."""
    text = _case_sql(typ)
    if engine == "tachyon":
        return text.replace("`value`", "value")
    return text


def _case_sql(typ):
    if typ == "RESPIRATORY_RATE":
        return (
            "CASE WHEN `value` <= 8 THEN 3 WHEN `value` >= 25 THEN 3 "
            "WHEN `value` BETWEEN 21 AND 24 THEN 2 "
            "WHEN `value` BETWEEN 9 AND 11 THEN 1 "
            "WHEN `value` BETWEEN 12 AND 20 THEN 0 END"
        )
    if typ == "OXYGEN_SATURATION":
        return (
            "CASE WHEN `value` <= 91 THEN 3 "
            "WHEN `value` BETWEEN 92 AND 93 THEN 2 "
            "WHEN `value` BETWEEN 94 AND 95 THEN 1 "
            "WHEN `value` >= 96 THEN 0 END"
        )
    if typ == "BLOOD_PRESSURE_SYSTOLIC":
        return (
            "CASE WHEN `value` <= 90 THEN 3 WHEN `value` <= 100 THEN 2 "
            "WHEN `value` <= 110 THEN 1 WHEN `value` BETWEEN 111 AND 219 THEN 0 "
            "WHEN `value` >= 220 THEN 3 END"
        )
    if typ == "HEART_RATE":
        return (
            "CASE WHEN `value` <= 40 THEN 3 WHEN `value` >= 131 THEN 3 "
            "WHEN `value` BETWEEN 111 AND 130 THEN 2 "
            "WHEN `value` BETWEEN 41 AND 50 THEN 1 "
            "WHEN `value` BETWEEN 91 AND 110 THEN 1 "
            "WHEN `value` BETWEEN 51 AND 90 THEN 0 END"
        )
    if typ == "TEMPERATURE":
        return (
            "CASE WHEN `value` <= 35.0 THEN 3 WHEN `value` >= 39.1 THEN 2 "
            "WHEN `value` BETWEEN 38.1 AND 39.0 THEN 1 "
            "WHEN `value` BETWEEN 35.1 AND 36.0 THEN 1 "
            "WHEN `value` BETWEEN 36.1 AND 38.0 THEN 0 END"
        )
    if typ == "CONSCIOUSNESS":
        return "CASE WHEN `value` = 1 THEN 0 ELSE 3 END"
    raise KeyError(typ)


def wide_select_tachyon():
    """SELECT de la ventana. El orden es el de union.sql: valores, luego scores."""
    lines = [
        "SELECT patient_id, window_start, window_end,",
    ]
    for typ in TYPES:
        lines.append(
            "MAX(CASE WHEN measurement_type = '%s' THEN value END) AS %s,"
            % (typ, VALUE_COLUMN[typ])
        )
    for typ in TYPES:
        lines.append(
            "COALESCE(MAX(CASE WHEN measurement_type = '%s' THEN score END), 0) AS %s,"
            % (typ, SCORE_COLUMN[typ])
        )
    terms = " + ".join(SCORE_COLUMN[typ] for typ in TYPES)
    lines.append(f"{terms} AS news2_score")
    branches = []
    for typ in TYPES:
        branches.append(
            "SELECT measurement_type, patient_id, value, score, measurement_timestamp "
            "FROM scores_%s" % SUFFIX[typ]
        )
    body = "\nUNION ALL\n".join(branches)
    lines.append("FROM (\n%s\n)" % body)
    lines.append(
        "GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)"
    )
    return "\n".join(lines)


def _q(name):
    return "`value`" if name == "value" else name


def ddl(engine):
    """DDL Paimon. Flink usa un bucket por CPU; Tachyon, uno solo."""
    bucket = 1 if engine == "tachyon" else int(os.environ.get("PARALLELISM", "4"))
    warehouse = "/wh/%s" % engine
    lines = [
        "SET 'sql-client.execution.result-mode' = 'tableau';",
        "CREATE CATALOG lake WITH ('type' = 'paimon', 'warehouse' = 'file://%s');" % warehouse,
        "USE CATALOG lake;",
        "CREATE DATABASE IF NOT EXISTS delta;",
    ]
    # patient_id es la clave de la ventana: Tachyon la exige Utf8 no nulo.
    # Los valores de la fila ancha sí pueden faltar (vital ausente).
    meas = [
        ("event_id", "BIGINT NOT NULL"),
        ("patient_id", "STRING NOT NULL"),
        ("measurement_type", "STRING NOT NULL"),
        ("value", "DOUBLE NOT NULL"),
        ("measurement_timestamp", "BIGINT NOT NULL"),
    ]
    score = meas[:-1] + [("score", "BIGINT NOT NULL"), meas[-1]]
    wide = [
        ("patient_id", "STRING NOT NULL"),
        ("window_start", "BIGINT NOT NULL"),
        ("window_end", "BIGINT NOT NULL"),
    ]
    wide += [(VALUE_COLUMN[typ], "DOUBLE") for typ in TYPES]
    wide += [(SCORE_COLUMN[typ], "BIGINT NOT NULL") for typ in TYPES]
    wide.append(("news2_score", "BIGINT NOT NULL"))

    def create(name, cols, pk, sequence):
        body = ",\n".join("    %s %s" % (_q(col), typ) for col, typ in cols)
        keys = ", ".join(pk)
        # Flink lee estas tablas en streaming: input deja cada insert en el
        # changelog. Tachyon lee appends y no acepta ese productor.
        producer = "input" if engine == "flink" and not name.startswith("news2_wide") else "none"
        # 13 sinks comparten el heap del TaskManager. El buffer por defecto
        # (256 mb, sin spill) no cabe; 32 mb con spill sigue dentro de 3072m.
        spill = ""
        if engine == "flink":
            spill = (
                ",\n    'write-buffer-size' = '32 mb',\n"
                "    'write-buffer-spillable' = 'true'"
            )
        opts = (
            "    'bucket' = '%d',\n"
            "    'sequence.field' = '%s',\n"
            "    'file.format' = 'parquet',\n"
            "    'file.compression' = 'zstd',\n"
            "    'changelog-producer' = '%s',\n"
            "    'write-only' = 'true'%s"
        ) % (bucket, sequence, producer, spill)
        lines.append(
            "CREATE TABLE delta.%s (\n%s,\n    PRIMARY KEY (%s) NOT ENFORCED\n) WITH (\n%s\n);"
            % (name, body, keys, opts)
        )

    for typ in TYPES:
        suffix = SUFFIX[typ]
        create(
            "measurements_%s" % suffix,
            meas,
            ["event_id"],
            "event_id",
        )
        create(
            "scores_%s" % suffix,
            score,
            ["event_id"],
            "event_id",
        )
    create("news2_wide", wide, ["patient_id", "window_start"], "window_end")
    return "\n".join(lines) + "\n"


def flink_job():
    """Trece INSERT en un solo job para que compartan los slots."""
    topic = os.environ["TOPIC"]
    group = os.environ["GROUP"]
    commit = os.environ.get("COMMIT", "1s")
    lag_s = os.environ.get("LAG_S", "10.000")
    parallelism = os.environ.get("PARALLELISM", "4")
    name = os.environ.get("NAME", "news2")
    lines = [
        "SET 'pipeline.name' = '%s';" % name,
        "SET 'parallelism.default' = '%s';" % parallelism,
        "SET 'execution.checkpointing.interval' = '%s';" % commit,
        "SET 'table.dml-sync' = 'false';",
        "SET 'table.local-time-zone' = 'UTC';",
        "CREATE CATALOG lake WITH ('type' = 'paimon', 'warehouse' = 'file:///wh/flink');",
        "USE CATALOG lake;",
    ]
    for typ in TYPES:
        suffix = SUFFIX[typ]
        lines.append(
            """
CREATE TEMPORARY TABLE raw_%s (
    event_id BIGINT NOT NULL,
    patient_id STRING NOT NULL,
    measurement_type STRING NOT NULL,
    `value` DOUBLE NOT NULL,
    measurement_timestamp BIGINT NOT NULL
) WITH (
    'connector' = 'kafka',
    'topic' = '%s',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = '%s-%s',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);"""
            % (suffix, topic, group, suffix)
        )
        lines.append(
            """
CREATE TEMPORARY TABLE measurements_%s_src (
    event_id BIGINT NOT NULL,
    patient_id STRING NOT NULL,
    measurement_type STRING NOT NULL,
    `value` DOUBLE NOT NULL,
    measurement_timestamp BIGINT NOT NULL,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'path' = 'file:///wh/flink/delta.db/measurements_%s',
    'scan.mode' = 'from-timestamp',
    'scan.timestamp-millis' = '1',
    'scan.remove-normalize' = 'true',
    'continuous.discovery-interval' = '1s'
);"""
            % (suffix, suffix)
        )
        lines.append(
            """
CREATE TEMPORARY TABLE scores_%s_src (
    event_id BIGINT NOT NULL,
    patient_id STRING NOT NULL,
    measurement_type STRING NOT NULL,
    `value` DOUBLE NOT NULL,
    score BIGINT NOT NULL,
    measurement_timestamp BIGINT NOT NULL,
    ts AS TO_TIMESTAMP_LTZ(measurement_timestamp, 3),
    WATERMARK FOR ts AS ts - INTERVAL '%s' SECOND,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'path' = 'file:///wh/flink/delta.db/scores_%s',
    'scan.mode' = 'from-timestamp',
    'scan.timestamp-millis' = '1',
    'scan.remove-normalize' = 'true',
    'continuous.discovery-interval' = '1s'
);"""
            % (suffix, lag_s, suffix)
        )
    unions = "\nUNION ALL\n".join(
        "SELECT measurement_type, patient_id, `value`, score, ts FROM scores_%s_src" % SUFFIX[typ]
        for typ in TYPES
    )
    lines.append("CREATE TEMPORARY VIEW news2_unions AS\n%s;" % unions)
    value_sql = ",\n    ".join(
        "MAX(CASE WHEN measurement_type = '%s' THEN `value` END)" % typ for typ in TYPES
    )
    score_sql = ",\n    ".join(
        "CAST(COALESCE(MAX(CASE WHEN measurement_type = '%s' THEN score END), 0) AS BIGINT)" % typ
        for typ in TYPES
    )
    news = " +\n        ".join(
        "COALESCE(MAX(CASE WHEN measurement_type = '%s' THEN score END), 0)" % typ for typ in TYPES
    )
    inserts = []
    for typ in TYPES:
        suffix = SUFFIX[typ]
        inserts.append(
            """INSERT INTO delta.measurements_%s
SELECT event_id, patient_id, measurement_type, `value`, measurement_timestamp
FROM raw_%s
WHERE measurement_type = '%s'"""
            % (suffix, suffix, typ)
        )
        inserts.append(
            """INSERT INTO delta.scores_%s
SELECT event_id, patient_id, measurement_type, `value`,
    CAST(%s AS BIGINT),
    measurement_timestamp
FROM measurements_%s_src
WHERE measurement_type = '%s'"""
            % (suffix, case_sql(typ, "flink"), suffix, typ)
        )
    inserts.append(
        """INSERT INTO delta.news2_wide
SELECT
    patient_id,
    CAST(TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', window_start) AS BIGINT) * 1000,
    CAST(TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', window_end) AS BIGINT) * 1000,
    %s,
    %s,
    CAST(%s AS BIGINT)
FROM TABLE(TUMBLE(TABLE news2_unions, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
GROUP BY patient_id, window_start, window_end"""
        % (value_sql, score_sql, news)
    )
    lines.append("EXECUTE STATEMENT SET\nBEGIN\n" + ";\n".join(inserts) + ";\nEND;\n")
    return "\n".join(lines)


def _yaml_route(suffix, typ):
    topic = os.environ["TOPIC"]
    commit = os.environ.get("COMMIT", "1s")
    name = os.environ.get("NAME", "news2")
    return f"""pipeline:
  name: {name}-route-{suffix}
connectors:
  redpanda:
    brokers: [localhost:9092]
  paimon:
    warehouse: /wh/tachyon
inputs:
  - name: raw
    topic: {topic}
    key: patient_id
    schema: /bench/tachyon/schemas/news2.json
output:
  name: measurements_{suffix}
  table: delta.measurements_{suffix}
  key: patient_id
  bucket: 1
  sequence_field: event_id
deployment:
  partitions: 1
  commit_interval: {commit}
"""


def _sql_route(suffix, typ):
    return (
        "INSERT INTO measurements_%s\n"
        "SELECT event_id, patient_id, measurement_type, value, measurement_timestamp\n"
        "FROM raw\n"
        "WHERE measurement_type = '%s'\n" % (suffix, typ)
    )


def _yaml_score(suffix):
    commit = os.environ.get("COMMIT", "1s")
    name = os.environ.get("NAME", "news2")
    return f"""pipeline:
  name: {name}-score-{suffix}
connectors:
  paimon:
    warehouse: /wh/tachyon
inputs:
  - name: measurements_{suffix}
    table: delta.measurements_{suffix}
    key: patient_id
output:
  name: scores_{suffix}
  table: delta.scores_{suffix}
  key: patient_id
  bucket: 1
  sequence_field: event_id
deployment:
  partitions: 1
  commit_interval: {commit}
"""


def _sql_score(suffix, typ):
    return (
        "INSERT INTO scores_%s\n"
        "SELECT event_id, patient_id, measurement_type, value,\n"
        "CAST(%s AS BIGINT) AS score,\n"
        "measurement_timestamp\n"
        "FROM measurements_%s\n"
        "WHERE measurement_type = '%s'\n" % (suffix, case_sql(typ, "tachyon"), suffix, typ)
    )


def _yaml_union():
    commit = os.environ.get("COMMIT", "1s")
    lag = os.environ.get("LAG_MS", "10000")
    name = os.environ.get("NAME", "news2")
    inputs = []
    for typ in TYPES:
        suffix = SUFFIX[typ]
        inputs.append(
            f"""  - name: scores_{suffix}
    table: delta.scores_{suffix}
    key: patient_id
    watermark:
      column: measurement_timestamp
      lag: {lag}ms"""
        )
    body = "\n".join(inputs)
    return f"""pipeline:
  name: {name}-union
connectors:
  paimon:
    warehouse: /wh/tachyon
inputs:
{body}
output:
  name: news2_wide
  table: delta.news2_wide
  key: patient_id
  bucket: 1
  sequence_field: window_end
deployment:
  partitions: 1
  commit_interval: {commit}
  metrics:
    bind_addr: 127.0.0.1:9464
"""


def _sql_union():
    return "INSERT INTO news2_wide\n" + wide_select_tachyon() + "\n"


def tachyon_supervisor():
    names = []
    for typ in TYPES:
        names.append("route-%s" % SUFFIX[typ])
        names.append("score-%s" % SUFFIX[typ])
    names.append("union")
    runs = "\n".join('run %s' % name for name in names)
    return f"""#!/usr/bin/env bash
# Trece procesos, un cgroup. Si uno se cae, caen los demás.
set -u
bin="${{TACHYON_BIN:-/work/target/benchfast/tachyon}}"
pids=()
stop() {{
  for pid in "${{pids[@]}}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait || true
}}
trap stop TERM INT
run() {{
  local name=$1
  "$bin" --config "/bench/.run/news2/${{name}}.yaml" --sql "/bench/.run/news2/${{name}}.sql" \
    > "/bench/.run/news2/${{name}}.log" 2>&1 &
  pids+=($!)
  echo "started $name pid $!"
}}
{runs}
while true; do
  for pid in "${{pids[@]}}"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "process $pid exited" >&2
      stop
      exit 1
    fi
  done
  sleep 1
done
"""


def render(engine, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "ddl.sql"), "w") as fh:
        fh.write(ddl(engine))
    if engine == "flink":
        with open(os.path.join(out_dir, "job.sql"), "w") as fh:
            fh.write(flink_job())
        return
    job = os.path.join(out_dir, "news2")
    os.makedirs(job, exist_ok=True)
    for typ in TYPES:
        suffix = SUFFIX[typ]
        with open(os.path.join(job, "route-%s.yaml" % suffix), "w") as fh:
            fh.write(_yaml_route(suffix, typ))
        with open(os.path.join(job, "route-%s.sql" % suffix), "w") as fh:
            fh.write(_sql_route(suffix, typ))
        with open(os.path.join(job, "score-%s.yaml" % suffix), "w") as fh:
            fh.write(_yaml_score(suffix))
        with open(os.path.join(job, "score-%s.sql" % suffix), "w") as fh:
            fh.write(_sql_score(suffix, typ))
    with open(os.path.join(job, "union.yaml"), "w") as fh:
        fh.write(_yaml_union())
    with open(os.path.join(job, "union.sql"), "w") as fh:
        fh.write(_sql_union())
    path = os.path.join(out_dir, "news2-tachyon.sh")
    with open(path, "w") as fh:
        fh.write(tachyon_supervisor())
    os.chmod(path, 0o755)


def _self_check():
    events = list(iter_events(2, 2))
    types = {event["measurement_type"] for event in events}
    assert types == set(TYPES), types
    patients = {event["patient_id"] for event in events}
    assert patients == {"p0000", "p0001"}, patients
    early = [
        event
        for event in events
        if event["patient_id"] == "p0000"
        and event["measurement_type"] == "OXYGEN_SATURATION"
        and event["measurement_timestamp"] == BASE_MS + 1_000
    ]
    assert early == [], early
    starts = {event["measurement_timestamp"] - event["measurement_timestamp"] % WINDOW_MS for event in events}
    assert BASE_MS in starts and BASE_MS + WINDOW_MS in starts
    wide = fold(events)
    assert len(wide) == 4, len(wide)
    by = {(row["patient_id"], row["window_start"]): row for row in wide}
    assert ( "p0000", BASE_MS + 2 * WINDOW_MS) not in by

    def expect(patient, start, **kw):
        row = by[(patient, start)]
        assert row["window_end"] == start + WINDOW_MS
        for key, value in kw.items():
            assert row[key] == value, (patient, start, key, row[key], value)
        total = sum(row[SCORE_COLUMN[typ]] for typ in TYPES)
        assert row["news2_score"] == total

    expect(
        "p0000",
        BASE_MS,
        respiratory_rate_value=8.0,
        respiratory_rate_score=3,
        oxygen_saturation_value=None,
        oxygen_saturation_score=0,
        blood_pressure_value=90.0,
        blood_pressure_score=3,
        heart_rate_value=40.0,
        heart_rate_score=3,
        temperature_value=35.0,
        temperature_score=3,
        consciousness_value=1.0,
        consciousness_score=0,
        news2_score=12,
    )
    expect(
        "p0000",
        BASE_MS + WINDOW_MS,
        respiratory_rate_value=10.0,
        respiratory_rate_score=1,
        oxygen_saturation_value=93.0,
        oxygen_saturation_score=2,
        blood_pressure_value=100.0,
        blood_pressure_score=2,
        heart_rate_value=45.0,
        heart_rate_score=1,
        temperature_value=35.5,
        temperature_score=1,
        consciousness_value=0.0,
        consciousness_score=3,
        news2_score=10,
    )
    expect(
        "p0001",
        BASE_MS,
        respiratory_rate_value=22.0,
        respiratory_rate_score=2,
        oxygen_saturation_value=97.0,
        oxygen_saturation_score=0,
        blood_pressure_value=120.0,
        blood_pressure_score=0,
        heart_rate_value=100.0,
        heart_rate_score=1,
        temperature_value=38.5,
        temperature_score=1,
        consciousness_value=2.0,
        consciousness_score=3,
        news2_score=7,
    )
    expect(
        "p0001",
        BASE_MS + WINDOW_MS,
        respiratory_rate_value=26.0,
        respiratory_rate_score=3,
        oxygen_saturation_value=90.0,
        oxygen_saturation_score=3,
        blood_pressure_value=220.0,
        blood_pressure_score=3,
        heart_rate_value=120.0,
        heart_rate_score=2,
        temperature_value=39.25,
        temperature_score=2,
        consciousness_value=1.0,
        consciousness_score=0,
        news2_score=13,
    )
    # Dos eventos del mismo tipo en el minuto: value y score se maximizan aparte.
    extra = list(events)
    extra.append(
        {
            "event_id": 10_000,
            "patient_id": "p0001",
            "measurement_type": "HEART_RATE",
            "value": 40.0,
            "measurement_timestamp": BASE_MS + WINDOW_MS + 2_000,
        }
    )
    folded = fold(extra)
    got = {(row["patient_id"], row["window_start"]): row for row in folded}[
        ("p0001", BASE_MS + WINDOW_MS)
    ]
    assert got["heart_rate_value"] == 120.0
    assert got["heart_rate_score"] == 3
    print(json.dumps({"ok": True, "fixture_rows": len(wide), "events": len(events)}))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--render":
        render(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else ".run")
    else:
        _self_check()
