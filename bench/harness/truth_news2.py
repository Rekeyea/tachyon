"""Verdad del pipeline NEWS2: filas Paimon contra fold() del topic.

No usa la salida de un motor como oráculo. Lee el topic, calcula las filas
anchas con la fórmula y compara las trece tablas.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import compare  # noqa: E402
import news2  # noqa: E402


def read_topic(topic):
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "news2-truth",
            "enable.auto.commit": False,
        }
    )
    low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
    consumer.assign([TopicPartition(topic, 0, low)])
    events = []
    idle = 0
    while len(events) < high - low:
        msg = consumer.poll(1.0)
        if msg is None:
            idle += 1
            if idle > 30:
                break
            continue
        idle = 0
        if msg.error():
            raise RuntimeError(str(msg.error()))
        events.append(json.loads(msg.value()))
    consumer.close()
    if len(events) != high - low:
        raise RuntimeError("el topic trae %d y se leyeron %d" % (high - low, len(events)))
    return events


def _close(got, exp):
    if got is None or exp is None:
        return got is None and exp is None
    return abs(float(got) - float(exp)) < 1e-6


def _rows(table, pk):
    data = table.to_pydict()
    out = []
    n = table.num_rows
    for i in range(n):
        out.append({col: data[col][i] for col in data})
    out.sort(key=lambda row: tuple(row[col] for col in pk))
    return out


def _load(path, pk, cols):
    if not os.path.isdir(os.path.join(path, "snapshot")):
        raise RuntimeError("falta la tabla %s" % path)
    return compare.load(path, pk, cols)


def check(topic, warehouse):
    events = read_topic(topic)
    expected = news2.fold(events)
    by_type = {}
    for event in events:
        by_type.setdefault(event["measurement_type"], []).append(event)
    problems = []
    layers = {}
    for typ in news2.TYPES:
        suffix = news2.SUFFIX[typ]
        got_events = by_type.get(typ, [])
        for kind, extra in (
            ("measurements", []),
            ("scores", ["score"]),
        ):
            name = "%s_%s" % (kind, suffix)
            path = os.path.join(warehouse, "delta.db", name)
            cols = [
                "patient_id",
                "measurement_type",
                "value",
                "measurement_timestamp",
            ] + extra
            try:
                table, dup = _load(path, ["event_id"], cols)
            except Exception as err:
                problems.append("%s: %s" % (name, err))
                continue
            rows = _rows(table, ["event_id"])
            layers[name] = {"rows": len(rows), "dup_versions": dup}
            if dup:
                problems.append("%s tiene %d versiones de la misma clave" % (name, dup))
            types = {row["measurement_type"] for row in rows}
            if types != {typ}:
                problems.append("%s contiene %s" % (name, sorted(types)))
            if len(rows) != len(got_events):
                problems.append(
                    "%s tiene %d filas y el topic %d" % (name, len(rows), len(got_events))
                )
            want = {event["event_id"]: event for event in got_events}
            for row in rows:
                src = want.get(row["event_id"])
                if src is None:
                    problems.append("%s event_id %s no está en el topic" % (name, row["event_id"]))
                    continue
                if row["patient_id"] != src["patient_id"]:
                    problems.append("%s patient %s" % (name, row["event_id"]))
                if not _close(row["value"], src["value"]):
                    problems.append("%s value %s" % (name, row["event_id"]))
                if int(row["measurement_timestamp"]) != int(src["measurement_timestamp"]):
                    problems.append("%s ts %s" % (name, row["event_id"]))
                if kind == "scores":
                    scored = news2.score(typ, float(src["value"]))
                    if int(row["score"]) != scored:
                        problems.append(
                            "%s score %s es %s y la fórmula %s"
                            % (name, row["event_id"], row["score"], scored)
                        )
    wide_path = os.path.join(warehouse, "delta.db", "news2_wide")
    wide_cols = [col for col in news2.WIDE_COLUMNS if col not in ("patient_id", "window_start")]
    try:
        table, dup = _load(wide_path, ["patient_id", "window_start"], wide_cols)
        rows = _rows(table, ["patient_id", "window_start"])
    except Exception as err:
        problems.append("news2_wide: %s" % err)
        rows = []
        dup = None
    layers["news2_wide"] = {"rows": len(rows), "dup_versions": dup}
    if dup:
        problems.append("news2_wide tiene %d versiones de la misma ventana" % dup)
    if len(rows) != len(expected):
        problems.append("news2_wide tiene %d filas y la fórmula %d" % (len(rows), len(expected)))
    want = {(row["patient_id"], row["window_start"]): row for row in expected}
    seen = set()
    for row in rows:
        key = (row["patient_id"], int(row["window_start"]))
        if key in seen:
            problems.append("ventana duplicada %s" % (key,))
        seen.add(key)
        exp = want.get(key)
        if exp is None:
            problems.append("ventana extra %s" % (key,))
            continue
        if int(row["window_end"]) != int(exp["window_end"]):
            problems.append("window_end %s" % (key,))
        for typ in news2.TYPES:
            if not _close(row[news2.VALUE_COLUMN[typ]], exp[news2.VALUE_COLUMN[typ]]):
                problems.append(
                    "%s %s es %s y la fórmula %s"
                    % (news2.VALUE_COLUMN[typ], key, row[news2.VALUE_COLUMN[typ]], exp[news2.VALUE_COLUMN[typ]])
                )
            if int(row[news2.SCORE_COLUMN[typ]]) != int(exp[news2.SCORE_COLUMN[typ]]):
                problems.append("%s %s" % (news2.SCORE_COLUMN[typ], key))
        if int(row["news2_score"]) != int(exp["news2_score"]):
            problems.append("news2_score %s es %s" % (key, row["news2_score"]))
        total = sum(int(row[news2.SCORE_COLUMN[typ]]) for typ in news2.TYPES)
        if int(row["news2_score"]) != total:
            problems.append("news2_score %s no es la suma" % (key,))
    missing = [key for key in want if key not in seen]
    if missing:
        problems.append("faltan %d ventanas" % len(missing))
    return {
        "ok": not problems,
        "topic_events": len(events),
        "wide_rows": len(expected),
        "layers": layers,
        "problems": problems[:20],
    }


def main():
    topic, warehouse = sys.argv[1], sys.argv[2]
    try:
        print(json.dumps(check(topic, warehouse)))
    except Exception as err:
        print(json.dumps({"ok": False, "problems": [str(err)]}))
        sys.exit(1)


if __name__ == "__main__":
    main()
