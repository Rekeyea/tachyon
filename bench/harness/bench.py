"""Harness del benchmark Tachyon vs Flink (misma máquina, mismo input).

Mide los dos motores desde afuera y de la misma forma:

- throughput: pendiente de los offsets *commiteados* del consumer group entre
  el 20% y el 80% del backlog. Los dos motores commitean en Kafka recién
  después de que el snapshot de Paimon existe, así que es progreso
  exactly-once visible, no filas leídas.
- latencia: `timeMillis` del snapshot de Paimon que hace visible la fila menos
  su `event_time` (ETL) o su `window_end` (ventana). Se leen los manifests
  (Avro) y la columna de cada data file nuevo (parquet).
- CPU y memoria: el cgroup del contenedor del motor (JobManager + TaskManager
  en Flink, el proceso en Tachyon).

Subcomandos:
  preload  pre-carga un topic con N eventos (una vez; cada corrida lo relee)
  drain    mide el drenado de un topic pre-cargado
  live     produce a tasa fija y mide latencia de punta a punta
"""

import argparse
import glob
import json
import multiprocessing as mp
import os
import sys
import threading
import time

import numpy as np

PARTITIONS = 8
BROKER = "localhost:9092"


def now_ms():
    return time.time_ns() // 1_000_000


# --------------------------------------------------------------------------
# Datos
# --------------------------------------------------------------------------
# Cada proceso productor es dueño de un conjunto de particiones y escribe en
# ellas en orden, así el event time es monótono dentro de cada partición (no
# hay late data artificial por intercalar productores). La clave cumple
# `order_id % 8 == partición`: el topic está particionado por la clave.
#
# etl: order_id único, 10% cancelados (el SQL los filtra).
# win: 10K claves; en preload el event time es sintético (100K eventos por
#      segundo de event time -> 10 eventos por clave y ventana de 1s); en live
#      es el reloj de pared.

WIN_KEYS = 10_000
WIN_EVENTS_PER_SEC = 100_000


def payload(kind, j, partition, event_time):
    if kind == "etl":
        order_id = j * PARTITIONS + partition
        status = "cancelled" if j % 10 == 9 else "paid"
        return order_id, (
            '{"order_id":%d,"status":"%s","source_version":%d,"amount":%d.5,"event_time":%d}'
            % (order_id, status, j, j % 1000, event_time)
        )
    order_id = (j % (WIN_KEYS // PARTITIONS)) * PARTITIONS + partition
    return order_id, '{"order_id":%d,"event_time":%d,"amount":%d}' % (
        order_id,
        event_time,
        j % 100,
    )


def _producer():
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": BROKER,
            "linger.ms": 5,
            "batch.size": 1_048_576,
            "queue.buffering.max.messages": 2_000_000,
            "queue.buffering.max.kbytes": 1_048_576,
            "compression.type": "none",
            "acks": 1,
        }
    )


def _produce_one(p, topic, value, key, partition):
    while True:
        try:
            p.produce(topic, value=value, key=key, partition=partition)
            return
        except BufferError:
            p.poll(0.005)


def _preload_worker(topic, kind, parts, per_part, base_ms, out):
    p = _producer()
    sent = 0
    for j in range(per_part):
        for part in parts:
            if kind == "win":
                # Event time sintético: el mismo en todas las particiones
                # para el mismo j (avance parejo del watermark).
                et = base_ms + (j * PARTITIONS * 1000) // WIN_EVENTS_PER_SEC
            else:
                et = base_ms + j // 10_000
            key, value = payload(kind, j, part, et)
            _produce_one(p, topic, value, str(key), part)
            sent += 1
        if j % 10_000 == 0:
            p.poll(0)
    p.flush(300)
    out.put(sent)


def create_topic(topic, partitions=PARTITIONS):
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": BROKER})
    fs = admin.delete_topics([topic], operation_timeout=10)
    for f in fs.values():
        try:
            f.result()
        except Exception:
            pass
    time.sleep(2)
    for attempt in range(10):
        fs = admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
        )
        try:
            for f in fs.values():
                f.result()
            return
        except Exception as e:  # el delete todavía no terminó
            if attempt == 9:
                raise
            time.sleep(1)


def cmd_preload(a):
    create_topic(a.topic)
    procs = a.procs
    per_part = a.events // PARTITIONS
    base_ms = now_ms() - 3_600_000
    q = mp.Queue()
    workers = []
    t0 = time.time()
    for w in range(procs):
        parts = [x for x in range(PARTITIONS) if x % procs == w]
        pr = mp.Process(target=_preload_worker, args=(a.topic, a.kind, parts, per_part, base_ms, q))
        pr.start()
        workers.append(pr)
    total = sum(q.get() for _ in workers)
    for pr in workers:
        pr.join()
    dt = time.time() - t0
    print(json.dumps({"topic": a.topic, "kind": a.kind, "events": total, "secs": round(dt, 1),
                      "rate": round(total / dt)}))


# --------------------------------------------------------------------------
# cgroup del motor
# --------------------------------------------------------------------------


class Cgroup:
    def __init__(self, container_id):
        self.path = f"/cg/docker/{container_id}"
        if not os.path.isdir(self.path):
            raise SystemExit(f"no encuentro el cgroup {self.path}")
        self.peak_anon = 0
        self._stop = False
        self._t = threading.Thread(target=self._sample, daemon=True)
        self._t.start()

    def cpu_s(self):
        with open(f"{self.path}/cpu.stat") as f:
            for line in f:
                k, v = line.split()
                if k == "usage_usec":
                    return int(v) / 1e6
        return 0.0

    def anon_mb(self):
        with open(f"{self.path}/memory.stat") as f:
            for line in f:
                k, v = line.split()
                if k == "anon":
                    return int(v) / 2**20
        return 0.0

    def _sample(self):
        while not self._stop:
            try:
                self.peak_anon = max(self.peak_anon, self.anon_mb())
            except OSError:
                return
            time.sleep(0.1)

    def stop(self):
        self._stop = True


# --------------------------------------------------------------------------
# Offsets commiteados
# --------------------------------------------------------------------------


class Committed:
    def __init__(self, group, topic):
        from confluent_kafka.admin import AdminClient

        self.admin = AdminClient({"bootstrap.servers": BROKER})
        self.group = group
        self.topic = topic

    def total(self):
        from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition

        req = [ConsumerGroupTopicPartitions(
            self.group, [TopicPartition(self.topic, p) for p in range(PARTITIONS)])]
        try:
            res = self.admin.list_consumer_group_offsets(req, request_timeout=5)
            got = list(res.values())[0].result()
        except Exception:
            return 0
        return sum(max(tp.offset, 0) for tp in got.topic_partitions)


def end_offsets(topic):
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": BROKER, "group.id": "bench-probe"})
    total = 0
    for p in range(PARTITIONS):
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        total += hi
    c.close()
    return total


# --------------------------------------------------------------------------
# Tailer de snapshots de Paimon -> latencia por fila
# --------------------------------------------------------------------------


class SnapshotTail:
    """Sigue `snapshot-N` de una tabla y, por cada data file agregado, lee la
    columna de tiempo y registra `timeMillis - valor` por fila."""

    def __init__(self, table_path, column, since_ms=0):
        self.table = table_path
        self.column = column
        self.since_ms = since_ms
        self.next_id = 1
        self.lat = []  # arrays de latencia (ms)
        self.rows = 0
        self.snapshots = []  # (id, timeMillis, rows)
        self._stop = False
        self._t = threading.Thread(target=self._loop, daemon=True)
        self._t.start()

    def _read_avro(self, path):
        import fastavro

        with open(path, "rb") as f:
            return list(fastavro.reader(f))

    def _values(self, path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        col = pq.read_table(path, columns=[self.column]).column(0)
        if pa.types.is_timestamp(col.type):
            col = col.cast(pa.timestamp("ms")).cast(pa.int64())
        return col.to_numpy(zero_copy_only=False).astype(np.int64)

    def _process(self, snap):
        if snap.get("commitKind") not in ("APPEND", None):
            return
        t = snap["timeMillis"]
        rows = 0
        mlist = os.path.join(self.table, "manifest", snap["deltaManifestList"])
        for m in self._read_avro(mlist):
            for e in self._read_avro(os.path.join(self.table, "manifest", m["_FILE_NAME"])):
                if e["_KIND"] != 0:
                    continue
                f = e["_FILE"]
                path = os.path.join(self.table, f"bucket-{e['_BUCKET']}", f["_FILE_NAME"])
                vals = self._values(path)
                if self.column == "window_end":
                    vals = vals[vals >= self.since_ms]
                elif self.since_ms:
                    vals = vals[vals >= self.since_ms]
                self.lat.append(t - vals)
                rows += len(vals)
        self.rows += rows
        self.snapshots.append((snap["id"], t, rows))

    def _loop(self):
        sdir = os.path.join(self.table, "snapshot")
        while not self._stop:
            path = os.path.join(sdir, f"snapshot-{self.next_id}")
            if os.path.exists(path):
                try:
                    with open(path) as f:
                        snap = json.load(f)
                except (json.JSONDecodeError, OSError):
                    time.sleep(0.02)  # escritura en curso
                    continue
                self._process(snap)
                self.next_id += 1
                continue
            time.sleep(0.05)

    def stop(self):
        self._stop = True
        self._t.join(timeout=30)

    def summary(self):
        if not self.lat:
            return {"rows": 0}
        a = np.concatenate(self.lat)
        gaps = np.diff([s[1] for s in self.snapshots]) if len(self.snapshots) > 1 else np.array([0])
        return {
            "rows": int(a.size),
            "p50_ms": int(np.percentile(a, 50)),
            "p90_ms": int(np.percentile(a, 90)),
            "p99_ms": int(np.percentile(a, 99)),
            "p999_ms": int(np.percentile(a, 99.9)),
            "max_ms": int(a.max()),
            "mean_ms": round(float(a.mean()), 1),
            "std_ms": round(float(a.std()), 1),
            "snapshots": len(self.snapshots),
            "commit_gap_p50_ms": int(np.percentile(gaps, 50)),
            "commit_gap_max_ms": int(gaps.max()),
        }


def wait_table(path, timeout=120):
    deadline = time.time() + timeout
    while not os.path.isdir(os.path.join(path, "schema")):
        if time.time() > deadline:
            raise SystemExit(f"la tabla {path} no apareció")
        time.sleep(0.2)


# --------------------------------------------------------------------------
# drain: throughput de un backlog
# --------------------------------------------------------------------------


def scrape(url):
    """Contadores de Tachyon (`/metrics`, texto `nombre valor`)."""
    import urllib.request

    try:
        body = urllib.request.urlopen(url, timeout=2).read().decode()
    except Exception:
        return None
    out = {}
    for line in body.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0].startswith("tachyon_"):
            out[parts[0][8:]] = float(parts[1])
    return out


def cmd_drain(a):
    cg = Cgroup(a.container)
    total = end_offsets(a.topic)
    com = Committed(a.group, a.topic)
    t_start = time.time()
    cpu_start = cg.cpu_s()
    samples = []
    mark = {}
    deadline = t_start + a.timeout
    first_commit = None
    while time.time() < deadline:
        c = com.total()
        t = time.time()
        cpu = cg.cpu_s()
        samples.append((t, c, cpu))
        if c > 0 and first_commit is None:
            first_commit = t - t_start
        for q in (0.2, 0.8):
            if q not in mark and c >= total * q:
                mark[q] = (t, c, cpu)
                if a.metrics:
                    mark[("m", q)] = scrape(a.metrics)
        if c >= total:
            break
        time.sleep(0.05)
    t_end = time.time()
    cg.stop()
    done = samples[-1][1]
    out = {"engine": a.engine, "kind": a.kind, "backlog": total, "committed": done,
           "complete": done >= total}
    # Tasa por los saltos de la escalera: el offset commiteado avanza de a un
    # checkpoint entero (a varios M filas/s, un escalón de 1s son millones de
    # filas). Cortar en "la primera muestra >= 20%" y ">= 80%" daba errores de
    # hasta un escalón entero sobre ventanas de pocos segundos. Se toma el
    # instante de cada salto entre el 20% y el 95% del backlog (fuera el
    # arranque) y se ajusta una
    # recta; la CPU se mide sobre el mismo tramo.
    jumps = [samples[i] for i in range(1, len(samples)) if samples[i][1] != samples[i - 1][1]]
    inner = [j for j in jumps if total * 0.20 <= j[1] <= total * 0.95]
    if len(inner) >= 3:
        ts = np.array([j[0] for j in inner]); cs = np.array([j[1] for j in inner], dtype=float)
        slope = float(np.polyfit(ts - ts[0], cs, 1)[0])
        span = ts[-1] - ts[0]
        cpu = inner[-1][2] - inner[0][2]
        rows = cs[-1] - cs[0]
        out.update({
            "rows_s_fit": round(slope),
            "fit_points": len(inner),
            "fit_span_s": round(span, 2),
            "cpu_cores_fit": round(cpu / span, 2) if span else None,
            "rows_per_cpu_s_fit": round(rows / cpu) if cpu else None,
        })
    if 0.2 in mark and 0.8 in mark:
        (t0, c0, u0), (t1, c1, u1) = mark[0.2], mark[0.8]
        dt = t1 - t0
        out.update({
            "rows_s": round((c1 - c0) / dt),
            "cpu_cores": round((u1 - u0) / dt, 2),
            "rows_per_cpu_s": round((c1 - c0) / max(u1 - u0, 1e-9)),
        })
        # Estabilidad: tasa por tramo de 1s dentro del 20-80%.
        win = [s for s in samples if t0 <= s[0] <= t1]
        rates = []
        i = 0
        for j in range(len(win)):
            while win[j][0] - win[i][0] > 2.0:
                i += 1
            if win[j][0] - win[i][0] >= 1.5:
                rates.append((win[j][1] - win[i][1]) / (win[j][0] - win[i][0]))
        if rates:
            r = np.array(rates)
            out["rate_cv"] = round(float(r.std() / max(r.mean(), 1)), 3)
        m0, m1 = mark.get(("m", 0.2)), mark.get(("m", 0.8))
        if m0 and m1:
            # Segundos de cada etapa en la ventana 20-80% (source_next y
            # send_wait son del loop principal; write y commit, de sus tasks).
            out["stages_s"] = {k: round((m1[k] - m0[k]) / 1e9, 2)
                               for k in ("source_next_ns", "send_wait_ns", "write_ns", "commit_ns")
                               if k in m0 and k in m1}
    out.update({
        "first_commit_s": round(first_commit, 2) if first_commit else None,
        "drain_s": round(t_end - t_start, 2),
        "cpu_total_s": round(samples[-1][2] - cpu_start, 1),
        "peak_anon_mb": round(cg.peak_anon),
    })
    print(json.dumps(out))


# --------------------------------------------------------------------------
# live: tasa fija, latencia de punta a punta
# --------------------------------------------------------------------------


def _live_worker(topic, kind, parts, rate_per_part, duration, start_at, out):
    p = _producer()
    while time.time() < start_at:
        time.sleep(0.001)
    sent = 0
    j = 0
    t0 = time.time()
    end = t0 + duration
    step = max(1, int(rate_per_part * len(parts) / 1000))  # ~1ms por tramo
    while True:
        now = time.time()
        if now >= end:
            break
        target = int((now - t0) * rate_per_part)
        if j >= target:
            time.sleep(0.0005)
            continue
        et = now_ms()
        for _ in range(min(step, target - j + 1)):
            for part in parts:
                key, value = payload(kind, j, part, et)
                _produce_one(p, topic, value, str(key), part)
                sent += 1
            j += 1
        p.poll(0)
    p.flush(60)
    out.put(sent)


def cmd_live(a):
    cg = Cgroup(a.container)
    column = "event_time" if a.kind == "etl" else "window_end"
    wait_table(a.table)
    start_at = time.time() + 1.0
    tail = SnapshotTail(a.table, column, since_ms=int(start_at * 1000))
    q = mp.Queue()
    procs = []
    per_part = a.rate / PARTITIONS
    for w in range(a.procs):
        parts = [x for x in range(PARTITIONS) if x % a.procs == w]
        pr = mp.Process(target=_live_worker,
                        args=(a.topic, a.kind, parts, per_part, a.duration, start_at, q))
        pr.start()
        procs.append(pr)
    time.sleep(max(0, start_at - time.time()))
    cpu0 = cg.cpu_s()
    t0 = time.time()
    produced = sum(q.get() for _ in procs)
    for pr in procs:
        pr.join()
    t_prod = time.time() - t0
    cpu1 = cg.cpu_s()
    # Deja que terminen de aparecer las filas (o las ventanas) del tramo.
    time.sleep(a.settle)
    tail.stop()
    cg.stop()
    s = tail.summary()
    expected = produced * 9 // 10 if a.kind == "etl" else None
    out = {"engine": a.engine, "kind": a.kind, "rate_target": a.rate,
           "rate_real": round(produced / t_prod), "produced": produced,
           "cpu_cores": round((cpu1 - cpu0) / t_prod, 2),
           "peak_anon_mb": round(cg.peak_anon)}
    out.update(s)
    if expected is not None:
        out["landed_fraction"] = round(s.get("rows", 0) / max(expected, 1), 4)
    print(json.dumps(out))


def cmd_verify_etl(a):
    """ETL: cada evento no cancelado aparece una vez (claves únicas)."""
    import verify

    got = verify.rows(a.table)
    expected = end_offsets(a.topic) * 9 // 10
    print(json.dumps({"rows": got["rows"], "expected": expected, "ok": got["rows"] == expected}))


def cmd_truth_win(a):
    """Ventana: COUNT/SUM por clave y ventana contra la fórmula del generador."""
    import subprocess

    out = subprocess.run(
        [sys.executable, "/bench/harness/truth_win.py", a.topic, a.table, str(end_offsets(a.topic))],
        capture_output=True, text=True, check=False)
    line = (out.stdout.strip().splitlines() or ["{}"])[-1]
    try:
        r = json.loads(line)
    except json.JSONDecodeError:
        r = {"error": (out.stderr or out.stdout)[-500:]}
    r["ok"] = r.get("wrong_values") == 0 and r.get("missing_rows") == 0 and r.get("dup_versions") == 0
    r.pop("wrong_sample", None)
    print(json.dumps(r))


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("preload")
    p.add_argument("--topic", required=True)
    p.add_argument("--kind", choices=["etl", "win"], required=True)
    p.add_argument("--events", type=int, required=True)
    p.add_argument("--procs", type=int, default=4)
    p.set_defaults(fn=cmd_preload)
    p = sub.add_parser("verify_etl")
    p.add_argument("table")
    p.add_argument("topic")
    p.set_defaults(fn=cmd_verify_etl)
    p = sub.add_parser("truth_win")
    p.add_argument("topic")
    p.add_argument("table")
    p.set_defaults(fn=cmd_truth_win)
    p = sub.add_parser("mktopic")
    p.add_argument("--topic", required=True)
    p.set_defaults(fn=lambda a: create_topic(a.topic))
    p = sub.add_parser("drain")
    p.add_argument("--engine", required=True)
    p.add_argument("--kind", choices=["etl", "win"], required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--group", required=True)
    p.add_argument("--container", required=True)
    p.add_argument("--timeout", type=float, default=600)
    p.add_argument("--metrics", default=None)
    p.set_defaults(fn=cmd_drain)
    p = sub.add_parser("live")
    p.add_argument("--engine", required=True)
    p.add_argument("--kind", choices=["etl", "win"], required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--container", required=True)
    p.add_argument("--rate", type=int, required=True)
    p.add_argument("--duration", type=float, default=30)
    p.add_argument("--settle", type=float, default=8)
    p.add_argument("--procs", type=int, default=4)
    p.set_defaults(fn=cmd_live)
    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
