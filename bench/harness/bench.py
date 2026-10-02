"""Harness del benchmark Tachyon vs Flink (misma máquina, mismo input).

Mide los dos motores desde afuera y de la misma forma:

- throughput: pendiente del progreso *commiteado* entre el 20% y el 95% del
  backlog. En Redpanda son los offsets del consumer group; en Kinesis/SQS es
  `totalRecordCount` del snapshot de Paimon (el commit atómico posición+snapshot
  es la misma garantía exactly-once visible).
- latencia: `timeMillis` del snapshot de Paimon que hace visible la fila menos
  su `event_time` (ETL) o su `window_end` (ventana). Se leen los manifests
  (Avro) y la columna de cada data file nuevo (parquet).
- CPU y memoria: el cgroup del contenedor del motor (JobManager + TaskManager
  en Flink, el proceso en Tachyon).

Subcomandos:
  preload  pre-carga un topic/stream/cola con N eventos
  drain    mide el drenado de un backlog pre-cargado
  live     produce a tasa fija y mide latencia de punta a punta

`--source` elige la fuente: redpanda (default), kinesis o sqs (estos dos usan
floCi en localhost:4566 y el SDK boto3).
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

# AWS (floCi): el emulador corre en el netns compartido del bench
# (localhost:4566). Credenciales falsas: floCi no las valida.
AWS_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
RUN_DIR = "/bench/.run"


def aws_client(service):
    import boto3

    return boto3.Session(
        region_name=AWS_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ).client(service, endpoint_url=AWS_ENDPOINT)


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
    if a.source == "redpanda":
        create_topic(a.topic)
        worker, target = _preload_worker, a.topic
    elif a.source == "kinesis":
        create_stream(a.stream, a.shards)
        worker, target = _kinesis_worker, a.stream
    else:
        target = create_queue(a.queue)
        worker = _sqs_worker
    procs = a.procs
    per_part = a.events // PARTITIONS
    base_ms = now_ms() - 3_600_000
    q = mp.Queue()
    workers = []
    t0 = time.time()
    for w in range(procs):
        parts = [x for x in range(PARTITIONS) if x % procs == w]
        pr = mp.Process(target=worker, args=(target, a.kind, parts, per_part, base_ms, q))
        pr.start()
        workers.append(pr)
    total = sum(q.get() for _ in workers)
    for pr in workers:
        pr.join()
    dt = time.time() - t0
    out = {"source": a.source, "kind": a.kind, "events": total, "secs": round(dt, 1),
           "rate": round(total / dt)}
    if a.source == "redpanda":
        out["topic"] = a.topic
    elif a.source == "kinesis":
        out["stream"] = a.stream
        out["shards"] = a.shards
    else:
        out["queue"] = a.queue
    _save_preload_meta(a.source, base_ms, a.events, total)
    print(json.dumps(out))


# --------------------------------------------------------------------------
# AWS (floCi): Kinesis y SQS
# --------------------------------------------------------------------------


def create_stream(stream, shards):
    """Stream fresco: borra el anterior (si existe) y espera a que quede ACTIVE."""
    k = aws_client("kinesis")
    try:
        k.delete_stream(StreamName=stream)
    except k.exceptions.ResourceNotFoundException:
        pass
    while True:
        try:
            k.describe_stream(StreamName=stream)
            time.sleep(0.5)
        except k.exceptions.ResourceNotFoundException:
            break
    k.create_stream(StreamName=stream, ShardCount=shards)
    while k.describe_stream(StreamName=stream)["StreamDescription"]["StreamStatus"] != "ACTIVE":
        time.sleep(0.5)


def create_queue(queue):
    """Cola fresca: la crea (o reutiliza) y devuelve su URL."""
    q = aws_client("sqs")
    return q.create_queue(QueueName=queue)["QueueUrl"]


def ensure_stream(stream, shards):
    """Stream para live: lo crea si no existe y espera ACTIVE. No borra uno
    existente: borrarlo a mitad de corrida rompe los iteradores del motor."""
    k = aws_client("kinesis")
    try:
        k.describe_stream(StreamName=stream)
    except k.exceptions.ResourceNotFoundException:
        k.create_stream(StreamName=stream, ShardCount=shards)
    while k.describe_stream(StreamName=stream)["StreamDescription"]["StreamStatus"] != "ACTIVE":
        time.sleep(0.5)


def _put_records(k, stream, batch):
    """PutRecords en lotes de 500; reintenta solo los registros que fallan."""
    pending = list(batch)
    while pending:
        chunk = pending[:500]
        resp = k.put_records(
            Records=[{"Data": d, "PartitionKey": pk} for pk, d in chunk],
            StreamName=stream,
        )
        if resp["FailedRecordCount"] == 0:
            pending = pending[500:]
        else:
            pending = [(pk, d) for (pk, d), r in zip(chunk, resp["Records"]) if "ErrorCode" in r] \
                + pending[500:]


def _send_batch(q, queue_url, bodies):
    """SendMessageBatch en lotes de 10; reintenta solo los que fallan."""
    pending = list(bodies)
    while pending:
        chunk = pending[:10]
        resp = q.send_message_batch(
            QueueUrl=queue_url,
            Entries=[{"Id": str(i), "MessageBody": b} for i, b in enumerate(chunk)],
        )
        failed = {f["Id"] for f in resp.get("Failed", [])}
        if not failed:
            pending = pending[10:]
        else:
            pending = [b for i, b in enumerate(chunk) if str(i) in failed] + pending[10:]


def _gen(kind, parts, per_part, base_ms):
    """Misma fórmula y mismo reparto de particiones que _preload_worker: el
    conjunto de order_id es idéntico al de Redpanda para los mismos eventos."""
    for j in range(per_part):
        for part in parts:
            if kind == "win":
                et = base_ms + (j * PARTITIONS * 1000) // WIN_EVENTS_PER_SEC
            else:
                et = base_ms + j // 10_000
            yield payload(kind, j, part, et)


def _kinesis_worker(stream, kind, parts, per_part, base_ms, out):
    k = aws_client("kinesis")
    sent = 0
    batch = []
    for key, value in _gen(kind, parts, per_part, base_ms):
        batch.append((str(key), value.encode()))
        sent += 1
        if len(batch) >= 500:
            _put_records(k, stream, batch)
            batch = []
    if batch:
        _put_records(k, stream, batch)
    out.put(sent)


def _sqs_worker(queue_url, kind, parts, per_part, base_ms, out):
    q = aws_client("sqs")
    sent = 0
    batch = []
    for key, value in _gen(kind, parts, per_part, base_ms):
        batch.append(value)
        sent += 1
        if len(batch) >= 10:
            _send_batch(q, queue_url, batch)
            batch = []
    if batch:
        _send_batch(q, queue_url, batch)
    out.put(sent)


def _kinesis_live_worker(stream, kind, parts, rate_per_part, duration, start_at, out):
    k = aws_client("kinesis")
    while time.time() < start_at:
        time.sleep(0.001)
    sent = 0
    j = 0
    t0 = time.time()
    end = t0 + duration
    step = max(1, int(rate_per_part * len(parts) / 1000))  # ~1ms por tramo
    batch = []
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
                batch.append((str(key), value.encode()))
                sent += 1
            j += 1
        if batch:
            _put_records(k, stream, batch)
            batch = []
    if batch:
        _put_records(k, stream, batch)
    out.put(sent)


def _sqs_live_worker(queue_url, kind, parts, rate_per_part, duration, start_at, out):
    q = aws_client("sqs")
    while time.time() < start_at:
        time.sleep(0.001)
    sent = 0
    j = 0
    t0 = time.time()
    end = t0 + duration
    step = max(1, int(rate_per_part * len(parts) / 1000))  # ~1ms por tramo
    batch = []
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
                batch.append(value)
                sent += 1
            j += 1
        if batch:
            _send_batch(q, queue_url, batch)
            batch = []
    if batch:
        _send_batch(q, queue_url, batch)
    out.put(sent)


def _save_preload_meta(source, base_ms, events, total):
    os.makedirs(RUN_DIR, exist_ok=True)
    with open(os.path.join(RUN_DIR, f"preload-{source}.json"), "w") as f:
        json.dump({"source": source, "base_ms": base_ms, "events": events, "total": total}, f)


# --------------------------------------------------------------------------
# cgroup del motor
# --------------------------------------------------------------------------


class Cgroup:
    def __init__(self, container_id):
        self.path = self._find_cgroup(container_id)
        if self.path is None:
            raise SystemExit(f"no encuentro el cgroup del contenedor {container_id}")
        self.peak_anon = 0
        self._stop = False
        self._t = threading.Thread(target=self._sample, daemon=True)
        self._t.start()

    @staticmethod
    def _find_cgroup(container_id):
        """Ruta del cgroup del contenedor: directa (`/cg/docker/<id>`) o bajo
        systemd (`/cg/system.slice/docker-<id>.scope`), según el host."""
        candidates = [
            f"/cg/docker/{container_id}",
            f"/cg/system.slice/docker-{container_id}.scope",
        ]
        for path in candidates:
            if os.path.isdir(path):
                return path
        # Último recurso: buscar cualquier directorio que contenga el id.
        for root, dirs, _ in os.walk("/cg"):
            for d in dirs:
                if container_id in d or container_id[:12] in d:
                    return os.path.join(root, d)
        return None

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
# Progreso commiteado
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


class PaimonProgress:
    """`totalRecordCount` del último snapshot de una tabla Paimon: filas
    commiteadas (el commit atómico posición+snapshot es la misma garantía
    exactly-once visible que los offsets de Kafka). Sirve de señal de progreso
    para Kinesis/SQS (y para Redpanda, para medir los tres motores a igual)."""

    def __init__(self, table_path):
        self.table = table_path
        self._last = 0

    def total(self):
        sdir = os.path.join(self.table, "snapshot")
        try:
            latest = max(int(f.split("-")[1]) for f in os.listdir(sdir)
                         if f.startswith("snapshot-"))
            with open(os.path.join(sdir, f"snapshot-{latest}")) as f:
                self._last = json.load(f)["totalRecordCount"]
        except (OSError, ValueError, json.JSONDecodeError, KeyError):
            pass  # escritura en curso o tabla aún vacía: se queda en el último valor
        return self._last


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
    wait_table(a.table)
    # Progreso = filas commiteadas en Paimon (igual señal para los tres motores
    # y las tres fuentes). El backlog es el 90% de los eventos (el ETL filtra
    # el 10% cancelado).
    if a.source == "redpanda":
        total = end_offsets(a.topic) * 9 // 10
    else:
        total = a.events * 9 // 10
    prog = PaimonProgress(a.table)
    t_start = time.time()
    cpu_start = cg.cpu_s()
    samples = []
    mark = {}
    deadline = t_start + a.timeout
    first_commit = None
    while time.time() < deadline:
        c = prog.total()
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
# sink: throughput de la salida AWS (Kinesis/SQS) por ventana fija
# --------------------------------------------------------------------------
# floCi no expone un contador de registros (ni LatestSequenceNumber) y
# GetRecords no da abasto para seguir un stream a ~100K rec/s en tiempo real.
# Así que el throughput del sink se mide por ventana: el motor escribe durante
# warmup+duration y se mide la CPU del cgroup en ese tramo (drain_sink). Luego
# se lee la salida una sola vez para contar y verificar (verify_sink, vía
# sink_truth.py): la tasa es el promedio sobre la ventana (incluye el warmup).

def cmd_drain_sink(a):
    """CPU del motor durante la ventana del sink. El motor ya está corriendo
    (run.sh lo arrancó); se espera el warmup para que llegue a régimen y se
    mide la CPU sobre la ventana. El conteo de salida lo hace verify_sink."""
    cg = Cgroup(a.container)
    time.sleep(a.warmup)
    t0 = time.time()
    cpu0 = cg.cpu_s()
    time.sleep(a.duration)
    t1 = time.time()
    cpu1 = cg.cpu_s()
    cg.stop()
    out = {
        "engine": a.engine, "kind": a.kind, "source": a.source,
        "warmup_s": a.warmup, "window_s": round(t1 - t0, 2),
        "cpu_cores": round((cpu1 - cpu0) / (t1 - t0), 2),
        "peak_anon_mb": round(cg.peak_anon),
    }
    print(json.dumps(out))


def cmd_verify_sink(a):
    """Correctitud del sink: lee la salida de AWS y la compara contra la
    fórmula del generador (sink_truth.py). Devuelve el conteo y la correctitud."""
    import subprocess
    target = a.stream if a.source == "kinesis" else a.queue
    out = subprocess.run(
        [sys.executable, "/bench/harness/sink_truth.py", a.source, target, str(a.events)],
        capture_output=True, text=True, check=False)
    line = (out.stdout.strip().splitlines() or ["{}"])[-1]
    try:
        r = json.loads(line)
    except json.JSONDecodeError:
        r = {"error": (out.stderr or out.stdout)[-500:]}
    r["ok"] = (r.get("extra") == 0 and r.get("wrong_values") == 0
               and r.get("dup_versions") == 0)
    r.pop("wrong_sample", None)
    print(json.dumps(r))


def cmd_mksink(a):
    """Salida fresca: stream o cola nueva para el sink (borra la anterior)."""
    if a.source == "kinesis":
        create_stream(a.stream, a.shards)
    else:
        q = aws_client("sqs")
        try:
            url = q.get_queue_url(QueueName=a.queue)["QueueUrl"]
            q.delete_queue(QueueUrl=url)
        except q.exceptions.QueueDoesNotExist:
            pass
        time.sleep(1)
        q.create_queue(QueueName=a.queue)
    print(json.dumps({"source": a.source, "created": True}))


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
    if a.source == "redpanda":
        worker, target = _live_worker, a.topic
    elif a.source == "kinesis":
        ensure_stream(a.stream, a.shards)
        worker, target = _kinesis_live_worker, a.stream
    else:
        target = create_queue(a.queue)
        worker = _sqs_live_worker
    for w in range(a.procs):
        parts = [x for x in range(PARTITIONS) if x % a.procs == w]
        pr = mp.Process(target=worker,
                        args=(target, a.kind, parts, per_part, a.duration, start_at, q))
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
    if a.source == "redpanda":
        expected = end_offsets(a.topic) * 9 // 10
    else:
        expected = a.events * 9 // 10
    print(json.dumps({"rows": got["rows"], "expected": expected, "ok": got["rows"] == expected}))


def cmd_etl_truth(a):
    """ETL fila por fila: cada fila contra la fórmula del generador."""
    import subprocess

    out = subprocess.run(
        [sys.executable, "/bench/harness/etl_truth.py", a.source, a.table, str(a.events)],
        capture_output=True, text=True, check=False)
    line = (out.stdout.strip().splitlines() or ["{}"])[-1]
    try:
        r = json.loads(line)
    except json.JSONDecodeError:
        r = {"error": (out.stderr or out.stdout)[-500:]}
    r["ok"] = (r.get("missing") == 0 and r.get("extra") == 0
              and r.get("wrong_values") == 0 and r.get("dup_versions") == 0)
    r.pop("wrong_sample", None)
    print(json.dumps(r))


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
    p.add_argument("--source", choices=["redpanda", "kinesis", "sqs"], default="redpanda")
    p.add_argument("--topic", default=None)
    p.add_argument("--stream", default=None)
    p.add_argument("--queue", default=None)
    p.add_argument("--kind", choices=["etl", "win"], required=True)
    p.add_argument("--events", type=int, required=True)
    p.add_argument("--procs", type=int, default=4)
    p.add_argument("--shards", type=int, default=16)
    p.set_defaults(fn=cmd_preload)
    p = sub.add_parser("verify_etl")
    p.add_argument("table")
    p.add_argument("topic")
    p.add_argument("--source", choices=["redpanda", "kinesis", "sqs"], default="redpanda")
    p.add_argument("--events", type=int, default=0)
    p.set_defaults(fn=cmd_verify_etl)
    p = sub.add_parser("etl_truth")
    p.add_argument("source", choices=["redpanda", "kinesis", "sqs"])
    p.add_argument("table")
    p.add_argument("events", type=int)
    p.set_defaults(fn=cmd_etl_truth)
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
    p.add_argument("--source", choices=["redpanda", "kinesis", "sqs"], default="redpanda")
    p.add_argument("--topic", default=None)
    p.add_argument("--stream", default=None)
    p.add_argument("--queue", default=None)
    p.add_argument("--events", type=int, default=0)
    p.add_argument("--shards", type=int, default=16)
    p.add_argument("--table", required=True)
    p.add_argument("--group", required=True)
    p.add_argument("--container", required=True)
    p.add_argument("--timeout", type=float, default=600)
    p.add_argument("--metrics", default=None)
    p.set_defaults(fn=cmd_drain)
    p = sub.add_parser("live")
    p.add_argument("--engine", required=True)
    p.add_argument("--kind", choices=["etl", "win"], required=True)
    p.add_argument("--source", choices=["redpanda", "kinesis", "sqs"], default="redpanda")
    p.add_argument("--topic", default=None)
    p.add_argument("--stream", default=None)
    p.add_argument("--queue", default=None)
    p.add_argument("--shards", type=int, default=16)
    p.add_argument("--table", required=True)
    p.add_argument("--container", required=True)
    p.add_argument("--rate", type=int, required=True)
    p.add_argument("--duration", type=float, default=30)
    p.add_argument("--settle", type=float, default=8)
    p.add_argument("--procs", type=int, default=4)
    p.set_defaults(fn=cmd_live)
    p = sub.add_parser("mksink")
    p.add_argument("--source", choices=["kinesis", "sqs"], required=True)
    p.add_argument("--stream", default=None)
    p.add_argument("--queue", default=None)
    p.add_argument("--shards", type=int, default=16)
    p.set_defaults(fn=cmd_mksink)
    p = sub.add_parser("drain_sink")
    p.add_argument("--engine", required=True)
    p.add_argument("--kind", choices=["etl", "win"], required=True)
    p.add_argument("--source", choices=["kinesis", "sqs"], required=True)
    p.add_argument("--container", required=True)
    p.add_argument("--warmup", type=float, default=10)
    p.add_argument("--duration", type=float, default=30)
    p.set_defaults(fn=cmd_drain_sink)
    p = sub.add_parser("verify_sink")
    p.add_argument("--source", choices=["kinesis", "sqs"], required=True)
    p.add_argument("--stream", default=None)
    p.add_argument("--queue", default=None)
    p.add_argument("--events", type=int, required=True)
    p.set_defaults(fn=cmd_verify_sink)
    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
