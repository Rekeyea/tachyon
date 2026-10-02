"""Correctitud del sink de Kinesis/SQS: cada registro de salida se compara
contra la fórmula del generador (bench.py). Lee la salida de AWS y verifica
registro por registro.

El event time base sale del meta del preload (`/bench/.run/preload-<source>.json`),
que guarda el `base_ms` exacto que usaron los productores.

  python harness/sink_truth.py <source> <target> <events>

source: kinesis o sqs
target: nombre del stream (kinesis) o URL de la cola (sqs)
events: total de eventos del input (para acotar el rango de order_id)

Reporta: output_records (cuántos llegaron), extra (order_id que no existe en
la verdad), wrong_values (registro presente pero con un valor distinto) y
dup_versions (más de un registro por order_id). No hay "missing": el sink se
mide por ventana fija, el motor no procesa todo el backlog y las filas sin
procesar son esperadas.
"""
import json, os, sys
import concurrent.futures as cf
sys.path.insert(0, "/bench/harness")
from bench import PARTITIONS, RUN_DIR, AWS_ENDPOINT, AWS_REGION

import boto3

source, target, events = sys.argv[1], sys.argv[2], int(sys.argv[3])
base = json.load(open(os.path.join(RUN_DIR, f"preload-{source}.json")))["base_ms"]
per_part = events // PARTITIONS
max_order = per_part * PARTITIONS  # order_id en [0, max_order)


def session():
    return boto3.Session(
        region_name=AWS_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def new_stats():
    return {"extra": 0, "wrong_values": 0, "dup_versions": 0, "wrong_sample": None}


def verify_record(data, seen, stats):
    """Un registro de salida contra la fórmula del generador. `seen` es un
    bytearray compartido (cada order_id cae en un solo shard, sin solapamiento)."""
    r = json.loads(data)
    oid = r["order_id"]
    j = oid // PARTITIONS
    if j >= per_part:
        stats["extra"] += 1
        return
    if j % 10 == 9:  # el SQL debió filtrar el 10% cancelado
        stats["extra"] += 1
        return
    amount = (j % 1000) + 0.5
    if (r.get("source_version") != j
            or abs(r.get("amount", 0) - amount) > 1e-9
            or r.get("event_time") != base + j // 10_000
            or r.get("status") != "paid"):
        stats["wrong_values"] += 1
        if stats["wrong_sample"] is None:
            stats["wrong_sample"] = r
        return
    if seen[oid]:
        stats["dup_versions"] += 1
    else:
        seen[oid] = 1


def read_kinesis(stream, seen):
    """Lee el stream completo en paralelo por shard (GetRecords, 100 por
    llamada). No consume: es una lectura con iterador."""
    def read_shard(shard):
        k = session().client("kinesis", endpoint_url=AWS_ENDPOINT)
        it = k.get_shard_iterator(
            StreamName=stream, ShardId=shard,
            ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        stats = new_stats()
        count = 0
        while True:
            r = k.get_records(ShardIterator=it, Limit=100)
            recs = r["Records"]
            if not recs:
                break
            for rec in recs:
                verify_record(rec["Data"], seen, stats)
                count += 1
            it = r["NextShardIterator"]
        return count, stats
    k = session().client("kinesis", endpoint_url=AWS_ENDPOINT)
    desc = k.describe_stream(StreamName=stream)["StreamDescription"]
    shards = [s["ShardId"] for s in desc["Shards"]]
    with cf.ThreadPoolExecutor(max_workers=max(1, len(shards))) as ex:
        results = list(ex.map(read_shard, shards))
    return combine(results)


def read_sqs(queue_url, seen):
    """Lee la cola completa (receive es destructivo: los deja en vuelo)."""
    q = session().client("sqs", endpoint_url=AWS_ENDPOINT)
    stats = new_stats()
    count = 0
    while True:
        r = q.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        msgs = r.get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            verify_record(m["Body"].encode(), seen, stats)
            count += 1
    return combine([(count, stats)])


def combine(results):
    count = sum(c for c, _ in results)
    stats = new_stats()
    for _, s in results:
        stats["extra"] += s["extra"]
        stats["wrong_values"] += s["wrong_values"]
        stats["dup_versions"] += s["dup_versions"]
        if s["wrong_sample"] is not None and stats["wrong_sample"] is None:
            stats["wrong_sample"] = s["wrong_sample"]
    return count, stats


seen = bytearray(max_order)
if source == "kinesis":
    count, stats = read_kinesis(target, seen)
else:
    count, stats = read_sqs(target, seen)
print(json.dumps({
    "source": source, "target": target,
    "output_records": count,
    "extra": stats["extra"], "wrong_values": stats["wrong_values"],
    "dup_versions": stats["dup_versions"],
    "wrong_sample": stats["wrong_sample"],
}))
