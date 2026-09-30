"""Verdad del workload de ventana: recalcula COUNT/SUM por (clave, ventana de
1s) a partir de la fórmula del generador (bench.py) y la compara con la
salida de un motor. Toma el event time base del primer mensaje del topic."""
import json, sys
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from confluent_kafka import Consumer, TopicPartition
sys.path.insert(0, "/bench/harness")
from bench import PARTITIONS, WIN_KEYS, WIN_EVENTS_PER_SEC
import compare  # noqa: reutiliza load()

topic, table, events = sys.argv[1], sys.argv[2], int(sys.argv[3])
c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "truth-probe", "enable.auto.commit": False})
c.assign([TopicPartition(topic, 0, 0)])
m = c.poll(10)
base = json.loads(m.value())["event_time"]
c.close()

per_part = events // PARTITIONS
j = np.arange(per_part, dtype=np.int64)
et = base + (j * PARTITIONS * 1000) // WIN_EVENTS_PER_SEC
ws_all, key_all, amt_all = [], [], []
for p in range(PARTITIONS):
    key_all.append((j % (WIN_KEYS // PARTITIONS)) * PARTITIONS + p)
    ws_all.append(et - et % 1000)
    amt_all.append(j % 100)
key = np.concatenate(key_all); ws = np.concatenate(ws_all); amt = np.concatenate(amt_all)
t = pa.table({"order_id": key, "window_start": ws, "amount": amt})
truth = t.group_by(["order_id", "window_start"]).aggregate([("amount", "count"), ("amount", "sum")])
truth = truth.rename_columns(["order_id", "window_start", "n_t", "amount_t"])

got, dup = compare.load(table, ["order_id", "window_start"], ["n", "amount"])
jn = got.join(truth, keys=["order_id", "window_start"], join_type="left outer")
missing_in_truth = jn.filter(pc.is_null(jn.column("n_t"))).num_rows
bad = jn.filter(pc.or_(pc.not_equal(jn.column("n"), jn.column("n_t")),
                       pc.not_equal(jn.column("amount"), jn.column("amount_t"))))
emitted_windows = pc.unique(got.column("window_start"))
max_ws = pc.max(got.column("window_start")).as_py()
# Ventanas cerradas esperadas: todas las de window_start <= la última emitida.
expected = truth.filter(pc.less_equal(truth.column("window_start"), max_ws))
print(json.dumps({
    "table": table, "rows": got.num_rows, "dup_versions": dup,
    "wrong_values": bad.num_rows, "not_in_truth": missing_in_truth,
    "expected_rows_up_to_last_window": expected.num_rows,
    "missing_rows": expected.num_rows - (got.num_rows - missing_in_truth),
    "wrong_sample": bad.slice(0, 3).to_pylist(),
}))
