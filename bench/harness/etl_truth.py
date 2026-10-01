"""Verdad del workload ETL: recalcula cada fila a partir de la fórmula del
generador (bench.py) y la compara con la salida de un motor, fila por fila.

El event time base sale del meta del preload (`/bench/.run/preload-<source>.json`),
que guarda el `base_ms` exacto que usaron los productores.

  python harness/etl_truth.py <source> <table> <events>

Reporta: missing (en la verdad, no en la tabla), extra (en la tabla, no en la
verdad), wrong_values (fila presente pero con un valor distinto) y
dup_versions (más de una versión por clave, el merge debió dejar una).
"""
import json, os, sys
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
sys.path.insert(0, "/bench/harness")
from bench import PARTITIONS, RUN_DIR
import compare  # noqa: reutiliza load()

source, table, events = sys.argv[1], sys.argv[2], int(sys.argv[3])
base = json.load(open(os.path.join(RUN_DIR, f"preload-{source}.json")))["base_ms"]

per_part = events // PARTITIONS
j = np.arange(per_part, dtype=np.int64)
keep = j % 10 != 9  # el SQL filtra el 10% cancelado
j = j[keep]
parts = np.arange(PARTITIONS, dtype=np.int64)
J, P = np.meshgrid(j, parts, indexing="ij")
J, P = J.ravel(), P.ravel()
truth = pa.table({
    "order_id": J * PARTITIONS + P,
    "source_version": J,
    "amount": (J % 1000).astype(np.float64) + 0.5,
    "event_time": base + J // 10_000,
    "status": np.full(len(J), "paid"),
}).rename_columns(["order_id", "source_version_t", "amount_t", "event_time_t", "status_t"])

got, dup = compare.load(table, ["order_id"], ["source_version", "amount", "event_time", "status"])
jn = got.join(truth, keys=["order_id"], join_type="full outer")
missing = jn.filter(pc.is_null(jn.column("source_version"))).num_rows   # en verdad, no en tabla
extra = jn.filter(pc.is_null(jn.column("source_version_t"))).num_rows  # en tabla, no en verdad
both = jn.filter(pc.and_(pc.is_valid(jn.column("source_version")),
                         pc.is_valid(jn.column("source_version_t"))))
bad_mask = pc.or_(
    pc.not_equal(both.column("source_version"), both.column("source_version_t")),
    pc.or_(
        pc.not_equal(both.column("amount"), both.column("amount_t")),
        pc.or_(
            pc.not_equal(both.column("event_time"), both.column("event_time_t")),
            pc.not_equal(both.column("status"), both.column("status_t")),
        ),
    ),
)
bad = both.filter(bad_mask)
print(json.dumps({
    "source": source, "table": table,
    "truth_rows": truth.num_rows, "table_rows": got.num_rows,
    "dup_versions": dup, "missing": missing, "extra": extra,
    "wrong_values": bad.num_rows,
    "wrong_sample": bad.slice(0, 3).to_pylist(),
}))
