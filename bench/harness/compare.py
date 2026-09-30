"""Compara dos tablas Paimon PK del benchmark fila por fila.

Lee los data files vivos del último snapshot de cada tabla, deduplica por
primary key (se queda con el `_SEQUENCE_NUMBER` más alto, como el merge de
Paimon) y compara las columnas de valor en las claves que están en las dos.

  python harness/compare.py <tabla_a> <tabla_b> <pk,cols> <value,cols>
"""
import json, os, sys
import fastavro
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def live_files(table):
    sdir = os.path.join(table, "snapshot")
    latest = max(int(f.split("-")[1]) for f in os.listdir(sdir) if f.startswith("snapshot-"))
    snap = json.load(open(os.path.join(sdir, f"snapshot-{latest}")))
    live = {}
    for lst in (snap["baseManifestList"], snap["deltaManifestList"]):
        for m in fastavro.reader(open(os.path.join(table, "manifest", lst), "rb")):
            for e in fastavro.reader(open(os.path.join(table, "manifest", m["_FILE_NAME"]), "rb")):
                key = (e["_BUCKET"], e["_FILE"]["_FILE_NAME"])
                if e["_KIND"] == 0:
                    live[key] = True
                else:
                    live.pop(key, None)
    return [os.path.join(table, f"bucket-{b}", f) for b, f in live]


def load(table, pk, vals):
    cols = pk + vals + ["_SEQUENCE_NUMBER"]
    t = pa.concat_tables(pq.read_table(f, columns=cols) for f in live_files(table))
    # Último sequence por clave (merge engine deduplicate).
    t = t.sort_by([(c, "ascending") for c in pk] + [("_SEQUENCE_NUMBER", "descending")])
    keys = [t.column(c).to_numpy() for c in pk]
    import numpy as np
    first = np.ones(t.num_rows, dtype=bool)
    if t.num_rows:
        same = np.ones(t.num_rows - 1, dtype=bool)
        for k in keys:
            same &= k[1:] == k[:-1]
        first[1:] = ~same
    dup = int((~first).sum())
    return t.filter(pa.array(first)).drop_columns(["_SEQUENCE_NUMBER"]), dup


def main():
    a_path, b_path = sys.argv[1], sys.argv[2]
    pk = sys.argv[3].split(",")
    vals = sys.argv[4].split(",")
    a, a_dup = load(a_path, pk, vals)
    b, b_dup = load(b_path, pk, vals)
    j = a.join(b, keys=pk, join_type="full outer", left_suffix="_a", right_suffix="_b")
    only_a = j.filter(pc.is_null(j.column(vals[0] + "_b"))).num_rows
    only_b = j.filter(pc.is_null(j.column(vals[0] + "_a"))).num_rows
    both = j.filter(pc.and_(pc.is_valid(j.column(vals[0] + "_a")), pc.is_valid(j.column(vals[0] + "_b"))))
    diff = 0
    for v in vals:
        diff += both.filter(pc.not_equal(both.column(v + "_a"), both.column(v + "_b"))).num_rows
    out = {"rows_a": a.num_rows, "rows_b": b.num_rows, "dup_versions_a": a_dup, "dup_versions_b": b_dup,
           "both": both.num_rows, "only_a": only_a, "only_b": only_b, "value_mismatches": diff}
    if only_a or only_b:
        for name, side in (("a", "_b"), ("b", "_a")):
            sub = j.filter(pc.is_null(j.column(vals[0] + side)))
            if sub.num_rows and "window_start" in pk:
                ws = sub.column("window_start")
                out[f"only_{name}_windows"] = [int(pc.min(ws).as_py()), int(pc.max(ws).as_py()),
                                               len(pc.unique(ws))]
    print(json.dumps(out))


if __name__ == "__main__":
    main()
