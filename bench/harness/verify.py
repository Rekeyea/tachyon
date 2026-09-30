"""Cuenta las filas visibles del último snapshot de una tabla Paimon sumando
`_ROW_COUNT` de los data files vivos (ADD - DELETE de todos los manifests)."""
import json, os, sys
import fastavro

def rows(table):
    sdir = os.path.join(table, "snapshot")
    ids = [int(f.split("-")[1]) for f in os.listdir(sdir) if f.startswith("snapshot-")]
    if not ids:
        return {"snapshots": 0, "rows": 0}
    snap = json.load(open(os.path.join(sdir, f"snapshot-{max(ids)}")))
    live = {}
    for lst in (snap["baseManifestList"], snap["deltaManifestList"]):
        for m in fastavro.reader(open(os.path.join(table, "manifest", lst), "rb")):
            for e in fastavro.reader(open(os.path.join(table, "manifest", m["_FILE_NAME"]), "rb")):
                key = (e["_BUCKET"], e["_FILE"]["_FILE_NAME"])
                if e["_KIND"] == 0:
                    live[key] = e["_FILE"]["_ROW_COUNT"]
                else:
                    live.pop(key, None)
    return {"snapshots": len(ids), "latest": max(ids), "files": len(live),
            "rows": sum(live.values()), "totalRecordCount": snap.get("totalRecordCount")}

if __name__ == "__main__":
    print(json.dumps(rows(sys.argv[1])))
