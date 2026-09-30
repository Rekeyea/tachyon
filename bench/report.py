"""Resumen de bench/results/*.jsonl: mediana y dispersión por motor y
escenario, y la razón Tachyon/Flink.

  python3 bench/report.py [results/2026-09-29.jsonl ...] [--since EPOCH]

Solo cuenta corridas completas cuya salida se verificó (`verify.ok`) en
drain; en live, las que aterrizaron todo lo producido (ETL) o cerraron
ventanas.
"""
import glob
import json
import statistics
import sys


def load(paths):
    rows = []
    for path in paths:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("{"):
                    rows.append(json.loads(line))
    return rows


def med(values):
    return statistics.median(values) if values else None


def cv(values):
    if len(values) < 2:
        return None
    mean = statistics.fmean(values)
    return statistics.pstdev(values) / mean if mean else None


def fmt(value, unit=""):
    if value is None:
        return "—"
    if abs(value) >= 1e6:
        return f"{value / 1e6:.2f}M{unit}"
    if abs(value) >= 1e3:
        return f"{value / 1e3:.0f}K{unit}"
    return f"{value:.2f}{unit}" if isinstance(value, float) else f"{value}{unit}"


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    paths = args or sorted(glob.glob("bench/results/*.jsonl"))
    rows = load(paths)
    drains = {}
    lives = {}
    for r in rows:
        tag = r.get("tag", "")
        if "rows_s" in r and r.get("complete") and r.get("verify", {}).get("ok"):
            drains.setdefault((r["kind"], r["cpus"], r["commit"], tag), {}).setdefault(r["engine"], []).append(r)
        elif "p50_ms" in r:
            key = (r["kind"], r["cpus"], r["commit"], r.get("lag_ms"), r["rate_target"], tag)
            lives.setdefault(key, {}).setdefault(r["engine"], []).append(r)

    print("## Throughput (backlog drenado, exactly-once, salida verificada)\n")
    print("| workload | cpus | commit | motor | corridas | filas/s (mediana) | min–max | CV entre corridas | cores usados | filas/CPU-s | RSS anon pico |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for (kind, cpus, commit, tag), engines in sorted(drains.items()):
        for engine in ("tachyon", "flink"):
            runs = engines.get(engine, [])
            if not runs:
                continue
            rates = [r.get("rows_s_fit", r["rows_s"]) for r in runs]
            print(
                f"| {kind}{' ' + tag if tag else ''} | {cpus} | {commit} | {engine} | {len(runs)} | {fmt(med(rates))} | "
                f"{fmt(min(rates))}–{fmt(max(rates))} | {fmt(cv(rates))} | {med([r.get('cpu_cores_fit', r['cpu_cores']) for r in runs]):.2f} | "
                f"{fmt(med([r.get('rows_per_cpu_s_fit', r['rows_per_cpu_s']) for r in runs]))} | {med([r['peak_anon_mb'] for r in runs]):.0f} MB |"
            )
        t = [r.get("rows_s_fit", r["rows_s"]) for r in engines.get("tachyon", [])]
        f = [r.get("rows_s_fit", r["rows_s"]) for r in engines.get("flink", [])]
        if t and f:
            print(f"| | | | **Tachyon / Flink** | | **{med(t) / med(f):.2f}x** (peor Tachyon / mejor Flink: {min(t) / max(f):.2f}x) | | | | | |")
    print()
    print("## Latencia de punta a punta (tasa fija; evento -> visible en Paimon)\n")
    print("| workload | cpus | commit | lag | tasa | motor | corridas | p50 | p99 | p99.9 | max | std | cores |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for (kind, cpus, commit, lag, rate, tag), engines in sorted(lives.items()):
        for engine in ("tachyon", "flink"):
            runs = engines.get(engine, [])
            if not runs:
                continue
            m = lambda k: med([r[k] for r in runs if k in r])
            print(
                f"| {kind}{' ' + tag if tag else ''} | {cpus} | {commit} | {lag if kind == 'win' else '—'} | {fmt(rate)}/s | {engine} | {len(runs)} | "
                f"{m('p50_ms'):.0f} ms | {m('p99_ms'):.0f} ms | {m('p999_ms'):.0f} ms | {m('max_ms'):.0f} ms | {m('std_ms'):.0f} ms | {m('cpu_cores'):.2f} |"
            )
        t = engines.get("tachyon", [])
        f = engines.get("flink", [])
        if t and f:
            tp50 = med([r["p50_ms"] for r in t]); fp50 = med([r["p50_ms"] for r in f])
            tp99 = med([r["p99_ms"] for r in t]); fp99 = med([r["p99_ms"] for r in f])
            print(f"| | | | | | **Flink / Tachyon** | | **{fp50 / max(tp50, 1):.2f}x** | **{fp99 / max(tp99, 1):.2f}x** | | | | |")


if __name__ == "__main__":
    main()
