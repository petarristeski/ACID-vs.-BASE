from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import matplotlib.pyplot as plt
import typer


def _ensure_outdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _bar(labels: List[str], values: List[float], title: str, ylabel: str, out: Path, ylim: float | None = None) -> None:
    plt.figure(figsize=(7, 4))
    plt.bar(labels, values, color=["#1f77b4", "#2ca02c", "#ff7f0e"][: len(labels)])
    plt.ylabel(ylabel)
    plt.title(title)
    if ylim is not None:
        plt.ylim(0, ylim)
    plt.tight_layout()
    plt.savefig(out, dpi=120)
    plt.close()


def _cluster_latency(db_labels: List[str], p50: List[float], p95: List[float], title: str, out: Path) -> None:
    import numpy as np

    idx = np.arange(len(db_labels))
    width = 0.35
    plt.figure(figsize=(8, 4))
    plt.bar(idx - width / 2, p50, width, label="p50", color="#1f77b4")
    plt.bar(idx + width / 2, p95, width, label="p95", color="#ff7f0e")
    plt.xticks(idx, db_labels)
    plt.ylabel("Latency (ms)")
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out, dpi=120)
    plt.close()


app = typer.Typer(help="IoT time-series visualizations")


@app.command()
def time_series(
    kpis: Path = typer.Option(Path("results/tables/iot_time_series_kpis.json"), "--kpis", help="Path to iot_time_series_kpis.json"),
    outdir: Path = typer.Option(Path("results/plots"), "--outdir", help="Output directory for charts"),
) -> None:
    data_list: List[Dict[str, Any]] = json.loads(Path(kpis).read_text(encoding="utf-8"))
    wanted = ["postgres", "mongodb", "cassandra"]
    by_engine: Dict[str, Dict[str, Any]] = {d.get("engine"): d for d in data_list}
    labels = [e for e in wanted if e in by_engine] + [e for e in by_engine.keys() if e not in wanted]

    _ensure_outdir(outdir)

    thr = [float(by_engine[e].get("throughput_reads_per_s", 0.0)) for e in labels]
    _bar(labels, thr, "IoT: Time-series throughput (reads/sec)", "Reads/sec", outdir / "iot_time_series_throughput.png")

    p50 = [float(by_engine[e].get("latency_ms", {}).get("ts_read", {}).get("p50", 0.0)) for e in labels]
    p95 = [float(by_engine[e].get("latency_ms", {}).get("ts_read", {}).get("p95", 0.0)) for e in labels]
    _cluster_latency(labels, p50, p95, "IoT: Time-series latency p50/p95", outdir / "iot_time_series_latency.png")

    errs = [float(by_engine[e].get("errors", 0.0)) for e in labels]
    _bar(labels, errs, "IoT: Time-series errors", "Count", outdir / "iot_time_series_errors.png", ylim=max(1.0, max(errs) if errs else 1.0))

    pts = [float(by_engine[e].get("counts", {}).get("points", 0.0)) for e in labels]
    reads = [float(by_engine[e].get("counts", {}).get("reads", 0.0)) for e in labels]
    avg_pts_per_read = [ (pts[i] / reads[i]) if reads[i] else 0.0 for i in range(len(labels)) ]
    _bar(labels, avg_pts_per_read, "IoT: Avg points per read", "Points/read", outdir / "iot_time_series_avg_points_per_read.png")

    typer.echo(f"Saved charts to {outdir}")

