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


app = typer.Typer(help="IoT visualizations")


@app.command()
def sensor_writes(
    kpis: Path = typer.Option(Path("results/tables/iot_sensor_writes_kpis.json"), "--kpis", help="Path to iot_sensor_writes_kpis.json"),
    outdir: Path = typer.Option(Path("results/plots"), "--outdir", help="Output directory for charts"),
) -> None:
    data_list: List[Dict[str, Any]] = json.loads(Path(kpis).read_text(encoding="utf-8"))
    wanted = ["postgres", "mongodb", "cassandra"]
    by_engine: Dict[str, Dict[str, Any]] = {d.get("engine"): d for d in data_list}
    labels = [e for e in wanted if e in by_engine] + [e for e in by_engine.keys() if e not in wanted]

    _ensure_outdir(outdir)

    # Throughput (points/sec)
    thr = [float(by_engine[e].get("throughput_points_per_s", 0.0)) for e in labels]
    _bar(labels, thr, "IoT: Throughput (points/sec)", "Points/sec", outdir / "iot_sensor_writes_throughput.png")

    # Latency p50/p95
    p50 = [float(by_engine[e].get("latency_ms", {}).get("p50", 0.0)) for e in labels]
    p95 = [float(by_engine[e].get("latency_ms", {}).get("p95", 0.0)) for e in labels]
    _cluster_latency(labels, p50, p95, "IoT: Latency p50/p95", outdir / "iot_sensor_writes_latency.png")

    # Error rate (0..1)
    err_rate = [float(by_engine[e].get("error_rate", 0.0)) for e in labels]
    _bar(labels, err_rate, "IoT: Error rate", "Rate", outdir / "iot_sensor_writes_error_rate.png", ylim=1.0)

    # Total points ingested (sum across runs)
    ok_points = [float(by_engine[e].get("counts", {}).get("ok_points", 0.0)) for e in labels]
    _bar(labels, ok_points, "IoT: Total points ingested", "Points", outdir / "iot_sensor_writes_counts.png")

    typer.echo(f"Saved charts to {outdir}")

