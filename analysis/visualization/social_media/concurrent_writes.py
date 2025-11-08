from __future__ import annotations

import json
from pathlib import Path
from typing import List, Dict, Any

import matplotlib.pyplot as plt
import numpy as np
import typer


def _ensure_outdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _load_kpis(path: Path) -> List[Dict[str, Any]]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _bar(labels: List[str], values: List[float], title: str, ylabel: str, out: Path):
    plt.figure(figsize=(7, 4))
    plt.bar(labels, values, color=["#1f77b4", "#2ca02c", "#ff7f0e"][: len(labels)])
    plt.ylabel(ylabel)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out, dpi=120)
    plt.close()


def _stack_counts(engines: List[Dict[str, Any]], out: Path):
    labels = [e["engine"] for e in engines]
    posts = [int(e.get("counts", {}).get("posts", 0)) for e in engines]
    likes = [int(e.get("counts", {}).get("likes", 0)) for e in engines]
    comments = [int(e.get("counts", {}).get("comments", 0)) for e in engines]
    reads = [int(e.get("counts", {}).get("reads", 0)) for e in engines]
    bottom = np.zeros(len(labels))
    plt.figure(figsize=(8, 4))
    for series, name, color in [
        (posts, "posts", "#1f77b4"),
        (likes, "likes", "#2ca02c"),
        (comments, "comments", "#ff7f0e"),
        (reads, "reads", "#9467bd"),
    ]:
        plt.bar(labels, series, bottom=bottom, label=name, color=color)
        bottom = bottom + np.array(series)
    plt.ylabel("Count")
    plt.title("Social Media: operation counts by engine")
    plt.legend(ncol=4)
    plt.tight_layout()
    plt.savefig(out, dpi=120)
    plt.close()


def _latency_by_op(engines: List[Dict[str, Any]], outdir: Path):
    ops = ["create_post", "like", "comment", "read"]
    labels = [e["engine"] for e in engines]
    for op in ops:
        p50 = [float(e.get("latency_ms", {}).get(op, {}).get("p50", 0.0)) for e in engines]
        p95 = [float(e.get("latency_ms", {}).get(op, {}).get("p95", 0.0)) for e in engines]
        idx = np.arange(len(labels)); width = 0.35
        plt.figure(figsize=(8, 4))
        plt.bar(idx - width / 2, p50, width, label="p50", color="#1f77b4")
        plt.bar(idx + width / 2, p95, width, label="p95", color="#ff7f0e")
        plt.xticks(idx, labels)
        plt.ylabel("Latency (ms)")
        plt.title(f"Social Media: {op} latency p50/p95")
        plt.legend()
        plt.tight_layout()
        plt.savefig(outdir / f"social_{op}_latency.png", dpi=120)
        plt.close()


app = typer.Typer(help="Social media concurrent-writes visualizations")


@app.command()
def concurrent_writes(
    kpis: Path = typer.Option(Path("results/tables/social_media_concurrent_writes_kpis.json"), "--kpis", help="Path to KPI JSON (list of dicts)"),
    outdir: Path = typer.Option(Path("results/plots"), "--outdir", help="Output directory"),
) -> None:
    _ensure_outdir(outdir)
    engines = _load_kpis(kpis)
    if not engines:
        typer.echo("No KPI entries found.")
        raise typer.Exit(code=1)

    labels = [e["engine"] for e in engines]

    # Throughput and RYW
    thr = [float(e.get("throughput_ops_per_s", 0.0)) for e in engines]
    _bar(labels, thr, "Social Media: Throughput (ops/sec)", "ops/sec", outdir / "social_throughput.png")
    ryw = [float(e.get("ryw_success_rate", 0.0)) for e in engines]
    _bar(labels, ryw, "Social Media: RYW success rate", "rate", outdir / "social_ryw.png")

    # Duplicate like rejects (unique enforcement behavior)
    dups = [int(e.get("dup_like_rejects", 0)) for e in engines]
    _bar(labels, dups, "Social Media: duplicate like rejects", "count", outdir / "social_dup_like_rejects.png")

    # Latency by operation (p50/p95)
    _latency_by_op(engines, outdir)

    # Operational counts (stacked)
    _stack_counts(engines, outdir / "social_counts_stacked.png")
    typer.echo(f"Saved charts to {outdir}")

