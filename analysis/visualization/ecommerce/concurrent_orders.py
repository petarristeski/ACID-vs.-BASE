from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

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


def _pg_abort_retries(abort_rate: float, retries_total: int, out: Path) -> None:
    fig, ax1 = plt.subplots(figsize=(7, 4))
    color1 = "#1f77b4"
    ax1.bar(["abort_rate"], [abort_rate], color=color1)
    ax1.set_ylabel("Abort rate")
    ax1.set_ylim(0, 1)
    ax1.set_title("Postgres: abort rate and retries total")
    ax2 = ax1.twinx()
    color2 = "#ff7f0e"
    ax2.bar(["retries_total"], [retries_total], color=color2)
    ax2.set_ylabel("Retries total")
    fig.tight_layout()
    plt.savefig(out, dpi=120)
    plt.close()


app = typer.Typer(help="Concurrent Orders visualizations")


@app.command()
def concurrent_orders(
    kpis: Path = typer.Option(Path("results/tables/concurrent_orders_kpis.json"), "--kpis", help="Path to concurrent_orders_kpis.json"),
    outdir: Path = typer.Option(Path("results/plots"), "--outdir", help="Output directory for charts"),
) -> None:
    """Plot concurrent-orders KPIs and operational/latency views."""
    data = json.loads(Path(kpis).read_text(encoding="utf-8"))
    _ensure_outdir(outdir)

    initial_stock = float(data.get("initial_stock", 0)) or 1.0

    # DB labels
    dbs = ["postgres", "mongodb", "cassandra"]

    # 1) Reliability / correctness
    oversell_event = [
        1.0 if data.get("postgres", {}).get("oversell_event", False) else 0.0,
        1.0 if data.get("mongodb", {}).get("oversell_event", False) else 0.0,
        1.0 if data.get("cassandra", {}).get("oversell_event", False) else 0.0,
    ]
    _bar(dbs, oversell_event, "Concurrent Orders: Oversell indicator (1=yes)", "Indicator", outdir / "concurrent_kpi_oversell.png", ylim=1)

    paid_pg = float(data.get("postgres", {}).get("paid_orders", 0))
    paid_mg = float(data.get("mongodb", {}).get("paid_orders", 0))
    paid_cs = float(data.get("cassandra", {}).get("paid_orders", 0))
    oversell_factor = [
        max(0.0, (paid_pg - initial_stock) / initial_stock),
        max(0.0, (paid_mg - initial_stock) / initial_stock),
        max(0.0, (paid_cs - initial_stock) / initial_stock),
    ]
    _bar(dbs, oversell_factor, "Concurrent Orders: Oversell factor", "(paid - stock) / stock", outdir / "concurrent_kpi_oversell_factor.png")

    final_available = [
        float(data.get("postgres", {}).get("qty_on_hand_end", 0)),
        float(data.get("mongodb", {}).get("available_end", 0)),
        float(data.get("cassandra", {}).get("available_end", 0)),
    ]
    _bar(dbs, final_available, "Concurrent Orders: Final stock/available", "Units", outdir / "concurrent_kpi_available_end.png")

    # 2) Operational behavior
    paid = [paid_pg, paid_mg, paid_cs]
    oos = [
        float(data.get("postgres", {}).get("oos_attempts", 0)),
        float(data.get("mongodb", {}).get("oos_attempts", 0)),
        float(data.get("cassandra", {}).get("oos_attempts", 0)),
    ]
    # stacked bar for success vs OOS
    import numpy as np

    idx = np.arange(len(dbs))
    plt.figure(figsize=(8, 4))
    plt.bar(dbs, paid, label="paid_orders", color="#2ca02c")
    plt.bar(dbs, oos, bottom=paid, label="oos_attempts", color="#ff7f0e")
    plt.ylabel("Count")
    plt.title("Concurrent Orders: Success vs OOS attempts")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outdir / "concurrent_counts_success_oos.png", dpi=120)
    plt.close()

    # PG aborts vs retries
    _pg_abort_retries(
        float(data.get("postgres", {}).get("abort_rate", 0.0)),
        int(data.get("postgres", {}).get("retries_total", 0)),
        outdir / "concurrent_pg_abort_retries.png",
    )

    # 3) Performance: throughput and latency
    thr = [
        float(data.get("postgres", {}).get("throughput_succ_per_s", 0.0)),
        float(data.get("mongodb", {}).get("throughput_succ_per_s", 0.0)),
        float(data.get("cassandra", {}).get("throughput_succ_per_s", 0.0)),
    ]
    _bar(dbs, thr, "Concurrent Orders: Throughput (success/sec)", "Success/sec", outdir / "concurrent_perf_throughput.png")

    lat_p50 = [
        float(data.get("postgres", {}).get("latency_ms", {}).get("p50", 0.0)),
        float(data.get("mongodb", {}).get("latency_ms", {}).get("p50", 0.0)),
        float(data.get("cassandra", {}).get("latency_ms", {}).get("p50", 0.0)),
    ]
    lat_p95 = [
        float(data.get("postgres", {}).get("latency_ms", {}).get("p95", 0.0)),
        float(data.get("mongodb", {}).get("latency_ms", {}).get("p95", 0.0)),
        float(data.get("cassandra", {}).get("latency_ms", {}).get("p95", 0.0)),
    ]
    _cluster_latency(dbs, lat_p50, lat_p95, "Concurrent Orders: Latency p50/p95", outdir / "concurrent_perf_latency.png")

    # 4) Optional: scatter throughput vs reliability (1 - oversell_factor)
    try:
        reliability = [1 - oversell_factor[0], 1 - oversell_factor[1], 1 - oversell_factor[2]]
        plt.figure(figsize=(6, 4))
        colors = {"postgres": "#1f77b4", "mongodb": "#2ca02c", "cassandra": "#ff7f0e"}
        for i, db in enumerate(dbs):
            plt.scatter(thr[i], reliability[i], color=colors.get(db, "#333"))
            plt.text(thr[i], reliability[i] + 0.02, db, ha="center")
        plt.xlabel("Success/sec")
        plt.ylabel("Reliability (1 - oversell_factor)")
        plt.title("Throughput vs Reliability")
        plt.ylim(0, 1.05)
        plt.tight_layout()
        plt.savefig(outdir / "concurrent_scatter_thr_vs_reliability.png", dpi=120)
        plt.close()
    except Exception:
        pass
    typer.echo(f"Saved charts to {outdir}")

