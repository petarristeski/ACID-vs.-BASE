from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import typer


def _ensure_outdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _bar_plot(values: Dict[str, float], title: str, ylabel: str, out: Path) -> None:
    if not values:
        return
    labels = list(values.keys())
    data = [values[k] for k in labels]
    plt.figure(figsize=(7, 4))
    plt.bar(labels, data, color=["#1f77b4", "#2ca02c", "#ff7f0e"][: len(labels)])
    plt.ylabel(ylabel)
    plt.title(title)
    plt.ylim(0, 1)
    plt.tight_layout()
    plt.savefig(out, dpi=120)
    plt.close()


def _stacked_counts(counts: Dict[str, int], title: str, out: Path) -> None:
    # Build per-DB components using available keys
    dbs: List[str] = []
    stacks: Dict[str, List[int]] = {}
    labels: List[str] = []

    # Postgres
    pg_parts = [
        ("pg_orders_ok", "ok"),
        ("pg_rolled_back", "rolled_back"),
        ("pg_abort", "abort"),
    ]
    if any(k in counts for k, _ in pg_parts):
        dbs.append("postgres")
        stacks["postgres"] = [counts.get(k, 0) for k, _ in pg_parts]
        labels = [lab for _, lab in pg_parts]

    # MongoDB
    mongo_parts = [
        ("mongo_orders_ok", "ok"),
        ("mongo_compensations", "compensations"),
        ("mongo_stale_reads", "stale_reads"),
    ]
    if any(k in counts for k, _ in mongo_parts):
        dbs.append("mongodb")
        stacks["mongodb"] = [counts.get(k, 0) for k, _ in mongo_parts]
        if not labels:
            labels = [lab for _, lab in mongo_parts]

    # Cassandra
    cass_parts = [
        ("cass_orders_ok", "ok"),
        ("cass_compensations", "compensations"),
        ("cass_stale_reads", "stale_reads"),
    ]
    if any(k in counts for k, _ in cass_parts):
        dbs.append("cassandra")
        stacks["cassandra"] = [counts.get(k, 0) for k, _ in cass_parts]
        if not labels:
            labels = [lab for _, lab in cass_parts]

    if not dbs:
        return

    # Transpose stacks into series per component label
    bottoms = [0] * len(dbs)
    colors = ["#2ca02c", "#ff7f0e", "#1f77b4", "#d62728"]
    plt.figure(figsize=(8, 4))
    for idx, lab in enumerate(labels):
        vals = [stacks[db][idx] if idx < len(stacks[db]) else 0 for db in dbs]
        plt.bar(dbs, vals, bottom=bottoms, label=lab, color=colors[idx % len(colors)])
        bottoms = [bottoms[i] + vals[i] for i in range(len(dbs))]
    plt.ylabel("Count")
    plt.title(title)
    plt.legend(ncol=3)
    plt.tight_layout()
    plt.savefig(out, dpi=120)
    plt.close()


app = typer.Typer(help="Rollback visualizations")


@app.command()
def rollback(
    kpis: Path = typer.Option(Path("results/tables/rollback_kpis.json"), "--kpis", help="Path to rollback_kpis.json"),
    outdir: Path = typer.Option(Path("results/plots"), "--outdir", help="Output directory for charts"),
) -> None:
    """Plot high-level KPI bars and operational stacked counts for rollback scenario."""
    data = json.loads(Path(kpis).read_text(encoding="utf-8"))
    _ensure_outdir(outdir)

    # 1) KPI bars
    _bar_plot(data.get("oversell_rate", {}), "Rollback: Oversell rate by DB", "Rate", outdir / "rollback_kpi_oversell.png")
    _bar_plot(data.get("orphan_payment_rate", {}), "Rollback: Orphan payment rate by DB", "Rate", outdir / "rollback_kpi_orphan.png")
    _bar_plot(data.get("stale_read_rate", {}), "Rollback: Stale read rate by DB", "Rate", outdir / "rollback_kpi_stale.png")
    _bar_plot(data.get("abort_rate", {}), "Rollback: Abort rate by DB", "Rate", outdir / "rollback_kpi_abort.png")

    # 2) Operational counts stacked
    counts = data.get("counts", {})
    _stacked_counts(counts, "Rollback: Operational counts by DB", outdir / "rollback_counts_stacked.png")
    typer.echo(f"Saved charts to {outdir}")

