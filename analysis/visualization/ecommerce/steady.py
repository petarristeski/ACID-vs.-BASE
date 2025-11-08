from __future__ import annotations

from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import typer


app = typer.Typer(help="E-commerce steady-load visualizations")


def _load(input_csv: Path, filter_concurrency: Optional[int]) -> pd.DataFrame:
    df = pd.read_csv(input_csv)
    if "scenario" in df.columns:
        df = df[df["scenario"] == "steady"]
    if filter_concurrency is not None and "concurrency" in df.columns:
        df = df[df["concurrency"] == filter_concurrency]
    # Derived
    if {"customers", "orders_per_user"}.issubset(df.columns):
        df["attempted"] = df["customers"] * df["orders_per_user"]
        df["exception_count"] = (df["attempted"] - df["total"]).clip(lower=0)
        df["exception_rate"] = (df["exception_count"] / df["attempted"]).fillna(0.0)
    return df


def _ensure_outdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


@app.command()
def steady(
    input: Path = typer.Option(..., "--input", help="Path to steady_summary.csv"),
    outdir: Path = typer.Option(Path("results/plots"), "--outdir", help="Output directory for charts"),
    filter_concurrency: Optional[int] = typer.Option(None, help="Only include rows with this concurrency"),
) -> None:
    """Generate TPS and error-rate charts for the steady scenario."""
    sns.set_theme(style="whitegrid")
    _ensure_outdir(outdir)
    df = _load(input, filter_concurrency)
    if df.empty:
        typer.echo("No rows to plot. Check input or filters.")
        raise typer.Exit(code=1)

    # 1) Mean TPS by DB with 95% CI (normal approx)
    g = df.groupby("db").agg(n=("tps", "count"), mean_tps=("tps", "mean"), std_tps=("tps", "std"))
    g["stderr"] = (g["std_tps"] / g["n"].clip(lower=1) ** 0.5).fillna(0.0)
    g["ci95"] = 1.96 * g["stderr"]
    order = g.sort_values("mean_tps").index.tolist()
    plt.figure(figsize=(7, 4))
    plt.bar(g.index, g["mean_tps"], yerr=g["ci95"], capsize=4)
    plt.ylabel("Mean TPS (95% CI)")
    plt.xlabel("DB")
    plt.title("Steady: TPS by DB" + (f" (concurrency={filter_concurrency})" if filter_concurrency else ""))
    plt.tight_layout()
    p1 = outdir / ("steady_tps_by_db_fixed.png" if filter_concurrency else "steady_tps_by_db.png")
    plt.savefig(p1, dpi=120)
    plt.close()

    # 2) TPS distribution (boxplot) by DB
    plt.figure(figsize=(7, 4))
    sns.boxplot(data=df, x="db", y="tps", order=order)
    sns.stripplot(data=df, x="db", y="tps", order=order, color="#333", size=3, alpha=0.6)
    plt.ylabel("TPS")
    plt.xlabel("DB")
    plt.title("Steady: TPS distribution by DB" + (f" (concurrency={filter_concurrency})" if filter_concurrency else ""))
    plt.tight_layout()
    p2 = outdir / ("steady_tps_box_fixed.png" if filter_concurrency else "steady_tps_box.png")
    plt.savefig(p2, dpi=120)
    plt.close()

    # 3) Outcome rates (stacked) by DB: ok, failed, out_of_stock, exception
    # Weighted by totals
    w = df.groupby("db").agg(
        ok_sum=("ok", "sum"),
        failed_sum=("failed", "sum"),
        oos_sum=("out_of_stock", "sum"),
        total_sum=("total", "sum"),
        attempted_sum=("attempted", "sum"),
        ex_sum=("exception_count", "sum"),
    )
    w = w[(w["total_sum"] > 0) & (w["attempted_sum"] > 0)]
    rates = pd.DataFrame({
        "ok_rate": w["ok_sum"] / w["total_sum"],
        "failed_rate": w["failed_sum"] / w["total_sum"],
        "oos_rate": w["oos_sum"] / w["total_sum"],
        "exception_rate": w["ex_sum"] / w["attempted_sum"],
    })
    plt.figure(figsize=(8, 4))
    bottom = None
    colors = ["#2ca02c", "#ff7f0e", "#1f77b4", "#d62728"]
    for (col, color) in zip(["ok_rate", "failed_rate", "oos_rate", "exception_rate"], colors):
        plt.bar(rates.index, rates[col], bottom=bottom, label=col.replace("_", " "), color=color)
        bottom = (rates[col] if bottom is None else bottom + rates[col])
    plt.ylim(0, 1)
    plt.ylabel("Proportion")
    plt.xlabel("DB")
    plt.title("Steady: Outcome rates by DB" + (f" (concurrency={filter_concurrency})" if filter_concurrency else ""))
    plt.legend(ncol=2)
    plt.tight_layout()
    p3 = outdir / ("steady_outcomes_by_db_fixed.png" if filter_concurrency else "steady_outcomes_by_db.png")
    plt.savefig(p3, dpi=120)
    plt.close()

    # 4) TPS vs concurrency (line per DB)
    if "concurrency" in df.columns:
        gg = df.groupby(["db", "concurrency"]).agg(mean_tps=("tps", "mean")).reset_index()
        plt.figure(figsize=(8, 4))
        sns.lineplot(data=gg, x="concurrency", y="mean_tps", hue="db", marker="o")
        plt.ylabel("Mean TPS")
        plt.xlabel("Concurrency")
        plt.title("Steady: TPS vs concurrency by DB")
        plt.tight_layout()
        p4 = outdir / "steady_tps_vs_concurrency.png"
        plt.savefig(p4, dpi=120)
        plt.close()

    typer.echo(f"Saved charts to {outdir}")

