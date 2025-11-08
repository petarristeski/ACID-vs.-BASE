from __future__ import annotations

from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import typer


app = typer.Typer(help="E-commerce payments visualizations")


def _load(input_csv: Path, filter_wave_size: Optional[int], filter_waves: Optional[int]) -> pd.DataFrame:
    df = pd.read_csv(input_csv)
    if "scenario" in df.columns:
        df = df[df["scenario"] == "payments"]
    # Coerce numeric columns we rely on
    for col in [
        "ok",
        "failed",
        "out_of_stock",
        "total",
        "attempted",
        "compensations",
        "tps",
        "failure_rate",
        "wave_size",
        "waves",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if filter_wave_size is not None and "wave_size" in df.columns:
        df = df[df["wave_size"] == filter_wave_size]
    if filter_waves is not None and "waves" in df.columns:
        df = df[df["waves"] == filter_waves]
    # Derived
    if {"attempted"}.issubset(df.columns):
        if "exception_count" in df.columns:
            df["exception_count"] = pd.to_numeric(df["exception_count"], errors="coerce").fillna(0)
        else:
            df["exception_count"] = 0
        df["exception_rate"] = (df["exception_count"] / df["attempted"]).fillna(0.0)
    else:
        if {"customers", "orders_per_user"}.issubset(df.columns):
            df["attempted"] = df["customers"] * df["orders_per_user"]
            df["exception_count"] = (df["attempted"] - df["total"]).clip(lower=0)
            df["exception_rate"] = (df["exception_count"] / df["attempted"]).fillna(0.0)
    if "compensations" not in df.columns:
        df["compensations"] = 0
    if "wave_size" not in df.columns and "concurrency" in df.columns:
        df["wave_size"] = df["concurrency"]
    return df


def _ensure_outdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


@app.command()
def payments(
    input: Path = typer.Option(..., "--input", help="Path to merged payments CSV"),
    outdir: Path = typer.Option(Path("results/plots"), "--outdir", help="Output directory for charts"),
    filter_wave_size: Optional[int] = typer.Option(None, help="Only include rows with this wave size (concurrency)"),
    filter_waves: Optional[int] = typer.Option(None, help="Only include rows with this number of waves"),
) -> None:
    """Generate payments scenario charts (TPS, outcomes, compensation, excess failure)."""
    sns.set_theme(style="whitegrid")
    _ensure_outdir(outdir)
    df = _load(input, filter_wave_size, filter_waves)
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
    ttl = "Payments: TPS by DB"
    if filter_wave_size:
        ttl += f" (wave_size={filter_wave_size})"
    if filter_waves:
        ttl += f" (waves={filter_waves})"
    plt.title(ttl)
    plt.tight_layout()
    p1 = outdir / ("payments_tps_by_db_fixed.png" if (filter_wave_size or filter_waves) else "payments_tps_by_db.png")
    plt.savefig(p1, dpi=120)
    plt.close()

    # 2) TPS distribution (boxplot) by DB
    plt.figure(figsize=(7, 4))
    sns.boxplot(data=df, x="db", y="tps", order=order)
    sns.stripplot(data=df, x="db", y="tps", order=order, color="#333", size=3, alpha=0.6)
    plt.ylabel("TPS")
    plt.xlabel("DB")
    plt.title("Payments: TPS distribution by DB")
    plt.tight_layout()
    p2 = outdir / ("payments_tps_box_fixed.png" if (filter_wave_size or filter_waves) else "payments_tps_box.png")
    plt.savefig(p2, dpi=120)
    plt.close()

    # 3) Outcome rates (stacked) by DB: ok, failed, out_of_stock, exception
    w = df.groupby("db").agg(
        ok_sum=("ok", "sum"),
        failed_sum=("failed", "sum"),
        oos_sum=("out_of_stock", "sum"),
        total_sum=("total", "sum"),
        attempted_sum=("attempted", "sum"),
        ex_sum=("exception_count", "sum"),
    )
    # Keep rows with total > 0; attempteds may be 0/NaN for older runs
    w = w[(w["total_sum"] > 0)]
    rates = pd.DataFrame({
        "ok_rate": w["ok_sum"] / w["total_sum"],
        "failed_rate": w["failed_sum"] / w["total_sum"],
        "oos_rate": w["oos_sum"] / w["total_sum"],
        "exception_rate": (w["ex_sum"] / w["attempted_sum"]).fillna(0.0),
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
    plt.title("Payments: Outcome rates by DB")
    plt.legend(ncol=2)
    plt.tight_layout()
    p3 = outdir / ("payments_outcomes_by_db_fixed.png" if (filter_wave_size or filter_waves) else "payments_outcomes_by_db.png")
    plt.savefig(p3, dpi=120)
    plt.close()

    # 4) Excess failure over configured failure_rate
    gg = df.groupby("db").apply(
        lambda x: pd.Series({
            "obs_fail_rate": (x["failed"].sum() / x["total"].sum()) if x["total"].sum() else 0.0,
            "cfg_fail_rate": (x["failure_rate"].mul(x["total"]).sum() / x["total"].sum()) if x["total"].sum() else 0.0,
        })
    )
    gg["excess_failure"] = gg["obs_fail_rate"] - gg["cfg_fail_rate"]
    plt.figure(figsize=(7, 4))
    plt.bar(gg.index, gg["excess_failure"], color="#ff7f0e")
    plt.axhline(0, color="#333", linewidth=1)
    plt.ylabel("Observed - Configured failure rate")
    plt.xlabel("DB")
    plt.title("Payments: Excess failure over target")
    plt.tight_layout()
    p4 = outdir / ("payments_excess_failure_by_db_fixed.png" if (filter_wave_size or filter_waves) else "payments_excess_failure_by_db.png")
    plt.savefig(p4, dpi=120)
    plt.close()

    # 5) Compensation rate by DB
    gg2 = df.groupby("db").apply(lambda x: (x["compensations"].sum() / x["total"].sum()) if x["total"].sum() else 0.0)
    plt.figure(figsize=(7, 4))
    plt.bar(gg2.index, gg2.values, color="#1f77b4")
    plt.ylabel("Compensations / total")
    plt.xlabel("DB")
    plt.title("Payments: Compensation rate by DB")
    plt.tight_layout()
    p5 = outdir / ("payments_compensation_rate_by_db_fixed.png" if (filter_wave_size or filter_waves) else "payments_compensation_rate_by_db.png")
    plt.savefig(p5, dpi=120)
    plt.close()

    # 6) TPS vs wave_size (if varied)
    if "wave_size" in df.columns:
        gg3 = df.groupby(["db", "wave_size"]).agg(mean_tps=("tps", "mean")).reset_index()
        if len(gg3["wave_size"].unique()) > 1:
            plt.figure(figsize=(8, 4))
            sns.lineplot(data=gg3, x="wave_size", y="mean_tps", hue="db", marker="o")
            plt.ylabel("Mean TPS")
            plt.xlabel("Wave size (concurrency)")
            plt.title("Payments: TPS vs wave size by DB")
            plt.tight_layout()
            p6 = outdir / "payments_tps_vs_wave_size.png"
            plt.savefig(p6, dpi=120)
            plt.close()

    # 7) TPS vs waves (if varied)
    if "waves" in df.columns:
        gg4 = df.groupby(["db", "waves"]).agg(mean_tps=("tps", "mean")).reset_index()
        if len(gg4["waves"].unique()) > 1:
            plt.figure(figsize=(8, 4))
            sns.lineplot(data=gg4, x="waves", y="mean_tps", hue="db", marker="o")
            plt.ylabel("Mean TPS")
            plt.xlabel("Number of waves")
            plt.title("Payments: TPS vs waves by DB")
            plt.tight_layout()
            p7 = outdir / "payments_tps_vs_waves.png"
            plt.savefig(p7, dpi=120)
            plt.close()

    typer.echo(f"Saved payments charts to {outdir}")
