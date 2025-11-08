from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import typer


@dataclass
class Row:
    db: str
    users: int
    hot_skus: int
    initial_stock: int
    late_fail_prob: float
    duration_s: float
    orders_ok: int
    rolled_back: int
    abort: int
    compensations: int
    stale_reads: int
    oversell_events: int
    orphan_payments: int


def _to_int(v):
    try:
        return int(float(v))
    except Exception:
        return 0


def _to_float(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def _load_rows(path: Path) -> List[Row]:
    rows: List[Row] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for d in r:
            if d.get("scenario") != "rollback":
                continue
            rows.append(
                Row(
                    db=str(d.get("db", "")).strip(),
                    users=_to_int(d.get("users", 0)),
                    hot_skus=_to_int(d.get("hot_skus", 0)),
                    initial_stock=_to_int(d.get("initial_stock", 0)),
                    late_fail_prob=_to_float(d.get("late_fail_prob", 0.0)),
                    duration_s=_to_float(d.get("duration_s", 0.0)),
                    orders_ok=_to_int(d.get("orders_ok", 0)),
                    rolled_back=_to_int(d.get("rolled_back", 0)),
                    abort=_to_int(d.get("abort", 0)),
                    compensations=_to_int(d.get("compensations", 0)),
                    stale_reads=_to_int(d.get("stale_reads", 0)),
                    oversell_events=_to_int(d.get("oversell_events", 0)),
                    orphan_payments=_to_int(d.get("orphan_payments", 0)),
                )
            )
    return rows


def _totals_for_db(rows: List[Row]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for r in rows:
        b = out.setdefault(r.db, {"orders_ok": 0, "rolled_back": 0, "abort": 0, "compensations": 0, "stale_reads": 0, "oversell_events": 0, "orphan_payments": 0, "hot_skus": 0})
        b["orders_ok"] += r.orders_ok
        b["rolled_back"] += r.rolled_back
        b["abort"] += r.abort
        b["compensations"] += r.compensations
        b["stale_reads"] += r.stale_reads
        b["oversell_events"] += r.oversell_events
        b["orphan_payments"] += r.orphan_payments
        b["hot_skus"] += max(1, r.hot_skus)
    return out


def _kpis(totals: Dict[str, Dict[str, int]]):
    oversell_rate: Dict[str, float] = {}
    orphan_payment_rate: Dict[str, float] = {}
    stale_read_rate: Dict[str, float] = {}
    abort_rate: Dict[str, float] = {}
    final_totals: Dict[str, Dict[str, int]] = {}

    for db, t in totals.items():
        if db == "postgres":
            total_db = t["orders_ok"] + t["rolled_back"] + t["abort"]
            abort_rate[db] = (t["abort"] / total_db) if total_db else 0.0
            stale_read_rate[db] = 0.0
        else:
            total_db = t["orders_ok"] + t["compensations"]
            abort_rate[db] = 0.0
            stale_read_rate[db] = (t["stale_reads"] / total_db) if total_db else 0.0

        final_totals[db] = {"total": total_db}
        oversell_rate[db] = (t["oversell_events"] / t["hot_skus"]) if t["hot_skus"] else 0.0
        orphan_payment_rate[db] = (t["orphan_payments"] / total_db) if total_db else 0.0

    return oversell_rate, orphan_payment_rate, stale_read_rate, abort_rate, final_totals


app = typer.Typer(help="Rollback stats")


@app.command()
def rollback(
    input_csv: Path = typer.Option(..., "--input", help="Merged CSV (results/rollback_summary.csv)"),
    out_json: Path = typer.Option(Path("results/tables/rollback_kpis.json"), "--out-json", help="Write KPIs as JSON"),
) -> None:
    rows = _load_rows(input_csv)
    if not rows:
        typer.echo("No rollback rows to analyze.")
        raise typer.Exit(code=1)
    totals = _totals_for_db(rows)
    oversell_rate, orphan_payment_rate, stale_read_rate, abort_rate, final_totals = _kpis(totals)

    # counts: expose raw counters (close to the example)
    counts: Dict[str, int] = {}
    for db, t in totals.items():
        if db == "postgres":
            counts.update({
                "pg_orders_ok": t["orders_ok"],
                "pg_rolled_back": t["rolled_back"],
                "pg_abort": t["abort"],
            })
        elif db == "cassandra":
            counts.update({
                "cass_stale_reads": t["stale_reads"],
                "cass_orders_ok": t["orders_ok"],
                "cass_compensations": t["compensations"],
            })
        elif db == "mongodb":
            counts.update({
                "mongo_stale_reads": t["stale_reads"],
                "mongo_orders_ok": t["orders_ok"],
                "mongo_compensations": t["compensations"],
            })

    result = {
        "oversell_rate": oversell_rate,
        "orphan_payment_rate": orphan_payment_rate,
        "stale_read_rate": stale_read_rate,
        "abort_rate": abort_rate,
        "counts": counts,
        "totals": {db: v["total"] for db, v in final_totals.items()},
    }

    typer.echo("\n=== KPI OUTPUT ===")
    for k in ["oversell_rate", "orphan_payment_rate", "stale_read_rate", "abort_rate", "counts", "totals"]:
        typer.echo(f"{k}: {result[k]}")

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
    typer.echo(f"Wrote KPIs to {out_json}")

