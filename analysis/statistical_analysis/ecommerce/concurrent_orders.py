from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import typer


@dataclass
class Row:
    db: str
    scenario: str
    users: int
    duration_s: int
    initial_stock: int
    # PG fields (may be absent if db != postgres)
    pg_paid_orders: int = 0
    pg_qty_on_hand_end: int = 0
    pg_abort_rate: float = 0.0
    pg_retries_total: int = 0
    pg_throughput_succ_per_s: float = 0.0
    pg_latency_p50_ms: float = 0.0
    pg_latency_p95_ms: float = 0.0
    pg_oos_attempts: int = 0
    pg_gave_up: int = 0
    # Cassandra fields (may be absent if db != cassandra)
    cass_paid_orders: int = 0
    cass_available_end: int = 0
    cass_oversell_event: bool = False
    cass_throughput_succ_per_s: float = 0.0
    cass_latency_p50_ms: float = 0.0
    cass_latency_p95_ms: float = 0.0
    cass_oos_attempts: int = 0
    cass_fail: int = 0
    # MongoDB fields
    mongo_paid_orders: int = 0
    mongo_available_end: int = 0
    mongo_oversell_event: bool = False
    mongo_throughput_succ_per_s: float = 0.0
    mongo_latency_p50_ms: float = 0.0
    mongo_latency_p95_ms: float = 0.0
    mongo_oos_attempts: int = 0
    mongo_fail: int = 0


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
            if d.get("scenario") != "concurrent_orders_single_hot_sku":
                continue
            rows.append(
                Row(
                    db=str(d.get("db", "")).strip(),
                    scenario=d.get("scenario", ""),
                    users=_to_int(d.get("users", 0)),
                    duration_s=_to_int(d.get("duration_s", 0)),
                    initial_stock=_to_int(d.get("initial_stock", 0)),
                    pg_paid_orders=_to_int(d.get("pg_paid_orders", 0)),
                    pg_qty_on_hand_end=_to_int(d.get("pg_qty_on_hand_end", 0)),
                    pg_abort_rate=_to_float(d.get("pg_abort_rate", 0.0)),
                    pg_retries_total=_to_int(d.get("pg_retries_total", 0)),
                    pg_throughput_succ_per_s=_to_float(d.get("pg_throughput_succ_per_s", 0.0)),
                    pg_latency_p50_ms=_to_float(d.get("pg_latency_p50_ms", 0.0)),
                    pg_latency_p95_ms=_to_float(d.get("pg_latency_p95_ms", 0.0)),
                    pg_oos_attempts=_to_int(d.get("pg_oos_attempts", 0)),
                    pg_gave_up=_to_int(d.get("pg_gave_up", 0)),
                    cass_paid_orders=_to_int(d.get("cass_paid_orders", 0)),
                    cass_available_end=_to_int(d.get("cass_available_end", 0)),
                    cass_oversell_event=str(d.get("cass_oversell_event", "False")).lower() in {"1","true","t","yes"},
                    cass_throughput_succ_per_s=_to_float(d.get("cass_throughput_succ_per_s", 0.0)),
                    cass_latency_p50_ms=_to_float(d.get("cass_latency_p50_ms", 0.0)),
                    cass_latency_p95_ms=_to_float(d.get("cass_latency_p95_ms", 0.0)),
                    cass_oos_attempts=_to_int(d.get("cass_oos_attempts", 0)),
                    cass_fail=_to_int(d.get("cass_fail", 0)),
                    mongo_paid_orders=_to_int(d.get("mongo_paid_orders", 0)),
                    mongo_available_end=_to_int(d.get("mongo_available_end", 0)),
                    mongo_oversell_event=str(d.get("mongo_oversell_event", "False")).lower() in {"1","true","t","yes"},
                    mongo_throughput_succ_per_s=_to_float(d.get("mongo_throughput_succ_per_s", 0.0)),
                    mongo_latency_p50_ms=_to_float(d.get("mongo_latency_p50_ms", 0.0)),
                    mongo_latency_p95_ms=_to_float(d.get("mongo_latency_p95_ms", 0.0)),
                    mongo_oos_attempts=_to_int(d.get("mongo_oos_attempts", 0)),
                    mongo_fail=_to_int(d.get("mongo_fail", 0)),
                )
            )
    return rows


app = typer.Typer(help="Concurrent Orders (single hot SKU) KPIs")


@app.command()
def concurrent_orders(
    input_csv: Path = typer.Option(..., "--input", help="Merged CSV (results/concurrent_orders_summary.csv)"),
    out_json: Path = typer.Option(Path("results/tables/concurrent_orders_kpis.json"), "--out-json", help="Write KPIs as JSON"),
) -> None:
    rows = _load_rows(input_csv)
    if not rows:
        typer.echo("No concurrent_orders rows to analyze.")
        raise typer.Exit(code=1)

    # Choose common parameters from first row
    scenario = rows[0].scenario
    initial_stock = rows[0].initial_stock
    duration_s = rows[0].duration_s
    users = rows[0].users

    # Aggregate per DB
    agg: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    lat_p50s: Dict[str, List[float]] = defaultdict(list)
    lat_p95s: Dict[str, List[float]] = defaultdict(list)

    for r in rows:
        if r.db == "postgres":
            agg["postgres"]["paid_orders"] += r.pg_paid_orders
            agg["postgres"]["qty_on_hand_end"] = r.pg_qty_on_hand_end  # last
            agg["postgres"]["abort_rate_sum"] += r.pg_abort_rate
            agg["postgres"]["runs"] += 1
            agg["postgres"]["retries_total"] += r.pg_retries_total
            agg["postgres"]["throughput_succ_per_s_sum"] += r.pg_throughput_succ_per_s
            agg["postgres"]["oos_attempts"] += r.pg_oos_attempts
            agg["postgres"]["gave_up"] += r.pg_gave_up
            lat_p50s["postgres"].append(r.pg_latency_p50_ms)
            lat_p95s["postgres"].append(r.pg_latency_p95_ms)
        elif r.db == "cassandra":
            agg["cassandra"]["paid_orders"] += r.cass_paid_orders
            agg["cassandra"]["available_end"] = r.cass_available_end
            agg["cassandra"]["oversell_event_any"] = 1.0 if (agg["cassandra"].get("oversell_event_any", 0.0) or r.cass_oversell_event) else 0.0
            agg["cassandra"]["throughput_succ_per_s_sum"] += r.cass_throughput_succ_per_s
            agg["cassandra"]["runs"] += 1
            agg["cassandra"]["oos_attempts"] += r.cass_oos_attempts
            agg["cassandra"]["fail"] += r.cass_fail
            lat_p50s["cassandra"].append(r.cass_latency_p50_ms)
            lat_p95s["cassandra"].append(r.cass_latency_p95_ms)
        elif r.db == "mongodb":
            agg["mongodb"]["paid_orders"] += r.mongo_paid_orders
            agg["mongodb"]["available_end"] = r.mongo_available_end
            agg["mongodb"]["oversell_event_any"] = 1.0 if (agg["mongodb"].get("oversell_event_any", 0.0) or r.mongo_oversell_event) else 0.0
            agg["mongodb"]["throughput_succ_per_s_sum"] += r.mongo_throughput_succ_per_s
            agg["mongodb"]["runs"] += 1
            agg["mongodb"]["oos_attempts"] += r.mongo_oos_attempts
            agg["mongodb"]["fail"] += r.mongo_fail
            lat_p50s["mongodb"].append(r.mongo_latency_p50_ms)
            lat_p95s["mongodb"].append(r.mongo_latency_p95_ms)

    # Build output similar to example
    def med(xs: List[float]) -> float:
        return statistics.median(xs) if xs else 0.0

    pg_runs = int(agg["postgres"].get("runs", 0))
    mongo_runs = int(agg["mongodb"].get("runs", 0))
    cass_runs = int(agg["cassandra"].get("runs", 0))

    out = {
        "scenario": scenario,
        "initial_stock": initial_stock,
        "duration_s": duration_s,
        "users": users,
        "postgres": {
            "paid_orders": int(agg["postgres"].get("paid_orders", 0)),
            "qty_on_hand_end": int(agg["postgres"].get("qty_on_hand_end", 0)),
            "abort_rate": (agg["postgres"].get("abort_rate_sum", 0.0) / pg_runs) if pg_runs else 0.0,
            "retries_total": int(agg["postgres"].get("retries_total", 0)),
            "throughput_succ_per_s": (agg["postgres"].get("throughput_succ_per_s_sum", 0.0) / pg_runs) if pg_runs else 0.0,
            "latency_ms": {"p50": med(lat_p50s["postgres"]), "p95": med(lat_p95s["postgres"])},
            "oos_attempts": int(agg["postgres"].get("oos_attempts", 0)),
            "gave_up": int(agg["postgres"].get("gave_up", 0)),
        },
        "cassandra": {
            "paid_orders": int(agg["cassandra"].get("paid_orders", 0)),
            "available_end": int(agg["cassandra"].get("available_end", 0)),
            "oversell_event": bool(agg["cassandra"].get("oversell_event_any", 0.0)),
            "throughput_succ_per_s": (agg["cassandra"].get("throughput_succ_per_s_sum", 0.0) / cass_runs) if cass_runs else 0.0,
            "latency_ms": {"p50": med(lat_p50s["cassandra"]), "p95": med(lat_p95s["cassandra"])},
            "oos_attempts": int(agg["cassandra"].get("oos_attempts", 0)),
            "fail": int(agg["cassandra"].get("fail", 0)),
        },
        "mongodb": {
            "paid_orders": int(agg["mongodb"].get("paid_orders", 0)),
            "available_end": int(agg["mongodb"].get("available_end", 0)),
            "oversell_event": bool(agg["mongodb"].get("oversell_event_any", 0.0)),
            "throughput_succ_per_s": (agg["mongodb"].get("throughput_succ_per_s_sum", 0.0) / mongo_runs) if mongo_runs else 0.0,
            "latency_ms": {"p50": med(lat_p50s["mongodb"]), "p95": med(lat_p95s["mongodb"])},
            "oos_attempts": int(agg["mongodb"].get("oos_attempts", 0)),
            "fail": int(agg["mongodb"].get("fail", 0)),
        },
    }

    typer.echo("\n=== CONCURRENT ORDERS KPI OUTPUT ===")
    for k in ["scenario", "initial_stock", "duration_s", "users", "postgres", "mongodb", "cassandra"]:
        typer.echo(f"{k}: {out[k]}")

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(out, indent=2), encoding="utf-8")
    typer.echo(f"Wrote KPIs to {out_json}")
