"""Metrics collection utilities and CLI for benchmarks.

Provides RunResult dataclass and helpers to write JSONL/CSV, plus a small
Typer CLI to merge multiple runs into a single CSV for analysis.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Sequence

import typer


@dataclass
class RunResult:
    run_id: str
    scenario: str
    db: str
    sku: str
    customers: int
    initial_stock: int
    orders_per_user: int
    concurrency: int
    failure_rate: float
    started_at: str
    ended_at: str
    duration_s: float
    ok: int
    failed: int
    out_of_stock: int
    total: int
    tps: float
    # Optional/derived fields for richer analysis
    waves: int = 0                 # payments scenario only
    wave_size: int = 0             # alias for concurrency per wave
    attempted: int = 0             # explicit attempted ops (preferred over deriving)
    compensations: int = 0         # number of compensating actions (BASE)
    exception_count: int = 0       # worker exceptions (attempted - total)
    pg_rollback_count: int = 0     # optional: postgres serialization/deadlock/timeout rollbacks
    cas_retries: int = 0           # optional: cassandra LWT retry count

    def to_dict(self) -> dict:
        return asdict(self)


def write_jsonl(path: Path, results: Sequence[object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in results:
            payload = r.to_dict() if hasattr(r, "to_dict") else r
            f.write(json.dumps(payload) + "\n")


def write_csv(path: Path, results: Sequence[object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [r.to_dict() if hasattr(r, "to_dict") else r for r in results]
    fieldnames = list(rows[0].keys()) if rows else []
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            w.writeheader()
        for row in rows:
            w.writerow(row)


app = typer.Typer(help="Metrics utilities")


@app.command()
def merge(
    input_dir: Path = typer.Option(..., "--in", help="Directory with JSONL/CSV run files (recursively)"),
    out_csv: Path = typer.Option(..., "--out", help="Output CSV file path"),
) -> None:
    """Merge JSONL/CSV run files into a single CSV for analysis."""
    input_dir = input_dir.resolve()
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    collected: List[dict] = []
    for p in input_dir.rglob("*.jsonl"):
        for line in p.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            collected.append(json.loads(line))
    for p in input_dir.rglob("*.csv"):
        with p.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                collected.append(row)

    if not collected:
        typer.echo("No input files found.")
        raise typer.Exit(code=1)

    # Normalize numeric fields if read from CSV
    numeric_fields = {
        "customers",
        "initial_stock",
        "orders_per_user",
        "concurrency",
        "ok",
        "failed",
        "out_of_stock",
        "total",
        # extended numeric fields
        "waves",
        "wave_size",
        "attempted",
        "compensations",
        "exception_count",
        "pg_rollback_count",
        "cas_retries",
    }
    float_fields = {"failure_rate", "duration_s", "tps"}
    for row in collected:
        for k in list(row.keys()):
            if k in numeric_fields and isinstance(row[k], str) and row[k].isdigit():
                row[k] = int(row[k])
            if k in float_fields and isinstance(row[k], str):
                try:
                    row[k] = float(row[k])
                except ValueError:
                    pass

    # Determine header order. Start with common fields if present, then include any others
    base_fields = [
        "run_id",
        "scenario",
        "db",
        # steady/payments fields (may be absent for rollback)
        "sku",
        "customers",
        "initial_stock",
        "orders_per_user",
        "concurrency",
        "failure_rate",
        "started_at",
        "ended_at",
        "duration_s",
        "ok",
        "failed",
        "out_of_stock",
        "total",
        "tps",
    ]
    extra_fields = [
        "waves",
        "wave_size",
        "attempted",
        "compensations",
        "exception_count",
        "pg_rollback_count",
        "cas_retries",
    ]
    # Rollback-specific fields
    rollback_fields = [
        "users",
        "hot_skus",
        "late_fail_prob",
        "orders_ok",
        "rolled_back",
        "abort",
        "stale_reads",
        "oversell_events",
        "orphan_payments",
    ]

    # Union of all keys seen in collected rows
    seen_keys: set[str] = set()
    for row in collected:
        seen_keys.update(row.keys())

    ordered: List[str] = []
    def add_existing(keys: List[str]):
        for k in keys:
            if k in seen_keys and k not in ordered:
                ordered.append(k)

    add_existing(base_fields)
    add_existing(extra_fields)
    add_existing(rollback_fields)
    # Add any remaining keys deterministically
    for k in sorted(seen_keys):
        if k not in ordered:
            ordered.append(k)

    fieldnames = ordered

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in collected:
            w.writerow({k: row.get(k, "") for k in fieldnames})


if __name__ == "__main__":
    app()
