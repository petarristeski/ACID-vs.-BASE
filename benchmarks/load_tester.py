"""Load testing driver for running workloads (Typer CLI).

Implements the E-commerce rollback scenario across databases and
persists structured results via metrics_collector.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import List, Optional, Dict

import typer

from benchmarks.rollback import PostgresRollback, CassandraRollback, MongoRollback
from benchmarks.concurrent_orders import ConcurrentOrdersPostgres, ConcurrentOrdersCassandra, ConcurrentOrdersMongo
from benchmarks.metrics_collector import write_csv, write_jsonl
from benchmarks.social_media.feed_reads import run_feed as sm_run_feed
from benchmarks.iot.sensor_writes import run_engine as iot_run
from benchmarks.iot.time_series import run_engine as iot_ts_run


class DBChoice(str, Enum):
    postgres = "postgres"
    mongodb = "mongodb"
    cassandra = "cassandra"
    all = "all"


app = typer.Typer(help="Load tester orchestrating benchmark runs and persisting metrics")
social_app = typer.Typer(help="Social media load tests")
iot_app = typer.Typer(help="IoT load tests")
app.add_typer(social_app, name="social_media")
app.add_typer(iot_app, name="iot")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _make_gen(backend: str, users: int, duration_sec: int, hot_skus: int, initial_stock: int, late_fail_prob: float):
    skus = [f"SKU-{i:03d}" for i in range(hot_skus)]
    if backend == "postgres":
        return PostgresRollback(users, duration_sec, late_fail_prob, skus, initial_stock=initial_stock)
    if backend == "mongodb":
        return MongoRollback(users, duration_sec, late_fail_prob, skus, initial_stock=initial_stock)
    if backend == "cassandra":
        return CassandraRollback(users, duration_sec, late_fail_prob, skus, initial_stock=initial_stock)
    raise ValueError(f"Unsupported backend: {backend}")


def _rollback_once(backend: str, users: int, duration_sec: int, hot_skus: int, initial_stock: int, late_fail_prob: float):
    gen = _make_gen(backend, users, duration_sec, hot_skus, initial_stock, late_fail_prob)
    started_at = _iso_now()
    gen.setup()
    counts = gen.run()
    ended_at = _iso_now()
    oversell, orphan = gen.kpis()
    # Build generic row
    total_ok = counts.get("orders_ok", 0)
    row = {
        "run_id": f"{ended_at}_{backend}",
        "scenario": "rollback",
        "db": backend,
        "users": users,
        "hot_skus": hot_skus,
        "initial_stock": initial_stock,
        "late_fail_prob": late_fail_prob,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_s": duration_sec,
        "orders_ok": total_ok,
        "rolled_back": counts.get("rolled_back", 0),
        "abort": counts.get("abort", 0),
        "compensations": counts.get("compensations", 0),
        "stale_reads": counts.get("stale_reads", 0),
        "oversell_events": oversell,
        "orphan_payments": orphan,
    }
    return row


def _co_once(backend: str, users: int, duration_sec: int, initial_stock: int, retry_max: int) -> Dict[str, object]:
    if backend == "postgres":
        gen = ConcurrentOrdersPostgres(users, duration_sec, initial_stock, retry_max=retry_max)
    elif backend == "mongodb":
        gen = ConcurrentOrdersMongo(users, duration_sec, initial_stock)
    elif backend == "cassandra":
        gen = ConcurrentOrdersCassandra(users, duration_sec, initial_stock)
    else:
        raise ValueError(f"Unsupported backend for concurrent_orders: {backend}")
    gen.setup()
    return gen.run()


@app.command()
def rollback(
    db: DBChoice = typer.Option(DBChoice.all, "--db", help="Database to test or 'all'"),
    users: int = typer.Option(100, help="Concurrent users (threads)"),
    duration_sec: int = typer.Option(30, help="Duration of run in seconds"),
    hot_skus: int = typer.Option(50, help="Number of hot SKUs"),
    initial_stock: int = typer.Option(50, help="Initial stock per SKU"),
    late_fail_prob: float = typer.Option(0.2, help="Probability of late failure forcing rollback"),
    repeats: int = typer.Option(1, help="How many times to repeat per DB"),
    out: Path = typer.Option(Path("results/raw_data/rollback"), help="Output directory for results"),
) -> None:
    """Run the rollback (late-failure) scenario and write JSONL/CSV under results/raw_data/rollback/<db>/"""
    backends: List[str] = (["postgres", "mongodb", "cassandra"] if db == DBChoice.all else [db.value])
    for backend in backends:
        db_dir = out / backend
        db_dir.mkdir(parents=True, exist_ok=True)
        for i in range(repeats):
            row = _rollback_once(backend, users, duration_sec, hot_skus, initial_stock, late_fail_prob)
            ts = row["ended_at"].replace(":", "-")
            jsonl_path = db_dir / f"run_{ts}.jsonl"
            csv_path = db_dir / f"run_{ts}.csv"
            write_jsonl(jsonl_path, [row])
            write_csv(csv_path, [row])
            typer.echo(f"Saved results for {backend} repeat {i+1}/{repeats} -> {jsonl_path.name}, {csv_path.name}")


@app.command()
def concurrent_orders(
    db: DBChoice = typer.Option(DBChoice.all, "--db", help="Database to test or 'all'"),
    users: int = typer.Option(200, help="Concurrent users (threads)"),
    duration_sec: int = typer.Option(20, help="Duration of run in seconds"),
    initial_stock: int = typer.Option(1000, help="Initial stock for the single hot SKU"),
    retry_max: int = typer.Option(5, help="Postgres: max retries on serialization failure"),
    repeats: int = typer.Option(1, help="How many times to repeat per DB"),
    out: Path = typer.Option(Path("results/raw_data/concurrent_orders"), help="Output directory for results"),
) -> None:
    """Run the Concurrent Orders (single hot SKU) scenario and write JSONL/CSV under results/raw_data/concurrent_orders/<db>/"""
    backends: List[str] = (["postgres", "mongodb", "cassandra"] if db == DBChoice.all else [db.value])
    for backend in backends:
        db_dir = out / backend
        db_dir.mkdir(parents=True, exist_ok=True)
        for i in range(repeats):
            started_at = _iso_now()
            res = _co_once(backend, users, duration_sec, initial_stock, retry_max)
            ended_at = _iso_now()
            row: Dict[str, object] = {
                "run_id": f"{ended_at}_{backend}",
                "scenario": "concurrent_orders_single_hot_sku",
                "db": backend,
                "users": users,
                "initial_stock": initial_stock,
                "duration_s": duration_sec,
                "started_at": started_at,
                "ended_at": ended_at,
            }
            row.update(res)
            ts = ended_at.replace(":", "-")
            jsonl_path = db_dir / f"run_{ts}.jsonl"
            csv_path = db_dir / f"run_{ts}.csv"
            write_jsonl(jsonl_path, [row])
            write_csv(csv_path, [row])
            typer.echo(f"Saved results for {backend} repeat {i+1}/{repeats} -> {jsonl_path.name}, {csv_path.name}")


    

@social_app.command("concurrent_writes")
def sm_concurrent_writes(
    db: DBChoice = typer.Option(DBChoice.all, "--db", help="Database to test or 'all'"),
    concurrency: int = typer.Option(64, help="Concurrent workers"),
    duration_sec: int = typer.Option(20, help="Duration seconds"),
    repeats: int = typer.Option(1, help="How many times to repeat per DB"),
    out: Path = typer.Option(Path("results/raw_data/social_media/concurrent_writes"), help="Output directory"),
) -> None:
    from benchmarks.social_media import run_engine as sm_run
    backends: List[str] = (["postgres", "mongodb", "cassandra"] if db == DBChoice.all else [db.value])
    out = out.resolve()
    for backend in backends:
        db_dir = out / backend
        db_dir.mkdir(parents=True, exist_ok=True)
        for _ in range(repeats):
            started_at = _iso_now()
            summary = sm_run(backend, concurrency, duration_sec)
            ended_at = _iso_now()
            row = {
                "run_id": f"{ended_at}_{backend}",
                "scenario": "social_media_concurrent_writes",
                "db": backend,
                "concurrency": concurrency,
                "duration_s": summary.get("duration_s", duration_sec),
                "started_at": started_at,
                "ended_at": ended_at,
            }
            row.update(summary)
            ts = ended_at.replace(":", "-")
            write_jsonl(db_dir / f"run_{ts}.jsonl", [row])
            write_csv(db_dir / f"run_{ts}.csv", [row])
            typer.echo(f"Saved results for {backend} -> run_{ts}")


@social_app.command("feed_reads")
def sm_feed_reads(
    db: DBChoice = typer.Option(DBChoice.all, "--db", help="Database to test or 'all'"),
    concurrency: int = typer.Option(64, help="Concurrent readers"),
    duration_sec: int = typer.Option(20, help="Duration seconds"),
    page_size: int = typer.Option(50, help="Feed page size"),
    repeats: int = typer.Option(1, help="How many times to repeat per DB"),
    out: Path = typer.Option(Path("results/raw_data/social_media/feed_reads"), help="Output directory"),
) -> None:
    backends: List[str] = (["postgres", "mongodb", "cassandra"] if db == DBChoice.all else [db.value])
    out = out.resolve()
    for backend in backends:
        db_dir = out / backend
        db_dir.mkdir(parents=True, exist_ok=True)
        for _ in range(repeats):
            started_at = _iso_now()
            summary = sm_run_feed(backend, concurrency, duration_sec, page_size)
            ended_at = _iso_now()
            row = {
                "run_id": f"{ended_at}_{backend}",
                "scenario": "social_media_feed_reads",
                "db": backend,
                "concurrency": concurrency,
                "duration_s": summary.get("duration_s", duration_sec),
                "page_size": page_size,
                "started_at": started_at,
                "ended_at": ended_at,
            }
            row.update(summary)
            ts = ended_at.replace(":", "-")
            write_jsonl(db_dir / f"run_{ts}.jsonl", [row])
            write_csv(db_dir / f"run_{ts}.csv", [row])
            typer.echo(f"Saved results for {backend} -> run_{ts}")


@iot_app.command("sensor_writes")
def iot_sensor_writes(
    db: DBChoice = typer.Option(DBChoice.all, "--db", help="Database to test or 'all'"),
    concurrency: int = typer.Option(64, help="Concurrent writers"),
    duration_sec: int = typer.Option(20, help="Duration seconds"),
    devices: int = typer.Option(100_000, help="Distinct devices"),
    batch_size: int = typer.Option(1, help="Insert batch size per thread"),
    repeats: int = typer.Option(1, help="How many times to repeat per DB"),
    out: Path = typer.Option(Path("results/raw_data/iot/sensor_writes"), help="Output directory"),
) -> None:
    """Run IoT sensor write workload and persist results under results/raw_data/iot/sensor_writes/<db>/"""
    backends: List[str] = (["postgres", "mongodb", "cassandra"] if db == DBChoice.all else [db.value])
    out = out.resolve()
    for backend in backends:
        db_dir = out / backend
        db_dir.mkdir(parents=True, exist_ok=True)
        for _ in range(repeats):
            started_at = _iso_now()
            summary = iot_run(backend, concurrency, duration_sec, devices, batch_size)
            ended_at = _iso_now()
            row = {
                "run_id": f"{ended_at}_{backend}",
                "scenario": "iot_sensor_writes",
                "db": backend,
                "concurrency": concurrency,
                "duration_s": summary.get("duration_s", duration_sec),
                "devices": devices,
                "batch_size": batch_size,
                "started_at": started_at,
                "ended_at": ended_at,
            }
            row.update(summary)
            ts = ended_at.replace(":", "-")
            write_jsonl(db_dir / f"run_{ts}.jsonl", [row])
            write_csv(db_dir / f"run_{ts}.csv", [row])
            typer.echo(f"Saved results for {backend} -> run_{ts}")


@iot_app.command("time_series")
def iot_time_series(
    db: DBChoice = typer.Option(DBChoice.all, "--db", help="Database to test or 'all'"),
    concurrency: int = typer.Option(64, help="Concurrent readers"),
    duration_sec: int = typer.Option(20, help="Duration seconds"),
    devices: int = typer.Option(10_000, help="Distinct devices to seed/query"),
    points_per_device: int = typer.Option(50, help="Seed points per device"),
    window_seconds: int = typer.Option(30, help="Query window seconds"),
    repeats: int = typer.Option(1, help="How many times to repeat per DB"),
    out: Path = typer.Option(Path("results/raw_data/iot/time_series"), help="Output directory"),
) -> None:
    """Run IoT time-series range-reads (seed + read) and persist results."""
    backends: List[str] = (["postgres", "mongodb", "cassandra"] if db == DBChoice.all else [db.value])
    out = out.resolve()
    for backend in backends:
        db_dir = out / backend
        db_dir.mkdir(parents=True, exist_ok=True)
        for _ in range(repeats):
            started_at = _iso_now()
            summary = iot_ts_run(backend, concurrency, duration_sec, devices, points_per_device, window_seconds)
            ended_at = _iso_now()
            row = {
                "run_id": f"{ended_at}_{backend}",
                "scenario": "iot_time_series",
                "db": backend,
                "concurrency": concurrency,
                "duration_s": summary.get("duration_s", duration_sec),
                "devices": devices,
                "points_per_device": points_per_device,
                "window_seconds": window_seconds,
                "started_at": started_at,
                "ended_at": ended_at,
            }
            row.update(summary)
            ts = ended_at.replace(":", "-")
            write_jsonl(db_dir / f"run_{ts}.jsonl", [row])
            write_csv(db_dir / f"run_{ts}.csv", [row])
            typer.echo(f"Saved results for {backend} -> run_{ts}")


if __name__ == "__main__":
    app()
