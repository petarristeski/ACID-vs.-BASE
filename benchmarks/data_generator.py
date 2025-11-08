"""
E-commerce data/transaction generator CLI (Typer)

Current scenario: rollback (late failure & compensation) for Postgres, MongoDB, Cassandra.

DB env defaults used by drivers:
  Postgres DSN: PG_DSN (fallback: dbname=shop user=postgres password=postgres host=127.0.0.1 port=5432)
  MongoDB URI:  MONGO_URI=mongodb://root:root@localhost:27017/?authSource=admin
  Cassandra:    CASS_HOSTS=127.0.0.1 (comma-separated)
"""

from enum import Enum
from typing import Annotated

import typer

from benchmarks.rollback import PostgresRollback, CassandraRollback, MongoRollback
from benchmarks.concurrent_orders import ConcurrentOrdersPostgres, ConcurrentOrdersCassandra
from benchmarks.social_media import run_engine as sm_run
from benchmarks.social_media.feed_reads import run_feed as sm_run_feed
from benchmarks.iot.sensor_writes import run_engine as iot_run
from benchmarks.iot.time_series import run_engine as iot_ts_run


class Backend(str, Enum):
    postgres = "postgres"
    mongodb = "mongodb"
    cassandra = "cassandra"


app = typer.Typer(help="Data generation CLI")
ecommerce_app = typer.Typer(help="E-commerce data generation and checks")
social_app = typer.Typer(help="Social media data generation and checks")
iot_app = typer.Typer(help="IoT data generation and checks")


@ecommerce_app.command("rollback")
def rollback(
    db: Backend = typer.Option(..., "--db", help="Target database backend"),
    users: Annotated[int, typer.Option(help="Concurrent users (threads)")] = 100,
    duration_sec: Annotated[int, typer.Option(help="Duration of run in seconds")]=30,
    hot_skus: Annotated[int, typer.Option(help="Number of hot SKUs")]=50,
    initial_stock: Annotated[int, typer.Option(help="Initial stock per SKU")]=50,
    late_fail_prob: Annotated[float, typer.Option(help="Probability of late failure forcing rollback")]=0.2,
) -> None:
    """Rollback (late failure) scenario. Focus: reliability under contention and rollbacks."""
    skus = [f"SKU-{i:03d}" for i in range(hot_skus)]
    if db == Backend.postgres:
        gen = PostgresRollback(users, duration_sec, late_fail_prob, skus, initial_stock=initial_stock)
    elif db == Backend.mongodb:
        gen = MongoRollback(users, duration_sec, late_fail_prob, skus, initial_stock=initial_stock)
    elif db == Backend.cassandra:
        gen = CassandraRollback(users, duration_sec, late_fail_prob, skus, initial_stock=initial_stock)
    else:
        raise typer.BadParameter(f"Unsupported db: {db}")
    gen.setup()
    counts = gen.run()
    oversell, orphan = gen.kpis()
    typer.echo({"counts": counts, "oversell_events": oversell, "orphan_payments": orphan})


app.add_typer(ecommerce_app, name="ecommerce")
app.add_typer(social_app, name="social_media")
app.add_typer(iot_app, name="iot")


@ecommerce_app.command("concurrent_orders")
def concurrent_orders(
    db: Backend = typer.Option(..., "--db", help="Target database backend"),
    users: Annotated[int, typer.Option(help="Concurrent users (threads)")] = 200,
    duration_sec: Annotated[int, typer.Option(help="Duration of run in seconds")]=20,
    initial_stock: Annotated[int, typer.Option(help="Initial stock for the single hot SKU")]=1000,
    retry_max: Annotated[int, typer.Option(help="Postgres: max retries on serialization failure")]=5,
) -> None:
    """Concurrent Orders (single hot SKU) scenario.

    Focus: contention on a single item; Postgres uses SERIALIZABLE with retries; Cassandra uses naive read→write (no LWT).
    """
    if db == Backend.postgres:
        gen = ConcurrentOrdersPostgres(users, duration_sec, initial_stock, retry_max=retry_max)
    elif db == Backend.mongodb:
        from benchmarks.concurrent_orders import ConcurrentOrdersMongo
        gen = ConcurrentOrdersMongo(users, duration_sec, initial_stock)
    elif db == Backend.cassandra:
        gen = ConcurrentOrdersCassandra(users, duration_sec, initial_stock)
    else:
        raise typer.BadParameter("concurrent_orders supports postgres, mongodb and cassandra")
    gen.setup()
    res = gen.run()
    typer.echo(res)


@social_app.command("concurrent_writes")
def social_concurrent_writes(
    db: Backend = typer.Option(..., "--db", help="Target database backend"),
    concurrency: Annotated[int, typer.Option(help="Concurrent workers")] = 64,
    duration_sec: Annotated[int, typer.Option(help="Duration of run in seconds")] = 20,
) -> None:
    """Social media concurrent writes/reads workload (posts, likes, comments)."""
    summary = sm_run(db.value, concurrency, duration_sec)
    typer.echo(summary)


@social_app.command("feed_reads")
def social_feed_reads(
    db: Backend = typer.Option(..., "--db", help="Target database backend"),
    concurrency: Annotated[int, typer.Option(help="Concurrent feed readers")] = 64,
    duration_sec: Annotated[int, typer.Option(help="Duration of run in seconds")] = 20,
    page_size: Annotated[int, typer.Option(help="Feed page size (recent posts)")] = 50,
) -> None:
    """Social media feed read benchmark (global recent posts)."""
    summary = sm_run_feed(db.value, concurrency, duration_sec, page_size)
    typer.echo(summary)


app.add_typer(ecommerce_app, name="ecommerce")
@iot_app.command("sensor_writes")
def iot_sensor_writes(
    db: Backend = typer.Option(..., "--db", help="Target database backend"),
    concurrency: int = typer.Option(64, help="Concurrent writers"),
    duration_sec: int = typer.Option(20, help="Duration seconds"),
    devices: int = typer.Option(100_000, help="Distinct devices"),
    batch_size: int = typer.Option(1, help="Per-thread insert batch size"),
) -> None:
    """IoT sensor write-focused run (single engine), prints summary."""
    summary = iot_run(db.value, concurrency, duration_sec, devices, batch_size)
    typer.echo(summary)


@iot_app.command("time_series")
def iot_time_series(
    db: Backend = typer.Option(..., "--db", help="Target database backend"),
    concurrency: int = typer.Option(64, help="Concurrent readers"),
    duration_sec: int = typer.Option(20, help="Duration seconds"),
    devices: int = typer.Option(10_000, help="Distinct devices to seed/query"),
    points_per_device: int = typer.Option(50, help="Seed points per device (current day)"),
    window_seconds: int = typer.Option(30, help="Range window for queries (seconds)"),
) -> None:
    """IoT time-series range queries (seed + read)."""
    summary = iot_ts_run(db.value, concurrency, duration_sec, devices, points_per_device, window_seconds)
    typer.echo(summary)


if __name__ == "__main__":
    app()
