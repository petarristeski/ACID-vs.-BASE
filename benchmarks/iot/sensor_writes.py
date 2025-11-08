from __future__ import annotations

"""
IoT sensor writes benchmark runners for Postgres, MongoDB, Cassandra.

Exposes a single entrypoint `run_engine(backend, concurrency, duration_sec, devices, batch_size)`
that prepares the schema and runs a write-only ingest for the given duration,
returning a summary dict compatible with metrics collection in this project.
"""

import os
import random
import threading
import time
import statistics
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
from datetime import datetime, timezone


# Env defaults
PG_DSN = os.getenv("PG_DSN", "dbname=iot user=postgres host=127.0.0.1")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
CASS_HOSTS = os.getenv("CASS_HOSTS", "127.0.0.1").split(",")


def _p50(xs: List[float]) -> float:
    return statistics.median(xs) if xs else 0.0


def _p95(xs: List[float]) -> float:
    if not xs:
        return 0.0
    xs_sorted = sorted(xs)
    k = int(round(0.95 * (len(xs_sorted) - 1)))
    return xs_sorted[k]


@dataclass
class Metrics:
    name: str
    lat_write_ms: List[float]
    ok_points: int = 0
    errors: int = 0
    batches: int = 0

    def to_summary(self, duration: float, batch_size: int) -> Dict[str, Any]:
        return {
            "engine": self.name,
            "duration_s": round(duration, 2),
            "throughput_points_per_s": round(self.ok_points / max(1e-9, duration), 1),
            "error_rate": round(self.errors / max(1, (self.batches)), 4),
            "batch_size": batch_size,
            "latency_ms": {
                "p50": round(_p50(self.lat_write_ms), 2),
                "p95": round(_p95(self.lat_write_ms), 2),
            },
            "counts": {
                "ok_points": self.ok_points,
                "batches": self.batches,
                "errors": self.errors,
            },
        }


# -------------- Generators --------------
def _make_row_tuple(devices: int) -> Tuple[int, int, float, float, float]:
    device_id = random.randint(1, devices)
    ts_ms = int(time.time() * 1000)
    temp = 15.0 + random.random() * 20.0
    hum = 30.0 + random.random() * 50.0
    volt = 3.0 + random.random() * 0.7
    return (device_id, ts_ms, temp, hum, volt)


# --------------- Postgres ---------------
def _pg_reset_and_prepare():
    import psycopg2

    # Use autocommit and avoid toggling inside a transaction to prevent set_session errors
    with psycopg2.connect(PG_DSN) as c:
        c.autocommit = True
        cur = c.cursor()
        cur.execute(
            """
            DO $$
            BEGIN
              IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='sensor') THEN
                DROP TABLE sensor;
              END IF;
            END$$;
            """
        )
        cur.execute(
            """
            CREATE TABLE sensor (
              device_id BIGINT NOT NULL,
              ts BIGINT NOT NULL,
              temp_c DOUBLE PRECISION,
              humidity DOUBLE PRECISION,
              voltage DOUBLE PRECISION,
              PRIMARY KEY (device_id, ts)
            );
            CREATE INDEX ON sensor(ts);
            """
        )


def _pg_conn():
    import psycopg2

    return psycopg2.connect(PG_DSN)


def _pg_insert_batch(cur, rows: List[Tuple[int, int, float, float, float]]):
    from psycopg2.extras import execute_values

    execute_values(
        cur,
        "INSERT INTO sensor(device_id, ts, temp_c, humidity, voltage) VALUES %s ON CONFLICT DO NOTHING",
        rows,
    )


def _run_pg(concurrency: int, duration_sec: int, devices: int, batch_size: int, metrics: Metrics) -> float:
    import psycopg2

    _pg_reset_and_prepare()
    stop_at = time.time() + duration_sec

    def worker():
        conn = _pg_conn()
        cur = conn.cursor()
        try:
            while time.time() < stop_at:
                rows = [_make_row_tuple(devices) for _ in range(batch_size)]
                t0 = time.perf_counter()
                try:
                    cur.execute("BEGIN")
                    _pg_insert_batch(cur, rows)
                    conn.commit()
                    dt = (time.perf_counter() - t0) * 1000
                    metrics.lat_write_ms.append(dt)
                    metrics.ok_points += len(rows)
                    metrics.batches += 1
                except psycopg2.Error:
                    conn.rollback()
                    metrics.errors += 1
                    metrics.batches += 1
        finally:
            conn.close()

    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.time() - t0


# ---------------- MongoDB ---------------
def _mongo_reset_and_prepare():
    from pymongo import MongoClient

    client = MongoClient(MONGO_URI, uuidRepresentation="standard")
    client.drop_database("iot")
    db = client["iot"]
    try:
        db.create_collection(
            "sensor",
            timeseries={"timeField": "ts", "metaField": "device_id", "granularity": "minutes"},
        )
    except Exception:
        db.create_collection("sensor")
    db["sensor"].create_index("ts")
    return client


def _mongo_insert_batch(col, rows: List[Dict[str, Any]]):
    col.insert_many(rows, ordered=False)


def _run_mongo(concurrency: int, duration_sec: int, devices: int, batch_size: int, metrics: Metrics) -> float:
    client = _mongo_reset_and_prepare()
    col = client["iot"]["sensor"]
    stop_at = time.time() + duration_sec

    def worker():
        while time.time() < stop_at:
            rows = []
            for _ in range(batch_size):
                device_id, ts_ms, t, h, v = _make_row_tuple(devices)
                # Mongo time-series requires a Date type in timeField
                ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                rows.append({"device_id": device_id, "ts": ts_dt, "temp_c": t, "humidity": h, "voltage": v})
            t0 = time.perf_counter()
            try:
                _mongo_insert_batch(col, rows)
                dt = (time.perf_counter() - t0) * 1000
                metrics.lat_write_ms.append(dt)
                metrics.ok_points += len(rows)
                metrics.batches += 1
            except Exception:
                metrics.errors += 1
                metrics.batches += 1

    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    client.close()
    return time.time() - t0


# --------------- Cassandra --------------
def _cass_reset_and_prepare():
    from cassandra.cluster import Cluster

    cl = Cluster(CASS_HOSTS)
    s = cl.connect()
    try:
        s.execute("DROP KEYSPACE IF EXISTS iot")
        s.execute("CREATE KEYSPACE iot WITH REPLICATION={'class':'SimpleStrategy','replication_factor':1}")
        s.set_keyspace("iot")
        s.execute(
            """
            CREATE TABLE sensor_by_device_day (
              device_id bigint,
              day int,
              ts bigint,
              temp_c double,
              humidity double,
              voltage double,
              PRIMARY KEY ((device_id, day), ts)
            ) WITH CLUSTERING ORDER BY (ts ASC)
            """
        )
    finally:
        cl.shutdown()


def _cass_session():
    from cassandra.cluster import Cluster

    cl = Cluster(CASS_HOSTS)
    s = cl.connect("iot")
    return cl, s


def _yyyymmdd(ts_ms: int) -> int:
    d = time.gmtime(ts_ms // 1000)
    return d.tm_year * 10000 + d.tm_mon * 100 + d.tm_mday


def _run_cassandra(concurrency: int, duration_sec: int, devices: int, batch_size: int, metrics: Metrics) -> float:
    _cass_reset_and_prepare()
    stop_at = time.time() + duration_sec

    def worker():
        cl, s = _cass_session()
        try:
            while time.time() < stop_at:
                rows = [_make_row_tuple(devices) for _ in range(batch_size)]
                t0 = time.perf_counter()
                try:
                    for (device_id, ts_ms, temp, hum, volt) in rows:
                        day = _yyyymmdd(ts_ms)
                        s.execute(
                            "INSERT INTO sensor_by_device_day(device_id, day, ts, temp_c, humidity, voltage) VALUES (%s,%s,%s,%s,%s,%s)",
                            (device_id, day, ts_ms, temp, hum, volt),
                        )
                    dt = (time.perf_counter() - t0) * 1000
                    metrics.lat_write_ms.append(dt)
                    metrics.ok_points += len(rows)
                    metrics.batches += 1
                except Exception:
                    metrics.errors += 1
                    metrics.batches += 1
        finally:
            cl.shutdown()

    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.time() - t0


def run_engine(backend: str, concurrency: int, duration_sec: int, devices: int, batch_size: int) -> Dict[str, Any]:
    """Run a single engine's write-only ingest and return a summary dict."""
    metrics = Metrics(name=backend, lat_write_ms=[])
    random.seed(7)  # deterministic per run
    if backend == "postgres":
        dur = _run_pg(concurrency, duration_sec, devices, batch_size, metrics)
    elif backend == "mongodb":
        dur = _run_mongo(concurrency, duration_sec, devices, batch_size, metrics)
    elif backend == "cassandra":
        dur = _run_cassandra(concurrency, duration_sec, devices, batch_size, metrics)
    else:
        raise ValueError(f"Unsupported backend for iot.sensor_writes: {backend}")
    summary = metrics.to_summary(dur, batch_size)
    # Unify keys with other scenarios naming style
    summary["engine"] = backend
    summary["duration_s"] = round(summary.get("duration_s", dur), 2)
    return summary
