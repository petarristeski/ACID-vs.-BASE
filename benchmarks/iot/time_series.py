from __future__ import annotations

"""
IoT time-series query benchmark: evaluate range-read performance per engine.

It prepares/ensures schema, seeds a modest dataset (devices x points_per_device)
for the current day, then runs concurrent range queries for a configured window.
"""

import os
import random
import threading
import time
import statistics
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple


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


# ---------- Helpers shared ----------
def _yyyymmdd(ts_ms: int) -> int:
    d = time.gmtime(ts_ms // 1000)
    return d.tm_year * 10000 + d.tm_mon * 100 + d.tm_mday


# --------------- Postgres ---------------
def _pg_ensure_schema():
    import psycopg2

    with psycopg2.connect(PG_DSN) as c:
        c.autocommit = True
        cur = c.cursor()
        cur.execute(
            """
            DO $$
            BEGIN
              IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='sensor') THEN
                CREATE TABLE sensor (
                  device_id BIGINT NOT NULL,
                  ts BIGINT NOT NULL,
                  temp_c DOUBLE PRECISION,
                  humidity DOUBLE PRECISION,
                  voltage DOUBLE PRECISION,
                  PRIMARY KEY (device_id, ts)
                );
                CREATE INDEX ON sensor(ts);
              END IF;
            END$$;
            """
        )


def _pg_seed(devices: int, points_per_device: int) -> None:
    import psycopg2
    from psycopg2.extras import execute_values

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - points_per_device * 1000
    rows: List[Tuple[int, int, float, float, float]] = []
    batch_size = 10_000
    with psycopg2.connect(PG_DSN) as c:
        cur = c.cursor()
        cur.execute("BEGIN")
        for d in range(1, devices + 1):
            for i in range(points_per_device):
                ts = start_ms + i * 1000
                t = 15.0 + random.random() * 20.0
                h = 30.0 + random.random() * 50.0
                v = 3.0 + random.random() * 0.7
                rows.append((d, ts, t, h, v))
                if len(rows) >= batch_size:
                    execute_values(cur, "INSERT INTO sensor(device_id, ts, temp_c, humidity, voltage) VALUES %s ON CONFLICT DO NOTHING", rows)
                    rows.clear()
        if rows:
            execute_values(cur, "INSERT INTO sensor(device_id, ts, temp_c, humidity, voltage) VALUES %s ON CONFLICT DO NOTHING", rows)
        c.commit()


def _pg_range_query(cur, device_id: int, ts_from: int, ts_to: int) -> int:
    cur.execute(
        "SELECT device_id, ts, temp_c, humidity, voltage FROM sensor WHERE device_id=%s AND ts >= %s AND ts < %s ORDER BY ts DESC",
        (device_id, ts_from, ts_to),
    )
    rows = cur.fetchall()
    return len(rows)


def _run_pg(concurrency: int, duration_sec: int, devices: int, points_per_device: int, window_seconds: int) -> Dict[str, Any]:
    import psycopg2

    _pg_ensure_schema()
    _pg_seed(devices, points_per_device)
    stop_at = time.time() + duration_sec
    lat_ms: List[float] = []
    reads = 0
    points = 0
    errors = 0

    def worker():
        nonlocal reads, points, errors
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()
        try:
            while time.time() < stop_at:
                did = random.randint(1, devices)
                ts_to = int(time.time() * 1000)
                ts_from = ts_to - window_seconds * 1000
                t0 = time.perf_counter()
                try:
                    n = _pg_range_query(cur, did, ts_from, ts_to)
                    dt = (time.perf_counter() - t0) * 1000
                    lat_ms.append(dt)
                    points += n
                    reads += 1
                except Exception:
                    errors += 1
        finally:
            conn.close()

    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    t0 = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    duration = time.time() - t0
    return {
        "engine": "postgres",
        "duration_s": round(duration, 2),
        "throughput_reads_per_s": round(reads / max(1e-9, duration), 1),
        "errors": int(errors),
        "latency_ms": {"ts_read": {"p50": round(_p50(lat_ms), 2), "p95": round(_p95(lat_ms), 2)}},
        "counts": {"reads": int(reads), "points": int(points)},
    }


# --------------- MongoDB ---------------
def _mongo_ensure_schema():
    from pymongo import MongoClient

    client = MongoClient(MONGO_URI, uuidRepresentation="standard")
    db = client["iot"]
    if "sensor" not in db.list_collection_names():
        try:
            db.create_collection(
                "sensor",
                timeseries={"timeField": "ts", "metaField": "device_id", "granularity": "minutes"},
            )
        except Exception:
            db.create_collection("sensor")
        db["sensor"].create_index("ts")
    return client


def _mongo_seed(client, devices: int, points_per_device: int) -> None:
    col = client["iot"]["sensor"]
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - points_per_device * 1000
    batch: List[Dict[str, Any]] = []
    batch_size = 10_000
    for d in range(1, devices + 1):
        for i in range(points_per_device):
            ts_ms = start_ms + i * 1000
            ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            t = 15.0 + random.random() * 20.0
            h = 30.0 + random.random() * 50.0
            v = 3.0 + random.random() * 0.7
            batch.append({"device_id": d, "ts": ts_dt, "temp_c": t, "humidity": h, "voltage": v})
            if len(batch) >= batch_size:
                col.insert_many(batch, ordered=False)
                batch.clear()
    if batch:
        col.insert_many(batch, ordered=False)


def _run_mongo(concurrency: int, duration_sec: int, devices: int, points_per_device: int, window_seconds: int) -> Dict[str, Any]:
    client = _mongo_ensure_schema()
    _mongo_seed(client, devices, points_per_device)
    col = client["iot"]["sensor"]
    stop_at = time.time() + duration_sec
    lat_ms: List[float] = []
    reads = 0
    points = 0
    errors = 0

    def worker():
        nonlocal reads, points, errors
        while time.time() < stop_at:
            did = random.randint(1, devices)
            ts_to = datetime.now(tz=timezone.utc)
            ts_from = ts_to - timedelta(seconds=window_seconds)
            t0 = time.perf_counter()
            try:
                docs = list(col.find({"device_id": did, "ts": {"$gte": ts_from, "$lt": ts_to}}))
                dt = (time.perf_counter() - t0) * 1000
                lat_ms.append(dt)
                points += len(docs)
                reads += 1
            except Exception:
                errors += 1

    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    t0 = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    client.close()
    duration = time.time() - t0
    return {
        "engine": "mongodb",
        "duration_s": round(duration, 2),
        "throughput_reads_per_s": round(reads / max(1e-9, duration), 1),
        "errors": int(errors),
        "latency_ms": {"ts_read": {"p50": round(_p50(lat_ms), 2), "p95": round(_p95(lat_ms), 2)}},
        "counts": {"reads": int(reads), "points": int(points)},
    }


# -------------- Cassandra --------------
def _cass_ensure_schema():
    from cassandra.cluster import Cluster

    cl = Cluster(CASS_HOSTS)
    s = cl.connect()
    try:
        s.execute("CREATE KEYSPACE IF NOT EXISTS iot WITH REPLICATION={'class':'SimpleStrategy','replication_factor':1}")
        s.set_keyspace("iot")
        s.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_by_device_day (
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


def _cass_seed(devices: int, points_per_device: int) -> None:
    from cassandra.cluster import Cluster
    from cassandra.query import PreparedStatement
    from cassandra.concurrent import execute_concurrent_with_args

    cl = Cluster(CASS_HOSTS)
    s = cl.connect("iot")
    try:
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - points_per_device * 1000
        ps = s.prepare(
            "INSERT INTO sensor_by_device_day(device_id, day, ts, temp_c, humidity, voltage) VALUES (?,?,?,?,?,?)"
        )
        # Build args in chunks to avoid huge memory, and send with client-side concurrency
        chunk: List[tuple] = []
        chunk_max = 5000
        def flush_chunk():
            if chunk:
                execute_concurrent_with_args(s, ps, list(chunk), concurrency=128)
                chunk.clear()
        for d in range(1, devices + 1):
            for i in range(points_per_device):
                ts = start_ms + i * 1000
                day = _yyyymmdd(ts)
                t = 15.0 + random.random() * 20.0
                h = 30.0 + random.random() * 50.0
                v = 3.0 + random.random() * 0.7
                chunk.append((d, day, ts, t, h, v))
                if len(chunk) >= chunk_max:
                    flush_chunk()
        flush_chunk()
    finally:
        cl.shutdown()


def _run_cassandra(concurrency: int, duration_sec: int, devices: int, points_per_device: int, window_seconds: int) -> Dict[str, Any]:
    _cass_ensure_schema()
    _cass_seed(devices, points_per_device)
    from cassandra.cluster import Cluster
    from cassandra.query import PreparedStatement

    stop_at = time.time() + duration_sec
    lat_ms: List[float] = []
    reads = 0
    points = 0
    errors = 0

    def worker():
        nonlocal reads, points, errors
        # Reuse shared session and prepared statements
        try:
            while time.time() < stop_at:
                did = random.randint(1, devices)
                ts_to_ms = int(time.time() * 1000)
                ts_from_ms = ts_to_ms - window_seconds * 1000
                day_from = _yyyymmdd(ts_from_ms)
                day_to = _yyyymmdd(ts_to_ms)
                t0 = time.perf_counter()
                try:
                    n = 0
                    if day_from == day_to:
                        rs = session.execute(ps_same, (did, day_from, ts_from_ms, ts_to_ms))
                        n += sum(1 for _ in rs)
                    else:
                        rs1 = session.execute(ps_from, (did, day_from, ts_from_ms))
                        rs2 = session.execute(ps_to, (did, day_to, ts_to_ms))
                        n += sum(1 for _ in rs1) + sum(1 for _ in rs2)
                    dt = (time.perf_counter() - t0) * 1000
                    lat_ms.append(dt)
                    points += n
                    reads += 1
                except Exception:
                    errors += 1
        finally:
            pass

    # Create one shared Cluster/Session and prepare statements once
    cl = Cluster(CASS_HOSTS)
    session = cl.connect("iot")
    session.default_timeout = 20.0
    ps_same = session.prepare(
        "SELECT device_id, ts FROM sensor_by_device_day WHERE device_id=? AND day=? AND ts >= ? AND ts < ?"
    )
    ps_from = session.prepare(
        "SELECT device_id, ts FROM sensor_by_device_day WHERE device_id=? AND day=? AND ts >= ?"
    )
    ps_to = session.prepare(
        "SELECT device_id, ts FROM sensor_by_device_day WHERE device_id=? AND day=? AND ts < ?"
    )

    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    t0 = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    cl.shutdown()
    duration = time.time() - t0
    return {
        "engine": "cassandra",
        "duration_s": round(duration, 2),
        "throughput_reads_per_s": round(reads / max(1e-9, duration), 1),
        "errors": int(errors),
        "latency_ms": {"ts_read": {"p50": round(_p50(lat_ms), 2), "p95": round(_p95(lat_ms), 2)}},
        "counts": {"reads": int(reads), "points": int(points)},
    }


def run_engine(backend: str, concurrency: int, duration_sec: int, devices: int, points_per_device: int, window_seconds: int) -> Dict[str, Any]:
    random.seed(7)
    if backend == "postgres":
        return _run_pg(concurrency, duration_sec, devices, points_per_device, window_seconds)
    if backend == "mongodb":
        return _run_mongo(concurrency, duration_sec, devices, points_per_device, window_seconds)
    if backend == "cassandra":
        return _run_cassandra(concurrency, duration_sec, devices, points_per_device, window_seconds)
    raise ValueError(f"Unsupported backend for iot.time_series: {backend}")
