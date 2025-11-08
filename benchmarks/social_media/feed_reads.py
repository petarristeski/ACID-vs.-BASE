from __future__ import annotations

import os
import random
import threading
import time
import uuid
import statistics
from dataclasses import dataclass
from typing import Dict, Any, List


DATASET_POSTS = int(os.getenv("SM_FEED_POSTS", "200000"))
PG_DSN = os.getenv("PG_DSN", "dbname=sm user=postgres host=127.0.0.1")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
CASS_HOSTS = os.getenv("CASS_HOSTS", "127.0.0.1").split(",")


def p50(xs: List[float]) -> float:
    return statistics.median(xs) if xs else 0.0


def p95(xs: List[float]) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    k = int(round(0.95 * (len(xs) - 1)))
    return xs[k]


@dataclass
class FeedMetrics:
    name: str
    lat_read: List[float]
    reads: int = 0
    errors: int = 0

    def to_summary(self, duration: float) -> Dict[str, Any]:
        return {
            "engine": self.name,
            "duration_s": round(duration, 2),
            "throughput_reads_per_s": round(self.reads / max(1e-9, duration), 1),
            "errors": self.errors,
            "latency_ms": {"feed_read": {"p50": round(p50(self.lat_read), 2), "p95": round(p95(self.lat_read), 2)}},
            "counts": {"reads": self.reads},
        }


# ---------------- Postgres ----------------
import psycopg2


class PGFeed:
    def reset_and_seed(self):
        # Drop phase with autocommit
        with psycopg2.connect(PG_DSN) as c:
            c.autocommit = True
            cur = c.cursor()
            cur.execute("DROP TABLE IF EXISTS feed_posts")
        # Create + seed phase
        with psycopg2.connect(PG_DSN) as c2:
            cur = c2.cursor()
            cur.execute("CREATE TABLE feed_posts (id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL, author_id BIGINT NOT NULL, text TEXT NOT NULL)")
            cur.execute("CREATE INDEX ON feed_posts (ts DESC)")
            # seed: most recent first
            batch = 5000
            now = time.time()
            for start in range(0, DATASET_POSTS, batch):
                rows = []
                for i in range(start, min(start + batch, DATASET_POSTS)):
                    ts = now - (DATASET_POSTS - i) * 0.001
                    rows.append((ts, random.randint(1, 1_000_000), "hi"))
                # Use execute_values for efficient bulk insert
                from psycopg2.extras import execute_values as _exec_vals
                _exec_vals(
                    cur,
                    "INSERT INTO feed_posts(ts,author_id,text) VALUES %s",
                    rows,
                    template="(to_timestamp(%s),%s,%s)",
                    page_size=1000,
                )
            c2.commit()

    def conn(self):
        return psycopg2.connect(PG_DSN)

    def read_feed(self, cur, page_size: int) -> None:
        cur.execute("SELECT id,author_id,ts FROM feed_posts ORDER BY ts DESC LIMIT %s", (page_size,))
        _ = cur.fetchall()


# ---------------- MongoDB ----------------
from pymongo import MongoClient, DESCENDING


class MongoFeed:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)

    def reset_and_seed(self):
        self.client.drop_database("sm_feed")
        db = self.client["sm_feed"]
        posts = db.posts
        posts.create_index([("ts", DESCENDING)])
        batch = 50_000
        now = time.time()
        for start in range(0, DATASET_POSTS, batch):
            docs = []
            for i in range(start, min(start + batch, DATASET_POSTS)):
                ts = now - (DATASET_POSTS - i) * 0.001
                docs.append({"_id": i + 1, "ts": ts, "author_id": random.randint(1, 1_000_000), "text": "hi"})
            if docs:
                posts.insert_many(docs)

    def db(self):
        return self.client["sm_feed"]

    def read_feed(self, db, page_size: int) -> None:
        list(db.posts.find({}, {"_id": 1, "author_id": 1, "ts": 1}).sort("ts", -1).limit(page_size))


# ---------------- Cassandra ----------------
from cassandra.cluster import Cluster


class CassFeed:
    def reset_and_seed(self):
        from cassandra.concurrent import execute_concurrent_with_args
        cl = Cluster(CASS_HOSTS)
        s = cl.connect()
        s.execute("DROP KEYSPACE IF EXISTS sm_feed")
        s.execute("CREATE KEYSPACE sm_feed WITH REPLICATION={'class':'SimpleStrategy','replication_factor':1}")
        s.set_keyspace("sm_feed")
        s.execute("CREATE TABLE posts_by_time (bucket int, ts bigint, post_id uuid, author_id bigint, text text, PRIMARY KEY ((bucket), ts, post_id)) WITH CLUSTERING ORDER BY (ts DESC)")
        # Efficient seeding using prepared statement + concurrent execution
        now_ms = int(time.time() * 1000)
        prepared = s.prepare("INSERT INTO posts_by_time(bucket,ts,post_id,author_id,text) VALUES (0,?,?,?,?)")
        chunk = 5000
        for start in range(0, DATASET_POSTS, chunk):
            args = []
            end = min(start + chunk, DATASET_POSTS)
            for i in range(start, end):
                ts = now_ms - (DATASET_POSTS - i)
                args.append((ts, uuid.uuid4(), random.randint(1, 1_000_000), "hi"))
            if args:
                execute_concurrent_with_args(s, prepared, args, concurrency=128)
        cl.shutdown()

    def session(self):
        cl = Cluster(CASS_HOSTS)
        s = cl.connect("sm_feed")
        return cl, s

    def read_feed(self, s, page_size: int) -> None:
        list(s.execute("SELECT ts,post_id,author_id FROM posts_by_time WHERE bucket=0 LIMIT %s", (page_size,)))


def run_feed(engine: str, concurrency: int, duration_s: int, page_size: int) -> Dict[str, Any]:
    if engine == "postgres":
        adapter = PGFeed(); adapter.reset_and_seed()
        m = FeedMetrics("postgres", lat_read=[])
        stop_at = time.time() + duration_s
        def worker():
            conn = adapter.conn(); cur = conn.cursor()
            while time.time() < stop_at:
                try:
                    t0 = time.perf_counter(); adapter.read_feed(cur, page_size); m.lat_read.append((time.perf_counter() - t0) * 1000); m.reads += 1
                except Exception:
                    m.errors += 1
            conn.close()
        threads = [threading.Thread(target=worker) for _ in range(concurrency)]
        t0 = time.time(); [t.start() for t in threads]; [t.join() for t in threads]; dur = time.time() - t0
        return m.to_summary(dur)
    if engine == "mongodb":
        adapter = MongoFeed(); adapter.reset_and_seed()
        m = FeedMetrics("mongodb", lat_read=[])
        stop_at = time.time() + duration_s
        def worker():
            db = adapter.db()
            while time.time() < stop_at:
                try:
                    t0 = time.perf_counter(); adapter.read_feed(db, page_size); m.lat_read.append((time.perf_counter() - t0) * 1000); m.reads += 1
                except Exception:
                    m.errors += 1
        threads = [threading.Thread(target=worker) for _ in range(concurrency)]
        t0 = time.time(); [t.start() for t in threads]; [t.join() for t in threads]; dur = time.time() - t0
        return m.to_summary(dur)
    if engine == "cassandra":
        adapter = CassFeed(); adapter.reset_and_seed()
        m = FeedMetrics("cassandra", lat_read=[])
        stop_at = time.time() + duration_s
        # Use a single shared session across threads (Session is thread-safe)
        cl, s = adapter.session()
        def worker():
            while time.time() < stop_at:
                try:
                    t0 = time.perf_counter(); adapter.read_feed(s, page_size); m.lat_read.append((time.perf_counter() - t0) * 1000); m.reads += 1
                except Exception:
                    m.errors += 1
        threads = [threading.Thread(target=worker) for _ in range(concurrency)]
        t0 = time.time()
        [t.start() for t in threads]
        [t.join() for t in threads]
        dur = time.time() - t0
        cl.shutdown()
        return m.to_summary(dur)
    raise ValueError(f"Unsupported engine: {engine}")
