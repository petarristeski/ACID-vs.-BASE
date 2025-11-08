from __future__ import annotations

import os
import random
import threading
import time
import uuid
import statistics
from dataclasses import dataclass
from typing import Dict, Any, Tuple, List


# Defaults (can be overridden via CLI)
CONCURRENCY = int(os.getenv("CONCURRENCY", "64"))
DURATION_SEC = int(os.getenv("DURATION_SEC", "20"))

READ_RATIO = float(os.getenv("READ_RATIO", "0.2"))
WRITE_RATIO_POST = float(os.getenv("WRITE_RATIO_POST", "0.2"))
WRITE_RATIO_LIKE = float(os.getenv("WRITE_RATIO_LIKE", "0.5"))
WRITE_RATIO_COMMENT = float(os.getenv("WRITE_RATIO_COMMENT", "0.3"))

DATASET_USERS = int(os.getenv("DATASET_USERS", "10000"))
DATASET_POSTS = int(os.getenv("DATASET_POSTS", "20000"))

PG_DSN = os.getenv("PG_DSN", "dbname=sm user=postgres host=127.0.0.1")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
CASS_HOSTS = os.getenv("CASS_HOSTS", "127.0.0.1").split(",")

random.seed(1234)


def p50(xs: List[float]) -> float:
    return statistics.median(xs) if xs else 0.0


def p95(xs: List[float]) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    k = int(round(0.95 * (len(xs) - 1)))
    return xs[k]


@dataclass
class Metrics:
    name: str
    lat_create_post: List[float]
    lat_like: List[float]
    lat_comment: List[float]
    lat_read: List[float]
    ok_posts: int = 0
    ok_likes: int = 0
    ok_comments: int = 0
    reads: int = 0
    errors: int = 0
    dup_like_rejects: int = 0
    ryw_checks: int = 0
    ryw_success: int = 0

    def to_summary(self, duration: float) -> Dict[str, Any]:
        ops_ok = self.ok_posts + self.ok_likes + self.ok_comments + self.reads
        return {
            "engine": self.name,
            "duration_s": round(duration, 2),
            "throughput_ops_per_s": round(ops_ok / max(1e-9, duration), 1),
            "errors": self.errors,
            "dup_like_rejects": self.dup_like_rejects,
            "ryw_success_rate": round(self.ryw_success / max(1, self.ryw_checks), 3),
            "latency_ms": {
                "create_post": {"p50": round(p50(self.lat_create_post), 2), "p95": round(p95(self.lat_create_post), 2)},
                "like": {"p50": round(p50(self.lat_like), 2), "p95": round(p95(self.lat_like), 2)},
                "comment": {"p50": round(p50(self.lat_comment), 2), "p95": round(p95(self.lat_comment), 2)},
                "read": {"p50": round(p50(self.lat_read), 2), "p95": round(p95(self.lat_read), 2)},
            },
            "counts": {
                "posts": self.ok_posts,
                "likes": self.ok_likes,
                "comments": self.ok_comments,
                "reads": self.reads,
            },
        }


# ---------------- Postgres ----------------
import psycopg2
from psycopg2.extras import execute_values


class PGAdapter:
    def reset_and_seed(self):
        # Drop phase (autocommit ON)
        with psycopg2.connect(PG_DSN) as c:
            c.autocommit = True
            cur = c.cursor()
            cur.execute(
                """
                DO $$
                BEGIN
                  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='likes') THEN DROP TABLE likes; END IF;
                  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='comments') THEN DROP TABLE comments; END IF;
                  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='posts') THEN DROP TABLE posts; END IF;
                  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='users') THEN DROP TABLE users; END IF;
                END$$;
                """
            )
        # Create + seed phase (separate connection, default autocommit False)
        with psycopg2.connect(PG_DSN) as c2:
            cur = c2.cursor()
            cur.execute(
                """
                CREATE TABLE users (id BIGSERIAL PRIMARY KEY);
                CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, author_id BIGINT NOT NULL REFERENCES users(id), ts TIMESTAMPTZ NOT NULL DEFAULT now(), text TEXT NOT NULL);
                CREATE TABLE likes (post_id BIGINT NOT NULL REFERENCES posts(id), user_id BIGINT NOT NULL REFERENCES users(id), ts TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (post_id, user_id));
                CREATE TABLE comments (id BIGSERIAL PRIMARY KEY, post_id BIGINT NOT NULL REFERENCES posts(id), user_id BIGINT NOT NULL REFERENCES users(id), ts TIMESTAMPTZ NOT NULL DEFAULT now(), text TEXT NOT NULL);
                CREATE INDEX ON posts(author_id, ts DESC);
                CREATE INDEX ON comments(post_id, ts);
                """
            )
            execute_values(cur, "INSERT INTO users(id) VALUES %s", [(i,) for i in range(1, DATASET_USERS + 1)])
            batch = 1000
            pid = 1
            while pid <= DATASET_POSTS:
                n = min(batch, DATASET_POSTS - pid + 1)
                rows = [(random.randint(1, DATASET_USERS), f"hello {pid + i}") for i in range(n)]
                execute_values(cur, "INSERT INTO posts(author_id, text) VALUES %s", rows)
                pid += n
            c2.commit()

    def conn(self):
        return psycopg2.connect(PG_DSN)

    # Actions
    def create_post(self, cur):
        author = random.randint(1, DATASET_USERS)
        txt = f"post by {author} #{random.randint(1, 1_000_000)}"
        cur.execute("INSERT INTO posts(author_id, text) VALUES (%s,%s) RETURNING id", (author, txt))
        return cur.fetchone()[0]

    def random_post_id(self, cur):
        cur.execute("SELECT id FROM posts OFFSET floor(random()*%s)::int LIMIT 1", (DATASET_POSTS,))
        row = cur.fetchone()
        return row[0] if row else None

    def like_post(self, cur, post_id, user_id):
        try:
            cur.execute(
                "INSERT INTO likes(post_id, user_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (post_id, user_id),
            )
            return cur.rowcount == 1
        except Exception:
            return False

    def comment_post(self, cur, post_id, user_id):
        txt = f"c{user_id}-{random.randint(1, 1_000_000)}"
        cur.execute("INSERT INTO comments(post_id,user_id,text) VALUES (%s,%s,%s)", (post_id, user_id, txt))
        return True

    def read_post_counters(self, cur, post_id) -> Tuple[int, int]:
        cur.execute("SELECT COUNT(*) FROM likes WHERE post_id=%s", (post_id,))
        lc = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM comments WHERE post_id=%s", (post_id,))
        cc = cur.fetchone()[0]
        return lc, cc


# ---------------- MongoDB ----------------
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError


class MongoAdapter:
    def __init__(self):
        self.client = MongoClient(MONGO_URI, uuidRepresentation="standard")

    def reset_and_seed(self):
        self.client.drop_database("sm")
        db = self.client["sm"]
        users = db.users
        posts = db.posts
        likes = db.likes
        comments = db.comments
        # Indexes
        posts.create_index([("author_id", ASCENDING), ("ts", ASCENDING)])
        likes.create_index([("post_id", ASCENDING), ("user_id", ASCENDING)], unique=True)
        comments.create_index([("post_id", ASCENDING), ("ts", ASCENDING)])
        # Seed
        users.insert_many([{"_id": i} for i in range(1, DATASET_USERS + 1)])
        bulk = []
        for pid in range(1, DATASET_POSTS + 1):
            bulk.append({"_id": pid, "author_id": random.randint(1, DATASET_USERS), "ts": time.time(), "text": f"hello {pid}"})
            if len(bulk) >= 10_000:
                posts.insert_many(bulk)
                bulk = []
        if bulk:
            posts.insert_many(bulk)

    def db(self):
        return self.client["sm"]

    def create_post(self, db):
        pid = uuid.uuid4().hex
        db.posts.insert_one({"_id": pid, "author_id": random.randint(1, DATASET_USERS), "ts": time.time(), "text": "hi"})
        return pid

    def random_post_id(self, db):
        return random.randint(1, DATASET_POSTS)

    def like_post(self, db, post_id, user_id):
        try:
            db.likes.insert_one({"post_id": post_id, "user_id": user_id, "ts": time.time()})
            return True
        except DuplicateKeyError:
            return False

    def comment_post(self, db, post_id, user_id):
        db.comments.insert_one({"post_id": post_id, "user_id": user_id, "ts": time.time(), "text": "c"})
        return True

    def read_post_counters(self, db, post_id) -> Tuple[int, int]:
        lc = db.likes.count_documents({"post_id": post_id})
        cc = db.comments.count_documents({"post_id": post_id})
        return lc, cc


# ---------------- Cassandra ----------------
from cassandra.cluster import Cluster


class CassAdapter:
    def reset_and_seed(self):
        cl = Cluster(CASS_HOSTS)
        s = cl.connect()
        s.execute("DROP KEYSPACE IF EXISTS sm")
        s.execute("CREATE KEYSPACE sm WITH REPLICATION={'class':'SimpleStrategy','replication_factor':1}")
        s.set_keyspace("sm")
        s.execute("CREATE TABLE posts_by_id (post_id uuid PRIMARY KEY, author_id bigint, ts bigint, text text)")
        s.execute("CREATE TABLE posts_seed (id int PRIMARY KEY)")
        s.execute("CREATE TABLE likes_by_post (post_id int, user_id bigint, ts bigint, PRIMARY KEY ((post_id), user_id))")
        s.execute("CREATE TABLE comments_by_post (post_id int, ts bigint, comment_id uuid, user_id bigint, text text, PRIMARY KEY ((post_id), ts, comment_id))")
        for pid in range(1, DATASET_POSTS + 1):
            s.execute("INSERT INTO posts_seed(id) VALUES (%s)", (pid,))
        cl.shutdown()

    def session(self):
        cl = Cluster(CASS_HOSTS)
        s = cl.connect("sm")
        return cl, s

    def create_post(self, s):
        pid = uuid.uuid4()
        s.execute(
            "INSERT INTO posts_by_id(post_id, author_id, ts, text) VALUES (%s,%s,%s,%s)",
            (pid, random.randint(1, DATASET_USERS), int(time.time() * 1000), "hi"),
        )
        return pid

    def random_post_id(self, s):
        return random.randint(1, DATASET_POSTS)

    def like_post(self, s, post_id, user_id):
        try:
            s.execute(
                "INSERT INTO likes_by_post(post_id, user_id, ts) VALUES (%s,%s,%s)",
                (post_id, user_id, int(time.time() * 1000)),
            )
            return True
        except Exception:
            return False

    def comment_post(self, s, post_id, user_id):
        s.execute(
            "INSERT INTO comments_by_post(post_id, ts, comment_id, user_id, text) VALUES (%s,%s,%s,%s,%s)",
            (post_id, int(time.time() * 1000), uuid.uuid4(), user_id, "c"),
        )
        return True

    def read_post_counters(self, s, post_id) -> Tuple[int, int]:
        lc = s.execute("SELECT count(*) FROM likes_by_post WHERE post_id=%s", (post_id,)).one()[0]
        cc = s.execute("SELECT count(*) FROM comments_by_post WHERE post_id=%s", (post_id,)).one()[0]
        return int(lc), int(cc)


def _run_pg(concurrency: int, duration_s: int, ratios: Tuple[float, float, float, float]) -> Dict[str, Any]:
    read_ratio, w_post, w_like, w_comment = ratios
    adapter = PGAdapter()
    adapter.reset_and_seed()
    stop_at = time.time() + duration_s
    m = Metrics(name="postgres", lat_create_post=[], lat_like=[], lat_comment=[], lat_read=[])

    def worker():
        conn = adapter.conn(); cur = conn.cursor()
        while time.time() < stop_at:
            r = random.random()
            try:
                if r < w_post:
                    t0 = time.perf_counter(); cur.execute("BEGIN"); pid = adapter.create_post(cur); conn.commit()
                    m.lat_create_post.append((time.perf_counter() - t0) * 1000); m.ok_posts += 1
                    t1 = time.perf_counter(); adapter.read_post_counters(cur, pid); m.lat_read.append((time.perf_counter() - t1) * 1000)
                    m.ryw_checks += 1; m.ryw_success += 1
                elif r < w_post + w_like:
                    post_id = adapter.random_post_id(cur);  
                    if not post_id: continue
                    user = random.randint(1, DATASET_USERS)
                    t0 = time.perf_counter(); cur.execute("BEGIN"); ok = adapter.like_post(cur, post_id, user); conn.commit()
                    m.lat_like.append((time.perf_counter() - t0) * 1000)
                    if ok: m.ok_likes += 1
                    else: m.dup_like_rejects += 1
                    t1 = time.perf_counter(); adapter.read_post_counters(cur, post_id); m.lat_read.append((time.perf_counter() - t1) * 1000)
                    m.ryw_checks += 1; m.ryw_success += 1
                elif r < w_post + w_like + w_comment:
                    post_id = adapter.random_post_id(cur); 
                    if not post_id: continue
                    user = random.randint(1, DATASET_USERS)
                    t0 = time.perf_counter(); cur.execute("BEGIN"); adapter.comment_post(cur, post_id, user); conn.commit()
                    m.lat_comment.append((time.perf_counter() - t0) * 1000); m.ok_comments += 1
                    t1 = time.perf_counter(); adapter.read_post_counters(cur, post_id); m.lat_read.append((time.perf_counter() - t1) * 1000)
                    m.ryw_checks += 1; m.ryw_success += 1
                else:
                    post_id = adapter.random_post_id(cur);  
                    if not post_id: continue
                    t1 = time.perf_counter(); adapter.read_post_counters(cur, post_id); m.lat_read.append((time.perf_counter() - t1) * 1000)
                    m.reads += 1
            except psycopg2.Error:
                conn.rollback(); m.errors += 1
        conn.close()

    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    t0 = time.time(); [t.start() for t in threads]; [t.join() for t in threads]
    dur = time.time() - t0
    return m.to_summary(dur)


def _run_mongo(concurrency: int, duration_s: int, ratios: Tuple[float, float, float, float]) -> Dict[str, Any]:
    read_ratio, w_post, w_like, w_comment = ratios
    adapter = MongoAdapter(); adapter.reset_and_seed()
    stop_at = time.time() + duration_s
    m = Metrics(name="mongodb", lat_create_post=[], lat_like=[], lat_comment=[], lat_read=[])

    def worker():
        db = adapter.db()
        while time.time() < stop_at:
            r = random.random()
            try:
                if r < w_post:
                    t0 = time.perf_counter(); pid = adapter.create_post(db)
                    m.lat_create_post.append((time.perf_counter() - t0) * 1000); m.ok_posts += 1
                    t1 = time.perf_counter(); adapter.read_post_counters(db, pid); m.lat_read.append((time.perf_counter() - t1) * 1000)
                    m.ryw_checks += 1; m.ryw_success += 1
                elif r < w_post + w_like:
                    post_id = adapter.random_post_id(db); user = random.randint(1, DATASET_USERS)
                    t0 = time.perf_counter(); ok = adapter.like_post(db, post_id, user)
                    m.lat_like.append((time.perf_counter() - t0) * 1000)
                    if ok: m.ok_likes += 1
                    else: m.dup_like_rejects += 1
                    t1 = time.perf_counter(); adapter.read_post_counters(db, post_id); m.lat_read.append((time.perf_counter() - t1) * 1000)
                    m.ryw_checks += 1; m.ryw_success += 1
                elif r < w_post + w_like + w_comment:
                    post_id = adapter.random_post_id(db); user = random.randint(1, DATASET_USERS)
                    t0 = time.perf_counter(); adapter.comment_post(db, post_id, user)
                    m.lat_comment.append((time.perf_counter() - t0) * 1000); m.ok_comments += 1
                    t1 = time.perf_counter(); adapter.read_post_counters(db, post_id); m.lat_read.append((time.perf_counter() - t1) * 1000)
                    m.ryw_checks += 1; m.ryw_success += 1
                else:
                    post_id = adapter.random_post_id(db)
                    t1 = time.perf_counter(); adapter.read_post_counters(db, post_id); m.lat_read.append((time.perf_counter() - t1) * 1000)
                    m.reads += 1
            except Exception:
                m.errors += 1

    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    t0 = time.time(); [t.start() for t in threads]; [t.join() for t in threads]
    dur = time.time() - t0
    return m.to_summary(dur)


def _run_cass(concurrency: int, duration_s: int, ratios: Tuple[float, float, float, float]) -> Dict[str, Any]:
    read_ratio, w_post, w_like, w_comment = ratios
    adapter = CassAdapter(); adapter.reset_and_seed()
    stop_at = time.time() + duration_s
    m = Metrics(name="cassandra", lat_create_post=[], lat_like=[], lat_comment=[], lat_read=[])

    def worker():
        cl, s = adapter.session()
        try:
            while time.time() < stop_at:
                r = random.random()
                try:
                    if r < w_post:
                        t0 = time.perf_counter(); adapter.create_post(s)
                        m.lat_create_post.append((time.perf_counter() - t0) * 1000); m.ok_posts += 1
                    elif r < w_post + w_like:
                        post_id = adapter.random_post_id(s); user = random.randint(1, DATASET_USERS)
                        t0 = time.perf_counter(); ok = adapter.like_post(s, post_id, user)
                        m.lat_like.append((time.perf_counter() - t0) * 1000)
                        if ok: m.ok_likes += 1
                        t1 = time.perf_counter(); adapter.read_post_counters(s, post_id); m.lat_read.append((time.perf_counter() - t1) * 1000)
                        m.ryw_checks += 1; m.ryw_success += 1
                    elif r < w_post + w_like + w_comment:
                        post_id = adapter.random_post_id(s); user = random.randint(1, DATASET_USERS)
                        t0 = time.perf_counter(); adapter.comment_post(s, post_id, user)
                        m.lat_comment.append((time.perf_counter() - t0) * 1000); m.ok_comments += 1
                        t1 = time.perf_counter(); adapter.read_post_counters(s, post_id); m.lat_read.append((time.perf_counter() - t1) * 1000)
                        m.ryw_checks += 1; m.ryw_success += 1
                    else:
                        post_id = adapter.random_post_id(s)
                        t1 = time.perf_counter(); adapter.read_post_counters(s, post_id); m.lat_read.append((time.perf_counter() - t1) * 1000)
                        m.reads += 1
                except Exception:
                    m.errors += 1
        finally:
            cl.shutdown()

    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    t0 = time.time(); [t.start() for t in threads]; [t.join() for t in threads]
    dur = time.time() - t0
    return m.to_summary(dur)


def run_engine(engine: str, concurrency: int, duration_s: int) -> Dict[str, Any]:
    # Normalize ratios: scale writes so read + writes = 1.0
    total_w = WRITE_RATIO_POST + WRITE_RATIO_LIKE + WRITE_RATIO_COMMENT
    if total_w <= 0.0 or READ_RATIO >= 1.0:
        raise ValueError("Invalid ratios: total writes must be >0 and READ_RATIO < 1.0")
    write_budget = max(0.0, 1.0 - READ_RATIO)
    scale = write_budget / total_w
    ratios = (
        READ_RATIO,
        WRITE_RATIO_POST * scale,
        WRITE_RATIO_LIKE * scale,
        WRITE_RATIO_COMMENT * scale,
    )
    if engine == "postgres":
        return _run_pg(concurrency, duration_s, ratios)
    if engine == "mongodb":
        return _run_mongo(concurrency, duration_s, ratios)
    if engine == "cassandra":
        return _run_cass(concurrency, duration_s, ratios)
    raise ValueError(f"Unsupported engine: {engine}")
