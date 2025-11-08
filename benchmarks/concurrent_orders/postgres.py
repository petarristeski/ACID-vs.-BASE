from __future__ import annotations

import os
import random
import statistics
import threading
import time
from contextlib import contextmanager
from decimal import Decimal
from typing import Dict

import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import parse_dsn, make_dsn


HOT_SKU_DEFAULT = "SKU-HOT"
UNIT_PRICE_DEFAULT = Decimal("49.00")


def _dsn_set_dbname(dsn: str, dbname: str) -> str:
    try:
        parts = parse_dsn(dsn)
        parts["dbname"] = dbname
        return make_dsn(**parts)
    except Exception:
        return dsn if "dbname=" in dsn else f"{dsn} dbname={dbname}"


def _dsn_get_dbname(dsn: str) -> str:
    try:
        parts = parse_dsn(dsn)
        return parts.get("dbname") or "shop"
    except Exception:
        return "shop"


def pg_ensure_db(dsn: str, admin_db: str) -> None:
    try:
        psycopg2.connect(dsn).close()
        return
    except psycopg2.OperationalError as e:
        if "does not exist" not in str(e):
            raise
    target = _dsn_get_dbname(dsn)
    for adb in (admin_db, "template1"):
        try:
            admin = psycopg2.connect(_dsn_set_dbname(dsn, adb))
            admin.autocommit = True
            cur = admin.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (target,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{target}"')
            cur.close(); admin.close()
            psycopg2.connect(dsn).close()
            return
        except Exception:
            continue
    raise RuntimeError("Cannot create/connect target PG database")


@contextmanager
def pg_conn(dsn: str):
    c = psycopg2.connect(dsn)
    try:
        yield c
    finally:
        c.close()


class ConcurrentOrdersPostgres:
    def __init__(self, users: int, duration_s: int, initial_stock: int,
                 retry_max: int = 5, hot_sku: str = HOT_SKU_DEFAULT,
                 unit_price: Decimal = UNIT_PRICE_DEFAULT,
                 seed: int = 42) -> None:
        self.users = users
        self.duration_s = duration_s
        self.initial_stock = initial_stock
        self.retry_max = retry_max
        self.hot_sku = hot_sku
        self.unit_price = unit_price
        random.seed(seed)
        self._dsn = os.environ.get("PG_DSN", "dbname=shop user=postgres password=postgres host=127.0.0.1 port=5432")
        self._admin = os.environ.get("PG_ADMIN_DB", "postgres")
        self.mx = threading.Lock()
        # Cap concurrent connections to avoid exhausting Postgres max_connections
        self.conn_cap = int(os.environ.get("PG_CONN_CAP", str(min(users, 50))))
        self._sem = threading.Semaphore(self.conn_cap)
        self.metrics: Dict[str, int] = {"pg_success": 0, "pg_oos": 0, "pg_aborts": 0, "pg_giveup": 0}
        self.lat_ms: list[float] = []

    def setup(self) -> None:
        pg_ensure_db(self._dsn, self._admin)
        with pg_conn(self._dsn) as c:
            c.autocommit = True
            cur = c.cursor()
            cur.execute(
                """
            DO $$
            BEGIN
              IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='payments') THEN DROP TABLE payments; END IF;
              IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='order_items') THEN DROP TABLE order_items; END IF;
              IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='orders') THEN DROP TABLE orders; END IF;
              IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='inventory') THEN DROP TABLE inventory; END IF;
              IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='products') THEN DROP TABLE products; END IF;
              IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='customers') THEN DROP TABLE customers; END IF;
            END$$;
            """
            )
            c.autocommit = False
            cur.execute(
                """
            CREATE TABLE customers (id BIGSERIAL PRIMARY KEY, email TEXT UNIQUE NOT NULL);
            CREATE TABLE products (id BIGSERIAL PRIMARY KEY, sku TEXT UNIQUE NOT NULL, name TEXT NOT NULL, price NUMERIC(12,2) NOT NULL);
            CREATE TABLE inventory (product_id BIGINT PRIMARY KEY REFERENCES products(id), qty_on_hand INT NOT NULL CHECK (qty_on_hand>=0));
            CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, status TEXT NOT NULL CHECK (status IN('PENDING','PAID','CANCELLED')), created_at TIMESTAMPTZ DEFAULT now());
            CREATE TABLE order_items (id BIGSERIAL PRIMARY KEY, order_id BIGINT REFERENCES orders(id) ON DELETE CASCADE, product_id BIGINT REFERENCES products(id), qty INT NOT NULL CHECK (qty>0));
            CREATE TABLE payments (id BIGSERIAL PRIMARY KEY, order_id BIGINT REFERENCES orders(id) UNIQUE, status TEXT NOT NULL CHECK (status IN('CAPTURED','REFUNDED')), amount NUMERIC(12,2) NOT NULL);
            """
            )
            cur.execute("INSERT INTO customers(email) VALUES('buyer@example.com')")
            cur.execute("INSERT INTO products(sku,name,price) VALUES(%s,%s,%s) RETURNING id", (self.hot_sku, self.hot_sku, str(self.unit_price)))
            pid = cur.fetchone()[0]
            cur.execute("INSERT INTO inventory(product_id, qty_on_hand) VALUES(%s,%s)", (pid, self.initial_stock))
            c.commit()

    def _buy_one(self, cur) -> bool:
        cur.execute("INSERT INTO orders(status) VALUES('PENDING') RETURNING id")
        oid = cur.fetchone()[0]
        cur.execute("SELECT id, price FROM products WHERE sku=%s", (self.hot_sku,))
        pid, price = cur.fetchone()
        cur.execute("INSERT INTO order_items(order_id, product_id, qty) VALUES(%s,%s,1)", (oid, pid))
        cur.execute("""
            UPDATE inventory SET qty_on_hand = qty_on_hand - 1
            WHERE product_id=%s AND qty_on_hand >= 1
        """, (pid,))
        if cur.rowcount != 1:
            cur.execute("UPDATE orders SET status='CANCELLED' WHERE id=%s", (oid,))
            return False
        cur.execute("INSERT INTO payments(order_id,status,amount) VALUES(%s,'CAPTURED',%s)", (oid, price))
        cur.execute("UPDATE orders SET status='PAID' WHERE id=%s", (oid,))
        return True

    @contextmanager
    def _conn(self):
        self._sem.acquire()
        try:
            c = psycopg2.connect(self._dsn)
            try:
                yield c
            finally:
                c.close()
        finally:
            self._sem.release()

    def _worker(self, stop_at: float) -> None:
        with self._conn() as c:
            while time.time() < stop_at:
                tries = 0
                t0 = time.perf_counter()
                while True:
                    try:
                        cur = c.cursor()
                        cur.execute("BEGIN")
                        cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                        ok = self._buy_one(cur)
                        c.commit()
                        dt = (time.perf_counter() - t0) * 1000
                        with self.mx:
                            if ok:
                                self.metrics["pg_success"] += 1
                            else:
                                self.metrics["pg_oos"] += 1
                            self.lat_ms.append(dt)
                        break
                    except psycopg2.errors.SerializationFailure:
                        c.rollback()
                        tries += 1
                        with self.mx:
                            self.metrics["pg_aborts"] += 1
                        if tries > self.retry_max:
                            dt = (time.perf_counter() - t0) * 1000
                            with self.mx:
                                self.metrics["pg_giveup"] += 1
                                self.lat_ms.append(dt)
                            break

    def run(self) -> Dict[str, object]:
        stop_at = time.time() + self.duration_s
        threads = [threading.Thread(target=self._worker, args=(stop_at,)) for _ in range(self.users)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # final
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("SELECT qty_on_hand FROM inventory")
            onhand = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM orders WHERE status='PAID'")
            paid = cur.fetchone()[0]
        total_attempts = self.metrics["pg_success"] + self.metrics["pg_oos"] + self.metrics["pg_aborts"] + self.metrics["pg_giveup"]
        abort_rate = (self.metrics["pg_aborts"] / total_attempts) if total_attempts else 0.0
        tp_succ = self.metrics["pg_success"] / max(1, self.duration_s)
        lat_p50 = statistics.median(self.lat_ms) if self.lat_ms else 0.0
        def p95(xs):
            if not xs:
                return 0.0
            xs = sorted(xs)
            k = int(round(0.95 * (len(xs) - 1)))
            return xs[k]
        lat_p95 = p95(self.lat_ms)
        return {
            "pg_paid_orders": paid,
            "pg_qty_on_hand_end": onhand,
            "pg_abort_rate": abort_rate,
            "pg_retries_total": self.metrics["pg_aborts"],
            "pg_throughput_succ_per_s": tp_succ,
            "pg_latency_p50_ms": lat_p50,
            "pg_latency_p95_ms": lat_p95,
            "pg_oos_attempts": self.metrics["pg_oos"],
            "pg_gave_up": self.metrics["pg_giveup"],
        }
