from __future__ import annotations

import os
import random
import threading
import time
from contextlib import contextmanager
from decimal import Decimal
from typing import Dict, List, Tuple

import psycopg2
from psycopg2.extras import execute_values


HOT_SKUS_DEFAULT = [f"SKU-{i:03d}" for i in range(50)]


def dsn() -> str:
    return os.environ.get(
        "PG_DSN",
        "dbname=shop user=postgres password=postgres host=127.0.0.1 port=5432",
    )


@contextmanager
def pg_conn():
    c = psycopg2.connect(dsn())
    try:
        yield c
    finally:
        c.close()


def setup_schema_and_seed(hot_skus: List[str], initial_stock: int) -> None:
    with pg_conn() as c:
        c.autocommit = True
        cur = c.cursor()
        # Drop objects if exist (order due to FKs)
        cur.execute(
            """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='payments') THEN
            DROP TABLE payments;
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='order_items') THEN
            DROP TABLE order_items;
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='orders') THEN
            DROP TABLE orders;
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='inventory') THEN
            DROP TABLE inventory;
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='products') THEN
            DROP TABLE products;
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='customers') THEN
            DROP TABLE customers;
          END IF;
        END$$;
        """
        )
        c.autocommit = False
        cur.execute(
            """
        CREATE TABLE customers (
          id BIGSERIAL PRIMARY KEY,
          email TEXT UNIQUE NOT NULL
        );
        CREATE TABLE products (
          id BIGSERIAL PRIMARY KEY,
          sku TEXT UNIQUE NOT NULL,
          name TEXT NOT NULL
        );
        CREATE TABLE inventory (
          product_id BIGINT PRIMARY KEY REFERENCES products(id),
          initial_qty INTEGER NOT NULL CHECK (initial_qty >= 0),
          qty_on_hand INTEGER NOT NULL CHECK (qty_on_hand >= 0)
        );
        CREATE TABLE orders (
          id BIGSERIAL PRIMARY KEY,
          customer_id BIGINT NOT NULL REFERENCES customers(id),
          status TEXT NOT NULL CHECK (status IN ('PENDING','PAID','CANCELLED','FULFILLED')),
          total_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE TABLE order_items (
          id BIGSERIAL PRIMARY KEY,
          order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
          product_id BIGINT NOT NULL REFERENCES products(id),
          qty INTEGER NOT NULL CHECK (qty > 0),
          unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0)
        );
        CREATE INDEX oi_order_idx ON order_items(order_id);
        CREATE TABLE payments (
          id BIGSERIAL PRIMARY KEY,
          order_id BIGINT NOT NULL REFERENCES orders(id) UNIQUE,
          status TEXT NOT NULL CHECK (status IN ('INIT','CAPTURED','FAILED','REFUNDED')),
          amount NUMERIC(12,2) NOT NULL,
          provider_ref TEXT
        );
        """
        )
        # Seed inventory + a single customer
        for sku in hot_skus:
            cur.execute("INSERT INTO products(sku,name) VALUES(%s,%s)", (sku, sku))
        cur.execute("SELECT id, sku FROM products WHERE sku = ANY(%s)", (hot_skus,))
        pid_by_sku = {sku: pid for (pid, sku) in cur.fetchall()}
        rows = [(pid_by_sku[sku], initial_stock, initial_stock) for sku in hot_skus]
        execute_values(
            cur,
            "INSERT INTO inventory(product_id, initial_qty, qty_on_hand) VALUES %s",
            rows,
        )
        cur.execute("INSERT INTO customers(email) VALUES('alice@example.com')")
        c.commit()


class PostgresRollback:
    def __init__(self, users: int, duration_s: int, late_fail_prob: float,
                 hot_skus: List[str] | None = None,
                 prices: Dict[str, Decimal] | None = None,
                 initial_stock: int = 50,
                 seed: int = 7) -> None:
        self.users = users
        self.duration_s = duration_s
        self.late_fail = late_fail_prob
        self.hot_skus = hot_skus or HOT_SKUS_DEFAULT
        self.initial_stock = initial_stock
        self.prices = prices or {"A": Decimal("499.00"), "B": Decimal("19.00")}
        random.seed(seed)
        self._lock = threading.Lock()
        self.counts: Dict[str, int] = {"orders_ok": 0, "rolled_back": 0, "abort": 0}

    def setup(self) -> None:
        setup_schema_and_seed(self.hot_skus, self.initial_stock)

    def _rand_cart(self) -> Tuple[List[Tuple[str, int, Decimal]], Decimal]:
        skuA = random.choice(self.hot_skus)
        lines = [(skuA, 1, self.prices["A"])]
        for _ in range(random.randint(0, 2)):
            lines.append((random.choice(self.hot_skus), 1, self.prices["B"]))
        total = sum(q * price for _, q, price in lines)
        return lines, total

    def _worker(self, stop_at: float) -> None:
        with pg_conn() as c:
            c.autocommit = False
            cur = c.cursor()
            while time.time() < stop_at:
                try:
                    cur.execute("BEGIN")
                    cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                    cur.execute("SELECT id FROM customers LIMIT 1")
                    customer_id = cur.fetchone()[0]
                    cur.execute(
                        "INSERT INTO orders(customer_id, status) VALUES (%s,'PENDING') RETURNING id",
                        (customer_id,),
                    )
                    order_id = cur.fetchone()[0]
                    lines, _ = self._rand_cart()
                    sku_list = [l[0] for l in lines]
                    cur.execute("SELECT id, sku FROM products WHERE sku = ANY(%s)", (sku_list,))
                    pid_by_sku = {sku: pid for (pid, sku) in cur.fetchall()}
                    vals = [(order_id, pid_by_sku[sku], qty, str(price)) for (sku, qty, price) in lines]
                    execute_values(
                        cur,
                        "INSERT INTO order_items(order_id, product_id, qty, unit_price) VALUES %s",
                        vals,
                    )
                    cur.execute(
                        """
                        UPDATE orders
                        SET total_amount = (
                            SELECT SUM(qty * unit_price)::numeric(12,2)
                            FROM order_items WHERE order_id=%s
                        ) WHERE id=%s
                        """,
                        (order_id, order_id),
                    )
                    for (sku, qty, _price) in lines:
                        pid = pid_by_sku[sku]
                        cur.execute(
                            """
                            UPDATE inventory
                            SET qty_on_hand = qty_on_hand - %s
                            WHERE product_id=%s AND qty_on_hand >= %s
                            """,
                            (qty, pid, qty),
                        )
                        if cur.rowcount != 1:
                            raise RuntimeError("Insufficient stock")

                    cur.execute(
                        "INSERT INTO payments(order_id, status, amount, provider_ref) "
                        "VALUES (%s,'CAPTURED',(SELECT total_amount FROM orders WHERE id=%s),'pg_ch')",
                        (order_id, order_id),
                    )
                    if random.random() < self.late_fail:
                        raise RuntimeError("Late shipping failure")
                    cur.execute("UPDATE orders SET status='PAID' WHERE id=%s", (order_id,))
                    c.commit()
                    with self._lock:
                        self.counts["orders_ok"] += 1
                except psycopg2.errors.SerializationFailure:
                    c.rollback()
                    with self._lock:
                        self.counts["abort"] += 1
                except Exception:
                    c.rollback()
                    with self._lock:
                        self.counts["rolled_back"] += 1

    def run(self) -> Dict[str, int]:
        stop_at = time.time() + self.duration_s
        threads = [threading.Thread(target=self._worker, args=(stop_at,)) for _ in range(self.users)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return dict(self.counts)

    def kpis(self) -> Tuple[int, int]:
        with pg_conn() as c:
            cur = c.cursor()
            cur.execute("SELECT i.initial_qty, i.qty_on_hand FROM inventory i")
            oversell = sum(1 for initial, onhand in cur.fetchall() if onhand < 0 or onhand > initial)
            cur.execute(
                """
                SELECT pay.order_id
                FROM payments pay LEFT JOIN orders o ON o.id=pay.order_id
                WHERE pay.status='CAPTURED' AND (o.id IS NULL OR o.status <> 'PAID')
                """
            )
            orphan = len(cur.fetchall())
        return oversell, orphan

