from __future__ import annotations

import os
import random
import threading
import time
import uuid
from decimal import Decimal
from typing import Dict, List, Tuple

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel


HOT_SKUS_DEFAULT = [f"SKU-{i:03d}" for i in range(50)]


def _cluster() -> Cluster:
    hosts = os.environ.get("CASS_HOSTS", "127.0.0.1").split(",")
    return Cluster(hosts)


def setup_schema_and_seed(hot_skus: List[str], initial_stock: int) -> None:
    cl = _cluster()
    s = cl.connect()
    s.execute("DROP KEYSPACE IF EXISTS shop")
    s.execute("CREATE KEYSPACE shop WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}")
    s.set_keyspace("shop")
    s.execute("CREATE TABLE inventory_by_sku (sku TEXT PRIMARY KEY, initial INT, available INT)")
    s.execute("CREATE TABLE orders_by_id (order_id UUID PRIMARY KEY, customer_id UUID, status TEXT, total DECIMAL, created_at TIMESTAMP)")
    s.execute("CREATE TABLE order_items_by_order (order_id UUID, line_no INT, sku TEXT, qty INT, unit_price DECIMAL, PRIMARY KEY((order_id), line_no))")
    s.execute("CREATE TABLE payments_by_order (order_id UUID PRIMARY KEY, status TEXT, amount DECIMAL, provider_ref TEXT)")
    s.execute("CREATE TABLE orders_projection_by_id (order_id UUID PRIMARY KEY, status TEXT, total DECIMAL, last_update TIMESTAMP)")
    for sku in hot_skus:
        s.execute("INSERT INTO inventory_by_sku(sku, initial, available) VALUES (%s,%s,%s)", (sku, initial_stock, initial_stock))
    cl.shutdown()


def _cass_get_available(s, sku: str) -> int:
    row = s.execute("SELECT available FROM inventory_by_sku WHERE sku=%s", (sku,)).one()
    return int(row.available) if row and row.available is not None else 0


def _cass_set_available(s, sku: str, new_value: int) -> None:
    s.execute("UPDATE inventory_by_sku SET available = %s WHERE sku=%s", (new_value, sku))


class CassandraRollback:
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
        self.counts: Dict[str, int] = {"orders_ok": 0, "compensations": 0, "stale_reads": 0}

    def setup(self) -> None:
        setup_schema_and_seed(self.hot_skus, self.initial_stock)

    def _rand_cart(self) -> Tuple[List[Tuple[str, int, Decimal]], Decimal]:
        skuA = random.choice(self.hot_skus)
        lines = [(skuA, 1, self.prices["A"])]
        for _ in range(random.randint(0, 2)):
            lines.append((random.choice(self.hot_skus), 1, self.prices["B"]))
        total = sum(q * price for _, q, price in lines)
        return lines, total

    def _projection_daemon(self, stop_at: float) -> None:
        cl = _cluster()
        s = cl.connect("shop")
        sel = SimpleStatement("SELECT order_id, status, total FROM orders_by_id", consistency_level=ConsistencyLevel.ONE)
        up = SimpleStatement("INSERT INTO orders_projection_by_id(order_id,status,total,last_update) VALUES (?,?,?,toTimestamp(now()))", consistency_level=ConsistencyLevel.ONE)
        while time.time() < stop_at:
            rows = s.execute(sel)
            for r in rows:
                if random.random() < 0.5:
                    time.sleep(random.random() * 0.01)
                s.execute(up, (r.order_id, r.status, r.total))
            time.sleep(0.05)
        cl.shutdown()

    def _worker(self, stop_at: float) -> None:
        cl = _cluster()
        s = cl.connect("shop")
        while time.time() < stop_at:
            order_id = uuid.uuid4()
            try:
                lines, total = self._rand_cart()
                s.execute(
                    "INSERT INTO orders_by_id(order_id,customer_id,status,total,created_at) VALUES (%s,%s,%s,%s,toTimestamp(now()))",
                    (order_id, uuid.uuid4(), "PENDING", total),
                )
                for i, (sku, qty, price) in enumerate(lines, start=1):
                    s.execute(
                        "INSERT INTO order_items_by_order(order_id,line_no,sku,qty,unit_price) VALUES (%s,%s,%s,%s,%s)",
                        (order_id, i, sku, qty, price),
                    )
                # Naive decrement (read -> write) without LWT by design
                for (sku, qty, _) in lines:
                    cur = _cass_get_available(s, sku)
                    _cass_set_available(s, sku, cur - int(qty))

                s.execute(
                    "INSERT INTO payments_by_order(order_id,status,amount,provider_ref) VALUES (%s,%s,%s,%s)",
                    (order_id, "CAPTURED", total, "cass_ch"),
                )
                if random.random() < self.late_fail:
                    raise RuntimeError("Late failure")
                s.execute("UPDATE orders_by_id SET status='PAID' WHERE order_id=%s", (order_id,))
                row = s.execute("SELECT status FROM orders_projection_by_id WHERE order_id=%s", (order_id,)).one()
                if (row is None) or (row.status != "PAID"):
                    with self._lock:
                        self.counts["stale_reads"] += 1
                with self._lock:
                    self.counts["orders_ok"] += 1
            except Exception:
                # compensations
                s.execute("UPDATE orders_by_id SET status='CANCELLED' WHERE order_id=%s", (order_id,))
                s.execute("UPDATE payments_by_order SET status='REFUNDED' WHERE order_id=%s", (order_id,))
                for (sku, qty, _) in lines:
                    cur = _cass_get_available(s, sku)
                    _cass_set_available(s, sku, cur + int(qty))
                row = s.execute("SELECT status FROM orders_projection_by_id WHERE order_id=%s", (order_id,)).one()
                if (row is None) or (row.status != "CANCELLED"):
                    with self._lock:
                        self.counts["stale_reads"] += 1
                with self._lock:
                    self.counts["compensations"] += 1
        cl.shutdown()

    def run(self) -> Dict[str, int]:
        stop_at = time.time() + self.duration_s
        proj_thr = threading.Thread(target=self._projection_daemon, args=(stop_at,))
        proj_thr.start()
        threads = [threading.Thread(target=self._worker, args=(stop_at,)) for _ in range(self.users)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        proj_thr.join()
        return dict(self.counts)

    def kpis(self) -> Tuple[int, int]:
        cl = _cluster()
        s = cl.connect("shop")
        oversell = 0
        orphan = 0
        for r in s.execute("SELECT sku, initial, available FROM inventory_by_sku"):
            if r.available < 0 or r.available > r.initial:
                oversell += 1
        for p in s.execute("SELECT order_id, status FROM payments_by_order"):
            if p.status == "CAPTURED":
                o = s.execute("SELECT status FROM orders_by_id WHERE order_id=%s", (p.order_id,)).one()
                if (o is None) or (o.status != "PAID"):
                    orphan += 1
        cl.shutdown()
        return oversell, orphan
