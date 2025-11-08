from __future__ import annotations

import os
import random
import statistics
import threading
import time
import uuid
from decimal import Decimal
from typing import Dict

from cassandra.cluster import Cluster


HOT_SKU_DEFAULT = "SKU-HOT"
UNIT_PRICE_DEFAULT = Decimal("49.00")


def _cluster() -> Cluster:
    hosts = os.environ.get("CASS_HOSTS", "127.0.0.1").split(",")
    return Cluster(hosts)


def _get_available(s, sku: str) -> int:
    row = s.execute("SELECT available FROM inventory WHERE sku=%s", (sku,)).one()
    return int(row.available) if row else 0


def _set_available(s, sku: str, v: int) -> None:
    s.execute("UPDATE inventory SET available=%s WHERE sku=%s", (int(v), sku))


class ConcurrentOrdersCassandra:
    def __init__(self, users: int, duration_s: int, initial_stock: int,
                 hot_sku: str = HOT_SKU_DEFAULT,
                 unit_price: Decimal = UNIT_PRICE_DEFAULT,
                 seed: int = 42) -> None:
        self.users = users
        self.duration_s = duration_s
        self.initial_stock = initial_stock
        self.hot_sku = hot_sku
        self.unit_price = unit_price
        random.seed(seed)
        self.mx = threading.Lock()
        self.metrics: Dict[str, int] = {"cass_success": 0, "cass_oos": 0, "cass_fail": 0}
        self.lat_ms: list[float] = []

    def setup(self) -> None:
        cl = _cluster(); s = cl.connect()
        s.execute("DROP KEYSPACE IF EXISTS shop")
        s.execute("CREATE KEYSPACE shop WITH REPLICATION={'class':'SimpleStrategy','replication_factor':1}")
        s.set_keyspace("shop")
        s.execute("CREATE TABLE products (sku TEXT PRIMARY KEY, price DECIMAL)")
        s.execute("CREATE TABLE inventory (sku TEXT PRIMARY KEY, available INT)")
        s.execute("CREATE TABLE orders (order_id UUID PRIMARY KEY, status TEXT)")
        s.execute("CREATE TABLE payments (order_id UUID PRIMARY KEY, status TEXT, amount DECIMAL)")
        s.execute("INSERT INTO products(sku,price) VALUES (%s,%s)", (self.hot_sku, self.unit_price))
        s.execute("INSERT INTO inventory(sku,available) VALUES (%s,%s)", (self.hot_sku, self.initial_stock))
        cl.shutdown()

    def _worker(self, stop_at: float) -> None:
        cl = _cluster(); s = cl.connect("shop")
        while time.time() < stop_at:
            t0 = time.perf_counter()
            try:
                oid = uuid.uuid4()
                s.execute("INSERT INTO orders(order_id,status) VALUES (%s,'PENDING')", (oid,))
                avail = _get_available(s, self.hot_sku)
                if avail >= 1:
                    _set_available(s, self.hot_sku, avail - 1)
                    s.execute("INSERT INTO payments(order_id,status,amount) VALUES (%s,'CAPTURED',%s)", (oid, self.unit_price))
                    s.execute("UPDATE orders SET status='PAID' WHERE order_id=%s", (oid,))
                    with self.mx:
                        self.metrics["cass_success"] += 1
                        self.lat_ms.append((time.perf_counter() - t0) * 1000)
                else:
                    s.execute("UPDATE orders SET status='CANCELLED' WHERE order_id=%s", (oid,))
                    with self.mx:
                        self.metrics["cass_oos"] += 1
                        self.lat_ms.append((time.perf_counter() - t0) * 1000)
            except Exception:
                with self.mx:
                    self.metrics["cass_fail"] += 1
        cl.shutdown()

    def run(self) -> Dict[str, object]:
        stop_at = time.time() + self.duration_s
        threads = [threading.Thread(target=self._worker, args=(stop_at,)) for _ in range(self.users)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        cl = _cluster(); s = cl.connect("shop")
        avail = _get_available(s, self.hot_sku)
        paid = s.execute("SELECT COUNT(*) FROM orders WHERE status='PAID' ALLOW FILTERING").one()[0]
        cl.shutdown()

        oversell = (avail < 0) or (paid > self.initial_stock)
        tp_succ = self.metrics["cass_success"] / max(1, self.duration_s)
        lat_p50 = statistics.median(self.lat_ms) if self.lat_ms else 0.0
        def p95(xs):
            if not xs: return 0.0
            xs = sorted(xs)
            k = int(round(0.95 * (len(xs)-1)))
            return xs[k]
        lat_p95 = p95(self.lat_ms)
        return {
            "cass_paid_orders": int(paid),
            "cass_available_end": int(avail),
            "cass_oversell_event": bool(oversell),
            "cass_throughput_succ_per_s": tp_succ,
            "cass_latency_p50_ms": lat_p50,
            "cass_latency_p95_ms": lat_p95,
            "cass_oos_attempts": self.metrics["cass_oos"],
            "cass_fail": self.metrics["cass_fail"],
        }
