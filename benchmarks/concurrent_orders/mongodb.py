from __future__ import annotations

import os
import random
import statistics
import threading
import time
import uuid
from decimal import Decimal
from typing import Dict

from pymongo import MongoClient, ReturnDocument


HOT_SKU_DEFAULT = "SKU-HOT"
UNIT_PRICE_DEFAULT = Decimal("49.00")


def _client() -> MongoClient:
    uri = os.environ.get("MONGO_URI", "mongodb://root:root@localhost:27017/?authSource=admin")
    return MongoClient(uri, maxPoolSize=300, serverSelectionTimeoutMS=20000)


class ConcurrentOrdersMongo:
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
        self.metrics: Dict[str, int] = {"mongo_success": 0, "mongo_oos": 0, "mongo_fail": 0}
        self.lat_ms: list[float] = []

    def setup(self) -> None:
        cli = _client()
        db = cli["shop"]
        # Drop or truncate collections
        cols = ["products", "inventory", "orders", "payments"]
        for name in cols:
            try:
                db[name].drop()
            except Exception:
                try:
                    db[name].delete_many({})
                except Exception:
                    pass
        # Create
        for name in cols:
            if name not in db.list_collection_names():
                db.create_collection(name)
        db["products"].create_index("sku", unique=True)
        db["inventory"].create_index("sku", unique=True)
        # _id index exists by default on 'orders'; do not recreate with options
        db["payments"].create_index("order_id", unique=True)
        # Seed
        db["products"].insert_one({"sku": self.hot_sku, "name": self.hot_sku, "price": float(self.unit_price)})
        db["inventory"].insert_one({"sku": self.hot_sku, "available": int(self.initial_stock)})

    def _worker(self, stop_at: float) -> None:
        cli = _client(); db = cli["shop"]
        while time.time() < stop_at:
            t0 = time.perf_counter()
            try:
                oid = uuid.uuid4().hex
                db["orders"].insert_one({"_id": oid, "status": "PENDING", "created_at": time.time()})
                # Try atomic reservation
                prev = db["inventory"].find_one_and_update(
                    {"sku": self.hot_sku, "available": {"$gte": 1}},
                    {"$inc": {"available": -1}},
                    return_document=ReturnDocument.BEFORE,
                )
                if not prev or prev.get("available", 0) < 1:
                    db["orders"].update_one({"_id": oid}, {"$set": {"status": "CANCELLED"}})
                    with self.mx:
                        self.metrics["mongo_oos"] += 1
                        self.lat_ms.append((time.perf_counter() - t0) * 1000)
                    continue
                # Payment and close
                db["payments"].insert_one({"order_id": oid, "status": "CAPTURED", "amount": float(self.unit_price)})
                db["orders"].update_one({"_id": oid}, {"$set": {"status": "PAID"}})
                with self.mx:
                    self.metrics["mongo_success"] += 1
                    self.lat_ms.append((time.perf_counter() - t0) * 1000)
            except Exception:
                with self.mx:
                    self.metrics["mongo_fail"] += 1

    def run(self) -> Dict[str, object]:
        stop_at = time.time() + self.duration_s
        threads = [threading.Thread(target=self._worker, args=(stop_at,)) for _ in range(self.users)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        cli = _client(); db = cli["shop"]
        inv = db["inventory"].find_one({"sku": self.hot_sku}) or {}
        avail = int(inv.get("available", 0))
        paid = db["orders"].count_documents({"status": "PAID"})

        oversell = (avail < 0) or (paid > self.initial_stock)
        tp_succ = self.metrics["mongo_success"] / max(1, self.duration_s)
        lat_p50 = statistics.median(self.lat_ms) if self.lat_ms else 0.0
        def p95(xs):
            if not xs: return 0.0
            xs = sorted(xs)
            k = int(round(0.95 * (len(xs)-1)))
            return xs[k]
        lat_p95 = p95(self.lat_ms)
        return {
            "mongo_paid_orders": int(paid),
            "mongo_available_end": int(avail),
            "mongo_oversell_event": bool(oversell),
            "mongo_throughput_succ_per_s": tp_succ,
            "mongo_latency_p50_ms": lat_p50,
            "mongo_latency_p95_ms": lat_p95,
            "mongo_oos_attempts": self.metrics["mongo_oos"],
            "mongo_fail": self.metrics["mongo_fail"],
        }
