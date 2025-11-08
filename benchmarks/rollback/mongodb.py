from __future__ import annotations

import os
import random
import threading
import time
import uuid
from decimal import Decimal
from typing import Dict, List, Tuple

from pymongo import MongoClient, ReturnDocument


HOT_SKUS_DEFAULT = [f"SKU-{i:03d}" for i in range(50)]


def _client() -> MongoClient:
    uri = os.environ.get("MONGO_URI", "mongodb://root:root@localhost:27017/?authSource=admin")
    return MongoClient(uri, maxPoolSize=300, serverSelectionTimeoutMS=20000)


def setup_schema_and_seed(hot_skus: List[str], initial_stock: int) -> None:
    cli = _client()
    db = cli["shop"]
    # Drop collections for a clean run; if concurrent writers exist, fall back to truncate
    from pymongo.errors import CollectionInvalid
    collections = [
        "inventory_by_sku",
        "orders",
        "order_items_by_order",
        "payments_by_order",
        "orders_projection_by_id",
    ]
    for col in collections:
        try:
            db[col].drop()
        except Exception:
            try:
                # If drop fails or collection locked, at least clear data
                db[col].delete_many({})
            except Exception:
                pass
    # Create (idempotent): if already exists, clear contents
    def ensure_collection(name: str):
        try:
            db.create_collection(name)
        except CollectionInvalid:
            db[name].delete_many({})

    ensure_collection("inventory_by_sku")
    db["inventory_by_sku"].create_index("sku", unique=True)
    ensure_collection("orders")
    ensure_collection("order_items_by_order")
    ensure_collection("payments_by_order")
    ensure_collection("orders_projection_by_id")
    db["orders_projection_by_id"].create_index("order_id", unique=True)
    # Seed inventory
    db["inventory_by_sku"].insert_many([
        {"sku": sku, "initial": initial_stock, "available": initial_stock} for sku in hot_skus
    ])


class MongoRollback:
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
        cli = _client()
        db = cli["shop"]
        while time.time() < stop_at:
            # pull all orders and copy into projection with slight random lag
            for o in db["orders"].find({}, {"_id": 1, "status": 1, "total": 1}):
                if random.random() < 0.5:
                    time.sleep(random.random() * 0.01)
                db["orders_projection_by_id"].update_one(
                    {"order_id": o["_id"]},
                    {"$set": {"status": o.get("status"), "total": o.get("total"), "last_update": time.time()}},
                    upsert=True,
                )
            time.sleep(0.05)

    def _worker(self, stop_at: float) -> None:
        cli = _client()
        db = cli["shop"]
        while time.time() < stop_at:
            order_id = uuid.uuid4().hex
            try:
                lines, total = self._rand_cart()
                db["orders"].insert_one({"_id": order_id, "customer_id": uuid.uuid4().hex, "status": "PENDING", "total": float(total), "created_at": time.time()})
                for i, (sku, qty, price) in enumerate(lines, start=1):
                    db["order_items_by_order"].insert_one({"order_id": order_id, "line_no": i, "sku": sku, "qty": int(qty), "unit_price": float(price)})
                # naive decrement: conditional update if available>=qty
                for (sku, qty, _) in lines:
                    res = db["inventory_by_sku"].find_one_and_update(
                        {"sku": sku, "available": {"$gte": int(qty)}},
                        {"$inc": {"available": -int(qty)}},
                        return_document=ReturnDocument.BEFORE,
                    )
                    if not res:
                        raise RuntimeError("Insufficient stock")
                db["payments_by_order"].insert_one({"order_id": order_id, "status": "CAPTURED", "amount": float(total), "provider_ref": "mongo_ch"})
                if random.random() < self.late_fail:
                    raise RuntimeError("Late failure")
                db["orders"].update_one({"_id": order_id}, {"$set": {"status": "PAID"}})
                row = db["orders_projection_by_id"].find_one({"order_id": order_id})
                if (row is None) or (row.get("status") != "PAID"):
                    with self._lock:
                        self.counts["stale_reads"] += 1
                with self._lock:
                    self.counts["orders_ok"] += 1
            except Exception:
                db["orders"].update_one({"_id": order_id}, {"$set": {"status": "CANCELLED"}})
                db["payments_by_order"].update_one({"order_id": order_id}, {"$set": {"status": "REFUNDED"}})
                for (sku, qty, _) in lines:
                    db["inventory_by_sku"].update_one({"sku": sku}, {"$inc": {"available": int(qty)}})
                row = db["orders_projection_by_id"].find_one({"order_id": order_id})
                if (row is None) or (row.get("status") != "CANCELLED"):
                    with self._lock:
                        self.counts["stale_reads"] += 1
                with self._lock:
                    self.counts["compensations"] += 1

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
        cli = _client()
        db = cli["shop"]
        oversell = 0
        for r in db["inventory_by_sku"].find({}, {"initial": 1, "available": 1}):
            if r.get("available", 0) < 0 or r.get("available", 0) > r.get("initial", 0):
                oversell += 1
        orphan = 0
        for p in db["payments_by_order"].find({"status": "CAPTURED"}, {"order_id": 1}):
            o = db["orders"].find_one({"_id": p["order_id"]}, {"status": 1})
            if (o is None) or (o.get("status") != "PAID"):
                orphan += 1
        return oversell, orphan
