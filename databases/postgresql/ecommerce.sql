-- E-commerce schema (PostgreSQL)
-- Strict consistency focus: use transactions and SELECT ... FOR UPDATE in app logic.

CREATE SCHEMA IF NOT EXISTS ecommerce;
SET search_path TO ecommerce, public;

-- Basic reference data
CREATE TABLE IF NOT EXISTS products (
  sku           text PRIMARY KEY,
  name          text        NOT NULL,
  price_cents   integer     NOT NULL CHECK (price_cents >= 0),
  stock         integer     NOT NULL CHECK (stock >= 0),
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS customers (
  customer_id   uuid        PRIMARY KEY,
  email         text        UNIQUE,
  created_at    timestamptz NOT NULL DEFAULT now()
);

-- Orders and items
CREATE TABLE IF NOT EXISTS orders (
  order_id      uuid        PRIMARY KEY,
  customer_id   uuid        NOT NULL REFERENCES customers(customer_id),
  status        text        NOT NULL CHECK (status IN ('pending','paid','cancelled','shipped','failed')),
  total_cents   integer     NOT NULL CHECK (total_cents >= 0),
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_created
  ON orders (customer_id, created_at DESC);

CREATE TABLE IF NOT EXISTS order_items (
  order_id      uuid        NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
  sku           text        NOT NULL REFERENCES products(sku),
  qty           integer     NOT NULL CHECK (qty > 0),
  price_cents   integer     NOT NULL CHECK (price_cents >= 0),
  PRIMARY KEY (order_id, sku)
);

-- One payment per order (unique), capture/authorize states tracked
CREATE TABLE IF NOT EXISTS payments (
  payment_id    uuid        PRIMARY KEY,
  order_id      uuid        NOT NULL UNIQUE REFERENCES orders(order_id) ON DELETE CASCADE,
  amount_cents  integer     NOT NULL CHECK (amount_cents >= 0),
  status        text        NOT NULL CHECK (status IN ('authorized','captured','failed','refunded')),
  created_at    timestamptz NOT NULL DEFAULT now()
);

-- Note: Preventing overselling relies on transactional updates like:
-- BEGIN; SELECT stock FROM products WHERE sku = $1 FOR UPDATE; -- check and decrement; COMMIT;

