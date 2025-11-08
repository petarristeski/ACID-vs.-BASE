-- E-commerce schema (Cassandra)
-- Notes:
--  - Partition for read/write patterns; joins are done in application.
--  - For stock decrement, consider Lightweight Transactions (IF stock >= qty) in app logic.

CREATE KEYSPACE IF NOT EXISTS ecommerce
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE ecommerce;

-- Products by SKU
CREATE TABLE IF NOT EXISTS products_by_sku (
  sku text PRIMARY KEY,
  name text,
  price_cents int,
  stock int
);

-- Orders by ID
CREATE TABLE IF NOT EXISTS orders_by_id (
  order_id uuid PRIMARY KEY,
  customer_id uuid,
  status text,
  total_cents int,
  created_at timestamp
);

-- Order items by order
CREATE TABLE IF NOT EXISTS order_items_by_order (
  order_id uuid,
  sku text,
  qty int,
  price_cents int,
  PRIMARY KEY (order_id, sku)
);

-- Payments by order (1:1)
CREATE TABLE IF NOT EXISTS payments_by_order (
  order_id uuid PRIMARY KEY,
  payment_id uuid,
  amount_cents int,
  status text,
  created_at timestamp
);

