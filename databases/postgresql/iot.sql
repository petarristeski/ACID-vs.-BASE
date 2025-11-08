-- IoT schema (PostgreSQL)
-- High write throughput, time-series queries, rare updates.

CREATE SCHEMA IF NOT EXISTS iot;
SET search_path TO iot, public;

-- Device metadata
CREATE TABLE IF NOT EXISTS devices (
  device_id text PRIMARY KEY,
  model     text,
  location  text,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Partitioned time-series readings (range by timestamp)
CREATE TABLE IF NOT EXISTS readings (
  device_id text        NOT NULL,
  ts        timestamptz NOT NULL,
  value     double precision NOT NULL,
  status    smallint,
  PRIMARY KEY (device_id, ts)
) PARTITION BY RANGE (ts);

-- Example monthly partition (create more per month/day as needed)
-- CREATE TABLE IF NOT EXISTS readings_2025_01 PARTITION OF readings
--   FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

-- Recommended indexes on partitions for time-range scans:
-- CREATE INDEX ON readings_2025_01 (ts DESC);

