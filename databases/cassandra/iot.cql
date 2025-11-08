-- IoT schema (Cassandra)
-- High write throughput, time-series partitions.

CREATE KEYSPACE IF NOT EXISTS iot
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE iot;

-- Device metadata
CREATE TABLE IF NOT EXISTS devices (
  device_id text PRIMARY KEY,
  model text,
  location text,
  created_at timestamp
);

-- Time-series readings partitioned by device and day
-- Use TWCS and optional TTL for retention
CREATE TABLE IF NOT EXISTS readings_by_device_day (
  device_id text,
  day date,
  ts timestamp,
  value double,
  status smallint,
  PRIMARY KEY ((device_id, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': '1'
  };

-- Optional: set default_time_to_live on table for retention, e.g. 365 days
-- ALTER TABLE readings_by_device_day WITH default_time_to_live = 31536000;

