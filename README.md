Database stack for ACID vs BASE benchmarking

This compose file starts PostgreSQL (ACID), MongoDB (BASE), and Apache Cassandra (BASE) for local development and benchmarking.

Quick start
- Start: `docker compose up -d`
- Stop: `docker compose down`
- Reset data (removes volumes): `docker compose down -v`

Default configuration
- PostgreSQL
  - Image: `postgres:16`
  - Port: `5432`
  - User/Pass: `postgres` / `postgres`
  - Database: `testdb`
  - Conn string: `postgresql://postgres:postgres@localhost:5432/testdb`
- MongoDB
  - Image: `mongo:6.0`
  - Port: `27017`
  - Root User/Pass: `root` / `root`
  - Conn string: `mongodb://root:root@localhost:27017/?authSource=admin`
- Cassandra
  - Image: `cassandra:4.1`
  - Port: `9042`
  - Auth: disabled by default (good for quick local benchmarking)

Health checks and readiness
- Each service has a healthcheck; `docker compose ps` shows when they are healthy.
- Cassandra takes the longest; give it ~1 minute on first start.

Customizing credentials
- You can override defaults with environment variables when running compose, e.g.:
  - `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
  - `MONGO_USER`, `MONGO_PASSWORD`
- Example: `POSTGRES_PASSWORD=mysecret MONGO_PASSWORD=secret docker compose up -d`

Enabling Cassandra authentication (optional)
- If you need auth, add these to the `cassandra` service in `docker-compose.yml`:
  - `CASSANDRA_AUTHENTICATOR=PasswordAuthenticator`
  - `CASSANDRA_PASSWORD_SEEDER=yes`
  - Then use `-u cassandra -p cassandra` with `cqlsh` (or set a custom password).

Useful CLI commands
- PostgreSQL shell: `docker exec -it postgres16 psql -U postgres -d testdb`
- Mongo shell: `docker exec -it mongodb6 mongosh --username root --password root --authenticationDatabase admin`
- Cassandra shell: `docker exec -it cassandra41 cqlsh`

Data persistence
- Data is persisted in named volumes: `pgdata`, `mongodata`, `cassandradata`.
- Remove them with `docker compose down -v` to start fresh.

Applying database schemas
- Start services and wait for health: `docker compose up -d && docker compose ps`
- Apply all schemas: `make schemas` (or `bash scripts/apply_schemas.sh`)
  - Postgres reads `databases/postgresql/*.sql` into `testdb`
  - MongoDB runs `databases/mongodb/*.js` with auth (root/root by default)
  - Cassandra runs `databases/cassandra/*.cql`
- Override defaults via env, e.g.:
  - `POSTGRES_DB=mydb MONGO_PASSWORD=secret make schemas`
  - Available overrides: `PG_CONTAINER`, `POSTGRES_USER`, `POSTGRES_DB`, `MONGO_CONTAINER`, `MONGO_USER`, `MONGO_PASSWORD`, `MONGO_AUTH_DB`, `CASSANDRA_CONTAINER`.

Re-running schemas
- Scripts are idempotent for Postgres/Cassandra.
- Mongo files check for collection existence before creating; indexes are safe if unchanged.
- To reset from scratch: `docker compose down -v` then `docker compose up -d` and re-apply.

Configuration
- The CLI reads `config.json` at the project root and applies values on every run.
- Example (default):
  - `{ "mongo_uri": "mongodb://root:root@localhost:27017/?authSource=admin" }`
- Keys supported in `config.json`:
  - `mongo_uri`: overrides `MONGO_URI`.
  - `pg_pool_max`: sets Postgres pool max size (`PG_POOL_MAX`) for the generators.
  - `pg_pool_min`: sets Postgres pool min size (`PG_POOL_MIN`).
  
  Example:
  - `{ "mongo_uri": "mongodb://root:root@localhost:27017/?authSource=admin", "pg_pool_max": 30, "pg_pool_min": 1 }`

## Benchmarks

### E-commerce

Features
- Rollback scenario backends in `benchmarks/rollback/` (Postgres, MongoDB, Cassandra).
- Typer CLI: `benchmarks/data_generator.py` with subcommand under `ecommerce` (`rollback`).
- Root entrypoint: `app.py` exposes the CLI.
- Dependencies: `requirements.txt` contains `psycopg2-binary`, `pymongo`, `cassandra-driver`, `typer`.

How to run
- Install drivers: `pip install -r requirements.txt`
- Ensure schemas are applied: `make schemas`
- Show help: `python app.py --help` or `python app.py data_generator --help`
- E-commerce generator (rollback scenario):
  - Postgres: `python app.py data_generator ecommerce rollback --db postgres --users 100 --duration-sec 30 --hot-skus 50 --initial-stock 50 --late-fail-prob 0.2`
  - MongoDB: `python app.py data_generator ecommerce rollback --db mongodb --users 100 --duration-sec 30 --hot-skus 50 --initial-stock 50 --late-fail-prob 0.2`
  - Cassandra: `python app.py data_generator ecommerce rollback --db cassandra --users 100 --duration-sec 30 --hot-skus 50 --initial-stock 50 --late-fail-prob 0.2`

Load tester (Rollback)
- All DBs in one go:
  - `python app.py load_tester rollback --db all --users 100 --duration-sec 30 --hot-skus 50 --initial-stock 50 --late-fail-prob 0.2 --repeats 3 --out results/raw_data/rollback`
- Output per run (under `results/raw_data/rollback/<db>/`):
  - `run_<timestamp>.jsonl` (JSON Lines; one record per run)
  - `run_<timestamp>.csv` (CSV; one row per run)

Metrics utilities
- Merge multiple runs into a single CSV:
  - `python app.py metrics merge --in results/raw_data/rollback --out results/rollback_summary.csv`

Analysis
– KPI analysis for rollback:
  - `python app.py analysis stats ecommerce rollback --input results/rollback_summary.csv --out-json results/tables/rollback_kpis.json`
  - Prints oversell_rate, orphan_payment_rate, stale_read_rate, abort_rate, counts, totals.

Visualization (Rollback)
- Generate KPI and counts charts from KPIs JSON:
  - `python app.py analysis viz ecommerce rollback --kpis results/tables/rollback_kpis.json --outdir results/plots`
- Charts produced:
  - `rollback_kpi_oversell.png`, `rollback_kpi_orphan.png`, `rollback_kpi_stale.png`, `rollback_kpi_abort.png`
  - `rollback_counts_stacked.png` (ok/rolled_back/abort for PG; ok/compensations/stale_reads for BASE)

### Rollback Scenario (E-commerce)

What it does
- Mixed-cart orders across hot SKUs with late failures to trigger rollback/compensation paths.
- Postgres executes the flow transactionally (SERIALIZABLE); MongoDB/Cassandra use compensations and a projection that may be stale.

Data created by the scenario (dropped/recreated each run)
- PostgreSQL: customers, products, inventory, orders, order_items, payments in database `shop` (via `PG_DSN`).
- MongoDB: inventory_by_sku, orders, order_items_by_order, payments_by_order, orders_projection_by_id in DB `shop` (via `MONGO_URI`).
- Cassandra: inventory_by_sku, orders_by_id, order_items_by_order, payments_by_order, orders_projection_by_id in keyspace `shop`.

Artifacts on disk
- Per run: `results/raw_data/steady/<db>/run_<timestamp>.jsonl` and `.csv` with fields
  - `run_id, scenario, db, sku, customers, initial_stock, orders_per_user, concurrency, failure_rate, started_at, ended_at, duration_s, ok, failed, out_of_stock, total, tps`.
- Merged summary: `results/steady_summary.csv` (from `metrics merge`).
- Stats tables: `results/tables/steady_stats_*.csv` (from `analysis stats`).

How to generate single runs (direct generator)
- Postgres: `python app.py data_generator ecommerce steady --db postgres --customers 2000 --initial-stock 1000 --orders-per-user 1 --concurrency 100 --failure-rate 0.1 --sku SKU-STEADY-1`
- MongoDB: `python app.py data_generator ecommerce steady --db mongodb --customers 2000 --initial-stock 1000 --orders-per-user 1 --concurrency 100 --failure-rate 0.1 --sku SKU-STEADY-1`
- Cassandra: `python app.py data_generator ecommerce steady --db cassandra --customers 2000 --initial-stock 1000 --orders-per-user 1 --concurrency 30 --failure-rate 0.1 --sku SKU-STEADY-1`

How to run orchestrated benchmarks (recommended)
- All DBs, steady params, with repeats and output directory:
  - `python app.py load_tester steady --db all --customers 2000 --initial-stock 1000 --orders-per-user 1 --concurrency 100 --failure-rate 0.1 --sku SKU-STEADY-1 --repeats 5 --out results/raw_data/steady`
- Recommended concurrency:
  - PostgreSQL/MongoDB: `--concurrency 100`
  - Cassandra: `--concurrency 30` (LWT contention at higher levels may cause CAS timeouts)

Post-processing
- Merge: `python app.py metrics merge --in results/raw_data/steady --out results/steady_summary.csv`
- Stats (fixed-load): `python app.py analysis stats ecommerce steady --input results/steady_summary.csv --out results/tables/steady_stats_fixed.csv --filter-concurrency 100`
- Stats (all runs): `python app.py analysis stats ecommerce steady --input results/steady_summary.csv --out results/tables/steady_stats_all.csv`
 - Visualization (fixed-load): `python app.py analysis viz ecommerce steady --input results/steady_summary.csv --outdir results/plots --filter-concurrency 100`
 - Visualization (all runs): `python app.py analysis viz ecommerce steady --input results/steady_summary.csv --outdir results/plots`

Notes
- Postgres provides strict consistency with in-transaction stock locking and commit/rollback — ideal for preventing overselling.
- MongoDB and Cassandra use atomic stock guards and compensating actions; cross-entity consistency is eventual and may be slower under high contention.
- Env defaults match docker-compose; override via `PG*` / `MONGO_URI` / `CASSANDRA_HOSTS` as needed.

SKU flag
- Purpose: identifies the single test product for a run and isolates datasets per scenario.
- Default: `--sku SKU-TEST-1`. Use distinct values per scenario (e.g., `SKU-FLASH-1`, `SKU-CHAOS-1`, `SKU-STEADY-1`).
- Initial stock behavior:
  - PostgreSQL & MongoDB: `--initial-stock` is applied only on first insert for a given SKU (later runs keep existing stock). Use a new `--sku` or manually reset stock for fresh runs.
  - Cassandra: subsequent runs overwrite stock for the same SKU (idempotent upsert).
- Tip: for repeatable experiments without resets, vary `--sku` between runs to avoid cross-run interference.

### Payments Scenario (E-commerce)

What it does
- Creates synchronized bursts of concurrent checkout attempts to stress payment paths.
- High payment failure rate to exercise rollback (Postgres) vs compensation (Mongo/Cassandra).
- Demonstrates impact of write locks and transactional rollbacks on error rates under contention.

Run via data generator
- Postgres: `python app.py data_generator ecommerce payments --db postgres --customers 100 --initial-stock 200 --orders-per-user 2 --concurrency 100 --failure-rate 0.4 --sku SKU-PAY-1 --waves 20`
- MongoDB: `python app.py data_generator ecommerce payments --db mongodb --customers 100 --initial-stock 200 --orders-per-user 2 --concurrency 100 --failure-rate 0.4 --sku SKU-PAY-1 --waves 20`
- Cassandra: `python app.py data_generator ecommerce payments --db cassandra --customers 100 --initial-stock 200 --orders-per-user 2 --concurrency 100 --failure-rate 0.4 --sku SKU-PAY-1 --waves 20`

Run orchestrated benchmarks
- Single DB (repeats):
  - `python app.py load_tester payments --db postgres --customers 1000 --initial-stock 5000 --orders-per-user 1 --concurrency 100 --failure-rate 0.4 --sku SKU-PAY-1 --waves 20 --repeats 5 --out results/raw_data/payments`
- All DBs in one go:
  - `python app.py load_tester payments --db all --customers 1000 --initial-stock 5000 --orders-per-user 1 --concurrency 100 --failure-rate 0.4 --sku SKU-PAY-1 --waves 20 --repeats 5 --out results/raw_data/payments`

Output per run (under `results/raw_data/payments/<db>/`):
- Same schema as steady runs; `scenario` is `payments`.

Notes
- Postgres executes checkout in a single transaction guarded by row locks (`FOR UPDATE`), so failed payments rollback without compensating writes.
- MongoDB and Cassandra decrement stock first, then compensate on failure; under bursty contention this tends to increase failure/compensation counts vs Postgres.

Post-processing
- Merge: `python app.py metrics merge --in results/raw_data/payments --out results/payments_summary.csv`
- Stats (fixed wave-size): `python app.py analysis stats ecommerce payments --input results/payments_summary.csv --out results/tables/payments_stats_fixed.csv --filter-wave-size 100`
- Stats (all runs): `python app.py analysis stats ecommerce payments --input results/payments_summary.csv --out results/tables/payments_stats_all.csv`
- Visualization (fixed wave-size): `python app.py analysis viz ecommerce payments --input results/payments_summary.csv --outdir results/plots --filter-wave-size 100`
- Visualization (all runs): `python app.py analysis viz ecommerce payments --input results/payments_summary.csv --outdir results/plots`

Generated plots (payments)
- `payments_tps_by_db_fixed.png` (or `payments_tps_by_db.png`): Mean TPS by DB with 95% CI.
- `payments_tps_box_fixed.png` (or `payments_tps_box.png`): TPS distribution by DB.
- `payments_outcomes_by_db_fixed.png` (or `payments_outcomes_by_db.png`): Stacked outcome rates (ok, failed, out_of_stock, exception).
- `payments_excess_failure_by_db_fixed.png` (or `payments_excess_failure_by_db.png`): Observed minus configured failure rate by DB.
- `payments_compensation_rate_by_db_fixed.png` (or `payments_compensation_rate_by_db.png`): Compensation rate (BASE vs PG).
- Optional trend plots (emitted when varied): `payments_tps_vs_wave_size.png`, `payments_tps_vs_waves.png`.

<!-- Other scenarios (steady, payments, concurrent_orders) have been removed during the rewrite
     and will be reintroduced later with updated models. -->

Analysis (Rollback)
- Merge runs: `python app.py metrics merge --in results/raw_data/rollback --out results/rollback_summary.csv`
- KPIs (prints and writes JSON):
  - `python app.py analysis stats ecommerce rollback --input results/rollback_summary.csv --out-json results/tables/rollback_kpis.json`
  
  Output format:
  
  === KPI OUTPUT ===
  oversell_rate: { 'postgres': 0.0, 'mongodb': 0.0, 'cassandra': 1.0 }
  orphan_payment_rate: { 'postgres': 0.0, 'mongodb': 0.0, 'cassandra': 0.0 }
  stale_read_rate: { 'postgres': 0.0, 'mongodb': 0.XX, 'cassandra': 1.0 }
  abort_rate: { 'postgres': 0.07, 'mongodb': 0.0, 'cassandra': 0.0 }
  counts: { ... aggregated counters per DB ... }
  totals: { 'postgres': <total_pg>, 'mongodb': <total_mongo>, 'cassandra': <total_cass> }

 Artifacts and naming
 - Merge step writes a per-run summary CSV: `results/rollback_summary.csv` (raw runs, one row per run).
 - Stats step writes aggregated KPIs: `results/tables/rollback_kpis.json` (derived metrics used for charts).
 - Viz step reads the KPIs JSON and writes charts under `results/plots/`.
   These are distinct on purpose: CSV = raw inputs; JSON = computed KPIs.

Analysis (Concurrent Orders)
- Merge: `python app.py metrics merge --in results/raw_data/concurrent_orders --out results/concurrent_orders_summary.csv`
- KPIs (prints and writes JSON):
  - `python app.py analysis stats ecommerce concurrent_orders --input results/concurrent_orders_summary.csv --out-json results/tables/concurrent_orders_kpis.json`
  - Output matches the example block above.
- Visualization:
  - `python app.py analysis viz ecommerce concurrent_orders --kpis results/tables/concurrent_orders_kpis.json --outdir results/plots`
  - Charts: `concurrent_kpi_oversell.png`, `concurrent_kpi_oversell_factor.png`, `concurrent_kpi_available_end.png`,
     `concurrent_counts_success_oos.png`, `concurrent_pg_abort_retries.png`, `concurrent_perf_throughput.png`, `concurrent_perf_latency.png`,
     `concurrent_scatter_thr_vs_reliability.png` (optional).

### Social Media – Concurrent Writes (posts, likes, comments)

What it is
- Write-heavy workload with mixed operations (posts/likes/comments) and immediate reads to probe RYW behavior.
- Each engine resets and seeds its own dataset (DB/keyspace `sm`).

CLI
- Data generator (single DB):
  - Postgres: `python app.py data_generator social_media concurrent_writes --db postgres --concurrency 64 --duration-sec 20`
  - MongoDB: `python app.py data_generator social_media concurrent_writes --db mongodb --concurrency 64 --duration-sec 20`
  - Cassandra: `python app.py data_generator social_media concurrent_writes --db cassandra --concurrency 64 --duration-sec 20`
- Load tester (batch):
  - `python app.py load_tester social_media concurrent_writes --db all --concurrency 64 --duration-sec 20 --repeats 3 --out results/raw_data/social_media/concurrent_writes`

Artifacts
- Per run: `results/raw_data/social_media/concurrent_writes/<db>/run_<ts>.jsonl/.csv` with fields:
  - engine, duration_s, throughput_ops_per_s, errors, dup_like_rejects, ryw_success_rate,
  - latency_ms (create_post/like/comment/read p50/p95), counts (posts/likes/comments/reads)

Analysis (Social Media)
- Merge: `python app.py metrics merge --in results/raw_data/social_media/concurrent_writes --out results/social_media_concurrent_writes_summary.csv`
- KPIs (prints and writes JSON):
  - `python app.py analysis stats social_media concurrent_writes --input results/social_media_concurrent_writes_summary.csv --out-json results/tables/social_media_concurrent_writes_kpis.json`
  - Output matches the example block in the prompt.
- Visualization:
  - `python app.py analysis viz social_media concurrent_writes --kpis results/tables/social_media_concurrent_writes_kpis.json --outdir results/plots`
  - Charts: `social_throughput.png`, `social_ryw.png`, `social_dup_like_rejects.png`,
     `social_create_post_latency.png`, `social_like_latency.png`, `social_comment_latency.png`, `social_read_latency.png`,
     `social_counts_stacked.png`.

Feed Reads scenario
- Run: `python app.py load_tester social_media feed_reads --db all --concurrency 64 --duration-sec 20 --page-size 50 --repeats 3 --out results/raw_data/social_media/feed_reads`
- Merge: `python app.py metrics merge --in results/raw_data/social_media/feed_reads --out results/social_media_feed_reads_summary.csv`
- KPIs: `python app.py analysis stats social_media feed_reads --input results/social_media_feed_reads_summary.csv --out-json results/tables/social_media_feed_reads_kpis.json`
- Output (per engine):
  - `engine, duration_s, throughput_reads_per_s, errors, latency_ms.feed_read.{p50,p95}, counts.reads`
  - Interpreting:
    - throughput_reads_per_s: feed QPS per engine (higher = more pages served per second)
    - p50/p95: median and tail latency for feed page fetch
    - errors: any query errors/timeouts
    - reads: total pages fetched during the run

Models/migrations
- This scenario resets and seeds its own `sm` database/keyspace/collections on each run; you don't need to run project-wide migrations.
- If you prefer schema files under `databases/`, re-run your migration step after adding new models. Otherwise, the scenario code handles creation/seed.

 Artifacts and naming
 - Merge step writes raw per-run CSV: `results/concurrent_orders_summary.csv`.
 - Stats step writes aggregated KPIs: `results/tables/concurrent_orders_kpis.json`.
 - Viz step reads the KPIs JSON and writes charts under `results/plots/`.

Analysis (Concurrent Orders)
- Merge: `python app.py metrics merge --in results/raw_data/concurrent_orders --out results/concurrent_orders_summary.csv`
- KPIs (prints and writes JSON):
  - `python app.py analysis stats ecommerce concurrent_orders --input results/concurrent_orders_summary.csv --out-json results/tables/concurrent_orders_kpis.json`
  - Output matches the example block in this README.
