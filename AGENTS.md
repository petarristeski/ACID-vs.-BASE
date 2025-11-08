# Repository Guidelines

## Project Structure & Module Organization

- Root contains `docker-compose.yml` (services: PostgreSQL, MongoDB, Cassandra) and `README.md` with usage notes.
- No application source code or tests are tracked here; data persists via named volumes: `pgdata`, `mongodata`, `cassandradata`.
- Add new services in `docker-compose.yml`; keep related comments close to the block you edit.

## Build, Test, and Development Commands

- Start stack: `docker compose up -d`
- Follow logs: `docker compose logs -f --tail=100`
- Status/health: `docker compose ps`
- Stop stack: `docker compose down`
- Reset data (drops volumes): `docker compose down -v`
- Validate compose file: `docker compose config --quiet`
- Shell access:
  - Postgres: `docker exec -it postgres16 psql -U postgres -d testdb`
  - MongoDB: `docker exec -it mongodb6 mongosh --username root --password root --authenticationDatabase admin`
  - Cassandra: `docker exec -it cassandra41 cqlsh`

## Coding Style & Naming Conventions

- YAML: 2-space indentation, keys in lowercase; group service settings logically (env, ports, volumes, healthcheck).
- Service names: descriptive and versioned (e.g., `postgres16`, `mongodb6`, `cassandra41`).
- Environment variables: UPPER_SNAKE_CASE (e.g., `POSTGRES_PASSWORD`). Use `.env` for local overrides; do not hardcode secrets.
- Pin images to major versions and document changes in `README.md`.

## Testing Guidelines

- Quick health: `docker compose ps` should show `healthy` for all services; Cassandra may take ~1 minute on cold start.
- Config sanity: `docker compose config --quiet` must pass.
- Connectivity checks (optional): run the shell commands above and issue a trivial query (e.g., `SELECT 1;`, `db.runCommand({ ping: 1 })`, `DESCRIBE KEYSPACES;`).

## Commit & Pull Request Guidelines

- Commits: concise, imperative subject; include rationale for version bumps, port or volume changes.
- PRs should include:
  - Summary of changes and why.
  - Impact on credentials, ports, volumes, or healthchecks.
  - Updated `README.md` if behavior or defaults change.
  - Steps to validate locally (`up`, `ps`, connectivity check outputs).

## Security & Configuration Tips

- Prefer `.env` and runtime overrides: `POSTGRES_PASSWORD=... docker compose up -d`.
- Avoid exposing ports you don’t need; keep default auth enabled (enable Cassandra auth when required).
- Never commit real credentials or sample data containing secrets.

## CLI & IO Conventions

- Use Typer for CLIs (typed options, help):
  - `app = typer.Typer(); @app.command() def generate(...): ...; if __name__ == "__main__": app()`
  - Example help: `python -m benchmarks.data_generator --help`
- Prefer `pathlib.Path` for file IO (results, fixtures); avoid `os.path`.
  - Example: `from pathlib import Path; Path('results/raw_data/run.json').write_text('{}')`.

## Writing style

When writing the paper (paper.tex), you shouldn't use an academic language too much. Keep the language understandable, something a master student in computer science can understand.

🧩 Project Context

Title: Comparative Analysis of ACID vs BASE Properties in Modern Workloads
Goal: Perform a comparative benchmark of three database systems — PostgreSQL (ACID), MongoDB (BASE), and Cassandra (BASE) — across real-world scenarios (E-commerce, Social Media, IoT).
Objective: Measure and analyze performance trade-offs between consistency, latency, and throughput under varying workloads.

🧠 Agents Overview
Agent Responsibility Outputs
prep_agent Setup databases and environments Configured DB containers, seed data
scenario_agent Design workload schemas & operations Scenario schemas, example transactions
benchmark_agent Run load tests with concurrency variations Raw benchmark results, logs
metrics_agent Collect throughput, latency, CPU/memory metrics JSON metrics files
analysis_agent Aggregate and visualize performance Graphs, tables, summary report
paper_agent Generate research paper sections LaTeX/Word doc with analysis
dataset_agent Publish benchmark data Zenodo / IEEE DataPort dataset
presentation_agent Prepare final presentation 15–20 slides with results
⚙️ Phase Breakdown
Phase 1 – Подготовка (Setup, 2–3 weeks)

Install & configure PostgreSQL 15+, MongoDB 6+, Cassandra 4+

Design schemas for each scenario

Create synthetic datasets

Phase 2 – Дизајнирање на сценарија (3–4 weeks)

Create three workload scenarios:

🛒 Scenario 1: E-commerce

Ops: order creation, stock updates, payments

Critical: strict consistency (no overselling)

Tests: concurrent orders, rollback handling, order-processing speed

💬 Scenario 2: Social Media

Ops: posts, likes, comments, feed generation

Tolerance: eventual consistency acceptable

Tests: many concurrent writes, feed reads, scalability

🌐 Scenario 3: IoT

Ops: sensor writes, time-series queries, aggregations

Characteristics: high write throughput, rare updates

Tests: high-throughput writes, time-range queries, retention & compression

Phase 3 – Имплементација на benchmarks (4–5 weeks)

Write scripts for:

Data generation (data_generator.py)

Test execution (load_tester.py) — vary:

Concurrent users/connections

Read/write mix

Dataset size

Metrics collection (metrics_collector.py) for:

Throughput (TPS)

Latency (ms)

CPU/memory usage

Consistency violations (if any)

Phase 4 – Извршување на тестови (3–4 weeks)

For each combination (DB × Scenario × Params):

Deploy database with optimal config

Run benchmark

Collect results

Repeat ≥ 3 times for statistical validity

Phase 5 – Анализа и пишување (4–5 weeks)

Analyze results

Create visualizations and tables

Write the research paper

📘 Deliverables
A) Research Paper (6–8 pages)

Structure:

Abstract (150–200 words)

Introduction – motivation

Related Work – prior benchmarks

Methodology – detailed approach

Experimental Setup – hardware/software/configs

Results – graphs, tables, analysis

Discussion – interpretation

Conclusion – insights & future work

References – 15–20 (≤ 5 years old)

B) Benchmark Suite (Code Repository)
📁 benchmark-suite/
├── README.md # detailed documentation
├── docker-compose.yml # for easy setup
├── databases/
│ ├── postgresql/ (schema, config)
│ ├── mongodb/ (schema, config)
│ └── cassandra/ (schema, config)
├── scenarios/
│ ├── ecommerce/
│ ├── social_media/
│ └── iot/
├── benchmarks/
│ ├── data_generator.py
│ ├── load_tester.py
│ └── metrics_collector.py
├── results/
│ └── raw_data/
└── analysis/
├── visualization.py
└── statistical_analysis.py

C) Public Dataset

CSV/JSON with all measured results

Should include:

Raw metrics per test

Aggregated results

Metadata (configs, hardware specs)

Optional publication: Zenodo or IEEE DataPort

D) Presentation

15–20 slides for thesis defense

Optional demo of the benchmark suite

🎯 Expected Outcomes

Be able to answer:

When are ACID systems faster, and when BASE?

What is the performance cost of consistency?

Which database fits each use case?

Are trade-offs predictable?

💡 Success Tips

Start simple – begin with a basic test on one DB, then expand

Document everything – configs, tests, results

Use Docker – ensure reproducibility

Read related work – understand existing methods

🗓 Recommended Timeline
Weeks Tasks
1–3 Setup & preparation
4–7 Scenario design & implementation
8–11 Execute tests
12–15 Analyze & write
16 Finalize & prepare defense
🧪 Tools & Technologies

Databases: PostgreSQL 15+, MongoDB 6+, Cassandra 4+
Benchmark Tools: YCSB, Apache JMeter, pgbench, Sysbench
Monitoring: Grafana + Prometheus, pg_stat_statements, MongoDB Compass/mongostat
Other: Docker Compose, GitHub, LaTeX / Word

📊 Example Summary Table
Database E-comm (TPS/ms) Social (TPS/ms) IoT (TPS/ms) Overall Rank
PostgreSQL 1250 / 12 3800 / 25 8500 / 45 — #2
MongoDB 850 / 18 12400 / 8 15200 / 12 — #1
Cassandra 450 / 35 9600 / 15 22300 / 5 — #1

Would you like me to also create agents.yml (Codex-style configuration file defining triggers, inputs/outputs, and task chains for each agent)?
That would let you plug this directly into a Codex or LangGraph workflow.
