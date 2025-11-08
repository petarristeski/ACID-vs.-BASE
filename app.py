"""
Project CLI entrypoint.

Reads config.json (if present) to set environment defaults, then exposes the
benchmarks Typer app.
"""

from pathlib import Path
import json
import os

# Load config.json at repo root, if available
_cfg_path = Path(__file__).parent / "config.json"
if _cfg_path.exists():
    try:
        data = json.loads(_cfg_path.read_text(encoding="utf-8"))
        # Map known keys to env vars; config overrides environment unconditionally
        if "mongo_uri" in data:
            os.environ["MONGO_URI"] = str(data["mongo_uri"])
        # Optional Postgres pool tuning
        if "pg_pool_max" in data:
            os.environ["PG_POOL_MAX"] = str(data["pg_pool_max"])
        if "pg_pool_min" in data:
            os.environ["PG_POOL_MIN"] = str(data["pg_pool_min"])
    except Exception:
        # Silent fallback if config is malformed; CLI help still works
        pass

from benchmarks import app as app

if __name__ == "__main__":
    app()
