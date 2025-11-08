from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import typer


@dataclass
class Row:
    db: str
    scenario: str
    duration_s: float
    engine: str
    throughput_ops_per_s: float
    errors: int
    dup_like_rejects: int
    ryw_success_rate: float
    lat_create_p50: float
    lat_create_p95: float
    lat_like_p50: float
    lat_like_p95: float
    lat_comment_p50: float
    lat_comment_p95: float
    lat_read_p50: float
    lat_read_p95: float
    posts: int
    likes: int
    comments: int
    reads: int


def _to_float(v) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _to_int(v) -> int:
    try:
        return int(float(v))
    except Exception:
        return 0


def _read_rows(path: Path) -> List[Row]:
    rows: List[Row] = []
    import ast
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for d in r:
            if d.get("scenario") != "social_media_concurrent_writes":
                continue
            lat_raw = d.get("latency_ms", "{}")
            counts_raw = d.get("counts", "{}")
            # Parse nested structures that may be JSON or Python dict repr from CSV
            def parse_obj(text: str) -> Dict[str, Any]:
                if isinstance(text, dict):
                    return text
                try:
                    return json.loads(text)
                except Exception:
                    try:
                        val = ast.literal_eval(text)
                        return val if isinstance(val, dict) else {}
                    except Exception:
                        return {}
            lat = parse_obj(lat_raw)
            counts = parse_obj(counts_raw)
            rows.append(
                Row(
                    db=d.get("db", ""),
                    scenario=d.get("scenario", ""),
                    duration_s=_to_float(d.get("duration_s", 0.0)),
                    engine=d.get("engine", d.get("db", "")),
                    throughput_ops_per_s=_to_float(d.get("throughput_ops_per_s", 0.0)),
                    errors=_to_int(d.get("errors", 0)),
                    dup_like_rejects=_to_int(d.get("dup_like_rejects", 0)),
                    ryw_success_rate=_to_float(d.get("ryw_success_rate", 0.0)),
                    lat_create_p50=_to_float(lat.get("create_post", {}).get("p50", 0.0)),
                    lat_create_p95=_to_float(lat.get("create_post", {}).get("p95", 0.0)),
                    lat_like_p50=_to_float(lat.get("like", {}).get("p50", 0.0)),
                    lat_like_p95=_to_float(lat.get("like", {}).get("p95", 0.0)),
                    lat_comment_p50=_to_float(lat.get("comment", {}).get("p50", 0.0)),
                    lat_comment_p95=_to_float(lat.get("comment", {}).get("p95", 0.0)),
                    lat_read_p50=_to_float(lat.get("read", {}).get("p50", 0.0)),
                    lat_read_p95=_to_float(lat.get("read", {}).get("p95", 0.0)),
                    posts=_to_int(counts.get("posts", 0)),
                    likes=_to_int(counts.get("likes", 0)),
                    comments=_to_int(counts.get("comments", 0)),
                    reads=_to_int(counts.get("reads", 0)),
                )
            )
    return rows


app = typer.Typer(help="Social media concurrent writes stats")


@app.command()
def concurrent_writes(
    input_csv: Path = typer.Option(..., "--input", help="Merged CSV (results/social_media_concurrent_writes_summary.csv)"),
    out_json: Path = typer.Option(Path("results/tables/social_media_concurrent_writes_kpis.json"), "--out-json", help="Output JSON path"),
) -> None:
    rows = _read_rows(input_csv)
    if not rows:
        typer.echo("No social_media_concurrent_writes rows found.")
        raise typer.Exit(code=1)

    # Summaries per engine
    by_engine: Dict[str, List[Row]] = {}
    for r in rows:
        by_engine.setdefault(r.engine or r.db, []).append(r)

    summaries: List[Dict[str, Any]] = []
    for eng, items in by_engine.items():
        # aggregate by averaging numeric metrics, summing counts
        n = len(items)
        def avg(get):
            return sum(get(it) for it in items) / n if n else 0.0
        def sumi(get):
            return int(sum(get(it) for it in items))
        summaries.append({
            "engine": eng,
            "duration_s": round(avg(lambda x: x.duration_s), 2),
            "throughput_ops_per_s": round(avg(lambda x: x.throughput_ops_per_s), 1),
            "errors": sumi(lambda x: x.errors),
            "dup_like_rejects": sumi(lambda x: x.dup_like_rejects),
            "ryw_success_rate": round(avg(lambda x: x.ryw_success_rate), 3),
            "latency_ms": {
                "create_post": {"p50": round(avg(lambda x: x.lat_create_p50), 2), "p95": round(avg(lambda x: x.lat_create_p95), 2)},
                "like": {"p50": round(avg(lambda x: x.lat_like_p50), 2), "p95": round(avg(lambda x: x.lat_like_p95), 2)},
                "comment": {"p50": round(avg(lambda x: x.lat_comment_p50), 2), "p95": round(avg(lambda x: x.lat_comment_p95), 2)},
                "read": {"p50": round(avg(lambda x: x.lat_read_p50), 2), "p95": round(avg(lambda x: x.lat_read_p95), 2)},
            },
            "counts": {
                "posts": sumi(lambda x: x.posts),
                "likes": sumi(lambda x: x.likes),
                "comments": sumi(lambda x: x.comments),
                "reads": sumi(lambda x: x.reads),
            }
        })

    typer.echo("\n=== SOCIAL WRITE-HEAVY RESULTS ===")
    for s in summaries:
        typer.echo(s)

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(summaries, indent=2), encoding="utf-8")
    typer.echo(f"Wrote KPI summaries to {out_json}")
