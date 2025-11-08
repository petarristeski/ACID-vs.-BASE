from __future__ import annotations

import csv
import json
import ast
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
    throughput_reads_per_s: float
    errors: int
    p50: float
    p95: float
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
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for d in r:
            if d.get("scenario") != "social_media_feed_reads":
                continue
            lat_raw = d.get("latency_ms", "{}")
            counts_raw = d.get("counts", "{}")
            # Parse nested objects that may appear as JSON or Python dict repr in CSV
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
            fr = lat.get("feed_read", {})
            counts = parse_obj(counts_raw)
            rows.append(
                Row(
                    db=d.get("db", ""),
                    scenario=d.get("scenario", ""),
                    duration_s=_to_float(d.get("duration_s", 0.0)),
                    engine=d.get("engine", d.get("db", "")),
                    throughput_reads_per_s=_to_float(d.get("throughput_reads_per_s", 0.0)),
                    errors=_to_int(d.get("errors", 0)),
                    p50=_to_float(fr.get("p50", 0.0)),
                    p95=_to_float(fr.get("p95", 0.0)),
                    reads=_to_int(counts.get("reads", 0)),
                )
            )
    return rows


app = typer.Typer(help="Social media feed reads stats")


@app.command()
def feed_reads(
    input_csv: Path = typer.Option(..., "--input", help="Merged CSV (results/social_media_feed_reads_summary.csv)"),
    out_json: Path = typer.Option(Path("results/tables/social_media_feed_reads_kpis.json"), "--out-json", help="Output JSON path"),
) -> None:
    rows = _read_rows(input_csv)
    if not rows:
        typer.echo("No social_media_feed_reads rows found.")
        raise typer.Exit(code=1)

    by_engine: Dict[str, List[Row]] = {}
    for r in rows:
        by_engine.setdefault(r.engine or r.db, []).append(r)

    summaries: List[Dict[str, Any]] = []
    for eng, items in by_engine.items():
        n = len(items)
        def avg(get):
            return sum(get(it) for it in items) / n if n else 0.0
        def sumi(get):
            return int(sum(get(it) for it in items))
        summaries.append({
            "engine": eng,
            "duration_s": round(avg(lambda x: x.duration_s), 2),
            "throughput_reads_per_s": round(avg(lambda x: x.throughput_reads_per_s), 1),
            "errors": sumi(lambda x: x.errors),
            "latency_ms": {"feed_read": {"p50": round(avg(lambda x: x.p50), 2), "p95": round(avg(lambda x: x.p95), 2)}},
            "counts": {"reads": sumi(lambda x: x.reads)},
        })

    typer.echo("\n=== SOCIAL FEED-READ RESULTS ===")
    for s in summaries:
        typer.echo(s)

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(summaries, indent=2), encoding="utf-8")
    typer.echo(f"Wrote KPI summaries to {out_json}")
