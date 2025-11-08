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
    engine: str
    duration_s: float
    throughput_points_per_s: float
    error_rate: float
    p50: float
    p95: float
    ok_points: int
    batches: int
    errors: int


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
            if d.get("scenario") != "iot_sensor_writes":
                continue
            # nested fields
            lat_raw = d.get("latency_ms", "{}")
            counts_raw = d.get("counts", "{}")
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
            rows.append(Row(
                engine=d.get("engine") or d.get("db", ""),
                duration_s=_to_float(d.get("duration_s", 0.0)),
                throughput_points_per_s=_to_float(d.get("throughput_points_per_s", 0.0)),
                error_rate=_to_float(d.get("error_rate", 0.0)),
                p50=_to_float(lat.get("p50", 0.0)),
                p95=_to_float(lat.get("p95", 0.0)),
                ok_points=_to_int(counts.get("ok_points", 0)),
                batches=_to_int(counts.get("batches", 0)),
                errors=_to_int(counts.get("errors", 0)),
            ))
    return rows


app = typer.Typer(help="IoT statistical analysis")


@app.command()
def sensor_writes(
    input_csv: Path = typer.Option(..., "--input", help="Merged CSV (results/iot_sensor_writes_summary.csv)"),
    out_json: Path = typer.Option(Path("results/tables/iot_sensor_writes_kpis.json"), "--out-json", help="Output JSON path"),
) -> None:
    rows = _read_rows(input_csv)
    if not rows:
        typer.echo("No iot_sensor_writes rows found.")
        raise typer.Exit(code=1)

    by_engine: Dict[str, List[Row]] = {}
    for r in rows:
        by_engine.setdefault(r.engine, []).append(r)

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
            "throughput_points_per_s": round(avg(lambda x: x.throughput_points_per_s), 1),
            "error_rate": round(avg(lambda x: x.error_rate), 4),
            "latency_ms": {"p50": round(avg(lambda x: x.p50), 2), "p95": round(avg(lambda x: x.p95), 2)},
            "counts": {"ok_points": sumi(lambda x: x.ok_points), "batches": sumi(lambda x: x.batches), "errors": sumi(lambda x: x.errors)},
        })

    typer.echo("\n=== IOT SENSOR-WRITE RESULTS ===")
    for s in summaries:
        typer.echo(s)

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(summaries, indent=2), encoding="utf-8")
    typer.echo(f"Wrote KPI summaries to {out_json}")

