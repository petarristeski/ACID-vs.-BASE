"""Project-level CLI aggregator for benchmark commands.

Exposes subcommands: `data_generator`, `load_tester`, `metrics`.
"""

import typer

from .data_generator import app as data_generator_app
from .load_tester import app as load_tester_app
from .metrics_collector import app as metrics_app
from analysis import app as analysis_app


app = typer.Typer(help="Benchmarks CLI")
app.add_typer(data_generator_app, name="data_generator")
app.add_typer(load_tester_app, name="load_tester")
app.add_typer(metrics_app, name="metrics")
app.add_typer(analysis_app, name="analysis")
