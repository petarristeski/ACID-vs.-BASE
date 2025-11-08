"""Analysis CLI aggregator."""

import typer

from .statistical_analysis import app as stats_app
from .visualization import app as viz_app


app = typer.Typer(help="Analysis CLI")
app.add_typer(stats_app, name="stats")
app.add_typer(viz_app, name="viz")
