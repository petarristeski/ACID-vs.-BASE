"""Social media visualization CLI."""

import typer

from .concurrent_writes import concurrent_writes as cw_cmd
from .feed_reads import feed_reads as fr_cmd


app = typer.Typer(help="Social media visualizations")
app.command("concurrent_writes")(cw_cmd)
app.command("feed_reads")(fr_cmd)
