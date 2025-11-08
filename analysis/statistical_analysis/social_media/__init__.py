"""Social media stats CLI."""

import typer

from .concurrent_writes import concurrent_writes as sm_cw
from .feed_reads import feed_reads as sm_fr


app = typer.Typer(help="Social media stats")
app.command("concurrent_writes")(sm_cw)
app.command("feed_reads")(sm_fr)
