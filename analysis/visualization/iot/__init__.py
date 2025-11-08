"""IoT visualization CLI."""

import typer

from .sensor_writes import sensor_writes as sw_cmd
from .time_series import time_series as ts_cmd


app = typer.Typer(help="IoT visualizations")
app.command("sensor_writes")(sw_cmd)
app.command("time_series")(ts_cmd)
