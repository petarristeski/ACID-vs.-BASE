"""E-commerce visualization CLI."""

import typer

from .rollback import rollback as rollback_cmd
from .concurrent_orders import concurrent_orders as concurrent_orders_cmd


app = typer.Typer(help="E-commerce visualizations")
app.command("rollback")(rollback_cmd)
app.command("concurrent_orders")(concurrent_orders_cmd)
