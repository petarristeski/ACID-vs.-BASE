"""Visualization CLI."""

import typer

from .ecommerce import app as ecommerce_app
from .social_media import app as sm_viz_app
from .iot import app as iot_viz_app


app = typer.Typer(help="Visualization CLI")
app.add_typer(ecommerce_app, name="ecommerce")
app.add_typer(sm_viz_app, name="social_media")
app.add_typer(iot_viz_app, name="iot")
