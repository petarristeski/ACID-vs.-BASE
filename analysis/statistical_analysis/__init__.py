"""Statistical analysis CLI and helpers."""

import typer

from .ecommerce import app as ecommerce_app
from .social_media import app as sm_app
from .iot import app as iot_app


app = typer.Typer(help="Statistical analysis")
app.add_typer(ecommerce_app, name="ecommerce")
app.add_typer(sm_app, name="social_media")
app.add_typer(iot_app, name="iot")
