from __future__ import annotations

from cyclopts import App, Parameter, validators

from .providers.aws_cli import AwsCLI
from .providers.salad_cli import SaladCLI


def create_app() -> App:
    app = App()
    create_app = App(name="create")
    launch_app = App(name="launch")
    worker_app = App(name="worker")
    _aws_cli = AwsCLI()
    _aws_cli.register_cli(
        create_app=create_app,
        launch_app=launch_app,
        worker_app=worker_app,
    )
    _salad_cli = SaladCLI()
    _salad_cli.register_cli(
        create_app=create_app,
        launch_app=launch_app,
        worker_app=worker_app,
    )
    app.command(create_app)
    app.command(launch_app)
    app.command(worker_app)
    return app
