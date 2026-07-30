from __future__ import annotations

from cyclopts import App

from .job_worker import run_job_worker
from .providers.aws_cli import AwsCLI
from .providers.brev_cli import BrevCLI


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
    BrevCLI().register_cli(create_app)
    worker_app.command(run_job_worker, name="job")
    app.command(create_app)
    app.command(launch_app)
    app.command(worker_app)
    return app
