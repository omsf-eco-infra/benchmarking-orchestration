from __future__ import annotations

from collections.abc import Iterable

from cyclopts import App

from .providers.aws_cli import AwsCLI
from .providers.cli_protocol import ProviderCLI


def create_app(*, providers: Iterable[ProviderCLI] | None = None) -> App:
    """Build the Cyclopts application.

    Parameters
    ----------
    providers : Iterable[ProviderCLI] | None, optional
        Additional provider CLI adapters to register after the built-in AWS
        provider. When ``None``, only the built-in provider is registered.

    Returns
    -------
    App
        Configured Cyclopts application.
    """
    app = App()
    create_group = App(name="create")
    launch_group = App(name="launch")
    worker_group = App(name="worker")

    for provider in [AwsCLI(), *(providers or ())]:
        provider.register_cli(
            create_app=create_group,
            launch_app=launch_group,
            worker_app=worker_group,
        )

    app.command(create_group)
    app.command(launch_group)
    app.command(worker_group)
    return app
