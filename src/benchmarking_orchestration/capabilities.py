from __future__ import annotations

from enum import StrEnum

import click


class WorkerCapability(StrEnum):
    """Supported worker capability names."""

    LAUNCH = "launch"
    G3 = "g3"
    G4DN = "g4-dn"
    G6 = "g6"
    G6E = "g6-e"
    G5 = "g5"
    P = "p"
    VT1 = "vt1"


def _resolve_bench_worker_capability(instance_type: str) -> WorkerCapability:
    """Resolve benchmark worker capability from an EC2 instance type.

    Parameters
    ----------
    instance_type : str
        Normalized EC2 instance type string.

    Returns
    -------
    WorkerCapability
        Worker capability corresponding to the instance family.

    Raises
    ------
    click.ClickException
        If the instance family does not map to a supported worker capability.
    """
    instance_family = instance_type.split(".", maxsplit=1)[0]
    if instance_family.startswith("p"):
        return WorkerCapability.P

    # EC2 instance families use compact names (e.g. ``g6e``, ``g4dn``)
    # while enum values use kebab-case (``g6-e``, ``g4-dn``) to align
    # with the CLI name transform applied by cyclopts.
    _family_to_capability: dict[str, WorkerCapability] = {
        "g4dn": WorkerCapability.G4DN,
        "g6e": WorkerCapability.G6E,
    }
    if instance_family in _family_to_capability:
        return _family_to_capability[instance_family]

    try:
        return WorkerCapability(instance_family)
    except ValueError as exc:
        raise click.ClickException(
            f"Unsupported benchmark worker capability for instance family "
            f"'{instance_family}'."
        ) from exc
