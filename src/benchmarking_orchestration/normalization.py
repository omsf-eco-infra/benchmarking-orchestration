from __future__ import annotations

import click


def _normalize_required_value(field_name: str, value: str) -> str:
    """Normalize a required CLI string value.

    Parameters
    ----------
    field_name : str
        Human-readable field label for error messages.
    value : str
        Raw value provided by the caller.

    Returns
    -------
    str
        Stripped value.

    Raises
    ------
    click.BadParameter
        If the stripped value is empty.
    """
    normalized = value.strip()
    if not normalized:
        raise click.BadParameter(f"{field_name} cannot be empty.")
    return normalized


def _normalize_instance_type(instance_type: str) -> str:
    """Normalize and lowercase an EC2 instance type value.

    Parameters
    ----------
    instance_type : str
        Raw instance type argument from the CLI.

    Returns
    -------
    str
        Lowercased, stripped instance type.
    """
    return _normalize_required_value("instance type", instance_type).lower()


def _normalize_region(region: str) -> str:
    """Normalize an AWS region value.

    Parameters
    ----------
    region : str
        Raw region argument from the CLI.

    Returns
    -------
    str
        Stripped region value.
    """
    return _normalize_required_value("region", region)


def _normalize_db_path(db_path: str) -> str:
    """Normalize a task database path value.

    Parameters
    ----------
    db_path : str
        Raw database path argument from the CLI.

    Returns
    -------
    str
        Stripped path value.
    """
    return _normalize_required_value("db path", db_path)


def _normalize_ami_id(ami_id: str) -> str:
    """Normalize and lowercase an AMI identifier value.

    Parameters
    ----------
    ami_id : str
        Raw AMI identifier argument from the CLI.

    Returns
    -------
    str
        Lowercased, stripped AMI identifier.
    """
    return _normalize_required_value("ami id", ami_id).lower()
