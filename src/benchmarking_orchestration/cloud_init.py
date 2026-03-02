from __future__ import annotations

import base64
import binascii
import os
from pathlib import Path
from string import Template

import click

from .normalization import _normalize_required_value


def _normalize_cloud_init_file_path(cloud_init_file: str | None) -> str | None:
    """Normalize an optional cloud-init file path.

    Parameters
    ----------
    cloud_init_file : str, optional
        Raw cloud-init file path from the CLI.

    Returns
    -------
    str | None
        Stripped file path, or ``None`` when no path is provided.

    Raises
    ------
    click.BadParameter
        If a value is provided but is empty after stripping.
    """
    if cloud_init_file is None:
        return None
    return _normalize_required_value("cloud init file", cloud_init_file)


class _CloudInitTemplate(Template):
    """Template class for cloud-init rendering.

    Uses ``@`` as the placeholder delimiter to avoid collisions with shell
    variables such as ``$HOME`` and ``${PATH}``.
    """

    delimiter = "@"


def _fill_cloud_init_template(cloud_init_file: Path, **kwargs) -> str:
    """Render a cloud-init file with Python template substitution.

    Parameters
    ----------
    cloud_init_file : Path
        Path to the cloud-init template file.
    **kwargs
        Mapping values used for template placeholder replacement.

    Returns
    -------
    str
        Rendered cloud-init text content.

    Raises
    ------
    click.ClickException
        If the file cannot be read as UTF-8 text, template syntax is invalid,
        or required placeholders are missing from ``kwargs``.
    """
    try:
        template = cloud_init_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise click.ClickException(
            f"Unable to read cloud-init file '{cloud_init_file}': {exc}"
        ) from exc
    except UnicodeDecodeError as exc:
        raise click.ClickException(
            f"Cloud-init file '{cloud_init_file}' must be UTF-8 text."
        ) from exc

    try:
        parsed = _CloudInitTemplate(template)
        return parsed.substitute(**kwargs)
    except KeyError as exc:
        missing_key = exc.args[0]
        raise click.ClickException(
            "Missing template value "
            f"'{missing_key}' for cloud-init file '{cloud_init_file}'."
        ) from exc
    except ValueError as exc:
        raise click.ClickException(
            f"Invalid cloud-init template in '{cloud_init_file}': {exc}"
        ) from exc


def _read_cloud_init_file_as_base64(cloud_init_file: str | None) -> str | None:
    """Read a cloud-init file and return a base64 payload.

    Parameters
    ----------
    cloud_init_file : str, optional
        Path to a cloud-init file.

    Returns
    -------
    str | None
        Base64-encoded file contents when provided, otherwise ``None``.

    Raises
    ------
    click.ClickException
        If file reading fails or the file is empty.
    """
    normalized_path = _normalize_cloud_init_file_path(cloud_init_file)
    if normalized_path is None:
        return None

    file_path = Path(normalized_path)
    template_values = dict(os.environ)
    if "TURSO_DATABASE_URL" in template_values:
        template_values["turso_database_url"] = template_values["TURSO_DATABASE_URL"]
    if "TURSO_AUTH_TOKEN" in template_values:
        template_values["turso_auth_token"] = template_values["TURSO_AUTH_TOKEN"]

    rendered_cloud_init = _fill_cloud_init_template(file_path, **template_values)
    file_bytes = rendered_cloud_init.encode("utf-8")

    if not file_bytes:
        raise click.ClickException(f"Cloud-init file '{normalized_path}' is empty.")

    return base64.b64encode(file_bytes).decode("ascii")


def _decode_cloud_init_base64(cloud_init_b64: str) -> str:
    """Decode a base64 cloud-init payload from task metadata.

    Parameters
    ----------
    cloud_init_b64 : str, optional
        Base64 cloud-init payload parsed from the task ID.

    Returns
    -------
    str | None
        Decoded UTF-8 cloud-init content, or ``None`` if not provided.

    Raises
    ------
    ValueError
        If payload encoding is invalid or not UTF-8 text.
    """
    try:
        decoded_bytes = base64.b64decode(cloud_init_b64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(
            "Invalid cloud-init payload encoding in launch task ID."
        ) from exc

    try:
        return decoded_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Cloud-init payload is not valid UTF-8 text.") from exc
