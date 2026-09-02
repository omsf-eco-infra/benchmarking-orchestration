from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
from pathlib import Path
from string import Template


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
    ValueError
        If the file cannot be read as UTF-8 text, template syntax is invalid,
        or required placeholders are missing from ``kwargs``.
    """
    try:
        template = cloud_init_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(
            f"Unable to read cloud-init file '{cloud_init_file}': {exc}"
        ) from exc
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"Cloud-init file '{cloud_init_file}' must be UTF-8 text."
        ) from exc

    try:
        parsed = _CloudInitTemplate(template)
        return parsed.substitute(**kwargs)
    except KeyError as exc:
        missing_key = exc.args[0]
        raise ValueError(
            "Missing template value "
            f"'{missing_key}' for cloud-init file '{cloud_init_file}'."
        ) from exc
    except ValueError as exc:
        raise ValueError(
            f"Invalid cloud-init template in '{cloud_init_file}': {exc}"
        ) from exc


def _read_cloud_init_file_as_base64(
    cloud_init_file: str | Path | None,
    template_values: Mapping[str, str] | None = None,
) -> str | None:
    """Render a cloud-init file with explicit values and return base64.

    Parameters
    ----------
    cloud_init_file : str | Path | None
        Path to a cloud-init template, or ``None``.
    template_values : Mapping[str, str] | None, optional
        Explicit template placeholder values.

    Returns
    -------
    str | None
        Base64-encoded rendered contents, or ``None`` when no path is supplied.

    Raises
    ------
    ValueError
        If file reading, rendering, or encoding fails, or the file is empty.
    """
    if cloud_init_file is None:
        return None

    file_path = Path(cloud_init_file)
    rendered_cloud_init = _fill_cloud_init_template(
        file_path, **dict(template_values or {})
    )
    file_bytes = rendered_cloud_init.encode("utf-8")

    if not file_bytes:
        raise ValueError(f"Cloud-init file '{cloud_init_file}' is empty.")

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
