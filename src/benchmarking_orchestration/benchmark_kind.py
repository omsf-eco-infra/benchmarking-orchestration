from __future__ import annotations

from enum import StrEnum

import click


class BenchmarkKind(StrEnum):
    """Supported benchmark workload kinds."""

    MD = "md"
    RBFE = "rbfe"


def _benchmark_kind_choices() -> tuple[str, ...]:
    """Return supported benchmark kind values for CLI choices.

    Returns
    -------
    tuple[str, ...]
        Sorted benchmark kind values accepted by the CLI.
    """
    return tuple(sorted(kind.value for kind in BenchmarkKind))


def _normalize_benchmark_kind(raw_value: str) -> BenchmarkKind:
    """Normalize a raw benchmark kind string to an enum value.

    Parameters
    ----------
    raw_value : str
        Raw benchmark kind value.

    Returns
    -------
    BenchmarkKind
        Normalized benchmark kind enum value.

    Raises
    ------
    ValueError
        If the benchmark kind is unknown.
    """
    normalized = raw_value.strip().lower()
    if not normalized:
        raise ValueError("benchmark kind cannot be empty.")
    try:
        return BenchmarkKind(normalized)
    except ValueError as exc:
        choices = ", ".join(_benchmark_kind_choices())
        raise ValueError(
            f"Unsupported benchmark kind '{raw_value}'. Supported kinds: {choices}."
        ) from exc


def _parse_benchmark_kind(
    _ctx: click.Context,
    _param: click.Parameter,
    value: str,
) -> BenchmarkKind:
    """Parse and normalize benchmark kind option value.

    Parameters
    ----------
    _ctx : click.Context
        Click context (unused).
    _param : click.Parameter
        Click parameter metadata (unused).
    value : str
        Selected benchmark kind value.

    Returns
    -------
    BenchmarkKind
        Parsed benchmark kind enum value.
    """
    return BenchmarkKind(value.lower())
