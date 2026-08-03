from __future__ import annotations

from enum import StrEnum


class BenchmarkKind(StrEnum):
    """Supported benchmark workload kinds."""

    MD = "md"
    RBFE = "rbfe"
    BOTH = "both"


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
