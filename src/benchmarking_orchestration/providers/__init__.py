from __future__ import annotations

from .aws_provider import AwsProvider, DEFAULT_LAUNCH_AMI_ID
from .base import LaunchSpec, Provider

DEFAULT_PROVIDER_NAME = "aws"


def _provider_choices() -> tuple[str, ...]:
    """Return supported provider names.

    Returns
    -------
    tuple[str, ...]
        Sorted provider names accepted by orchestration commands.
    """
    return (DEFAULT_PROVIDER_NAME,)


def get_provider(provider_name: str = DEFAULT_PROVIDER_NAME) -> Provider:
    """Resolve a provider adapter from a provider name.

    Parameters
    ----------
    provider_name : str, default="aws"
        Provider selector value.

    Returns
    -------
    Provider
        Instantiated provider adapter.

    Raises
    ------
    ValueError
        If the provider name is unknown.
    """
    normalized_provider_name = provider_name.strip().lower()
    if normalized_provider_name == DEFAULT_PROVIDER_NAME:
        return AwsProvider()

    supported = ", ".join(_provider_choices())
    raise ValueError(
        f"Unsupported provider '{provider_name}'. Supported providers: {supported}."
    )


__all__ = [
    "AwsProvider",
    "DEFAULT_LAUNCH_AMI_ID",
    "DEFAULT_PROVIDER_NAME",
    "LaunchSpec",
    "Provider",
    "get_provider",
    "_provider_choices",
]
