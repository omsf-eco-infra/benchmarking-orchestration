from __future__ import annotations

from typing import Protocol


class Provider(Protocol):
    """Contract for provider compute lifecycle operations."""

    name: str

    def status(self, handle: str, region: str) -> str:
        """Return current lifecycle status for a provider handle.

        Parameters
        ----------
        handle : str
            Provider-specific instance or job identifier.
        region : str
            Region containing the resource.

        Returns
        -------
        str
            Provider-specific status string.
        """
