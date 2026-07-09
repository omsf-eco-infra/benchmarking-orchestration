from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class LaunchSpec:
    """Provider-agnostic launch specification.

    Attributes
    ----------
    instance_type : str, optional
        Compute instance type or shape identifier.
    ami_id : str, optional
        Machine image identifier for VM-based providers.
    region : str, default="us-east-1"
        Provider region where launch operations occur.
    user_data : str, optional
        Startup payload for VM bootstrap.
    key_name : str, optional
        Optional SSH key pair name.
    instance_profile_name : str, optional
        Optional provider identity/profile attachment name.
    """

    instance_type: str | None = None
    ami_id: str | None = None
    region: str = "us-east-1"
    user_data: str | None = None
    key_name: str | None = None
    instance_profile_name: str | None = None


class Provider(Protocol):
    """Contract for provider compute lifecycle operations."""

    name: str

    def submit(self, spec: LaunchSpec) -> str:
        """Submit a launch request and return a provider instance handle.

        Parameters
        ----------
        spec : LaunchSpec
            Launch specification to submit.

        Returns
        -------
        str
            Provider-specific instance or job identifier.
        """

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
