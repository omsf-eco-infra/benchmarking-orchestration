from benchmarking_orchestration.providers import Provider, LaunchSpec
from dataclasses import dataclass


@dataclass
class SaladProvider(Provider):
    name = "salad"

    def validate_spec(self, spec: LaunchSpec) -> None: ...

    def submit(self, spec: LaunchSpec) -> str:
        return "hi"

    def status(self, handle: str, region: str) -> str:
        # I think in this case, the region would be the container group
        # handle would be the machine-id
        return "hello"

    def cancel(self, handle: str, region: str) -> None:
        """Cancel or terminate a provider resource.

        Parameters
        ----------
        handle : str
            Provider-specific instance or job identifier.
        region : str
            Region containing the resource.
        """


# class Provider(Protocol):
#     """Contract for provider compute lifecycle operations."""
#
#     name: str
#
#     def validate_spec(self, spec: LaunchSpec) -> None:
#         """Validate launch-related fields for a provider.
#
#         Parameters
#         ----------
#         spec : LaunchSpec
#             Launch specification containing one or more fields to validate.
#         """
#
#     def submit(self, spec: LaunchSpec) -> str:
#         """Submit a launch request and return a provider instance handle.
#
#         Parameters
#         ----------
#         spec : LaunchSpec
#             Launch specification to submit.
#
#         Returns
#         -------
#         str
#             Provider-specific instance or job identifier.
#         """
#
#     def status(self, handle: str, region: str) -> str:
#         """Return current lifecycle status for a provider handle.
#
#         Parameters
#         ----------
#         handle : str
#             Provider-specific instance or job identifier.
#         region : str
#             Region containing the resource.
#
#         Returns
#         -------
#         str
#             Provider-specific status string.
#         """
#
#     def cancel(self, handle: str, region: str) -> None:
#         """Cancel or terminate a provider resource.
#
#         Parameters
#         ----------
#         handle : str
#             Provider-specific instance or job identifier.
#         region : str
#             Region containing the resource.
#         """
