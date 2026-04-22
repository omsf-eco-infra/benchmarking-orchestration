from benchmarking_orchestration.providers import Provider, LaunchSpec
from dataclasses import dataclass


@dataclass
class SaladProvider(Provider):
    name = "salad"

    def submit(self, spec: LaunchSpec) -> str:
        return "hi"

    def status(self, handle: str, region: str) -> str:
        # I think in this case, the region would be the container group
        # handle would be the machine-id
        return "hello"
