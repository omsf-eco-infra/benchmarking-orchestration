"""Brev provider semantics without application or cloud SDK dependencies.

The official Brev CLI is an external controller-side executable; importing
this library never invokes it or requires credentials.
"""

from .transport import BrevTransport

BREV_TASK_CAPABILITY = "brev"

__all__ = ["BREV_TASK_CAPABILITY", "BrevTransport"]
