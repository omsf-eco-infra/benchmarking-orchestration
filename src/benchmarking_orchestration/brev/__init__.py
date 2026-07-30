from __future__ import annotations

from ..benchmark_kind import BenchmarkKind
from ..task_id import _build_brev_task_id
from ..tasks import TaskStatusDB
from .transport import BrevTransport

BREV_TASK_CAPABILITY = "brev"


def queue_brev_tasks(
    task_db: TaskStatusDB,
    instance_type: str,
    profile: str,
    benchmark_kind: BenchmarkKind,
    mps_process_count: int,
    timeout_seconds: float,
) -> list[str]:
    """Queue one Exorcist task per requested Brev benchmark kind.

    Parameters
    ----------
    task_db : TaskStatusDB
        Task database in which every Brev task is queued.
    instance_type : str
        Explicit Brev instance type.
    profile : str
        Explicit benchmark profile identifier.
    benchmark_kind : BenchmarkKind
        Benchmark kind to queue, including ``both``.
    mps_process_count : int
        Number of worker benchmark processes.
    timeout_seconds : float
        Controller timeout for each job.

    Returns
    -------
    list[str]
        Created Brev task IDs.
    """
    kinds = (
        (BenchmarkKind.MD, BenchmarkKind.RBFE)
        if benchmark_kind is BenchmarkKind.BOTH
        else (benchmark_kind,)
    )
    task_ids = []
    for kind in kinds:
        task_id = _build_brev_task_id(
            instance_type,
            profile,
            kind,
            mps_process_count,
            timeout_seconds,
        )
        task_db.add_task_with_capability(
            taskid=task_id,
            requirements=[],
            max_tries=1,
            capability=BREV_TASK_CAPABILITY,
        )
        task_ids.append(task_id)
    return task_ids


__all__ = ["BREV_TASK_CAPABILITY", "BrevTransport", "queue_brev_tasks"]
