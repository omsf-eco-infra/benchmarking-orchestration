"""Application glue connecting AWS operations to the task database and runner.

Reusable AWS semantics live in the separate ``benchmarking_aws`` distribution.
"""

from .orchestration import (
    process_aws_benchmark_task as process_aws_benchmark_task,
    process_aws_launch_task as process_aws_launch_task,
    queue_aws_tasks as queue_aws_tasks,
)
