"""AWS semantics for benchmarking infrastructure.

EC2 capabilities and task formats live here, not in the provider-neutral core.
The SDK client is available in ``benchmarking_aws.client``; importing task and
capability helpers does not import boto3 or the orchestration application.
"""

from .capabilities import WorkerCapability

__all__ = ["WorkerCapability"]
