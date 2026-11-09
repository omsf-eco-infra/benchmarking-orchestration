"""AMI approval, regional quota preflight, and EC2 capacity retry policy."""

import logging
import time as _time

from .client import (
    get_instance_type_vcpu_count as _get_instance_type_vcpu_count,
    get_ondemand_g_vcpu_quota as _get_ondemand_g_vcpu_quota,
    get_ondemand_g_vcpus_used as _get_ondemand_g_vcpus_used,
    get_ondemand_p_vcpu_quota as _get_ondemand_p_vcpu_quota,
    get_ondemand_p_vcpus_used as _get_ondemand_p_vcpus_used,
    launch_ec2_instance as _launch_ec2_instance,
)

_logger = logging.getLogger(__name__)
_CAPACITY_RETRY_SLEEP_SECONDS = 60 * 15
_WIGGLE_ROOM = 8


def validate_expected_ami(task_ami_id: str, expected_ami_id: str | None) -> None:
    """Validate a task AMI against an explicitly approved AMI.

    Parameters
    ----------
    task_ami_id : str
        AMI identifier embedded in the launch task.
    expected_ami_id : str | None
        Approved AMI identifier, or ``None`` to allow the task AMI.

    Raises
    ------
    RuntimeError
        If the task and approved AMI identifiers differ.
    """
    if expected_ami_id is None:
        return

    if task_ami_id != expected_ami_id.strip().lower():
        raise RuntimeError(
            "Queued launch task AMI does not match the approved AMI. "
            f"Task AMI: '{task_ami_id}'. Approved AMI: '{expected_ami_id}'."
        )


def _wait_for_ondemand_vcpu_quota(task: str, instance_type: str, region: str) -> None:
    """Wait until the instance family's On-Demand quota permits a launch.

    Parameters
    ----------
    task : str
        Launch task identifier.
    instance_type : str
        EC2 instance type requested by the task.
    region : str
        AWS region containing the requested capacity.
    """
    needed_vcpus = _get_instance_type_vcpu_count(instance_type, region=region)
    if instance_type.startswith("p"):
        get_quota = _get_ondemand_p_vcpu_quota
        get_used = _get_ondemand_p_vcpus_used
        wiggle_room = 0  # A P instance can consume the entire quota by itself.
    else:
        get_quota = _get_ondemand_g_vcpu_quota
        get_used = _get_ondemand_g_vcpus_used
        wiggle_room = _WIGGLE_ROOM
    quota = get_quota(region=region)
    used = get_used(region=region)
    available = max(quota - used - wiggle_room, 0)
    if needed_vcpus <= available:
        return

    _logger.warning(
        f"Insufficient vCPU quota to launch '{task}': "
        f"needed={needed_vcpus}, available={available} "
        f"(quota={quota}, used={used}). Waiting for capacity..."
    )
    while needed_vcpus > available:
        _time.sleep(_CAPACITY_RETRY_SLEEP_SECONDS)
        quota = get_quota(region=region)
        used = get_used(region=region)
        available = max(quota - used - wiggle_room, 0)


def _is_insufficient_instance_capacity_error(exc: BaseException) -> bool:
    """Return whether an exception represents EC2 capacity exhaustion.

    Parameters
    ----------
    exc : BaseException
        Exception raised during EC2 launch.

    Returns
    -------
    bool
        Whether the error code is ``InsufficientInstanceCapacity``.
    """
    if "InsufficientInstanceCapacity" in str(exc):
        return True

    cause = getattr(exc, "__cause__", None)
    response = getattr(cause, "response", {}) if cause is not None else {}
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    return error.get("Code") == "InsufficientInstanceCapacity"


def launch_with_capacity_retry(
    task: str,
    instance_type: str,
    ami_id: str,
    region: str,
    user_data: str | None,
    key_name: str | None,
    instance_profile_name: str | None,
) -> str:
    """Launch an instance after quota waits and retry capacity failures.

    Parameters
    ----------
    task : str
        Launch task identifier.
    instance_type : str
        EC2 instance type.
    ami_id : str
        AMI identifier.
    region : str
        AWS region.
    user_data : str | None
        Optional EC2 user data.
    key_name : str | None
        Optional EC2 key pair name.
    instance_profile_name : str | None
        Optional IAM instance profile name.

    Returns
    -------
    str
        Launched EC2 instance identifier.

    Notes
    -----
    Capacity failures retry indefinitely. Other AWS errors, including expired
    SSO credentials, stop the loop so the user can intervene and reauthenticate.
    """
    while True:
        _wait_for_ondemand_vcpu_quota(task, instance_type, region)
        try:
            return _launch_ec2_instance(
                instance_type,
                ami_id=ami_id,
                region=region,
                user_data=user_data,
                key_name=key_name,
                instance_profile_name=instance_profile_name,
            )
        except Exception as exc:
            if not _is_insufficient_instance_capacity_error(exc):
                raise
            _logger.warning(
                f"Insufficient EC2 instance capacity while launching '{task}'. "
                f"Retrying in {_CAPACITY_RETRY_SLEEP_SECONDS} seconds. "
                f"Original error: {exc}"
            )
            _time.sleep(_CAPACITY_RETRY_SLEEP_SECONDS)
