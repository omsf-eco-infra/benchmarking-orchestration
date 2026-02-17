from typing import Any, Iterable

import boto3
from botocore.exceptions import BotoCoreError, ClientError, WaiterError

DEFAULT_LAUNCH_AMI_ID = "ami-0ec16471888b25545"


def _is_ondemand_g_quota_name(name: str) -> bool:
    """Check whether a quota name is for On-Demand G/VT capacity.

    Parameters
    ----------
    name : str
        AWS quota name to evaluate.

    Returns
    -------
    bool
        ``True`` if the name matches the On-Demand G/VT quota category,
        otherwise ``False``.
    """
    return "running on-demand g" in name.lower()


def _is_ondemand_g_or_vt_instance_type(instance_type: str) -> bool:
    """Check whether an instance type is in the G or VT family.

    Parameters
    ----------
    instance_type : str
        EC2 instance type identifier (for example, ``g5.xlarge``).

    Returns
    -------
    bool
        ``True`` when the type starts with ``g`` or ``vt``, otherwise ``False``.
    """
    lower = instance_type.lower()
    return lower.startswith("g") or lower.startswith("vt")


def _chunked(items: list[str], size: int) -> Iterable[list[str]]:
    """Yield fixed-size chunks from a list.

    Parameters
    ----------
    items : list[str]
        Input items to split into chunks.
    size : int
        Maximum number of items per chunk.

    Yields
    ------
    list[str]
        Consecutive slices from ``items`` with up to ``size`` elements.
    """
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _extract_running_ondemand_g_instance_types(
    describe_instances_pages: Iterable[dict[str, Any]],
) -> list[str]:
    """Extract running On-Demand G/VT instance types from EC2 pages.

    Parameters
    ----------
    describe_instances_pages : Iterable[dict[str, Any]]
        Paginated responses from ``ec2.describe_instances``.

    Returns
    -------
    list[str]
        Matching instance types for running, non-Spot G/VT instances.
    """
    instance_types = []
    for page in describe_instances_pages:
        for reservation in page.get("Reservations", []):
            for instance in reservation.get("Instances", []):
                instance_type = instance.get("InstanceType", "")
                instance_lifecycle = instance.get("InstanceLifecycle")
                if instance_lifecycle == "spot":
                    continue
                if _is_ondemand_g_or_vt_instance_type(instance_type):
                    instance_types.append(instance_type)
    return instance_types


def _resolve_vcpus_by_instance_type(
    ec2_client: Any, instance_types: list[str]
) -> dict[str, int]:
    """Resolve default vCPU counts for instance types.

    Parameters
    ----------
    ec2_client : Any
        Boto3 EC2 client (or compatible test double).
    instance_types : list[str]
        Instance types that require vCPU metadata.

    Returns
    -------
    dict[str, int]
        Mapping of instance type to its default vCPU count.
    """
    unique_types = sorted(set(instance_types))
    vcpus_by_type = {}
    for batch in _chunked(unique_types, size=100):
        response = ec2_client.describe_instance_types(InstanceTypes=batch)
        for instance_type_info in response.get("InstanceTypes", []):
            instance_type = instance_type_info["InstanceType"]
            default_vcpus = instance_type_info["VCpuInfo"]["DefaultVCpus"]
            vcpus_by_type[instance_type] = default_vcpus
    return vcpus_by_type


def validate_launch_instance_type(
    instance_type: str, region: str = "us-east-1", ec2_client: Any = None
) -> None:
    """Validate that a launch instance type is G/VT and exists in AWS.

    Parameters
    ----------
    instance_type : str
        EC2 instance type identifier.
    region : str, default="us-east-1"
        AWS region used to validate availability.
    ec2_client : Any, optional
        Boto3 EC2 client (or compatible test double). When ``None``,
        a client is created from ``boto3``.

    Raises
    ------
    ValueError
        If the provided instance type is empty or outside G/VT families.
    RuntimeError
        If AWS validation fails or the type is unavailable in region.
    """
    normalized = instance_type.strip().lower()
    if not normalized:
        raise ValueError("instance type cannot be empty.")

    if not _is_ondemand_g_or_vt_instance_type(normalized):
        raise ValueError(
            "Instance type must be in the G/VT family (start with 'g' or 'vt')."
        )

    ec2 = ec2_client or boto3.client("ec2", region_name=region)
    try:
        response = ec2.describe_instance_types(InstanceTypes=[normalized])
    except ClientError as exc:
        error = exc.response.get("Error", {})
        code = error.get("Code", "")
        message = error.get("Message", str(exc))
        if code in {"InvalidInstanceType", "InvalidParameterValue"}:
            raise RuntimeError(
                f"Invalid or unavailable instance type '{normalized}' in region '{region}'."
            ) from exc
        raise RuntimeError(
            f"AWS error while validating instance type '{normalized}' in region '{region}': "
            f"{code or message}"
        ) from exc
    except BotoCoreError as exc:
        raise RuntimeError(
            f"AWS error while validating instance type '{normalized}' in region '{region}': {exc}"
        ) from exc

    resolved_types = {
        item.get("InstanceType", "").lower()
        for item in response.get("InstanceTypes", [])
        if item.get("InstanceType")
    }
    if normalized not in resolved_types:
        raise RuntimeError(
            f"Invalid or unavailable instance type '{normalized}' in region '{region}'."
        )


def validate_launch_ami(
    ami_id: str, region: str = "us-east-1", ec2_client: Any = None
) -> None:
    """Validate that a launch AMI exists and is available in AWS.

    Parameters
    ----------
    ami_id : str
        EC2 AMI identifier to validate.
    region : str, default="us-east-1"
        AWS region where AMI availability should be checked.
    ec2_client : Any, optional
        Boto3 EC2 client (or compatible test double). When ``None``,
        a client is created from ``boto3``.

    Raises
    ------
    ValueError
        If the AMI identifier is empty.
    RuntimeError
        If AWS validation fails, the AMI is missing in region, or the
        AMI state is not ``available``.
    """
    normalized_ami_id = ami_id.strip().lower()
    if not normalized_ami_id:
        raise ValueError("ami id cannot be empty.")

    normalized_region = region.strip()
    if not normalized_region:
        raise ValueError("region cannot be empty.")

    ec2 = ec2_client or boto3.client("ec2", region_name=normalized_region)
    try:
        response = ec2.describe_images(ImageIds=[normalized_ami_id])
    except ClientError as exc:
        error = exc.response.get("Error", {})
        code = error.get("Code", "")
        message = error.get("Message", str(exc))
        if code == "InvalidAMIID.NotFound":
            raise RuntimeError(
                f"AMI '{normalized_ami_id}' is unavailable in region '{normalized_region}'."
            ) from exc
        raise RuntimeError(
            f"AWS error while validating AMI '{normalized_ami_id}' in region "
            f"'{normalized_region}': {code or message}"
        ) from exc
    except BotoCoreError as exc:
        raise RuntimeError(
            f"AWS error while validating AMI '{normalized_ami_id}' in region "
            f"'{normalized_region}': {exc}"
        ) from exc

    images = response.get("Images", [])
    first_image = images[0] if images else {}
    state = first_image.get("State")
    if not first_image or state != "available":
        raise RuntimeError(
            f"AMI '{normalized_ami_id}' is unavailable in region '{normalized_region}'."
        )


def launch_ec2_instance(
    instance_type: str,
    ami_id: str = DEFAULT_LAUNCH_AMI_ID,
    region: str = "us-east-1",
    ec2_client: Any = None,
) -> str:
    """Launch a single EC2 instance and return its instance ID.

    Parameters
    ----------
    instance_type : str
        EC2 instance type identifier to launch.
    ami_id : str, default=DEFAULT_LAUNCH_AMI_ID
        AMI identifier to launch.
    region : str, default="us-east-1"
        AWS region where the launch should occur.
    ec2_client : Any, optional
        Boto3 EC2 client (or compatible test double). When ``None``,
        a client is created from ``boto3``.

    Returns
    -------
    str
        The launched EC2 instance identifier.

    Raises
    ------
    ValueError
        If required inputs are empty.
    RuntimeError
        If AWS launch fails, the instance does not reach ``running``
        state, or response data does not include an instance identifier.
    """
    normalized_instance_type = instance_type.strip().lower()
    if not normalized_instance_type:
        raise ValueError("instance type cannot be empty.")

    normalized_ami_id = ami_id.strip().lower()
    if not normalized_ami_id:
        raise ValueError("ami id cannot be empty.")

    normalized_region = region.strip()
    if not normalized_region:
        raise ValueError("region cannot be empty.")

    ec2 = ec2_client or boto3.client("ec2", region_name=normalized_region)
    try:
        response = ec2.run_instances(
            ImageId=normalized_ami_id,
            InstanceType=normalized_instance_type,
            MinCount=1,
            MaxCount=1,
        )
    except ClientError as exc:
        error = exc.response.get("Error", {})
        code = error.get("Code", "")
        message = error.get("Message", str(exc))
        raise RuntimeError(
            f"AWS error while launching instance type '{normalized_instance_type}' "
            f"with AMI '{normalized_ami_id}' in region '{normalized_region}': {code or message}"
        ) from exc
    except BotoCoreError as exc:
        raise RuntimeError(
            f"AWS error while launching instance type '{normalized_instance_type}' "
            f"with AMI '{normalized_ami_id}' in region '{normalized_region}': {exc}"
        ) from exc

    instances = response.get("Instances", [])
    first_instance = instances[0] if instances else {}
    instance_id = first_instance.get("InstanceId")
    if not instance_id:
        raise RuntimeError(
            f"AWS did not return an instance ID for instance type '{normalized_instance_type}' "
            f"with AMI '{normalized_ami_id}' in region '{normalized_region}'."
        )

    try:
        waiter = ec2.get_waiter("instance_running")
        waiter.wait(
            InstanceIds=[instance_id],
            WaiterConfig={"Delay": 5, "MaxAttempts": 24},
        )
    except WaiterError as exc:
        raise RuntimeError(
            f"Instance '{instance_id}' did not reach running state in region "
            f"'{normalized_region}'."
        ) from exc
    except BotoCoreError as exc:
        raise RuntimeError(
            f"AWS error while waiting for instance '{instance_id}' to reach "
            f"running state in region '{normalized_region}': {exc}"
        ) from exc

    return instance_id


def get_ondemand_g_vcpu_quota(
    region: str = "us-east-1", service_quotas_client: Any = None
) -> int:
    """Return the On-Demand G/VT vCPU quota for an AWS region.

    Parameters
    ----------
    region : str, default="us-east-1"
        AWS region to query.
    service_quotas_client : Any, optional
        Boto3 Service Quotas client (or compatible test double). When ``None``,
        a client is created from ``boto3``.

    Returns
    -------
    int
        Configured On-Demand G/VT vCPU quota for the region.

    Raises
    ------
    RuntimeError
        If the matching quota is missing or has no value.
    """
    client = service_quotas_client or boto3.client("service-quotas", region_name=region)
    paginator = client.get_paginator("list_service_quotas")

    for page in paginator.paginate(ServiceCode="ec2"):
        for quota in page.get("Quotas", []):
            name = quota.get("QuotaName", "")
            if _is_ondemand_g_quota_name(name):
                value = quota.get("Value")
                if value is None:
                    raise RuntimeError(
                        f"Quota value missing for '{name}' in region {region}."
                    )
                return int(value)

    raise RuntimeError(
        f"No EC2 On-Demand G/VT instance quota found in region {region}."
    )


def get_ondemand_g_vcpus_used(region: str = "us-east-1", ec2_client: Any = None) -> int:
    """Return running On-Demand G/VT vCPUs currently in use.

    Parameters
    ----------
    region : str, default="us-east-1"
        AWS region to query.
    ec2_client : Any, optional
        Boto3 EC2 client (or compatible test double). When ``None``,
        a client is created from ``boto3``.

    Returns
    -------
    int
        Total running On-Demand G/VT vCPUs.

    Raises
    ------
    RuntimeError
        If vCPU metadata cannot be resolved for one or more discovered
        instance types.
    """
    ec2 = ec2_client or boto3.client("ec2", region_name=region)

    paginator = ec2.get_paginator("describe_instances")
    pages = paginator.paginate(
        Filters=[{"Name": "instance-state-name", "Values": ["running"]}]
    )
    instance_types = _extract_running_ondemand_g_instance_types(pages)

    if not instance_types:
        return 0

    vcpus_by_type = _resolve_vcpus_by_instance_type(ec2, instance_types)
    unique_types = sorted(set(instance_types))

    missing_types = [itype for itype in unique_types if itype not in vcpus_by_type]
    if missing_types:
        raise RuntimeError(
            f"Unable to resolve vCPU counts for instance types: {missing_types}"
        )

    running_vcpus = sum(vcpus_by_type[itype] for itype in instance_types)
    return running_vcpus
