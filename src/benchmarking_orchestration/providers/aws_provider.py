from __future__ import annotations

from dataclasses import dataclass

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .. import aws as aws_helpers
from .base import LaunchSpec

DEFAULT_LAUNCH_AMI_ID = aws_helpers.DEFAULT_LAUNCH_AMI_ID


@dataclass
class AwsProvider:
    """AWS provider adapter for launch lifecycle operations."""

    name: str = "aws"

    def validate_spec(self, spec: LaunchSpec) -> None:
        """Validate launch specification fields against AWS.

        Parameters
        ----------
        spec : LaunchSpec
            Launch specification containing optional fields to validate.

        Raises
        ------
        ValueError
            If no launch fields are provided for validation.
        """
        if spec.instance_type is None and spec.ami_id is None:
            raise ValueError(
                "launch specification must include instance type and/or ami id."
            )

        if spec.instance_type is not None:
            aws_helpers.validate_launch_instance_type(
                spec.instance_type, region=spec.region
            )

        if spec.ami_id is not None:
            aws_helpers.validate_launch_ami(spec.ami_id, region=spec.region)

    def submit(self, spec: LaunchSpec) -> str:
        """Launch an EC2 instance for a validated specification.

        Parameters
        ----------
        spec : LaunchSpec
            Launch specification containing required AWS launch fields.

        Returns
        -------
        str
            Launched EC2 instance identifier.

        Raises
        ------
        ValueError
            If required AWS launch fields are missing.
        """
        if spec.instance_type is None:
            raise ValueError("launch specification must include instance type.")
        if spec.ami_id is None:
            raise ValueError("launch specification must include ami id.")

        return aws_helpers.launch_ec2_instance(
            spec.instance_type,
            ami_id=spec.ami_id,
            region=spec.region,
            user_data=spec.user_data,
            key_name=spec.key_name,
            instance_profile_name=spec.instance_profile_name,
        )

    def status(self, handle: str, region: str) -> str:
        """Return the current EC2 instance state.

        Parameters
        ----------
        handle : str
            EC2 instance identifier.
        region : str
            AWS region for the instance.

        Returns
        -------
        str
            EC2 instance state name.

        Raises
        ------
        RuntimeError
            If AWS request fails or does not include instance state.
        """
        normalized_handle = handle.strip()
        if not normalized_handle:
            raise ValueError("instance handle cannot be empty.")

        normalized_region = region.strip()
        if not normalized_region:
            raise ValueError("region cannot be empty.")

        ec2_client = boto3.client("ec2", region_name=normalized_region)
        try:
            response = ec2_client.describe_instances(InstanceIds=[normalized_handle])
        except ClientError as exc:
            error = exc.response.get("Error", {})
            code = error.get("Code", "")
            message = error.get("Message", str(exc))
            raise RuntimeError(
                f"AWS error while checking status for instance '{normalized_handle}' in region "
                f"'{normalized_region}': {code or message}"
            ) from exc
        except BotoCoreError as exc:
            raise RuntimeError(
                f"AWS error while checking status for instance '{normalized_handle}' in region "
                f"'{normalized_region}': {exc}"
            ) from exc

        reservations = response.get("Reservations", [])
        first_reservation = reservations[0] if reservations else {}
        instances = first_reservation.get("Instances", [])
        first_instance = instances[0] if instances else {}
        state = first_instance.get("State", {})
        state_name = state.get("Name") if isinstance(state, dict) else None
        if state_name is None:
            raise RuntimeError(
                f"AWS did not return status for instance '{normalized_handle}' in region "
                f"'{normalized_region}'."
            )

        return state_name

    def cancel(self, handle: str, region: str) -> None:
        """Terminate an EC2 instance.

        Parameters
        ----------
        handle : str
            EC2 instance identifier.
        region : str
            AWS region for the instance.

        Raises
        ------
        RuntimeError
            If AWS request to terminate the instance fails.
        """
        normalized_handle = handle.strip()
        if not normalized_handle:
            raise ValueError("instance handle cannot be empty.")

        normalized_region = region.strip()
        if not normalized_region:
            raise ValueError("region cannot be empty.")

        ec2_client = boto3.client("ec2", region_name=normalized_region)
        try:
            ec2_client.terminate_instances(InstanceIds=[normalized_handle])
        except ClientError as exc:
            error = exc.response.get("Error", {})
            code = error.get("Code", "")
            message = error.get("Message", str(exc))
            raise RuntimeError(
                f"AWS error while terminating instance '{normalized_handle}' in region "
                f"'{normalized_region}': {code or message}"
            ) from exc
        except BotoCoreError as exc:
            raise RuntimeError(
                f"AWS error while terminating instance '{normalized_handle}' in region "
                f"'{normalized_region}': {exc}"
            ) from exc
