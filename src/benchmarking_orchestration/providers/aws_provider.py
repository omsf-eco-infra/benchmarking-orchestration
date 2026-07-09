from __future__ import annotations

from dataclasses import dataclass

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .. import aws as aws_helpers
DEFAULT_LAUNCH_AMI_ID = aws_helpers.DEFAULT_LAUNCH_AMI_ID


@dataclass
class AwsProvider:
    """AWS provider adapter for launch lifecycle operations."""

    name: str = "aws"

    def submit(
        self,
        instance_type: str,
        ami_id: str,
        region: str,
        user_data: str | None,
        key_name: str | None,
        instance_profile_name: str | None,
    ) -> str:
        """Launch an EC2 instance.

        Parameters
        ----------
        instance_type : str
            EC2 instance type to launch.
        ami_id : str
            AMI identifier to use.
        region : str
            AWS region where the instance is launched.
        user_data : str | None
            Optional startup payload for the instance.
        key_name : str | None
            Optional EC2 SSH key pair name.
        instance_profile_name : str | None
            Optional IAM instance profile name.

        Returns
        -------
        str
            Launched EC2 instance identifier.

        """
        return aws_helpers.launch_ec2_instance(
            instance_type,
            ami_id=ami_id,
            region=region,
            user_data=user_data,
            key_name=key_name,
            instance_profile_name=instance_profile_name,
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
