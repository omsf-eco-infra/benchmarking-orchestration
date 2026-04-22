import pytest

from benchmarking_orchestration.providers import (
    AwsProvider,
    LaunchSpec,
    get_provider,
)
import benchmarking_orchestration.providers.aws_provider as aws_provider_module


def test_get_provider_defaults_to_aws():
    provider = get_provider()
    assert isinstance(provider, AwsProvider)
    assert provider.name == "aws"


def test_get_provider_rejects_unknown_provider():
    with pytest.raises(ValueError, match="Unsupported provider"):
        get_provider("modal")


def test_aws_provider_submit_delegates_to_launch_helper(monkeypatch):
    provider = AwsProvider()
    calls = []

    monkeypatch.setattr(
        aws_provider_module.aws_helpers,
        "launch_ec2_instance",
        lambda instance_type, ami_id, region, user_data, key_name, instance_profile_name: (
            calls.append(
                {
                    "instance_type": instance_type,
                    "ami_id": ami_id,
                    "region": region,
                    "user_data": user_data,
                    "key_name": key_name,
                    "instance_profile_name": instance_profile_name,
                }
            )
            or "i-1234567890abcdef0"
        ),
    )

    instance_id = provider.submit(
        LaunchSpec(
            instance_type="g5.xlarge",
            ami_id="ami-0123456789abcdef0",
            region="us-east-1",
            user_data="#cloud-config\n",
            key_name="my-key",
            instance_profile_name="my-profile",
        )
    )

    assert instance_id == "i-1234567890abcdef0"
    assert calls == [
        {
            "instance_type": "g5.xlarge",
            "ami_id": "ami-0123456789abcdef0",
            "region": "us-east-1",
            "user_data": "#cloud-config\n",
            "key_name": "my-key",
            "instance_profile_name": "my-profile",
        }
    ]
