import boto3
import pytest
from botocore.stub import Stubber
from moto import mock_aws

from benchmarking_orchestration.aws import (
    _extract_running_ondemand_g_instance_types,
    _is_ondemand_g_or_vt_instance_type,
    _is_ondemand_g_quota_name,
    _resolve_vcpus_by_instance_type,
    get_ondemand_g_vcpu_quota,
    get_ondemand_g_vcpus_used,
)


@pytest.fixture(autouse=True)
def _aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


@pytest.fixture
def service_quotas_client():
    return boto3.client("service-quotas", region_name="us-east-1")


@pytest.fixture
def ec2_client():
    with mock_aws():
        yield boto3.client("ec2", region_name="us-east-1")


def _run_instance(ec2_client, instance_type: str, count: int = 1, spot: bool = False):
    kwargs = {
        "ImageId": "ami-12345678",
        "MinCount": count,
        "MaxCount": count,
        "InstanceType": instance_type,
    }
    if spot:
        kwargs["InstanceMarketOptions"] = {"MarketType": "spot"}
    ec2_client.run_instances(**kwargs)


class _MissingInstanceTypeMetadataClient:
    def __init__(self, ec2_client):
        self._ec2_client = ec2_client

    def get_paginator(self, name):
        return self._ec2_client.get_paginator(name)

    def describe_instance_types(self, InstanceTypes):
        return {"InstanceTypes": []}


def test_quota_name_match():
    assert _is_ondemand_g_quota_name("Running On-Demand G and VT instances")
    assert not _is_ondemand_g_quota_name(
        "Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances"
    )


def test_instance_type_match():
    assert _is_ondemand_g_or_vt_instance_type("g5.2xlarge")
    assert _is_ondemand_g_or_vt_instance_type("vt1.3xlarge")
    assert not _is_ondemand_g_or_vt_instance_type("c6a.large")


def test_extract_running_ondemand_g_instance_types():
    pages = [
        {
            "Reservations": [
                {
                    "Instances": [
                        {"InstanceType": "g5.xlarge"},
                        {"InstanceType": "vt1.3xlarge"},
                        {"InstanceType": "g4dn.xlarge", "InstanceLifecycle": "spot"},
                        {"InstanceType": "m6i.large"},
                    ]
                }
            ]
        }
    ]
    assert _extract_running_ondemand_g_instance_types(pages) == ["g5.xlarge", "vt1.3xlarge"]


def test_get_ondemand_g_vcpu_quota_returns_int(service_quotas_client):
    with Stubber(service_quotas_client) as stubber:
        stubber.add_response(
            "list_service_quotas",
            {
                "Quotas": [
                    {"QuotaName": "Running On-Demand Standard instances", "Value": 256.0},
                    {"QuotaName": "Running On-Demand G and VT instances", "Value": 16.0},
                ]
            },
            {"ServiceCode": "ec2"},
        )
        assert get_ondemand_g_vcpu_quota(service_quotas_client=service_quotas_client) == 16


def test_get_ondemand_g_vcpu_quota_raises_when_missing(service_quotas_client):
    with Stubber(service_quotas_client) as stubber:
        stubber.add_response(
            "list_service_quotas",
            {"Quotas": [{"QuotaName": "Running On-Demand Standard instances", "Value": 256.0}]},
            {"ServiceCode": "ec2"},
        )
        with pytest.raises(RuntimeError, match="No EC2 On-Demand G/VT instance quota found"):
            get_ondemand_g_vcpu_quota(service_quotas_client=service_quotas_client)


def test_get_ondemand_g_vcpu_quota_raises_when_value_is_missing(service_quotas_client):
    with Stubber(service_quotas_client) as stubber:
        stubber.add_response(
            "list_service_quotas",
            {"Quotas": [{"QuotaName": "Running On-Demand G and VT instances"}]},
            {"ServiceCode": "ec2"},
        )
        with pytest.raises(RuntimeError, match="Quota value missing"):
            get_ondemand_g_vcpu_quota(service_quotas_client=service_quotas_client)


def test_resolve_vcpus_by_instance_type(ec2_client):
    result = _resolve_vcpus_by_instance_type(ec2_client, ["g5.xlarge", "g4dn.xlarge", "g5.xlarge"])
    assert result["g5.xlarge"] > 0
    assert result["g4dn.xlarge"] > 0


def test_get_ondemand_g_vcpus_used_returns_int(ec2_client):
    _run_instance(ec2_client, "g5.xlarge", count=2)
    _run_instance(ec2_client, "vt1.3xlarge", count=1)
    _run_instance(ec2_client, "g4dn.xlarge", count=1, spot=True)
    _run_instance(ec2_client, "c6a.large", count=1)

    instance_types = ec2_client.describe_instance_types(
        InstanceTypes=["g5.xlarge", "vt1.3xlarge"]
    )["InstanceTypes"]
    vcpus = {item["InstanceType"]: item["VCpuInfo"]["DefaultVCpus"] for item in instance_types}
    expected = (2 * vcpus["g5.xlarge"]) + vcpus["vt1.3xlarge"]

    assert get_ondemand_g_vcpus_used(ec2_client=ec2_client) == expected


def test_get_ondemand_g_vcpus_used_returns_zero_when_no_matching_instances(ec2_client):
    _run_instance(ec2_client, "c6a.large", count=1)
    assert get_ondemand_g_vcpus_used(ec2_client=ec2_client) == 0


def test_get_ondemand_g_vcpus_used_raises_for_missing_instance_type_metadata(ec2_client):
    _run_instance(ec2_client, "g5.xlarge", count=1)
    client = _MissingInstanceTypeMetadataClient(ec2_client)
    with pytest.raises(RuntimeError, match="Unable to resolve vCPU counts"):
        get_ondemand_g_vcpus_used(ec2_client=client)
