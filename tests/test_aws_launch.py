"""AWS launch policy tests independent of application persistence."""

import pytest

import benchmarking_aws.launch as launch


def test_capacity_retry_propagates_other_aws_errors(monkeypatch):
    """Do not retry credentials or other non-capacity failures."""
    error = RuntimeError("ExpiredToken")

    def fail_launch(*args, **kwargs):
        """Raise the original AWS failure for this test."""
        raise error

    def unexpected_sleep(seconds):
        """Fail if a non-capacity error enters the retry loop."""
        pytest.fail("Non-capacity error was retried")

    monkeypatch.setattr(launch, "_wait_for_ondemand_vcpu_quota", lambda *args: None)
    monkeypatch.setattr(launch, "_launch_ec2_instance", fail_launch)
    monkeypatch.setattr(launch._time, "sleep", unexpected_sleep)

    with pytest.raises(RuntimeError, match="ExpiredToken") as caught:
        launch.launch_with_capacity_retry(
            "task", "g5.xlarge", "ami-1234", "us-east-1", None, None, None
        )
    assert caught.value is error
