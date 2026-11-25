"""Brev readiness deadline policy tests without app dependencies."""

from __future__ import annotations

import subprocess
from typing import Any, cast

import pytest

import benchmarking_brev.readiness as readiness
from benchmarking_brev import BrevTransport


class _ReadinessTransport:
    """Record readiness transport calls and injectable timeout failures."""

    def __init__(
        self,
        instance: dict[str, Any] | None,
        inspect_error: Exception | None = None,
        exec_error: Exception | None = None,
    ) -> None:
        """Configure controlled readiness probes.

        Parameters
        ----------
        instance : dict[str, Any] | None
            Instance metadata returned by ``inspect``.
        inspect_error : Exception | None, optional
            Failure raised by ``inspect`` instead of returning metadata.
        exec_error : Exception | None, optional
            Failure raised by ``exec`` instead of succeeding.
        """
        self.instance = instance
        self.inspect_error = inspect_error
        self.exec_error = exec_error
        self.calls: list[tuple[str, float | None]] = []

    def inspect(
        self, instance_name: str, timeout: float | None = None
    ) -> dict[str, Any] | None:
        """Record and answer one readiness inspection.

        Parameters
        ----------
        instance_name : str
            Brev instance name.
        timeout : float | None, optional
            Remaining readiness budget for the call.

        Returns
        -------
        dict[str, Any] | None
            Configured instance metadata.
        """
        self.calls.append(("inspect", timeout))
        if self.inspect_error is not None:
            raise self.inspect_error
        return self.instance

    def exec(
        self, instance_name: str, command: str, timeout: float | None = None
    ) -> str:
        """Record and answer one readiness SSH probe.

        Parameters
        ----------
        instance_name : str
            Brev instance name.
        command : str
            Remote shell command.
        timeout : float | None, optional
            Remaining readiness budget for the call.

        Returns
        -------
        str
            Empty successful probe output.
        """
        self.calls.append(("exec", timeout))
        if self.exec_error is not None:
            raise self.exec_error
        return ""


def test_wait_for_instance_ready_recomputes_remaining_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass the reduced remaining deadline to each readiness transport call.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    transport = _ReadinessTransport(
        {
            "name": "ready-instance",
            "status": "RUNNING",
            "shell_status": "READY",
            "health_status": "HEALTHY",
        }
    )
    times = iter((0.0, 40.0, 42.0, 45.0))
    monkeypatch.setattr(readiness.time, "monotonic", lambda: next(times))
    monkeypatch.setattr(readiness.time, "sleep", lambda _seconds: None)

    instance = readiness.wait_for_instance_ready(
        cast(BrevTransport, transport), "ready-instance", 100.0
    )

    assert instance["name"] == "ready-instance"
    assert transport.calls == [("inspect", 100.0), ("exec", 58.0)]


def test_wait_for_instance_ready_rejects_success_after_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse a successful probe that finishes after the readiness deadline.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    transport = _ReadinessTransport(
        {
            "name": "ready-instance",
            "status": "RUNNING",
            "shell_status": "READY",
            "health_status": "HEALTHY",
        }
    )
    times = iter((0.0, 10.0, 20.0, 200.0))
    monkeypatch.setattr(readiness.time, "monotonic", lambda: next(times))
    sleep_calls: list[float] = []
    monkeypatch.setattr(readiness.time, "sleep", sleep_calls.append)

    with pytest.raises(RuntimeError, match="did not become SSH-ready within 100"):
        readiness.wait_for_instance_ready(
            cast(BrevTransport, transport), "ready-instance", 100.0
        )

    assert transport.calls == [("inspect", 100.0), ("exec", 80.0)]
    assert sleep_calls == []


def test_wait_for_instance_ready_exhausts_deadline_after_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop probing once the readiness deadline has been consumed.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    transport = _ReadinessTransport(
        {
            "name": "slow-instance",
            "status": "RUNNING",
            "shell_status": "NOT READY",
            "health_status": "UNHEALTHY",
        }
    )
    times = iter((0.0, 5.0, 11.0))
    monkeypatch.setattr(readiness.time, "monotonic", lambda: next(times))
    sleep_calls: list[float] = []
    monkeypatch.setattr(readiness.time, "sleep", sleep_calls.append)

    with pytest.raises(RuntimeError, match="shell_status='NOT READY'"):
        readiness.wait_for_instance_ready(
            cast(BrevTransport, transport), "slow-instance", 10.0
        )

    assert transport.calls == [("inspect", 10.0)]
    assert sleep_calls == [5.0]


def test_wait_for_instance_ready_reports_inspect_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convert an inspect transport timeout into a readiness error.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    transport = _ReadinessTransport(
        None,
        inspect_error=subprocess.TimeoutExpired(["brev", "ls", "--json"], 20.0),
    )
    times = iter((0.0,))
    monkeypatch.setattr(readiness.time, "monotonic", lambda: next(times))

    with pytest.raises(
        RuntimeError,
        match="readiness inspection exceeded the remaining 20-second deadline",
    ):
        readiness.wait_for_instance_ready(
            cast(BrevTransport, transport), "ready-instance", 20.0
        )

    assert transport.calls == [("inspect", 20.0)]


def test_wait_for_instance_ready_reports_ssh_probe_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convert an SSH probe transport timeout into a readiness error.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    transport = _ReadinessTransport(
        {
            "name": "ready-instance",
            "status": "RUNNING",
            "shell_status": "READY",
            "health_status": "HEALTHY",
        },
        exec_error=subprocess.TimeoutExpired(["brev", "exec"], 90.0),
    )
    times = iter((0.0, 10.0, 10.0))
    monkeypatch.setattr(readiness.time, "monotonic", lambda: next(times))

    with pytest.raises(
        RuntimeError,
        match="SSH/startup probe exceeded the remaining 90-second deadline",
    ):
        readiness.wait_for_instance_ready(
            cast(BrevTransport, transport), "ready-instance", 100.0
        )

    assert transport.calls == [("inspect", 100.0), ("exec", 90.0)]


def test_wait_for_instance_ready_shortens_sleep_after_slow_failed_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cap the next sleep by the budget a slow failed probe left behind.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    transport = _ReadinessTransport(
        {
            "name": "ready-instance",
            "status": "RUNNING",
            "shell_status": "READY",
            "health_status": "HEALTHY",
        },
        exec_error=RuntimeError("Connection closed by SSH gateway"),
    )
    times = iter((0.0, 0.0, 0.0, 99.0, 100.0))
    monkeypatch.setattr(readiness.time, "monotonic", lambda: next(times))
    sleep_calls: list[float] = []
    monkeypatch.setattr(readiness.time, "sleep", sleep_calls.append)

    with pytest.raises(RuntimeError, match="did not become SSH-ready within 100"):
        readiness.wait_for_instance_ready(
            cast(BrevTransport, transport), "ready-instance", 100.0
        )

    assert transport.calls == [("inspect", 100.0), ("exec", 100.0)]
    assert sleep_calls == [1.0]


def test_wait_for_instance_ready_stops_after_failed_probe_past_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise without sleeping when a failed probe finishes past the deadline.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    transport = _ReadinessTransport(
        {
            "name": "ready-instance",
            "status": "RUNNING",
            "shell_status": "READY",
            "health_status": "HEALTHY",
        },
        exec_error=RuntimeError("Connection closed by SSH gateway"),
    )
    times = iter((0.0, 0.0, 0.0, 101.0))
    monkeypatch.setattr(readiness.time, "monotonic", lambda: next(times))
    sleep_calls: list[float] = []
    monkeypatch.setattr(readiness.time, "sleep", sleep_calls.append)

    with pytest.raises(RuntimeError, match="did not become SSH-ready within 100"):
        readiness.wait_for_instance_ready(
            cast(BrevTransport, transport), "ready-instance", 100.0
        )

    assert transport.calls == [("inspect", 100.0), ("exec", 100.0)]
    assert sleep_calls == []


def test_wait_for_instance_ready_stops_when_inspect_exhausts_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip SSH probing when inspection consumes the whole budget.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    transport = _ReadinessTransport(
        {
            "name": "slow-instance",
            "status": "RUNNING",
            "shell_status": "NOT READY",
            "health_status": "UNHEALTHY",
        }
    )
    times = iter((0.0, 101.0))
    monkeypatch.setattr(readiness.time, "monotonic", lambda: next(times))
    sleep_calls: list[float] = []
    monkeypatch.setattr(readiness.time, "sleep", sleep_calls.append)

    with pytest.raises(RuntimeError, match="did not become SSH-ready within 100"):
        readiness.wait_for_instance_ready(
            cast(BrevTransport, transport), "slow-instance", 100.0
        )

    assert transport.calls == [("inspect", 100.0)]
    assert sleep_calls == []
