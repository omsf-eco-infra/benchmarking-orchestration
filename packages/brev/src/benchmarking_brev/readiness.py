"""Bounded Brev instance and startup readiness checks."""

from __future__ import annotations

import shlex
import subprocess
import time
from typing import Any

from .transport import BrevTransport

_REMOTE_STARTUP_MARKER = "workspace/startup-complete"
_INSTANCE_READY_POLL_INTERVAL_SECONDS = 5
_PROGRESS_INTERVAL_SECONDS = 30


def _instance_ready_timeout_message(
    instance_name: str,
    timeout_seconds: float,
    status: str,
    shell_status: str,
    health_status: str,
    last_probe_error: str | None,
) -> str:
    """Format the readiness deadline error for one Brev instance.

    Parameters
    ----------
    instance_name : str
        Created Brev instance name.
    timeout_seconds : float
        Maximum configured wait after ``brev create`` returns.
    status : str
        Last observed instance status.
    shell_status : str
        Last observed shell status.
    health_status : str
        Last observed health status.
    last_probe_error : str | None
        Most recent SSH/startup probe error, if any.

    Returns
    -------
    str
        Human-readable readiness timeout message.
    """
    probe_detail = (
        f" Last SSH/startup probe error: {last_probe_error}."
        if last_probe_error
        else ""
    )
    return (
        f"Brev instance '{instance_name}' did not become SSH-ready within "
        f"{timeout_seconds:g} seconds (status={status!r}, "
        f"shell_status={shell_status!r}, health_status={health_status!r})."
        f"{probe_detail}"
    )


def wait_for_instance_ready(
    transport: BrevTransport,
    instance_name: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Wait until Brev reports a healthy instance with a ready shell.

    Each inspection and SSH probe receives the remaining slice of the readiness
    deadline so a single slow transport call cannot extend the overall budget.

    Parameters
    ----------
    transport : BrevTransport
        Brev CLI transport.
    instance_name : str
        Created Brev instance name.
    timeout_seconds : float
        Maximum time to wait after ``brev create`` returns.

    Returns
    -------
    dict[str, Any]
        Ready Brev instance metadata.

    Raises
    ------
    RuntimeError
        If the instance disappears, stops, does not become SSH-ready, or a
        transport call exceeds the remaining readiness deadline.
    """
    now = time.monotonic()
    deadline = now + timeout_seconds
    next_progress = now
    previous_statuses: tuple[str, str, str] | None = None
    last_probe_error: str | None = None
    status = shell_status = health_status = ""
    while True:
        remaining = deadline - now
        if remaining <= 0:
            raise RuntimeError(
                _instance_ready_timeout_message(
                    instance_name,
                    timeout_seconds,
                    status,
                    shell_status,
                    health_status,
                    last_probe_error,
                )
            )
        try:
            instance = transport.inspect(instance_name, timeout=remaining)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"Brev instance '{instance_name}' readiness inspection exceeded "
                f"the remaining {remaining:g}-second deadline."
            ) from exc
        if instance is None:
            raise RuntimeError(f"Brev instance '{instance_name}' disappeared.")

        status = str(instance.get("status") or "").upper()
        shell_status = str(instance.get("shell_status") or "").upper()
        health_status = str(instance.get("health_status") or "").upper()
        statuses = status, shell_status, health_status
        now = time.monotonic()
        if statuses != previous_statuses or now >= next_progress:
            print(
                f"[brev] {instance_name}: waiting for SSH readiness "
                f"(status={status}, shell={shell_status}, health={health_status})",
                flush=True,
            )
            previous_statuses = statuses
            next_progress = now + _PROGRESS_INTERVAL_SECONDS
        if (
            status == "RUNNING"
            and shell_status == "READY"
            and health_status == "HEALTHY"
        ):
            now = time.monotonic()
            remaining = deadline - now
            if remaining <= 0:
                raise RuntimeError(
                    _instance_ready_timeout_message(
                        instance_name,
                        timeout_seconds,
                        status,
                        shell_status,
                        health_status,
                        last_probe_error,
                    )
                )
            try:
                transport.exec(
                    instance_name,
                    f"test -f {shlex.quote(_REMOTE_STARTUP_MARKER)}",
                    timeout=remaining,
                )
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError(
                    f"Brev instance '{instance_name}' SSH/startup probe exceeded "
                    f"the remaining {remaining:g}-second deadline."
                ) from exc
            except Exception as exc:
                last_probe_error = str(exc).splitlines()[-1]
                print(
                    f"[brev] {instance_name}: SSH/startup probe failed "
                    f"({last_probe_error}); retrying",
                    flush=True,
                )
                now = time.monotonic()
            else:
                now = time.monotonic()
                if deadline - now <= 0:
                    raise RuntimeError(
                        _instance_ready_timeout_message(
                            instance_name,
                            timeout_seconds,
                            status,
                            shell_status,
                            health_status,
                            last_probe_error,
                        )
                    )
                print(
                    f"[brev] {instance_name}: SSH connection and startup ready",
                    flush=True,
                )
                return instance
        if status in {"DELETING", "ERROR", "FAILED", "FAILURE", "STOPPED", "STOPPING"}:
            raise RuntimeError(
                f"Brev instance '{instance_name}' entered {status!r} before its "
                "shell became ready."
            )

        remaining = deadline - now
        if remaining <= 0:
            raise RuntimeError(
                _instance_ready_timeout_message(
                    instance_name,
                    timeout_seconds,
                    status,
                    shell_status,
                    health_status,
                    last_probe_error,
                )
            )
        time.sleep(min(_INSTANCE_READY_POLL_INTERVAL_SECONDS, remaining))
        now = time.monotonic()
