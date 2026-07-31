from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any


class BrevTransport:
    """Run the Brev CLI operations needed by controller orchestration."""

    def __init__(self, executable: str = "brev") -> None:
        """Configure the Brev executable.

        Parameters
        ----------
        executable : str, default="brev"
            Brev CLI executable name or path.
        """
        self.executable = executable

    def _run(self, *arguments: str) -> str:
        """Run one Brev command and return standard output.

        Parameters
        ----------
        *arguments : str
            Brev CLI arguments.

        Returns
        -------
        str
            Command standard output with surrounding whitespace removed.
        """
        try:
            result = subprocess.run(
                [self.executable, *arguments],
                capture_output=True,
                check=True,
                text=True,
            )
        except subprocess.CalledProcessError as error:
            if error.stderr:
                raise RuntimeError(f"{error}: {error.stderr.strip()}") from error
            raise
        return result.stdout.strip()

    def create(
        self,
        instance_name: str,
        instance_type: str,
        startup_script: Path,
    ) -> str:
        """Create one Brev instance and wait until it is ready.

        Parameters
        ----------
        instance_name : str
            Unique Brev instance name.
        instance_type : str
            Explicit Brev instance type.
        startup_script : Path
            Local startup script passed through Brev's ``@filepath`` syntax.

        Returns
        -------
        str
            Brev CLI standard output.
        """
        return self._run(
            "create",
            instance_name,
            "--type",
            instance_type,
            "--startup-script",
            f"@{startup_script}",
        )

    def copy(self, source: str | Path, destination: str | Path) -> str:
        """Copy a file or directory between the controller and an instance.

        Parameters
        ----------
        source : str | Path
            Local path or ``instance:/path`` source.
        destination : str | Path
            Local path or ``instance:/path`` destination.

        Returns
        -------
        str
            Brev CLI standard output.
        """
        return self._run("copy", str(source), str(destination))

    def exec(self, instance_name: str, command: str) -> str:
        """Execute one command non-interactively on an instance.

        Parameters
        ----------
        instance_name : str
            Brev instance name.
        command : str
            Command string interpreted by the remote shell.

        Returns
        -------
        str
            Remote command standard output.
        """
        return self._run("exec", instance_name, command)

    def inspect(self, instance_name: str) -> dict[str, Any] | None:
        """Inspect one instance from Brev's JSON instance listing.

        Parameters
        ----------
        instance_name : str
            Brev instance name.

        Returns
        -------
        dict[str, Any] | None
            Instance metadata, or ``None`` when the instance is absent.
        """
        payload = json.loads(self._run("ls", "--json"))
        return next(
            (
                instance
                for instance in payload.get("workspaces") or []
                if instance.get("name") == instance_name
            ),
            None,
        )

    def delete(self, instance_name: str) -> str:
        """Permanently delete one Brev instance.

        Parameters
        ----------
        instance_name : str
            Brev instance name or identifier.

        Returns
        -------
        str
            Brev CLI standard output.
        """
        return self._run("delete", instance_name)
