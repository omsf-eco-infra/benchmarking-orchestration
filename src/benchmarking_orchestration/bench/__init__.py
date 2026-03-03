from __future__ import annotations

import subprocess
from pathlib import Path


def clone_performance_benchmarks(
    repo_url: str,
    destination: str | Path = "performance_benchmarks",
) -> Path:
    """Clone the OpenFE performance benchmark scripts repository.

    Parameters
    ----------
    destination : str | pathlib.Path, default="performance_benchmarks"
        Local directory path where the repository should be cloned.
    repo_url : str, default=PERFORMANCE_BENCHMARKS_REPO_URL
        Git URL for the benchmark scripts repository.

    Returns
    -------
    pathlib.Path
        Absolute path to the cloned benchmark repository.

    Raises
    ------
    FileExistsError
        If ``destination`` already exists.
    RuntimeError
        If ``git`` is unavailable or cloning fails.
    """
    destination_path = Path(destination).expanduser().resolve()
    if destination_path.exists():
        raise FileExistsError(
            f"Destination '{destination_path}' already exists. "
            "Provide a new destination path."
        )

    destination_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        subprocess.run(
            ["git", "clone", repo_url, str(destination_path)],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Unable to run git clone because the 'git' executable was not found."
        ) from exc
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        raise RuntimeError(
            f"Unable to clone benchmark repository from '{repo_url}' to "
            f"'{destination_path}': {stderr or str(exc)}"
        ) from exc

    return destination_path
