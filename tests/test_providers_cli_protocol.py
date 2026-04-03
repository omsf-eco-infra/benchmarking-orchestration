from __future__ import annotations

from pathlib import Path

import benchmarking_orchestration.providers.cli_protocol as cli_protocol_module


def test_setup_task_status_db_uses_filename_when_db_path_is_provided(monkeypatch):
    captured: dict[str, Path] = {}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename: Path):
            captured["filename"] = filename
            return "db-from-file"

    monkeypatch.setattr(cli_protocol_module, "TaskStatusDB", _FakeTaskStatusDB)

    db = cli_protocol_module._setup_task_status_db(Path("custom.db"))

    assert db == "db-from-file"
    assert captured["filename"] == Path("custom.db")


def test_setup_task_status_db_uses_turso_env_when_available(monkeypatch):
    captured: dict[str, str] = {}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename: Path):
            raise AssertionError("from_filename should not be called")

        @classmethod
        def from_environment_variables(cls, db_url: str, auth_token: str):
            captured["db_url"] = db_url
            captured["auth_token"] = auth_token
            return "db-from-env"

    monkeypatch.setattr(cli_protocol_module, "TaskStatusDB", _FakeTaskStatusDB)
    monkeypatch.setenv("TURSO_DATABASE_URL", "libsql://example.turso.io")
    monkeypatch.setenv("TURSO_AUTH_TOKEN", "token")

    db = cli_protocol_module._setup_task_status_db(None)

    assert db == "db-from-env"
    assert captured == {
        "db_url": "libsql://example.turso.io",
        "auth_token": "token",
    }
