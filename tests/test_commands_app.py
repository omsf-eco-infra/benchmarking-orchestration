from benchmarking_orchestration.commands import create_app


def test_create_app_registers_provider_groups_and_aws_commands():
    app = create_app()

    assert "create" in list(app)
    assert "launch" in list(app)
    assert "worker" in list(app)

    assert "aws" in list(app["create"])
    assert "aws" in list(app["launch"])
    assert "aws" in list(app["worker"])
    assert "job" in list(app["worker"])
