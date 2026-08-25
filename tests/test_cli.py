from types import SimpleNamespace

from typer.testing import CliRunner

from job_monitor import cli
from job_monitor.cli import app
from job_monitor.pipeline import RunReport


def test_cli_help_uses_product_brand():
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "Job Radar TW" in result.stdout


def test_run_allows_chatgpt_summary_without_telegram(monkeypatch):
    settings = SimpleNamespace(
        telegram_bot_token=None,
        telegram_chat_id=None,
        daily_summary_max_matches=15,
    )
    monkeypatch.setattr(cli, "_load", lambda company: (settings, [], {}, None))

    async def fake_run_pipeline(*args, **kwargs):
        return RunReport(run_key="manual-test")

    monkeypatch.setattr(cli, "run_pipeline", fake_run_pipeline)

    result = CliRunner().invoke(app, ["run"])

    assert result.exit_code == 0
    assert '"run_key": "manual-test"' in result.stdout


def test_doctor_allows_database_only_without_telegram(monkeypatch):
    settings = SimpleNamespace(
        database_url="postgresql://example",
        telegram_bot_token=None,
        telegram_chat_id=None,
    )
    monkeypatch.setattr(cli, "_load", lambda: (settings, [], {}, None))
    monkeypatch.setattr(
        cli,
        "Storage",
        lambda database_url: SimpleNamespace(schema_status=lambda: []),
    )

    result = CliRunner().invoke(app, ["doctor"])

    assert result.exit_code == 0
    assert "Database connection and schema: ok" in result.stdout
