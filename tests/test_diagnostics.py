"""Tests for :mod:`climate_trace_etl.diagnostics`.

The checks only read metadata, so every test runs against ``duckdb.connect(":memory:")`` with a
patched connection and never touches MotherDuck.
"""

from __future__ import annotations

from typing import Any

import duckdb
import pytest

from climate_trace_etl.config import Settings
from climate_trace_etl.diagnostics import EXIT_FAILURE, EXIT_OK, check, list_motherduck_databases


def make_settings(**overrides: Any) -> Settings:
    """Build settings without touching a developer's local ``.env`` file."""
    return Settings(_env_file=None, **overrides)


@pytest.fixture
def in_memory_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the diagnostics connect to an in-memory DuckDB that exposes a ``memory`` database."""
    real_connect = duckdb.connect

    def fake_connect(target: str, **kwargs: Any) -> duckdb.DuckDBPyConnection:
        return real_connect(":memory:")

    monkeypatch.setattr("climate_trace_etl.diagnostics.duckdb.connect", fake_connect)


def test_list_motherduck_databases_returns_the_visible_names(
    in_memory_connect: None,
) -> None:
    settings = make_settings(motherduck_token="secret-token")

    assert list_motherduck_databases(settings) == ["memory"]


def test_check_without_a_token_fails_closed() -> None:
    messages: list[str] = []

    assert check(make_settings(), echo=messages.append) == EXIT_FAILURE
    assert any("MOTHERDUCK_TOKEN is not set" in message for message in messages)


def test_check_lists_the_databases_and_confirms_the_target(
    in_memory_connect: None,
) -> None:
    settings = make_settings(motherduck_token="secret-token", motherduck_database="memory")
    messages: list[str] = []

    assert check(settings, echo=messages.append) == EXIT_OK
    assert "  - memory" in messages
    assert any("MOTHERDUCK_DATABASE=memory is visible" in message for message in messages)


def test_check_reports_an_invisible_database(in_memory_connect: None) -> None:
    settings = make_settings(motherduck_token="secret-token")
    messages: list[str] = []

    assert check(settings, echo=messages.append) == EXIT_FAILURE
    assert any("MOTHERDUCK_DATABASE=emissions_db is NOT visible" in message for message in messages)


def test_check_reports_a_connection_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_connect(target: str, **kwargs: Any) -> Any:
        raise duckdb.Error("Authentication failed: invalid token")

    monkeypatch.setattr("climate_trace_etl.diagnostics.duckdb.connect", fake_connect)
    settings = make_settings(motherduck_token="secret-token")
    messages: list[str] = []

    assert check(settings, echo=messages.append) == EXIT_FAILURE
    assert messages == ["could not connect to MotherDuck: Authentication failed: invalid token"]
