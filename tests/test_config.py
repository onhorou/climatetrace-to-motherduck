"""Tests for :mod:`climate_trace_etl.config`."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import SecretStr, ValidationError

from climate_trace_etl.config import ConfigurationError, Settings, get_settings


def make_settings(**overrides: Any) -> Settings:
    """Build settings without touching a developer's local ``.env`` file."""
    return Settings(_env_file=None, **overrides)


def test_documented_defaults_are_applied() -> None:
    settings = make_settings()

    assert settings.api_base_url == "https://api.climatetrace.org/v7"
    assert settings.fetch_limit == 500
    assert settings.emissions_gas == "co2e_100yr"
    assert settings.emissions_year is None
    assert settings.request_timeout_seconds == 30.0
    assert settings.max_retries == 5
    assert settings.retry_backoff_seconds == 1.0
    assert settings.motherduck_database == "emissions_db"
    assert settings.motherduck_schema == "main"
    assert settings.motherduck_configured is False
    assert settings.environment == "local"
    assert settings.log_level == "INFO"
    assert settings.log_json is False


def test_environment_variables_override_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "md-token-123")
    monkeypatch.setenv("CLIMATE_TRACE_API_URL", "https://staging.api.climatetrace.org/v7/")
    monkeypatch.setenv("FETCH_LIMIT", "250")
    monkeypatch.setenv("EMISSIONS_YEAR", "2023")
    monkeypatch.setenv("LOG_JSON", "true")

    settings = Settings(_env_file=None)

    assert settings.api_base_url == "https://staging.api.climatetrace.org/v7"
    assert settings.fetch_limit == 250
    assert settings.emissions_year == 2023
    assert settings.log_json is True
    assert settings.motherduck_configured is True


def test_empty_environment_variables_fall_back_to_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """``FETCH_LIMIT=`` (as in .env.example) must not break parsing."""
    monkeypatch.setenv("FETCH_LIMIT", "")
    monkeypatch.setenv("EMISSIONS_YEAR", "")

    settings = Settings(_env_file=None)

    assert settings.fetch_limit == 500
    assert settings.emissions_year is None


def test_api_url_is_normalised() -> None:
    settings = make_settings(climate_trace_api_url="https://api.example.org/v7///")

    assert settings.api_base_url == "https://api.example.org/v7"


@pytest.mark.parametrize("raw_url", ["not-a-url", "ftp://api.climatetrace.org", ""])
def test_invalid_api_url_is_rejected(raw_url: str) -> None:
    with pytest.raises(ValidationError):
        make_settings(climate_trace_api_url=raw_url)


@pytest.mark.parametrize("fetch_limit", [0, -1, 10_001])
def test_fetch_limit_bounds_are_enforced(fetch_limit: int) -> None:
    with pytest.raises(ValidationError):
        make_settings(fetch_limit=fetch_limit)


@pytest.mark.parametrize(("raw_gas", "expected"), [(" CO2E_100YR ", "co2e_100yr"), ("Ch4", "ch4")])
def test_gas_is_normalised(raw_gas: str, expected: str) -> None:
    assert make_settings(emissions_gas=raw_gas).emissions_gas == expected


@pytest.mark.parametrize("invalid", [{"environment": "staging"}, {"log_level": "verbose"}])
def test_invalid_enum_values_are_rejected(invalid: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        make_settings(**invalid)


def test_motherduck_dsn_is_built_from_database_and_token() -> None:
    settings = make_settings(motherduck_token="secret-token", motherduck_database="analytics")

    assert settings.motherduck_dsn == "md:analytics?motherduck_token=secret-token"
    assert settings.motherduck_dsn_masked == "md:analytics?motherduck_token=***"


def test_motherduck_dsn_requires_a_token() -> None:
    settings = make_settings()

    with pytest.raises(ConfigurationError, match="MOTHERDUCK_TOKEN"):
        _ = settings.motherduck_dsn


def test_secrets_never_leak_into_repr_or_serialisation() -> None:
    settings = make_settings(motherduck_token="super-secret")

    assert isinstance(settings.motherduck_token, SecretStr)
    assert "super-secret" not in repr(settings)
    assert "super-secret" not in str(settings.motherduck_token)
    assert "super-secret" not in settings.model_dump_json()


def test_get_settings_is_cached_until_cache_is_cleared(monkeypatch: pytest.MonkeyPatch) -> None:
    first = get_settings()
    second = get_settings()

    assert first is second

    monkeypatch.setenv("FETCH_LIMIT", "999")
    get_settings.cache_clear()
    third = get_settings()

    assert third is not first
    assert third.fetch_limit == 999
