"""Shared pytest fixtures for the Climate TRACE ETL test suite."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from loguru import logger

from climate_trace_etl.config import get_settings

#: Every environment variable the pipeline understands, used to isolate tests.
PIPELINE_ENV_VARS = (
    "MOTHERDUCK_TOKEN",
    "MOTHERDUCK_DATABASE",
    "MOTHERDUCK_SCHEMA",
    "CLIMATE_TRACE_API_URL",
    "FETCH_LIMIT",
    "EMISSIONS_GAS",
    "EMISSIONS_YEAR",
    "REQUEST_TIMEOUT_SECONDS",
    "MAX_RETRIES",
    "RETRY_BACKOFF_SECONDS",
    "ENVIRONMENT",
    "LOG_LEVEL",
    "LOG_JSON",
)


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Detach tests from the ambient environment and from the settings cache."""
    for variable in PIPELINE_ENV_VARS:
        monkeypatch.delenv(variable, raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
def reset_loguru() -> Iterator[None]:
    """Leave loguru in its pristine state after every test."""
    yield
    logger.remove()
