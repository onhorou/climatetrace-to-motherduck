"""Tests for :mod:`climate_trace_etl.logging_config`."""

from __future__ import annotations

import io
import json
import logging
from typing import Any

from loguru import logger

from climate_trace_etl.config import Settings
from climate_trace_etl.logging_config import setup_logging


def build_settings(**overrides: Any) -> Settings:
    """Settings detached from any developer ``.env`` file."""
    return Settings(_env_file=None, **overrides)


class FakeTty(io.StringIO):
    """In-memory sink that reports itself as an interactive terminal."""

    def isatty(self) -> bool:
        return True


def test_records_are_written_with_level_and_message() -> None:
    stream = io.StringIO()

    setup_logging(level="INFO", json_logs=False, sink=stream, settings=build_settings())
    logger.info("pipeline started")

    output = stream.getvalue()
    assert "pipeline started" in output
    assert "INFO" in output


def test_non_interactive_sink_has_no_ansi_escapes() -> None:
    stream = io.StringIO()

    setup_logging(level="INFO", json_logs=False, sink=stream, settings=build_settings())
    logger.info("colourless")

    assert "\x1b[" not in stream.getvalue()


def test_configured_level_filters_lower_levels() -> None:
    stream = io.StringIO()

    setup_logging(level="WARNING", json_logs=False, sink=stream, settings=build_settings())
    logger.info("ignored record")
    logger.error("kept record")

    output = stream.getvalue()
    assert "ignored record" not in output
    assert "kept record" in output


def test_settings_log_level_is_used_when_no_override_is_given() -> None:
    stream = io.StringIO()
    settings = build_settings(log_level="DEBUG")

    returned = setup_logging(sink=stream, settings=settings)
    logger.debug("debug is visible")

    assert returned is settings
    assert "debug is visible" in stream.getvalue()


def test_json_logs_are_machine_readable() -> None:
    stream = io.StringIO()

    setup_logging(json_logs=True, sink=stream, settings=build_settings(log_json=True))
    logger.bind(run_id="run-42").info("loaded 10 rows")

    payload = json.loads(stream.getvalue().strip())
    assert payload["record"]["message"] == "loaded 10 rows"
    assert payload["text"].strip() == "loaded 10 rows"
    assert payload["record"]["level"]["name"] == "INFO"
    assert payload["record"]["extra"]["run_id"] == "run-42"


def test_json_logs_stay_ansi_free_on_a_tty() -> None:
    stream = FakeTty()

    setup_logging(json_logs=True, sink=stream, settings=build_settings(log_json=True))
    logger.info("parseable even in a terminal")

    raw = stream.getvalue()
    assert "\x1b[" not in raw
    assert json.loads(raw.strip())["record"]["message"] == "parseable even in a terminal"


def test_stdlib_logger_records_are_intercepted() -> None:
    stream = io.StringIO()

    setup_logging(level="INFO", json_logs=False, sink=stream, settings=build_settings())
    logging.getLogger("httpx").warning("retrying request after 503")

    assert "retrying request after 503" in stream.getvalue()


def test_stdout_is_the_default_sink(capsys: Any) -> None:
    setup_logging(level="INFO", json_logs=False, settings=build_settings())
    logger.info("goes to stdout by default")

    assert "goes to stdout by default" in capsys.readouterr().out
