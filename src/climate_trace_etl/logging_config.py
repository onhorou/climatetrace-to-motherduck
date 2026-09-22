"""Structured logging setup built on :mod:`loguru`.

The pipeline logs through loguru only. Standard library loggers (``httpx``,
``httpcore``, ``duckdb``, ``warnings``) are intercepted so that a single sink and
a single formatter own the output. :func:`setup_logging` is idempotent and is
meant to be called once from the CLI entrypoint (and once per test).
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence
from typing import Any

from loguru import logger

from climate_trace_etl.config import LogLevel, Settings, get_settings

#: Human readable format: ``timestamp | level | module:function:line - message``.
CONSOLE_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)

#: Format used for JSON logs: the message only, because the serialised record already
#: carries the structured fields and the payload must stay free of ANSI escapes.
JSON_FORMAT = "{message}"

#: Third-party loggers that are chatty below WARNING level.
NOISY_LOGGERS: Sequence[str] = ("httpx", "httpcore", "urllib3", "duckdb", "asyncio")

#: Log levels for which third-party DEBUG/INFO noise is kept.
VERBOSE_LEVELS = frozenset({"TRACE", "DEBUG"})


class InterceptHandler(logging.Handler):
    """Forward standard library log records to loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        """Publish ``record`` through loguru, preserving level and source location."""
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Walk up the stack until we leave the logging module, so that
        # `{name}:{function}:{line}` points at the real caller.
        frame, depth = logging.currentframe(), 2
        while frame is not None and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def _should_colorize(sink: Any) -> bool:
    """Enable ANSI colours only for interactive terminals (never for files/buffers)."""
    try:
        return bool(sink.isatty())
    except (AttributeError, ValueError):  # closed or non-stream sinks
        return False


def setup_logging(
    level: LogLevel | str | None = None,
    *,
    json_logs: bool | None = None,
    sink: Any | None = None,
    settings: Settings | None = None,
) -> Settings:
    """Configure loguru as the single logging backend of the process.

    Args:
        level: Overrides ``LOG_LEVEL`` when provided.
        json_logs: Overrides ``LOG_JSON``; ``True`` emits one JSON object per line,
            which GitHub Actions and log aggregators can parse natively.
        sink: Destination of the records. Defaults to ``sys.stdout``; tests pass an
            :class:`io.StringIO`.
        settings: Pre-built settings object, mainly useful for tests.

    Returns:
        The :class:`~climate_trace_etl.config.Settings` object backing the configuration.
    """
    current_settings = settings or get_settings()
    resolved_level = str(level or current_settings.log_level).upper()
    serialise = current_settings.log_json if json_logs is None else json_logs
    target = sys.stdout if sink is None else sink

    logger.remove()
    logger.add(
        target,
        level=resolved_level,
        format=JSON_FORMAT if serialise else CONSOLE_FORMAT,
        serialize=serialise,
        colorize=_should_colorize(target) and not serialise,
        backtrace=resolved_level in VERBOSE_LEVELS,
        diagnose=False,  # never dump locals: they may hold the MotherDuck token
        enqueue=False,
        catch=True,
    )

    # Route stdlib logging and warnings into loguru as well.
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    logging.captureWarnings(True)  # -> "py.warnings" -> root logger -> loguru

    if resolved_level not in VERBOSE_LEVELS:
        for name in NOISY_LOGGERS:
            logging.getLogger(name).setLevel(logging.WARNING)

    return current_settings
