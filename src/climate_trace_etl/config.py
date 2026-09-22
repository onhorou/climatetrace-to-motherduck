"""Typed, environment-driven configuration for the Climate TRACE -> MotherDuck pipeline.

Values are resolved with the following precedence:

1. real environment variables (for example ``MOTHERDUCK_TOKEN``),
2. a ``.env`` file stored next to ``pyproject.toml``,
3. the defaults declared on :class:`Settings`.

Always obtain configuration through :func:`get_settings` so the whole process
shares one validated settings object (and one dotenv read).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal, get_args

from pydantic import AnyHttpUrl, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

#: Repository root (``.../climatetrace-to-motherduck``), derived from this file's location.
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]

#: Location of the optional local dotenv file.
ENV_FILE: Path = PROJECT_ROOT / ".env"

#: Log levels understood by loguru.
LogLevel = Literal["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]

#: Every supported log level, handy for CLI ``choices``.
LOG_LEVELS: tuple[str, ...] = get_args(LogLevel)

#: Deployment targets used to switch environment-specific behaviour.
Environment = Literal["local", "ci", "prod"]

#: MotherDuck attach modes; ``single`` keeps automated runs out of the saved workspace, while
#: ``default`` omits the parameter and leaves the choice to the MotherDuck extension (workspace
#: mode). See
#: https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/attach-modes/
MotherDuckAttachMode = Literal["workspace", "single", "default"]


class ConfigurationError(RuntimeError):
    """Raised when the configuration is valid on its own but incomplete for the task at hand.

    Example: loading data into MotherDuck without ``MOTHERDUCK_TOKEN`` being set.
    """


class Settings(BaseSettings):
    """Every knob of the pipeline, validated at startup (fail fast)."""

    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_ignore_empty=True,
        extra="ignore",
        validate_default=True,
    )

    # ------------------------------------------------------------------ API ---
    climate_trace_api_url: AnyHttpUrl = AnyHttpUrl("https://api.climatetrace.org/v7")
    fetch_limit: int = Field(
        default=500,
        ge=1,
        le=10_000,
        description="Amount of records requested per paginated Climate TRACE API call.",
    )
    max_enrich_records: int = Field(
        default=500,
        ge=1,
        le=100_000,
        description=(
            "Maximum number of facilities enriched with ownership details through "
            "'GET /sources/:id'. Each enriched facility costs one extra API request."
        ),
    )
    enrich_workers: int = Field(
        default=8,
        ge=1,
        le=32,
        description=(
            "Worker threads used to enrich facilities concurrently. Set to 1 to run sequentially."
        ),
    )
    emissions_gas: str = Field(
        default="co2e_100yr",
        min_length=1,
        description="Gas reported by the API, e.g. co2e_100yr, co2, ch4, n2o.",
    )
    emissions_year: int | None = Field(
        default=None,
        ge=2021,
        le=2100,
        description="Reporting year to query; None makes the API return its latest available year.",
    )
    request_timeout_seconds: float = Field(
        default=30.0,
        gt=0,
        le=600,
        description="Timeout of a single HTTP request, in seconds.",
    )
    max_retries: int = Field(
        default=5,
        ge=0,
        le=10,
        description="Maximum number of retries for transient HTTP/network failures.",
    )
    retry_backoff_seconds: float = Field(
        default=1.0,
        gt=0,
        le=60,
        description="Base delay of the exponential retry backoff, in seconds.",
    )

    # ------------------------------------------------------------ MotherDuck ---
    motherduck_token: SecretStr | None = Field(
        default=None,
        description="MotherDuck read/write token; required only when loading to the cloud.",
    )
    motherduck_database: str = Field(default="emissions_db", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    motherduck_schema: str = Field(default="main", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    motherduck_attach_mode: MotherDuckAttachMode = Field(
        default="single",
        description=(
            "MotherDuck attach mode: 'single' keeps automated runs in a one-off session that "
            "neither reads nor pollutes the saved workspace, 'workspace' reuses the attachments "
            "of the MotherDuck UI. The pipeline only ever needs a single database."
        ),
    )

    # --------------------------------------------------------------- Runtime ---
    environment: Environment = "local"
    log_level: LogLevel = "INFO"
    log_json: bool = Field(
        default=False,
        description="Emit one JSON object per log line (used by GitHub Actions log ingestion).",
    )

    @field_validator("climate_trace_api_url", mode="after")
    @classmethod
    def _strip_trailing_slash(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        """Normalise the base URL so request paths can be appended deterministically."""
        return AnyHttpUrl(str(value).rstrip("/"))

    @field_validator("emissions_gas", mode="after")
    @classmethod
    def _normalise_gas(cls, value: str) -> str:
        """Accept ``" CO2E_100YR "`` and store ``"co2e_100yr"``."""
        return value.strip().lower()

    @property
    def api_base_url(self) -> str:
        """Plain string form of :attr:`climate_trace_api_url` without a trailing slash."""
        return str(self.climate_trace_api_url)

    @property
    def motherduck_configured(self) -> bool:
        """``True`` when a non-empty MotherDuck token is available."""
        return self.motherduck_token is not None and bool(self.motherduck_token.get_secret_value())

    def require_motherduck_token(self) -> str:
        """Return the plaintext MotherDuck token.

        Raises:
            ConfigurationError: if ``MOTHERDUCK_TOKEN`` is not configured.
        """
        token = self.motherduck_token
        if token is None or not token.get_secret_value():
            raise ConfigurationError(
                "MOTHERDUCK_TOKEN is not set: export it or add it to .env before loading data."
            )
        return token.get_secret_value()

    @property
    def motherduck_attach_mode_parameter(self) -> str:
        """``&attach_mode=…`` DSN fragment; empty when ``MOTHERDUCK_ATTACH_MODE=default``."""
        if self.motherduck_attach_mode == "default":
            return ""
        return f"&attach_mode={self.motherduck_attach_mode}"

    @property
    def motherduck_dsn(self) -> str:
        """MotherDuck connection string for :attr:`motherduck_database`.

        Example: ``md:emissions_db?motherduck_token=***&attach_mode=single``.

        Raises:
            ConfigurationError: if ``MOTHERDUCK_TOKEN`` is not configured.
        """
        return (
            f"md:{self.motherduck_database}"
            f"?motherduck_token={self.require_motherduck_token()}"
            f"{self.motherduck_attach_mode_parameter}"
        )

    @property
    def motherduck_dsn_masked(self) -> str:
        """Token-free connection string, safe to write to logs."""
        return (
            f"md:{self.motherduck_database}?motherduck_token=***"
            f"{self.motherduck_attach_mode_parameter}"
        )

    @property
    def motherduck_workspace_dsn(self) -> str:
        """``md:`` connection string without a database, used for account-level statements.

        MotherDuck only accepts ``create database`` statements on a workspace-mode ``md:``
        connection, so the loader uses this DSN to bootstrap a missing database. See
        :func:`climate_trace_etl.loader.ensure_motherduck_database`.

        Raises:
            ConfigurationError: if ``MOTHERDUCK_TOKEN`` is not configured.
        """
        return f"md:?motherduck_token={self.require_motherduck_token()}"

    @property
    def motherduck_workspace_dsn_masked(self) -> str:
        """Token-free ``md:`` connection string, safe to write to logs."""
        return "md:?motherduck_token=***"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton.

    The object is cached because reading and validating the environment is only
    meaningful once per process; tests may call ``get_settings.cache_clear()``.
    """
    return Settings()
