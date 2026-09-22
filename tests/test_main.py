"""Tests for :mod:`climate_trace_etl.main`.

The pipeline is exercised with a mocked HTTP transport and an in-memory DuckDB connection, so
the orchestration, the CLI wiring and the exit codes are all verified without network access.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterator
from typing import Any

import duckdb
import httpx
import pytest
from loguru import logger

from climate_trace_etl.client import ClimateTraceClient
from climate_trace_etl.config import ConfigurationError, Settings
from climate_trace_etl.loader import LoadSummary
from climate_trace_etl.main import (
    EXIT_FAILURE,
    EXIT_OK,
    RunReport,
    build_parser,
    collect_documents,
    main,
    positive_int,
    run_pipeline,
)

LISTING: list[dict[str, Any]] = [
    {
        "id": 1,
        "name": "Facility One",
        "sector": "power",
        "country": "DEU",
        "gas": "co2e_100yr",
        "year": 2024,
        "emissionsQuantity": 100.0,
        "centroid": {"latitude": 52.0, "longitude": 13.0, "srid": 4326},
    },
    {
        "id": 2,
        "name": "Facility Two",
        "sector": "power",
        "country": "FRA",
        "gas": "co2e_100yr",
        "year": 2024,
        "emissionsQuantity": 60.0,
        "centroid": {"latitude": 48.0, "longitude": 2.0, "srid": 4326},
    },
]

DETAILS: dict[int, dict[str, Any]] = {
    1: {
        **LISTING[0],
        "owners": [{"id": "E1", "name": "Acme Energy"}],
        "emissions": [{"year": 2024, "gas": "co2e_100yr", "emissionsQuantity": 100.0}],
    },
    2: {
        **LISTING[1],
        "owners": None,
        "emissions": [{"year": 2024, "gas": "co2e_100yr", "emissionsQuantity": 60.0}],
    },
}


def api_handler(request: httpx.Request) -> httpx.Response:
    """Mock API serving the listing and the per-facility details."""
    path = request.url.path
    if path == "/v7/sources":
        return httpx.Response(200, json=LISTING)
    if path.startswith("/v7/sources/"):
        return httpx.Response(200, json=DETAILS[int(path.rsplit("/", 1)[-1])])
    return httpx.Response(404, json={"detail": "not found"})


def make_client(
    settings: Settings,
    handler: Callable[[httpx.Request], httpx.Response] = api_handler,
) -> ClimateTraceClient:
    """Client wired to a mock transport."""
    return ClimateTraceClient(settings, httpx.Client(transport=httpx.MockTransport(handler)))


@pytest.fixture
def settings() -> Settings:
    """Fast, deterministic settings for pipeline tests."""
    return Settings(
        _env_file=None,
        fetch_limit=10,
        max_enrich_records=10,
        enrich_workers=2,
        max_retries=0,
        retry_backoff_seconds=0.001,
    )


@pytest.fixture
def connection() -> Iterator[duckdb.DuckDBPyConnection]:
    """Fresh in-memory DuckDB connection."""
    with duckdb.connect(":memory:") as handle:
        yield handle


# ---------------------------------------------------------------------- parser ---
def test_positive_int_accepts_positive_numbers() -> None:
    assert positive_int("5") == 5


@pytest.mark.parametrize("value", ["0", "-3", "abc"])
def test_positive_int_rejects_invalid_values(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        positive_int(value)


def test_parser_defaults_come_from_the_settings() -> None:
    defaults = Settings(
        _env_file=None,
        max_enrich_records=7,
        enrich_workers=3,
        motherduck_schema="analytics",
        log_level="DEBUG",
    )

    arguments = build_parser(defaults).parse_args([])

    assert arguments.max_records is None
    assert arguments.max_enrich_records == 7
    assert arguments.workers == 3
    assert arguments.schema == "analytics"
    assert arguments.log_level == "DEBUG"
    assert arguments.dry_run is False


def test_cli_arguments_override_the_settings() -> None:
    arguments = build_parser(Settings(_env_file=None)).parse_args(
        [
            "--max-records",
            "5",
            "--max-enrich-records",
            "2",
            "--workers",
            "1",
            "--schema",
            "marts",
            "--dry-run",
            "--log-level",
            "warning",
        ]
    )

    assert arguments.max_records == 5
    assert arguments.max_enrich_records == 2
    assert arguments.workers == 1
    assert arguments.schema == "marts"
    assert arguments.dry_run is True
    assert arguments.log_level == "WARNING"


@pytest.mark.parametrize("value", ["0", "abc"])
def test_invalid_cli_values_exit_with_an_error(value: str) -> None:
    with pytest.raises(SystemExit):
        build_parser(Settings(_env_file=None)).parse_args(["--max-records", value])


# ------------------------------------------------------------ collect_documents ---
def test_collect_documents_merges_enriched_and_listing_documents(settings: Settings) -> None:
    with make_client(settings) as client:
        documents, enriched = collect_documents(client, settings)

    assert enriched == 2
    by_id = {document["id"]: document for document in documents}
    assert by_id[1]["owners"] == [{"id": "E1", "name": "Acme Energy"}]
    assert by_id[2]["owners"] is None


def test_collect_documents_tolerates_enrichment_failures(settings: Settings) -> None:
    def failing_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v7/sources":
            return httpx.Response(200, json=LISTING)
        return httpx.Response(500, json={"detail": "boom"})

    messages: list[str] = []
    sink_id = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        with make_client(settings, failing_handler) as client:
            documents, enriched = collect_documents(client, settings)
    finally:
        logger.remove(sink_id)

    assert enriched == 0
    assert len(documents) == 2
    assert all(document.get("owners") is None for document in documents)
    assert any("enrichment stopped early" in message for message in messages)


def test_collect_documents_handles_an_empty_listing(settings: Settings) -> None:
    def empty_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    with make_client(settings, empty_handler) as client:
        documents, enriched = collect_documents(client, settings)

    assert documents == []
    assert enriched == 0


# ---------------------------------------------------------------- run_pipeline ---
def test_run_pipeline_publishes_the_staging_table_and_both_marts(
    settings: Settings,
    connection: duckdb.DuckDBPyConnection,
) -> None:
    with make_client(settings) as client:
        report = run_pipeline(settings, client=client, connection=connection)

    assert report.dry_run is False
    assert report.extracted_facilities == 2
    assert report.enriched_facilities == 2
    assert report.owner_rows == 2
    assert report.requests == 3
    assert report.retries == 0
    assert report.load is not None
    assert report.load.as_dict() == {"staging_rows": 2, "corporate_rows": 2, "detail_rows": 2}

    rows = connection.execute(
        """
        select company_name, round(attributed_emissions_tco2e, 2)
        from mart_corporate_emissions
        order by company_name
        """
    ).fetchall()
    assert rows == [("Acme Energy", 100.0), ("State / Unmapped Owner", 60.0)]


def test_run_pipeline_dry_run_skips_every_write(settings: Settings) -> None:
    messages: list[str] = []
    sink_id = logger.add(messages.append, level="INFO", format="{message}")
    try:
        with make_client(settings) as client:
            report = run_pipeline(settings, client=client, dry_run=True)
    finally:
        logger.remove(sink_id)

    assert report.dry_run is True
    assert report.load is None
    assert report.owner_rows == 2
    assert "load" not in report.as_dict()
    assert any("dry run" in message for message in messages)


def test_run_pipeline_honours_the_record_caps(
    settings: Settings,
    connection: duckdb.DuckDBPyConnection,
) -> None:
    with make_client(settings) as client:
        report = run_pipeline(
            settings,
            client=client,
            connection=connection,
            max_records=1,
            max_enrich_records=1,
        )

    assert report.extracted_facilities == 1
    assert report.enriched_facilities == 1
    assert report.owner_rows == 1


def test_run_pipeline_targets_the_requested_schema(
    settings: Settings,
    connection: duckdb.DuckDBPyConnection,
) -> None:
    with make_client(settings) as client:
        report = run_pipeline(settings, client=client, connection=connection, schema="analytics")

    tables = {
        row[0]
        for row in connection.execute(
            "select table_name from information_schema.tables where table_schema = 'analytics'"
        ).fetchall()
    }

    assert report.load is not None
    assert tables == {
        "stg_asset_owners",
        "mart_corporate_emissions",
        "mart_company_assets_detail",
    }


# -------------------------------------------------------------------------- cli ---
def test_main_returns_the_success_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run_pipeline(config: Settings, **kwargs: Any) -> RunReport:
        captured.update(kwargs)
        return RunReport(
            extracted_facilities=1,
            enriched_facilities=1,
            owner_rows=2,
            duration_seconds=0.25,
            requests=3,
            retries=0,
            dry_run=bool(kwargs["dry_run"]),
        )

    monkeypatch.setattr("climate_trace_etl.main.run_pipeline", fake_run_pipeline)
    monkeypatch.setattr("climate_trace_etl.main.get_settings", lambda: settings)

    exit_code = main(
        [
            "--max-records",
            "5",
            "--max-enrich-records",
            "2",
            "--workers",
            "1",
            "--schema",
            "marts",
            "--dry-run",
            "--log-level",
            "debug",
        ]
    )

    assert exit_code == EXIT_OK
    assert captured["max_records"] == 5
    assert captured["max_enrich_records"] == 2
    assert captured["workers"] == 1
    assert captured["schema"] == "marts"
    assert captured["dry_run"] is True


def test_main_returns_the_failure_exit_code_for_pipeline_errors(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    def failing_pipeline(config: Settings, **kwargs: Any) -> RunReport:
        raise duckdb.Error("cannot open the database")

    monkeypatch.setattr("climate_trace_etl.main.run_pipeline", failing_pipeline)
    monkeypatch.setattr("climate_trace_etl.main.get_settings", lambda: settings)

    assert main(["--dry-run"]) == EXIT_FAILURE


def test_main_returns_the_failure_exit_code_for_configuration_errors(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    def failing_pipeline(config: Settings, **kwargs: Any) -> RunReport:
        raise ConfigurationError("MOTHERDUCK_TOKEN is not set")

    monkeypatch.setattr("climate_trace_etl.main.run_pipeline", failing_pipeline)
    monkeypatch.setattr("climate_trace_etl.main.get_settings", lambda: settings)

    assert main(["--dry-run"]) == EXIT_FAILURE


def test_report_as_dict_exposes_the_load_counts() -> None:
    report = RunReport(
        extracted_facilities=2,
        enriched_facilities=2,
        owner_rows=3,
        duration_seconds=1.234,
        requests=4,
        retries=1,
        load=LoadSummary(staging_rows=3, corporate_rows=2, detail_rows=3),
    )

    assert report.as_dict() == {
        "extracted_facilities": 2,
        "enriched_facilities": 2,
        "owner_rows": 3,
        "requests": 4,
        "retries": 1,
        "duration_seconds": 1.23,
        "dry_run": False,
        "staging_rows": 3,
        "corporate_rows": 2,
        "detail_rows": 3,
    }
