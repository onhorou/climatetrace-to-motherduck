"""CLI entrypoint that chains the client, the transformer and the loader.

``run-etl`` performs one extraction window:

1. ``GET /sources`` (paginated) to pick up the ranked facilities,
2. concurrent ``GET /sources/:id`` enrichment for their owners and emission time series,
3. normalisation into the flat (facility, owner) frame,
4. publication of the staging table and both MotherDuck marts.

Every operational knob can be overridden on the command line while the environment (or ``.env``)
provides the defaults. The process exits with ``0`` on success and ``1`` on any failure, so
GitHub Actions can react to it.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import duckdb
from loguru import logger

from climate_trace_etl.client import ClimateTraceClient
from climate_trace_etl.config import (
    LOG_LEVELS,
    ConfigurationError,
    Settings,
    get_settings,
)
from climate_trace_etl.loader import LoadSummary, connect, publish_frame
from climate_trace_etl.logging_config import setup_logging
from climate_trace_etl.transformer import transform_assets

#: Exit code for a successful run.
EXIT_OK = 0

#: Exit code for a failed run.
EXIT_FAILURE = 1

#: Number of rows shown when ``--dry-run`` previews the result.
DRY_RUN_PREVIEW_ROWS = 5


@dataclass(frozen=True)
class RunReport:
    """Outcome of one pipeline execution."""

    extracted_facilities: int
    enriched_facilities: int
    owner_rows: int
    duration_seconds: float
    requests: int
    retries: int
    load: LoadSummary | None = None
    dry_run: bool = False

    def as_dict(self) -> dict[str, Any]:
        """Flat representation used for the closing log line."""
        report: dict[str, Any] = {
            "extracted_facilities": self.extracted_facilities,
            "enriched_facilities": self.enriched_facilities,
            "owner_rows": self.owner_rows,
            "requests": self.requests,
            "retries": self.retries,
            "duration_seconds": round(self.duration_seconds, 2),
            "dry_run": self.dry_run,
        }
        if self.load is not None:
            report.update(self.load.as_dict())
        return report


def positive_int(value: str) -> int:
    """``argparse`` type for integer arguments that must be at least ``1``."""
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected an integer, got {value!r}") from error
    if number < 1:
        raise argparse.ArgumentTypeError(f"expected a value >= 1, got {number}")
    return number


def build_parser(defaults: Settings) -> argparse.ArgumentParser:
    """Build the ``run-etl`` parser; every default is taken from the settings."""
    parser = argparse.ArgumentParser(
        prog="run-etl",
        description=(
            "Extract Climate TRACE corporate emission assets, normalise ownership and "
            "publish the marts to MotherDuck."
        ),
    )
    parser.add_argument(
        "--max-records",
        type=positive_int,
        metavar="N",
        help="Maximum number of facilities to extract (default: unlimited).",
    )
    parser.add_argument(
        "--max-enrich-records",
        type=positive_int,
        default=defaults.max_enrich_records,
        metavar="N",
        help=(
            "Facilities enriched with owners through 'GET /sources/:id' "
            f"(default: {defaults.max_enrich_records})."
        ),
    )
    parser.add_argument(
        "--workers",
        type=positive_int,
        default=defaults.enrich_workers,
        metavar="N",
        help=f"Enrichment threads, 1 disables concurrency (default: {defaults.enrich_workers}).",
    )
    parser.add_argument(
        "--schema",
        default=defaults.motherduck_schema,
        metavar="NAME",
        help=(
            "Target schema for the staging table and the marts "
            f"(default: {defaults.motherduck_schema})."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and transform only: skip every write to MotherDuck.",
    )
    parser.add_argument(
        "--log-level",
        type=str.upper,
        choices=list(LOG_LEVELS),
        default=defaults.log_level,
        metavar="LEVEL",
        help=f"Log verbosity, one of {', '.join(LOG_LEVELS)} (default: {defaults.log_level}).",
    )
    return parser


def collect_documents(
    client: ClimateTraceClient,
    settings: Settings,
    *,
    max_records: int | None = None,
    max_enrich_records: int | None = None,
    workers: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Extract the facility listing and enrich it with ownership details.

    Enrichment failures are tolerated: facilities whose detail request fails keep their listing
    document and end up attributed to :data:`climate_trace_etl.transformer.UNMAPPED_OWNER_NAME`
    instead of aborting the run.

    Returns:
        The documents to transform and the number of successfully enriched facilities.
    """
    listing = client.fetch_sources(max_records=max_records)
    if not listing:
        return [], 0

    cap = settings.max_enrich_records if max_enrich_records is None else max_enrich_records
    source_ids = [record["id"] for record in listing if "id" in record]

    details: list[dict[str, Any]] = []
    try:
        details = list(client.iter_source_details(source_ids, max_records=cap, workers=workers))
    except Exception as error:  # noqa: BLE001 - one bad facility must not sink the run
        logger.warning("ownership enrichment stopped early ({}); continuing without it", error)

    enriched_ids = {document.get("id") for document in details}
    documents = [*details, *(record for record in listing if record.get("id") not in enriched_ids)]

    logger.info(
        "extracted {} facility document(s), enriched {} with ownership details",
        len(documents),
        len(details),
    )
    return documents, len(details)


def run_pipeline(
    settings: Settings,
    *,
    max_records: int | None = None,
    max_enrich_records: int | None = None,
    workers: int | None = None,
    schema: str | None = None,
    dry_run: bool = False,
    client: ClimateTraceClient | None = None,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> RunReport:
    """Run the extraction, transformation and loading of a single window.

    Args:
        settings: Validated configuration.
        max_records: Cap on the number of extracted facilities.
        max_enrich_records: Cap on the number of facilities enriched with ownership details.
        workers: Enrichment threads, overrides ``ENRICH_WORKERS``.
        schema: Target schema, overrides ``MOTHERDUCK_SCHEMA``.
        dry_run: Extract and transform only, without touching MotherDuck.
        client: Pre-built client (tests inject one with a mock transport).
        connection: Pre-built DuckDB connection (tests inject an in-memory database).

    Returns:
        A :class:`RunReport` describing what happened.
    """
    started = time.perf_counter()
    target_schema = schema or settings.motherduck_schema

    if client is not None:
        return _execute(
            client,
            settings,
            target_schema=target_schema,
            max_records=max_records,
            max_enrich_records=max_enrich_records,
            workers=workers,
            dry_run=dry_run,
            started=started,
            connection=connection,
        )

    with ClimateTraceClient(settings) as owned_client:
        return _execute(
            owned_client,
            settings,
            target_schema=target_schema,
            max_records=max_records,
            max_enrich_records=max_enrich_records,
            workers=workers,
            dry_run=dry_run,
            started=started,
            connection=connection,
        )


def _execute(
    client: ClimateTraceClient,
    settings: Settings,
    *,
    target_schema: str,
    max_records: int | None,
    max_enrich_records: int | None,
    workers: int | None,
    dry_run: bool,
    started: float,
    connection: duckdb.DuckDBPyConnection | None,
) -> RunReport:
    """Run every pipeline step against already-built collaborators."""
    documents, enriched = collect_documents(
        client,
        settings,
        max_records=max_records,
        max_enrich_records=max_enrich_records,
        workers=workers,
    )
    frame = transform_assets(documents, gas=settings.emissions_gas)

    load_summary: LoadSummary | None = None
    if dry_run:
        _log_dry_run(frame)
    elif connection is None:
        with connect(settings) as owned_connection:
            load_summary = publish_frame(owned_connection, frame, schema=target_schema)
    else:
        load_summary = publish_frame(connection, frame, schema=target_schema)

    report = RunReport(
        extracted_facilities=len(documents),
        enriched_facilities=enriched,
        owner_rows=len(frame),
        duration_seconds=time.perf_counter() - started,
        requests=client.request_count,
        retries=client.retry_count,
        load=load_summary,
        dry_run=dry_run,
    )
    logger.info("{}", " | ".join(f"{key}={value}" for key, value in report.as_dict().items()))
    return report


def _log_dry_run(frame: Any) -> None:
    """Log the most relevant rows of a dry run instead of loading them."""
    if frame.empty:
        logger.info("dry run: nothing to publish, the extraction returned no owner rows")
        return

    preview = frame.nlargest(DRY_RUN_PREVIEW_ROWS, "attributed_emissions_tco2e")[
        ["facility_name", "company_name", "country", "attributed_emissions_tco2e"]
    ]
    logger.info(
        "dry run: skipping the MotherDuck load, top rows would be:\n{}",
        preview.to_string(index=False),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Entrypoint of ``poetry run run-etl``; returns the process exit code."""
    settings = get_settings()
    arguments = build_parser(settings).parse_args(argv)
    setup_logging(level=arguments.log_level, settings=settings)

    logger.info(
        "starting Climate TRACE -> MotherDuck run (environment={}, api={}, database={})",
        settings.environment,
        settings.api_base_url,
        settings.motherduck_dsn_masked,
    )
    try:
        report = run_pipeline(
            settings,
            max_records=arguments.max_records,
            max_enrich_records=arguments.max_enrich_records,
            workers=arguments.workers,
            schema=arguments.schema,
            dry_run=arguments.dry_run,
        )
    except ConfigurationError as error:
        logger.error("configuration error: {}", error)
        return EXIT_FAILURE
    except Exception:  # noqa: BLE001 - turn any failure into a clean exit code for CI
        logger.exception("pipeline failed")
        return EXIT_FAILURE

    logger.success("pipeline completed in {:.2f}s", report.duration_seconds)
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover - manual invocation
    sys.exit(main())
