"""Load the normalised asset/owner frame into DuckDB or MotherDuck and build the marts.

The loader publishes three objects inside ``MOTHERDUCK_SCHEMA``:

``stg_asset_owners``
    Staging table holding the ``transform_assets()`` frame as-is (facility x owner grain).
``mart_corporate_emissions``
    Aggregated attributed emissions per company, country, sector and year.
``mart_company_assets_detail``
    Facility-level breakdown per company, including geography and ownership percentage.

Every run replaces the staging table and both marts, so the publication is idempotent and the
marts never mix two extraction windows. Connections are opened against MotherDuck when
``MOTHERDUCK_TOKEN`` is configured and against an in-memory DuckDB otherwise, which keeps the
whole path unit-testable with ``duckdb.connect(":memory:")``.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb
import pandas as pd
from loguru import logger

from climate_trace_etl.config import Settings, get_settings

#: Default schema for every published object.
DEFAULT_SCHEMA = "main"

#: Staging table with one row per (facility, owner) pair.
ASSET_OWNER_TABLE = "stg_asset_owners"

#: Aggregated company-level mart.
MART_CORPORATE_EMISSIONS = "mart_corporate_emissions"

#: Facility-level drill-down mart.
MART_COMPANY_ASSETS_DETAIL = "mart_company_assets_detail"


def quote_identifier(name: str) -> str:
    """Quote a SQL identifier, escaping embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


def mask_dsn(dsn: str) -> str:
    """Return a connection string without its secret token, safe for logs."""
    if "?" not in dsn:
        return dsn
    return f"{dsn.split('?', 1)[0]}?motherduck_token=***"


@dataclass(frozen=True)
class LoadSummary:
    """Row counts produced by one publication."""

    staging_rows: int
    corporate_rows: int
    detail_rows: int

    def as_dict(self) -> dict[str, int]:
        """Flat representation, handy for logging and tests."""
        return {
            "staging_rows": self.staging_rows,
            "corporate_rows": self.corporate_rows,
            "detail_rows": self.detail_rows,
        }


def connect(
    settings: Settings | None = None,
    *,
    database: str | None = None,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection to MotherDuck, or to an in-memory database.

    Args:
        settings: Configuration to use; defaults to
            :func:`climate_trace_etl.config.get_settings`.
        database: Explicit DuckDB/MotherDuck connection string, overriding the configuration.
        read_only: Forwarded to ``duckdb.connect``.

    Returns:
        An open :class:`duckdb.DuckDBPyConnection`. Callers close it (or use it as a context
        manager) when done.
    """
    current = settings or get_settings()
    if database is not None:
        target = database
    elif current.motherduck_configured:
        target = current.motherduck_dsn
    else:
        logger.warning(
            "MOTHERDUCK_TOKEN is not set: loading into a temporary in-memory DuckDB instead"
        )
        target = ":memory:"

    logger.info("opening DuckDB connection to {}", mask_dsn(target))
    connection = duckdb.connect(target, read_only=read_only)
    if target.startswith("md:"):
        logger.debug("MotherDuck session established for database {}", current.motherduck_database)
    return connection


def ensure_schema(connection: duckdb.DuckDBPyConnection, schema: str = DEFAULT_SCHEMA) -> None:
    """Create ``schema`` when it does not exist yet."""
    connection.execute(f"create schema if not exists {quote_identifier(schema)}")


def load_asset_owners(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
    *,
    schema: str = DEFAULT_SCHEMA,
    table: str = ASSET_OWNER_TABLE,
) -> int:
    """Replace ``table`` with the contents of ``frame`` and return the number of loaded rows.

    The staging table is fully replaced on every run, which makes repeated executions
    idempotent and guarantees that the marts never mix two extraction windows.
    """
    ensure_schema(connection, schema)
    qualified = f"{quote_identifier(schema)}.{quote_identifier(table)}"
    registered = "asset_owner_frame"

    connection.register(registered, frame)
    try:
        connection.execute(
            f"create or replace table {qualified} as select * from {quote_identifier(registered)}"
        )
        rows = count_rows(connection, schema, table)
    finally:
        connection.unregister(registered)

    logger.info("loaded {} row(s) into {}", rows, qualified)
    return rows


def count_rows(
    connection: duckdb.DuckDBPyConnection,
    schema: str = DEFAULT_SCHEMA,
    table: str = ASSET_OWNER_TABLE,
) -> int:
    """Number of rows currently stored in ``schema.table``."""
    qualified = f"{quote_identifier(schema)}.{quote_identifier(table)}"
    result = connection.execute(f"select count(*) from {qualified}").fetchone()
    return 0 if result is None else int(result[0])


def corporate_emissions_sql(
    *,
    schema: str = DEFAULT_SCHEMA,
    source_table: str = ASSET_OWNER_TABLE,
    target_table: str = MART_CORPORATE_EMISSIONS,
) -> str:
    """SQL that (re)creates :data:`MART_CORPORATE_EMISSIONS`.

    ``attributed_emissions_tco2e`` is the sum of the per-owner attributed emissions, so it never
    double counts a facility that has several owners.
    """
    source = f"{quote_identifier(schema)}.{quote_identifier(source_table)}"
    target = f"{quote_identifier(schema)}.{quote_identifier(target_table)}"
    return f"""
        create or replace table {target} as
        select
            company_id,
            company_name,
            coalesce(country, 'Unknown') as country,
            coalesce(sector, 'Unknown') as sector,
            reporting_year as year,
            count(distinct source_id) as facility_count,
            sum(attributed_emissions_tco2e) as attributed_emissions_tco2e,
            avg(ownership_share) as avg_ownership_share,
            bool_or(ownership_share_is_estimated) as ownership_share_is_estimated,
            (company_id is null) as is_state_or_unmapped_owner
        from {source}
        group by all
        order by attributed_emissions_tco2e desc
    """


def company_assets_detail_sql(
    *,
    schema: str = DEFAULT_SCHEMA,
    source_table: str = ASSET_OWNER_TABLE,
    target_table: str = MART_COMPANY_ASSETS_DETAIL,
) -> str:
    """SQL that (re)creates :data:`MART_COMPANY_ASSETS_DETAIL` at facility x owner grain."""
    source = f"{quote_identifier(schema)}.{quote_identifier(source_table)}"
    target = f"{quote_identifier(schema)}.{quote_identifier(target_table)}"
    return f"""
        create or replace table {target} as
        select
            source_id,
            facility_name,
            company_id,
            company_name,
            country,
            sector,
            subsector,
            asset_type,
            source_type,
            latitude,
            longitude,
            gas,
            reporting_year as year,
            owner_index,
            owner_count,
            ownership_share,
            ownership_share_is_estimated,
            total_emissions_tco2e,
            attributed_emissions_tco2e,
            emissions_reported,
            (company_id is null) as is_state_or_unmapped_owner
        from {source}
        order by attributed_emissions_tco2e desc
    """


def create_marts(
    connection: duckdb.DuckDBPyConnection,
    *,
    schema: str = DEFAULT_SCHEMA,
    source_table: str = ASSET_OWNER_TABLE,
) -> dict[str, int]:
    """(Re)create both presentation marts and return their row counts."""
    statements = {
        MART_CORPORATE_EMISSIONS: corporate_emissions_sql(
            schema=schema,
            source_table=source_table,
        ),
        MART_COMPANY_ASSETS_DETAIL: company_assets_detail_sql(
            schema=schema,
            source_table=source_table,
        ),
    }

    counts: dict[str, int] = {}
    for name, statement in statements.items():
        connection.execute(statement)
        counts[name] = count_rows(connection, schema, name)
        logger.info("built mart {}.{} with {} row(s)", schema, name, counts[name])
    return counts


def publish_frame(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
    *,
    schema: str = DEFAULT_SCHEMA,
) -> LoadSummary:
    """Publish one transformed frame: staging table first, then both marts.

    Args:
        connection: Open DuckDB/MotherDuck connection.
        frame: Frame produced by :func:`climate_trace_etl.transformer.transform_assets`.
        schema: Target schema for the staging table and the marts.

    Returns:
        Row counts of the staging table, the company mart and the detail mart.
    """
    staging_rows = load_asset_owners(connection, frame, schema=schema)
    counts = create_marts(connection, schema=schema)
    return LoadSummary(
        staging_rows=staging_rows,
        corporate_rows=counts[MART_CORPORATE_EMISSIONS],
        detail_rows=counts[MART_COMPANY_ASSETS_DETAIL],
    )
