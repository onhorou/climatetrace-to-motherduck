"""Tests for :mod:`climate_trace_etl.loader`.

Everything runs against ``duckdb.connect(":memory:")``, so the SQL of both marts is verified
before anything is pushed to MotherDuck.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import duckdb
import pandas as pd
import pytest
from loguru import logger

from climate_trace_etl.config import ConfigurationError, Settings
from climate_trace_etl.loader import (
    ASSET_OWNER_TABLE,
    MART_COMPANY_ASSETS_DETAIL,
    MART_CORPORATE_EMISSIONS,
    connect,
    count_rows,
    create_database_sql,
    create_marts,
    ensure_motherduck_database,
    load_asset_owners,
    mask_dsn,
    publish_frame,
    quote_identifier,
)
from climate_trace_etl.transformer import UNMAPPED_OWNER_NAME, transform_assets


@pytest.fixture
def connection() -> Iterator[duckdb.DuckDBPyConnection]:
    """Fresh in-memory DuckDB connection per test."""
    with duckdb.connect(":memory:") as handle:
        yield handle


def facility(
    source_id: int,
    *,
    company: tuple[str, str] | None,
    emissions: float,
    country: str = "DEU",
    sector: str = "power",
) -> dict[str, Any]:
    """Detail-style document with a single owner (or none)."""
    owners = None if company is None else [{"id": company[0], "name": company[1]}]
    return {
        "id": source_id,
        "name": f"Facility {source_id}",
        "sector": sector,
        "subsector": "electricity-generation",
        "country": country,
        "assetType": "plant",
        "sourceType": "point-source",
        "centroid": {"latitude": 51.0 + source_id, "longitude": 7.0 + source_id, "srid": 4326},
        "gas": "co2e_100yr",
        "year": 2024,
        "emissionsQuantity": emissions,
        "emissions": [{"year": 2024, "gas": "co2e_100yr", "emissionsQuantity": emissions}],
        "owners": owners,
    }


@pytest.fixture
def frame() -> pd.DataFrame:
    """Two facilities: one owned by Acme, one state-owned; plus a 50/50 shared facility."""
    documents = [
        facility(1, company=("E1", "Acme Energy"), emissions=100.0, country="DEU"),
        facility(2, company=None, emissions=40.0, country="FRA"),
        {
            **facility(3, company=None, emissions=60.0, country="DEU"),
            "owners": [{"id": "E1", "name": "Acme Energy"}, {"id": "E2", "name": "Beta Power"}],
        },
    ]
    return transform_assets(documents)


def capture_connect_target(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Patch ``duckdb.connect`` so a test can inspect its target without a real connection."""
    captured: dict[str, str] = {}
    real_connect = duckdb.connect

    def fake_connect(target: str, **kwargs: Any) -> duckdb.DuckDBPyConnection:
        captured["target"] = target
        return real_connect(":memory:")

    monkeypatch.setattr("climate_trace_etl.loader.duckdb.connect", fake_connect)
    return captured


class MissingDatabaseError(duckdb.Error):
    """Stands in for the ``InvalidInputException`` MotherDuck raises for unknown databases."""


class BootstrapRecorder:
    """Stand-in for the short-lived ``md:`` connection that creates a missing database."""

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.closed = False

    def execute(self, sql: str) -> BootstrapRecorder:
        self.statements.append(sql)
        return self

    def __enter__(self) -> BootstrapRecorder:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.closed = True


def missing_database_error(database: str = "emissions_db") -> MissingDatabaseError:
    """Reproduce the MotherDuck message for a database the token cannot see."""
    return MissingDatabaseError(
        f"Failed to attach '{database}': no database/share named '{database}' found"
    )


def install_bootstrap_connect(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fail_after_create: bool = False,
) -> tuple[list[str], BootstrapRecorder]:
    """Patch ``duckdb.connect``: the attach fails once, the ``md:`` connection creates the database.

    Returns the connection targets in call order and the recorder used for the bootstrap
    connection.
    """
    real_connect = duckdb.connect
    targets: list[str] = []
    admin = BootstrapRecorder()
    state = {"created": False}

    def fake_connect(target: str, **kwargs: Any) -> Any:
        targets.append(target)
        if target.startswith("md:?"):
            state["created"] = not fail_after_create
            return admin
        if target.startswith("md:") and not state["created"]:
            raise missing_database_error()
        return real_connect(":memory:")

    monkeypatch.setattr("climate_trace_etl.loader.duckdb.connect", fake_connect)
    return targets, admin


# ------------------------------------------------------------------ connection ---
def test_quote_identifier_escapes_embedded_quotes() -> None:
    assert quote_identifier("plain") == '"plain"'
    assert quote_identifier('we"ird') == '"we""ird"'


def test_mask_dsn_hides_the_token() -> None:
    assert mask_dsn("md:db?motherduck_token=secret") == "md:db?motherduck_token=***"
    assert mask_dsn(":memory:") == ":memory:"


def test_mask_dsn_keeps_the_non_secret_parameters() -> None:
    masked = mask_dsn("md:db?motherduck_token=secret&attach_mode=single")

    assert masked == "md:db?motherduck_token=***&attach_mode=single"
    assert "secret" not in masked


def test_connect_falls_back_to_memory_without_a_token() -> None:
    messages: list[str] = []
    sink_id = logger.add(messages.append, level="WARNING", format="{message}")
    handle: duckdb.DuckDBPyConnection | None = None
    try:
        handle = connect(Settings(_env_file=None))
        assert handle.execute("select 1").fetchone() == (1,)
    finally:
        if handle is not None:
            handle.close()
        logger.remove(sink_id)

    assert any("in-memory DuckDB" in message for message in messages)


def test_connect_uses_the_motherduck_dsn_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = capture_connect_target(monkeypatch)
    settings = Settings(
        _env_file=None,
        motherduck_token="secret-token",
        motherduck_database="emissions_db",
    )
    messages: list[str] = []
    sink_id = logger.add(messages.append, level="DEBUG", format="{message}")
    try:
        handle = connect(settings)
        handle.close()
    finally:
        logger.remove(sink_id)

    assert captured["target"] == "md:emissions_db?motherduck_token=secret-token&attach_mode=single"
    assert all("secret-token" not in message for message in messages)


def test_connect_prefers_an_explicit_database(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = capture_connect_target(monkeypatch)
    settings = Settings(_env_file=None, motherduck_token="secret-token")

    handle = connect(settings, database=":memory:")
    handle.close()

    assert captured["target"] == ":memory:"


def test_connect_creates_a_missing_motherduck_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first attach fails with 'no database/share named', so the loader bootstraps it."""
    targets, admin = install_bootstrap_connect(monkeypatch)
    settings = Settings(_env_file=None, motherduck_token="secret-token")
    expected_dsn = "md:emissions_db?motherduck_token=secret-token&attach_mode=single"
    messages: list[str] = []
    sink_id = logger.add(messages.append, level="DEBUG", format="{message}")
    try:
        handle = connect(settings)
        handle.close()
    finally:
        logger.remove(sink_id)

    assert targets == [expected_dsn, "md:?motherduck_token=secret-token", expected_dsn]
    assert admin.statements == ['create database if not exists "emissions_db"']
    assert admin.closed is True
    assert any("creating it" in message for message in messages)
    assert any("MotherDuck database emissions_db is ready" in message for message in messages)
    assert all("secret-token" not in message for message in messages)


def test_connect_read_only_never_creates_the_database(monkeypatch: pytest.MonkeyPatch) -> None:
    targets, admin = install_bootstrap_connect(monkeypatch)
    settings = Settings(_env_file=None, motherduck_token="secret-token")

    with pytest.raises(ConfigurationError, match="check-motherduck"):
        connect(settings, read_only=True)

    assert targets == ["md:emissions_db?motherduck_token=secret-token&attach_mode=single"]
    assert admin.statements == []


def test_connect_reports_a_failed_bootstrap(monkeypatch: pytest.MonkeyPatch) -> None:
    """A token that cannot create the database produces an actionable ConfigurationError."""
    targets, admin = install_bootstrap_connect(monkeypatch, fail_after_create=True)
    settings = Settings(_env_file=None, motherduck_token="secret-token")

    with pytest.raises(ConfigurationError, match="MOTHERDUCK_DATABASE"):
        connect(settings)

    assert len(targets) == 3
    assert admin.statements == ['create database if not exists "emissions_db"']


def test_connect_propagates_unrelated_motherduck_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_connect(target: str, **kwargs: Any) -> Any:
        raise duckdb.Error("Authentication failed: invalid token")

    monkeypatch.setattr("climate_trace_etl.loader.duckdb.connect", fake_connect)
    settings = Settings(_env_file=None, motherduck_token="secret-token")

    with pytest.raises(duckdb.Error, match="Authentication failed"):
        connect(settings)


def test_ensure_motherduck_database_uses_the_workspace_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    targets: list[str] = []
    admin = BootstrapRecorder()

    def fake_connect(target: str, **kwargs: Any) -> Any:
        targets.append(target)
        return admin

    monkeypatch.setattr("climate_trace_etl.loader.duckdb.connect", fake_connect)
    settings = Settings(
        _env_file=None,
        motherduck_token="secret-token",
        motherduck_database="analytics",
    )

    assert ensure_motherduck_database(settings) == "analytics"
    assert targets == ["md:?motherduck_token=secret-token"]
    assert admin.statements == ['create database if not exists "analytics"']


def test_create_database_sql_quotes_the_identifier() -> None:
    assert create_database_sql("emissions_db") == 'create database if not exists "emissions_db"'
    assert create_database_sql('we"ird') == 'create database if not exists "we""ird"'


# --------------------------------------------------------------------- staging ---
def test_load_asset_owners_replaces_the_staging_table(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    assert load_asset_owners(connection, frame) == 4
    assert count_rows(connection, "main", ASSET_OWNER_TABLE) == 4

    smaller = transform_assets([facility(1, company=None, emissions=5.0)])
    assert load_asset_owners(connection, smaller) == 1
    assert count_rows(connection, "main", ASSET_OWNER_TABLE) == 1


def test_load_asset_owners_creates_the_target_schema(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    load_asset_owners(connection, frame, schema="analytics")

    tables = connection.execute(
        "select table_schema, table_name from information_schema.tables"
    ).fetchall()

    assert ("analytics", ASSET_OWNER_TABLE) in tables
    assert count_rows(connection, "analytics", ASSET_OWNER_TABLE) == 4


def test_load_asset_owners_handles_an_empty_frame(connection: duckdb.DuckDBPyConnection) -> None:
    assert load_asset_owners(connection, transform_assets([])) == 0
    assert count_rows(connection, "main", ASSET_OWNER_TABLE) == 0


# ----------------------------------------------------------------------- marts ---
def test_corporate_mart_aggregates_per_company_country_sector_year(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    load_asset_owners(connection, frame)
    create_marts(connection)

    rows = connection.execute(
        f"""
        select company_name, country, sector, year, facility_count,
               round(attributed_emissions_tco2e, 2) as attributed,
               round(avg_ownership_share, 2) as share,
               ownership_share_is_estimated, is_state_or_unmapped_owner
        from {MART_CORPORATE_EMISSIONS}
        order by attributed desc
        """
    ).fetchall()

    assert rows == [
        ("Acme Energy", "DEU", "power", 2024, 2, 130.00, 0.75, True, False),
        (UNMAPPED_OWNER_NAME, "FRA", "power", 2024, 1, 40.00, 1.00, True, True),
        ("Beta Power", "DEU", "power", 2024, 1, 30.00, 0.50, True, False),
    ]


def test_detail_mart_keeps_the_facility_grain_and_flags(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    publish_frame(connection, frame)

    rows = connection.execute(
        f"""
        select facility_name, company_name, year, ownership_share,
               is_state_or_unmapped_owner, latitude, longitude
        from {MART_COMPANY_ASSETS_DETAIL}
        order by attributed_emissions_tco2e desc
        """
    ).fetchall()

    assert len(rows) == 4
    assert rows[0] == ("Facility 1", "Acme Energy", 2024, 1.0, False, 52.0, 8.0)

    state_rows = [row for row in rows if row[4] is True]
    assert len(state_rows) == 1
    assert state_rows[0][1] == UNMAPPED_OWNER_NAME


def test_detail_mart_preserves_the_attributed_totals(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    publish_frame(connection, frame)

    total = connection.execute(
        f"select sum(attributed_emissions_tco2e) from {MART_COMPANY_ASSETS_DETAIL}"
    ).fetchone()

    assert total is not None
    assert total[0] == pytest.approx(200.0)


def test_active_companies_have_no_null_keys(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    publish_frame(connection, frame)

    broken = connection.execute(
        f"""
        select count(*) from {MART_CORPORATE_EMISSIONS}
        where not is_state_or_unmapped_owner
          and (company_id is null or company_name is null or year is null)
        """
    ).fetchone()

    assert broken is not None
    assert broken[0] == 0


def test_create_marts_is_idempotent(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    load_asset_owners(connection, frame)

    first = create_marts(connection)
    second = create_marts(connection)

    expected = {MART_CORPORATE_EMISSIONS: 3, MART_COMPANY_ASSETS_DETAIL: 4}
    assert first == expected
    assert second == expected


def test_publish_frame_returns_the_row_counts(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    summary = publish_frame(connection, frame)

    assert summary.staging_rows == 4
    assert summary.corporate_rows == 3
    assert summary.detail_rows == 4
    assert summary.as_dict()["detail_rows"] == 4
    assert count_rows(connection, "main", ASSET_OWNER_TABLE) == 4


def test_marts_handle_an_empty_frame(connection: duckdb.DuckDBPyConnection) -> None:
    summary = publish_frame(connection, transform_assets([]))

    assert summary.as_dict() == {"staging_rows": 0, "corporate_rows": 0, "detail_rows": 0}
    assert count_rows(connection, "main", MART_CORPORATE_EMISSIONS) == 0
    assert count_rows(connection, "main", MART_COMPANY_ASSETS_DETAIL) == 0


def test_mart_columns_are_stable(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    publish_frame(connection, frame)

    corporate = [
        row[0] for row in connection.execute(f"describe {MART_CORPORATE_EMISSIONS}").fetchall()
    ]
    detail = [
        row[0] for row in connection.execute(f"describe {MART_COMPANY_ASSETS_DETAIL}").fetchall()
    ]

    assert corporate == [
        "company_id",
        "company_name",
        "country",
        "sector",
        "year",
        "facility_count",
        "attributed_emissions_tco2e",
        "avg_ownership_share",
        "ownership_share_is_estimated",
        "is_state_or_unmapped_owner",
    ]
    assert detail == [
        "source_id",
        "facility_name",
        "company_id",
        "company_name",
        "country",
        "sector",
        "subsector",
        "asset_type",
        "source_type",
        "latitude",
        "longitude",
        "gas",
        "year",
        "owner_index",
        "owner_count",
        "ownership_share",
        "ownership_share_is_estimated",
        "total_emissions_tco2e",
        "attributed_emissions_tco2e",
        "emissions_reported",
        "is_state_or_unmapped_owner",
    ]
