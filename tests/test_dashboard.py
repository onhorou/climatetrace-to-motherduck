"""Tests for the Streamlit dashboard entrypoint (``streamlit_app.py``).

The pure helpers (DSN handling and SQL builders) are loaded through :mod:`importlib`, while the
rendered page is driven through ``st.testing.v1.AppTest`` against a local DuckDB snapshot whose
marts are built by the real loader. Both need the optional ``dashboard`` dependencies, which the
default ``poetry install`` already provides; the module is skipped when they are missing
(``poetry install --without dashboard``).
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import duckdb
import pandas as pd
import pytest

from climate_trace_etl.loader import publish_frame
from climate_trace_etl.transformer import transform_assets

MISSING_GROUP = "the dashboard group is not installed (poetry install --with dashboard)"

streamlit_testing = pytest.importorskip("streamlit.testing.v1", reason=MISSING_GROUP)
pytest.importorskip("plotly", reason=MISSING_GROUP)

APP_PATH = Path(__file__).resolve().parents[1] / "streamlit_app.py"
TITLE = "🌱 Корпоративные выбросы CO2e (Climate TRACE)"
EMPTY_SELECTION = "Под выбранные фильтры не попала ни одна компания."


@pytest.fixture(scope="module")
def dashboard() -> ModuleType:
    """``streamlit_app.py`` imported as a module, without rendering the page."""
    spec = importlib.util.spec_from_file_location("climate_trace_dashboard", APP_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _facility(
    source_id: int,
    *,
    owners: list[dict[str, str]] | None,
    emissions: float,
    country: str,
) -> dict[str, Any]:
    """Detail-style Climate TRACE v7 document owned by ``owners``."""
    return {
        "id": source_id,
        "name": f"Facility {source_id}",
        "sector": "power",
        "subsector": "electricity-generation",
        "country": country,
        "assetType": "plant",
        "sourceType": "point-source",
        "centroid": {"latitude": 51.0, "longitude": 7.0, "srid": 4326},
        "gas": "co2e_100yr",
        "year": 2024,
        "emissionsQuantity": emissions,
        "emissions": [{"year": 2024, "gas": "co2e_100yr", "emissionsQuantity": emissions}],
        "owners": owners,
    }


@pytest.fixture
def marts_db(tmp_path: Path) -> Path:
    """Local DuckDB snapshot holding both marts, built by the real loader.

    One facility belongs to Acme, one to nobody (state / unmapped) and one is shared 50/50 by
    Acme and Beta, so the KPI assertions cover both ownership paths.
    """
    documents = [
        _facility(
            1,
            owners=[{"id": "E1", "name": "Acme Energy"}],
            emissions=2_000_000.0,
            country="DEU",
        ),
        _facility(2, owners=None, emissions=1_000_000.0, country="FRA"),
        _facility(
            3,
            owners=[{"id": "E1", "name": "Acme Energy"}, {"id": "E2", "name": "Beta Power"}],
            emissions=1_000_000.0,
            country="DEU",
        ),
    ]

    path = tmp_path / "emissions.duckdb"
    with duckdb.connect(str(path)) as connection:
        publish_frame(connection, transform_assets(documents))
    return path


def _run_app(secrets: dict[str, Any] | None = None) -> Any:
    """Run ``streamlit_app.py`` with an explicit secret store, ignoring any local secrets file."""
    instance = streamlit_testing.AppTest.from_file(str(APP_PATH), default_timeout=30)
    instance.secrets.clear()
    instance.secrets.update(secrets or {})
    instance.run()
    return instance


@pytest.fixture
def app(marts_db: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """The dashboard rendered against the local mart snapshot."""
    monkeypatch.delenv("DASHBOARD_DUCKDB_PATH", raising=False)
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    yield _run_app(
        {"DASHBOARD_DUCKDB_PATH": str(marts_db), "MOTHERDUCK_SCHEMA": "main"},
    )


def test_dsn_is_masked_for_display(dashboard: ModuleType) -> None:
    dsn = dashboard.build_dsn(database="emissions_db", token="secret", attach_mode="single")
    assert dsn == "md:emissions_db?motherduck_token=secret&attach_mode=single"
    assert dashboard.mask_dsn(dsn) == "md:emissions_db?motherduck_token=***&attach_mode=single"


def test_default_attach_mode_is_not_sent(dashboard: ModuleType) -> None:
    dsn = dashboard.build_dsn(database="emissions_db", token="secret", attach_mode="default")
    assert dsn == "md:emissions_db?motherduck_token=secret"


def test_country_label_expands_iso_codes(dashboard: ModuleType) -> None:
    """ISO 3166-1 alpha-3 codes become ``Name (CODE)`` labels."""
    assert dashboard.country_label("DEU") == "Germany (DEU)"
    assert dashboard.country_label("deu") == "Germany (DEU)"
    assert dashboard.country_label("BRA") == "Brazil (BRA)"


def test_country_label_keeps_the_codes_it_cannot_resolve(dashboard: ModuleType) -> None:
    """The sentinel, the loader's placeholder and unknown codes fall back to the raw value."""
    assert dashboard.country_label(dashboard.ALL_COUNTRIES) == dashboard.ALL_COUNTRIES
    assert dashboard.country_label("Unknown") == "Unknown"
    assert dashboard.country_label("ZZZ") == "ZZZ"
    assert dashboard.country_label(None) is None


def test_country_labels_are_applied_to_a_frame(dashboard: ModuleType) -> None:
    frame = pd.DataFrame({"country": ["DEU", "Unknown"], "emissions_m_tons": [1.0, 2.0]})
    labelled = dashboard.with_country_labels(frame)
    assert labelled["country"].tolist() == ["Germany (DEU)", "Unknown"]
    assert frame["country"].tolist() == ["DEU", "Unknown"]  # the original frame is untouched


def test_where_clause_is_parameterised(dashboard: ModuleType) -> None:
    filters = dashboard.Filters(country="DEU", sectors=("power", "steel"), years=(2024, 2025))
    where, parameters = filters.where_clause()
    assert where == (
        "where is_state_or_unmapped_owner = FALSE and country = ?"
        " and sector in (?, ?) and year in (?, ?)"
    )
    assert parameters == ["DEU", "power", "steel", 2024, 2025]


def test_where_clause_is_empty_without_filters(dashboard: ModuleType) -> None:
    where, parameters = dashboard.Filters(exclude_state_owners=False).where_clause()
    assert where == ""
    assert parameters == []


def test_top_companies_query_aggregates_per_company(dashboard: ModuleType) -> None:
    sql, parameters = dashboard.top_companies_sql("main", dashboard.Filters(), 10)
    assert '"main"."mart_corporate_emissions"' in sql
    assert "group by company_name, country" in sql
    assert parameters == [10]


def test_detail_query_reads_the_facility_mart(dashboard: ModuleType) -> None:
    sql, parameters = dashboard.detail_sql("analytics", dashboard.Filters(), 5)
    assert '"analytics"."mart_company_assets_detail"' in sql
    assert parameters == [5]


def test_dashboard_renders_the_kpis(app: Any) -> None:
    assert not app.exception
    assert app.title[0].value == TITLE
    # Acme 2.5 Mt + Beta 0.5 Mt; the unmapped facility stays excluded and both are in Germany.
    assert [float(metric.value) for metric in app.metric] == [2.0, 3.0, 1.0, 2.0]
    assert len(app.tabs) == 3


def test_state_owned_emissions_can_be_included(app: Any) -> None:
    app.checkbox[0].set_value(False).run()
    assert not app.exception
    assert [float(metric.value) for metric in app.metric] == [3.0, 4.0, 2.0, 3.0]


def test_country_filter_restricts_the_selection(app: Any) -> None:
    app.selectbox[0].select("DEU").run()
    assert not app.exception
    assert app.selectbox[0].value == "DEU"
    assert [float(metric.value) for metric in app.metric] == [2.0, 3.0, 1.0, 2.0]


def test_country_dropdown_offers_full_names(app: Any) -> None:
    """The dropdown renders ``Germany (DEU)`` while the selection stays the bare ISO code."""
    assert "Germany (DEU)" in app.selectbox[0].options
    app.selectbox[0].select("DEU").run()
    assert not app.exception
    assert app.selectbox[0].value == "DEU"
    assert [float(metric.value) for metric in app.metric] == [2.0, 3.0, 1.0, 2.0]


def test_empty_selection_is_reported(app: Any) -> None:
    app.selectbox[0].select("FRA").run()
    assert not app.exception
    assert [warning.value for warning in app.warning] == [EMPTY_SELECTION]


def test_missing_marts_are_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = tmp_path / "empty.duckdb"
    duckdb.connect(str(snapshot)).close()

    monkeypatch.delenv("DASHBOARD_DUCKDB_PATH", raising=False)
    instance = _run_app({"DASHBOARD_DUCKDB_PATH": str(snapshot), "MOTHERDUCK_SCHEMA": "main"})

    assert not instance.exception
    assert any("mart_corporate_emissions" in error.value for error in instance.error)
    assert any("run-etl" in info.value for info in instance.info)


def test_missing_configuration_explains_the_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DASHBOARD_DUCKDB_PATH", raising=False)
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)

    # Empty strings count as "not configured", and passing them keeps a local secrets.toml
    # from leaking a real token into the test.
    instance = _run_app({"MOTHERDUCK_TOKEN": "", "DASHBOARD_DUCKDB_PATH": ""})

    assert not instance.exception
    assert instance.error
    assert "MOTHERDUCK_TOKEN" in instance.error[0].value
