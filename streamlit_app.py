"""Streamlit dashboard for the Climate TRACE corporate emission marts.

The script is the entrypoint of the Streamlit Community Cloud deployment: it attaches the
MotherDuck database filled by ``run-etl`` and renders the company, sector and facility views of
``mart_corporate_emissions`` and ``mart_company_assets_detail``.

Configuration is resolved from Streamlit secrets first -- *App -> Settings -> Secrets* in
Streamlit Community Cloud -- and from the process environment afterwards, so
``streamlit run streamlit_app.py`` keeps working in a shell with the variables exported:

| Setting | Default | Purpose |
| --- | --- | --- |
| ``MOTHERDUCK_TOKEN`` | *(unset)* | Token used to attach MotherDuck (read access is enough). |
| ``MOTHERDUCK_DATABASE`` | ``emissions_db`` | Database that hosts the marts. |
| ``MOTHERDUCK_SCHEMA`` | ``main`` | Schema that hosts the marts. |
| ``MOTHERDUCK_ATTACH_MODE`` | ``single`` | ``single``, ``workspace`` or ``default``. |
| ``DASHBOARD_DUCKDB_PATH`` | *(unset)* | Render a local DuckDB snapshot instead of MotherDuck. |

Table and column names mirror :mod:`climate_trace_etl.loader`. The duplication is deliberate:
the dashboard stays a self-contained script, so Streamlit Community Cloud only has to install
``requirements.txt`` and never the ETL package itself.
"""

from __future__ import annotations

import os
import textwrap
from dataclasses import dataclass
from typing import Any

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

#: Company-level mart published by :mod:`climate_trace_etl.loader`.
MART_CORPORATE_EMISSIONS = "mart_corporate_emissions"

#: Facility-level mart published by :mod:`climate_trace_etl.loader`.
MART_COMPANY_ASSETS_DETAIL = "mart_company_assets_detail"

#: Defaults mirroring the pipeline settings (``config.py`` / ``.env.example``).
DEFAULT_DATABASE = "emissions_db"
DEFAULT_SCHEMA = "main"
DEFAULT_ATTACH_MODE = "single"

#: Secret / environment variable that previews a local DuckDB snapshot instead of MotherDuck.
LOCAL_DUCKDB_SETTING = "DASHBOARD_DUCKDB_PATH"

#: Divisor that turns tonnes of CO2e into millions of tonnes.
MILLION = 1_000_000.0

#: Rows rendered in the facility drill-down table.
DETAIL_TABLE_ROWS = 200

#: Seconds the filter dropdowns are cached for; a fresh ``run-etl`` is picked up afterwards.
FILTER_CACHE_SECONDS = 600

#: Section label shown while no specific country is selected.
ALL_COUNTRIES = "Все страны"

#: Plotly layout margin reused by every chart.
PLOTLY_MARGIN: dict[str, int] = {"l": 10, "r": 10, "t": 10, "b": 10}


def read_setting(name: str, default: str | None = None) -> str | None:
    """Resolve ``name`` from Streamlit secrets first, then from the process environment.

    Streamlit Community Cloud injects secrets through :data:`st.secrets`; the environment
    fallback keeps ``streamlit run streamlit_app.py`` usable with an exported ``MOTHERDUCK_TOKEN``.
    A missing ``secrets.toml`` raises :class:`FileNotFoundError`, which counts as "not set".
    """
    value: Any = None
    try:
        value = st.secrets.get(name)
    except (KeyError, FileNotFoundError):
        value = None
    if value is None or value == "":
        value = os.getenv(name)
    if value is None or value == "":
        return default
    return str(value)


def build_dsn(*, database: str, token: str, attach_mode: str = DEFAULT_ATTACH_MODE) -> str:
    """MotherDuck connection string for ``database``.

    ``attach_mode=default`` omits the parameter and leaves the choice to the MotherDuck
    extension; ``single`` keeps the dashboard out of the saved workspace.
    """
    dsn = f"md:{database}?motherduck_token={token}"
    if attach_mode and attach_mode.lower() != "default":
        dsn = f"{dsn}&attach_mode={attach_mode}"
    return dsn


def mask_dsn(dsn: str) -> str:
    """Token-free connection string, safe to render in the UI."""
    base, separator, query = dsn.partition("?")
    if not separator:
        return dsn

    parameters = [
        "motherduck_token=***" if parameter.startswith("motherduck_token=") else parameter
        for parameter in query.split("&")
        if parameter
    ]
    return f"{base}?{'&'.join(parameters)}"


@dataclass(frozen=True)
class ConnectionTarget:
    """Data source of one dashboard session."""

    dsn: str
    schema: str
    caption: str
    read_only: bool = False


def resolve_target() -> ConnectionTarget | None:
    """Build the connection target from the secrets/environment.

    Returns:
        The target to attach, or ``None`` when neither :data:`LOCAL_DUCKDB_SETTING` nor
        ``MOTHERDUCK_TOKEN`` is configured.
    """
    schema = read_setting("MOTHERDUCK_SCHEMA", DEFAULT_SCHEMA) or DEFAULT_SCHEMA

    local_path = read_setting(LOCAL_DUCKDB_SETTING)
    if local_path:
        return ConnectionTarget(
            dsn=local_path,
            schema=schema,
            caption=f"локальный DuckDB · {local_path}",
            read_only=True,
        )

    token = read_setting("MOTHERDUCK_TOKEN")
    if not token:
        return None

    database = read_setting("MOTHERDUCK_DATABASE", DEFAULT_DATABASE) or DEFAULT_DATABASE
    attach_mode = read_setting("MOTHERDUCK_ATTACH_MODE", DEFAULT_ATTACH_MODE) or DEFAULT_ATTACH_MODE
    dsn = build_dsn(database=database, token=token, attach_mode=attach_mode)
    return ConnectionTarget(dsn=dsn, schema=schema, caption=mask_dsn(dsn))


def quote_identifier(name: str) -> str:
    """Quote a SQL identifier, escaping embedded double quotes (mirrors the loader)."""
    return '"' + name.replace('"', '""') + '"'


def qualified(schema: str, table: str) -> str:
    """``schema.table`` with both parts quoted."""
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def _placeholders(amount: int) -> str:
    """``?, ?, ?`` for an ``in (...)`` list of ``amount`` values."""
    return ", ".join("?" * amount)


@dataclass(frozen=True)
class Filters:
    """Sidebar selection translated into a parameterised ``where`` clause."""

    country: str | None = None
    sectors: tuple[str, ...] = ()
    years: tuple[int, ...] = ()
    exclude_state_owners: bool = True

    def where_clause(self) -> tuple[str, list[Any]]:
        """``where …`` fragment plus its bind parameters; empty when nothing is filtered."""
        clauses: list[str] = []
        parameters: list[Any] = []

        if self.exclude_state_owners:
            clauses.append("is_state_or_unmapped_owner = FALSE")
        if self.country:
            clauses.append("country = ?")
            parameters.append(self.country)
        if self.sectors:
            clauses.append(f"sector in ({_placeholders(len(self.sectors))})")
            parameters.extend(self.sectors)
        if self.years:
            clauses.append(f"year in ({_placeholders(len(self.years))})")
            parameters.extend(self.years)

        if not clauses:
            return "", []
        return "where " + " and ".join(clauses), parameters


def kpi_sql(schema: str, filters: Filters) -> tuple[str, list[Any]]:
    """Companies, countries and attributed emissions of the current selection."""
    where, parameters = filters.where_clause()
    return (
        f"""
        select
            count(distinct company_name) as companies,
            count(distinct country) as countries,
            sum(attributed_emissions_tco2e) as attributed_emissions_tco2e
        from {qualified(schema, MART_CORPORATE_EMISSIONS)}
        {where}
        """,
        parameters,
    )


def facility_count_sql(schema: str, filters: Filters) -> tuple[str, list[Any]]:
    """Distinct facilities behind the current selection."""
    where, parameters = filters.where_clause()
    return (
        f"""
        select count(distinct source_id) as facilities
        from {qualified(schema, MART_COMPANY_ASSETS_DETAIL)}
        {where}
        """,
        parameters,
    )


def top_companies_sql(schema: str, filters: Filters, limit: int) -> tuple[str, list[Any]]:
    """Top ``limit`` companies by attributed emissions, labelled with their main country.

    The mart stores one row per company x country x sector x year, so the ranking aggregates per
    company first and keeps the country that contributes the most.
    """
    where, parameters = filters.where_clause()
    return (
        f"""
        with per_country as (
            select company_name, country, sum(attributed_emissions_tco2e) as emissions
            from {qualified(schema, MART_CORPORATE_EMISSIONS)}
            {where}
            group by company_name, country
        ),
        ranked as (
            select
                company_name,
                country,
                sum(emissions) over (partition by company_name) as company_emissions,
                row_number() over (
                    partition by company_name order by emissions desc, country
                ) as country_rank
            from per_country
        )
        select
            company_name,
            country,
            round(company_emissions / {MILLION}, 2) as emissions_m_tons
        from ranked
        where country_rank = 1
        order by company_emissions desc
        limit ?
        """,
        [*parameters, limit],
    )


def sector_sql(schema: str, filters: Filters) -> tuple[str, list[Any]]:
    """Attributed emissions per sector of the current selection."""
    where, parameters = filters.where_clause()
    return (
        f"""
        select
            sector,
            round(sum(attributed_emissions_tco2e) / {MILLION}, 2) as emissions_m_tons
        from {qualified(schema, MART_CORPORATE_EMISSIONS)}
        {where}
        group by sector
        order by emissions_m_tons desc
        """,
        parameters,
    )


def yearly_trend_sql(schema: str, filters: Filters) -> tuple[str, list[Any]]:
    """Attributed emissions per reporting year of the current selection."""
    where, parameters = filters.where_clause()
    return (
        f"""
        select
            year,
            round(sum(attributed_emissions_tco2e) / {MILLION}, 2) as emissions_m_tons
        from {qualified(schema, MART_CORPORATE_EMISSIONS)}
        {where}
        group by year
        order by year
        """,
        parameters,
    )


def detail_sql(schema: str, filters: Filters, limit: int) -> tuple[str, list[Any]]:
    """Facility drill-down of the current selection."""
    where, parameters = filters.where_clause()
    return (
        f"""
        select
            company_name,
            facility_name,
            country,
            sector,
            year,
            round(total_emissions_tco2e, 0) as total_tco2e,
            round(attributed_emissions_tco2e, 0) as attributed_tco2e,
            round(ownership_share, 2) as ownership_share
        from {qualified(schema, MART_COMPANY_ASSETS_DETAIL)}
        {where}
        order by attributed_emissions_tco2e desc
        limit ?
        """,
        [*parameters, limit],
    )


def run_query(
    connection: duckdb.DuckDBPyConnection,
    query: tuple[str, list[Any]],
) -> pd.DataFrame:
    """Execute a ``(sql, parameters)`` pair produced by the SQL builders."""
    sql, parameters = query
    return connection.execute(sql, parameters).df()


@st.cache_resource(show_spinner=False)
def get_connection(dsn: str, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Cache one connection per target, so interactions do not reconnect to MotherDuck.

    MotherDuck sessions are opened read/write (write-scaling tokens stay usable), while a local
    snapshot is attached read-only to keep dashboard browsing from modifying the file.
    """
    return duckdb.connect(dsn, read_only=read_only)


def missing_marts(connection: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
    """Marts that ``run-etl`` has not published to ``schema`` yet."""
    rows = connection.execute(
        "select table_name from information_schema.tables where table_schema = ?",
        [schema],
    ).fetchall()
    present = {str(row[0]) for row in rows}
    return [
        table
        for table in (MART_CORPORATE_EMISSIONS, MART_COMPANY_ASSETS_DETAIL)
        if table not in present
    ]


def _unique_values(frame: pd.DataFrame, kind: str) -> list[str]:
    """Sorted, non-empty values of one ``kind`` of the filter-option frame."""
    values = frame.loc[frame["kind"] == kind, "value"]
    return sorted({str(value) for value in values if pd.notna(value) and str(value) != ""})


@st.cache_data(ttl=FILTER_CACHE_SECONDS, show_spinner=False)
def load_filter_options(dsn: str, schema: str, read_only: bool) -> dict[str, list[Any]]:
    """Countries, sectors and years available in the marts.

    One ``union all`` round trip fills every dropdown; the result is cached for
    :data:`FILTER_CACHE_SECONDS` and can be refreshed with the *Обновить данные* button.
    """
    connection = get_connection(dsn, read_only)
    table = qualified(schema, MART_CORPORATE_EMISSIONS)
    frame = connection.execute(
        f"""
        select 'country' as kind, country as value from {table}
        union all
        select 'sector', sector from {table}
        union all
        select 'year', cast(year as varchar) from {table}
        """
    ).df()

    return {
        "country": _unique_values(frame, "country"),
        "sector": _unique_values(frame, "sector"),
        "year": sorted(int(year) for year in _unique_values(frame, "year")),
    }


def render_configuration_help() -> None:
    """Explain how to provide the MotherDuck token when nothing is configured."""
    st.title("🌱 Корпоративные выбросы CO2e (Climate TRACE)")
    st.error("Не задан `MOTHERDUCK_TOKEN`: дашборду не с чем соединяться.")
    st.markdown(
        textwrap.dedent(
            """
            Добавьте секреты в **Streamlit Community Cloud** (*App → Settings → Secrets*):

            ```toml
            MOTHERDUCK_TOKEN = "eyJhbGciOiJ..."
            MOTHERDUCK_DATABASE = "emissions_db"
            MOTHERDUCK_SCHEMA = "main"
            ```

            Локально положите тот же TOML в `.streamlit/secrets.toml` (файл уже в `.gitignore`)
            или экспортируйте переменные окружения:

            ```bash
            MOTHERDUCK_TOKEN=... poetry run streamlit run streamlit_app.py
            ```
            """
        )
    )


def render_header(target: ConnectionTarget) -> None:
    """Title and data-source caption of the page."""
    st.title("🌱 Корпоративные выбросы CO2e (Climate TRACE)")
    st.caption(
        "Атрибутированные выбросы компаний и объектов по данным Climate TRACE API v7. "
        f"Источник: `{target.caption}`."
    )


def render_sidebar(options: dict[str, list[Any]], target: ConnectionTarget) -> tuple[Filters, int]:
    """Render the filter widgets and return the selection plus the top-N size."""
    st.sidebar.header("Параметры фильтрации")

    country = st.sidebar.selectbox("Страна", [ALL_COUNTRIES, *options["country"]])
    sectors = st.sidebar.multiselect("Секторы", options["sector"])
    years = st.sidebar.multiselect("Годы", options["year"])
    exclude_state_owners = st.sidebar.checkbox(
        "Только коммерческие владельцы",
        value=True,
        help="Исключает выбросы, не привязанные к компании (`is_state_or_unmapped_owner`).",
    )
    top_n = st.sidebar.slider(
        "Сколько компаний показать в топе?", min_value=5, max_value=50, value=10, step=5
    )

    st.sidebar.divider()
    st.sidebar.caption(f"Источник: `{target.caption}`")
    st.sidebar.caption("Данные обновляются после каждого запуска `run-etl`.")
    if st.sidebar.button("Обновить данные"):
        load_filter_options.clear()
        st.rerun()

    filters = Filters(
        country=None if country == ALL_COUNTRIES else country,
        sectors=tuple(sectors),
        years=tuple(years),
        exclude_state_owners=exclude_state_owners,
    )
    return filters, top_n


def _number(value: Any, default: float = 0.0) -> float:
    """``value`` as a float, tolerating the ``None``/``NaN`` of an empty aggregate."""
    if value is None or pd.isna(value):
        return default
    return float(value)


def render_kpis(connection: duckdb.DuckDBPyConnection, schema: str, filters: Filters) -> int:
    """Render the KPI row and return the number of companies in the selection."""
    row = run_query(connection, kpi_sql(schema, filters)).iloc[0]
    facilities = run_query(connection, facility_count_sql(schema, filters)).iloc[0]

    companies = int(_number(row["companies"]))
    emissions_m_tons = round(_number(row["attributed_emissions_tco2e"]) / MILLION, 2)

    columns = st.columns(4)
    columns[0].metric("Коммерческих компаний", companies)
    columns[1].metric("Выбросы CO2e (Млн тонн)", emissions_m_tons)
    columns[2].metric("Стран", int(_number(row["countries"])))
    columns[3].metric("Объектов", int(_number(facilities["facilities"])))
    return companies


def render_top_companies(
    connection: duckdb.DuckDBPyConnection,
    schema: str,
    filters: Filters,
    top_n: int,
) -> None:
    """Horizontal bar chart of the largest emitters."""
    frame = run_query(connection, top_companies_sql(schema, filters, top_n))
    st.subheader(f"ТОП-{top_n} компаний по выбросам")
    if frame.empty:
        st.info("Нет компаний, подходящих под выбранные фильтры.")
        return

    figure = px.bar(
        frame,
        x="emissions_m_tons",
        y="company_name",
        color="country",
        orientation="h",
        labels={
            "emissions_m_tons": "Выбросы (Млн тонн CO2e)",
            "company_name": "Компания",
            "country": "Страна",
        },
    )
    figure.update_layout(
        yaxis={"categoryorder": "total ascending"},
        template="plotly_white",
        margin=PLOTLY_MARGIN,
        height=max(400, 30 * len(frame)),
        legend_title_text="Страна",
    )
    st.plotly_chart(figure, width="stretch")


def render_sector_breakdown(
    connection: duckdb.DuckDBPyConnection,
    schema: str,
    filters: Filters,
) -> None:
    """Treemap of the attributed emissions per sector."""
    frame = run_query(connection, sector_sql(schema, filters))
    st.subheader("Распределение по секторам")
    if frame.empty:
        st.info("Нет секторов, подходящих под выбранные фильтры.")
        return

    figure = px.treemap(
        frame,
        path=[px.Constant("Все секторы"), "sector"],
        values="emissions_m_tons",
        color="emissions_m_tons",
        color_continuous_scale="Greens",
        labels={"emissions_m_tons": "Выбросы (Млн тонн CO2e)", "sector": "Сектор"},
    )
    figure.update_layout(template="plotly_white", margin=PLOTLY_MARGIN)
    st.plotly_chart(figure, width="stretch")


def render_yearly_trend(
    connection: duckdb.DuckDBPyConnection,
    schema: str,
    filters: Filters,
) -> None:
    """Line chart of the attributed emissions per reporting year."""
    frame = run_query(connection, yearly_trend_sql(schema, filters))
    st.subheader("Динамика по годам")
    if frame.empty:
        st.info("Нет данных за выбранные годы.")
        return

    figure = px.line(
        frame,
        x="year",
        y="emissions_m_tons",
        markers=True,
        labels={"year": "Год", "emissions_m_tons": "Выбросы (Млн тонн CO2e)"},
    )
    figure.update_layout(
        template="plotly_white",
        margin=PLOTLY_MARGIN,
        height=420,
        xaxis={"dtick": 1},
    )
    st.plotly_chart(figure, width="stretch")


def render_detail_table(
    connection: duckdb.DuckDBPyConnection,
    schema: str,
    filters: Filters,
) -> None:
    """Collapsible facility drill-down of the current selection."""
    frame = run_query(connection, detail_sql(schema, filters, DETAIL_TABLE_ROWS))
    with st.expander(f"Детализация по объектам (первые {DETAIL_TABLE_ROWS} строк)"):
        if frame.empty:
            st.info("Нет объектов под выбранные фильтры.")
            return

        st.dataframe(
            frame.rename(
                columns={
                    "company_name": "Компания",
                    "facility_name": "Объект",
                    "country": "Страна",
                    "sector": "Сектор",
                    "year": "Год",
                    "total_tco2e": "Всего, т CO2e",
                    "attributed_tco2e": "Атрибутировано, т CO2e",
                    "ownership_share": "Доля владения",
                }
            ),
            width="stretch",
            hide_index=True,
        )


def render_footer() -> None:
    """Ownership disclaimer and data source."""
    st.divider()
    st.caption(
        "Доли владения оценены как `1 / число владельцев` (`ownership_share_is_estimated`), "
        "потому что Climate TRACE не публикует проценты. "
        "Данные: [Climate TRACE](https://climatetrace.org/)."
    )


def main() -> None:
    """Render the dashboard; the entrypoint of ``streamlit run streamlit_app.py``."""
    st.set_page_config(page_title="Climate TRACE Dashboard", page_icon="🌱", layout="wide")

    target = resolve_target()
    if target is None:
        render_configuration_help()
        return

    render_header(target)

    try:
        connection = get_connection(target.dsn, target.read_only)
        missing = missing_marts(connection, target.schema)
    except duckdb.Error as error:
        st.error(f"Не удалось подключиться к `{target.caption}`: {error}")
        st.info(
            "Проверьте `MOTHERDUCK_TOKEN`, `MOTHERDUCK_DATABASE` и `MOTHERDUCK_SCHEMA`. "
            "`poetry run check-motherduck` показывает базы, доступные токену."
        )
        return

    if missing:
        tables = ", ".join(f"`{table}`" for table in missing)
        st.error(f"В схеме `{target.schema}` нет таблиц: {tables}.")
        st.info("Опубликуйте данные из пайплайна: `poetry run run-etl`.")
        return

    try:
        options = load_filter_options(target.dsn, target.schema, target.read_only)
    except duckdb.Error as error:
        load_filter_options.clear()
        st.error(f"Не удалось прочитать справочники фильтров: {error}")
        return

    filters, top_n = render_sidebar(options, target)

    try:
        companies = render_kpis(connection, target.schema, filters)
    except duckdb.Error as error:
        st.error(f"Не удалось выполнить запрос метрик: {error}")
        return

    if companies == 0:
        st.warning("Под выбранные фильтры не попала ни одна компания.")
        render_footer()
        return

    tabs = st.tabs(["ТОП компаний", "Секторы", "Динамика по годам"])
    with tabs[0]:
        render_top_companies(connection, target.schema, filters, top_n)
    with tabs[1]:
        render_sector_breakdown(connection, target.schema, filters)
    with tabs[2]:
        render_yearly_trend(connection, target.schema, filters)

    render_detail_table(connection, target.schema, filters)
    render_footer()


if __name__ == "__main__":
    main()
