# Climate TRACE → MotherDuck Emissions Pipeline

Automated, production-ready ELT pipeline that extracts corporate emission assets from the
**Climate TRACE API (v7)**, normalises ownership and facility emissions, and loads analytics
data marts into **MotherDuck** (DuckDB Cloud).

The pipeline is managed with **Poetry**, is designed to run on lightweight GitHub Actions
runners, and keeps a 45-second execution budget in mind (small fetch batches, no heavy
compute, no local warehouse).

```
Climate TRACE API v7  ──httpx + tenacity──▶  client  ──▶  transformer  ──pandas──▶  loader  ──duckdb──▶  MotherDuck
   /sources, /owners                       (pydantic)      (flat DataFrames)                         mart_corporate_emissions
                                                                                                     mart_company_assets_detail
```

## Data source notes

The v7 API documentation (<https://api.climatetrace.org/v7/docs>) is the source of truth and
differs from older Climate TRACE material:

* There is **no `/v7/assets` endpoint**. Facilities ("assets") are served by:
  * `GET /v7/sources` – ranked list of emission sources: `id`, `name`, `sector`, `subsector`,
    `country`, `assetType`, `sourceType`, `centroid{latitude,longitude}`, `gas`,
    `emissionsQuantity`, `year`;
  * `GET /v7/sources/:id` – per-facility detail, adding `owners[]` (`id`, `name`),
    `emissions[]` (yearly time series), `totals`, `confidence[]`, `subsectorRanks[]`.
* There is **no nested `emissionsSummary`** in the payload; the latest reporting year and
  `co2e_100yr` emissions come from `year` + `emissionsQuantity` / `totals`.
* `GET /v7/owners?name=…` returns owner `id`/`name` pairs only, and `owners` can be `null`.
  The API publishes **no ownership percentage**, so `ownership_share` is treated as a
  configurable/enrichable input that defaults to `1.0` (100 % attribution) while the
  `emissions × ownership_share` formula stays intact; facilities without owners are labelled
  `State / Unmapped Owner`.

## Repository layout

```
.
├── src/climate_trace_etl/
│   ├── __init__.py          package metadata and version
│   ├── config.py            pydantic-settings configuration + get_settings()
│   ├── logging_config.py    loguru bootstrap and stdlib interception
│   └── client.py            resilient Climate TRACE API v7 client
├── tests/
│   ├── conftest.py          environment isolation and loguru reset fixtures
│   ├── test_config.py
│   ├── test_logging.py
│   └── test_client.py
├── .env.example             documented environment template
├── pyproject.toml           Poetry, ruff and pytest configuration
└── README.md
```

> `logging_config.py` is a small extension of the layout from the technical specification: it
> keeps the loguru bootstrap out of `config.py`, which stays dedicated to settings.

## Prerequisites

* Python **3.11+** (3.12 recommended)
* [Poetry](https://python-poetry.org/) **2.x**:
  `curl -sSL https://install.python-poetry.org | python3 -`
* A MotherDuck account and read/write token (only needed once data is loaded to the cloud):
  <https://app.motherduck.com/settings/tokens>

## Local setup

```bash
git clone git@github.com:onhorou/climatetrace-to-motherduck.git
cd climatetrace-to-motherduck

# 1. Point Poetry at a supported interpreter and install everything (incl. dev tools)
poetry env use 3.12
poetry install

# 2. Create your local configuration
cp .env.example .env
$EDITOR .env                     # fill in MOTHERDUCK_TOKEN

# 3. Verify the environment
poetry run pytest                # unit tests
poetry run ruff check .          # lint
poetry run ruff format --check . # formatting
```

Poetry creates the virtual environment inside the project (`.venv/`), so `poetry shell` or
`poetry run <command>` both work without activating anything manually.

## Usage

### Configuration

```python
from climate_trace_etl.config import get_settings

settings = get_settings()  # cached, validated singleton
settings.fetch_limit  # 500
settings.motherduck_dsn_masked  # "md:emissions_db?motherduck_token=***"
settings.motherduck_configured  # False until MOTHERDUCK_TOKEN is set
```

Secrets are held in `pydantic.SecretStr`, so they never show up in logs, `repr()` or
`model_dump_json()`.

### Logging

```python
from loguru import logger
from climate_trace_etl.logging_config import setup_logging

setup_logging()  # honours LOG_LEVEL / LOG_JSON
logger.bind(run_id="2026-09-22T06:00Z").info("extraction started")
```

### Extracting from Climate TRACE

```python
from climate_trace_etl.client import ClimateTraceClient

with ClimateTraceClient() as client:
    # paginated asset list; offset/limit handled for you, bounded by max_records
    sources = client.fetch_sources(max_records=500, sectors="power")
    # one facility, including owners[] and the yearly emission time series
    detail = client.fetch_source(sources[0]["id"])
    # owner (company) lookup by name
    owners = client.search_owners("Petro", limit=20)

print(client.request_count, client.retry_count)
```

`iter_sources()` streams the same data lazily, which keeps memory flat on large extractions:

```python
with ClimateTraceClient() as client:
    for source in client.iter_sources(max_records=2_000, countryGroup="EUU"):
        ...
```

Client behaviour:

| Topic | Behaviour |
| --- | --- |
| Pagination | `limit`/`offset` paging until a short or empty page; a page-count limit and a repeated-page guard stop a misbehaving API. |
| Retries | `429`, `500`, `502`, `503`, `504` and `httpx.TransportError` (DNS, resets, timeouts), up to `MAX_RETRIES` additional attempts. |
| Backoff | Exponential with jitter, bounded by `8 × RETRY_BACKOFF_SECONDS`, overridden by a numeric `Retry-After` header (HTTP-date values are ignored). |
| Errors | `ClimateTraceHTTPError` (fails fast, e.g. `400`/`404`), `RetryableHTTPError` (carries `retry_after`), `ClimateTraceResponseError` (HTTP 200 with an unexpected body). All inherit `ClimateTraceAPIError` and expose `url`, `status_code` and `detail`. |
| Counters | `client.request_count` / `client.retry_count` feed the run summary. |
| Payloads | Raw JSON documents exactly as returned by the API; schema validation belongs to the transformer. |

## Configuration reference

Every setting lives in `src/climate_trace_etl/config.py` and is read from environment variables
or `.env`. Values are validated at startup (fail fast), empty variables are ignored, and
`SecretStr` keeps secrets out of logs.

| Variable | Default | Description |
| --- | --- | --- |
| `MOTHERDUCK_TOKEN` | *(unset)* | MotherDuck read/write token; required to load data. |
| `MOTHERDUCK_DATABASE` | `emissions_db` | Target MotherDuck database. |
| `MOTHERDUCK_SCHEMA` | `main` | Target schema for the data marts. |
| `CLIMATE_TRACE_API_URL` | `https://api.climatetrace.org/v7` | API base URL (trailing slash normalised). |
| `FETCH_LIMIT` | `500` | Records per paginated API request (`1`–`10000`). |
| `EMISSIONS_GAS` | `co2e_100yr` | Gas queried from the API (`co2e_100yr`, `co2`, `ch4`, `n2o`, …). |
| `EMISSIONS_YEAR` | *(unset → latest)* | Restrict the extraction to one year (`2021`–`2100`). |
| `REQUEST_TIMEOUT_SECONDS` | `30` | Timeout of a single HTTP request. |
| `MAX_RETRIES` | `5` | Retries for `429`/`5xx` and transport errors (`0`–`10`). |
| `RETRY_BACKOFF_SECONDS` | `1` | Base delay of the exponential backoff. |
| `ENVIRONMENT` | `local` | `local`, `ci` or `prod`. |
| `LOG_LEVEL` | `INFO` | `TRACE` … `CRITICAL`. |
| `LOG_JSON` | `false` | `true` → one JSON object per log line (CI friendly). |

## Quality gates

```bash
poetry run pytest                # unit tests
poetry run pytest tests/test_client.py -q
poetry run ruff check .          # lint rules: E, W, F, I, B, C4, UP, SIM, RET, PTH, RUF
poetry run ruff format .         # auto-format (line length 100, Markdown code blocks included)
```

## MotherDuck data marts

The loader publishes two presentation marts inside `MOTHERDUCK_DATABASE` / `MOTHERDUCK_SCHEMA`:

| Mart | Grain | Contents |
| --- | --- | --- |
| `mart_corporate_emissions` | company × country × sector × year | attributed emissions totals and facility counts per company. |
| `mart_company_assets_detail` | facility × owner | facility metadata, coordinates, ownership share and attributed emissions. |

Dashboard queries:

```sql
-- Total attributed CO2e per company and year
select company_name, country, sector, year, round(sum(attributed_emissions_tco2e)) as tco2e
from emissions_db.main.mart_corporate_emissions
group by all
order by tco2e desc
limit 25;

-- Facility-level drill-down for the largest emitters
select company_name, facility_name, country, sector, latitude, longitude,
       ownership_share, attributed_emissions_tco2e
from emissions_db.main.mart_company_assets_detail
order by attributed_emissions_tco2e desc
limit 50;
```

## License

MIT — see [LICENSE](LICENSE).


