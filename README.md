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
  The API publishes **no ownership percentage**, so the transformer derives `ownership_share`
  as `1.0` for a sole owner and `1 / n` for `n` owners (equal-split estimate); every row is
  flagged through `ownership_share_is_estimated`. The
  `attributed_emissions = total_emissions × ownership_share` formula stays intact, and
  facilities without published owners are attributed to `State / Unmapped Owner`.
* The `/sources` list payload contains **no owners at all**, so facilities are enriched
  individually through `GET /sources/:id`, capped by `MAX_ENRICH_RECORDS` (one request per
  facility).

## Repository layout

```
.
├── .github/workflows/emissions_etl.yml   # manual CI run: lint, tests, ETL, mart verification
├── src/climate_trace_etl/
│   ├── __init__.py          package metadata and version
│   ├── config.py            pydantic-settings configuration + get_settings()
│   ├── logging_config.py    loguru bootstrap and stdlib interception
│   ├── client.py            resilient Climate TRACE API v7 client
│   ├── transformer.py       payload schemas + normalisation into pandas DataFrames
│   ├── loader.py            DuckDB / MotherDuck loading, database bootstrap and data marts
│   ├── diagnostics.py       `check-motherduck`: prints what the token can see
│   └── main.py              CLI entrypoint (run-etl)
├── tests/
│   ├── conftest.py          environment isolation and loguru reset fixtures
│   ├── test_config.py
│   ├── test_logging.py
│   ├── test_client.py
│   ├── test_transformer.py
│   ├── test_loader.py
│   ├── test_diagnostics.py
│   └── test_main.py
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
  <https://app.motherduck.com/settings/tokens>. The target database does not have to exist: the
  loader creates it on the first run.

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
settings.motherduck_dsn_masked  # "md:emissions_db?motherduck_token=***&attach_mode=single"
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
    # concurrent ownership enrichment (ENRICH_WORKERS threads, 1 request per facility)
    details = list(client.iter_source_details([s["id"] for s in sources], max_records=500))
    # or, one facility at a time, including owners[] and the yearly emission time series
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
| Concurrency | Facility enrichment runs on `ENRICH_WORKERS` threads, keeps the input order and is capped by `MAX_ENRICH_RECORDS` (`1` thread = sequential). |
| Errors | `ClimateTraceHTTPError` (fails fast, e.g. `400`/`404`), `RetryableHTTPError` (carries `retry_after`), `ClimateTraceResponseError` (HTTP 200 with an unexpected body). All inherit `ClimateTraceAPIError` and expose `url`, `status_code` and `detail`. |
| Counters | `client.request_count` / `client.retry_count` feed the run summary. |
| Payloads | Raw JSON documents exactly as returned by the API; schema validation belongs to the transformer. |

### Normalising the extraction

```python
from climate_trace_etl.transformer import transform_assets

frame = transform_assets(detail_documents)  # one row per (facility, owner)
frame[["facility_name", "company_name", "country", "reporting_year", "attributed_emissions_tco2e"]]
```

The frame carries the columns of `ASSET_OWNER_COLUMNS` with pinned dtypes
(`Int64` / `float64` / `bool` / `string`), so it can be registered in DuckDB as-is:

| Column group | Columns |
| --- | --- |
| Identity | `source_id`, `facility_name`, `sector`, `subsector`, `country`, `asset_type`, `source_type` |
| Geography | `latitude`, `longitude` (NA when the API publishes no coordinates) |
| Emissions | `gas`, `reporting_year`, `total_emissions_tco2e`, `emissions_reported` |
| Ownership | `company_id`, `company_name`, `owner_index`, `owner_count`, `ownership_share`, `ownership_share_is_estimated` |
| Attribution | `attributed_emissions_tco2e` |

`emissions_reported` separates a reported `0` from a facility whose emissions are not published
yet, and malformed documents are logged and skipped instead of aborting the run.

### Loading into MotherDuck

```python
from climate_trace_etl.config import get_settings
from climate_trace_etl.loader import connect, publish_frame
from climate_trace_etl.transformer import transform_assets

settings = get_settings()
frame = transform_assets(detail_documents, gas=settings.emissions_gas)

with connect(settings) as connection:  # md:<database>?motherduck_token=…
    summary = publish_frame(connection, frame, schema=settings.motherduck_schema)

summary.as_dict()  # {'staging_rows': 13, 'corporate_rows': 10, 'detail_rows': 13}
```

`connect()` targets MotherDuck as soon as `MOTHERDUCK_TOKEN` is configured, masks the token in
every log line, and otherwise warns while falling back to a throw-away in-memory DuckDB. That
keeps the same code path usable locally, in CI and in tests (`duckdb.connect(":memory:")`).

#### Database bootstrap and troubleshooting

MotherDuck never creates a database implicitly: attaching `md:<name>` fails with
`no database/share named '<name>' found` until the database exists in the account behind the
token. `connect()` therefore creates a missing `MOTHERDUCK_DATABASE` on demand — through a
workspace-mode `md:` connection, the only one that accepts `create database` — and retries the
attachment. A database the token is not allowed to see (read-scaling token, or a token from
another account) raises a `ConfigurationError` that names the token, the database and the check
below instead of a raw DuckDB stack trace.

Inspect what a token can actually see before hunting a red CI run:

```bash
poetry run check-motherduck   # or: python -m climate_trace_etl.diagnostics
```

The command prints every database and share of the token's account, exits `0` when
`MOTHERDUCK_DATABASE` is among them and `1` otherwise. To create the database by hand instead:

```sql
create database if not exists emissions_db;
```

Automatic creation only helps when the token lacks the database, not when it points at the wrong
account: `MOTHERDUCK_TOKEN` must belong to the account that hosts the marts, and
`MOTHERDUCK_DATABASE` must name it.

### Running the pipeline

```bash
poetry run run-etl --dry-run --max-records 5      # extract + transform, no writes
poetry run run-etl --max-records 500 --workers 8  # full run into MotherDuck
poetry run run-etl --help
```

| Flag | Default | Purpose |
| --- | --- | --- |
| `--max-records N` | unlimited | Facilities extracted from `GET /sources`. |
| `--max-enrich-records N` | `MAX_ENRICH_RECORDS` | Facilities enriched with owners through `GET /sources/:id`. |
| `--workers N` | `ENRICH_WORKERS` | Enrichment threads (`1` = sequential). |
| `--schema NAME` | `MOTHERDUCK_SCHEMA` | Target schema for the staging table and the marts. |
| `--dry-run` | off | Extract and transform only, skipping every write. |
| `--log-level LEVEL` | `LOG_LEVEL` | `TRACE` … `CRITICAL`. |

`run-etl` exits with `0` on success and `1` for any failure (API, configuration, DuckDB), and
finishes with a single summary line:

```
extracted_facilities=4 | enriched_facilities=4 | owner_rows=5 | requests=5 | retries=0 |
duration_seconds=1.77 | dry_run=False | staging_rows=5 | corporate_rows=4 | detail_rows=5
```

An enrichment failure never sinks a run: affected facilities keep their listing document and are
attributed to `State / Unmapped Owner`. `--dry-run` logs the top rows by attributed emissions
instead of writing them.

## Configuration reference

Every setting lives in `src/climate_trace_etl/config.py` and is read from environment variables
or `.env`. Values are validated at startup (fail fast), empty variables are ignored, and
`SecretStr` keeps secrets out of logs.

| Variable | Default | Description |
| --- | --- | --- |
| `MOTHERDUCK_TOKEN` | *(unset)* | MotherDuck read/write token; required to load data. |
| `MOTHERDUCK_DATABASE` | `emissions_db` | Target MotherDuck database; created on demand when missing. |
| `MOTHERDUCK_SCHEMA` | `main` | Target schema for the data marts. |
| `MOTHERDUCK_ATTACH_MODE` | `single` | `single` = one-off session for automation, `workspace` = reuse the MotherDuck UI workspace, `default` = omit the parameter and let the extension decide. |
| `CLIMATE_TRACE_API_URL` | `https://api.climatetrace.org/v7` | API base URL (trailing slash normalised). |
| `FETCH_LIMIT` | `500` | Records per paginated API request (`1`–`10000`). |
| `MAX_ENRICH_RECORDS` | `500` | Facilities enriched with ownership details via `GET /sources/:id` (one request each). |
| `ENRICH_WORKERS` | `8` | Worker threads for concurrent enrichment (`1`–`32`; `1` = sequential). |
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
poetry run ruff check .          # lint rules: E, W, F, I, B, BLE, C4, UP, SIM, RET, PTH, RUF
poetry run ruff format .         # auto-format (line length 100, Markdown code blocks included)
poetry run run-etl --dry-run --max-records 5   # end-to-end smoke test without writes
```

## GitHub Actions

`.github/workflows/emissions_etl.yml` is **manual-only**: there is no `schedule:` trigger, so a run
starts exactly when you ask for one.

1. **Actions → Emissions ETL → Run workflow** (or `gh workflow run emissions_etl.yml`).
2. Optionally override the inputs: facilities to extract, facilities to enrich, worker threads,
   target schema, and a dry-run toggle that skips every write to MotherDuck.
3. The `quality` job lints, checks formatting and runs the unit tests; the `etl` job then
   extracts, transforms, loads, and finally verifies that both marts contain rows.

Setup:

| Kind | Name | Purpose |
| --- | --- | --- |
| Secret | `MOTHERDUCK_TOKEN` | Read/write token used by the loader. |
| Variable (optional) | `MOTHERDUCK_DATABASE` | Target database, defaults to `emissions_db` (created on the first run). |
| Variable (optional) | `MOTHERDUCK_ATTACH_MODE` | `single` (default), `workspace` or `default`. |

An unset `MOTHERDUCK_DATABASE` variable expands to an empty string, which the settings ignore, so
the `emissions_db` default applies. When the token cannot see that database, the run logs
`MotherDuck database emissions_db is not visible to this token yet: creating it`, creates it and
continues; if the token belongs to a different account, the job fails with a
`configuration error` line instead of a DuckDB traceback — reproduce it locally with
`poetry run check-motherduck`.

CI details: Poetry `2.3.2` installed through `pipx`, Python `3.12` with the Poetry download cache,
the `.venv` cached on `poetry.lock`, `LOG_JSON=true` for machine-readable logs, `ENVIRONMENT=ci`,
a `concurrency` group so two runs can never overlap, and a 10-minute job timeout.

To restore periodic execution later, add a schedule next to the manual trigger:

```yaml
on:
  schedule:
    - cron: "0 6 * * 1" # every Monday at 06:00 UTC
  workflow_dispatch:
```

## MotherDuck data marts

Every run replaces `stg_asset_owners` (one row per facility × owner) together with both
presentation marts inside `MOTHERDUCK_DATABASE` / `MOTHERDUCK_SCHEMA`, which makes re-runs
idempotent and keeps the marts from blending two extraction windows.

| Object | Grain | Contents |
| --- | --- | --- |
| `stg_asset_owners` | facility × owner | staging copy of the transformed frame. |
| `mart_corporate_emissions` | company × country × sector × year | `facility_count`, `sum(attributed_emissions_tco2e)`, `avg_ownership_share`, `ownership_share_is_estimated`, `is_state_or_unmapped_owner`. |
| `mart_company_assets_detail` | facility × owner | facility metadata, coordinates, `year`, `ownership_share`, `total_emissions_tco2e` and `attributed_emissions_tco2e`, plus the same ownership flags. |

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


