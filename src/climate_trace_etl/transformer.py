"""Normalise Climate TRACE API v7 payloads into flat pandas DataFrames.

The transformer turns the documents produced by :mod:`climate_trace_etl.client` into one tidy
frame with **one row per (facility, owner) pair**, ready to be registered in DuckDB and
aggregated into the presentation marts.

Adaptations to the real v7 payload (documented in the README as well):

* There is no nested ``emissionsSummary``. The latest reporting year and quantity are taken
  from the ``emissions[]`` time series (``GET /sources/:id``), falling back to ``totals`` and
  finally to the ``year``/``emissionsQuantity`` scalars of ``GET /sources``.
* Ownership percentages are not published by the API. ``ownership_share`` is therefore derived:
  ``1.0`` for a sole owner, ``1 / n`` for ``n`` owners (equal-split estimate), and every row is
  flagged through ``ownership_share_is_estimated``.
* Facilities without a published owner are attributed to :data:`UNMAPPED_OWNER_NAME` and
  computed as ``attributed_emissions = total_emissions x ownership_share``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import pandas as pd
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic.alias_generators import to_camel

#: Label used when the API publishes no owner for a facility (state-owned or unmapped assets).
UNMAPPED_OWNER_NAME = "State / Unmapped Owner"

#: Column order of the normalised facility/owner frame.
ASSET_OWNER_COLUMNS: tuple[str, ...] = (
    "source_id",
    "facility_name",
    "sector",
    "subsector",
    "country",
    "asset_type",
    "source_type",
    "latitude",
    "longitude",
    "gas",
    "reporting_year",
    "total_emissions_tco2e",
    "emissions_reported",
    "company_id",
    "company_name",
    "owner_index",
    "owner_count",
    "ownership_share",
    "ownership_share_is_estimated",
    "attributed_emissions_tco2e",
)

_INTEGER_COLUMNS: Sequence[str] = ("source_id", "reporting_year", "owner_index", "owner_count")
_FLOAT_COLUMNS: Sequence[str] = (
    "latitude",
    "longitude",
    "total_emissions_tco2e",
    "ownership_share",
    "attributed_emissions_tco2e",
)
_BOOLEAN_COLUMNS: Sequence[str] = ("emissions_reported", "ownership_share_is_estimated")
_STRING_COLUMNS: Sequence[str] = (
    "facility_name",
    "sector",
    "subsector",
    "country",
    "asset_type",
    "source_type",
    "gas",
    "company_id",
    "company_name",
)


class APIModel(BaseModel):
    """Base class for payload models: camelCase aliases, unknown fields ignored."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="ignore",
    )


class SourceCentroid(APIModel):
    """Geographic centre of a facility."""

    latitude: float | None = None
    longitude: float | None = None
    srid: int | None = None


class Owner(APIModel):
    """Corporate owner of a facility (``owners[]`` of ``GET /sources/:id``)."""

    id: str | None = None
    name: str | None = None


class EmissionRecord(APIModel):
    """One emissions observation, either from the time series or from ``totals``."""

    year: int | None = None
    month: int | None = None
    gas: str | None = None
    emissions_quantity: float | None = None


class EmissionSummary(BaseModel):
    """Latest emissions figure selected for one facility."""

    year: int
    gas: str | None = None
    quantity: float = 0.0


class SourceDocument(APIModel):
    """Union of a ``GET /sources`` list item and a ``GET /sources/:id`` detail document.

    ``id`` is the only mandatory field: everything else is optional so that API drift or a
    partially populated facility never aborts the whole extraction.
    """

    id: int
    name: str = ""
    sector: str | None = None
    subsector: str | None = None
    country: str | None = None
    asset_type: str | None = None
    source_type: str | None = None
    centroid: SourceCentroid | None = None
    gas: str | None = None
    year: int | None = None
    emissions_quantity: float | None = None
    emissions: list[EmissionRecord] = Field(default_factory=list)
    totals: EmissionRecord | None = None
    owners: list[Owner] | None = None


def parse_source(document: Mapping[str, Any]) -> SourceDocument:
    """Validate a raw API document against :class:`SourceDocument`.

    Raises:
        pydantic.ValidationError: if the document cannot be interpreted, for example when the
            facility identifier is missing.
    """
    return SourceDocument.model_validate(dict(document))


def _matching_gas(records: Sequence[EmissionRecord], gas: str | None) -> list[EmissionRecord]:
    """Records of the requested gas; all records when no gas is requested."""
    if gas is None:
        return list(records)
    return [record for record in records if record.gas == gas]


def select_latest_emissions(
    source: SourceDocument,
    *,
    gas: str | None = None,
) -> EmissionSummary | None:
    """Return the latest reported emissions of ``source``, or ``None`` when unavailable.

    Precedence: the ``emissions[]`` time series of the requested gas, then ``totals`` (only when
    its gas matches), then the ``year``/``emissionsQuantity`` scalars of the list payload.
    """
    requested_gas = gas or source.gas

    series = [
        record
        for record in _matching_gas(source.emissions, requested_gas)
        if record.year is not None
    ]
    if series:
        latest_year = max(record.year for record in series if record.year is not None)
        quantity = sum(
            record.emissions_quantity or 0.0 for record in series if record.year == latest_year
        )
        return EmissionSummary(year=latest_year, gas=requested_gas, quantity=quantity)

    totals = source.totals
    gas_matches = requested_gas is None or totals is None or totals.gas in (None, requested_gas)
    if totals is not None and totals.emissions_quantity is not None and gas_matches:
        year = source.year or max(
            (record.year for record in source.emissions if record.year), default=None
        )
        if year is not None:
            quantity = totals.emissions_quantity
            return EmissionSummary(year=year, gas=totals.gas or requested_gas, quantity=quantity)

    if source.year is not None and source.emissions_quantity is not None:
        return EmissionSummary(
            year=source.year,
            gas=requested_gas,
            quantity=source.emissions_quantity,
        )

    return None


def _record(
    source: SourceDocument,
    *,
    owner: Owner | None,
    owner_index: int,
    owner_count: int,
    ownership_share: float,
    reporting_year: int | None,
    gas: str | None,
    total_emissions: float,
    emissions_reported: bool,
) -> dict[str, Any]:
    """Build one flat (facility, owner) record."""
    centroid = source.centroid
    if owner is None:
        company_id: str | None = None
        company_name = UNMAPPED_OWNER_NAME
    else:
        company_id = owner.id
        company_name = owner.name or owner.id or UNMAPPED_OWNER_NAME

    return {
        "source_id": source.id,
        "facility_name": source.name or f"Source {source.id}",
        "sector": source.sector,
        "subsector": source.subsector,
        "country": source.country,
        "asset_type": source.asset_type,
        "source_type": source.source_type,
        "latitude": None if centroid is None else centroid.latitude,
        "longitude": None if centroid is None else centroid.longitude,
        "gas": gas,
        "reporting_year": reporting_year,
        "total_emissions_tco2e": total_emissions,
        "emissions_reported": emissions_reported,
        "company_id": company_id,
        "company_name": company_name,
        "owner_index": owner_index,
        "owner_count": owner_count,
        "ownership_share": ownership_share,
        "ownership_share_is_estimated": True,
        "attributed_emissions_tco2e": total_emissions * ownership_share,
    }


def transform_asset(document: Mapping[str, Any], *, gas: str | None = None) -> list[dict[str, Any]]:
    """Normalise one facility document into one record per (facility, owner) pair.

    A facility without published owners yields a single record attributed to
    :data:`UNMAPPED_OWNER_NAME`.

    Raises:
        pydantic.ValidationError: if the document carries no usable identity.
    """
    source = parse_source(document)
    summary = select_latest_emissions(source, gas=gas)
    total_emissions = 0.0 if summary is None else summary.quantity
    reporting_year = None if summary is None else summary.year
    reported_gas = (None if summary is None else summary.gas) or source.gas or gas
    emissions_reported = summary is not None

    owners = [owner for owner in (source.owners or []) if owner is not None]
    if not owners:
        logger.debug(
            "source {} publishes no owner; attributing its emissions to {!r}",
            source.id,
            UNMAPPED_OWNER_NAME,
        )
        return [
            _record(
                source,
                owner=None,
                owner_index=0,
                owner_count=0,
                ownership_share=1.0,
                reporting_year=reporting_year,
                gas=reported_gas,
                total_emissions=total_emissions,
                emissions_reported=emissions_reported,
            )
        ]

    share = 1.0 / len(owners)
    return [
        _record(
            source,
            owner=owner,
            owner_index=index,
            owner_count=len(owners),
            ownership_share=share,
            reporting_year=reporting_year,
            gas=reported_gas,
            total_emissions=total_emissions,
            emissions_reported=emissions_reported,
        )
        for index, owner in enumerate(owners)
    ]


def _first_error(error: ValidationError) -> str:
    """Short, log-friendly description of the first validation failure."""
    problems = error.errors()
    if not problems:
        return "invalid document"
    first = problems[0]
    location = ".".join(str(part) for part in first.get("loc", ()))
    return f"{location or 'document'}: {first.get('msg', 'invalid document')}"


def transform_assets(
    documents: Iterable[Mapping[str, Any]],
    *,
    gas: str | None = None,
) -> pd.DataFrame:
    """Normalise many facility documents into the typed (facility, owner) frame.

    Malformed documents are logged and skipped instead of failing the whole run.

    Args:
        documents: Raw documents from ``GET /sources`` and/or ``GET /sources/:id``.
        gas: Gas to select from the emission time series; defaults to the gas of each document.

    Returns:
        A :class:`pandas.DataFrame` with the columns listed in :data:`ASSET_OWNER_COLUMNS`.
    """
    records: list[dict[str, Any]] = []
    processed = 0
    skipped = 0

    for document in documents:
        try:
            records.extend(transform_asset(document, gas=gas))
        except ValidationError as error:
            skipped += 1
            logger.warning("skipping malformed Climate TRACE document: {}", _first_error(error))
        else:
            processed += 1

    logger.info(
        "normalised {} source document(s) into {} owner row(s){}",
        processed,
        len(records),
        f", skipped {skipped} malformed document(s)" if skipped else "",
    )
    return build_asset_owner_frame(records)


def build_asset_owner_frame(records: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    """Build the typed DataFrame; an empty input still yields the full, typed schema."""
    frame = pd.DataFrame(list(records), columns=list(ASSET_OWNER_COLUMNS))
    return _coerce_dtypes(frame)


def _coerce_dtypes(frame: pd.DataFrame) -> pd.DataFrame:
    """Pin column dtypes so the frame can be registered in DuckDB without surprises."""
    for column in _INTEGER_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    for column in _FLOAT_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float64")
    for column in _BOOLEAN_COLUMNS:
        frame[column] = frame[column].fillna(False).astype(bool)
    for column in _STRING_COLUMNS:
        frame[column] = frame[column].astype("string")
    return frame.loc[:, list(ASSET_OWNER_COLUMNS)]
