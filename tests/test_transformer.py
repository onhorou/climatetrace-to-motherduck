"""Tests for :mod:`climate_trace_etl.transformer`."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
from loguru import logger

from climate_trace_etl.transformer import (
    ASSET_OWNER_COLUMNS,
    UNMAPPED_OWNER_NAME,
    build_asset_owner_frame,
    parse_source,
    select_latest_emissions,
    transform_asset,
    transform_assets,
)

#: Realistic ``GET /sources`` item (values taken from the live API response).
LIST_DOCUMENT: dict[str, Any] = {
    "id": 53059054,
    "name": "West Siberia - Conventional onshore",
    "sector": "fossil-fuel-operations",
    "subsector": "oil-and-gas-production",
    "country": "RUS",
    "assetType": "",
    "sourceType": "point-source",
    "centroid": {"longitude": 75.07028719, "latitude": 63.14992056, "srid": 4326},
    "gas": "co2e_100yr",
    "emissionsQuantity": 260_292_292.6,
    "year": 2025,
}


def detail_document(**overrides: Any) -> dict[str, Any]:
    """A ``GET /sources/:id``-style document, mirroring the live payload."""
    document: dict[str, Any] = {
        **LIST_DOCUMENT,
        "owners": None,
        "emissions": [{"year": 2025, "gas": "co2e_100yr", "emissionsQuantity": 260_292_292.6}],
        "totals": {"gas": "co2e_100yr", "emissionsQuantity": 260_292_292.6},
    }
    document.update(overrides)
    return document


def rows_of(document: dict[str, Any], **kwargs: Any) -> list[dict[str, Any]]:
    """Normalised records of a single document as plain dictionaries."""
    return [dict(record) for record in transform_asset(document, **kwargs)]


# ------------------------------------------------------------- owner handling ---
def test_facility_without_owners_is_attributed_to_the_placeholder() -> None:
    record = rows_of(LIST_DOCUMENT)[0]

    assert record["company_id"] is None
    assert record["company_name"] == UNMAPPED_OWNER_NAME
    assert record["owner_count"] == 0
    assert record["ownership_share"] == 1.0
    assert record["total_emissions_tco2e"] == pytest.approx(260_292_292.6)
    assert record["attributed_emissions_tco2e"] == pytest.approx(260_292_292.6)
    assert record["reporting_year"] == 2025
    assert record["emissions_reported"] is True


def test_null_owner_list_is_treated_like_a_missing_owner_list() -> None:
    assert rows_of(detail_document(owners=None))[0]["company_name"] == UNMAPPED_OWNER_NAME
    assert rows_of(detail_document(owners=[]))[0]["company_name"] == UNMAPPED_OWNER_NAME


def test_single_owner_keeps_full_ownership() -> None:
    document = detail_document(owners=[{"id": "E1", "name": "Acme Energy"}])
    record = rows_of(document)[0]

    assert record["company_id"] == "E1"
    assert record["company_name"] == "Acme Energy"
    assert record["owner_count"] == 1
    assert record["ownership_share"] == 1.0
    assert record["attributed_emissions_tco2e"] == pytest.approx(record["total_emissions_tco2e"])


def test_multiple_owners_split_the_ownership_equally() -> None:
    owners = [
        {"id": "E1", "name": "Acme Energy"},
        {"id": "E2", "name": "Beta Power"},
        {"id": "E3", "name": "Gamma Holdings"},
    ]
    records = rows_of(detail_document(owners=owners))

    assert [record["company_name"] for record in records] == [
        "Acme Energy",
        "Beta Power",
        "Gamma Holdings",
    ]
    assert [record["owner_index"] for record in records] == [0, 1, 2]
    assert {record["owner_count"] for record in records} == {3}
    assert all(record["ownership_share"] == pytest.approx(1 / 3) for record in records)
    assert sum(record["attributed_emissions_tco2e"] for record in records) == pytest.approx(
        records[0]["total_emissions_tco2e"]
    )


def test_ownership_shares_are_always_flagged_as_estimated() -> None:
    document = detail_document(owners=[{"id": "E1", "name": "Acme Energy"}])

    assert rows_of(document)[0]["ownership_share_is_estimated"] is True
    assert rows_of(LIST_DOCUMENT)[0]["ownership_share_is_estimated"] is True


def test_unnamed_owner_falls_back_to_its_identifier() -> None:
    document = detail_document(owners=[{"id": "E9", "name": None}])
    record = rows_of(document)[0]

    assert record["company_id"] == "E9"
    assert record["company_name"] == "E9"


def test_facility_metadata_is_carried_over() -> None:
    record = rows_of(detail_document(owners=[{"id": "E1", "name": "Acme"}]))[0]

    assert record["source_id"] == 53059054
    assert record["facility_name"] == "West Siberia - Conventional onshore"
    assert record["sector"] == "fossil-fuel-operations"
    assert record["subsector"] == "oil-and-gas-production"
    assert record["country"] == "RUS"
    assert record["source_type"] == "point-source"
    assert record["latitude"] == pytest.approx(63.14992056)
    assert record["longitude"] == pytest.approx(75.07028719)


# --------------------------------------------------------- emission selection ---
def test_latest_year_is_selected_and_months_are_summed() -> None:
    emissions = [
        {"year": 2023, "month": 1, "gas": "co2e_100yr", "emissionsQuantity": 10.0},
        {"year": 2023, "month": 2, "gas": "co2e_100yr", "emissionsQuantity": 5.0},
        {"year": 2024, "month": 1, "gas": "co2e_100yr", "emissionsQuantity": 7.0},
        {"year": 2024, "month": 2, "gas": "co2e_100yr", "emissionsQuantity": 3.0},
    ]
    record = rows_of(detail_document(emissions=emissions, totals=None))[0]

    assert record["reporting_year"] == 2024
    assert record["total_emissions_tco2e"] == pytest.approx(10.0)


def test_time_series_takes_precedence_over_the_list_scalars() -> None:
    document = detail_document(
        year=2022,
        emissionsQuantity=999.0,
        emissions=[{"year": 2024, "gas": "co2e_100yr", "emissionsQuantity": 42.0}],
        totals=None,
    )
    record = rows_of(document)[0]

    assert record["reporting_year"] == 2024
    assert record["total_emissions_tco2e"] == pytest.approx(42.0)


def test_totals_are_used_when_the_time_series_is_unavailable() -> None:
    document = detail_document(
        emissions=[],
        totals={"gas": "co2e_100yr", "emissionsQuantity": 123.5},
    )
    record = rows_of(document)[0]

    assert record["reporting_year"] == 2025  # taken from the list scalar `year`
    assert record["total_emissions_tco2e"] == pytest.approx(123.5)


def test_totals_of_another_gas_are_ignored() -> None:
    document = detail_document(
        year=None,
        emissionsQuantity=None,
        emissions=[{"year": 2024, "gas": "ch4", "emissionsQuantity": 5.0}],
        totals={"gas": "ch4", "emissionsQuantity": 5.0},
    )
    record = rows_of(document, gas="co2e_100yr")[0]

    assert record["emissions_reported"] is False
    assert record["total_emissions_tco2e"] == 0.0
    assert pd.isna(record["reporting_year"])


def test_time_series_of_another_gas_is_ignored() -> None:
    document = detail_document(
        year=None,
        emissionsQuantity=None,
        emissions=[{"year": 2024, "gas": "ch4", "emissionsQuantity": 5.0}],
        totals=None,
    )

    assert select_latest_emissions(parse_source(document), gas="co2e_100yr") is None


def test_document_gas_is_used_when_no_gas_is_requested() -> None:
    document = detail_document(
        emissions=[
            {"year": 2024, "gas": "ch4", "emissionsQuantity": 5.0},
            {"year": 2024, "gas": "co2e_100yr", "emissionsQuantity": 42.0},
        ],
        totals=None,
    )
    summary = select_latest_emissions(parse_source(document))

    assert summary is not None
    assert summary.gas == "co2e_100yr"
    assert summary.quantity == pytest.approx(42.0)


def test_requested_gas_overrides_the_document_gas() -> None:
    document = detail_document(
        emissions=[
            {"year": 2024, "gas": "ch4", "emissionsQuantity": 5.0},
            {"year": 2024, "gas": "co2e_100yr", "emissionsQuantity": 42.0},
        ],
        totals=None,
    )
    summary = select_latest_emissions(parse_source(document), gas="ch4")

    assert summary is not None
    assert summary.gas == "ch4"
    assert summary.quantity == pytest.approx(5.0)


def test_reported_zero_emissions_are_kept_as_zero() -> None:
    document = detail_document(
        emissionsQuantity=0.0,
        emissions=[{"year": 2025, "gas": "co2e_100yr", "emissionsQuantity": 0.0}],
        totals=None,
    )
    record = rows_of(document)[0]

    assert record["total_emissions_tco2e"] == 0.0
    assert record["emissions_reported"] is True


def test_missing_emissions_are_flagged_as_not_reported() -> None:
    document: dict[str, Any] = {
        "id": 7,
        "name": "Unreported plant",
        "sector": "power",
        "gas": "co2e_100yr",
        "year": None,
        "emissionsQuantity": None,
    }
    record = rows_of(document)[0]

    assert record["total_emissions_tco2e"] == 0.0
    assert record["emissions_reported"] is False
    assert pd.isna(record["reporting_year"])
    assert record["company_name"] == UNMAPPED_OWNER_NAME


def test_missing_coordinates_become_na() -> None:
    assert pd.isna(rows_of({"id": 8, "name": "No centroid"})[0]["latitude"])
    assert pd.isna(rows_of({"id": 8, "name": "No centroid"})[0]["longitude"])

    partial = {"id": 9, "name": "Partial", "centroid": {"latitude": None, "longitude": None}}
    record = rows_of(partial)[0]
    assert pd.isna(record["latitude"])
    assert pd.isna(record["longitude"])


# ------------------------------------------------------------- frame building ---
def test_transform_assets_combines_every_document() -> None:
    documents = [
        detail_document(owners=[{"id": "E1", "name": "Acme Energy"}]),
        detail_document(
            id=2,
            owners=[{"id": "E2", "name": "Beta Power"}, {"id": "E3", "name": "Gamma"}],
        ),
        LIST_DOCUMENT,
    ]
    frame = transform_assets(documents)

    assert list(frame.columns) == list(ASSET_OWNER_COLUMNS)
    assert len(frame) == 4
    assert set(frame["company_name"]) == {
        "Acme Energy",
        "Beta Power",
        "Gamma",
        UNMAPPED_OWNER_NAME,
    }


def test_attributed_emissions_follow_the_documented_formula() -> None:
    document = detail_document(
        emissionsQuantity=300.0,
        emissions=[{"year": 2025, "gas": "co2e_100yr", "emissionsQuantity": 300.0}],
        totals=None,
        owners=[
            {"id": "E1", "name": "A"},
            {"id": "E2", "name": "B"},
            {"id": "E3", "name": "C"},
        ],
    )
    frame = transform_assets([document])

    assert frame["total_emissions_tco2e"].tolist() == pytest.approx([300.0, 300.0, 300.0])
    assert frame["attributed_emissions_tco2e"].tolist() == pytest.approx([100.0, 100.0, 100.0])
    assert frame["ownership_share"].tolist() == pytest.approx([1 / 3] * 3)


def test_malformed_documents_are_skipped_with_a_warning() -> None:
    messages: list[str] = []
    sink_id = logger.add(messages.append, level="WARNING", format="{message}")

    try:
        frame = transform_assets([{"name": "no id"}, detail_document()])
    finally:
        logger.remove(sink_id)

    assert len(frame) == 1
    assert any("skipping malformed" in message for message in messages)


def test_empty_input_keeps_the_full_schema() -> None:
    frame = transform_assets([])

    assert frame.empty
    assert frame.shape == (0, len(ASSET_OWNER_COLUMNS))
    assert list(frame.columns) == list(ASSET_OWNER_COLUMNS)
    assert str(frame["source_id"].dtype) == "Int64"
    assert str(frame["total_emissions_tco2e"].dtype) == "float64"
    assert str(frame["company_name"].dtype) == "string"
    assert str(frame["emissions_reported"].dtype) == "bool"


def test_dtypes_are_pinned_for_loading() -> None:
    frame = transform_assets([detail_document(owners=[{"id": "E1", "name": "Acme"}])])

    expected = {
        "source_id": "Int64",
        "reporting_year": "Int64",
        "owner_index": "Int64",
        "owner_count": "Int64",
        "latitude": "float64",
        "longitude": "float64",
        "total_emissions_tco2e": "float64",
        "ownership_share": "float64",
        "attributed_emissions_tco2e": "float64",
        "emissions_reported": "bool",
        "ownership_share_is_estimated": "bool",
        "facility_name": "string",
        "gas": "string",
        "company_id": "string",
        "company_name": "string",
    }
    assert {column: str(frame[column].dtype) for column in expected} == expected


def test_build_asset_owner_frame_accepts_plain_records() -> None:
    frame = build_asset_owner_frame([])

    assert frame.shape == (0, len(ASSET_OWNER_COLUMNS))
