"""Tests for :mod:`climate_trace_etl.client`.

Every test runs against ``httpx.MockTransport``, so no network access is required and the
retry logic is exercised deterministically (the backoff is set to 1 ms).
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future
from typing import Any

import httpx
import pytest
from loguru import logger
from tenacity import RetryCallState

from climate_trace_etl.client import (
    RETRYABLE_STATUS_CODES,
    ClimateTraceClient,
    ClimateTraceHTTPError,
    ClimateTraceResponseError,
    RetryableHTTPError,
    RetryAfterWait,
    _parse_retry_after,
)
from climate_trace_etl.config import Settings

Handler = Callable[[httpx.Request], httpx.Response]


def source(source_id: int) -> dict[str, Any]:
    """Minimal stand-in for one ``GET /sources`` item."""
    return {
        "id": source_id,
        "name": f"Facility {source_id}",
        "sector": "power",
        "subsector": "electricity-generation",
        "country": "USA",
        "gas": "co2e_100yr",
        "emissionsQuantity": float(source_id) * 10,
        "year": 2024,
    }


def make_client(handler: Handler, **overrides: Any) -> ClimateTraceClient:
    """Return a client bound to a mock transport with a 2-record page size."""
    settings = Settings(_env_file=None, fetch_limit=2, retry_backoff_seconds=0.001, **overrides)
    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    return ClimateTraceClient(settings=settings, http_client=http_client)


def retry_state_with(error: BaseException) -> RetryCallState:
    """Build the retry state tenacity would hand to a wait strategy."""
    state = RetryCallState(retry_object=None, fn=None, args=(), kwargs={})
    outcome: Future[Any] = Future()
    outcome.set_exception(error)
    state.outcome = outcome
    return state


def query_of(request: httpx.Request) -> dict[str, str]:
    """Query parameters of a recorded request as a plain dictionary."""
    return dict(request.url.params)


# ---------------------------------------------------------------- extraction ---
def test_fetch_sources_requests_the_documented_endpoint() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[source(1)])

    with make_client(handler, emissions_year=2024) as client:
        records = client.fetch_sources()

    assert records == [source(1)]
    assert len(requests) == 1
    url = requests[0].url
    assert (url.scheme, url.host, url.path) == ("https", "api.climatetrace.org", "/v7/sources")
    expected = {"gas": "co2e_100yr", "year": "2024", "limit": "2", "offset": "0"}
    assert query_of(requests[0]) == expected


def test_request_headers_describe_the_client() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[])

    with make_client(handler) as client:
        client.fetch_sources()

    assert requests[0].headers["accept"] == "application/json"
    assert requests[0].headers["user-agent"].startswith("climate-trace-etl/")


def test_year_parameter_is_omitted_when_no_year_is_configured() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[])

    with make_client(handler) as client:
        client.fetch_sources()

    assert "year" not in query_of(requests[0])


def test_extra_filters_are_forwarded_and_none_values_are_dropped() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[])

    with make_client(handler) as client:
        client.fetch_sources(sectors="power", countryGroup=None)

    params = query_of(requests[0])
    assert params["sectors"] == "power"
    assert "countryGroup" not in params


def test_explicit_page_size_and_offset_are_used() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[])

    with make_client(handler) as client:
        assert client.fetch_sources(limit=50, offset=100) == []

    assert query_of(requests[0])["limit"] == "50"
    assert query_of(requests[0])["offset"] == "100"


def test_iter_sources_paginates_until_a_short_page() -> None:
    pages = {"0": [source(1), source(2)], "2": [source(3)]}
    offsets: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        offset = query_of(request)["offset"]
        offsets.append(offset)
        return httpx.Response(200, json=pages.get(offset, []))

    with make_client(handler) as client:
        records = client.fetch_sources()
        assert client.request_count == 2

    assert [record["id"] for record in records] == [1, 2, 3]
    assert offsets == ["0", "2"]


def test_empty_first_page_yields_no_records() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    with make_client(handler) as client:
        assert client.fetch_sources() == []
        assert client.request_count == 1


def test_pagination_stops_at_max_records() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        offset = int(query_of(request)["offset"])
        return httpx.Response(200, json=[source(offset + 1), source(offset + 2)])

    with make_client(handler) as client:
        records = client.fetch_sources(max_records=3)

    assert [record["id"] for record in records] == [1, 2, 3]
    assert len(requests) == 2


def test_repeated_page_stops_pagination() -> None:
    messages: list[str] = []
    sink_id = logger.add(messages.append, level="WARNING", format="{message}")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[source(1), source(2)])

    try:
        with make_client(handler) as client:
            records = client.fetch_sources()
            assert client.request_count == 2
    finally:
        logger.remove(sink_id)

    assert len(records) == 2
    assert any("repeats an earlier page" in message for message in messages)


def test_zero_page_size_is_rejected() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    with make_client(handler) as client, pytest.raises(ValueError, match="limit"):
        client.fetch_sources(limit=0)


# -------------------------------------------------------------------- retries ---
@pytest.mark.parametrize("status_code", sorted(RETRYABLE_STATUS_CODES))
def test_transient_status_codes_are_retried(status_code: int) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            headers = {"retry-after": "0"}
            return httpx.Response(status_code, headers=headers, json={"detail": "try later"})
        return httpx.Response(200, json=[source(1)])

    with make_client(handler, max_retries=3) as client:
        assert client.fetch_sources() == [source(1)]
        assert client.request_count == 2
        assert client.retry_count == 1

    assert attempts == 2


def test_retryable_error_is_raised_after_exhausting_attempts() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(503, headers={"retry-after": "0"}, json={"detail": "down"})

    with make_client(handler, max_retries=2) as client, pytest.raises(RetryableHTTPError) as info:
        client.fetch_sources()

    assert attempts == 3  # initial attempt + 2 retries
    assert info.value.status_code == 503
    assert info.value.retry_after == 0.0
    assert "down" in (info.value.detail or "")


def test_non_retryable_errors_fail_fast() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(404, json={"detail": "unknown source"})

    with (
        make_client(handler, max_retries=3) as client,
        pytest.raises(ClimateTraceHTTPError) as info,
    ):
        client.fetch_sources()

    assert attempts == 1
    assert not isinstance(info.value, RetryableHTTPError)
    assert info.value.status_code == 404


def test_transport_errors_are_retried() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json=[source(1)])

    with make_client(handler, max_retries=2) as client:
        assert client.fetch_sources() == [source(1)]
        assert client.retry_count == 1

    assert attempts == 2


def test_retry_attempts_are_logged() -> None:
    messages: list[str] = []
    sink_id = logger.add(messages.append, level="WARNING", format="{message}")
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(500, json={})
        return httpx.Response(200, json=[source(1)])

    try:
        with make_client(handler) as client:
            client.fetch_sources()
    finally:
        logger.remove(sink_id)

    assert any("retrying in" in message for message in messages)


# ------------------------------------------------------------------- payloads ---
def test_invalid_json_raises_a_response_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>not json</html>")

    with make_client(handler) as client, pytest.raises(ClimateTraceResponseError, match="invalid"):
        client.fetch_sources()


def test_unexpected_payload_shape_raises_a_response_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"detail": "unexpected"})

    with make_client(handler) as client, pytest.raises(ClimateTraceResponseError, match="list"):
        client.fetch_sources()


def test_fetch_source_returns_the_detail_document() -> None:
    requests: list[httpx.Request] = []
    detail = {**source(53059054), "owners": None, "totals": {"gas": "co2e_100yr"}}

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=detail)

    with make_client(handler) as client:
        payload = client.fetch_source(53059054)

    assert payload == detail
    assert payload["owners"] is None
    assert requests[0].url.path == "/v7/sources/53059054"


def test_fetch_source_rejects_a_non_object_payload() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[source(1)])

    with make_client(handler) as client, pytest.raises(ClimateTraceResponseError, match="object"):
        client.fetch_source(1)


def test_search_owners_passes_the_name_filter() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[{"id": "abc", "name": "Petrobras"}])

    with make_client(handler) as client:
        owners = client.search_owners("Petro")

    assert owners == [{"id": "abc", "name": "Petrobras"}]
    assert requests[0].url.path == "/v7/owners"
    assert query_of(requests[0]) == {"limit": "100", "offset": "0", "name": "Petro"}


def test_search_owners_without_a_name_omits_the_filter() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[])

    with make_client(handler) as client:
        assert client.search_owners() == []

    assert "name" not in query_of(requests[0])


# ------------------------------------------------------------------ lifecycle ---
def test_client_closes_the_underlying_http_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    client = make_client(handler)
    assert client.closed is False
    with client:
        assert client.closed is False
    assert client.closed is True
    client.close()  # idempotent


# ----------------------------------------------------------------- wait policy ---
def test_retry_after_header_overrides_the_exponential_backoff() -> None:
    wait = RetryAfterWait(initial=1.0, maximum=60.0)

    assert wait(retry_state_with(RetryableHTTPError("boom", retry_after=7.5))) == 7.5
    assert wait(retry_state_with(RetryableHTTPError("boom", retry_after=500.0))) == 60.0


def test_exponential_backoff_is_bounded_without_a_retry_after_header() -> None:
    delay = RetryAfterWait(initial=1.0, maximum=8.0)(retry_state_with(RetryableHTTPError("boom")))

    assert 0 < delay <= 8.0


def test_non_numeric_retry_after_headers_are_ignored() -> None:
    assert _parse_retry_after(httpx.Response(429, headers={"retry-after": "2.5"})) == 2.5
    assert _parse_retry_after(httpx.Response(429, headers={"retry-after": "-3"})) == 0.0
    assert _parse_retry_after(httpx.Response(503)) is None
    http_date = {"retry-after": "Wed, 21 Oct 2026 07:28:00 GMT"}
    assert _parse_retry_after(httpx.Response(503, headers=http_date)) is None
