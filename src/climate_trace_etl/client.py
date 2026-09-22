"""Resilient HTTP client for the Climate TRACE API v7.

Endpoints used (documented at <https://api.climatetrace.org/v7/docs>):

* ``GET /sources``     - ranked emission sources ("assets"), paged with ``limit``/``offset``.
* ``GET /sources/:id`` - single facility detail, including ``owners`` and yearly ``emissions``.
* ``GET /owners``      - owner (company) lookup by name.

Transient failures (HTTP ``429``/``500``/``502``/``503``/``504`` and transport errors such as
connection resets or timeouts) are retried with an exponential backoff that honours a
server-provided ``Retry-After`` header. Everything else fails fast with a
:class:`ClimateTraceAPIError`, so the pipeline never retries a request that cannot succeed.

The client returns raw JSON documents; schema validation belongs to
:mod:`climate_trace_etl.transformer`.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from typing import Any

import httpx
from loguru import logger
from tenacity import RetryCallState, Retrying, retry_if_exception_type, stop_after_attempt
from tenacity import wait as tenacity_wait

from climate_trace_etl import __version__
from climate_trace_etl.config import Settings, get_settings

#: HTTP status codes considered transient and therefore retried.
RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})

#: Hard stop for pagination, so a misbehaving API cannot spin forever.
MAX_PAGES: int = 1_000

#: Maximum number of characters of a response body kept in error messages.
BODY_SNIPPET_LENGTH: int = 200


class ClimateTraceAPIError(RuntimeError):
    """Base class for every failure raised by :class:`ClimateTraceClient`."""

    def __init__(
        self,
        message: str,
        *,
        url: str | None = None,
        status_code: int | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code
        self.detail = detail


class ClimateTraceHTTPError(ClimateTraceAPIError):
    """Non-retryable HTTP error, for example ``400 Bad Request`` or ``404 Not Found``."""


class RetryableHTTPError(ClimateTraceHTTPError):
    """Transient HTTP error (``429``/``5xx``) that carries an optional ``Retry-After`` hint."""

    def __init__(self, message: str, *, retry_after: float | None = None, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class ClimateTraceResponseError(ClimateTraceAPIError):
    """The API answered with HTTP 200 but not with the expected JSON shape."""


#: Exceptions that trigger another attempt.
RETRYABLE_EXCEPTIONS: tuple[type[Exception], ...] = (httpx.TransportError, RetryableHTTPError)


def _exception_of(retry_state: RetryCallState) -> BaseException | None:
    """Return the exception of ``retry_state``, if the last attempt failed."""
    outcome = retry_state.outcome
    return None if outcome is None else outcome.exception()


def _parse_retry_after(response: httpx.Response) -> float | None:
    """Return the ``Retry-After`` delay in seconds, ignoring non-numeric (HTTP-date) values."""
    raw = response.headers.get("retry-after")
    if raw is None:
        return None
    try:
        return max(float(raw), 0.0)
    except ValueError:
        logger.debug("ignoring non-numeric Retry-After header: {!r}", raw)
        return None


def _body_snippet(response: httpx.Response) -> str:
    """First :data:`BODY_SNIPPET_LENGTH` characters of a response body, for error messages."""
    try:
        return response.text[:BODY_SNIPPET_LENGTH]
    except (UnicodeDecodeError, httpx.ResponseNotRead):  # pragma: no cover - defensive
        return "<unreadable body>"


def default_headers() -> dict[str, str]:
    """Headers sent with every request, independently of the underlying ``httpx.Client``."""
    return {
        "accept": "application/json",
        "user-agent": f"climate-trace-etl/{__version__}",
    }


class RetryAfterWait(tenacity_wait.wait_base):
    """Exponential backoff with jitter that prefers a server-provided ``Retry-After`` delay."""

    def __init__(self, initial: float = 1.0, maximum: float = 60.0) -> None:
        self._maximum = maximum
        self._exponential = tenacity_wait.wait_exponential_jitter(initial=initial, max=maximum)

    def __call__(self, retry_state: RetryCallState) -> float:
        """Delay before the next attempt: ``Retry-After`` when present, else exponential."""
        error = _exception_of(retry_state)
        retry_after = getattr(error, "retry_after", None)
        if retry_after is not None:
            return min(float(retry_after), self._maximum)
        return float(self._exponential(retry_state))


class ClimateTraceClient:
    """Thin, retrying, testable wrapper around the Climate TRACE HTTP API.

    The client is a context manager::

        with ClimateTraceClient() as client:
            for source in client.iter_sources(max_records=500):
                ...

    Args:
        settings: Configuration to use; defaults to
            :func:`climate_trace_etl.config.get_settings`.
        http_client: Pre-built ``httpx.Client`` (tests inject a mock transport here). It is
            closed by :meth:`close`, whether it was injected or created internally.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.request_count = 0
        self.retry_count = 0
        self._lock = threading.Lock()
        self._http = http_client or httpx.Client(
            timeout=httpx.Timeout(self.settings.request_timeout_seconds),
            follow_redirects=True,
        )
        self.headers = default_headers()
        self._retrying: Retrying = Retrying(
            stop=stop_after_attempt(self.settings.max_retries + 1),
            wait=RetryAfterWait(
                initial=self.settings.retry_backoff_seconds,
                maximum=self.settings.retry_backoff_seconds * 8,
            ),
            retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
            before_sleep=self._log_retry,
            reraise=True,
        )

    # ------------------------------------------------------------- lifecycle ---
    def __enter__(self) -> ClimateTraceClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    @property
    def closed(self) -> bool:
        """Whether the underlying ``httpx.Client`` has been closed."""
        return self._http.is_closed

    def close(self) -> None:
        """Close the underlying HTTP client. Safe to call more than once."""
        if not self._http.is_closed:
            self._http.close()

    # ------------------------------------------------------------ extraction ---
    def iter_sources(
        self,
        *,
        limit: int | None = None,
        offset: int = 0,
        max_records: int | None = None,
        **filters: Any,
    ) -> Iterator[dict[str, Any]]:
        """Yield emission sources from ``GET /sources``, paging until the API is exhausted.

        Args:
            limit: Page size, defaults to ``FETCH_LIMIT``.
            offset: Index of the first record, useful to resume an interrupted extraction.
            max_records: Safety cap on the number of yielded records (bounds the runtime).
            **filters: Additional documented query parameters, for example
                ``sectors="power"``, ``countryGroup="EUU"`` or ``ownerIds="abc,def"``.
                ``None`` values are dropped.

        Yields:
            Raw source documents exactly as returned by the API.

        Raises:
            ValueError: if ``limit`` is smaller than ``1``.
        """
        page_size = self.settings.fetch_limit if limit is None else limit
        if page_size < 1:
            raise ValueError(f"limit must be >= 1, got {page_size}")

        params = self._sources_params(filters)
        seen_pages: set[tuple[Any, ...]] = set()
        yielded = 0

        for page_number in range(1, MAX_PAGES + 1):
            if max_records is not None and yielded >= max_records:
                return

            page = self._get_json("sources", {**params, "limit": page_size, "offset": offset})
            items = self._expect_list(page, endpoint="sources")
            if not items:
                logger.debug("sources page {} is empty: pagination complete", page_number)
                return

            signature = tuple(item.get("id") for item in items)
            if signature in seen_pages:
                logger.warning(
                    "sources page {} repeats an earlier page; stopping pagination.", page_number
                )
                return
            seen_pages.add(signature)

            for item in items:
                yield item
                yielded += 1
                if max_records is not None and yielded >= max_records:
                    logger.debug("reached max_records={}, stopping pagination", max_records)
                    return

            if len(items) < page_size:
                return
            offset += len(items)

        logger.warning("pagination stopped after the {} page safety limit.", MAX_PAGES)

    def fetch_sources(
        self,
        *,
        limit: int | None = None,
        offset: int = 0,
        max_records: int | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Materialise :meth:`iter_sources` into a list of raw source documents."""
        return list(
            self.iter_sources(limit=limit, offset=offset, max_records=max_records, **filters)
        )

    def fetch_source(self, source_id: int | str) -> dict[str, Any]:
        """Return the detailed document of one facility (``GET /sources/:id``)."""
        payload = self._get_json(f"sources/{source_id}", {})
        return self._expect_mapping(payload, endpoint=f"sources/{source_id}")

    def iter_source_details(
        self,
        source_ids: Iterable[int | str],
        *,
        max_records: int | None = None,
        workers: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield ``GET /sources/:id`` documents for ``source_ids``.

        The ``/sources`` list payload carries no ownership information, so facilities have to be
        enriched individually — one request each. Requests run concurrently on ``ENRICH_WORKERS``
        threads (``1`` disables concurrency) while the results keep the order of ``source_ids``,
        and ``max_records`` bounds how many facilities are enriched.

        Args:
            source_ids: Identifier of the facilities to enrich.
            max_records: Maximum number of documents to fetch; ``None`` means no limit.
            workers: Overrides ``ENRICH_WORKERS`` for this call.

        Yields:
            Raw detail documents, in the order of ``source_ids``.
        """
        selected = source_ids if max_records is None else islice(source_ids, max_records)
        if max_records is not None:
            logger.debug("enriching at most {} facility detail(s)", max_records)

        worker_count = self.settings.enrich_workers if workers is None else workers
        if worker_count <= 1:
            for source_id in selected:
                yield self.fetch_source(source_id)
            return

        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="enrich") as pool:
            # ``map`` streams results as requests complete but yields them in input order.
            yield from pool.map(self.fetch_source, selected)

    def search_owners(
        self,
        name: str | None = None,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Look up owners (companies) by name through ``GET /owners``."""
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if name:
            params["name"] = name
        payload = self._get_json("owners", params)
        return self._expect_list(payload, endpoint="owners")

    # --------------------------------------------------------------- transport ---
    def _get_json(self, path: str, params: Mapping[str, Any]) -> Any:
        """Perform one retried GET request and decode its JSON body."""
        response = self._retrying(self._get_once, path, params)
        try:
            return response.json()
        except ValueError as error:
            raise ClimateTraceResponseError(
                f"Climate TRACE API returned invalid JSON for {response.request.url}",
                url=str(response.request.url),
                status_code=response.status_code,
                detail=_body_snippet(response),
            ) from error

    def _get_once(self, path: str, params: Mapping[str, Any]) -> httpx.Response:
        """Single GET attempt; raises exactly the exception that the retry policy inspects."""
        url = self._url(path)
        clean_params = {key: value for key, value in params.items() if value is not None}
        with self._lock:
            self.request_count += 1
        logger.debug("GET {} params={}", url, clean_params)

        response = self._http.get(url, params=clean_params, headers=self.headers)
        if response.status_code in RETRYABLE_STATUS_CODES:
            raise RetryableHTTPError(
                f"Climate TRACE API answered {response.status_code} for {url}",
                url=str(response.request.url),
                status_code=response.status_code,
                detail=_body_snippet(response),
                retry_after=_parse_retry_after(response),
            )
        if response.is_error:
            raise ClimateTraceHTTPError(
                f"Climate TRACE API answered {response.status_code} for {url}",
                url=str(response.request.url),
                status_code=response.status_code,
                detail=_body_snippet(response),
            )
        return response

    def _url(self, path: str) -> str:
        """Absolute URL of ``path`` relative to the configured API base URL."""
        return f"{self.settings.api_base_url}/{path.lstrip('/')}"

    def _sources_params(self, filters: Mapping[str, Any]) -> dict[str, Any]:
        """Default ``/sources`` query parameters, overridable through ``filters``."""
        params: dict[str, Any] = {"gas": self.settings.emissions_gas}
        if self.settings.emissions_year is not None:
            params["year"] = self.settings.emissions_year
        params.update({key: value for key, value in filters.items() if value is not None})
        return params

    def _log_retry(self, retry_state: RetryCallState) -> None:
        """Log a pending retry (wired into tenacity as its ``before_sleep`` hook)."""
        with self._lock:
            self.retry_count += 1
        action = retry_state.next_action
        delay = 0.0 if action is None else action.sleep
        logger.warning(
            "Climate TRACE request failed (attempt {}): {} - retrying in {:.2f}s",
            retry_state.attempt_number,
            _exception_of(retry_state),
            delay,
        )

    @staticmethod
    def _expect_list(payload: Any, *, endpoint: str) -> list[dict[str, Any]]:
        """Validate that ``payload`` is a JSON array of objects."""
        if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
            raise ClimateTraceResponseError(
                f"expected a JSON list of objects from Climate TRACE endpoint '{endpoint}'",
                detail=str(payload)[:BODY_SNIPPET_LENGTH],
            )
        return payload

    @staticmethod
    def _expect_mapping(payload: Any, *, endpoint: str) -> dict[str, Any]:
        """Validate that ``payload`` is a JSON object."""
        if not isinstance(payload, dict):
            raise ClimateTraceResponseError(
                f"expected a JSON object from Climate TRACE endpoint '{endpoint}'",
                detail=str(payload)[:BODY_SNIPPET_LENGTH],
            )
        return payload
