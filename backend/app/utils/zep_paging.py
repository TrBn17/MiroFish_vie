"""Helpers for interacting with Zep under pagination, transient failures, and rate limits."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, TypeVar

from zep_cloud import InternalServerError
from zep_cloud.client import Zep
from zep_cloud.core.api_error import ApiError

from .logger import get_logger

logger = get_logger('mirofish.zep_paging')

T = TypeVar("T")

_DEFAULT_PAGE_SIZE = 100
_MAX_NODES = 2000
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_RETRY_DELAY = 2.0  # seconds, doubles each retry
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class ZepRateLimitError(RuntimeError):
    """Raised when the Zep API keeps returning 429 after retries are exhausted."""

    def __init__(
        self,
        operation_description: str,
        retry_after_seconds: float | None = None,
        original_error: ApiError | None = None,
    ) -> None:
        self.operation_description = operation_description
        self.retry_after_seconds = retry_after_seconds
        self.original_error = original_error

        message = f"Zep rate limit exceeded while attempting to {operation_description}"
        if retry_after_seconds is not None:
            rounded_seconds = int(retry_after_seconds)
            if retry_after_seconds > rounded_seconds:
                rounded_seconds += 1
            message += f". Retry after about {rounded_seconds} seconds."
        else:
            message += ". Retry later."

        super().__init__(message)


def _parse_retry_after_seconds(headers: dict[str, str] | None) -> float | None:
    if not headers:
        return None

    retry_after = headers.get("retry-after") or headers.get("Retry-After")
    if not retry_after:
        return None

    try:
        return max(float(retry_after), 0.0)
    except (TypeError, ValueError):
        try:
            retry_after_datetime = parsedate_to_datetime(retry_after)
        except (TypeError, ValueError, IndexError):
            return None

        if retry_after_datetime.tzinfo is None:
            retry_after_datetime = retry_after_datetime.replace(tzinfo=timezone.utc)
        return max((retry_after_datetime - datetime.now(timezone.utc)).total_seconds(), 0.0)


def get_retry_after_seconds(error: Exception) -> float | None:
    if isinstance(error, ZepRateLimitError):
        return error.retry_after_seconds

    if isinstance(error, ApiError):
        return _parse_retry_after_seconds(error.headers)

    return None


def is_rate_limit_error(error: Exception) -> bool:
    if isinstance(error, ZepRateLimitError):
        return True

    return isinstance(error, ApiError) and error.status_code == 429


def _is_retryable_error(error: Exception) -> bool:
    if isinstance(error, (ConnectionError, TimeoutError, OSError, InternalServerError)):
        return True

    return isinstance(error, ApiError) and error.status_code in _RETRYABLE_STATUS_CODES


def call_with_retry(
    api_call: Callable[..., T],
    *args: Any,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    retry_delay: float = _DEFAULT_RETRY_DELAY,
    operation_description: str = "operation",
    **kwargs: Any,
) -> T:
    """Run a Zep API call with retries for transient failures and rate limits."""

    if max_retries < 1:
        raise ValueError("max_retries must be >= 1")

    last_exception: Exception | None = None
    delay = retry_delay

    for attempt in range(max_retries):
        try:
            return api_call(*args, **kwargs)
        except Exception as error:
            if not _is_retryable_error(error):
                raise

            last_exception = error
            retry_after_seconds = get_retry_after_seconds(error)

            if attempt < max_retries - 1:
                sleep_seconds = retry_after_seconds if retry_after_seconds is not None else delay
                logger.warning(
                    f"Zep {operation_description} attempt {attempt + 1} failed: {str(error)[:150]}, "
                    f"retrying in {sleep_seconds:.1f}s..."
                )
                time.sleep(sleep_seconds)
                if retry_after_seconds is None:
                    delay *= 2
            else:
                logger.error(
                    f"Zep {operation_description} failed after {max_retries} attempts: {str(error)}"
                )

    assert last_exception is not None
    if is_rate_limit_error(last_exception):
        raise ZepRateLimitError(
            operation_description=operation_description,
            retry_after_seconds=get_retry_after_seconds(last_exception),
            original_error=last_exception if isinstance(last_exception, ApiError) else None,
        ) from last_exception

    raise last_exception


def _fetch_page_with_retry(
    api_call: Callable[..., list[Any]],
    *args: Any,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    retry_delay: float = _DEFAULT_RETRY_DELAY,
    page_description: str = "page",
    **kwargs: Any,
) -> list[Any]:
    """Fetch a single page with exponential-backoff retries for transient network or I/O failures."""
    return call_with_retry(
        api_call,
        *args,
        max_retries=max_retries,
        retry_delay=retry_delay,
        operation_description=page_description,
        **kwargs,
    )


def fetch_all_nodes(
    client: Zep,
    graph_id: str,
    page_size: int = _DEFAULT_PAGE_SIZE,
    max_items: int = _MAX_NODES,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    retry_delay: float = _DEFAULT_RETRY_DELAY,
) -> list[Any]:
    """Fetch graph nodes with pagination, returning at most `max_items` entries with retries per page."""
    all_nodes: list[Any] = []
    cursor: str | None = None
    page_num = 0

    while True:
        kwargs: dict[str, Any] = {"limit": page_size}
        if cursor is not None:
            kwargs["uuid_cursor"] = cursor

        page_num += 1
        batch = _fetch_page_with_retry(
            client.graph.node.get_by_graph_id,
            graph_id,
            max_retries=max_retries,
            retry_delay=retry_delay,
            page_description=f"fetch nodes page {page_num} (graph={graph_id})",
            **kwargs,
        )
        if not batch:
            break

        all_nodes.extend(batch)
        if len(all_nodes) >= max_items:
            all_nodes = all_nodes[:max_items]
            logger.warning(f"Node count reached limit ({max_items}), stopping pagination for graph {graph_id}")
            break
        if len(batch) < page_size:
            break

        cursor = getattr(batch[-1], "uuid_", None) or getattr(batch[-1], "uuid", None)
        if cursor is None:
            logger.warning(f"Node missing uuid field, stopping pagination at {len(all_nodes)} nodes")
            break

    return all_nodes


def fetch_all_edges(
    client: Zep,
    graph_id: str,
    page_size: int = _DEFAULT_PAGE_SIZE,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    retry_delay: float = _DEFAULT_RETRY_DELAY,
) -> list[Any]:
    """Fetch all graph edges with pagination and retries per page."""
    all_edges: list[Any] = []
    cursor: str | None = None
    page_num = 0

    while True:
        kwargs: dict[str, Any] = {"limit": page_size}
        if cursor is not None:
            kwargs["uuid_cursor"] = cursor

        page_num += 1
        batch = _fetch_page_with_retry(
            client.graph.edge.get_by_graph_id,
            graph_id,
            max_retries=max_retries,
            retry_delay=retry_delay,
            page_description=f"fetch edges page {page_num} (graph={graph_id})",
            **kwargs,
        )
        if not batch:
            break

        all_edges.extend(batch)
        if len(batch) < page_size:
            break

        cursor = getattr(batch[-1], "uuid_", None) or getattr(batch[-1], "uuid", None)
        if cursor is None:
            logger.warning(f"Edge missing uuid field, stopping pagination at {len(all_edges)} edges")
            break

    return all_edges
