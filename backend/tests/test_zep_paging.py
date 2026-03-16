from types import SimpleNamespace

import pytest
from zep_cloud.core.api_error import ApiError

from app.services import graph_builder as graph_builder_module
from app.services.graph_builder import GraphBuilderService
from app.utils import zep_paging as zep_paging_module


def test_call_with_retry_respects_retry_after(monkeypatch):
    sleep_calls = []
    monkeypatch.setattr(zep_paging_module.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    attempts = {"count": 0}

    def api_call():
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise ApiError(
                status_code=429,
                headers={"retry-after": "7"},
                body="Rate limit exceeded for FREE plan",
            )
        return ["ok"]

    result = zep_paging_module.call_with_retry(api_call, operation_description="fetch nodes")

    assert result == ["ok"]
    assert attempts["count"] == 2
    assert sleep_calls == [7.0]


def test_call_with_retry_raises_friendly_rate_limit_error(monkeypatch):
    monkeypatch.setattr(zep_paging_module.time, "sleep", lambda _: None)

    def api_call():
        raise ApiError(
            status_code=429,
            headers={"retry-after": "60"},
            body="Rate limit exceeded for FREE plan",
        )

    with pytest.raises(zep_paging_module.ZepRateLimitError) as exc_info:
        zep_paging_module.call_with_retry(
            api_call,
            max_retries=2,
            operation_description="fetch nodes page 1",
        )

    assert exc_info.value.retry_after_seconds == 60.0
    assert "fetch nodes page 1" in str(exc_info.value)
    assert "60 seconds" in str(exc_info.value)


def test_wait_for_episodes_polls_by_graph_id(monkeypatch):
    builder = GraphBuilderService.__new__(GraphBuilderService)

    poll_calls = []
    responses = [
        SimpleNamespace(
            episodes=[
                SimpleNamespace(uuid_="ep-1", processed=True),
                SimpleNamespace(uuid_="ep-2", processed=False),
            ]
        ),
        SimpleNamespace(
            episodes=[
                SimpleNamespace(uuid_="ep-1", processed=True),
                SimpleNamespace(uuid_="ep-2", processed=True),
            ]
        ),
    ]

    def get_by_graph_id(graph_id, *, lastn=None, request_options=None):
        poll_calls.append((graph_id, lastn))
        return responses[min(len(poll_calls) - 1, len(responses) - 1)]

    builder.client = SimpleNamespace(
        graph=SimpleNamespace(
            episode=SimpleNamespace(get_by_graph_id=get_by_graph_id)
        )
    )

    monkeypatch.setattr(graph_builder_module.time, "sleep", lambda _: None)

    progress_updates = []
    builder._wait_for_episodes(
        "graph-123",
        ["ep-1", "ep-2"],
        progress_callback=lambda message, progress: progress_updates.append((message, progress)),
        timeout=5,
        poll_interval=0,
    )

    assert poll_calls == [("graph-123", 2), ("graph-123", 2)]
    assert progress_updates[-1] == ("Xu ly hoan tat: 2/2", 1.0)
