from __future__ import annotations

from typing import Any

import pytest

import main


class DummyResponse:
    def __init__(self, status_code: int, payload: dict[str, Any] | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise main.RequestException(f"status={self.status_code}")

    def json(self) -> dict[str, Any]:
        return self._payload


class DummySession:
    def __init__(self, steps: list[Any]) -> None:
        self.steps = steps
        self.calls = 0

    def get(self, url: str, params: dict[str, Any], timeout: int) -> DummyResponse:
        step = self.steps[self.calls]
        self.calls += 1
        if isinstance(step, Exception):
            raise step
        return step


def test_extract_konami_id_from_misc_info() -> None:
    card = {"misc_info": [{"konami_id": "1234"}]}
    assert main.extract_konami_id(card) == 1234


def test_get_json_retries_and_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    client = main.ApiClient(
        session=DummySession(
            [
                main.RequestException("timeout"),
                DummyResponse(status_code=200, payload={"ok": True}),
            ]
        )
    )
    monkeypatch.setattr(main, "sleep_rate", lambda: None)
    monkeypatch.setattr(main.time, "sleep", lambda _: None)

    result = client._get_json("https://example.test", {"misc": "yes"})

    assert result == {"ok": True}
    assert client.api_calls == 2


def test_get_json_fails_after_max_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    client = main.ApiClient(session=DummySession([main.RequestException("timeout")] * main.RETRY_MAX_ATTEMPTS))
    monkeypatch.setattr(main, "sleep_rate", lambda: None)
    monkeypatch.setattr(main.time, "sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="retries exhausted"):
        client._get_json("https://example.test", {"misc": "yes"})
