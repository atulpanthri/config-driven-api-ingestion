import json
from dataclasses import replace

import pytest
import requests

from ingest_framework.api_client import (
    backoff_delay, build_request, fetch_entity, fetch_once, parse_retry_after, render, render_params,
)
from ingest_framework.fault_injection import SimpleResponse


class ScriptedSession:
    """Returns/raises the scripted items in order and records every call."""

    def __init__(self, *script):
        self.script = list(script)
        self.calls = []

    def request(self, method, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {}), "headers": dict(headers or {})})
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def ok(body='{"hourly": {}}'):
    return SimpleResponse(200, body)


def run(cfg, *script, entity_id="tokyo"):
    sleeps = []
    session = ScriptedSession(*script)
    req = build_request(cfg, cfg.entity(entity_id), {"forecast_days": 3})
    result = fetch_once(session, cfg, req, entity_id=entity_id, sleep=sleeps.append, rng=lambda: 0.5)
    return result, session, sleeps


# ---------------------------------------------------------------- templating / request building
def test_render_keeps_types_and_omits_nulls(weather):
    tokyo = weather.entity("tokyo")
    assert render("{entity.latitude}", tokyo, {}) == 35.6762
    assert render("lat={entity.latitude}", tokyo, {}) == "lat=35.6762"
    params = render_params(weather.params, tokyo, {"forecast_days": 3, "start_date": None, "end_date": None})
    assert params["forecast_days"] == 3 and "start_date" not in params and params["timezone"] == "UTC"


def test_build_request_weather(weather):
    req = build_request(weather, weather.entity("tokyo"), {"forecast_days": 3})
    assert req.params["latitude"] == 35.6762
    assert "latitude=35.6762" in req.redacted_url() and "forecast_days=3" in req.redacted_url()


@pytest.mark.parametrize("where", ["query", "header"])
def test_api_key_injected_and_redacted(weather, where):
    cfg = replace(weather, auth={"type": "api_key", "in": where, "name": "X-API-Key" if where == "header" else "apikey",
                                 "secret_scope": "scope", "secret_key": "key"})
    req = build_request(cfg, cfg.entity("tokyo"), {"forecast_days": 3}, secret_resolver=lambda s, k: "s3cr3t")
    if where == "query":
        assert req.params["apikey"] == "s3cr3t" and req.redacted_params()["apikey"] == "***"
    else:
        assert req.headers["X-API-Key"] == "s3cr3t"
    assert "s3cr3t" not in req.redacted_url()
    session = ScriptedSession(SimpleResponse(401, "bad key s3cr3t"))
    result = fetch_once(session, cfg, req, entity_id="tokyo", sleep=lambda s: None)
    assert "s3cr3t" not in result.error_message and "s3cr3t" not in result.request_url
    assert "s3cr3t" not in json.dumps(result.request_params)


def test_api_key_requires_resolver(weather):
    cfg = replace(weather, auth={"type": "api_key", "in": "query", "name": "k", "secret_scope": "s", "secret_key": "k"})
    with pytest.raises(RuntimeError, match="secret_resolver"):
        build_request(cfg, cfg.entity("tokyo"), {})


# ---------------------------------------------------------------- retry policy (unchanged semantics)
def test_success_first_attempt(weather):
    result, session, sleeps = run(weather, ok("BODY"))
    assert result.ok and result.attempts == 1 and result.body_text == "BODY" and sleeps == []


def test_400_not_retried_body_kept(weather):
    body = '{"reason":"Latitude must be in range","error":true}'
    result, session, sleeps = run(weather, SimpleResponse(400, body), ok())
    assert not result.ok and len(session.calls) == 1 and sleeps == []
    assert result.error_type == "HTTP_4XX" and result.body_text == body


def test_429_honors_retry_after(weather):
    result, _, sleeps = run(weather, SimpleResponse(429, "", {"Retry-After": "2"}), ok())
    assert result.ok and result.attempts == 2 and sleeps == [2.0]


def test_retry_after_capped(weather):
    _, _, sleeps = run(weather, SimpleResponse(429, "", {"Retry-After": "9999"}), ok())
    assert sleeps == [weather.retry.retry_after_cap_seconds]


def test_5xx_retried_until_max(weather):
    result, session, sleeps = run(weather, *[SimpleResponse(503, "down")] * 3)
    assert not result.ok and len(session.calls) == 3 and len(sleeps) == 2 and result.error_type == "HTTP_5XX"


def test_retry_statuses_come_from_config(weather):
    cfg = replace(weather, retry=replace(weather.retry, retry_statuses=frozenset({429})))
    result, session, _ = run(cfg, SimpleResponse(503, "down"), ok())
    assert not result.ok and len(session.calls) == 1


def test_timeout_retried_no_body(weather):
    result, session, _ = run(weather, *[requests.Timeout("t")] * 3)
    assert not result.ok and len(session.calls) == 3 and result.error_type == "TIMEOUT" and result.body_text is None


def test_connect_timeout_is_timeout(weather):
    result, _, _ = run(weather, requests.ConnectTimeout("c"), ok())
    assert result.ok and "attempt 1: TIMEOUT" in result.attempt_log


def test_connection_error_then_success(weather):
    result, _, _ = run(weather, requests.ConnectionError("dns"), ok())
    assert result.ok and result.attempts == 2


def test_other_request_exception_not_retried(weather):
    result, session, _ = run(weather, requests.exceptions.InvalidURL("bad"), ok())
    assert not result.ok and len(session.calls) == 1 and result.error_type == "REQUEST_ERROR"


def test_backoff_capped_with_jitter(weather):
    assert backoff_delay(1, weather.retry, rng=lambda: 0.0) == pytest.approx(0.5)
    assert backoff_delay(1, weather.retry, rng=lambda: 1.0) == pytest.approx(1.0)
    assert backoff_delay(10, weather.retry, rng=lambda: 1.0) == pytest.approx(weather.retry.backoff_cap_seconds)


@pytest.mark.parametrize("value, expected", [("3", 3.0), ("0", 0.0), ("-1", None), ("soon", None),
                                             ("Wed, 21 Oct 2026 07:28:00 GMT", None), (None, None)])
def test_parse_retry_after(value, expected):
    assert parse_retry_after(value, 30) == expected


# ---------------------------------------------------------------- pagination
def records_page(n, **extra):
    return SimpleResponse(200, json.dumps({"features": [{"id": str(i)} for i in range(n)], **extra}))


def paged(usgs, pagination):
    return replace(usgs, pagination=pagination)


def fetch(cfg, *script):
    session = ScriptedSession(*script)
    pages, err = fetch_entity(session, cfg, cfg.entity("japan"), {"starttime": "2026-10-02", "endtime": "2026-10-09",
                                                                   "min_magnitude": "2.5"}, sleep=lambda s: None)
    return pages, err, session


def test_offset_pagination_stops_on_short_page(usgs):
    pages, err, session = fetch(usgs, records_page(10), records_page(10), records_page(3))
    assert [p.page for p in pages] == [1, 2, 3] and err is None
    assert [c["params"]["offset"] for c in session.calls] == [1, 11, 21]
    assert all(c["params"]["limit"] == 10 for c in session.calls)


def test_offset_pagination_empty_last_page(usgs):
    pages, err, _ = fetch(usgs, records_page(10), records_page(0))
    assert len(pages) == 2 and err is None


def test_page_number_pagination(usgs):
    cfg = paged(usgs, {"type": "page_number", "page_param": "page", "start": 1, "total_pages_path": "meta.pages"})
    pages, err, session = fetch(cfg, records_page(5, meta={"pages": 2}), records_page(5, meta={"pages": 2}))
    assert [c["params"]["page"] for c in session.calls] == [1, 2] and len(pages) == 2


def test_cursor_pagination(usgs):
    cfg = paged(usgs, {"type": "cursor", "cursor_path": "next", "cursor_param": "cursor"})
    pages, err, session = fetch(cfg, records_page(2, next="abc"), records_page(2, next=None))
    assert "cursor" not in session.calls[0]["params"] and session.calls[1]["params"]["cursor"] == "abc"


def test_next_url_pagination(usgs):
    cfg = paged(usgs, {"type": "next_url", "next_url_path": "links.next"})
    pages, err, session = fetch(cfg, records_page(2, links={"next": "https://earthquake.usgs.gov/next?p=2"}),
                                records_page(2, links={}))
    assert session.calls[1]["url"] == "https://earthquake.usgs.gov/next?p=2" and session.calls[1]["params"] == {}


def test_max_pages_cap_fails_instead_of_truncating(usgs):
    cfg = paged(usgs, {**usgs.pagination, "max_pages": 2})
    pages, err, _ = fetch(cfg, records_page(10), records_page(10))
    assert len(pages) == 2 and "max_pages=2" in err


def test_failure_mid_pagination_stops(usgs):
    pages, err, _ = fetch(usgs, records_page(10), SimpleResponse(400, "bad"))
    assert [p.ok for p in pages] == [True, False]
