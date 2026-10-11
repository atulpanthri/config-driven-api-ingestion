"""Offline stand-in for a source API (source_mode=fixture).

Serves real responses saved under <fixtures_dir>/<entity_id>.json (page 1) and
<entity_id>_p<N>.json (page N). Used by unit tests, repeatable edge-case demos and as the
fallback when serverless compute cannot reach an API host.

Requests are matched to an entity by their entity-dependent params (e.g. latitude/longitude),
so window params (dates) do not need to match the day the fixture was captured. Behaviour:
  other host                 -> connection error
  params match no entity     -> HTTP 400 (e.g. the bad_request scenario)
  no fixture for the page    -> HTTP 404
Page numbers come from the offset/page params; next_url/cursor sources use a per-entity counter.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from .fault_injection import RequestMatcher, SimpleResponse
from .source_config import SourceConfig


class FixtureSession:
    def __init__(self, cfg: SourceConfig, fixture_dir: str | Path | None = None):
        self.cfg = cfg
        self.fixture_dir = Path(fixture_dir or cfg.fixtures_dir)
        self.matcher = RequestMatcher(cfg)
        self.host = urlparse(cfg.base_url).hostname
        self._counters: dict[str, int] = {}
        self._last_entity: str | None = None

    def _page(self, entity_id: str, params: dict[str, Any], is_first: bool) -> int:
        pg = self.cfg.pagination
        kind = pg.get("type", "none")
        if kind == "offset":
            return (int(params.get(pg["offset_param"], pg.get("start", 0))) - int(pg.get("start", 0))) // int(pg["limit"]) + 1
        if kind == "page_number":
            return int(params.get(pg["page_param"], pg.get("start", 1))) - int(pg.get("start", 1)) + 1
        if kind == "none":
            return 1
        self._counters[entity_id] = 1 if is_first else self._counters.get(entity_id, 0) + 1
        return self._counters[entity_id]

    def request(self, method: str, url: str, params: dict[str, Any] | None = None,
                headers: dict[str, str] | None = None, timeout: float | None = None) -> SimpleResponse:
        params = params or {}
        if urlparse(url).hostname != self.host:
            raise requests.ConnectionError(f"fixture mode: cannot resolve host {urlparse(url).hostname}")
        entity_id = self.matcher.match(params)
        is_first = entity_id is not None
        if entity_id is None and self.cfg.pagination.get("type") in ("next_url", "cursor") and self._last_entity:
            entity_id = self._last_entity  # follow-up page without entity params
        if entity_id is None:
            return SimpleResponse(400, json.dumps({"error": True, "reason": "fixture: no entity matches request params"}))
        if is_first and self.cfg.pagination.get("type") == "cursor" and self.cfg.pagination["cursor_param"] in params:
            is_first = False
        self._last_entity = entity_id
        page = self._page(entity_id, params, is_first)
        name = f"{entity_id}.json" if page == 1 else f"{entity_id}_p{page}.json"
        path = self.fixture_dir / name
        if not path.exists():
            return SimpleResponse(404, json.dumps({"error": True, "reason": f"fixture: no file {name}"}))
        return SimpleResponse(200, path.read_text(encoding="utf-8"), {"Content-Type": "application/json"})
