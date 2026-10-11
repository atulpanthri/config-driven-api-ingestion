"""Generic HTTP client: templated requests, API-key auth from a secret, bounded retries, pagination.

Retry policy (per request/page):
  * 200                                  -> success, body kept exactly as received
  * status in retry.retry_statuses       -> retry; 429/503 honor Retry-After (seconds, capped)
  * timeout / connection error           -> retry
  * any other status (e.g. 400)          -> fail immediately; repeating a bad request cannot succeed
  * any other requests exception         -> fail immediately
Backoff: min(cap, base * 2^(attempt-1)) with "equal jitter" (50-100% of that delay).

Secrets: auth values are resolved at call time through `secret_resolver(scope, key)`
(dbutils.secrets.get in Databricks) and are masked in everything that is stored or logged.

Pagination (request.pagination.type), bounded by max_pages; reaching the cap while more data is
available fails the entity (PAGINATION_LIMIT) instead of silently truncating:
  next_url     follow the URL at next_url_path until it is empty
  cursor       send the token at cursor_path as cursor_param until it is empty
  page_number  increment page_param (from start) until a page has no records
               or total_pages_path is reached
  offset       advance offset_param by limit (limit_param=limit) until a page has fewer than limit records
"""
from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

import requests

from .source_config import TEMPLATE, Entity, RetrySpec, SourceConfig
from .transforms import MISSING, get_path

MAX_ERROR_LENGTH = 500
REDACTED = "***"
SecretResolver = Callable[[str, str], str]


@dataclass
class FetchResult:
    entity_id: str
    page: int
    ok: bool
    request_url: str                      # redacted
    request_params: dict[str, Any]        # redacted
    request_start: datetime
    request_end: datetime | None = None
    http_status: int | None = None
    attempts: int = 0
    body_text: str | None = None          # None only when no response was received
    error_type: str | None = None
    error_message: str | None = None
    attempt_log: list[str] = field(default_factory=list)


@dataclass
class PreparedRequest:
    url: str
    params: dict[str, Any]
    headers: dict[str, str]
    secret_values: tuple[str, ...] = ()

    def redacted_params(self) -> dict[str, Any]:
        return {k: (REDACTED if any(s and s in str(v) for s in self.secret_values) else v)
                for k, v in self.params.items()}

    def redacted_url(self) -> str:
        url = build_url(self.url, self.redacted_params())
        for secret in self.secret_values:
            url = url.replace(secret, REDACTED)
        return url


# ------------------------------------------------------------------ helpers
def sanitize(message: Any, secrets: tuple[str, ...] = ()) -> str:
    """Single-line, length-capped, secret-free error text for audit tables."""
    text = " ".join(str(message).split())
    for secret in secrets:
        if secret:
            text = text.replace(secret, REDACTED)
    return text[:MAX_ERROR_LENGTH]


def build_url(base_url: str, params: dict[str, Any]) -> str:
    return requests.Request("GET", base_url, params=params).prepare().url


def parse_retry_after(value: str | None, cap_seconds: float) -> float | None:
    """Only the delta-seconds form is supported; HTTP-date or junk falls back to backoff."""
    if value is None:
        return None
    try:
        seconds = float(str(value).strip())
    except ValueError:
        return None
    if seconds < 0:
        return None
    return min(seconds, cap_seconds)


def backoff_delay(attempt: int, retry: RetrySpec, rng: Callable[[], float] = random.random) -> float:
    ceiling = min(retry.backoff_cap_seconds, retry.backoff_base_seconds * (2 ** (attempt - 1)))
    return ceiling / 2 + rng() * ceiling / 2


def render(value: Any, entity: Entity, window: dict[str, Any]) -> Any:
    """Render {entity.x}/{window.x}. A whole-value template keeps the value's type; a template
    that resolves to None makes the whole value None (the param is then omitted)."""
    if not isinstance(value, str):
        return value
    refs = TEMPLATE.findall(value)
    if not refs:
        return value

    def lookup(scope: str, name: str) -> Any:
        return entity.attributes.get(name) if scope == "entity" else window.get(name)

    whole = TEMPLATE.fullmatch(value)
    if whole:
        return lookup(*whole.groups())
    if any(lookup(s, n) is None for s, n in refs):
        return None
    return TEMPLATE.sub(lambda m: str(lookup(m.group(1), m.group(2))), value)


def render_params(params: dict[str, Any], entity: Entity, window: dict[str, Any]) -> dict[str, Any]:
    rendered = {k: render(v, entity, window) for k, v in params.items()}
    return {k: v for k, v in rendered.items() if v is not None}


def entity_param_names(cfg: SourceConfig) -> list[str]:
    """Params whose value depends on the entity (used to recognise requests in fixtures/faults)."""
    return [k for k, v in cfg.params.items() if any(s == "entity" for s, _ in TEMPLATE.findall(str(v)))]


def build_request(cfg: SourceConfig, entity: Entity, window: dict[str, Any],
                  secret_resolver: SecretResolver | None = None) -> PreparedRequest:
    params = render_params(cfg.params, entity, window)
    headers = {k: str(v) for k, v in render_params(cfg.headers, entity, window).items()}
    secrets: tuple[str, ...] = ()
    if cfg.auth.get("type") == "api_key":
        if secret_resolver is None:
            raise RuntimeError("source needs an API key but no secret_resolver was provided")
        secret = secret_resolver(cfg.auth["secret_scope"], cfg.auth["secret_key"])
        if not secret:
            raise RuntimeError(f"secret {cfg.auth['secret_scope']}/{cfg.auth['secret_key']} is empty")
        secrets = (secret,)
        value = f"{cfg.auth.get('prefix', '')}{secret}"
        if cfg.auth["in"] == "query":
            params[cfg.auth["name"]] = value
        else:
            headers[cfg.auth["name"]] = value
    return PreparedRequest(cfg.base_url, params, headers, secrets)


# ------------------------------------------------------------------ single request with retries
def fetch_once(session: Any, cfg: SourceConfig, req: PreparedRequest, *, entity_id: str, page: int = 1,
               sleep: Callable[[float], None] = time.sleep, rng: Callable[[], float] = random.random) -> FetchResult:
    result = FetchResult(entity_id=entity_id, page=page, ok=False, request_url=req.redacted_url(),
                         request_params=req.redacted_params(), request_start=datetime.now(timezone.utc))
    for attempt in range(1, cfg.retry.max_attempts + 1):
        result.attempts = attempt
        retry_after: float | None = None
        try:
            response = session.request(cfg.method, req.url, params=req.params, headers=req.headers,
                                       timeout=cfg.timeout_seconds)
        except requests.Timeout as exc:  # before ConnectionError: ConnectTimeout is both
            retryable, result.error_type = True, "TIMEOUT"
            result.error_message, result.http_status, result.body_text = sanitize(exc, req.secret_values), None, None
        except requests.ConnectionError as exc:
            retryable, result.error_type = True, "CONNECTION_ERROR"
            result.error_message, result.http_status, result.body_text = sanitize(exc, req.secret_values), None, None
        except requests.RequestException as exc:
            retryable, result.error_type = False, "REQUEST_ERROR"
            result.error_message, result.http_status, result.body_text = sanitize(exc, req.secret_values), None, None
        else:
            result.http_status = response.status_code
            result.body_text = response.text  # kept even on errors, for investigation
            if response.status_code == 200:
                result.ok, result.error_type, result.error_message = True, None, None
                result.attempt_log.append(f"attempt {attempt}: HTTP 200")
                break
            retryable = response.status_code in cfg.retry.retry_statuses
            result.error_type = ("HTTP_429" if response.status_code == 429
                                 else "HTTP_5XX" if response.status_code >= 500 else "HTTP_4XX")
            result.error_message = sanitize(f"HTTP {response.status_code}: {response.text}", req.secret_values)
            if response.status_code in (429, 503):
                retry_after = parse_retry_after(response.headers.get("Retry-After"),
                                                cfg.retry.retry_after_cap_seconds)
        result.attempt_log.append(f"attempt {attempt}: {result.error_type}")
        if not retryable or attempt == cfg.retry.max_attempts:
            break
        sleep(retry_after if retry_after is not None else backoff_delay(attempt, cfg.retry, rng))
    result.request_end = datetime.now(timezone.utc)
    return result


# ------------------------------------------------------------------ pagination
def page_record_count(body_text: str | None, cfg: SourceConfig) -> int | None:
    """Records on a page (None if the body cannot be read; processing will reject it)."""
    try:
        payload = json.loads(body_text or "")
    except json.JSONDecodeError:
        return None
    data = get_path(payload, cfg.data_path)
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        arrays = [v for v in data.values() if isinstance(v, list)]
        return len(arrays[0]) if arrays else 0
    return None


def next_request(cfg: SourceConfig, req: PreparedRequest, result: FetchResult, page: int
                 ) -> PreparedRequest | None:
    pg = cfg.pagination
    kind = pg.get("type", "none")
    if kind == "none":
        return None
    try:
        payload = json.loads(result.body_text or "")
    except json.JSONDecodeError:
        return None
    count = page_record_count(result.body_text, cfg)
    if kind == "next_url":
        nxt = get_path(payload, pg["next_url_path"])
        if nxt in (MISSING, None, ""):
            return None
        auth_params = {cfg.auth["name"]: req.params[cfg.auth["name"]]} if cfg.auth.get("in") == "query" else {}
        return PreparedRequest(str(nxt), auth_params, req.headers, req.secret_values)
    if kind == "cursor":
        token = get_path(payload, pg["cursor_path"])
        if token in (MISSING, None, ""):
            return None
        return PreparedRequest(req.url, {**req.params, pg["cursor_param"]: token}, req.headers, req.secret_values)
    if kind == "page_number":
        if not count:
            return None
        total = get_path(payload, pg["total_pages_path"]) if pg.get("total_pages_path") else MISSING
        current = int(req.params.get(pg["page_param"], pg.get("start", 1)))
        if total is not MISSING and current >= int(total):
            return None
        return PreparedRequest(req.url, {**req.params, pg["page_param"]: current + 1}, req.headers, req.secret_values)
    if kind == "offset":
        limit = int(pg["limit"])
        if count is None or count < limit:
            return None
        current = int(req.params.get(pg["offset_param"], pg.get("start", 0)))
        return PreparedRequest(req.url, {**req.params, pg["offset_param"]: current + limit}, req.headers,
                               req.secret_values)
    return None


def first_page_request(cfg: SourceConfig, req: PreparedRequest) -> PreparedRequest:
    """Add the initial page/offset/limit params required by the pagination type."""
    pg = cfg.pagination
    params = dict(req.params)
    if pg.get("type") == "page_number":
        params.setdefault(pg["page_param"], pg.get("start", 1))
    elif pg.get("type") == "offset":
        params.setdefault(pg["offset_param"], pg.get("start", 0))
        params[pg["limit_param"]] = int(pg["limit"])
    return PreparedRequest(req.url, params, req.headers, req.secret_values)


def fetch_entity(session: Any, cfg: SourceConfig, entity: Entity, window: dict[str, Any], *,
                 secret_resolver: SecretResolver | None = None, params_override: dict[str, Any] | None = None,
                 base_url_override: str | None = None, sleep: Callable[[float], None] = time.sleep,
                 rng: Callable[[], float] = random.random) -> tuple[list[FetchResult], str | None]:
    """Fetch all pages for one entity. Returns (page results, pagination_error or None)."""
    req = build_request(cfg, entity, window, secret_resolver)
    if params_override:
        req = PreparedRequest(req.url, {**req.params, **params_override}, req.headers, req.secret_values)
    if base_url_override:
        req = PreparedRequest(base_url_override, req.params, req.headers, req.secret_values)
    req = first_page_request(cfg, req)
    max_pages = int(cfg.pagination.get("max_pages", 100)) if cfg.pagination.get("type", "none") != "none" else 1

    pages: list[FetchResult] = []
    page = 1
    while True:
        result = fetch_once(session, cfg, req, entity_id=entity.id, page=page, sleep=sleep, rng=rng)
        pages.append(result)
        if not result.ok:
            return pages, None
        nxt = next_request(cfg, req, result, page)
        if nxt is None:
            return pages, None
        if page >= max_pages:
            return pages, f"more pages available after max_pages={max_pages}"
        req, page = nxt, page + 1
