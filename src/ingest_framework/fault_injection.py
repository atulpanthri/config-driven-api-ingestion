"""Explicit, opt-in fault injection for demos and tests. OFF unless test_mode is set.

Faults hit ONE target entity at the transport boundary (request params, base URL or the
HTTP session), so the pipeline runs its normal code path and other entities are untouched.
Every run records test_mode in the run audit and bronze rows.

Built-in faults (every source):
  unreachable_url   host 'invalid.invalid'                  -> connection error, retried, FAILED
  timeout           session raises requests.Timeout          -> retried, FAILED
  rate_limit        first call returns 429 + Retry-After: 1  -> retried, SUCCESS
  server_error      every call returns 503                   -> retried, FAILED
  empty_response    body ""                                  -> EMPTY_RESPONSE
  truncated_response  body cut in half                       -> INVALID_JSON
  unequal_arrays    last contract array one element short    -> ARRAY_LENGTH_MISMATCH (columnar only)
  duplicate_record  first row repeated with a changed value  -> duplicate quarantined
Source scenarios (fault_injection.scenarios in YAML), actions:
  override_param {param, value}       e.g. latitude=999 -> real HTTP 400
  set_value {field, index, value}     set_null {field, index}
  drop_field {field}                  add_field {field, value}
"""
from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from typing import Any

import requests

from .api_client import entity_param_names, render
from .source_config import Entity, SourceConfig
from .transforms import delete_path, get_path, set_path

REQUEST_FAULTS = ("unreachable_url", "timeout", "rate_limit", "server_error")
PAYLOAD_FAULTS = ("empty_response", "truncated_response", "unequal_arrays", "duplicate_record")
BUILT_IN_FAULTS = REQUEST_FAULTS + PAYLOAD_FAULTS
UNREACHABLE_BASE_URL = "https://invalid.invalid/"


@dataclass
class SimpleResponse:
    status_code: int
    text: str
    headers: dict[str, str] = field(default_factory=dict)


def available_faults(cfg: SourceConfig) -> list[str]:
    built_in = [f for f in BUILT_IN_FAULTS if not (f == "unequal_arrays" and cfg.response_format != "columnar")]
    return built_in + sorted(cfg.fault_scenarios)


def normalize_test_mode(test_mode: str | None, cfg: SourceConfig) -> str | None:
    if test_mode is None or test_mode.strip().lower() in ("", "none", "off"):
        return None
    mode = test_mode.strip().lower()
    if mode not in available_faults(cfg):
        raise ValueError(f"unknown test_mode {test_mode!r} for {cfg.source_name}; expected one of {available_faults(cfg)}")
    return mode


def resolve_target(cfg: SourceConfig, fault_entity_id: str | None) -> Entity:
    target_id = fault_entity_id or cfg.fault_default_target or cfg.entities[0].id
    return cfg.entity(target_id)


class RequestMatcher:
    """Recognise which entity a request belongs to from its entity-dependent params."""

    def __init__(self, cfg: SourceConfig):
        names = entity_param_names(cfg)
        self.expected = {e.id: {n: render(cfg.params[n], e, {}) for n in names} for e in cfg.entities}

    def match(self, params: dict[str, Any] | None) -> str | None:
        params = params or {}
        for entity_id, expected in self.expected.items():
            if expected and all(v is None or str(params.get(k)) == str(v) for k, v in expected.items()):
                return entity_id
        return None


def request_overrides(mode: str | None, cfg: SourceConfig, entity: Entity, target: Entity | None
                      ) -> tuple[dict[str, Any] | None, str | None]:
    """(params_override, base_url_override) for the target entity, else (None, None)."""
    if mode is None or target is None or entity.id != target.id:
        return None, None
    if mode == "unreachable_url":
        return None, UNREACHABLE_BASE_URL
    scenario = cfg.fault_scenarios.get(mode, {})
    if scenario.get("action") == "override_param":
        return {scenario["param"]: scenario["value"]}, None
    return None, None


def _first_value_field(cfg: SourceConfig) -> str | None:
    """A numeric contract field that feeds a non-key silver column (to make duplicates conflict)."""
    for col in cfg.columns:
        if col.type == "double" and col.name not in cfg.silver_keys:
            return col.source
    return None


def mutate_payload(mode: str, body: str, cfg: SourceConfig) -> str:
    if mode == "empty_response":
        return ""
    if mode == "truncated_response":
        return body[: len(body) // 2]
    scenario = cfg.fault_scenarios.get(mode, {"action": mode})
    action = scenario["action"]
    payload = json.loads(body)
    data = get_path(payload, cfg.data_path)
    fld = scenario.get("field")
    if cfg.response_format == "columnar":
        if action == "set_value":
            data[fld][scenario.get("index", 0)] = scenario["value"]
        elif action == "set_null":
            data[fld][scenario.get("index", 0)] = None
        elif action == "drop_field":
            data.pop(fld, None)
            unit_path = (cfg.contract_field(fld).unit_path if cfg.contract_field(fld) else None)
            if unit_path:
                delete_path(payload, unit_path)
        elif action == "add_field":
            n = len(next(v for v in data.values() if isinstance(v, list)))
            data[fld] = [scenario["value"]] * n
        elif action == "unequal_arrays":
            data[cfg.contract[-1].name].pop()
        elif action == "duplicate_record":
            for values in data.values():
                if isinstance(values, list) and values:
                    values.append(values[0])
            value_field = _first_value_field(cfg)
            if value_field and isinstance(data[value_field][-1], (int, float)):
                data[value_field][-1] += 5
        else:
            raise ValueError(f"{mode} is not a payload fault")
    else:
        if action in ("set_value", "set_null"):
            set_path(data[scenario.get("index", 0)], fld, None if action == "set_null" else scenario["value"])
        elif action == "drop_field":
            for record in data:
                delete_path(record, fld)
        elif action == "add_field":
            for record in data:
                set_path(record, fld, scenario["value"])
        elif action == "duplicate_record":
            dup = copy.deepcopy(data[0])
            value_field = _first_value_field(cfg)
            current = get_path(dup, value_field) if value_field else None
            if isinstance(current, (int, float)) and not isinstance(current, bool):
                set_path(dup, value_field, current + 5)
            data.append(dup)
        else:
            raise ValueError(f"{mode} is not a payload fault for records sources")
    return json.dumps(payload, ensure_ascii=False)


class FaultySession:
    """Wraps a real or fixture session; injects the fault only for the target entity."""

    def __init__(self, inner: Any, cfg: SourceConfig, mode: str, target: Entity):
        self.inner, self.cfg, self.mode, self.target = inner, cfg, mode, target
        self.matcher = RequestMatcher(cfg)
        self.calls = 0
        self.mutated = False

    def request(self, method: str, url: str, params: dict[str, Any] | None = None,
                headers: dict[str, str] | None = None, timeout: float | None = None) -> Any:
        if self.matcher.match(params) != self.target.id:
            return self.inner.request(method, url, params=params, headers=headers, timeout=timeout)
        self.calls += 1
        if self.mode == "timeout":
            raise requests.Timeout(f"injected timeout after {timeout}s")
        if self.mode == "server_error":
            return SimpleResponse(503, '{"error":true,"reason":"injected 503"}')
        if self.mode == "rate_limit" and self.calls == 1:
            return SimpleResponse(429, '{"error":true,"reason":"injected 429"}', {"Retry-After": "1"})
        response = self.inner.request(method, url, params=params, headers=headers, timeout=timeout)
        is_payload_fault = self.mode in PAYLOAD_FAULTS or (
            self.cfg.fault_scenarios.get(self.mode, {}).get("action") not in (None, "override_param"))
        # Mutate only the target's first successful response (page 1); later pages pass through.
        if is_payload_fault and response.status_code == 200 and not self.mutated:
            self.mutated = True
            return SimpleResponse(200, mutate_payload(self.mode, response.text, self.cfg), dict(response.headers))
        return response


def build_session(base_session: Any, cfg: SourceConfig, mode: str | None, target: Entity | None) -> Any:
    if mode is None or target is None or mode == "unreachable_url":
        return base_session
    if cfg.fault_scenarios.get(mode, {}).get("action") == "override_param":
        return base_session
    return FaultySession(base_session, cfg, mode, target)
