"""Compare a raw payload with the source's explicit contract (response.contract).

Policy (same for every source):
  NEW_FIELD      field not in the contract (and not in ignore_fields) -> logged, kept in bronze,
                 not loaded; load continues
  MISSING_FIELD  required field absent (columnar: the array; records: from every record) or the
                 data_path container itself absent -> logged, payload rejected
                 (a field missing from only some records becomes a row-level NULL_<COL>)
  TYPE_CHANGE    container of the wrong type -> logged, payload rejected;
                 elements of an unexpected type -> logged; rows converted or quarantined per policy
  UNIT_CHANGE    unit at unit_path differs from the contract -> logged, payload rejected
                 (values are never converted silently)
Silver/gold schemas stay explicit; Delta mergeSchema is not used.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .source_config import SourceConfig, is_ignored
from .transforms import MISSING, get_path, leaf_paths


@dataclass
class SchemaCheck:
    events: list[dict[str, str | None]] = field(default_factory=list)
    blocking_errors: list[str] = field(default_factory=list)

    @property
    def contract_ok(self) -> bool:
        return not self.blocking_errors


def json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):  # bool is a subclass of int in Python
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def type_matches(actual: str, expected: str) -> bool:
    return actual == expected or (expected == "number" and actual == "integer")


def _event(change_type: str, field_name: str, expected: str | None, actual: str | None, details: str) -> dict:
    return {"change_type": change_type, "field_name": field_name,
            "expected_type": expected, "actual_type": actual, "details": details}


def _types_label(values: list[Any]) -> str:
    kinds = {json_type(v) for v in values if v is not None}
    kinds = {"number" if k == "integer" else k for k in kinds}
    return "|".join(sorted(kinds)) or "null"


def check_payload_schema(payload: Any, cfg: SourceConfig) -> SchemaCheck:
    check = SchemaCheck()
    data = get_path(payload, cfg.data_path)
    container = "object" if cfg.response_format == "columnar" else "array"
    if data is MISSING:
        check.events.append(_event("MISSING_FIELD", cfg.data_path, container, None, f"'{cfg.data_path}' absent"))
        check.blocking_errors.append("MISSING_FIELD")
        return check
    if json_type(data) != container:
        check.events.append(_event("TYPE_CHANGE", cfg.data_path, container, json_type(data),
                                   f"'{cfg.data_path}' is not an {container}"))
        check.blocking_errors.append("TYPE_CHANGE")
        return check

    if cfg.response_format == "columnar":
        _check_columnar(data, cfg, check)
    else:
        _check_records(data, cfg, check)

    for c in cfg.contract:
        if c.unit_path:
            unit = get_path(payload, c.unit_path)
            if unit is not MISSING and unit != c.unit:
                check.events.append(_event("UNIT_CHANGE", c.unit_path, c.unit, str(unit),
                                           "unit differs from contract; payload rejected to avoid mislabeled values"))
                check.blocking_errors.append("UNIT_CHANGE")
    return check


def _check_columnar(data: dict[str, Any], cfg: SourceConfig, check: SchemaCheck) -> None:
    contract = {c.name: c for c in cfg.contract}
    for name in sorted(set(data) - set(contract)):
        if is_ignored(name, cfg.ignore_fields):
            continue
        value = data[name]
        actual = f"array<{_types_label(value)}>" if isinstance(value, list) else json_type(value)
        check.events.append(_event("NEW_FIELD", f"{cfg.data_path}.{name}", None, actual,
                                   "additive field kept in bronze, not loaded to silver"))
    for name, c in contract.items():
        label = f"{cfg.data_path}.{name}"
        if name not in data:
            if c.required:
                check.events.append(_event("MISSING_FIELD", label, f"array<{c.type}>", None,
                                           "required field absent; payload rejected"))
                check.blocking_errors.append("MISSING_FIELD")
            continue
        value = data[name]
        if not isinstance(value, list):
            check.events.append(_event("TYPE_CHANGE", label, f"array<{c.type}>", json_type(value),
                                       "expected an array; payload rejected"))
            check.blocking_errors.append("TYPE_CHANGE")
            continue
        bad = [v for v in value if v is not None and not type_matches(json_type(v), c.type)]
        if bad:
            check.events.append(_event("TYPE_CHANGE", label, c.type, _types_label(value),
                                       f"{len(bad)} element(s) not {c.type}; converted or quarantined per row"))


def _check_records(records: list[Any], cfg: SourceConfig, check: SchemaCheck) -> None:
    label_prefix = f"{cfg.data_path}[]"
    non_objects = [r for r in records if not isinstance(r, dict)]
    if non_objects:
        check.events.append(_event("TYPE_CHANGE", label_prefix, "object", json_type(non_objects[0]),
                                   f"{len(non_objects)} record(s) are not objects; payload rejected"))
        check.blocking_errors.append("TYPE_CHANGE")
        return
    if not records:
        return

    # Contract paths may index into lists ("geometry.coordinates.2"); compare at the leaf level.
    contract_leaves = {".".join(p for p in c.name.split(".") if not p.lstrip("-").isdigit()) for c in cfg.contract}
    observed: set[str] = set()
    for r in records:
        observed.update(leaf_paths(r))
    for name in sorted(observed - contract_leaves):
        if not is_ignored(name, cfg.ignore_fields):
            check.events.append(_event("NEW_FIELD", f"{label_prefix}.{name}", None, None,
                                       "additive field kept in bronze, not loaded to silver"))

    for c in cfg.contract:
        label = f"{label_prefix}.{c.name}"
        values = [get_path(r, c.name) for r in records]
        present = [v for v in values if v is not MISSING]
        if not present:
            if c.required:
                check.events.append(_event("MISSING_FIELD", label, c.type, None,
                                           "required field absent from every record; payload rejected"))
                check.blocking_errors.append("MISSING_FIELD")
            continue
        bad = [v for v in present if v is not None and not type_matches(json_type(v), c.type)]
        if bad:
            check.events.append(_event("TYPE_CHANGE", label, c.type, _types_label(present),
                                       f"{len(bad)} value(s) not {c.type}; converted or quarantined per row"))
