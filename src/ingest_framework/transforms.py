"""Parse raw bodies, extract rows for both response shapes, and cast values under control.

Two response shapes are supported (response.format):
  columnar  parallel arrays under data_path ({"time": [...], "temp": [...]}); every
            contract array must have the same length *before* values are paired by index
            (zip() would silently truncate to the shortest array)
  records   a list of objects under data_path; fields are read by dot path, list
            indices allowed ("geometry.coordinates.2")
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

from .source_config import ColumnSpec, SourceConfig


class _Missing:
    def __repr__(self) -> str:
        return "MISSING"


MISSING = _Missing()


class PayloadError(Exception):
    """A defect that prevents forming any rows from a payload (payload-level rejection)."""

    def __init__(self, reason: str, details: str):
        super().__init__(f"{reason}: {details}")
        self.reason = reason
        self.details = details


# ------------------------------------------------------------------ paths
def get_path(obj: Any, path: str | None) -> Any:
    """Read a dot path; numeric segments index lists. Returns MISSING if absent."""
    if path is None or path == "":
        return obj
    current = obj
    for part in path.split("."):
        if isinstance(current, dict):
            if part not in current:
                return MISSING
            current = current[part]
        elif isinstance(current, list) and part.lstrip("-").isdigit():
            idx = int(part)
            if not -len(current) <= idx < len(current):
                return MISSING
            current = current[idx]
        else:
            return MISSING
    return current


def set_path(obj: Any, path: str, value: Any) -> None:
    parts = path.split(".")
    for part in parts[:-1]:
        obj = obj[int(part)] if isinstance(obj, list) else obj.setdefault(part, {})
    last = parts[-1]
    if isinstance(obj, list):
        obj[int(last)] = value
    else:
        obj[last] = value


def delete_path(obj: Any, path: str) -> None:
    parts = path.split(".")
    parent = get_path(obj, ".".join(parts[:-1])) if len(parts) > 1 else obj
    if isinstance(parent, dict):
        parent.pop(parts[-1], None)


def leaf_paths(obj: Any, prefix: str = "") -> dict[str, Any]:
    """All leaf paths of nested dicts (lists are leaves) -> value."""
    out: dict[str, Any] = {}
    if isinstance(obj, dict) and obj:
        for key, value in obj.items():
            out.update(leaf_paths(value, f"{prefix}.{key}" if prefix else str(key)))
    elif prefix:
        out[prefix] = obj
    return out


# ------------------------------------------------------------------ payload
def parse_payload(body_text: str | None, cfg: SourceConfig) -> dict[str, Any] | list[Any]:
    if body_text is None or not body_text.strip():
        raise PayloadError("EMPTY_RESPONSE", "response body is empty")
    try:
        payload = json.loads(body_text)
    except json.JSONDecodeError as exc:
        raise PayloadError("INVALID_JSON", f"cannot parse JSON ({exc.msg} at char {exc.pos}); "
                                           f"body length {len(body_text)}") from exc
    if not isinstance(payload, (dict, list)):
        raise PayloadError("INVALID_JSON_STRUCTURE", f"top-level JSON is {type(payload).__name__}")
    if isinstance(payload, dict) and cfg.error_flag_path and get_path(payload, cfg.error_flag_path) is True:
        message = get_path(payload, cfg.error_message_path) if cfg.error_message_path else MISSING
        raise PayloadError("API_ERROR", str(message if message is not MISSING else "API returned an error flag"))
    return payload


def utc_offset(payload: Any, cfg: SourceConfig) -> int:
    if not cfg.utc_offset_path:
        return 0
    value = get_path(payload, cfg.utc_offset_path)
    if value is MISSING:
        return 0
    if not isinstance(value, int) or isinstance(value, bool):
        raise PayloadError("INVALID_UTC_OFFSET", f"{cfg.utc_offset_path}={value!r}")
    return value


def check_array_lengths(data: dict[str, Any], fields: list[str]) -> int:
    """Return the common length of the contract arrays or raise ARRAY_LENGTH_MISMATCH listing all lengths."""
    lengths = {name: len(data[name]) for name in fields}
    if len(set(lengths.values())) > 1:
        raise PayloadError("ARRAY_LENGTH_MISMATCH", json.dumps(lengths))
    return next(iter(lengths.values()), 0)


def extract_rows(payload: Any, cfg: SourceConfig) -> list[dict[str, Any]]:
    """One dict per source row: {"row_index": i, "raw": {contract_field: raw value or None}}.
    Call after the schema check passed (required containers exist)."""
    data = get_path(payload, cfg.data_path)
    fields = [c.name for c in cfg.contract]
    if cfg.response_format == "columnar":
        present = [f for f in fields if isinstance(data.get(f), list)]
        n = check_array_lengths(data, present)
        return [{"row_index": i, "raw": {f: (data[f][i] if f in present else None) for f in fields}}
                for i in range(n)]
    rows = []
    for i, record in enumerate(data):
        raw = {}
        for f in fields:
            value = get_path(record, f) if isinstance(record, dict) else MISSING
            raw[f] = None if value is MISSING else value
        rows.append({"row_index": i, "raw": raw})
    return rows


# ------------------------------------------------------------------ casts
def to_float(value: Any, numeric_strings: str = "convert") -> tuple[float | None, str | None, bool]:
    """Returns (value, problem, converted_from_string); problem is None, "NULL" or "INVALID_TYPE".
    Booleans, NaN/Infinity and unparseable strings are rejected; numeric strings depend on policy."""
    if value is None:
        return None, "NULL", False
    if isinstance(value, bool):
        return None, "INVALID_TYPE", False
    if isinstance(value, (int, float)):
        return (float(value), None, False) if math.isfinite(value) else (None, "INVALID_TYPE", False)
    if isinstance(value, str) and numeric_strings == "convert":
        try:
            converted = float(value.strip())
        except ValueError:
            return None, "INVALID_TYPE", False
        return (converted, None, True) if math.isfinite(converted) else (None, "INVALID_TYPE", False)
    return None, "INVALID_TYPE", False


def parse_timestamp(value: Any, fmt: str, utc_offset_seconds: int = 0,
                    numeric_strings: str = "convert") -> tuple[datetime | None, bool]:
    """Parse to an aware UTC datetime. Returns (value, converted_from_string).
    fmt: a strptime pattern (source local time, normalized with utc_offset_seconds),
    'iso8601', 'epoch_s' or 'epoch_ms'."""
    if fmt in ("epoch_s", "epoch_ms"):
        number, problem, converted = to_float(value, numeric_strings)
        if problem:
            return None, False
        seconds = number / 1000.0 if fmt == "epoch_ms" else number
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc), converted
        except (OverflowError, OSError, ValueError):
            return None, False
    if not isinstance(value, str):
        return None, False
    try:
        if fmt == "iso8601":
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                return parsed.astimezone(timezone.utc), False
        else:
            parsed = datetime.strptime(value.strip(), fmt)
    except ValueError:
        return None, False
    return (parsed - timedelta(seconds=utc_offset_seconds)).replace(tzinfo=timezone.utc), False


def cast_value(value: Any, column: ColumnSpec, cfg: SourceConfig,
               utc_offset_seconds: int = 0) -> tuple[Any, str | None, bool]:
    """Cast one raw value for a silver column. Returns (value, problem, converted_from_string)."""
    if value is None:
        return None, "NULL", False
    t = column.type
    if t in ("double", "int", "bigint"):
        number, problem, converted = to_float(value, cfg.numeric_strings)
        if problem:
            return None, problem, False
        if t != "double":
            if not number.is_integer():
                return None, "INVALID_TYPE", False
            return int(number), None, converted
        return number, None, converted
    if t == "boolean":
        return (value, None, False) if isinstance(value, bool) else (None, "INVALID_TYPE", False)
    if t == "string":
        if isinstance(value, (dict, list)):
            return None, "INVALID_TYPE", False
        return (value if isinstance(value, str) else json.dumps(value)), None, False
    if t == "timestamp":
        ts, converted = parse_timestamp(value, column.format, utc_offset_seconds, cfg.numeric_strings)
        return (ts, None, converted) if ts else (None, "INVALID_TYPE", False)
    if t == "date":
        try:
            return datetime.strptime(str(value).strip(), column.format).date(), None, False
        except ValueError:
            return None, "INVALID_TYPE", False
    return None, "INVALID_TYPE", False


def utc_date(ts: datetime) -> date:
    return ts.astimezone(timezone.utc).date()


def raw_as_text(value: Any) -> str | None:
    """Render a raw JSON value for quarantine raw_values without losing its type."""
    if value is None:
        return None
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
