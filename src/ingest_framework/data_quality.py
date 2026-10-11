"""Config-driven row validation and duplicate policy.

Rules come from silver.columns in the source YAML. Every failed rule adds a reason code;
a row is valid only with zero reasons. Invalid rows are returned (never dropped) for quarantine.

Reason codes (reason_name defaults to the column name, upper-cased):
  NULL_<NAME>               required value missing / null
  INVALID_<NAME>_TYPE       numeric/boolean/string value of the wrong type ("ABC" for a double)
  INVALID_<NAME>            timestamp/date that cannot be parsed, or not on the hour when required
  <min.reason>/<max.reason> range rules (default <NAME>_BELOW_MIN / <NAME>_ABOVE_MAX)
  NOT_ALLOWED_<NAME>        value outside allowed_values
  <silver.duplicate_reason> later occurrence of an already-seen silver key in the same entity
                            (first occurrence wins, deterministic by page then array index)
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .source_config import Entity, SourceConfig
from .transforms import cast_value, utc_date


@dataclass
class ValidationResult:
    valid: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)
    numeric_strings_converted: int = 0


def validate_row(raw: dict[str, Any], cfg: SourceConfig, utc_offset_seconds: int = 0
                 ) -> tuple[dict[str, Any], list[str], int]:
    """Cast and check one row. Returns (typed_values, reasons, numeric_string_conversions)."""
    reasons: list[str] = []
    conversions = 0
    typed: dict[str, Any] = {}
    for col in cfg.columns:
        value, problem, converted = cast_value(raw.get(col.source), col, cfg, utc_offset_seconds)
        conversions += int(converted)
        if problem == "NULL":
            if col.required:
                reasons.append(f"NULL_{col.reason_name}")
        elif problem == "INVALID_TYPE":
            reasons.append(f"INVALID_{col.reason_name}" if col.type in ("timestamp", "date")
                           else f"INVALID_{col.reason_name}_TYPE")
        elif col.on_the_hour and (value.minute or value.second or value.microsecond):
            reasons.append(f"INVALID_{col.reason_name}")
            value = None
        typed[col.name] = value
        if value is None or problem:
            continue
        if col.min and value < col.min.value:
            reasons.append(col.min.reason)
        if col.max and value > col.max.value:
            reasons.append(col.max.reason)
        if col.allowed_values is not None and value not in col.allowed_values:
            reasons.append(f"NOT_ALLOWED_{col.reason_name}")
    for d in cfg.derived:
        source = typed.get(d.from_column)
        typed[d.name] = utc_date(source) if source is not None else None
    # range rules can repeat a shared reason (e.g. TEMPERATURE_OUT_OF_RANGE for min and max)
    return typed, list(dict.fromkeys(reasons)), conversions


def validate_rows(raw_rows: list[dict[str, Any]], entity: Entity, cfg: SourceConfig,
                  utc_offset_seconds: int = 0) -> ValidationResult:
    """raw_rows: [{"row_index", "page", "raw": {...}, "utc_offset": int?}] in page/array order."""
    result = ValidationResult()
    first_seen: dict[tuple, dict[str, Any]] = {}
    value_cols = [c.name for c in cfg.columns]
    for raw_row in raw_rows:
        offset = raw_row.get("utc_offset", utc_offset_seconds)
        typed, reasons, conversions = validate_row(raw_row["raw"], cfg, offset)
        result.numeric_strings_converted += conversions
        row = {**raw_row, "typed": typed, "dq_errors": reasons, "dq_details": None}
        if not reasons:
            key = tuple(entity.attributes.get(k) if k not in typed else typed[k] for k in cfg.silver_keys)
            kept = first_seen.get(key)
            if kept is None:
                first_seen[key] = row
            else:
                same = all(kept["typed"][c] == typed[c] for c in value_cols)
                row["dq_errors"] = [cfg.duplicate_reason]
                row["dq_details"] = (f"duplicate of page {kept.get('page', 1)} row_index {kept['row_index']}; "
                                     f"values {'identical' if same else 'conflicting'}; first occurrence kept")
        (result.rejected if row["dq_errors"] else result.valid).append(row)
    return result


def raw_record_json(row: dict[str, Any]) -> str:
    return json.dumps({"page": row.get("page", 1), "row_index": row["row_index"], **row["raw"]},
                      default=str, ensure_ascii=False)
