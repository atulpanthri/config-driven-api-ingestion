"""Spark-free orchestration: ingest each entity (all pages), then turn bronze records into
silver / quarantine / schema-event / audit rows. Pure Python so isolation, validation and
idempotency are testable locally; pipeline.py adds the Spark/Delta I/O.

Entity status rules:
  SUCCESS  every page HTTP 200, every payload passed the contract, and at least one valid row
           (or zero rows when response.allow_empty is true); some rows may still be quarantined
  FAILED   no response / non-200 after retries on any page, pagination limit hit, a payload
           rejected, no valid rows, or an unexpected error for this entity. A FAILED entity loads
           nothing (no partial snapshot); its fetched pages stay in bronze.
Run status: SUCCESS if all entities succeeded, FAILED if none did or an essential Delta write
failed, PARTIAL_SUCCESS otherwise.
"""
from __future__ import annotations

import json
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from .api_client import SecretResolver, sanitize
from .data_quality import raw_record_json, validate_rows
from .extractors import get_extractor
from .fault_injection import request_overrides
from .schema_validation import check_payload_schema
from .source_config import Entity, SourceConfig
from .transforms import PayloadError, extract_rows, parse_payload, raw_as_text, utc_offset

SUCCESS, PARTIAL_SUCCESS, FAILED, RUNNING = "SUCCESS", "PARTIAL_SUCCESS", "FAILED", "RUNNING"


@dataclass
class EntityOutcome:
    entity: Entity
    status: str = FAILED
    request_start: datetime | None = None
    request_end: datetime | None = None
    http_status: int | None = None
    pages: int = 0
    attempts: int = 0
    error_type: str | None = None
    error_message: str | None = None
    fetch_ok: bool = False
    records_received: int = 0
    valid_rows: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)
    schema_events: list[dict[str, Any]] = field(default_factory=list)
    payload_rejection: dict[str, Any] | None = None
    numeric_strings_converted: int = 0
    ingestion_timestamp: datetime | None = None
    source_uri: str | None = None                       # first unit's URI (for run_log_dtl.parameters)
    request_params: dict[str, Any] | None = None        # first unit's params, secrets masked

    def fail(self, error_type: str, message: str) -> None:
        self.status, self.error_type, self.error_message = FAILED, error_type, sanitize(message)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- ingestion
def ingest_entities(cfg: SourceConfig, session: Any, run_id: str, window: dict[str, Any], *,
                    source_mode: str, test_mode: str | None = None, fault_target: Entity | None = None,
                    secret_resolver: SecretResolver | None = None,
                    sleep: Callable[[float], None] | None = None) -> tuple[list[dict[str, Any]], dict[str, EntityOutcome]]:
    """Extract every entity independently (via the source's Extractor). Never raises for a single
    entity's failure. Returns bronze records (one per received raw unit/page, including error bodies)
    and outcomes. A timeout has no body, so it produces no bronze row; it is recorded in run_log_dtl."""
    extractor = get_extractor(cfg, session)
    bronze: list[dict[str, Any]] = []
    outcomes: dict[str, EntityOutcome] = {}
    for entity in cfg.entities:
        outcome = EntityOutcome(entity=entity, request_start=utcnow())
        outcomes[entity.id] = outcome
        try:
            params_override, url_override = request_overrides(test_mode, cfg, entity, fault_target)
            units, extraction_error = extractor.extract(entity, window, params_override=params_override,
                                                        base_url_override=url_override,
                                                        secret_resolver=secret_resolver, sleep=sleep)

            outcome.request_start, outcome.request_end = units[0].request_start, units[-1].request_end
            outcome.pages, outcome.attempts = len(units), sum(u.attempts for u in units)
            outcome.http_status = units[-1].status_code
            outcome.ingestion_timestamp = units[-1].request_end
            outcome.source_uri, outcome.request_params = units[0].source_uri, units[0].request_params
            failed = next((u for u in units if not u.ok), None)
            if failed:
                outcome.fail(failed.error_type or "UNKNOWN",
                             f"page {failed.page}: {failed.error_message or 'request failed'}")
            elif extraction_error:
                outcome.fail("PAGINATION_LIMIT", extraction_error)
            else:
                outcome.fetch_ok = True
            for p in units:
                if p.body_text is not None:
                    bronze.append({
                        "run_id": run_id,
                        "source_name": cfg.source_name,
                        "extraction_type": cfg.extraction_type.upper(),
                        "ingestion_timestamp": p.request_end,
                        "entity_id": entity.id,
                        "entity_attributes": json.dumps(entity.attributes, sort_keys=True, default=str),
                        "source_uri": p.source_uri,
                        "request_params": json.dumps(p.request_params, sort_keys=True, default=str),
                        "request_page": p.page,
                        "http_status": p.status_code,
                        "attempt_count": p.attempts,
                        "raw_response": p.body_text,
                        "raw_file_path": None,
                        "source_mode": source_mode,
                        "test_mode": test_mode,
                    })
        except Exception as exc:  # isolate anything unexpected to this entity
            outcome.request_end = utcnow()
            outcome.fail("UNEXPECTED_ERROR", f"{type(exc).__name__}: {exc}")
    return bronze, outcomes


# --------------------------------------------------------------------------- processing
def process_entity_records(records: list[dict[str, Any]], outcome: EntityOutcome, cfg: SourceConfig) -> None:
    """Validate and flatten all bronze pages of one entity into `outcome`. Never raises."""
    if not outcome.fetch_ok:
        return  # fetch failed: error bodies stay in bronze, nothing is loaded
    records = sorted(records, key=lambda r: r["request_page"])
    if records:
        outcome.ingestion_timestamp = records[-1]["ingestion_timestamp"]
    try:
        raw_rows: list[dict[str, Any]] = []
        for rec in records:
            page = rec["request_page"]
            try:
                payload = parse_payload(rec.get("raw_response"), cfg)
                check = check_payload_schema(payload, cfg)
                outcome.schema_events.extend({**e, "request_page": page} for e in check.events)
                if not check.contract_ok:
                    problems = [e for e in check.events if e["change_type"] != "NEW_FIELD"]
                    raise PayloadError(check.blocking_errors[0],
                                       "; ".join(f"{e['change_type']} {e['field_name']}" for e in problems))
                offset = utc_offset(payload, cfg)
                rows = extract_rows(payload, cfg)
            except PayloadError as exc:
                raise PayloadError(exc.reason, f"page {page}: {exc.details}") from exc
            raw_rows.extend({**r, "page": page, "utc_offset": offset} for r in rows)

        outcome.records_received = len(raw_rows)
        result = validate_rows(raw_rows, outcome.entity, cfg)
        outcome.valid_rows, outcome.rejected_rows = result.valid, result.rejected
        outcome.numeric_strings_converted = result.numeric_strings_converted
        if outcome.valid_rows or (cfg.allow_empty and not raw_rows):
            outcome.status, outcome.error_type, outcome.error_message = SUCCESS, None, None
        else:
            outcome.fail("NO_VALID_ROWS", f"all {len(raw_rows)} rows rejected")
    except PayloadError as exc:
        outcome.payload_rejection = {"reason": exc.reason, "details": exc.details}
        outcome.valid_rows, outcome.rejected_rows = [], []
        outcome.fail(exc.reason, exc.details)
    except Exception as exc:
        outcome.valid_rows, outcome.rejected_rows = [], []
        outcome.fail("UNEXPECTED_ERROR", f"{type(exc).__name__}: {exc} | {traceback.format_exc(limit=2)}")


def process_bronze(records: list[dict[str, Any]], outcomes: dict[str, EntityOutcome], cfg: SourceConfig) -> None:
    by_entity: dict[str, list[dict[str, Any]]] = {}
    for rec in records:
        by_entity.setdefault(rec["entity_id"], []).append(rec)
    for entity_id, outcome in outcomes.items():
        process_entity_records(by_entity.get(entity_id, []), outcome, cfg)


def overall_status(outcomes: dict[str, EntityOutcome], essential_write_error: str | None = None) -> str:
    if essential_write_error:
        return FAILED
    successes = sum(1 for o in outcomes.values() if o.status == SUCCESS)
    if outcomes and successes == len(outcomes):
        return SUCCESS
    return FAILED if successes == 0 else PARTIAL_SUCCESS


# --------------------------------------------------------------------------- row builders
def silver_source_rows(outcomes: dict[str, EntityOutcome], cfg: SourceConfig, run_id: str,
                       now: datetime) -> list[dict[str, Any]]:
    rows = []
    for o in outcomes.values():
        if o.status != SUCCESS:
            continue
        entity_values = {c.name: o.entity.attributes.get(c.name) for c in cfg.entity_columns}
        for r in o.valid_rows:
            rows.append({**entity_values, **r["typed"],
                         "source_ingestion_timestamp": o.ingestion_timestamp,
                         "created_run_id": run_id, "last_updated_run_id": run_id,
                         "created_at": now, "updated_at": now})
    return rows


def quarantine_rows(outcomes: dict[str, EntityOutcome], cfg: SourceConfig, run_id: str,
                    now: datetime) -> list[dict[str, Any]]:
    rows = []
    for o in outcomes.values():
        base = {"run_id": run_id, "source_name": cfg.source_name, "entity_id": o.entity.id,
                "source_ingestion_timestamp": o.ingestion_timestamp, "quarantined_at": now}
        if o.payload_rejection:
            # Payload-level defect: no observation can be formed, so none is invented.
            rows.append({**base, "rejection_level": "PAYLOAD", "request_page": None, "row_index": None,
                         "raw_values": None, "dq_errors": [o.payload_rejection["reason"]],
                         "error_details": sanitize(o.payload_rejection["details"]), "raw_record": None})
        for r in o.rejected_rows:
            rows.append({**base, "rejection_level": "ROW", "request_page": r.get("page", 1),
                         "row_index": r["row_index"],
                         "raw_values": {k: raw_as_text(v) for k, v in r["raw"].items()},
                         "dq_errors": list(r["dq_errors"]), "error_details": r.get("dq_details"),
                         "raw_record": raw_record_json(r)})
    return rows


def schema_event_rows(outcomes: dict[str, EntityOutcome], cfg: SourceConfig, run_id: str,
                      now: datetime) -> list[dict[str, Any]]:
    return [{"run_id": run_id, "source_name": cfg.source_name, "entity_id": o.entity.id,
             "detected_at": now, **e} for o in outcomes.values() for e in o.schema_events]


def run_log_dtl_rows(outcomes: dict[str, EntityOutcome], cfg: SourceConfig, run_id: str,
                       run_parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """One met_audit.run_log_dtl row per entity. `parameters` holds what was extracted and how:
    source URI and request params of the first unit (secrets already masked) plus run settings."""
    def parameters(o: EntityOutcome) -> str:
        return json.dumps({"source_uri": o.source_uri, "request_params": o.request_params,
                           **(run_parameters or {})}, sort_keys=True, default=str)

    return [{
        "run_dtl_id": f"{run_id}-{o.entity.id}",
        "run_id": run_id,
        "entity_name": o.entity.id,
        "extraction_type": cfg.extraction_type.upper(),
        "parameters": parameters(o),
        "start_time": o.request_start,
        "end_time": o.request_end,
        "status": o.status,
        "http_status": o.http_status,
        "page_count": o.pages,
        "attempt_count": o.attempts,
        "retry_count": max(o.attempts - o.pages, 0),
        "records_received": o.records_received,
        "records_valid": len(o.valid_rows),
        "records_rejected": len(o.rejected_rows),
        "schema_events": len(o.schema_events),
        "numeric_strings_converted": o.numeric_strings_converted,
        "error_type": o.error_type,
        "error_message": o.error_message,
    } for o in outcomes.values()]
