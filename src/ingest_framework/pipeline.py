"""End-to-end run for ONE configured source:
extract (API now; FILE/DB planned) -> landing files + met_raw -> (schema checks, extract rows, DQ)
-> met_quarantine / met_silver -> met_gold, with run_log, run_log_dtl and schema_change_log in met_audit.

Order of operations and failure handling:
  1. config validated (load_source) and window resolved; RUNNING row in met_audit.run_log
  2. every entity extracted independently (all pages/units); received data written to the landing
     volume and appended to bronze
  3. this run's rows read back from bronze (bronze is the source of truth; replay_run_id
     reprocesses an earlier run with the same code)
  4. each entity processed independently (a bad payload only fails its entity)
  5. schema events + quarantine appended, silver MERGE, gold MERGE for affected keys
  6. always (finally) run_log_dtl rows written (one per entity) and run_log finalized
A failed landing/Delta write in steps 2-5 marks the run FAILED with the error message.
A failed audit write is re-raised: we never report a run we could not record.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

import requests

from . import persistence, tables
from .api_client import SecretResolver
from .fault_injection import build_session, normalize_test_mode, resolve_target
from .fixture_source import FixtureSession
from .processing import (
    FAILED, RUNNING, SUCCESS, EntityOutcome, ingest_entities, overall_status, process_bronze,
    quarantine_rows, run_log_dtl_rows, schema_event_rows, silver_source_rows, utcnow,
)
from .source_config import Entity, SourceConfig, resolve_window


def _outcomes_for_replay(records: list[dict[str, Any]], cfg: SourceConfig) -> dict[str, EntityOutcome]:
    known = {e.id: e for e in cfg.entities}
    outcomes: dict[str, EntityOutcome] = {}
    for rec in sorted(records, key=lambda r: (r["entity_id"], r["request_page"])):
        entity = known.get(rec["entity_id"]) or Entity(rec["entity_id"], json.loads(rec["entity_attributes"] or "{}"))
        o = outcomes.setdefault(entity.id, EntityOutcome(entity=entity, fetch_ok=True))
        o.pages += 1
        o.attempts += rec["attempt_count"] or 0
        o.http_status, o.request_end = rec["http_status"], rec["ingestion_timestamp"]
        if rec["http_status"] != 200:
            o.fetch_ok = False
            o.fail(f"HTTP_{rec['http_status']}", "replayed bronze record was not a 200 response")
    return outcomes


def run_pipeline(spark, cfg: SourceConfig, *, source_mode: str = "live", test_mode: str | None = None,
                 fault_entity_id: str | None = None, window_overrides: dict[str, Any] | None = None,
                 replay_run_id: str | None = None, session: Any = None,
                 secret_resolver: SecretResolver | None = None, landing_root: str | None = None,
                 write_landing: bool = True) -> dict[str, Any]:
    if source_mode not in ("live", "fixture"):
        raise ValueError("source_mode must be 'live' or 'fixture'")
    mode = normalize_test_mode(test_mode, cfg)
    if mode and replay_run_id:
        raise ValueError("test_mode cannot be combined with replay_run_id")
    target = resolve_target(cfg, fault_entity_id) if mode else None
    window = resolve_window(cfg, window_overrides)

    run_id = str(uuid.uuid4())
    start = utcnow()
    # run-level settings are recorded per entity in run_log_dtl.parameters
    run_parameters = {**cfg.run_parameters(window), "source_mode": source_mode, "test_mode": mode,
                      "fault_entity_id": target.id if target else None, "replay_run_id": replay_run_id}
    run_row: dict[str, Any] = {
        "run_id": run_id, "source_name": cfg.source_name, "start_time": start, "status": RUNNING,
        "source_mode": source_mode, "test_mode": mode,
    }
    entities_requested = len(cfg.entities)
    persistence.start_run_log(spark, cfg, run_row)  # if this fails nothing else runs

    outcomes: dict[str, EntityOutcome] = {}
    counts = dict(bronze_rows=0, landing_files=0, source_rows=0, silver_valid_rows=0, silver_rows_inserted=0,
                  silver_rows_updated=0, quarantine_rows=0, schema_events=0,
                  gold_rows_inserted=0, gold_rows_updated=0)
    essential_error: str | None = None
    try:
        if replay_run_id:
            records = persistence.read_bronze_run(spark, cfg, replay_run_id)
            outcomes = _outcomes_for_replay(records, cfg)
            entities_requested = len(outcomes)
        else:
            if session is None:
                base = FixtureSession(cfg) if source_mode == "fixture" else requests.Session()
                session = build_session(base, cfg, mode, target)
            bronze, outcomes = ingest_entities(cfg, session, run_id, window, source_mode=source_mode,
                                               test_mode=mode, fault_target=target, secret_resolver=secret_resolver)
            if write_landing:
                counts["landing_files"] = persistence.write_landing_files(cfg, run_id, bronze, landing_root)
            counts["bronze_rows"] = persistence.append_rows(spark, cfg, tables.BRONZE, bronze)
            records = persistence.read_bronze_run(spark, cfg, run_id)

        process_bronze(records, outcomes, cfg)
        counts["source_rows"] = sum(o.records_received for o in outcomes.values())

        now = utcnow()
        counts["schema_events"] = persistence.append_rows(
            spark, cfg, tables.SCHEMA_CHANGE_LOG, schema_event_rows(outcomes, cfg, run_id, now))
        counts["quarantine_rows"] = persistence.append_rows(
            spark, cfg, tables.QUARANTINE, quarantine_rows(outcomes, cfg, run_id, now))

        silver_rows = silver_source_rows(outcomes, cfg, run_id, now)
        (counts["silver_valid_rows"], counts["silver_rows_inserted"],
         counts["silver_rows_updated"]) = persistence.merge_silver(spark, cfg, run_id, silver_rows)

        affected = {tuple(r[k] for k in cfg.gold_keys) for r in silver_rows}
        counts["gold_rows_inserted"], counts["gold_rows_updated"] = persistence.merge_gold(spark, cfg, run_id, affected)
    except Exception as exc:
        essential_error = f"{type(exc).__name__}: {exc}"[:1000]
    finally:
        status = overall_status(outcomes, essential_error)
        successes = sum(1 for o in outcomes.values() if o.status == SUCCESS)
        end = utcnow()
        failed = [f"{o.entity.id}: {o.error_type}" for o in outcomes.values() if o.status != SUCCESS]
        run_row.update({k: v for k, v in counts.items() if k != "landing_files"}, status=status,
                       job_success=successes, job_failed=entities_requested - successes,
                       duration_seconds=round((end - start).total_seconds(), 3),
                       error_message=essential_error or ("; ".join(failed) or None))
        # Audit writes are not swallowed: if they fail, the exception propagates.
        persistence.append_rows(spark, cfg, tables.RUN_LOG_DTL,
                                run_log_dtl_rows(outcomes, cfg, run_id, run_parameters))
        persistence.finalize_run_log(spark, cfg, run_row)

    return {
        "run_id": run_id,
        "source_name": cfg.source_name,
        "status": status,
        **counts,
        "duration_seconds": run_row["duration_seconds"],
        "error_message": run_row["error_message"],
        "entities": {eid: {"status": o.status, "http_status": o.http_status, "pages": o.pages,
                           "attempts": o.attempts, "valid": len(o.valid_rows), "rejected": len(o.rejected_rows),
                           "error_type": o.error_type} for eid, o in outcomes.items()},
        "failed": status == FAILED,
    }
