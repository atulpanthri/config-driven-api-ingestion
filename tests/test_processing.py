"""Spark-free end-to-end: fixture API -> fault injection -> bronze records -> validation -> outcomes.
Covers every weather fault mode, failure isolation, pagination outcomes and idempotent source rows."""
import pytest

from conftest import RUN, run_fixture
from ingest_framework.fault_injection import available_faults, normalize_test_mode, resolve_target
from ingest_framework.fixture_source import FixtureSession
from ingest_framework.processing import (
    FAILED, PARTIAL_SUCCESS, SUCCESS, overall_status, run_log_dtl_rows, quarantine_rows, schema_event_rows,
    silver_source_rows, utcnow,
)


def test_normal_weather_run(weather):
    bronze, outcomes = run_fixture(weather)
    assert len(bronze) == 5 and all(r["http_status"] == 200 and r["request_page"] == 1 for r in bronze)
    assert overall_status(outcomes) == SUCCESS
    rows = silver_source_rows(outcomes, weather, RUN, utcnow())
    assert len(rows) == 360 and len({(r["location_id"], r["timestamp_utc"]) for r in rows}) == 360
    assert set(rows[0]) >= {"location_id", "location_name", "latitude", "timestamp_utc", "temperature_c",
                            "weather_date_utc", "created_run_id"}


# mode -> (target status, error_type, bronze row for target?, attempts, valid, quarantine reasons, schema events)
CASES = {
    "bad_request":          (FAILED,  "HTTP_4XX",              True,  1, 0,  set(),                         set()),
    "unreachable_url":      (FAILED,  "CONNECTION_ERROR",      False, 3, 0,  set(),                         set()),
    "timeout":              (FAILED,  "TIMEOUT",               False, 3, 0,  set(),                         set()),
    "server_error":         (FAILED,  "HTTP_5XX",              True,  3, 0,  set(),                         set()),
    "rate_limit":           (SUCCESS, None,                    True,  2, 72, set(),                         set()),
    "invalid_humidity":     (SUCCESS, None,                    True,  1, 71, {"HUMIDITY_ABOVE_100"},        set()),
    "bad_temperature_type": (SUCCESS, None,                    True,  1, 71, {"INVALID_TEMPERATURE_TYPE"},  {"TYPE_CHANGE"}),
    "numeric_string":       (SUCCESS, None,                    True,  1, 72, set(),                         {"TYPE_CHANGE"}),
    "null_values":          (SUCCESS, None,                    True,  1, 71, {"NULL_WIND_SPEED"},           set()),
    "invalid_timestamp":    (SUCCESS, None,                    True,  1, 71, {"INVALID_TIMESTAMP"},         set()),
    "duplicate_record":     (SUCCESS, None,                    True,  1, 72, {"DUPLICATE_TIMESTAMP"},       set()),
    "new_field":            (SUCCESS, None,                    True,  1, 72, set(),                         {"NEW_FIELD"}),
    "missing_field":        (FAILED,  "MISSING_FIELD",         True,  1, 0,  {"MISSING_FIELD"},             {"MISSING_FIELD"}),
    "unequal_arrays":       (FAILED,  "ARRAY_LENGTH_MISMATCH", True,  1, 0,  {"ARRAY_LENGTH_MISMATCH"},     set()),
    "empty_response":       (FAILED,  "EMPTY_RESPONSE",        True,  1, 0,  {"EMPTY_RESPONSE"},            set()),
    "truncated_response":   (FAILED,  "INVALID_JSON",          True,  1, 0,  {"INVALID_JSON"},              set()),
}


def test_every_weather_fault_has_a_case(weather):
    assert set(available_faults(weather)) == set(CASES)


@pytest.mark.parametrize("mode", sorted(CASES))
def test_weather_fault_isolated_to_target(weather, mode):
    status, error_type, has_bronze, attempts, valid, q_reasons, schema_changes = CASES[mode]
    bronze, outcomes = run_fixture(weather, mode)
    now = utcnow()
    target = outcomes["london"]
    assert (target.status, target.error_type, target.attempts) == (status, error_type, attempts)
    assert any(r["entity_id"] == "london" for r in bronze) == has_bronze
    assert len(target.valid_rows) == valid
    assert {x for q in quarantine_rows(outcomes, weather, RUN, now) for x in q["dq_errors"]} == q_reasons
    assert {e["change_type"] for e in schema_event_rows(outcomes, weather, RUN, now)} == schema_changes

    others = [o for eid, o in outcomes.items() if eid != "london"]
    assert all(o.status == SUCCESS and len(o.valid_rows) == 72 and not o.rejected_rows for o in others)
    assert overall_status(outcomes) == (SUCCESS if status == SUCCESS else PARTIAL_SUCCESS)
    audit = run_log_dtl_rows(outcomes, weather, RUN)
    assert sorted(a["entity_name"] for a in audit) == sorted(e.id for e in weather.entities)
    assert all(a["extraction_type"] == "API" and a["run_dtl_id"] == f"{RUN}-{a['entity_name']}" for a in audit)


def test_bad_request_keeps_error_body_and_params(weather):
    bronze, _ = run_fixture(weather, "bad_request")
    london = next(r for r in bronze if r["entity_id"] == "london")
    assert london["http_status"] == 400 and '"latitude": 999' in london["request_params"]


def test_payload_rejection_does_not_invent_observations(weather):
    _, outcomes = run_fixture(weather, "unequal_arrays")
    (row,) = [q for q in quarantine_rows(outcomes, weather, RUN, utcnow()) if q["entity_id"] == "london"]
    assert row["rejection_level"] == "PAYLOAD" and row["raw_values"] is None
    assert '"wind_speed_10m": 71' in row["error_details"]


def test_bad_temperature_quarantine_preserves_raw_value(weather):
    _, outcomes = run_fixture(weather, "bad_temperature_type")
    (row,) = quarantine_rows(outcomes, weather, RUN, utcnow())
    assert row["raw_values"]["temperature_2m"] == "ABC" and row["dq_errors"] == ["INVALID_TEMPERATURE_TYPE"]
    assert '"ABC"' in row["raw_record"]


def test_unexpected_exception_isolated(weather):
    inner = FixtureSession(weather)

    class Exploding:
        def request(self, method, url, params=None, headers=None, timeout=None):
            if params["latitude"] == weather.entity("london").attributes["latitude"]:
                raise RuntimeError("boom")
            return inner.request(method, url, params=params, headers=headers, timeout=timeout)

    _, outcomes = run_fixture(weather, session=Exploding())
    assert outcomes["london"].error_type == "UNEXPECTED_ERROR"
    assert overall_status(outcomes) == PARTIAL_SUCCESS


def test_statuses(weather):
    _, outcomes = run_fixture(weather)
    assert overall_status(outcomes, essential_write_error="silver MERGE failed") == FAILED
    for o in outcomes.values():
        o.fail("TIMEOUT", "x")
    assert overall_status(outcomes) == FAILED and overall_status({}) == FAILED


def test_same_input_identical_silver_source(weather):
    _, first = run_fixture(weather)
    _, second = run_fixture(weather)
    now = utcnow()

    def keyed(outcomes):
        return {(r["location_id"], r["timestamp_utc"]): (r["temperature_c"], r["relative_humidity_pct"],
                                                         r["precipitation_mm"], r["wind_speed_kmh"])
                for r in silver_source_rows(outcomes, weather, RUN, now)}

    assert keyed(first) == keyed(second) and len(keyed(first)) == 360


def test_fault_target_defaults_to_config(weather):
    assert resolve_target(weather, None).id == "london" and resolve_target(weather, "tokyo").id == "tokyo"
    with pytest.raises(ValueError):
        resolve_target(weather, "atlantis")


def test_unknown_test_mode_rejected(weather, usgs):
    with pytest.raises(ValueError):
        normalize_test_mode("delete_everything", weather)
    with pytest.raises(ValueError):
        normalize_test_mode("unequal_arrays", usgs)  # columnar-only fault
    assert normalize_test_mode("none", weather) is None
