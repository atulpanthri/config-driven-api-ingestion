from datetime import datetime, timezone

import pytest

from ingest_framework.source_config import ColumnSpec
from ingest_framework.transforms import (
    MISSING, PayloadError, cast_value, check_array_lengths, extract_rows, get_path, parse_payload, parse_timestamp,
    to_float,
)


@pytest.mark.parametrize("body, reason", [
    (None, "EMPTY_RESPONSE"), ("", "EMPTY_RESPONSE"), ("   ", "EMPTY_RESPONSE"),
    ('{"hourly": {"time": [', "INVALID_JSON"), ('"just a string"', "INVALID_JSON_STRUCTURE"),
    ('{"error": true, "reason": "bad"}', "API_ERROR"),
])
def test_parse_payload_errors(weather, body, reason):
    with pytest.raises(PayloadError) as exc:
        parse_payload(body, weather)
    assert exc.value.reason == reason


def test_cut_off_response_is_invalid_json(weather, tokyo_body):
    with pytest.raises(PayloadError, match="INVALID_JSON"):
        parse_payload(tokyo_body[: len(tokyo_body) // 2], weather)


def test_get_path():
    obj = {"a": {"b": [10, 20, {"c": 1}]}}
    assert get_path(obj, "a.b.1") == 20 and get_path(obj, "a.b.2.c") == 1
    assert get_path(obj, "a.x") is MISSING and get_path(obj, "a.b.9") is MISSING
    assert get_path(obj, "") is obj


def test_columnar_extract_one_row_per_hour(weather, tokyo_payload):
    rows = extract_rows(tokyo_payload, weather)
    assert len(rows) == 72
    assert rows[5]["raw"]["time"] == tokyo_payload["hourly"]["time"][5]
    assert rows[5]["raw"]["wind_speed_10m"] == tokyo_payload["hourly"]["wind_speed_10m"][5]


def test_unequal_arrays_rejected_not_truncated(weather, tokyo_payload):
    tokyo_payload["hourly"]["wind_speed_10m"].pop()
    with pytest.raises(PayloadError) as exc:
        extract_rows(tokyo_payload, weather)
    assert exc.value.reason == "ARRAY_LENGTH_MISMATCH"
    assert '"wind_speed_10m": 71' in exc.value.details and '"time": 72' in exc.value.details


def test_check_array_lengths_ok(tokyo_payload):
    assert check_array_lengths(tokyo_payload["hourly"], ["time", "temperature_2m"]) == 72


def test_records_extract_nested_paths(usgs):
    payload = {"features": [{"id": "a", "properties": {"mag": 4.2, "time": 1790973296862},
                             "geometry": {"coordinates": [142.8, 40.0, 49.9]}}]}
    (row,) = extract_rows(payload, usgs)
    assert row["raw"]["properties.mag"] == 4.2 and row["raw"]["geometry.coordinates.2"] == 49.9
    assert row["raw"]["properties.place"] is None


@pytest.mark.parametrize("value, expected", [
    (21, (21.0, None, False)), (21.5, (21.5, None, False)), ("21.5", (21.5, None, True)),
    (" 3 ", (3.0, None, True)), (None, (None, "NULL", False)), ("ABC", (None, "INVALID_TYPE", False)),
    (True, (None, "INVALID_TYPE", False)), ("NaN", (None, "INVALID_TYPE", False)),
    (float("inf"), (None, "INVALID_TYPE", False)), ([1], (None, "INVALID_TYPE", False)),
])
def test_to_float(value, expected):
    assert to_float(value) == expected


def test_numeric_string_reject_policy():
    assert to_float("21.5", "reject") == (None, "INVALID_TYPE", False)


def test_parse_timestamp_formats():
    fmt = "%Y-%m-%dT%H:%M"
    assert parse_timestamp("2026-10-07T05:00", fmt)[0] == datetime(2026, 10, 7, 5, tzinfo=timezone.utc)
    assert parse_timestamp("2026-10-07T09:00", fmt, 9 * 3600)[0] == datetime(2026, 10, 7, 0, tzinfo=timezone.utc)
    assert parse_timestamp(1790973296862, "epoch_ms")[0] == datetime(2026, 10, 2, 20, 34, 56, 862000, tzinfo=timezone.utc)
    assert parse_timestamp(1790973296, "epoch_s")[0].year == 2026
    assert parse_timestamp("2026-10-07T05:00:00Z", "iso8601")[0] == datetime(2026, 10, 7, 5, tzinfo=timezone.utc)
    for bad in ("2026-13-45T99:00", "2026-10-07", "", None, 123):
        assert parse_timestamp(bad, fmt)[0] is None
    assert parse_timestamp("abc", "epoch_ms")[0] is None


def col(t, **kw):
    return ColumnSpec(name="c", source="c", type=t, required=True, reason_name="C", **kw)


@pytest.mark.parametrize("spec, value, expected", [
    (col("int"), 5, (5, None, False)), (col("int"), 5.5, (None, "INVALID_TYPE", False)),
    (col("bigint"), "7", (7, None, True)), (col("boolean"), True, (True, None, False)),
    (col("boolean"), "yes", (None, "INVALID_TYPE", False)), (col("string"), 12, ("12", None, False)),
    (col("string"), {"a": 1}, (None, "INVALID_TYPE", False)),
    (col("date", format="%Y-%m-%d"), "2026-10-07", (datetime(2026, 10, 7).date(), None, False)),
])
def test_cast_value(weather, spec, value, expected):
    assert cast_value(value, spec, weather) == expected
