import copy
import json

from conftest import ROOT
from ingest_framework.schema_validation import check_payload_schema


def changes(check):
    return {(e["change_type"], e["field_name"]) for e in check.events}


# ---------------------------------------------------------------- columnar (weather)
def test_real_weather_payload_matches_contract(weather, tokyo_payload):
    check = check_payload_schema(tokyo_payload, weather)
    assert check.contract_ok and check.events == []


def test_new_field_logged_not_blocking(weather, tokyo_payload):
    p = copy.deepcopy(tokyo_payload)
    p["hourly"]["cloud_cover"] = [10] * len(p["hourly"]["time"])
    check = check_payload_schema(p, weather)
    assert check.contract_ok and changes(check) == {("NEW_FIELD", "hourly.cloud_cover")}
    assert check.events[0]["actual_type"] == "array<number>"


def test_missing_required_field_blocks(weather, tokyo_payload):
    p = copy.deepcopy(tokyo_payload)
    del p["hourly"]["precipitation"]
    check = check_payload_schema(p, weather)
    assert not check.contract_ok and ("MISSING_FIELD", "hourly.precipitation") in changes(check)


def test_missing_data_container_blocks(weather, tokyo_payload):
    p = copy.deepcopy(tokyo_payload)
    del p["hourly"]
    assert check_payload_schema(p, weather).blocking_errors == ["MISSING_FIELD"]


def test_number_as_string_logged_not_blocking(weather, tokyo_payload):
    p = copy.deepcopy(tokyo_payload)
    p["hourly"]["temperature_2m"][0] = "21.5"
    check = check_payload_schema(p, weather)
    e = check.events[0]
    assert check.contract_ok and (e["change_type"], e["expected_type"], e["actual_type"]) == (
        "TYPE_CHANGE", "number", "number|string")


def test_array_replaced_by_scalar_blocks(weather, tokyo_payload):
    p = copy.deepcopy(tokyo_payload)
    p["hourly"]["wind_speed_10m"] = "n/a"
    check = check_payload_schema(p, weather)
    assert not check.contract_ok and check.events[0]["actual_type"] == "string"


def test_unit_change_blocks(weather, tokyo_payload):
    p = copy.deepcopy(tokyo_payload)
    p["hourly_units"]["wind_speed_10m"] = "m/s"
    assert check_payload_schema(p, weather).blocking_errors == ["UNIT_CHANGE"]


def test_nulls_are_not_schema_changes(weather, tokyo_payload):
    p = copy.deepcopy(tokyo_payload)
    p["hourly"]["temperature_2m"][0] = None
    assert check_payload_schema(p, weather).events == []


# ---------------------------------------------------------------- records (usgs)
def usgs_payload():
    return json.loads((ROOT / "tests" / "fixtures" / "usgs_earthquakes" / "alaska.json").read_text(encoding="utf-8"))


def test_real_usgs_payload_matches_contract(usgs):
    check = check_payload_schema(usgs_payload(), usgs)
    assert check.contract_ok and check.events == []


def test_records_new_nested_field(usgs):
    p = usgs_payload()
    p["features"][0]["properties"]["new_metric"] = 1
    check = check_payload_schema(p, usgs)
    assert check.contract_ok and changes(check) == {("NEW_FIELD", "features[].properties.new_metric")}


def test_records_field_missing_everywhere_blocks(usgs):
    p = usgs_payload()
    for f in p["features"]:
        del f["properties"]["time"]
    check = check_payload_schema(p, usgs)
    assert check.blocking_errors == ["MISSING_FIELD"]


def test_records_field_missing_in_some_is_row_level(usgs):
    p = usgs_payload()
    del p["features"][0]["properties"]["time"]
    assert check_payload_schema(p, usgs).contract_ok


def test_records_type_change(usgs):
    p = usgs_payload()
    p["features"][0]["properties"]["mag"] = "4.1"
    check = check_payload_schema(p, usgs)
    assert check.contract_ok and ("TYPE_CHANGE", "features[].properties.mag") in changes(check)


def test_records_not_objects_blocks(usgs):
    assert check_payload_schema({"features": [1, 2]}, usgs).blocking_errors == ["TYPE_CHANGE"]


def test_empty_records_no_events(usgs):
    check = check_payload_schema({"features": []}, usgs)
    assert check.contract_ok and check.events == []
