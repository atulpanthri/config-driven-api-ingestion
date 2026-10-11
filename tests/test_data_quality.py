import pytest

from ingest_framework.data_quality import validate_row, validate_rows


def raw(i=0, time="2026-10-07T00:00", temp=20.0, hum=50.0, precip=0.0, wind=10.0, page=1):
    return {"row_index": i, "page": page, "raw": {"time": time, "temperature_2m": temp, "relative_humidity_2m": hum,
                                                  "precipitation": precip, "wind_speed_10m": wind}}


@pytest.mark.parametrize("kwargs, reasons", [
    ({}, []),
    ({"hum": 0}, []), ({"hum": 100}, []),
    ({"hum": 100.1}, ["HUMIDITY_ABOVE_100"]), ({"hum": 130}, ["HUMIDITY_ABOVE_100"]),
    ({"hum": -0.1}, ["HUMIDITY_BELOW_ZERO"]),
    ({"precip": -0.1}, ["NEGATIVE_PRECIPITATION"]), ({"precip": 0}, []),
    ({"wind": -1}, ["NEGATIVE_WIND_SPEED"]),
    ({"temp": 60}, []), ({"temp": 60.1}, ["TEMPERATURE_OUT_OF_RANGE"]), ({"temp": -91}, ["TEMPERATURE_OUT_OF_RANGE"]),
    ({"temp": "ABC"}, ["INVALID_TEMPERATURE_TYPE"]), ({"hum": "wet"}, ["INVALID_HUMIDITY_TYPE"]),
    ({"temp": None}, ["NULL_TEMPERATURE"]), ({"wind": None}, ["NULL_WIND_SPEED"]),
    ({"time": "2026-13-45T99:00"}, ["INVALID_TIMESTAMP"]), ({"time": None}, ["NULL_TIMESTAMP"]),
    ({"time": "2026-10-07T05:30"}, ["INVALID_TIMESTAMP"]),
    ({"temp": "ABC", "hum": 130, "wind": -2}, ["INVALID_TEMPERATURE_TYPE", "HUMIDITY_ABOVE_100", "NEGATIVE_WIND_SPEED"]),
])
def test_weather_rules_keep_original_reason_codes(weather, kwargs, reasons):
    _, got, _ = validate_row(raw(**kwargs)["raw"], weather)
    assert got == reasons


def test_numeric_string_converted_and_counted(weather):
    typed, reasons, conversions = validate_row(raw(temp="21.5")["raw"], weather)
    assert reasons == [] and typed["temperature_c"] == 21.5 and conversions == 1


def test_derived_utc_date(weather):
    typed, _, _ = validate_row(raw(time="2026-10-07T23:00")["raw"], weather)
    assert str(typed["weather_date_utc"]) == "2026-10-07"


def test_invalid_rows_kept_with_raw_values(weather):
    result = validate_rows([raw(0), raw(1, time="2026-10-07T01:00", temp="ABC")], weather.entity("tokyo"), weather)
    assert len(result.valid) == 1 and result.rejected[0]["raw"]["temperature_2m"] == "ABC"


def test_duplicate_key_first_occurrence_wins(weather):
    rows = [raw(0, temp=20.0), raw(1, time="2026-10-07T01:00"), raw(2, temp=25.0)]
    result = validate_rows(rows, weather.entity("tokyo"), weather)
    assert [r["row_index"] for r in result.valid] == [0, 1] and result.valid[0]["typed"]["temperature_c"] == 20.0
    dup = result.rejected[0]
    assert dup["dq_errors"] == ["DUPLICATE_TIMESTAMP"] and "conflicting" in dup["dq_details"]


def test_duplicate_across_pages(usgs):
    rec = {"id": "x", "properties.time": 1790973296862, "properties.mag": 4.0, "properties.place": "p",
           "properties.type": "earthquake", "properties.updated": 1790973296862,
           "geometry.coordinates.0": 1.0, "geometry.coordinates.1": 2.0, "geometry.coordinates.2": 3.0}
    rows = [{"row_index": 0, "page": 1, "raw": rec}, {"row_index": 0, "page": 2, "raw": dict(rec)}]
    result = validate_rows(rows, usgs.entity("japan"), usgs)
    assert len(result.valid) == 1 and result.rejected[0]["dq_errors"] == ["DUPLICATE_EVENT"]
    assert "identical" in result.rejected[0]["dq_details"]


def test_usgs_rules(usgs):
    base = {"id": "x", "properties.time": 1790973296862, "properties.mag": 4.0,
            "geometry.coordinates.0": 1.0, "geometry.coordinates.1": 2.0, "geometry.coordinates.2": 3.0}
    assert validate_row(base, usgs)[1] == []
    assert validate_row({**base, "properties.mag": 15}, usgs)[1] == ["MAGNITUDE_OUT_OF_RANGE"]
    assert validate_row({**base, "properties.time": "soon"}, usgs)[1] == ["INVALID_EVENT_TIME"]
    assert validate_row({**base, "id": None}, usgs)[1] == ["NULL_EVENT_ID"]
    # optional column may be null
    assert validate_row({**base, "geometry.coordinates.2": None}, usgs)[1] == []
