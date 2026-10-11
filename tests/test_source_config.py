from datetime import date

import pytest

from conftest import ROOT, build
from ingest_framework.source_config import ConfigError, list_sources, load_source, resolve_window


def test_all_sources_load_and_validate():
    names = list_sources(ROOT)
    assert {"open_meteo_weather", "usgs_earthquakes"} <= set(names)
    for name in names:
        load_source(ROOT, name)


def test_weather_config_values(weather):
    assert len(weather.entities) == 5 and weather.entity("london").attributes["latitude"] == 51.5072
    assert weather.bronze_fqn == "workspace.met_raw.weather_raw"
    assert weather.silver_fqn == "workspace.met_silver.weather_hourly"
    assert weather.gold_fqn == "workspace.met_gold.weather_daily"
    assert weather.audit_table("run_log") == "workspace.met_audit.run_log"
    assert weather.quarantine_table("data_quarantine") == "workspace.met_quarantine.data_quarantine"
    assert weather.landing_root == "/Volumes/workspace/met_raw/raw_landing"
    assert weather.extraction_type == "api"
    assert weather.silver_keys == ("location_id", "timestamp_utc")


def test_schema_suffix_applies_to_every_layer():
    cfg = load_source(ROOT, "open_meteo_weather", schema_suffix="_edge")
    assert cfg.schemas == {"raw": "met_raw_edge", "silver": "met_silver_edge", "gold": "met_gold_edge",
                           "audit": "met_audit_edge", "quarantine": "met_quarantine_edge"}
    assert cfg.silver_fqn == "workspace.met_silver_edge.weather_hourly"
    assert cfg.landing_root == "/Volumes/workspace/met_raw_edge/raw_landing"


@pytest.mark.parametrize("kind, message", [
    ("file", "planned but not implemented"),
    ("db", "planned but not implemented"),
    ("ftp", "extraction.type must be one of"),
])
def test_extraction_types(weather_raw, kind, message):
    weather_raw["extraction"] = {"type": kind}
    with pytest.raises(ConfigError, match=message):
        build(weather_raw)


def test_api_extraction_requires_base_url(weather_raw):
    del weather_raw["request"]["base_url"]
    with pytest.raises(ConfigError, match="base_url is required for api"):
        build(weather_raw)


def mutate(raw, path, value):
    node = raw
    keys = path.split(".")
    for k in keys[:-1]:
        node = node[int(k)] if isinstance(node, list) else node[k]
    if value is KeyError:
        del node[keys[-1]]
    elif isinstance(node, list):
        node[int(keys[-1])] = value
    else:
        node[keys[-1]] = value
    return raw


@pytest.mark.parametrize("path, value, message", [
    ("entities.min_count", 6, "at least 6 entities"),
    ("entities.items.1.location_id", "tokyo", "unique"),
    ("entities.items.0.location_id", "bad id", "simple identifier"),
    ("request.params.latitude", "{entity.lat}", "not an attribute"),
    ("request.params.forecast_days", "{window.days}", "not defined under window"),
    ("request.params.api_key", "abc123", "looks like a credential"),
    ("request.base_url", "http://api.open-meteo.com/v1/forecast", "must use https"),
    ("request.auth", {"type": "api_key", "in": "header"}, "auth.name is required"),
    ("request.pagination", {"type": "offset"}, "pagination.offset_param is required"),
    ("retry.max_attempts", 10, "max_attempts"),
    ("response.format", "xml", "columnar or records"),
    ("response.contract.temperature_2m.type", "float", "unknown type"),
    ("silver.keys", ["location_id", "nope"], "not a silver column"),
    ("silver.columns.1.source", "temp", "not in response.contract"),
    ("silver.columns.1.type", "decimal", "unknown type"),
    ("silver.columns.0.format", None, "format is required"),
    ("silver.columns.2.min", {"value": 200, "reason": "X"}, "min > max"),
    ("gold.sql_file", "sql/gold/missing.sql", "not found"),
    ("gold.keys", ["location_id", "nope"], "must be a silver column"),
    ("gold.params", {"rainy_mm": 0.1}, ":heat_c is not defined"),
    ("window.forecast_days.value", 9, "outside"),
    ("fault_injection.default_target", "atlantis", "not an entity"),
    ("fault_injection.scenarios.bad_request.param", "lat", "not a request param"),
    ("target.bronze_table", "bad name", "simple identifier"),
])
def test_invalid_configs_rejected(weather_raw, path, value, message):
    with pytest.raises(ConfigError, match=message):
        build(mutate(weather_raw, path, value))


def test_unknown_source_lists_available():
    with pytest.raises(ConfigError, match="available"):
        load_source(ROOT, "no_such_source")


def test_window_resolution(weather, usgs):
    assert resolve_window(weather, {"forecast_days": 5})["forecast_days"] == 5
    fixed = resolve_window(weather, {"forecast_days": None, "start_date": "2026-10-07", "end_date": "2026-10-09"})
    assert fixed == {"forecast_days": None, "start_date": "2026-10-07", "end_date": "2026-10-09"}
    rel = resolve_window(usgs, {}, today=date(2026, 10, 9))
    assert rel["starttime"] == "2026-10-02" and rel["endtime"] == "2026-10-09"
    with pytest.raises(ConfigError, match="not defined"):
        resolve_window(weather, {"days": 3})
    with pytest.raises(ConfigError, match="valid date"):
        resolve_window(weather, {"start_date": "07/10/2026"})
