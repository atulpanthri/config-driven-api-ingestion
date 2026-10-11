import pytest

from conftest import ROOT, build
from ingest_framework import tables
from ingest_framework.persistence import _sql_string
from ingest_framework.source_config import ConfigError, list_sources, load_source


def test_sql_string_escaping():
    assert _sql_string("plain") == "'plain'"
    assert _sql_string("it's") == r"'it\'s'"
    assert _sql_string(r"a\b") == r"'a\\b'"


@pytest.mark.parametrize("name", list_sources(ROOT))
def test_every_source_declares_governance(name):
    cfg = load_source(ROOT, name)
    assert cfg.data_owner and cfg.classification in {"public", "internal", "confidential", "restricted"}
    docs = cfg.column_descriptions()
    for key in cfg.silver_keys:
        assert key in docs, f"{name}: merge key {key} needs a description"
    assert cfg.gold_descriptions, f"{name}: describe gold columns"


def test_invalid_classification_rejected(weather_raw):
    weather_raw["governance"]["classification"] = "top-secret"
    with pytest.raises(ConfigError, match="classification"):
        build(weather_raw)


def test_standard_docs_cover_bronze_columns():
    missing = [n for n, _, _ in tables.BRONZE_COLUMNS if n not in tables.STANDARD_COLUMN_DOCS]
    assert not missing


def test_steward_and_retention_parsed(weather):
    assert weather.data_steward == "data-engineering"
    assert weather.retention_days == {"raw": 90, "silver": 730, "gold": 730}


def test_shared_tables_have_framework_governance(weather):
    assert weather.shared_governance["data_owner"] == "data-engineering"
    assert weather.shared_governance["classification"] == "internal"


def test_steward_defaults_to_owner(weather_raw):
    del weather_raw["governance"]["data_steward"]
    assert build(weather_raw).data_steward == weather_raw["governance"]["data_owner"]


@pytest.mark.parametrize("days, message", [({"bronze": 10}, "layer must be raw, silver or gold"),
                                           ({"raw": 0}, ">= 1 day")])
def test_invalid_retention_rejected(weather_raw, days, message):
    weather_raw["governance"]["retention_days"] = days
    with pytest.raises(ConfigError, match=message):
        build(weather_raw)
