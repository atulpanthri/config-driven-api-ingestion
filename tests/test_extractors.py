from dataclasses import replace

import pytest

from conftest import RUN, run_fixture
from ingest_framework.extractors import ApiExtractor, DbExtractor, FileExtractor, RawUnit, get_extractor
from ingest_framework.fixture_source import FixtureSession
from ingest_framework.processing import run_log_dtl_rows
from ingest_framework.source_config import resolve_window


def test_registry_returns_api_extractor(weather):
    assert isinstance(get_extractor(weather, FixtureSession(weather)), ApiExtractor)


@pytest.mark.parametrize("kind, cls", [("file", FileExtractor), ("db", DbExtractor)])
def test_planned_extractors_fail_clearly(weather, kind, cls):
    cfg = replace(weather, extraction_type=kind)  # bypasses validation on purpose
    extractor = get_extractor(cfg)
    assert isinstance(extractor, cls)
    with pytest.raises(NotImplementedError, match=f"'{kind}' is not implemented yet"):
        extractor.extract(cfg.entities[0], {})


def test_unknown_extractor(weather):
    with pytest.raises(ValueError, match="unknown extraction.type"):
        get_extractor(replace(weather, extraction_type="ftp"))


def test_api_extractor_returns_raw_units(weather):
    units, error = get_extractor(weather, FixtureSession(weather)).extract(
        weather.entity("tokyo"), resolve_window(weather, {}), sleep=lambda s: None)
    assert error is None and len(units) == 1 and isinstance(units[0], RawUnit)
    u = units[0]
    assert u.ok and u.status_code == 200 and u.page == 1 and "latitude=35.6762" in u.source_uri


def test_bronze_records_are_generic(weather):
    bronze, _ = run_fixture(weather)
    assert all(r["extraction_type"] == "API" and r["source_uri"].startswith("https://") for r in bronze)


def test_run_log_dtl_rows(usgs):
    _, outcomes = run_fixture(usgs, "bad_request")
    rows = {r["entity_name"]: r for r in run_log_dtl_rows(outcomes, usgs, RUN, {"window": {"x": 1}})}
    assert len(rows) == 5 and all(r["extraction_type"] == "API" for r in rows.values())
    japan = rows["japan"]
    assert japan["run_dtl_id"] == f"{RUN}-japan" and japan["status"] == "FAILED" and japan["http_status"] == 400
    assert '"minlatitude": 999' in japan["parameters"] and '"window": {"x": 1}' in japan["parameters"]
    assert rows["alaska"]["page_count"] == 7 and rows["alaska"]["records_valid"] == 65
