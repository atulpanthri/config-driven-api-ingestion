"""Onboarding contract: every sources/*.yml must validate and run end-to-end on its fixtures.
Adding a new source with fixtures automatically adds it to this suite."""
import json
from datetime import date, datetime

import pytest

from conftest import ROOT, RUN, run_fixture
from ingest_framework.persistence import landing_path, write_landing_files
from ingest_framework.processing import SUCCESS, PARTIAL_SUCCESS, overall_status, quarantine_rows, silver_source_rows, utcnow
from ingest_framework.source_config import list_sources, load_source


@pytest.mark.parametrize("name", list_sources(ROOT))
def test_source_runs_end_to_end_on_fixtures(name):
    cfg = load_source(ROOT, name)
    assert cfg.fixtures_dir.exists(), f"{name}: add fixtures under {cfg.fixtures_dir}"
    bronze, outcomes = run_fixture(cfg)
    assert overall_status(outcomes) == SUCCESS, {k: (o.status, o.error_type, o.error_message) for k, o in outcomes.items()}
    rows = silver_source_rows(outcomes, cfg, RUN, utcnow())
    keys = {tuple(r[k] for k in cfg.silver_keys) for r in rows}
    assert len(keys) == len(rows), "silver source rows must be unique on silver.keys"
    for r in rows:
        assert all(r[k] is not None for k in cfg.silver_keys)
        assert all(r[k] is not None for k in cfg.gold_keys)
    assert not quarantine_rows(outcomes, cfg, RUN, utcnow()), "clean fixtures should not quarantine"


PY_TYPES = {"string": str, "double": float, "int": int, "bigint": int, "boolean": bool,
            "timestamp": datetime, "date": date}


@pytest.mark.parametrize("name", list_sources(ROOT))
def test_silver_values_match_declared_types(name):
    """Spark rejects e.g. an int in a DOUBLE column; catch type drift here without Spark."""
    cfg = load_source(ROOT, name)
    _, outcomes = run_fixture(cfg)
    types = cfg.silver_column_types()
    for r in silver_source_rows(outcomes, cfg, RUN, utcnow()):
        for col, t in types.items():
            value = r[col]
            assert value is None or type(value) is PY_TYPES[t] or (t == "date" and type(value) is date), \
                f"{name}.{col}: {value!r} is {type(value).__name__}, declared {t}"


@pytest.mark.parametrize("name", list_sources(ROOT))
def test_scenarios_only_affect_target(name):
    cfg = load_source(ROOT, name)
    for scenario in cfg.fault_scenarios:
        _, outcomes = run_fixture(cfg, scenario)
        others = [o for eid, o in outcomes.items() if eid != (cfg.fault_default_target or cfg.entities[0].id)]
        assert all(o.status == SUCCESS for o in others), f"{name}/{scenario} leaked to other entities"


def test_usgs_pagination_from_fixtures(usgs):
    bronze, outcomes = run_fixture(usgs)
    pages = {eid: o.pages for eid, o in outcomes.items()}
    assert pages == {"japan": 2, "california": 2, "alaska": 7, "indonesia": 4, "chile": 1}
    assert len(bronze) == sum(pages.values())
    assert {r["request_page"] for r in bronze if r["entity_id"] == "alaska"} == set(range(1, 8))
    assert len(outcomes["alaska"].valid_rows) == 65
    row = outcomes["alaska"].valid_rows[0]["typed"]
    assert row["event_time_utc"].tzinfo is not None and row["event_date_utc"] == row["event_time_utc"].date()


def test_usgs_empty_region_allowed(usgs):
    """allow_empty: a page with zero records is a valid (quiet) result."""
    class Empty:
        def request(self, method, url, params=None, headers=None, timeout=None):
            from ingest_framework.fault_injection import SimpleResponse
            return SimpleResponse(200, json.dumps({"type": "FeatureCollection", "features": []}))

    _, outcomes = run_fixture(usgs, session=Empty())
    assert all(o.status == SUCCESS and o.records_received == 0 for o in outcomes.values())


@pytest.mark.parametrize("mode, reason, status", [
    ("invalid_magnitude", "MAGNITUDE_OUT_OF_RANGE", SUCCESS),
    ("bad_magnitude_type", "INVALID_MAGNITUDE_TYPE", SUCCESS),
    ("missing_field", "MISSING_FIELD", "FAILED"),
    ("duplicate_record", "DUPLICATE_EVENT", SUCCESS),
])
def test_usgs_scenarios(usgs, mode, reason, status):
    _, outcomes = run_fixture(usgs, mode)
    assert outcomes["japan"].status == status
    reasons = {x for q in quarantine_rows(outcomes, usgs, RUN, utcnow()) for x in q["dq_errors"]}
    assert reason in reasons
    assert overall_status(outcomes) == (SUCCESS if status == SUCCESS else PARTIAL_SUCCESS)


def test_usgs_bad_request_fails_target_only(usgs):
    bronze, outcomes = run_fixture(usgs, "bad_request")
    assert outcomes["japan"].error_type == "HTTP_4XX" and outcomes["japan"].attempts == 1
    assert sum(o.status == SUCCESS for o in outcomes.values()) == 4


def test_landing_files_written_once(tmp_path, weather):
    bronze, _ = run_fixture(weather)
    n = write_landing_files(weather, RUN, bronze, landing_root=str(tmp_path))
    assert n == 5
    rec = bronze[0]
    assert rec["raw_file_path"].endswith(f"run_id={RUN}/{rec['entity_id']}.json")
    assert "source=open_meteo_weather/ingest_date=" in rec["raw_file_path"]
    with open(rec["raw_file_path"], encoding="utf-8") as fh:
        assert fh.read() == rec["raw_response"]  # byte-for-byte as received
    with pytest.raises(FileExistsError):
        write_landing_files(weather, RUN, bronze, landing_root=str(tmp_path))


def test_landing_path_pages(usgs):
    from datetime import date
    p1 = landing_path("/Volumes/c/s/v", usgs, RUN, date(2026, 10, 9), "alaska", 1)
    p3 = landing_path("/Volumes/c/s/v", usgs, RUN, date(2026, 10, 9), "alaska", 3)
    assert p1 == f"/Volumes/c/s/v/source=usgs_earthquakes/ingest_date=2026-10-09/run_id={RUN}/alaska.json"
    assert p3.endswith("alaska_p3.json")
