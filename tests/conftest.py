import copy
import json
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingest_framework.fault_injection import build_session, normalize_test_mode, resolve_target  # noqa: E402
from ingest_framework.fixture_source import FixtureSession  # noqa: E402
from ingest_framework.processing import ingest_entities, process_bronze  # noqa: E402
from ingest_framework.source_config import build_source, load_framework, load_source, resolve_window  # noqa: E402

RUN = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def weather():
    return load_source(ROOT, "open_meteo_weather")


@pytest.fixture
def usgs():
    return load_source(ROOT, "usgs_earthquakes")


@pytest.fixture
def weather_raw() -> dict:
    return yaml.safe_load((ROOT / "sources" / "open_meteo_weather.yml").read_text(encoding="utf-8"))


def build(raw: dict):
    return build_source(copy.deepcopy(raw), ROOT, load_framework(ROOT))


@pytest.fixture
def tokyo_body() -> str:
    return (ROOT / "tests" / "fixtures" / "open_meteo" / "tokyo.json").read_text(encoding="utf-8")


@pytest.fixture
def tokyo_payload(tokyo_body) -> dict:
    return json.loads(tokyo_body)


def run_fixture(cfg, test_mode=None, target_id=None, session=None, window=None):
    """Ingest + process every entity of `cfg` from fixtures (no Spark). Returns (bronze, outcomes)."""
    mode = normalize_test_mode(test_mode, cfg)
    target = resolve_target(cfg, target_id) if mode else None
    if session is None:
        session = build_session(FixtureSession(cfg), cfg, mode, target)
    win = resolve_window(cfg, window or {})
    bronze, outcomes = ingest_entities(cfg, session, RUN, win, source_mode="fixture", test_mode=mode,
                                       fault_target=target, sleep=lambda s: None)
    process_bronze(bronze, outcomes, cfg)
    return bronze, outcomes
