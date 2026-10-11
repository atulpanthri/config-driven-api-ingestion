"""Offline structural checks of the generated AI/BI dashboards (scripts/build_dashboards.py).
The live check (`--validate`) also runs every dataset on the warehouse and verifies column names."""
import importlib.util
import json

import pytest

from conftest import ROOT

spec = importlib.util.spec_from_file_location("build_dashboards", ROOT / "scripts" / "build_dashboards.py")
build_dashboards = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build_dashboards)
DASHBOARDS = [b() for b in build_dashboards.BUILDERS]


@pytest.mark.parametrize("dash", DASHBOARDS, ids=lambda d: d.key)
def test_dashboard_structure_is_valid(dash):
    assert dash.problems() == []


@pytest.mark.parametrize("dash", DASHBOARDS, ids=lambda d: d.key)
def test_committed_json_matches_generator(dash):
    committed = json.loads((ROOT / "dashboards" / f"{dash.key}.lvdash.json").read_text(encoding="utf-8"))
    assert committed == {"datasets": dash.datasets, "pages": dash.pages}, \
        "dashboards/*.lvdash.json is stale: run python scripts/build_dashboards.py --validate"


def test_validator_rejects_nested_expressions_and_missing_fields():
    d = build_dashboards.Dashboard("t")
    ds = d.dataset("ds", "ds", "SELECT 1 AS a")
    d.page("p")
    d.counter(ds, "ROUND(SUM(`a`), 1)", "nested", (0, 0, 2, 2))
    d.counter(ds, "SUM(`b`)", "missing column", (2, 0, 2, 2))
    d.counter(ds, "SUM(`a`)", "overlap", (3, 0, 2, 2))
    issues = "\n".join(d.problems({"ds": {"a"}}))
    assert "unsupported expression" in issues and "column 'b' not in dataset" in issues and "overlaps" in issues
