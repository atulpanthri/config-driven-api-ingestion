# Databricks notebook source
# MAGIC %md
# MAGIC Shared setup, included with `%run ./_bootstrap`: puts `src/` on the import path, makes sure
# MAGIC PyYAML is available, forces UTC for Spark timestamps, and defines helpers that read widgets.

# COMMAND ----------

import json
import os
import subprocess
import sys

try:
    import yaml  # noqa: F401
except ImportError:  # not expected on serverless, but keep the notebook self-sufficient
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "pyyaml"])


def _find_repo_root() -> str:
    """Repo root = parent of notebooks/. cwd is the notebook folder for workspace files;
    fall back to the notebook path from the context if it is not."""
    candidates = [os.path.abspath(os.path.join(os.getcwd(), ".."))]
    try:
        nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        candidates.append(os.path.dirname(os.path.dirname("/Workspace" + nb_path)))
    except Exception:
        pass
    for root in candidates:
        if os.path.exists(os.path.join(root, "framework.yml")):
            return root
    raise RuntimeError(f"Cannot locate repo root (tried {candidates})")


REPO_ROOT = _find_repo_root()
if os.path.join(REPO_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

# Silver keys and daily grouping are UTC; make Spark display/convert timestamps in UTC too.
spark.conf.set("spark.sql.session.timeZone", "UTC")

from ingest_framework.source_config import list_sources, load_source  # noqa: E402


def widget(name: str, default: str = "") -> str:
    try:
        return dbutils.widgets.get(name).strip()
    except Exception:
        return default


def get_config():
    """Source config + target overrides from widgets/job parameters (validated before any API call)."""
    return load_source(REPO_ROOT, widget("source", "open_meteo_weather"),
                       catalog=widget("catalog") or None,
                       schema_suffix=widget("schema_suffix") or None)


def window_overrides() -> dict:
    raw = widget("window")
    return json.loads(raw) if raw else {}


def secret_resolver(scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)


def common_widgets():
    dbutils.widgets.dropdown("source", "open_meteo_weather", list_sources(REPO_ROOT))
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema_suffix", "")  # e.g. _edge -> met_raw_edge, met_silver_edge, ... (demo isolation)


print(f"REPO_ROOT={REPO_ROOT}; sources={list_sources(REPO_ROOT)}")
