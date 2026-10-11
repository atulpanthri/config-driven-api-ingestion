# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Run pipeline (any configured source)
# MAGIC Extract (API; FILE/DB planned) → landing volume + `met_raw` (append) → contract check / extract / DQ →
# MAGIC `met_quarantine` + `met_silver` (MERGE) → `met_gold` (MERGE), with `run_log`, `run_log_dtl` and
# MAGIC `schema_change_log` in `met_audit`.
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `source` | a file in `sources/` (e.g. `open_meteo_weather`, `usgs_earthquakes`) |
# MAGIC | `schema_suffix` | appended to every layer schema, e.g. `_edge` → `met_raw_edge`, `met_audit_edge` (demo isolation) |
# MAGIC | `source_mode` | `live` = real API, `fixture` = saved real responses in the source's `fixtures_dir` |
# MAGIC | `test_mode` | `none` or a fault (built-in or from the source YAML), applied to `fault_entity_id` only |
# MAGIC | `replay_run_id` | reprocess an earlier run's bronze rows instead of calling the API |
# MAGIC | `window` | optional JSON overrides of the source window, e.g. `{"forecast_days": 5}` |

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

common_widgets()
dbutils.widgets.dropdown("source_mode", "live", ["live", "fixture"])
dbutils.widgets.text("test_mode", "none")
dbutils.widgets.text("fault_entity_id", "")   # empty = fault_injection.default_target
dbutils.widgets.text("replay_run_id", "")
dbutils.widgets.text("window", "")

# COMMAND ----------

from ingest_framework.fault_injection import available_faults
from ingest_framework.persistence import create_all, create_landing_volume
from ingest_framework.pipeline import run_pipeline

cfg = get_config()
print(f"source={cfg.source_name} extraction={cfg.extraction_type} schemas={cfg.schemas}")
print(f"available test modes: {available_faults(cfg)}")
create_all(spark, cfg)              # no-op when tables exist; lets this notebook run standalone
create_landing_volume(spark, cfg)

summary = run_pipeline(
    spark,
    cfg,
    source_mode=widget("source_mode", "live"),
    test_mode=widget("test_mode", "none"),
    fault_entity_id=widget("fault_entity_id") or None,
    window_overrides=window_overrides(),
    replay_run_id=widget("replay_run_id") or None,
    secret_resolver=secret_resolver,
)
print(json.dumps(summary, indent=2, default=str))

# COMMAND ----------

run_id = summary["run_id"]
display(spark.sql(f"SELECT * FROM {cfg.audit_table('run_log')} WHERE run_id = '{run_id}'"))
display(spark.sql(f"""SELECT entity_name, extraction_type, status, http_status, page_count, attempt_count,
                             records_received, records_valid, records_rejected, schema_events, error_type, error_message
                      FROM {cfg.audit_table('run_log_dtl')} WHERE run_id = '{run_id}' ORDER BY entity_name"""))
display(spark.sql(f"""SELECT entity_id, request_page, http_status, raw_file_path, length(raw_response) AS body_chars
                      FROM {cfg.bronze_fqn} WHERE run_id = '{run_id}' ORDER BY entity_id, request_page"""))

# COMMAND ----------

# Only a fully FAILED run fails the job; PARTIAL_SUCCESS is recorded in the audit and on the dashboard.
if summary["failed"]:
    raise RuntimeError(f"Pipeline run {run_id} FAILED: {summary['error_message']}")
