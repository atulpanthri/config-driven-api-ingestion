# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Same-window rerun proof (any configured source)
# MAGIC Runs the pipeline twice for the same window and compares counts. Expected: bronze grows by the
# MAGIC second run's pages; silver and gold key counts stay the same; no duplicate keys.
# MAGIC Use `source_mode=fixture` for an exactly identical source; with `live`, changed values show up as updates.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

common_widgets()
dbutils.widgets.dropdown("source_mode", "fixture", ["live", "fixture"])

# COMMAND ----------

from ingest_framework.persistence import create_all, create_landing_volume
from ingest_framework.pipeline import run_pipeline

cfg = get_config()
create_all(spark, cfg)
create_landing_volume(spark, cfg)


def snapshot():
    silver, gold = spark.table(cfg.silver_fqn), spark.table(cfg.gold_fqn)
    return {"bronze_rows": spark.table(cfg.bronze_fqn).count(),
            "silver_rows": silver.count(), "silver_keys": silver.select(*cfg.silver_keys).distinct().count(),
            "gold_rows": gold.count(), "gold_keys": gold.select(*cfg.gold_keys).distinct().count()}


runs = []
for label in ("first run", "same-window rerun"):
    s = run_pipeline(spark, cfg, source_mode=widget("source_mode", "fixture"), secret_resolver=secret_resolver)
    runs.append((label, s, snapshot()))
    print(f"{label:18} status={s['status']:16} inserted={s['silver_rows_inserted']:4} "
          f"updated={s['silver_rows_updated']:4} | {runs[-1][2]}")

(_, _, first), (_, second_run, second) = runs
assert second["bronze_rows"] == first["bronze_rows"] + second_run["bronze_rows"], "bronze must grow by the rerun's pages"
assert second["silver_keys"] == first["silver_keys"] == first["silver_rows"] == second["silver_rows"], "silver must not duplicate"
assert second["gold_keys"] == first["gold_keys"] == first["gold_rows"] == second["gold_rows"], "gold must not duplicate"
print("Rerun proof passed: bronze appended, silver and gold unchanged in key count.")
