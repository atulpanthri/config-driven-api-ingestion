# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the source's data schema (bronze, silver, gold, landing volume) and the shared ops schema
# MAGIC (run/entity/schema audits, quarantine), then applies governance metadata from the YAML
# MAGIC (Unity Catalog tags and column comments). Idempotent: only differences are changed.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

common_widgets()

# COMMAND ----------

from ingest_framework.persistence import apply_governance, create_all, create_landing_volume

cfg = get_config()
create_all(spark, cfg)
create_landing_volume(spark, cfg)
print("governance changes applied:", apply_governance(spark, cfg))  # UC tags + column comments from YAML
for layer, schema in cfg.schemas.items():
    display(spark.sql(f"SHOW TABLES IN {cfg.catalog}.{schema}"))
display(spark.sql(f"SHOW VOLUMES IN {cfg.catalog}.{cfg.schema('raw')}"))
