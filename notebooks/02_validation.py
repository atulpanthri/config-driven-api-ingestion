# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Validation and health checks (any configured source)
# MAGIC Checks generated from the source YAML that must return **zero** violating rows; the notebook
# MAGIC (and the job task) fails if any check is violated. Then shows the audit tables for this source.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

common_widgets()

# COMMAND ----------

cfg = get_config()
silver, gold, src = cfg.silver_fqn, cfg.gold_fqn, cfg.source_name
skeys, gkeys = ", ".join(cfg.silver_keys), ", ".join(cfg.gold_keys)

checks = {
    f"silver duplicate ({skeys})":
        f"SELECT {skeys}, COUNT(*) AS cnt FROM {silver} GROUP BY {skeys} HAVING COUNT(*) > 1",
    f"gold duplicate ({gkeys})":
        f"SELECT {gkeys}, COUNT(*) AS cnt FROM {gold} GROUP BY {gkeys} HAVING COUNT(*) > 1",
    "gold rows without created_run_id / created_at":
        f"SELECT {gkeys} FROM {gold} WHERE created_run_id IS NULL OR created_at IS NULL",
    "silver keys missing from gold":
        f"SELECT DISTINCT {', '.join('s.' + k for k in cfg.gold_keys)} FROM {silver} s LEFT ANTI JOIN {gold} g ON "
        + " AND ".join(f"s.{k} = g.{k}" for k in cfg.gold_keys),
    "runs whose detail rows do not match job_success + job_failed": f"""
        SELECT r.run_id, r.job_success + r.job_failed AS entities, COUNT(d.run_dtl_id) AS detail_rows
        FROM {cfg.audit_table('run_log')} r
        LEFT JOIN {cfg.audit_table('run_log_dtl')} d ON r.run_id = d.run_id
        WHERE r.source_name = '{src}' AND r.status <> 'RUNNING'
        GROUP BY r.run_id, r.job_success, r.job_failed HAVING COUNT(d.run_dtl_id) <> r.job_success + r.job_failed""",
}
for c in cfg.columns:
    if c.required:
        checks[f"silver NULL {c.name}"] = f"SELECT * FROM {silver} WHERE {c.name} IS NULL"
    if c.min:
        checks[f"silver {c.name} < {c.min.value}"] = f"SELECT * FROM {silver} WHERE {c.name} < {c.min.value}"
    if c.max:
        checks[f"silver {c.name} > {c.max.value}"] = f"SELECT * FROM {silver} WHERE {c.name} > {c.max.value}"

failures = []
for name, sql in checks.items():
    n = spark.sql(sql).count()
    print(f"{'PASS' if n == 0 else 'FAIL'}  {name}: {n} row(s)")
    if n:
        failures.append(name)
        display(spark.sql(sql).limit(20))

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'bronze' AS layer, COUNT(*) AS rows FROM {cfg.bronze_fqn}
    UNION ALL SELECT 'silver', COUNT(*) FROM {silver}
    UNION ALL SELECT 'gold', COUNT(*) FROM {gold}
    UNION ALL SELECT 'quarantine', COUNT(*) FROM {cfg.quarantine_table('data_quarantine')} WHERE source_name = '{src}'"""))
display(spark.sql(f"SELECT * FROM {cfg.audit_table('run_log')} WHERE source_name = '{src}' ORDER BY start_time DESC LIMIT 50"))
display(spark.sql(f"""SELECT d.* FROM {cfg.audit_table('run_log_dtl')} d JOIN {cfg.audit_table('run_log')} r USING (run_id)
                      WHERE r.source_name = '{src}' ORDER BY d.start_time DESC LIMIT 50"""))
display(spark.sql(f"SELECT * FROM {cfg.quarantine_table('data_quarantine')} WHERE source_name = '{src}' ORDER BY quarantined_at DESC LIMIT 50"))
display(spark.sql(f"SELECT * FROM {cfg.audit_table('schema_change_log')} WHERE source_name = '{src}' ORDER BY detected_at DESC LIMIT 50"))

# COMMAND ----------

if failures:
    raise AssertionError(f"Validation failed: {failures}")
print("All validation checks passed.")
