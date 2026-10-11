"""Local Spark + Delta smoke test of the framework in fixture mode (no API calls), for every source.

Per source: first run, same-window rerun, one fault scenario, bronze replay. Exercises the real
persistence code (DDL, landing files, appends, silver/gold MERGE, audit finalize) on a local
metastore. A developer check, not a substitute for running in the Databricks workspace.

Requirements: Java 11/17, pyspark==3.5.x, delta-spark==3.2.x, pyyaml. On Windows use Python 3.10/3.11
(Spark 3.5 Python workers crashed with Python 3.12 in our environment) and unset SPARK_HOME:
    env -u SPARK_HOME PYSPARK_PYTHON=<python> <python> scripts/local_spark_smoke.py <work_dir>
"""
import os
import sys
import tempfile

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(REPO, "src"))

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from ingest_framework.persistence import create_all  # noqa: E402
from ingest_framework.pipeline import run_pipeline  # noqa: E402
from ingest_framework.source_config import list_sources, load_source  # noqa: E402

work = sys.argv[1] if len(sys.argv) > 1 else tempfile.mkdtemp(prefix="ingest_smoke_")
builder = (SparkSession.builder.master("local[2]").appName("ingest-smoke")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("spark.sql.warehouse.dir", os.path.join(work, "warehouse"))
           .config("spark.sql.session.timeZone", "UTC")
           .config("spark.ui.enabled", "false"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
landing = os.path.join(work, "landing")
FAULT = {"open_meteo_weather": "bad_request", "usgs_earthquakes": "invalid_magnitude"}

for name in list_sources(REPO):
    cfg = load_source(REPO, name, catalog="spark_catalog", schema_suffix="_smoke")
    create_all(spark, cfg)

    def counts():
        return {"bronze": spark.table(cfg.bronze_fqn).count(), "silver": spark.table(cfg.silver_fqn).count(),
                "gold": spark.table(cfg.gold_fqn).count()}

    results = []
    for label, mode in (("first run", None), ("same-window rerun", None), (FAULT[name], FAULT[name])):
        r = run_pipeline(spark, cfg, source_mode="fixture", test_mode=mode, landing_root=landing)
        results.append((r, counts()))
        print(f"== {name} | {label}: {r['status']} landing={r['landing_files']} bronze+={r['bronze_rows']} "
              f"valid={r['silver_valid_rows']} ins={r['silver_rows_inserted']} upd={r['silver_rows_updated']} "
              f"q={r['quarantine_rows']} gold ins={r['gold_rows_inserted']} upd={r['gold_rows_updated']} "
              f"| {results[-1][1]}")
    replay = run_pipeline(spark, cfg, replay_run_id=results[0][0]["run_id"])
    print(f"== {name} | replay: {replay['status']} ins={replay['silver_rows_inserted']} "
          f"upd={replay['silver_rows_updated']}")

    (r1, c1), (r2, c2), (r3, _) = results
    assert r1["status"] == r2["status"] == "SUCCESS", (r1["error_message"], r2["error_message"])
    assert c2["silver"] == c1["silver"] and c2["gold"] == c1["gold"], "rerun must not add silver/gold rows"
    assert c2["bronze"] == c1["bronze"] + r2["bronze_rows"]
    assert r2["silver_rows_inserted"] == 0 and r2["silver_rows_updated"] == 0
    assert r1["landing_files"] == r1["bronze_rows"]
    assert r3["status"] in ("SUCCESS", "PARTIAL_SUCCESS") and replay["silver_rows_inserted"] == 0
    dupes = spark.sql(f"SELECT {', '.join(cfg.silver_keys)} FROM {cfg.silver_fqn} "
                      "GROUP BY ALL HAVING COUNT(*) > 1").count()
    assert dupes == 0
    creators = spark.sql(f"SELECT DISTINCT created_run_id FROM {cfg.gold_fqn}").collect()
    assert [c.created_run_id for c in creators] == [r1["run_id"]], "gold created_run_id must stay the first run"
    assert spark.sql(f"SELECT COUNT(*) FROM {cfg.gold_fqn} WHERE created_at IS NULL").collect()[0][0] == 0
    print(f"   gold created_run_id kept from first run {r1['run_id'][:8]}")
    expected = {cfg.bronze_fqn: ["ingestion_date"], cfg.silver_fqn: list(cfg.silver_partition_by),
                cfg.audit_table("run_log_dtl"): ["start_date"]}
    for table, parts in expected.items():
        actual = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0].partitionColumns
        assert list(actual) == parts, f"{table}: partitions {actual}, expected {parts}"
    print(f"   partitions OK: {expected}")
    row = spark.sql(f"SELECT raw_file_path, raw_response FROM {cfg.bronze_fqn} LIMIT 1").collect()[0]
    with open(row.raw_file_path, encoding="utf-8", newline="") as fh:
        assert fh.read() == row.raw_response, "landing file must equal bronze raw_response"

spark.sql("SELECT source_name, status, job_success, job_failed, bronze_rows, error_message "
          "FROM spark_catalog.met_audit_smoke.run_log ORDER BY start_time").show(truncate=50)
spark.sql("SELECT entity_name, extraction_type, status, http_status, page_count, records_valid "
          "FROM spark_catalog.met_audit_smoke.run_log_dtl ORDER BY start_time").show(truncate=40)
spark.sql("SELECT source_name, entity_id, rejection_level, dq_errors "
          "FROM spark_catalog.met_quarantine_smoke.data_quarantine").show(truncate=60)
print("SMOKE TEST PASSED")
