"""Delta reads/writes and landing files across the layer schemas (met_raw, met_silver, met_gold,
met_audit, met_quarantine). Write semantics:

  landing files (UC Volume in met_raw)                              -> write-once raw data per unit/page
  bronze, data_quarantine, schema_change_log, run_log_dtl         -> append only
  run_log    -> append RUNNING row at start, MERGE on run_id to finalize
  silver     -> dedup source rows, MERGE on silver.keys (update only changed values, never
                with an older ingestion)
  gold       -> run the source's gold SQL for the gold keys touched in this run, MERGE on gold.keys

MERGE row counts come from the MERGE result (num_inserted_rows / num_updated_rows), falling back
to Delta history; submitted rows are never reported as inserted rows.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any

from . import tables
from .source_config import SourceConfig


def _view_name(prefix: str, run_id: str) -> str:
    return f"{prefix}_{run_id.replace('-', '')}"


def _safe_id(value: str) -> str:
    """run IDs are UUIDs; reject anything else before it goes into SQL text."""
    if not value or not all(ch.isalnum() or ch == "-" for ch in value):
        raise ValueError(f"invalid run id {value!r}")
    return value


def as_utc(value: datetime | None) -> datetime | None:
    """Spark returns naive datetimes; the session time zone is UTC, so mark them as such."""
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


# --------------------------------------------------------------------------- setup
SCHEMA_COMMENTS = {
    "raw": "Bronze: raw data exactly as received (append-only) and the raw landing volume",
    "silver": "Silver: typed, validated rows (MERGE on business keys)",
    "gold": "Gold: business aggregates (MERGE of affected keys)",
    "audit": "Audit: run_log, run_log_dtl and schema_change_log for all sources",
    "quarantine": "Quarantine: rejected rows and payloads with reasons",
}


def create_all(spark, cfg: SourceConfig) -> None:
    for layer, schema in cfg.schemas.items():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema} COMMENT {_sql_string(SCHEMA_COMMENTS[layer])}")
    for table in (*tables.SHARED_TABLES, tables.BRONZE, tables.SILVER):
        create_table(spark, cfg, table)
    create_gold_table(spark, cfg)


def create_table(spark, cfg: SourceConfig, table: str, name: str | None = None) -> None:
    """CREATE TABLE IF NOT EXISTS from the contract in tables.py (with partitions / generated columns).
    Databricks accepts generated columns in SQL DDL; open-source Delta 3.x (local tests) only through
    the DeltaTable builder, so that is the fallback."""
    try:
        spark.sql(tables.table_ddl(cfg, table, name))
    except Exception as exc:
        if "generated columns" not in str(exc).lower():
            raise
        from delta.tables import DeltaTable

        target = name or tables.fqn(cfg, table)
        if target.startswith("spark_catalog."):  # the OSS builder only takes schema.table
            target = target.split(".", 1)[1]
        builder = DeltaTable.createIfNotExists(spark).tableName(target)
        for col, sql_type, not_null in tables.columns_for(cfg, table):
            builder = builder.addColumn(col, tables.spark_type(sql_type), nullable=not not_null)
        for col, sql_type, expr in tables.GENERATED.get(table, []):
            builder = builder.addColumn(col, tables.spark_type(sql_type), generatedAlwaysAs=expr)
        partitions = tables.partition_columns(cfg, table)
        if partitions:
            builder = builder.partitionedBy(*partitions)
        builder.execute()


def _sql_string(value: str) -> str:
    """Spark SQL string literal: escape backslashes, then single quotes."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def apply_governance(spark, cfg: SourceConfig) -> dict[str, int]:
    """Apply Unity Catalog tags and column comments from config. Idempotent: reads the current
    tags/comments from information_schema and only changes what differs."""
    # (layer schema, table) -> tags / column comments
    data_tables = {("raw", cfg.bronze_table): "bronze", ("silver", cfg.silver_table): "silver",
                   ("gold", cfg.gold_table): "gold"}
    wanted_tags = {}
    for key, layer in data_tables.items():
        tags = {"layer": layer, "source": cfg.source_name, "classification": cfg.classification,
                "data_owner": cfg.data_owner, "data_steward": cfg.data_steward,
                "extraction_type": cfg.extraction_type}
        if key[0] in cfg.retention_days:
            tags["retention_days"] = str(cfg.retention_days[key[0]])
        wanted_tags[key] = tags
    # shared audit / quarantine tables: owned by the framework, tagged once from framework.yml governance
    shared = cfg.shared_governance
    for name, (layer, _, _) in tables.SHARED_TABLES.items():
        if shared.get("data_owner"):
            wanted_tags[(layer, name)] = {
                "layer": layer, "source": "all", "classification": shared.get("classification", "internal"),
                "data_owner": shared["data_owner"], "data_steward": shared.get("data_steward", shared["data_owner"])}
    docs = {**tables.STANDARD_COLUMN_DOCS}
    comments_by_schema: dict[str, dict[str, dict[str, str]]] = {
        cfg.schema("raw"): {cfg.bronze_table: docs},
        cfg.schema("silver"): {cfg.silver_table: {**docs, **cfg.column_descriptions()}},
        cfg.schema("gold"): {cfg.gold_table: {**docs, **cfg.gold_descriptions}},
    }
    for name, (layer, _, _) in tables.SHARED_TABLES.items():
        comments_by_schema.setdefault(cfg.schema(layer), {})[name] = docs

    changed = {"tags": 0, "comments": 0}
    for (layer, table), tags in wanted_tags.items():
        current = {(r[0], r[1]) for r in spark.sql(
            f"SELECT tag_name, tag_value FROM {cfg.catalog}.information_schema.table_tags "
            f"WHERE schema_name = {_sql_string(cfg.schema(layer))} AND table_name = {_sql_string(table)}").collect()}
        missing = {k: v for k, v in tags.items() if (k, v) not in current}
        if missing:
            pairs = ", ".join(f"{_sql_string(k)} = {_sql_string(v)}" for k, v in missing.items())
            spark.sql(f"ALTER TABLE {cfg.table_in(layer, table)} SET TAGS ({pairs})")
            changed["tags"] += len(missing)

    # table comments the DDL cannot provide (gold is created from the SQL output) and refreshed schema comments
    gold_comment = f"Gold aggregates of {cfg.source_name}; MERGE on ({', '.join(cfg.gold_keys)}). "                    f"Lineage: created_* set once, last_updated_* when an aggregate changes."
    current_comment = spark.sql(
        f"SELECT comment FROM {cfg.catalog}.information_schema.tables WHERE table_schema = "
        f"{_sql_string(cfg.schema('gold'))} AND table_name = {_sql_string(cfg.gold_table)}").collect()
    if current_comment and not current_comment[0][0]:
        spark.sql(f"COMMENT ON TABLE {cfg.gold_fqn} IS {_sql_string(gold_comment)}")
        changed["comments"] += 1
    current_schema = {r[0]: r[1] for r in spark.sql(
        f"SELECT schema_name, comment FROM {cfg.catalog}.information_schema.schemata").collect()}
    for layer, schema in cfg.schemas.items():
        if current_schema.get(schema) != SCHEMA_COMMENTS[layer]:
            spark.sql(f"COMMENT ON SCHEMA {cfg.catalog}.{schema} IS {_sql_string(SCHEMA_COMMENTS[layer])}")
            changed["comments"] += 1

    for schema, wanted in comments_by_schema.items():
        names = ", ".join(_sql_string(t) for t in wanted)
        current = {(r[0], r[1]): r[2] for r in spark.sql(
            f"SELECT table_name, column_name, comment FROM {cfg.catalog}.information_schema.columns "
            f"WHERE table_schema = {_sql_string(schema)} AND table_name IN ({names})").collect()}
        for table, comments in wanted.items():
            for (t, column), comment in current.items():
                if t == table and column in comments and comment != comments[column]:
                    spark.sql(f"ALTER TABLE {cfg.catalog}.{schema}.{table} ALTER COLUMN {column} "
                              f"COMMENT {_sql_string(comments[column])}")
                    changed["comments"] += 1
    return changed


def create_landing_volume(spark, cfg: SourceConfig) -> None:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema('raw')}.{cfg.landing_volume} "
              f"COMMENT 'Raw source data as files, partitioned by source/ingest_date/run_id'")


def to_dataframe(spark, cols: list[tables.Column], rows: list[dict[str, Any]]):
    names = [n for n, _, _ in cols]
    return spark.createDataFrame([tuple(r.get(n) for n in names) for r in rows], tables.spark_schema(cols))


def append_rows(spark, cfg: SourceConfig, table: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    df = to_dataframe(spark, tables.columns_for(cfg, table), rows)
    df.write.format("delta").mode("append").saveAsTable(tables.fqn(cfg, table))
    return len(rows)


def read_bronze_run(spark, cfg: SourceConfig, run_id: str) -> list[dict[str, Any]]:
    rows = spark.table(cfg.bronze_fqn).where(f"run_id = '{_safe_id(run_id)}'").collect()
    records = [r.asDict() for r in rows]
    for rec in records:
        rec["ingestion_timestamp"] = as_utc(rec["ingestion_timestamp"])
    return records


def _merge_counts(spark, result_rows, table_fqn: str) -> tuple[int, int]:
    if result_rows:
        first = result_rows[0].asDict()
        if "num_inserted_rows" in first and "num_updated_rows" in first:
            return int(first["num_inserted_rows"]), int(first["num_updated_rows"])
    history = spark.sql(f"DESCRIBE HISTORY {table_fqn} LIMIT 1").collect()[0].asDict()
    metrics = history.get("operationMetrics") or {}
    return int(metrics.get("numTargetRowsInserted", 0)), int(metrics.get("numTargetRowsUpdated", 0))


# --------------------------------------------------------------------------- landing files
def landing_path(root: str, cfg: SourceConfig, run_id: str, ingest_date: date, entity_id: str, page: int) -> str:
    name = f"{entity_id}.json" if page == 1 else f"{entity_id}_p{page}.json"
    return f"{root}/source={cfg.source_name}/ingest_date={ingest_date.isoformat()}/run_id={run_id}/{name}"


def write_landing_files(cfg: SourceConfig, run_id: str, bronze_records: list[dict[str, Any]],
                        landing_root: str | None = None) -> int:
    """Write each received body, unmodified, to the landing volume; set raw_file_path on the record.
    Files are write-once: an existing path (same run/entity/page) is never overwritten."""
    root = landing_root or cfg.landing_root
    written = 0
    for rec in bronze_records:
        path = landing_path(root, cfg, run_id, rec["ingestion_timestamp"].date(), rec["entity_id"], rec["request_page"])
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "x", encoding="utf-8", newline="") as fh:
            fh.write(rec["raw_response"])
        rec["raw_file_path"] = path
        written += 1
    return written


# --------------------------------------------------------------------------- run log
def start_run_log(spark, cfg: SourceConfig, row: dict[str, Any]) -> None:
    append_rows(spark, cfg, tables.RUN_LOG, [row])


def finalize_run_log(spark, cfg: SourceConfig, row: dict[str, Any]) -> None:
    view = _view_name("run_log_src", row["run_id"])
    to_dataframe(spark, tables.columns_for(cfg, tables.RUN_LOG), [row]).createOrReplaceTempView(view)
    spark.sql(f"""
        MERGE INTO {tables.fqn(cfg, tables.RUN_LOG)} AS t
        USING {view} AS s
        ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).collect()


# --------------------------------------------------------------------------- silver
def merge_silver(spark, cfg: SourceConfig, run_id: str, rows: list[dict[str, Any]]) -> tuple[int, int, int]:
    """Returns (source_rows_after_dedup, inserted, updated)."""
    if not rows:
        return 0, 0, 0
    from pyspark.sql import Window, functions as F

    # One source row per key before MERGE; latest ingestion wins, run id breaks exact ties.
    w = Window.partitionBy(*cfg.silver_keys).orderBy(F.col("source_ingestion_timestamp").desc(),
                                                      F.col("created_run_id").desc())
    source = (to_dataframe(spark, tables.silver_columns(cfg), rows)
              .withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn"))
    source_count = source.count()
    view = _view_name("silver_src", run_id)
    source.createOrReplaceTempView(view)

    value_cols = [c for c in cfg.silver_column_types() if c not in cfg.silver_keys]
    on = " AND ".join(f"t.{k} = s.{k}" for k in cfg.silver_keys)
    unchanged = " AND ".join(f"t.{c} <=> s.{c}" for c in value_cols) or "TRUE"
    updates = ",\n            ".join(f"t.{c} = s.{c}" for c in
                                     (*value_cols, "source_ingestion_timestamp", "last_updated_run_id", "updated_at"))
    result = spark.sql(f"""
        MERGE INTO {cfg.silver_fqn} AS t
        USING {view} AS s
        ON {on}
        WHEN MATCHED AND NOT ({unchanged})
             AND (t.source_ingestion_timestamp IS NULL OR s.source_ingestion_timestamp >= t.source_ingestion_timestamp)
        THEN UPDATE SET
            {updates}
        WHEN NOT MATCHED THEN INSERT *
    """).collect()
    inserted, updated = _merge_counts(spark, result, cfg.silver_fqn)
    return source_count, inserted, updated


# --------------------------------------------------------------------------- gold
def _affected_keys_view(spark, cfg: SourceConfig, run_id: str, keys: list[tuple]) -> str:
    from pyspark.sql import types as T

    types = cfg.silver_column_types()
    schema = T.StructType([T.StructField(k, tables.spark_type(tables.SQL_TYPES[types[k]]), True)
                           for k in cfg.gold_keys])
    view = _view_name("gold_keys", run_id)
    spark.createDataFrame(keys, schema).createOrReplaceTempView(view)
    return view


def gold_select_sql(cfg: SourceConfig, keys_view: str) -> str:
    return cfg.gold_sql().replace("{silver}", cfg.silver_fqn).replace("{affected_keys}", keys_view)


def create_gold_table(spark, cfg: SourceConfig) -> None:
    """Create the gold table (if missing) with the schema produced by the source's gold SQL plus the
    lineage columns created_run_id/created_at (set once) and last_updated_run_id/updated_at (on change).
    An existing table without the created_* columns is upgraded in place and backfilled from silver."""
    if spark.catalog.tableExists(cfg.gold_fqn):
        _upgrade_gold_lineage(spark, cfg)
        return
    from pyspark.sql import functions as F

    keys_view = _affected_keys_view(spark, cfg, "setup", [])
    empty = (spark.sql(gold_select_sql(cfg, keys_view), args=cfg.gold_params)
             .withColumn("created_run_id", F.lit(None).cast("string"))
             .withColumn("created_at", F.lit(None).cast("timestamp"))
             .withColumn("last_updated_run_id", F.lit(None).cast("string"))
             .withColumn("updated_at", F.lit(None).cast("timestamp"))
             .limit(0))
    missing = [k for k in cfg.gold_keys if k not in empty.columns]
    if missing:
        raise ValueError(f"gold SQL output lacks gold.keys {missing}")
    empty.write.format("delta").mode("ignore").saveAsTable(cfg.gold_fqn)


def _upgrade_gold_lineage(spark, cfg: SourceConfig) -> None:
    """Add created_run_id/created_at to a gold table created before they existed, then backfill them:
    a gold key was created by the run that first loaded its silver rows."""
    columns = set(spark.table(cfg.gold_fqn).columns)
    if {"created_run_id", "created_at"} <= columns:
        return
    spark.sql(f"ALTER TABLE {cfg.gold_fqn} ADD COLUMNS (created_run_id STRING, created_at TIMESTAMP)")
    keys = ", ".join(cfg.gold_keys)
    on = " AND ".join(f"t.{k} = s.{k}" for k in cfg.gold_keys)
    spark.sql(f"""
        MERGE INTO {cfg.gold_fqn} AS t
        USING (SELECT {keys}, min_by(created_run_id, created_at) AS created_run_id, MIN(created_at) AS created_at
               FROM {cfg.silver_fqn} GROUP BY {keys}) AS s
        ON {on}
        WHEN MATCHED AND t.created_run_id IS NULL
        THEN UPDATE SET t.created_run_id = s.created_run_id, t.created_at = s.created_at
    """).collect()


def gold_merge_sql(cfg: SourceConfig, keys_view: str, gold_cols: list[str]) -> str:
    """MERGE for the affected gold keys. Inserts set all four lineage columns; updates (only when an
    aggregate changed) set the metrics plus last_updated_run_id/updated_at and never touch created_*."""
    metric_cols = [c for c in gold_cols if c not in cfg.gold_keys and c not in tables.GOLD_METADATA]
    on = " AND ".join(f"t.{k} = s.{k}" for k in cfg.gold_keys)
    unchanged = " AND ".join(f"t.{c} <=> s.{c}" for c in metric_cols) or "TRUE"
    updates = ",\n            ".join(f"t.{c} = s.{c}" for c in (*metric_cols, "last_updated_run_id", "updated_at"))
    return f"""
        MERGE INTO {cfg.gold_fqn} AS t
        USING (
            SELECT g.*, :run_id AS created_run_id, current_timestamp() AS created_at,
                   :run_id AS last_updated_run_id, current_timestamp() AS updated_at
            FROM ({gold_select_sql(cfg, keys_view)}) AS g
        ) AS s
        ON {on}
        WHEN MATCHED AND NOT ({unchanged}) THEN UPDATE SET
            {updates}
        WHEN NOT MATCHED THEN INSERT *
    """


def merge_gold(spark, cfg: SourceConfig, run_id: str, affected_keys: set[tuple]) -> tuple[int, int]:
    """Recompute only the gold keys touched by this run, from the full silver table, and MERGE.
    Entities that failed in this run keep their existing gold rows."""
    if not affected_keys:
        return 0, 0
    keys_view = _affected_keys_view(spark, cfg, run_id, sorted(affected_keys, key=lambda k: tuple(map(str, k))))
    sql = gold_merge_sql(cfg, keys_view, spark.table(cfg.gold_fqn).columns)
    result = spark.sql(sql, args={**cfg.gold_params, "run_id": run_id}).collect()
    return _merge_counts(spark, result, cfg.gold_fqn)
