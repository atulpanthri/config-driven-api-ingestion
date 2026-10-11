"""Table contracts for the layered model (schema names come from framework.yml):

  met_raw         <bronze_table>     generic raw table per source (append-only)   + landing volume
  met_silver      <silver_table>     generated from the source YAML (MERGE)
  met_gold        <gold_table>       schema of the source's gold SQL (MERGE of affected keys)
  met_audit       run_log            one row per run (all sources)
                  run_log_dtl        one row per entity per run, with extraction_type
                  schema_change_log  contract differences per payload
  met_quarantine  data_quarantine    rejected rows / payloads of all sources

One definition drives both DDL and the Spark schema, so they cannot drift. No mergeSchema.

Partitioning (Delta cannot partition on an expression, so date partitions use generated columns,
which Delta fills in on write; DataFrames never include them):
  met_raw.<bronze_table>    PARTITIONED BY ingestion_date = CAST(ingestion_timestamp AS DATE)
  met_audit.run_log_dtl     PARTITIONED BY start_date     = CAST(start_time AS DATE)
  met_silver.<silver_table> PARTITIONED BY silver.partition_by from the source YAML
The session time zone is UTC, so these are UTC dates.
"""
from __future__ import annotations

from .source_config import SourceConfig

# fixed (shared) tables: logical name -> (layer, comment, columns)
RUN_LOG = "run_log"
RUN_LOG_DTL = "run_log_dtl"
SCHEMA_CHANGE_LOG = "schema_change_log"
QUARANTINE = "data_quarantine"
BRONZE = "bronze"  # logical names; physical names come from target.* in the source YAML
SILVER = "silver"

SQL_TYPES = {"string": "STRING", "double": "DOUBLE", "int": "INT", "bigint": "BIGINT",
             "boolean": "BOOLEAN", "timestamp": "TIMESTAMP", "date": "DATE"}

Column = tuple[str, str, bool]  # (name, SQL type, not_null)

SHARED_TABLES: dict[str, tuple[str, str, list[Column]]] = {
    RUN_LOG: ("audit", "One row per pipeline run of any source; inserted as RUNNING, finalized by MERGE on run_id.", [
        ("run_id", "STRING", True), ("source_name", "STRING", True), ("start_time", "TIMESTAMP", True),
        ("status", "STRING", True), ("duration_seconds", "DOUBLE", False), ("test_mode", "STRING", False),
        ("source_mode", "STRING", False), ("job_success", "INT", False), ("job_failed", "INT", False),
        ("bronze_rows", "INT", False), ("source_rows", "INT", False), ("silver_valid_rows", "INT", False),
        ("silver_rows_inserted", "BIGINT", False), ("silver_rows_updated", "BIGINT", False),
        ("quarantine_rows", "INT", False), ("schema_events", "INT", False),
        ("gold_rows_inserted", "BIGINT", False), ("gold_rows_updated", "BIGINT", False),
        ("error_message", "STRING", False),
    ]),
    RUN_LOG_DTL: ("audit", "One row per entity per run: extraction details, HTTP/attempts and row counts.", [
        ("run_dtl_id", "STRING", True), ("run_id", "STRING", True), ("entity_name", "STRING", True),
        ("extraction_type", "STRING", True), ("parameters", "STRING", False),
        ("start_time", "TIMESTAMP", False), ("end_time", "TIMESTAMP", False),
        ("status", "STRING", True), ("http_status", "INT", False), ("page_count", "INT", False),
        ("attempt_count", "INT", False), ("retry_count", "INT", False), ("records_received", "INT", False),
        ("records_valid", "INT", False), ("records_rejected", "INT", False), ("schema_events", "INT", False),
        ("numeric_strings_converted", "INT", False), ("error_type", "STRING", False),
        ("error_message", "STRING", False),
    ]),
    SCHEMA_CHANGE_LOG: ("audit", "Differences between received payloads and each source's contract.", [
        ("run_id", "STRING", True), ("source_name", "STRING", True), ("entity_id", "STRING", True),
        ("request_page", "INT", False), ("detected_at", "TIMESTAMP", True), ("change_type", "STRING", True),
        ("field_name", "STRING", True), ("expected_type", "STRING", False), ("actual_type", "STRING", False),
        ("details", "STRING", False),
    ]),
    QUARANTINE: ("quarantine", "Rejected rows (ROW) and rejected payloads (PAYLOAD) of all sources, with reasons.", [
        ("run_id", "STRING", True), ("source_name", "STRING", True), ("entity_id", "STRING", True),
        ("rejection_level", "STRING", True), ("request_page", "INT", False), ("row_index", "INT", False),
        ("raw_values", "MAP<STRING,STRING>", False), ("dq_errors", "ARRAY<STRING>", False),
        ("error_details", "STRING", False), ("raw_record", "STRING", False),
        ("source_ingestion_timestamp", "TIMESTAMP", False), ("quarantined_at", "TIMESTAMP", False),
    ]),
}

BRONZE_COLUMNS: list[Column] = [
    ("run_id", "STRING", True), ("source_name", "STRING", True), ("extraction_type", "STRING", True),
    ("ingestion_timestamp", "TIMESTAMP", True), ("entity_id", "STRING", True), ("entity_attributes", "STRING", False),
    ("source_uri", "STRING", False), ("request_params", "STRING", False), ("request_page", "INT", False),
    ("http_status", "INT", False), ("attempt_count", "INT", False), ("raw_response", "STRING", False),
    ("raw_file_path", "STRING", False), ("source_mode", "STRING", False), ("test_mode", "STRING", False),
]

# Generated partition columns: table -> (name, SQL type, expression). Never part of DataFrames.
Generated = tuple[str, str, str]
GENERATED: dict[str, list[Generated]] = {
    BRONZE: [("ingestion_date", "DATE", "CAST(ingestion_timestamp AS DATE)")],
    RUN_LOG_DTL: [("start_date", "DATE", "CAST(start_time AS DATE)")],
}

SILVER_METADATA: list[Column] = [
    ("source_ingestion_timestamp", "TIMESTAMP", False), ("created_run_id", "STRING", False),
    ("last_updated_run_id", "STRING", False), ("created_at", "TIMESTAMP", False), ("updated_at", "TIMESTAMP", False),
]
# Gold lineage columns: created_* set once on insert, last_updated_*/updated_at only when an aggregate changes.
GOLD_METADATA = ("created_run_id", "created_at", "last_updated_run_id", "updated_at")


def silver_columns(cfg: SourceConfig) -> list[Column]:
    required = {c.name for c in cfg.columns if c.required} | set(cfg.silver_keys)
    cols = [(name, SQL_TYPES[t], name in required) for name, t in cfg.silver_column_types().items()]
    return cols + SILVER_METADATA


def columns_for(cfg: SourceConfig, table: str) -> list[Column]:
    if table == BRONZE:
        return BRONZE_COLUMNS
    if table == SILVER:
        return silver_columns(cfg)
    return SHARED_TABLES[table][2]


def fqn(cfg: SourceConfig, table: str) -> str:
    if table == BRONZE:
        return cfg.bronze_fqn
    if table == SILVER:
        return cfg.silver_fqn
    layer = SHARED_TABLES[table][0]
    return cfg.table_in(layer, table)


def partition_columns(cfg: SourceConfig, table: str) -> list[str]:
    if table == SILVER:
        return list(cfg.silver_partition_by)
    return [name for name, _, _ in GENERATED.get(table, [])]


def create_table_sql(name: str, cols: list[Column], comment: str, generated: list[Generated] = (),
                     partition_by: list[str] = ()) -> str:
    defs = [f"{n} {t}{' NOT NULL' if nn else ''}" for n, t, nn in cols]
    defs += [f"{n} {t} GENERATED ALWAYS AS ({expr})" for n, t, expr in generated]
    body = ",\n  ".join(defs)
    partition = f" PARTITIONED BY ({', '.join(partition_by)})" if partition_by else ""
    return (f"CREATE TABLE IF NOT EXISTS {name} (\n  {body}\n) USING DELTA{partition} "
            f"COMMENT '{comment.replace(chr(39), '')}'")


def table_ddl(cfg: SourceConfig, table: str, name: str | None = None) -> str:
    """CREATE TABLE for a logical table (shared, BRONZE or SILVER); `name` overrides the target (migrations)."""
    comments = {BRONZE: f"Append-only raw data of source {cfg.source_name}, one row per received unit/page.",
                SILVER: f"Typed rows of {cfg.source_name}; MERGE on ({', '.join(cfg.silver_keys)})."}
    comment = comments.get(table) or SHARED_TABLES[table][1]
    return create_table_sql(name or fqn(cfg, table), columns_for(cfg, table), comment,
                            GENERATED.get(table, []), partition_columns(cfg, table))


def all_create_statements(cfg: SourceConfig) -> list[str]:
    return [table_ddl(cfg, t) for t in (*SHARED_TABLES, BRONZE, SILVER)]


def spark_type(sql_type: str):
    from pyspark.sql import types as T

    simple = {"STRING": T.StringType(), "TIMESTAMP": T.TimestampType(), "DATE": T.DateType(),
              "DOUBLE": T.DoubleType(), "INT": T.IntegerType(), "BIGINT": T.LongType(), "BOOLEAN": T.BooleanType(),
              "ARRAY<STRING>": T.ArrayType(T.StringType()),
              "MAP<STRING,STRING>": T.MapType(T.StringType(), T.StringType())}
    return simple[sql_type]


def spark_schema(cols: list[Column]):
    from pyspark.sql import types as T

    return T.StructType([T.StructField(n, spark_type(t), True) for n, t, _ in cols])


# Descriptions for framework-owned columns (applied as UC column comments by persistence.apply_governance).
STANDARD_COLUMN_DOCS: dict[str, str] = {
    "run_id": "Pipeline run identifier (UUID), joins to met_audit.run_log",
    "run_dtl_id": "Detail row id: <run_id>-<entity>",
    "start_time": "Run start (run_log) or extraction start for the entity (run_log_dtl), UTC",
    "end_time": "Extraction end for the entity, UTC",
    "start_date": "Partition column: UTC date of start_time (generated)",
    "ingestion_date": "Partition column: UTC date of ingestion_timestamp (generated)",
    "source_name": "Source config name (sources/<source_name>.yml)",
    "entity_id": "Configured entity the data was extracted for (city, region, ...)",
    "entity_name": "Configured entity the data was extracted for (city, region, ...)",
    "extraction_type": "How the data was extracted: API, FILE or DB",
    "parameters": "Extraction parameters as JSON: source URI, request params (secrets masked), window, targets",
    "ingestion_timestamp": "UTC time the raw unit was received",
    "entity_attributes": "Entity attributes from config, as JSON",
    "source_uri": "Request URL (secrets masked), file path or table the raw unit came from",
    "request_params": "Request parameters as JSON, secrets masked",
    "request_page": "Page / unit number within the entity's extraction (1-based)",
    "http_status": "HTTP status of the final attempt (API only)",
    "attempt_count": "Attempts including retries",
    "raw_response": "Raw data exactly as received",
    "raw_file_path": "Copy of the raw data in the landing volume",
    "source_mode": "live = real source, fixture = saved responses",
    "test_mode": "Injected fault for demos/tests; NULL in normal runs",
    "source_ingestion_timestamp": "Ingestion time of the bronze row this row came from",
    "created_run_id": "Run that first inserted the row",
    "last_updated_run_id": "Run that last changed the row's values",
    "created_at": "UTC time the row was inserted",
    "updated_at": "UTC time the row's values last changed",
    "status": "RUNNING, SUCCESS, PARTIAL_SUCCESS or FAILED (runs); SUCCESS or FAILED (entities)",
    "job_success": "Entities that succeeded in the run",
    "job_failed": "Entities that failed in the run",
    "duration_seconds": "Run duration in seconds",
    "dq_errors": "Data-quality reason codes",
    "rejection_level": "ROW = one record rejected; PAYLOAD = whole response rejected",
    "raw_values": "Raw source values of the rejected row, as text",
    "change_type": "NEW_FIELD, MISSING_FIELD, TYPE_CHANGE or UNIT_CHANGE",
}
