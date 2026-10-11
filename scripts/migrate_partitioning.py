"""One-off migration (2026-10-10): partition existing tables and rename run_audit_dtl -> run_log_dtl.

Delta cannot add partitioning to an existing table, so each table is rebuilt without data loss:
  1. CREATE <table>__part with the new contract (tables.table_ddl: partitions + generated columns)
  2. INSERT INTO <table>__part SELECT ... FROM <table>      (run_audit_dtl: request_*_time -> start/end_time)
  3. check row counts are equal (abort otherwise, old table untouched)
  4. DROP the old table, RENAME <table>__part to the final name
Tables already partitioned as expected are skipped, so the script can be re-run safely.
Tags/column comments are re-applied by the next job run (setup task -> apply_governance).

    python scripts/migrate_partitioning.py             # dry run: print the plan and statements
    python scripts/migrate_partitioning.py --execute   # run them on the SQL warehouse
Environment: DBX (path to databricks CLI, optional), DBX_PROFILE (default Atul), DBX_WAREHOUSE (required).
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingest_framework import tables  # noqa: E402
from ingest_framework.source_config import list_sources, load_source  # noqa: E402

EXECUTE = "--execute" in sys.argv
CLI = os.environ.get("DBX") or shutil.which("databricks") or "databricks"


def query(statement: str) -> tuple[list[str], list[list]]:
    body = json.dumps({"warehouse_id": os.environ["DBX_WAREHOUSE"], "statement": statement, "wait_timeout": "50s"})
    out = subprocess.run([CLI, "-p", os.environ.get("DBX_PROFILE", "Atul"), "api", "post", "/api/2.0/sql/statements",
                          "--json", body], capture_output=True, text=True, encoding="utf-8")
    result = json.loads(out.stdout)
    if result.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(f"{statement[:120]} -> {result.get('status')}")
    columns = [c["name"] for c in result.get("manifest", {}).get("schema", {}).get("columns", [])]
    return columns, result.get("result", {}).get("data_array") or []


def sql(statement: str) -> list[list]:
    return query(statement)[1]


def exists(fqn: str) -> bool:
    catalog, schema, name = fqn.split(".")
    return bool(sql(f"SELECT 1 FROM {catalog}.information_schema.tables "
                    f"WHERE table_schema = '{schema}' AND table_name = '{name}'"))


def partitions(fqn: str) -> list[str]:
    """partitionColumns from DESCRIBE DETAIL (the statement API returns it as a JSON array string)."""
    columns, rows = query(f"DESCRIBE DETAIL {fqn}")
    value = rows[0][columns.index("partitionColumns")]
    return json.loads(value) if value else []


def migrate(cfg, table: str, old: str, select_list: str) -> None:
    final = tables.fqn(cfg, table)
    wanted = tables.partition_columns(cfg, table)
    if old == final and exists(final) and partitions(final) == wanted:
        print(f"skip  {final}: already partitioned by {wanted}")
        return
    if not exists(old):
        print(f"skip  {old}: does not exist (the next job run creates {final} with partitions)")
        return
    staging = f"{final}__part"
    cols = ", ".join(n for n, _, _ in tables.columns_for(cfg, table))
    steps = [
        f"DROP TABLE IF EXISTS {staging}",
        tables.table_ddl(cfg, table, name=staging),
        f"INSERT INTO {staging} ({cols}) SELECT {select_list} FROM {old}",
    ]
    print(f"\n-- migrate {old} -> {final}  PARTITIONED BY {wanted}")
    for s in steps:
        print(s if len(s) < 400 else s[:400] + " ...")
    if not EXECUTE:
        print(f"DROP TABLE {old}\nALTER TABLE {staging} RENAME TO {final}")
        return
    for s in steps:
        sql(s)
    n_old, n_new = sql(f"SELECT COUNT(*) FROM {old}")[0][0], sql(f"SELECT COUNT(*) FROM {staging}")[0][0]
    if n_old != n_new:
        raise RuntimeError(f"row count mismatch {old}={n_old} {staging}={n_new}; old table left untouched")
    sql(f"DROP TABLE {old}")
    sql(f"ALTER TABLE {staging} RENAME TO {final}")
    print(f"done  {final}: {n_new} rows, partitions {partitions(final)}")


def main() -> None:
    print("EXECUTE" if EXECUTE else "DRY RUN (add --execute to apply)")
    for suffix in ("", "_edge"):
        dtl_done = False
        for name in list_sources(ROOT):
            cfg = load_source(ROOT, name, schema_suffix=suffix)
            for table in (tables.BRONZE, tables.SILVER):
                cols = ", ".join(n for n, _, _ in tables.columns_for(cfg, table))
                migrate(cfg, table, tables.fqn(cfg, table), cols)
            if not dtl_done:
                dtl_cols = [n for n, _, _ in tables.columns_for(cfg, tables.RUN_LOG_DTL)]
                old_names = {"start_time": "request_start_time AS start_time", "end_time": "request_end_time AS end_time"}
                migrate(cfg, tables.RUN_LOG_DTL, cfg.audit_table("run_audit_dtl"),
                        ", ".join(old_names.get(c, c) for c in dtl_cols))
                dtl_done = True


if __name__ == "__main__":
    main()
