from dataclasses import replace

import pytest

from conftest import build
from ingest_framework import tables
from ingest_framework.source_config import ConfigError


def ddl(cfg, table):
    return tables.table_ddl(cfg, table)


def test_bronze_partitioned_by_ingestion_date(weather):
    sql = ddl(weather, tables.BRONZE)
    assert "CREATE TABLE IF NOT EXISTS workspace.met_raw.weather_raw" in sql
    assert "ingestion_date DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE))" in sql
    assert "PARTITIONED BY (ingestion_date)" in sql


def test_run_log_dtl_renamed_and_partitioned(weather):
    sql = ddl(weather, tables.RUN_LOG_DTL)
    assert "workspace.met_audit.run_log_dtl" in sql
    assert "start_time TIMESTAMP" in sql and "end_time TIMESTAMP" in sql and "request_start_time" not in sql
    assert "start_date DATE GENERATED ALWAYS AS (CAST(start_time AS DATE))" in sql
    assert "PARTITIONED BY (start_date)" in sql


def test_silver_partitioned_from_config(weather, usgs):
    assert "PARTITIONED BY (weather_date_utc)" in ddl(weather, tables.SILVER)
    assert "PARTITIONED BY (event_date_utc)" in ddl(usgs, tables.SILVER)
    assert "PARTITIONED BY" not in ddl(replace(weather, silver_partition_by=()), tables.SILVER)


def test_unpartitioned_shared_tables(weather):
    for t in (tables.RUN_LOG, tables.SCHEMA_CHANGE_LOG, tables.QUARANTINE):
        assert "PARTITIONED BY" not in ddl(weather, t)


def test_generated_columns_never_in_dataframe_columns(weather):
    for table, generated in tables.GENERATED.items():
        names = [n for n, _, _ in tables.columns_for(weather, table)]
        assert not {g for g, _, _ in generated} & set(names)


def test_migration_target_name_override(weather):
    sql = tables.table_ddl(weather, tables.BRONZE, name="workspace.met_raw.weather_raw__new")
    assert "workspace.met_raw.weather_raw__new" in sql and "PARTITIONED BY (ingestion_date)" in sql


@pytest.mark.parametrize("value, message", [(["nope"], "not a silver column"),
                                            (["temperature_c"], "low-cardinality")])
def test_invalid_partition_config(weather_raw, value, message):
    weather_raw["silver"]["partition_by"] = value
    with pytest.raises(ConfigError, match=message):
        build(weather_raw)
