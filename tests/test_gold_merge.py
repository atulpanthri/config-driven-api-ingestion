"""The gold MERGE keeps created_run_id/created_at from the first insert and only moves
last_updated_run_id/updated_at when an aggregate changes."""
import re

from ingest_framework import tables
from ingest_framework.persistence import gold_merge_sql

GOLD_COLS = ["location_id", "location_name", "weather_date_utc", "min_temperature_c", "max_temperature_c",
             "valid_hours", "created_run_id", "created_at", "last_updated_run_id", "updated_at"]


def sections(sql):
    update = re.search(r"THEN UPDATE SET(.*?)WHEN NOT MATCHED", sql, re.S).group(1)
    condition = re.search(r"WHEN MATCHED AND NOT \((.*?)\) THEN", sql, re.S).group(1)
    return update, condition


def test_update_never_touches_created_columns(weather):
    update, _ = sections(gold_merge_sql(weather, "keys_view", GOLD_COLS))
    assert "t.last_updated_run_id = s.last_updated_run_id" in update and "t.updated_at = s.updated_at" in update
    assert "created_run_id" not in update and "created_at" not in update
    assert "t.min_temperature_c = s.min_temperature_c" in update
    assert "UPDATE SET *" not in gold_merge_sql(weather, "keys_view", GOLD_COLS)


def test_insert_sets_all_lineage_and_change_check_ignores_it(weather):
    sql = gold_merge_sql(weather, "keys_view", GOLD_COLS)
    assert "WHEN NOT MATCHED THEN INSERT *" in sql
    assert ":run_id AS created_run_id" in sql and ":run_id AS last_updated_run_id" in sql
    _, condition = sections(sql)
    for col in tables.GOLD_METADATA:
        assert col not in condition
    assert "t.valid_hours <=> s.valid_hours" in condition


def test_merge_keys_are_gold_keys(weather):
    sql = gold_merge_sql(weather, "keys_view", GOLD_COLS)
    assert "ON t.location_id = s.location_id AND t.weather_date_utc = s.weather_date_utc" in sql
