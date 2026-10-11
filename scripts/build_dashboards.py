"""Generate the AI/BI (Lakeview) dashboard files in dashboards/ from compact definitions.

    python scripts/build_dashboards.py            # write dashboards/*.lvdash.json
    python scripts/build_dashboards.py --sql      # print every dataset query (to test them)
    python scripts/build_dashboards.py --validate # also run every dataset on the warehouse and check that
                                                  # each widget field exists in its dataset
                                                  # (databricks CLI; env DBX_PROFILE, DBX_WAREHOUSE)

The bundle deploys the generated files (resources/dashboards.yml). Edit this script, not the JSON.
Grid: 6 columns wide; positions are (x, y, width, height).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "dashboards"
import yaml  # noqa: E402

# Schema names come from framework.yml so the dashboards follow the configured layer model.
_FRAMEWORK = yaml.safe_load((ROOT / "framework.yml").read_text(encoding="utf-8"))
CATALOG = _FRAMEWORK["catalog"]
_S = _FRAMEWORK["schemas"]
RAW, SILVER, GOLD = f"{CATALOG}.{_S['raw']}", f"{CATALOG}.{_S['silver']}", f"{CATALOG}.{_S['gold']}"
AUDIT, QUARANTINE = f"{CATALOG}.{_S['audit']}", f"{CATALOG}.{_S['quarantine']}"
EDGE = "_edge"  # schema_suffix used for demo/edge-case runs
LAYER_SCHEMAS = tuple(_S[k] for k in ("raw", "silver", "gold", "audit", "quarantine"))
GOVERNED = ", ".join(f"'{s}'" for s in LAYER_SCHEMAS + tuple(s + EDGE for s in LAYER_SCHEMAS))
SLA = _FRAMEWORK.get("sla", {"max_run_seconds": 300, "freshness_hours": 24})
JOB_NAME = "api_ingest_job"  # resources/weather_job.yml name (dev mode adds a prefix, matched with LIKE)


GRID_COLUMNS = 12      # GRID_V1 page width
GRID_SCALE = 2         # authored on 6 columns -> 12


def _id(*parts: str) -> str:
    return hashlib.md5("|".join(parts).encode()).hexdigest()[:8]


class Dashboard:
    def __init__(self, key: str):
        self.key = key
        self.datasets: list[dict] = []
        self.pages: list[dict] = []
        self._page: dict | None = None

    # ------------------------------------------------------------- building blocks
    def dataset(self, name: str, display: str, sql: str) -> str:
        self.datasets.append({"name": name, "displayName": display, "queryLines": [sql.strip() + "\n"]})
        return name

    def page(self, display: str) -> None:
        self._page = {"name": _id(self.key, display), "displayName": display, "pageType": "PAGE_TYPE_CANVAS",
                      "layoutVersion": "GRID_V1", "layout": []}
        self.pages.append(self._page)

    def _add(self, widget: dict, pos: tuple[int, int, int, int]) -> None:
        # Layout is authored on a 6-column grid; GRID_V1 pages are 12 columns wide, so x/width are doubled
        # (otherwise every widget lands in the left half of the page).
        x, y, w, h = pos
        self._page["layout"].append({"widget": widget, "position": {
            "x": x * GRID_SCALE, "y": y, "width": w * GRID_SCALE, "height": h}})

    def text(self, markdown: str, pos) -> None:
        lines = [line + "\n" for line in markdown.split("\n")]
        lines[-1] = lines[-1].rstrip("\n")
        self._add({"name": _id(self.key, "text", markdown[:40], str(pos)),
                   "multilineTextboxSpec": {"lines": lines}}, pos)

    @staticmethod
    def _query(ds: str, fields: list[tuple[str, str]], disaggregated: bool) -> list[dict]:
        return [{"name": "main_query", "query": {
            "datasetName": ds, "disaggregated": disaggregated,
            "fields": [{"name": n, "expression": e} for n, e in fields]}}]

    def counter(self, ds: str, expression: str, title: str, pos) -> None:
        self._add({
            "name": _id(self.key, "counter", title),
            "queries": self._query(ds, [("value", expression)], False),
            "spec": {"version": 2, "widgetType": "counter",
                     "encodings": {"value": {"fieldName": "value", "displayName": title}},
                     "frame": {"showTitle": True, "title": title}},
        }, pos)

    def chart(self, kind: str, ds: str, x: str, y: str, title: str, pos, *, color: str | None = None,
              x_type: str = "temporal", x_title: str | None = None, y_title: str | None = None) -> None:
        fields = [(x, f"`{x}`"), (y, f"`{y}`")] + ([(color, f"`{color}`")] if color else [])
        encodings = {
            "x": {"fieldName": x, "displayName": x_title or x, "scale": {"type": x_type},
                  "axis": {"title": x_title or x}},
            "y": {"fieldName": y, "displayName": y_title or y, "scale": {"type": "quantitative"},
                  "axis": {"title": y_title or y}},
        }
        if color:
            encodings["color"] = {"fieldName": color, "displayName": color, "scale": {"type": "categorical"}}
        self._add({
            "name": _id(self.key, kind, title),
            "queries": self._query(ds, fields, True),
            "spec": {"version": 3, "widgetType": kind, "encodings": encodings,
                     "frame": {"showTitle": True, "title": title}},
        }, pos)

    def table(self, ds: str, columns: list[tuple[str, str]], title: str, pos) -> None:
        """columns: (field, kind). The v2 table widget infers formatting from the column types;
        kind only documents intent here."""
        self._add({
            "name": _id(self.key, "table", title),
            "queries": self._query(ds, [(n, f"`{n}`") for n, _ in columns], True),
            "spec": {"version": 2, "widgetType": "table",
                     "encodings": {"columns": [{"fieldName": n, "displayName": n} for n, _ in columns]},
                     "frame": {"showTitle": True, "title": title}},
        }, pos)

    def multi_filter(self, field: str, datasets: list[str], title: str, pos) -> None:
        queries, enc = [], []
        for ds in datasets:
            qname = f"filter_{_id(self.key, field, ds)}"
            queries.append({"name": qname, "query": {"datasetName": ds, "disaggregated": False,
                                                     "fields": [{"name": field, "expression": f"`{field}`"}]}})
            enc.append({"fieldName": field, "displayName": field, "queryName": qname})
        self._add({"name": _id(self.key, "filter", field), "queries": queries,
                   "spec": {"version": 2, "widgetType": "filter-multi-select", "encodings": {"fields": enc},
                            "frame": {"showTitle": True, "title": title}}}, pos)

    # ------------------------------------------------------------- validation
    # Widget field expressions the renderer accepts: a column or ONE aggregate/date function of a
    # column. Nested expressions such as ROUND(SUM(x), 1) fail as "invalid widget"; compute those
    # in the dataset SQL instead.
    ALLOWED_EXPRESSION = re.compile(
        r'^(`[^`]+`|(SUM|AVG|MIN|MAX|COUNT|COUNT_IF|MEDIAN)\(`[^`]+`\)|COUNT\(DISTINCT `[^`]+`\)'
        r'|DATE_TRUNC\("(HOUR|DAY|WEEK|MONTH|YEAR)", `[^`]+`\))$')

    def problems(self, dataset_columns: dict[str, set[str]] | None = None) -> list[str]:
        """Structural checks: grid bounds, overlaps, datasets, expressions, encodings, columns."""
        out = []
        names = {ds["name"] for ds in self.datasets}
        for page in self.pages:
            cells: dict[tuple[int, int], str] = {}
            for item in page["layout"]:
                w, pos = item["widget"], item["position"]
                title = w.get("spec", {}).get("frame", {}).get("title", w["name"])
                label = f"{self.key}/{page['displayName']}/{title}"
                if pos["x"] < 0 or pos["width"] < 1 or pos["x"] + pos["width"] > GRID_COLUMNS:
                    out.append(f"{label}: position outside the {GRID_COLUMNS}-column grid {pos}")
            right = max((i["position"]["x"] + i["position"]["width"] for i in page["layout"]), default=0)
            if page["layout"] and right != GRID_COLUMNS:
                out.append(f"{self.key}/{page['displayName']}: widgets only reach column {right} of {GRID_COLUMNS}")
            for item in page["layout"]:
                w, pos = item["widget"], item["position"]
                title = w.get("spec", {}).get("frame", {}).get("title", w["name"])
                label = f"{self.key}/{page['displayName']}/{title}"
                for x in range(pos["x"], pos["x"] + pos["width"]):
                    for y in range(pos["y"], pos["y"] + pos["height"]):
                        if (x, y) in cells:
                            out.append(f"{label}: overlaps {cells[(x, y)]} at {(x, y)}")
                        cells[(x, y)] = label
                query_fields: set[str] = set()
                for q in w.get("queries", []):
                    ds = q["query"]["datasetName"]
                    if ds not in names:
                        out.append(f"{label}: unknown dataset {ds}")
                    cols = (dataset_columns or {}).get(ds)
                    for f in q["query"]["fields"]:
                        query_fields.add(f["name"])
                        if not self.ALLOWED_EXPRESSION.match(f["expression"]):
                            out.append(f"{label}: unsupported expression {f['expression']!r}")
                        for ref in re.findall(r"`([^`]+)`", f["expression"]):
                            if cols is not None and ref not in cols:
                                out.append(f"{label}: column {ref!r} not in dataset {ds}")
                enc = w.get("spec", {}).get("encodings", {})
                refs = [e["fieldName"] for e in enc.get("columns", []) + enc.get("fields", [])]
                refs += [v["fieldName"] for v in enc.values() if isinstance(v, dict) and "fieldName" in v]
                out += [f"{label}: encoding field {r!r} is not a query field" for r in refs if r not in query_fields]
        return out

    def write(self) -> Path:
        OUT.mkdir(exist_ok=True)
        path = OUT / f"{self.key}.lvdash.json"
        path.write_text(json.dumps({"datasets": self.datasets, "pages": self.pages}, indent=2) + "\n", encoding="utf-8")
        return path


# =============================================================================== 1. meteorological data (business)
def _last_updated(d: Dashboard, name: str, gold: str) -> str:
    """One-row dataset: when the gold table last changed (max updated_at), formatted for display."""
    return d.dataset(name, "Last updated (gold)", f"""
SELECT concat(date_format(MAX(updated_at), 'yyyy-MM-dd HH:mm'), ' UTC') AS last_updated
FROM {gold}""")


def business() -> Dashboard:
    d = Dashboard("business_insights")
    daily = d.dataset("wx_daily", "Weather daily (gold)", f"""
SELECT location_name, weather_date_utc, min_temperature_c, avg_temperature_c, max_temperature_c,
       total_precipitation_mm, precipitation_hours, max_wind_speed_kmh, avg_relative_humidity_pct,
       CAST(heat_alert AS INT) AS heat_alert_day, CAST(high_wind_alert AS INT) AS wind_alert_day,
       CAST(heavy_precipitation_alert AS INT) AS heavy_precip_alert_day, is_complete_day
FROM {GOLD}.weather_daily""")
    hourly = d.dataset("wx_hourly", "Weather hourly (silver)", f"""
SELECT location_name, timestamp_utc, temperature_c, relative_humidity_pct, precipitation_mm, wind_speed_kmh
FROM {SILVER}.weather_hourly""")
    compare = d.dataset("wx_compare", "City comparison", f"""
SELECT location_name,
       MIN(min_temperature_c)                  AS window_min_temp_c,
       MAX(max_temperature_c)                  AS window_max_temp_c,
       ROUND(AVG(avg_temperature_c), 1)        AS window_avg_temp_c,
       ROUND(SUM(total_precipitation_mm), 1)   AS window_precip_mm,
       COUNT_IF(total_precipitation_mm >= 1.0) AS wet_days,
       MAX(max_wind_speed_kmh)                 AS window_max_wind_kmh,
       COUNT_IF(heat_alert OR high_wind_alert OR heavy_precipitation_alert) AS alert_days
FROM {GOLD}.weather_daily GROUP BY location_name""")
    alerts = d.dataset("wx_alerts", "Alert days", f"""
SELECT weather_date_utc, location_name, max_temperature_c, max_wind_speed_kmh, total_precipitation_mm,
       concat_ws(', ', IF(heat_alert, 'heat', NULL), IF(high_wind_alert, 'high wind', NULL),
                 IF(heavy_precipitation_alert, 'heavy precipitation', NULL)) AS alerts
FROM {GOLD}.weather_daily
WHERE heat_alert OR high_wind_alert OR heavy_precipitation_alert""")
    wx_updated = _last_updated(d, "wx_updated", f"{GOLD}.weather_daily")

    d.page("Weather")
    d.text("# Meteorological data\nOpen-Meteo forecast for 5 cities, hourly → daily (UTC days), from `met_silver` / "
           "`met_gold`. Alert flags use **demo thresholds** from the source YAML, not official warnings.", (0, 0, 4, 2))
    d.table(wx_updated, [("last_updated", "string")], "Last updated", (4, 0, 2, 2))
    d.multi_filter("location_name", [daily, hourly, compare, alerts], "City", (0, 2, 2, 1))
    d.counter(daily, "COUNT(DISTINCT `location_name`)", "Cities", (0, 3, 2, 2))
    d.counter(daily, "MAX(`max_temperature_c`)", "Hottest hour (°C)", (2, 3, 2, 2))
    d.counter(daily, "SUM(`total_precipitation_mm`)", "Total precipitation (mm)", (4, 3, 2, 2))
    d.counter(daily, "SUM(`heat_alert_day`)", "Heat alert days", (0, 5, 2, 2))
    d.counter(daily, "SUM(`wind_alert_day`)", "High-wind alert days", (2, 5, 2, 2))
    d.counter(daily, "SUM(`heavy_precip_alert_day`)", "Heavy-rain alert days", (4, 5, 2, 2))
    d.chart("line", hourly, "timestamp_utc", "temperature_c", "Hourly temperature by city (°C)", (0, 7, 6, 6),
            color="location_name", x_title="Hour (UTC)", y_title="°C")
    d.chart("bar", daily, "weather_date_utc", "total_precipitation_mm", "Daily precipitation by city (mm)",
            (0, 13, 3, 6), color="location_name", x_title="Date (UTC)", y_title="mm")
    d.chart("bar", daily, "weather_date_utc", "max_wind_speed_kmh", "Daily maximum wind by city (km/h)",
            (3, 13, 3, 6), color="location_name", x_title="Date (UTC)", y_title="km/h")
    d.chart("line", daily, "weather_date_utc", "avg_temperature_c", "Daily average temperature (°C)",
            (0, 19, 3, 6), color="location_name", x_title="Date (UTC)", y_title="°C")
    d.table(compare, [("location_name", "string"), ("window_min_temp_c", "number"), ("window_max_temp_c", "number"),
                      ("window_avg_temp_c", "number"), ("window_precip_mm", "number"), ("wet_days", "number"),
                      ("window_max_wind_kmh", "number"), ("alert_days", "number")],
            "City comparison (loaded window)", (3, 19, 3, 6))
    d.table(alerts, [("weather_date_utc", "datetime"), ("location_name", "string"), ("alerts", "string"),
                     ("max_temperature_c", "number"), ("max_wind_speed_kmh", "number"),
                     ("total_precipitation_mm", "number")], "Alert days (demo thresholds)", (0, 25, 6, 5))

    eq_daily = d.dataset("eq_daily", "Earthquakes daily (gold)", f"""
SELECT region_name, event_date_utc, quake_count, max_magnitude, avg_magnitude, significant_quakes, strongest_place
FROM {GOLD}.earthquakes_daily""")
    eq_region = d.dataset("eq_region", "Earthquakes by region", f"""
SELECT region_name, SUM(quake_count) AS quakes, MAX(max_magnitude) AS max_magnitude,
       SUM(significant_quakes) AS significant_quakes
FROM {GOLD}.earthquakes_daily GROUP BY region_name""")
    eq_top = d.dataset("eq_top", "Strongest events", f"""
SELECT event_time_utc, region_name, magnitude, place, depth_km
FROM {SILVER}.earthquake_events ORDER BY magnitude DESC LIMIT 15""")
    eq_updated = _last_updated(d, "eq_updated", f"{GOLD}.earthquakes_daily")
    d.page("Earthquakes")
    d.text("# Earthquake activity\nUSGS catalog (M2.5+, last 7 days) for 5 regions. "
           "This source was onboarded **by configuration only** (`sources/usgs_earthquakes.yml`).", (0, 0, 4, 2))
    d.table(eq_updated, [("last_updated", "string")], "Last updated", (4, 0, 2, 2))
    d.multi_filter("region_name", [eq_daily, eq_region, eq_top], "Region", (0, 2, 2, 1))
    d.counter(eq_region, "SUM(`quakes`)", "Earthquakes", (0, 3, 2, 2))
    d.counter(eq_region, "MAX(`max_magnitude`)", "Largest magnitude", (2, 3, 2, 2))
    d.counter(eq_region, "SUM(`significant_quakes`)", "Significant quakes (M4.5+)", (4, 3, 2, 2))
    d.chart("bar", eq_daily, "event_date_utc", "quake_count", "Earthquakes per day by region", (0, 5, 6, 6),
            color="region_name", x_title="Date (UTC)", y_title="events")
    d.chart("bar", eq_region, "region_name", "max_magnitude", "Largest magnitude by region", (0, 11, 3, 6),
            x_type="categorical", x_title="Region", y_title="magnitude")
    d.table(eq_top, [("event_time_utc", "datetime"), ("region_name", "string"), ("magnitude", "number"),
                     ("place", "string"), ("depth_km", "number")], "Strongest events", (3, 11, 3, 6))
    return d


# =============================================================================== 2. pipeline monitoring
def _dtl(audit: str) -> str:
    """run_log_dtl joined to run_log for the source name and run start."""
    return f"{audit}.run_log_dtl d JOIN {audit}.run_log r USING (run_id)"


JOB_IDS = f"(SELECT job_id FROM system.lakeflow.jobs WHERE name LIKE '%{JOB_NAME}%')"
LAST_7D = "r.start_time >= current_timestamp() - INTERVAL 7 DAYS AND r.status <> 'RUNNING'"
STAGE_SQL = """CASE
             WHEN d.error_type IN ('TIMEOUT', 'CONNECTION_ERROR', 'REQUEST_ERROR', 'PAGINATION_LIMIT')
                  OR d.error_type LIKE 'HTTP%' THEN '1 extraction'
             WHEN d.error_type IN ('NO_VALID_ROWS') THEN '3 data quality'
             WHEN d.error_type = 'UNEXPECTED_ERROR' THEN '5 unexpected'
             ELSE '2 parse & contract' END"""


def monitoring() -> Dashboard:
    d = Dashboard("pipeline_monitoring")
    A, Q = AUDIT, QUARANTINE
    max_s, fresh_h = SLA["max_run_seconds"], SLA["freshness_hours"]

    # ---- datasets (source-filterable unless noted)
    exec_kpi = d.dataset("exec_kpi", "Execution KPIs (7 days)", f"""
SELECT r.source_name, COUNT(*) AS runs, COUNT_IF(r.status = 'SUCCESS') AS successful,
       COUNT_IF(r.status = 'PARTIAL_SUCCESS') AS partial, COUNT_IF(r.status = 'FAILED') AS failed,
       COALESCE(SUM(t.retries), 0) AS retries
FROM {A}.run_log r
LEFT JOIN (SELECT run_id, SUM(retry_count) AS retries FROM {A}.run_log_dtl GROUP BY run_id) t USING (run_id)
WHERE {LAST_7D}
GROUP BY r.source_name""")
    job_kpi = d.dataset("job_kpi", "Databricks job runs (7 days, all sources)", f"""
SELECT COUNT(DISTINCT run_id) AS job_runs,
       COUNT(DISTINCT IF(result_state IN ('SKIPPED', 'CANCELLED', 'UPSTREAM_CANCELLED', 'EXCLUDED'), run_id, NULL))
         AS skipped_or_cancelled,
       COUNT(DISTINCT IF(result_state IN ('FAILED', 'ERROR', 'TIMED_OUT'), run_id, NULL)) AS job_failed
FROM system.lakeflow.job_run_timeline
WHERE job_id IN {JOB_IDS} AND period_start_time >= current_timestamp() - INTERVAL 7 DAYS""")
    perf = d.dataset("perf", "Run performance", f"""
SELECT r.source_name, r.run_id, r.start_time, r.status, r.duration_seconds,
       ROUND(AVG(timestampdiff(MILLISECOND, d.start_time, d.end_time)) / 1000.0, 2) AS avg_extraction_seconds
FROM {_dtl(A)}
WHERE r.status <> 'RUNNING' AND r.start_time >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY r.source_name, r.run_id, r.start_time, r.status, r.duration_seconds""")
    stages = d.dataset("stages", "Job task duration by stage (all sources)", f"""
SELECT task_key AS stage, ROUND(AVG(timestampdiff(MILLISECOND, period_start_time, period_end_time)) / 1000.0, 1)
         AS avg_seconds, COUNT(*) AS task_runs
FROM system.lakeflow.job_task_run_timeline
WHERE job_id IN {JOB_IDS} AND period_start_time >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY task_key""")
    ingest = d.dataset("ingest", "Ingestion per run", f"""
SELECT r.source_name, r.run_id, r.start_time, r.source_rows, r.silver_valid_rows AS target_rows,
       r.silver_rows_inserted + r.silver_rows_updated AS rows_written,
       ROUND(r.source_rows / NULLIF(r.duration_seconds, 0), 1) AS rows_per_second
FROM {A}.run_log r WHERE {LAST_7D}""")
    ingest_long = d.dataset("ingest_long", "Source vs target records per run", f"""
SELECT r.source_name, r.start_time, 'source records' AS measure, r.source_rows AS record_count
FROM {A}.run_log r WHERE {LAST_7D}
UNION ALL
SELECT r.source_name, r.start_time, 'target records (silver)', r.silver_valid_rows FROM {A}.run_log r WHERE {LAST_7D}""")
    dq = d.dataset("dq", "Data quality (7 days)", f"""
WITH runs AS (
  SELECT r.source_name, SUM(r.source_rows) AS source_rows, SUM(r.silver_valid_rows) AS valid_rows,
         SUM(r.quarantine_rows) AS rejected_rows
  FROM {A}.run_log r WHERE {LAST_7D} GROUP BY r.source_name),
q AS (
  SELECT source_name,
         COUNT_IF(exists(dq_errors, e -> e LIKE 'DUPLICATE%')) AS duplicate_rows,
         COUNT_IF(exists(dq_errors, e -> e LIKE 'NULL%'))      AS null_rows
  FROM {Q}.data_quarantine WHERE quarantined_at >= current_timestamp() - INTERVAL 7 DAYS GROUP BY source_name)
SELECT runs.source_name, runs.source_rows, runs.valid_rows, runs.rejected_rows,
       COALESCE(q.duplicate_rows, 0) AS duplicate_rows, COALESCE(q.null_rows, 0) AS null_rows,
       ROUND(100.0 * COALESCE(q.null_rows, 0) / NULLIF(runs.source_rows, 0), 3) AS null_pct,
       ROUND(100.0 * runs.rejected_rows / NULLIF(runs.source_rows, 0), 3) AS rejection_pct
FROM runs LEFT JOIN q USING (source_name)""")
    reasons = d.dataset("reasons", "Quarantine reasons", f"""
SELECT source_name, reason, COUNT(*) AS rows_quarantined
FROM {Q}.data_quarantine LATERAL VIEW explode(dq_errors) x AS reason GROUP BY source_name, reason""")
    sla = d.dataset("sla", "SLA per run", f"""
SELECT r.source_name, r.run_id, r.start_time, r.status, r.duration_seconds,
       timestampadd(SECOND, {max_s}, r.start_time)                               AS expected_completion,
       timestampadd(SECOND, CAST(r.duration_seconds AS INT), r.start_time)        AS actual_completion,
       IF(r.status = 'FAILED' OR r.duration_seconds > {max_s}, 'BREACHED', 'MET') AS sla_status,
       IF(r.status = 'FAILED' OR r.duration_seconds > {max_s}, 1, 0)              AS breach,
       IF(r.status = 'FAILED' OR r.duration_seconds > {max_s}, 0, 100)            AS met_pct
FROM {A}.run_log r WHERE {LAST_7D}""")
    fresh = d.dataset("fresh", "Freshness per entity", f"""
SELECT r.source_name, d.entity_name, MAX(d.end_time) AS last_successful_load,
       ROUND(timestampdiff(MINUTE, MAX(d.end_time), current_timestamp()) / 60.0, 1) AS hours_since_load,
       IF(timestampdiff(HOUR, MAX(d.end_time), current_timestamp()) <= {fresh_h}, 'OK', 'STALE') AS freshness_sla,
       IF(timestampdiff(HOUR, MAX(d.end_time), current_timestamp()) <= {fresh_h}, 0, 1) AS stale
FROM {_dtl(A)} WHERE d.status = 'SUCCESS' GROUP BY r.source_name, d.entity_name""")
    latency = d.dataset("latency", "Source-to-target latency per run", f"""
SELECT r.source_name, r.run_id, r.start_time,
       ROUND(timestampdiff(MILLISECOND, MAX(d.end_time),
             timestampadd(SECOND, CAST(r.duration_seconds AS INT), r.start_time)) / 1000.0, 1)
         AS source_to_target_seconds
FROM {_dtl(A)} WHERE {LAST_7D} AND d.status = 'SUCCESS'
GROUP BY r.source_name, r.run_id, r.start_time, r.duration_seconds""")
    dbu = d.dataset("dbu", "DBU consumption of the job (30 days, all sources)", f"""
SELECT usage_date, sku_name, ROUND(SUM(usage_quantity), 3) AS dbu
FROM system.billing.usage
WHERE usage_metadata.job_id IN {JOB_IDS} AND usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY usage_date, sku_name""")
    errors = d.dataset("errors", "Errors by code and stage", f"""
SELECT r.source_name, {STAGE_SQL} AS stage, d.error_type AS error_code, COUNT(*) AS failures,
       MAX(d.start_time) AS last_seen
FROM {_dtl(A)} WHERE d.status = 'FAILED' GROUP BY ALL
UNION ALL
SELECT r.source_name, '4 persist / run', 'RUN_FAILED', COUNT(*), MAX(r.start_time)
FROM {A}.run_log r WHERE r.status = 'FAILED' GROUP BY r.source_name""")
    exceptions = d.dataset("exceptions", "Recent exceptions", f"""
SELECT r.source_name, d.start_time, d.entity_name, {STAGE_SQL} AS stage, d.error_type AS error_code,
       d.http_status, d.attempt_count, d.error_message
FROM {_dtl(A)} WHERE d.status = 'FAILED'
UNION ALL
SELECT r.source_name, r.start_time, '(run)', '4 persist / run', 'RUN_FAILED', NULL, NULL, r.error_message
FROM {A}.run_log r WHERE r.status = 'FAILED'""")
    recon = d.dataset("recon", "Reconciliation per run", f"""
SELECT r.source_name, r.run_id, r.start_time, r.status, r.source_rows, t.valid AS dq_valid,
       t.rejected AS dq_rejected, r.source_rows - t.valid - t.rejected AS source_vs_dq_diff,
       r.silver_valid_rows, r.silver_valid_rows - t.valid AS dq_vs_silver_diff,
       r.silver_rows_inserted, r.silver_rows_updated,
       r.silver_valid_rows - r.silver_rows_inserted - r.silver_rows_updated AS silver_unchanged,
       IF(r.source_rows - t.valid - t.rejected = 0 AND r.silver_valid_rows = t.valid, 'OK', 'MISMATCH') AS recon_status,
       IF(r.source_rows - t.valid - t.rejected = 0 AND r.silver_valid_rows = t.valid, 0, 1) AS mismatch
FROM {A}.run_log r
JOIN (SELECT run_id, SUM(records_valid) AS valid, SUM(records_rejected) AS rejected
      FROM {A}.run_log_dtl GROUP BY run_id) t USING (run_id)
WHERE r.status <> 'RUNNING'""")

    # ---- layout (authored on 6 columns)
    d.page("Pipeline health")
    d.text("# Pipeline & data health\nFrom `met_audit.run_log` (one row per run), `met_audit.run_log_dtl` (one row "
           "per entity), `met_quarantine` and Databricks system tables. Last 7 days unless stated.", (0, 0, 6, 2))
    d.multi_filter("source_name", [exec_kpi, perf, ingest, ingest_long, dq, reasons, sla, fresh, latency, errors,
                                   exceptions, recon], "Source", (0, 2, 2, 1))
    d.text("Tiles marked *all sources* come from Databricks system tables (job level) and ignore the Source filter.",
           (2, 2, 4, 1))

    d.text("## 1 · Pipeline execution", (0, 3, 6, 1))
    d.counter(exec_kpi, "SUM(`runs`)", "Total runs", (0, 4, 1, 2))
    d.counter(exec_kpi, "SUM(`successful`)", "Successful", (1, 4, 1, 2))
    d.counter(exec_kpi, "SUM(`partial`)", "Partial", (2, 4, 1, 2))
    d.counter(exec_kpi, "SUM(`failed`)", "Failed", (3, 4, 1, 2))
    d.counter(exec_kpi, "SUM(`retries`)", "Retries", (4, 4, 1, 2))
    d.counter(job_kpi, "SUM(`skipped_or_cancelled`)", "Skipped/cancelled (all sources)", (5, 4, 1, 2))

    d.text("## 2 · Performance", (0, 6, 6, 1))
    d.counter(perf, "AVG(`duration_seconds`)", "Avg runtime (s)", (0, 7, 2, 2))
    d.counter(perf, "MAX(`duration_seconds`)", "Max runtime (s)", (2, 7, 2, 2))
    d.counter(perf, "AVG(`avg_extraction_seconds`)", "Avg extraction latency per entity (s)", (4, 7, 2, 2))
    d.chart("line", perf, "start_time", "duration_seconds", "Execution duration per run (s)", (0, 9, 4, 5),
            color="source_name", x_title="Run start (UTC)", y_title="seconds")
    d.chart("bar", stages, "stage", "avg_seconds", "Avg task duration by stage (all sources)", (4, 9, 2, 5),
            x_type="categorical", x_title="Job task", y_title="seconds")

    d.text("## 3 · Data ingestion", (0, 14, 6, 1))
    d.counter(ingest, "SUM(`source_rows`)", "Source records", (0, 15, 2, 2))
    d.counter(ingest, "SUM(`target_rows`)", "Target records (silver)", (2, 15, 2, 2))
    d.counter(ingest, "AVG(`rows_per_second`)", "Ingestion throughput (rows/s)", (4, 15, 2, 2))
    d.chart("bar", ingest_long, "start_time", "record_count", "Source vs target records per run", (0, 17, 6, 5),
            color="measure", x_title="Run start (UTC)", y_title="records")

    d.text("## 4 · Data quality", (0, 22, 6, 1))
    d.counter(dq, "SUM(`valid_rows`)", "Valid records", (0, 23, 1, 2))
    d.counter(dq, "SUM(`rejected_rows`)", "Rejected records", (1, 23, 1, 2))
    d.counter(dq, "SUM(`duplicate_rows`)", "Duplicate records", (2, 23, 1, 2))
    d.counter(dq, "AVG(`null_pct`)", "Null %", (3, 23, 1, 2))
    d.counter(dq, "AVG(`rejection_pct`)", "Rejection rate %", (4, 23, 2, 2))
    d.chart("bar", reasons, "reason", "rows_quarantined", "Quarantine reasons", (0, 25, 6, 5), color="source_name",
            x_type="categorical", x_title="Reason", y_title="rows")

    d.text("## 5 · SLA monitoring", (0, 30, 6, 1))
    d.counter(sla, "SUM(`breach`)", "SLA breaches", (0, 31, 2, 2))
    d.counter(sla, "AVG(`met_pct`)", "SLA met %", (2, 31, 2, 2))
    d.text(f"**Targets** (framework.yml `sla`): run completes within **{max_s} s** of start and does not FAIL; "
           f"each entity loads successfully at least every **{fresh_h} h**.", (4, 31, 2, 2))
    d.table(sla, [("start_time", "datetime"), ("source_name", "string"), ("status", "string"),
                  ("duration_seconds", "number"), ("expected_completion", "datetime"),
                  ("actual_completion", "datetime"), ("sla_status", "string")],
            "Expected vs actual completion per run", (0, 33, 6, 6))

    d.text("## 6 · Data freshness", (0, 39, 6, 1))
    d.counter(fresh, "MAX(`hours_since_load`)", "Hours since last successful load (worst)", (0, 40, 2, 2))
    d.counter(latency, "AVG(`source_to_target_seconds`)", "Avg source-to-target latency (s)", (2, 40, 2, 2))
    d.counter(fresh, "SUM(`stale`)", "Stale entities", (4, 40, 2, 2))
    d.table(fresh, [("source_name", "string"), ("entity_name", "string"), ("last_successful_load", "datetime"),
                    ("hours_since_load", "number"), ("freshness_sla", "string")],
            "Last successful load per entity", (0, 42, 6, 6))

    d.text("## 7 · Infrastructure", (0, 48, 6, 1))
    d.counter(dbu, "SUM(`dbu`)", "DBUs, 30 days (all sources)", (0, 49, 2, 2))
    d.text("Serverless compute: CPU, memory and cluster utilisation are managed by Databricks and not exposed "
           "(`system.compute.node_timeline` covers classic clusters only). Cost is tracked as DBU consumption "
           "from `system.billing.usage` for this job.", (2, 49, 4, 2))
    d.chart("bar", dbu, "usage_date", "dbu", "DBU consumption per day (all sources)", (0, 51, 6, 5),
            color="sku_name", x_title="Date", y_title="DBU")

    d.text("## 8 · Error monitoring", (0, 56, 6, 1))
    d.chart("bar", errors, "stage", "failures", "Failures by stage", (0, 57, 2, 6), color="error_code",
            x_type="categorical", x_title="Stage", y_title="failures")
    d.table(errors, [("source_name", "string"), ("stage", "string"), ("error_code", "string"),
                     ("failures", "number"), ("last_seen", "datetime")], "Error codes", (2, 57, 4, 6))
    d.table(exceptions, [("start_time", "datetime"), ("source_name", "string"), ("entity_name", "string"),
                         ("stage", "string"), ("error_code", "string"), ("http_status", "number"),
                         ("attempt_count", "number"), ("error_message", "string")],
            "Exception messages", (0, 63, 6, 6))

    d.text("## 9 · Audit & reconciliation", (0, 69, 6, 1))
    d.counter(recon, "SUM(`mismatch`)", "Runs with reconciliation differences", (0, 70, 2, 2))
    d.text("`source_rows = dq_valid + dq_rejected` (rows formed from payloads) and `silver_valid_rows = dq_valid` "
           "(rows sent to the silver MERGE); `silver_unchanged = valid − inserted − updated`. Any difference ≠ 0 "
           "is a MISMATCH.", (2, 70, 4, 2))
    d.table(recon, [("start_time", "datetime"), ("source_name", "string"), ("run_id", "string"),
                    ("status", "string"), ("source_rows", "number"), ("dq_valid", "number"),
                    ("dq_rejected", "number"), ("source_vs_dq_diff", "number"), ("silver_valid_rows", "number"),
                    ("dq_vs_silver_diff", "number"), ("silver_rows_inserted", "number"),
                    ("silver_rows_updated", "number"), ("silver_unchanged", "number"), ("recon_status", "string")],
            "Source / target reconciliation per run", (0, 72, 6, 7))

    XA, XQ = AUDIT + EDGE, QUARANTINE + EDGE
    e_runs = d.dataset("edge_runs", "Edge-case runs", f"""
SELECT start_time, source_name, test_mode, status, job_failed, quarantine_rows, schema_events, error_message
FROM {XA}.run_log WHERE status <> 'RUNNING'""")
    e_q = d.dataset("edge_q", "Edge-case quarantine", f"""
SELECT q.quarantined_at, q.source_name, r.test_mode, q.entity_id, q.rejection_level,
       array_join(q.dq_errors, ', ') AS dq_errors, CAST(q.raw_values AS STRING) AS raw_values, q.error_details
FROM {XQ}.data_quarantine q JOIN {XA}.run_log r USING (run_id)""")
    e_s = d.dataset("edge_s", "Edge-case schema events", f"""
SELECT s.detected_at, s.source_name, r.test_mode, s.entity_id, s.change_type, s.field_name, s.actual_type
FROM {XA}.schema_change_log s JOIN {XA}.run_log r USING (run_id)""")
    e_f = d.dataset("edge_f", "Edge-case entity failures", f"""
SELECT r.start_time, r.source_name, r.test_mode, d.entity_name, d.http_status, d.attempt_count, d.error_type,
       d.error_message
FROM {_dtl(XA)} WHERE d.status = 'FAILED'""")
    d.page("Edge-case runs")
    d.text(f"# Edge-case evidence\nFault-injected runs (fixture mode) in the `*{EDGE}` schemas "
           f"(`met_audit{EDGE}`, `met_quarantine{EDGE}`, ...), so demo data never reaches the main tables.",
           (0, 0, 6, 2))
    d.table(e_runs, [("start_time", "datetime"), ("source_name", "string"), ("test_mode", "string"),
                     ("status", "string"), ("job_failed", "number"), ("quarantine_rows", "number"),
                     ("schema_events", "number"), ("error_message", "string")], "Runs", (0, 2, 6, 5))
    d.table(e_f, [("start_time", "datetime"), ("source_name", "string"), ("test_mode", "string"),
                  ("entity_name", "string"), ("http_status", "number"), ("attempt_count", "number"),
                  ("error_type", "string"), ("error_message", "string")], "Entity failures", (0, 7, 6, 4))
    d.table(e_q, [("quarantined_at", "datetime"), ("source_name", "string"), ("test_mode", "string"),
                  ("entity_id", "string"), ("rejection_level", "string"), ("dq_errors", "string"),
                  ("raw_values", "string"), ("error_details", "string")], "Quarantine", (0, 11, 6, 5))
    d.table(e_s, [("detected_at", "datetime"), ("source_name", "string"), ("test_mode", "string"),
                  ("entity_id", "string"), ("change_type", "string"), ("field_name", "string"),
                  ("actual_type", "string")], "Schema events", (0, 16, 6, 4))
    return d


# =============================================================================== 3. governance
def _source_defs() -> list[dict]:
    """Source name, endpoint and target tables from sources/*.yml (declared lineage, per-dataset checks)."""
    out = []
    for path in sorted((ROOT / _FRAMEWORK.get("sources_dir", "sources")).glob("*.yml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        t = raw["target"]
        out.append({"source": raw["source_name"], "endpoint": raw.get("request", {}).get("base_url", ""),
                    "bronze": t["bronze_table"], "silver": t["silver_table"], "gold": t["gold_table"]})
    return out


def _lit(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def governance() -> Dashboard:
    """Nine governance sections on three pages (ownership/metadata/lineage, security/compliance/audit,
    data health). Governance covers the real schemas; the *_edge demo copies are excluded."""
    d = Dashboard("data_governance")
    I, A, Q = f"{CATALOG}.information_schema", AUDIT, QUARANTINE
    MAIN = ", ".join(f"'{s}'" for s in LAYER_SCHEMAS)
    max_s, fresh_h = SLA["max_run_seconds"], SLA["freshness_hours"]
    srcs = _source_defs()
    FULL_NAME = "request_params['full_name_arg']"
    like = " OR ".join(f"{{col}} LIKE '{CATALOG}.{s}.%'" for s in LAYER_SCHEMAS)
    governed_events = (f"event_date >= current_date() - INTERVAL 7 DAYS AND service_name = 'unityCatalog' "
                       f"AND ({like.format(col=FULL_NAME)})")

    tags = f"""(SELECT schema_name, table_name,
                 MAX(IF(tag_name = 'layer', tag_value, NULL))           AS layer,
                 MAX(IF(tag_name = 'source', tag_value, NULL))          AS source,
                 MAX(IF(tag_name = 'classification', tag_value, NULL))  AS classification,
                 MAX(IF(tag_name = 'data_owner', tag_value, NULL))      AS data_owner,
                 MAX(IF(tag_name = 'data_steward', tag_value, NULL))    AS data_steward,
                 MAX(IF(tag_name = 'retention_days', tag_value, NULL))  AS retention_days
          FROM {I}.table_tags GROUP BY schema_name, table_name)"""
    base = f"""SELECT t.table_schema AS schema_name, t.table_name, t.table_owner AS uc_owner, t.comment,
       g.layer, g.source, g.classification, g.data_owner, g.data_steward, g.retention_days
FROM {I}.tables t
LEFT JOIN {tags} g ON g.schema_name = t.table_schema AND g.table_name = t.table_name
WHERE t.table_schema IN ({MAIN})"""

    # declared chain Source API -> bronze -> silver -> gold, from the source YAMLs
    chain = []
    for s in srcs:
        bronze, silver, gold = f"{RAW}.{s['bronze']}", f"{SILVER}.{s['silver']}", f"{GOLD}.{s['gold']}"
        for hop, a, b in ((1, s["endpoint"], bronze), (2, bronze, silver), (3, silver, gold)):
            chain.append(f"SELECT {_lit(s['source'])} AS source_name, {hop} AS hop, "
                         f"{_lit(a)} AS from_node, {_lit(b)} AS to_node")
    edges = " UNION ALL ".join(chain)
    # oldest data per table, for retention compliance
    age_parts = []
    for s in srcs:
        age_parts += [
            f"SELECT '{_S['raw']}' AS schema_name, {_lit(s['bronze'])} AS table_name, "
            f"MIN(ingestion_date) AS oldest_date FROM {RAW}.{s['bronze']}",
            f"SELECT '{_S['silver']}', {_lit(s['silver'])}, MIN(CAST(created_at AS DATE)) FROM {SILVER}.{s['silver']}",
            f"SELECT '{_S['gold']}', {_lit(s['gold'])}, MIN(CAST(created_at AS DATE)) FROM {GOLD}.{s['gold']}"]
    ages = " UNION ALL ".join(age_parts)
    # one row per dataset with its source, for dataset-level freshness
    dataset_rows = " UNION ALL ".join(
        f"SELECT {_lit(s['source'])} AS source_name, {_lit(f'{layer}.{name}')} AS dataset"
        for s in srcs for layer, name in ((RAW, s["bronze"]), (SILVER, s["silver"]), (GOLD, s["gold"])))

    retention_sql = f"""
SELECT a.schema_name, a.table_name, a.oldest_date, datediff(current_date(), a.oldest_date) AS data_age_days,
       CAST(t.tag_value AS INT) AS policy_days,
       CASE WHEN t.tag_value IS NULL THEN 'NO POLICY'
            WHEN datediff(current_date(), a.oldest_date) <= CAST(t.tag_value AS INT) THEN 'COMPLIANT'
            ELSE 'VIOLATION' END AS retention_status
FROM ({ages}) a
LEFT JOIN {I}.table_tags t ON t.schema_name = a.schema_name AND t.table_name = a.table_name
                          AND t.tag_name = 'retention_days'"""
    violations_sql = f"""
SELECT schema_name, table_name, policy_check
FROM (SELECT schema_name, table_name,
             stack(5, 'missing data owner', data_owner IS NULL,
                      'missing data steward', data_steward IS NULL,
                      'missing classification', classification IS NULL,
                      'missing table description', comment IS NULL OR comment = '',
                      'missing retention policy', layer IN ('bronze', 'silver', 'gold') AND retention_days IS NULL)
               AS (policy_check, violated)
      FROM ({base}))
WHERE violated"""

    # ---------------------------------------------------------------- page 1 datasets
    own_kpi = d.dataset("own_kpi", "Ownership KPIs", f"""
SELECT COUNT(*) AS datasets,
       COUNT_IF(data_owner IS NULL OR data_steward IS NULL) AS without_owner_or_steward,
       COUNT(DISTINCT data_owner) AS distinct_owners,
       COUNT(DISTINCT data_steward) AS distinct_stewards
FROM ({base})""")
    own_tbl = d.dataset("own_tbl", "Dataset ownership", f"""
SELECT schema_name, table_name, layer, source, data_owner, data_steward, uc_owner, classification,
       IF(data_owner IS NULL, 'MISSING OWNER', IF(data_steward IS NULL, 'MISSING STEWARD', 'OK')) AS ownership_status
FROM ({base})""")
    meta_kpi = d.dataset("meta_kpi", "Metadata KPIs", f"""
SELECT (SELECT COUNT(*) FROM ({base}))                                                     AS registered_tables,
       (SELECT COUNT_IF(comment IS NULL OR comment = '') FROM ({base}))                    AS tables_without_description,
       (SELECT ROUND(100.0 * COUNT_IF(comment IS NOT NULL AND comment <> '') / COUNT(*), 1)
          FROM {I}.columns WHERE table_schema IN ({MAIN}))                                 AS pct_columns_described,
       (SELECT COUNT_IF(comment IS NULL OR comment = '')
          FROM {I}.columns WHERE table_schema IN ({MAIN}))                                 AS columns_without_description,
       (SELECT ROUND(100.0 * COUNT_IF(classification IS NOT NULL AND data_owner IS NOT NULL) / COUNT(*), 1)
          FROM ({base}))                                                                   AS pct_catalog_coverage""")
    meta_missing = d.dataset("meta_missing", "Missing descriptions", f"""
SELECT table_schema AS schema_name, table_name, column_name, data_type
FROM {I}.columns WHERE table_schema IN ({MAIN}) AND (comment IS NULL OR comment = '')""")
    meta_cov = d.dataset("meta_cov", "Column documentation coverage", f"""
SELECT table_schema AS schema_name, table_name, COUNT(*) AS columns,
       COUNT_IF(comment IS NOT NULL AND comment <> '') AS documented,
       ROUND(100.0 * COUNT_IF(comment IS NOT NULL AND comment <> '') / COUNT(*), 1) AS pct_documented
FROM {I}.columns WHERE table_schema IN ({MAIN}) GROUP BY table_schema, table_name""")
    meta_changes = d.dataset("meta_changes", "Schema changes", f"""
SELECT detected_at, source_name, entity_id, change_type, field_name, expected_type, actual_type, details
FROM {A}.schema_change_log""")
    meta_by = d.dataset("meta_by", "Schema changes by type", f"""
SELECT source_name, change_type, COUNT(*) AS changes FROM {A}.schema_change_log GROUP BY source_name, change_type""")
    lin_declared = d.dataset("lin_declared", "Declared lineage", f"""
SELECT source_name, hop,
       CASE hop WHEN 1 THEN 'Source API → Bronze' WHEN 2 THEN 'Bronze → Silver' ELSE 'Silver → Gold' END AS step,
       from_node, to_node
FROM ({edges})""")
    lin_deps = d.dataset("lin_deps", "Upstream / downstream dependencies", f"""
SELECT dataset, COUNT_IF(direction = 'upstream') AS upstream, COUNT_IF(direction = 'downstream') AS downstream
FROM (SELECT to_node AS dataset, 'upstream' AS direction FROM ({edges})
      UNION ALL SELECT from_node, 'downstream' FROM ({edges}))
WHERE dataset LIKE '{CATALOG}.%' GROUP BY dataset""")
    lin_observed = d.dataset("lin_observed", "Lineage observed by Unity Catalog (30 days)", f"""
SELECT COALESCE(source_table_full_name, concat('(', COALESCE(entity_type, 'external'), ' write)')) AS source,
       target_table_full_name AS target, entity_type, COUNT(*) AS events, MAX(event_time) AS last_seen
FROM system.access.table_lineage
WHERE event_date >= current_date() - INTERVAL 30 DAYS AND ({like.format(col='target_table_full_name')})
GROUP BY ALL""")

    # ---------------------------------------------------------------- page 2 datasets
    sec_kpi = d.dataset("sec_kpi", "Security KPIs (7 days)", f"""
SELECT (SELECT COUNT(*) FROM system.access.audit WHERE {governed_events})                          AS access_events,
       (SELECT COUNT(DISTINCT user_identity.email) FROM system.access.audit WHERE {governed_events}) AS principals,
       (SELECT COUNT(*) FROM system.access.audit WHERE event_date >= current_date() - INTERVAL 7 DAYS
          AND response.status_code IN (401, 403))                                                  AS unauthorized_attempts,
       (SELECT COUNT(*) FROM system.access.audit WHERE event_date >= current_date() - INTERVAL 7 DAYS
          AND response.status_code >= 400)                                                         AS failed_requests,
       (SELECT COUNT(*) FROM {I}.table_privileges WHERE table_schema IN ({MAIN}))
         + (SELECT COUNT(*) FROM {I}.schema_privileges WHERE schema_name IN ({MAIN}))              AS explicit_grants,
       (SELECT COUNT(*) FROM ({base}) WHERE classification IN ('confidential', 'restricted'))      AS sensitive_datasets""")
    sec_users = d.dataset("sec_users", "Principals accessing governed assets", f"""
SELECT user_identity.email AS principal, action_name, COUNT(*) AS events, MAX(event_time) AS last_seen
FROM system.access.audit WHERE {governed_events} GROUP BY ALL""")
    sec_failed = d.dataset("sec_failed", "Failed requests (workspace-wide, 7 days)", """
SELECT event_time, user_identity.email AS principal, service_name, action_name,
       response.status_code AS status_code, left(response.error_message, 140) AS error
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 7 DAYS AND response.status_code >= 400
ORDER BY event_time DESC LIMIT 50""")
    sec_class = d.dataset("sec_class", "Datasets by classification", f"""
SELECT schema_name, COALESCE(classification, 'unclassified') AS classification, COUNT(*) AS tables
FROM ({base}) GROUP BY ALL""")
    sec_grants = d.dataset("sec_grants", "Explicit privileges", f"""
SELECT schema_name, 'schema' AS object_type, schema_name AS object_name, grantee, privilege_type, inherited_from
FROM {I}.schema_privileges WHERE schema_name IN ({MAIN})
UNION ALL
SELECT table_schema, 'table', concat(table_schema, '.', table_name), grantee, privilege_type, inherited_from
FROM {I}.table_privileges WHERE table_schema IN ({MAIN})""")
    comp_kpi = d.dataset("comp_kpi", "Compliance KPIs", f"""
SELECT (SELECT COUNT(*) FROM ({violations_sql})) AS policy_violations,
       (SELECT COUNT_IF(retention_status = 'VIOLATION') FROM ({retention_sql})) AS retention_violations,
       (SELECT COUNT_IF(retention_status = 'NO POLICY') FROM ({retention_sql})) AS datasets_without_policy,
       (SELECT ROUND(100.0 * COUNT_IF(classification IS NOT NULL) / COUNT(*), 1) FROM ({base})) AS pct_classified""")
    comp_ret = d.dataset("comp_ret", "Retention compliance", retention_sql)
    comp_viol = d.dataset("comp_viol", "Policy violations", violations_sql)
    aud_kpi = d.dataset("aud_kpi", "Audit KPIs (7 days)", f"""
SELECT (SELECT COUNT(*) FROM {A}.run_log WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
          AND status <> 'RUNNING')                                                          AS pipeline_runs,
       (SELECT COUNT(*) FROM {A}.run_log WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
          AND status IN ('FAILED', 'PARTIAL_SUCCESS'))                                      AS failed_or_partial_runs,
       (SELECT COUNT(DISTINCT run_id) FROM system.lakeflow.job_run_timeline
         WHERE period_start_time >= current_timestamp() - INTERVAL 7 DAYS
           AND result_state IN ('FAILED', 'ERROR', 'TIMED_OUT'))                            AS failed_job_runs,
       (SELECT COUNT(*) FROM system.access.audit WHERE {governed_events}
          AND (action_name LIKE 'update%' OR action_name LIKE 'create%' OR action_name LIKE 'delete%'))
                                                                                            AS governed_changes""")
    aud_runs = d.dataset("aud_runs", "Pipeline execution history", f"""
SELECT start_time, source_name, status, duration_seconds, job_success, job_failed, bronze_rows,
       silver_rows_inserted, silver_rows_updated, quarantine_rows, test_mode, error_message
FROM {A}.run_log WHERE status <> 'RUNNING'""")
    aud_changes = d.dataset("aud_changes", "Changes to governed assets", f"""
SELECT event_date, action_name, COUNT(*) AS events
FROM system.access.audit
WHERE {governed_events} AND (action_name LIKE 'update%' OR action_name LIKE 'create%' OR action_name LIKE 'delete%')
GROUP BY event_date, action_name""")
    aud_jobs = d.dataset("aud_jobs", "Job runs by result (30 days)", """
SELECT result_state, COUNT(DISTINCT run_id) AS job_runs
FROM system.lakeflow.job_run_timeline
WHERE period_start_time >= current_timestamp() - INTERVAL 30 DAYS AND result_state IS NOT NULL
GROUP BY result_state""")
    aud_access = d.dataset("aud_access", "Access log (7 days)", f"""
SELECT event_date, action_name, COUNT(*) AS events
FROM system.access.audit WHERE {governed_events} GROUP BY event_date, action_name""")

    # ---------------------------------------------------------------- page 3 datasets
    dq_kpi = d.dataset("dq_kpi", "Data quality KPIs (30 days)", f"""
SELECT COALESCE(ROUND(100.0 * SUM(r.silver_valid_rows) / NULLIF(SUM(r.source_rows), 0), 2), 100) AS quality_score,
       COALESCE(ROUND(100.0 * SUM(r.quarantine_rows) / NULLIF(SUM(r.source_rows), 0), 2), 0)     AS fail_pct,
       COALESCE(SUM(r.source_rows), 0)                                                          AS source_rows,
       COALESCE(SUM(r.quarantine_rows), 0)                                                      AS rejected_records,
       (SELECT COUNT_IF(exists(dq_errors, e -> e LIKE 'NULL%')) FROM {Q}.data_quarantine
         WHERE quarantined_at >= current_timestamp() - INTERVAL 30 DAYS)                         AS null_value_rows,
       (SELECT COUNT_IF(exists(dq_errors, e -> e LIKE 'DUPLICATE%')) FROM {Q}.data_quarantine
         WHERE quarantined_at >= current_timestamp() - INTERVAL 30 DAYS)                         AS duplicate_rows,
       (SELECT COUNT_IF(NOT exists(dq_errors, e -> e LIKE 'NULL%' OR e LIKE 'DUPLICATE%'))
          FROM {Q}.data_quarantine WHERE quarantined_at >= current_timestamp() - INTERVAL 30 DAYS) AS invalid_rows
FROM {A}.run_log r
WHERE r.start_time >= current_timestamp() - INTERVAL 30 DAYS AND r.status <> 'RUNNING'""")
    dq_trend = d.dataset("dq_trend", "Quality score per run", f"""
SELECT source_name, start_time,
       COALESCE(ROUND(100.0 * silver_valid_rows / NULLIF(source_rows, 0), 2), 100) AS quality_score
FROM {A}.run_log WHERE status <> 'RUNNING'""")
    dq_source = d.dataset("dq_source", "Quality by source (30 days)", f"""
SELECT source_name, SUM(source_rows) AS source_rows, SUM(silver_valid_rows) AS valid_rows,
       SUM(quarantine_rows) AS rejected_rows,
       ROUND(100.0 * SUM(silver_valid_rows) / NULLIF(SUM(source_rows), 0), 2) AS pass_pct,
       ROUND(100.0 * SUM(quarantine_rows) / NULLIF(SUM(source_rows), 0), 2) AS fail_pct
FROM {A}.run_log WHERE start_time >= current_timestamp() - INTERVAL 30 DAYS AND status <> 'RUNNING'
GROUP BY source_name""")
    fr_kpi = d.dataset("fr_kpi", "Freshness KPIs", f"""
WITH last_ok AS (SELECT source_name, MAX(start_time) AS last_ok_time FROM {A}.run_log
                 WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS') GROUP BY source_name)
SELECT COALESCE(ROUND(MAX(timestampdiff(MINUTE, last_ok_time, current_timestamp())) / 60.0, 1), 0)
         AS hours_since_last_success_worst,
       COUNT_IF(timestampdiff(HOUR, last_ok_time, current_timestamp()) > {fresh_h}) AS stale_sources,
       (SELECT COUNT(*) FROM {A}.run_log WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
          AND status <> 'RUNNING' AND (status = 'FAILED' OR duration_seconds > {max_s})) AS sla_breaches_7d
FROM last_ok""")
    fr_datasets = d.dataset("fr_datasets", "Dataset freshness", f"""
SELECT ds.source_name, ds.dataset, l.last_successful_load,
       ROUND(timestampdiff(MINUTE, l.last_successful_load, current_timestamp()) / 60.0, 1) AS hours_since_load,
       IF(timestampdiff(HOUR, l.last_successful_load, current_timestamp()) <= {fresh_h}, 'FRESH', 'STALE')
         AS freshness_status
FROM ({dataset_rows}) ds
LEFT JOIN (SELECT source_name, MAX(start_time) AS last_successful_load FROM {A}.run_log
           WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS') GROUP BY source_name) l USING (source_name)""")
    fr_entities = d.dataset("fr_entities", "Last successful ingestion per entity", f"""
SELECT r.source_name, d.entity_name, MAX(d.end_time) AS last_successful_ingestion,
       ROUND(timestampdiff(MINUTE, MAX(d.end_time), current_timestamp()) / 60.0, 1) AS hours_since_ingestion,
       IF(timestampdiff(HOUR, MAX(d.end_time), current_timestamp()) <= {fresh_h}, 'OK', 'STALE') AS freshness_status
FROM {A}.run_log_dtl d JOIN {A}.run_log r USING (run_id)
WHERE d.status = 'SUCCESS' GROUP BY r.source_name, d.entity_name""")
    fr_sla = d.dataset("fr_sla", "SLA breaches", f"""
SELECT start_time, source_name, status, duration_seconds, {max_s} AS target_seconds, error_message
FROM {A}.run_log
WHERE status <> 'RUNNING' AND (status = 'FAILED' OR duration_seconds > {max_s})""")
    replayed = f"""(SELECT DISTINCT get_json_object(parameters, '$.replay_run_id') AS replayed_run_id
                    FROM {A}.run_log_dtl WHERE get_json_object(parameters, '$.replay_run_id') IS NOT NULL)"""
    qm_kpi = d.dataset("qm_kpi", "Quarantine KPIs", f"""
SELECT COUNT(*) AS rejected_records,
       COUNT_IF(q.rejection_level = 'ROW') AS row_level,
       COUNT_IF(q.rejection_level = 'PAYLOAD') AS payload_level,
       COUNT(DISTINCT q.run_id) AS affected_runs,
       COUNT_IF(rp.replayed_run_id IS NULL) AS pending_reprocessing
FROM {Q}.data_quarantine q LEFT JOIN {replayed} rp ON rp.replayed_run_id = q.run_id""")
    qm_reasons = d.dataset("qm_reasons", "Rejection reasons", f"""
SELECT source_name, reason, rejection_level, COUNT(*) AS rows_quarantined
FROM {Q}.data_quarantine LATERAL VIEW explode(dq_errors) x AS reason GROUP BY source_name, reason, rejection_level""")
    qm_recent = d.dataset("qm_recent", "Quarantined records", f"""
SELECT q.quarantined_at, q.source_name, q.entity_id, q.rejection_level, array_join(q.dq_errors, ', ') AS reasons,
       q.error_details, CAST(q.raw_values AS STRING) AS raw_values,
       IF(rp.replayed_run_id IS NULL, 'PENDING', 'REPLAYED') AS reprocessing_status
FROM {Q}.data_quarantine q LEFT JOIN {replayed} rp ON rp.replayed_run_id = q.run_id
ORDER BY q.quarantined_at DESC LIMIT 100""")

    # ================================================================ layout (6-column authoring grid)
    def section(cursor: list[int], title: str) -> int:
        y = cursor[0]
        d.text(f"## {title}", (0, y, 6, 1))
        cursor[0] += 1
        return cursor[0]

    # ---- page 1 · ownership, metadata & lineage
    d.page("Ownership, metadata & lineage")
    d.text("# Data governance\nOwnership, metadata and lineage for the `met_*` schemas (real data; `_edge` demo copies "
           "are excluded). Tags and descriptions are applied from the source YAMLs by the setup task.", (0, 0, 6, 2))
    d.multi_filter("schema_name", [own_tbl, meta_missing, meta_cov], "Schema", (0, 2, 2, 1))
    cur = [3]
    y = section(cur, "1 · Data ownership")
    d.counter(own_kpi, "MAX(`datasets`)", "Registered datasets", (0, y, 2, 2))
    d.counter(own_kpi, "MAX(`without_owner_or_steward`)", "Without owner or steward", (2, y, 2, 2))
    d.counter(own_kpi, "MAX(`distinct_owners`)", "Distinct owners", (4, y, 2, 2))
    d.table(own_tbl, [("schema_name", "string"), ("table_name", "string"), ("layer", "string"), ("source", "string"),
                      ("data_owner", "string"), ("data_steward", "string"), ("uc_owner", "string"),
                      ("classification", "string"), ("ownership_status", "string")],
            "Dataset ownership", (0, y + 2, 6, 6))
    cur[0] = y + 8
    y = section(cur, "2 · Metadata management")
    d.counter(meta_kpi, "MAX(`registered_tables`)", "Registered tables", (0, y, 1, 2))
    d.counter(meta_kpi, "MAX(`pct_columns_described`)", "% columns described", (1, y, 1, 2))
    d.counter(meta_kpi, "MAX(`columns_without_description`)", "Columns missing description", (2, y, 2, 2))
    d.counter(meta_kpi, "MAX(`tables_without_description`)", "Tables missing description", (4, y, 1, 2))
    d.counter(meta_kpi, "MAX(`pct_catalog_coverage`)", "% catalog coverage", (5, y, 1, 2))
    d.table(meta_cov, [("schema_name", "string"), ("table_name", "string"), ("columns", "number"),
                       ("documented", "number"), ("pct_documented", "number")],
            "Column documentation coverage", (0, y + 2, 3, 6))
    d.table(meta_missing, [("schema_name", "string"), ("table_name", "string"), ("column_name", "string"),
                           ("data_type", "string")], "Columns missing a description", (3, y + 2, 3, 6))
    d.chart("bar", meta_by, "change_type", "changes", "Schema changes by type", (0, y + 8, 2, 5),
            color="source_name", x_type="categorical", x_title="Change", y_title="changes")
    d.table(meta_changes, [("detected_at", "datetime"), ("source_name", "string"), ("entity_id", "string"),
                           ("change_type", "string"), ("field_name", "string"), ("expected_type", "string"),
                           ("actual_type", "string")], "Schema change log", (2, y + 8, 4, 5))
    cur[0] = y + 13
    y = section(cur, "3 · Data lineage")
    d.text("Source API → Bronze → Silver → Gold is **declared** from the source YAMLs: the raw → silver hop is a "
           "Python parsing step, which Unity Catalog lineage cannot see. Silver → Gold and job writes are "
           "**observed**.", (0, y, 6, 2))
    d.table(lin_declared, [("source_name", "string"), ("step", "string"), ("from_node", "string"),
                           ("to_node", "string")], "Declared lineage chain", (0, y + 2, 6, 5))
    d.table(lin_deps, [("dataset", "string"), ("upstream", "number"), ("downstream", "number")],
            "Upstream / downstream dependencies", (0, y + 7, 3, 5))
    d.table(lin_observed, [("source", "string"), ("target", "string"), ("entity_type", "string"),
                           ("events", "number"), ("last_seen", "datetime")],
            "Observed in Unity Catalog (30 days)", (3, y + 7, 3, 5))

    # ---- page 2 · security, compliance & audit
    d.page("Security, compliance & audit")
    d.text("# Security, compliance & audit\nAccess from `system.access.audit`, policy checks from Unity Catalog "
           "tags and descriptions, execution history from `met_audit` and Databricks job tables.", (0, 0, 6, 2))
    cur = [2]
    y = section(cur, "4 · Security & access")
    d.counter(sec_kpi, "MAX(`access_events`)", "Access events (7d)", (0, y, 1, 2))
    d.counter(sec_kpi, "MAX(`principals`)", "Principals", (1, y, 1, 2))
    d.counter(sec_kpi, "MAX(`unauthorized_attempts`)", "Unauthorized (401/403)", (2, y, 1, 2))
    d.counter(sec_kpi, "MAX(`failed_requests`)", "Failed requests", (3, y, 1, 2))
    d.counter(sec_kpi, "MAX(`explicit_grants`)", "Explicit grants", (4, y, 1, 2))
    d.counter(sec_kpi, "MAX(`sensitive_datasets`)", "Sensitive datasets", (5, y, 1, 2))
    d.table(sec_users, [("principal", "string"), ("action_name", "string"), ("events", "number"),
                        ("last_seen", "datetime")], "Who accessed governed assets (7 days)", (0, y + 2, 3, 6))
    d.chart("bar", sec_class, "schema_name", "tables", "Datasets by classification", (3, y + 2, 3, 6),
            color="classification", x_type="categorical", x_title="Schema", y_title="tables")
    d.table(sec_failed, [("event_time", "datetime"), ("principal", "string"), ("service_name", "string"),
                         ("action_name", "string"), ("status_code", "number"), ("error", "string")],
            "Failed requests, workspace-wide (7 days)", (0, y + 8, 4, 6))
    d.table(sec_grants, [("object_type", "string"), ("object_name", "string"), ("grantee", "string"),
                         ("privilege_type", "string"), ("inherited_from", "string")],
            "Explicit privileges on governed assets", (4, y + 8, 2, 6))
    cur[0] = y + 14
    y = section(cur, "5 · Compliance")
    d.counter(comp_kpi, "MAX(`policy_violations`)", "Policy violations", (0, y, 1, 2))
    d.counter(comp_kpi, "MAX(`retention_violations`)", "Retention violations", (1, y, 2, 2))
    d.counter(comp_kpi, "MAX(`datasets_without_policy`)", "Datasets without retention policy", (3, y, 2, 2))
    d.counter(comp_kpi, "MAX(`pct_classified`)", "% classified", (5, y, 1, 2))
    d.table(comp_ret, [("schema_name", "string"), ("table_name", "string"), ("oldest_date", "datetime"),
                       ("data_age_days", "number"), ("policy_days", "number"), ("retention_status", "string")],
            "Retention policy compliance (data age vs policy)", (0, y + 2, 3, 6))
    d.table(comp_viol, [("schema_name", "string"), ("table_name", "string"), ("policy_check", "string")],
            "Policy violations", (3, y + 2, 3, 6))
    cur[0] = y + 8
    y = section(cur, "6 · Audit & monitoring")
    d.counter(aud_kpi, "MAX(`pipeline_runs`)", "Pipeline runs (7d)", (0, y, 1, 2))
    d.counter(aud_kpi, "MAX(`failed_or_partial_runs`)", "Failed / partial runs", (1, y, 2, 2))
    d.counter(aud_kpi, "MAX(`failed_job_runs`)", "Failed job runs", (3, y, 1, 2))
    d.counter(aud_kpi, "MAX(`governed_changes`)", "Changes to governed assets", (4, y, 2, 2))
    d.table(aud_runs, [("start_time", "datetime"), ("source_name", "string"), ("status", "string"),
                       ("duration_seconds", "number"), ("job_success", "number"), ("job_failed", "number"),
                       ("quarantine_rows", "number"), ("test_mode", "string"), ("error_message", "string")],
            "Pipeline execution history", (0, y + 2, 6, 6))
    d.chart("bar", aud_changes, "event_date", "events", "Changes to governed assets per day", (0, y + 8, 3, 5),
            color="action_name", x_title="Date", y_title="events")
    d.chart("bar", aud_jobs, "result_state", "job_runs", "Job runs by result (30 days)", (3, y + 8, 3, 5),
            x_type="categorical", x_title="Result", y_title="runs")
    d.chart("bar", aud_access, "event_date", "events", "Access log per day (governed assets)", (0, y + 13, 6, 5),
            color="action_name", x_title="Date", y_title="events")

    # ---- page 3 · data health
    d.page("Data quality, freshness & quarantine")
    d.text("# Data health\nQuality, freshness and rejected records from `met_audit` and `met_quarantine`. "
           f"Freshness SLA: a successful load within **{fresh_h} h**; run SLA: **{max_s} s**, not FAILED.",
           (0, 0, 6, 2))
    cur = [2]
    y = section(cur, "7 · Data quality")
    d.counter(dq_kpi, "MAX(`quality_score`)", "Quality score % (30d)", (0, y, 1, 2))
    d.counter(dq_kpi, "MAX(`fail_pct`)", "Validation fail %", (1, y, 1, 2))
    d.counter(dq_kpi, "MAX(`null_value_rows`)", "Null values", (2, y, 1, 2))
    d.counter(dq_kpi, "MAX(`duplicate_rows`)", "Duplicates", (3, y, 1, 2))
    d.counter(dq_kpi, "MAX(`invalid_rows`)", "Invalid records", (4, y, 1, 2))
    d.counter(dq_kpi, "MAX(`source_rows`)", "Records checked", (5, y, 1, 2))
    d.chart("line", dq_trend, "start_time", "quality_score", "Quality score per run (%)", (0, y + 2, 4, 5),
            color="source_name", x_title="Run start (UTC)", y_title="% valid")
    d.table(dq_source, [("source_name", "string"), ("source_rows", "number"), ("valid_rows", "number"),
                        ("rejected_rows", "number"), ("pass_pct", "number"), ("fail_pct", "number")],
            "Validation pass / fail by source", (4, y + 2, 2, 5))
    cur[0] = y + 7
    y = section(cur, "8 · Data freshness")
    d.counter(fr_kpi, "MAX(`hours_since_last_success_worst`)", "Hours since last success (worst source)",
              (0, y, 2, 2))
    d.counter(fr_kpi, "MAX(`stale_sources`)", "Stale sources", (2, y, 2, 2))
    d.counter(fr_kpi, "MAX(`sla_breaches_7d`)", "SLA breaches (7d)", (4, y, 2, 2))
    d.table(fr_datasets, [("source_name", "string"), ("dataset", "string"), ("last_successful_load", "datetime"),
                          ("hours_since_load", "number"), ("freshness_status", "string")],
            "Dataset freshness (stale datasets flagged)", (0, y + 2, 6, 5))
    d.table(fr_entities, [("source_name", "string"), ("entity_name", "string"),
                          ("last_successful_ingestion", "datetime"), ("hours_since_ingestion", "number"),
                          ("freshness_status", "string")], "Last successful ingestion per entity",
            (0, y + 7, 3, 6))
    d.table(fr_sla, [("start_time", "datetime"), ("source_name", "string"), ("status", "string"),
                     ("duration_seconds", "number"), ("target_seconds", "number"), ("error_message", "string")],
            "SLA breaches", (3, y + 7, 3, 6))
    cur[0] = y + 13
    y = section(cur, "9 · Quarantine management")
    d.counter(qm_kpi, "MAX(`rejected_records`)", "Rejected records", (0, y, 1, 2))
    d.counter(qm_kpi, "MAX(`row_level`)", "Row-level", (1, y, 1, 2))
    d.counter(qm_kpi, "MAX(`payload_level`)", "Payload-level", (2, y, 1, 2))
    d.counter(qm_kpi, "MAX(`affected_runs`)", "Affected runs", (3, y, 1, 2))
    d.counter(qm_kpi, "MAX(`pending_reprocessing`)", "Pending reprocessing", (4, y, 2, 2))
    d.chart("bar", qm_reasons, "reason", "rows_quarantined", "Rejection reasons", (0, y + 2, 3, 5),
            color="source_name", x_type="categorical", x_title="Reason", y_title="rows")
    d.text("**Pending reprocessing** = quarantined rows whose run has not been replayed with `replay_run_id`. "
           "There is no other reprocessing workflow, so a row stays PENDING until a replay references its run.",
           (3, y + 2, 3, 5))
    d.table(qm_recent, [("quarantined_at", "datetime"), ("source_name", "string"), ("entity_id", "string"),
                        ("rejection_level", "string"), ("reasons", "string"), ("error_details", "string"),
                        ("raw_values", "string"), ("reprocessing_status", "string")],
            "Quarantined records (latest 100)", (0, y + 7, 6, 6))
    return d


BUILDERS = (business, monitoring, governance)


def remote_columns(dashboard: Dashboard) -> dict[str, set[str]]:
    """Run each dataset on the SQL warehouse (LIMIT 0) and return its output column names."""
    cli = os.environ.get("DBX") or shutil.which("databricks") or "databricks"
    profile, warehouse = os.environ.get("DBX_PROFILE", "Atul"), os.environ["DBX_WAREHOUSE"]
    cols = {}
    for ds in dashboard.datasets:
        body = json.dumps({"warehouse_id": warehouse, "wait_timeout": "50s",
                           "statement": f"SELECT * FROM ({ds['queryLines'][0]}) LIMIT 0"})
        out = subprocess.run([cli, "-p", profile, "api", "post", "/api/2.0/sql/statements", "--json", body],
                             capture_output=True, text=True, encoding="utf-8")
        result = json.loads(out.stdout)
        if result.get("status", {}).get("state") != "SUCCEEDED":
            raise RuntimeError(f"{dashboard.key}.{ds['name']} failed: {result.get('status')}")
        cols[ds["name"]] = {c["name"] for c in result["manifest"]["schema"]["columns"]}
    return cols


if __name__ == "__main__":
    dashboards = [b() for b in BUILDERS]
    if "--sql" in sys.argv:
        print(json.dumps({d.key: {ds["name"]: ds["queryLines"][0] for ds in d.datasets} for d in dashboards}))
        sys.exit(0)
    issues = []
    for d in dashboards:
        issues += d.problems(remote_columns(d) if "--validate" in sys.argv else None)
    if issues:
        print("\n".join(issues))
        sys.exit(1)
    for d in dashboards:
        widgets = sum(len(p["layout"]) for p in d.pages)
        print("wrote", d.write().relative_to(ROOT), f"({len(d.datasets)} datasets, {len(d.pages)} pages, {widgets} widgets)")
    # The same dataset queries, for ad-hoc use in the SQL editor.
    blocks = ["-- Generated by scripts/build_dashboards.py from the dashboard datasets. Do not edit by hand.\n"]
    for d in dashboards:
        blocks.append(f"\n-- {'=' * 86}\n-- Dashboard: {d.key}\n-- {'=' * 86}\n")
        blocks += [f"\n-- [{ds['name']}] {ds['displayName']}\n{ds['queryLines'][0].strip()};\n" for ds in d.datasets]
    (ROOT / "sql" / "dashboard_queries.sql").write_text("".join(blocks), encoding="utf-8")
    print("wrote sql/dashboard_queries.sql")
