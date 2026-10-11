# Runbook — config-driven API ingestion on Databricks


## 1. One-time local setup
```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements-dev.txt pyyaml
databricks auth profiles            # 'Atul' -> https://dbc-367bc80b-b0c3.cloud.databricks.com
databricks -p Atul current-user me  # confirms login (OAuth via the CLI)
```
Optional, for the local Spark check (Windows needs Python 3.10):
```powershell
py -3.10 -m venv .venv-spark
.\.venv-spark\Scripts\python -m pip install "pyspark==3.5.3" "delta-spark==3.2.1" requests pyyaml
```

## 2. Local verification (no workspace needed)
```powershell
.\.venv\Scripts\python -m pytest -q                       # expect: 206 passed (107 test functions, parametrized)
# optional end-to-end on local Spark + Delta, both sources, fixture mode:
Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue
$env:PYSPARK_PYTHON = "$PWD\.venv-spark\Scripts\python.exe"
.\.venv-spark\Scripts\python scripts\local_spark_smoke.py .local_spark   # expect: SMOKE TEST PASSED
```

## 3. Deploy to Databricks (job + 3 dashboards)
```powershell
$env:DBX_WAREHOUSE = "14fee02ae4063880"                    # databricks warehouses list -p Atul
.\.venv\Scripts\python scripts\build_dashboards.py --validate   # regenerate + validate dashboards
databricks bundle validate -t dev -p Atul
databricks bundle deploy   -t dev -p Atul
databricks bundle summary  -t dev -p Atul                   # job + dashboard URLs
```

## 4. Normal runs (live API, main schemas)
```powershell
databricks bundle run weather_pipeline_job -t dev -p Atul --params source=open_meteo_weather
databricks bundle run weather_pipeline_job -t dev -p Atul --params source=usgs_earthquakes
```
Each run executes setup → run_pipeline → validation:
- **setup:** creates the schemas, tables and landing volume, and applies tags and column comments.
- **run_pipeline:** API → landing files + bronze → silver MERGE → gold MERGE, with audits.
- **validation:** checks that must return zero rows; the task fails if any check fails.

Expected:
- **Weather:** SUCCESS, 5 landing files, 360 silver rows, 15 gold rows.
- **USGS:** SUCCESS, 16 pages, about 134 events, about 33 gold rows. Counts vary with the live data.

## 5. Same-window rerun (idempotency proof)
Run step 4 again for the same source, or run `notebooks/03_rerun_proof.py` in the workspace.

Expected:
- bronze grows by the rerun's pages
- silver and gold key counts are unchanged
- `silver_rows_inserted` = 0; updates happen only where the forecast changed

## 6. Deliberate failure + edge cases (demo)
```powershell
# real failure: London latitude=999 -> HTTP 400 -> PARTIAL_SUCCESS (main schema)
databricks bundle run weather_pipeline_job -t dev -p Atul --params "source=open_meteo_weather,test_mode=bad_request"
# fixture edge cases, kept in separate schemas
databricks bundle run weather_pipeline_job -t dev -p Atul --params "source=open_meteo_weather,schema_suffix=_edge,source_mode=fixture,test_mode=invalid_humidity"
databricks bundle run weather_pipeline_job -t dev -p Atul --params "source=open_meteo_weather,schema_suffix=_edge,source_mode=fixture,test_mode=missing_field"
databricks bundle run weather_pipeline_job -t dev -p Atul --params "source=open_meteo_weather,schema_suffix=_edge,source_mode=fixture,test_mode=timeout"
databricks bundle run weather_pipeline_job -t dev -p Atul --params "source=usgs_earthquakes,schema_suffix=_edge,source_mode=fixture,test_mode=invalid_magnitude"
```
Other weather test modes:
- `unreachable_url`, `server_error`, `rate_limit`
- `bad_temperature_type`, `numeric_string`, `null_values`, `invalid_timestamp`
- `duplicate_record`, `new_field`, `unequal_arrays`, `empty_response`, `truncated_response`

The evidence shows up on the Pipeline monitoring dashboard: the "Edge-case runs" page, plus the bad_request run
on "Pipeline health".

## 7. Inspect results
- **Dashboards** (dev prefix `[dev atulpanthriinbox]`):
  - [Business insights](https://dbc-367bc80b-b0c3.cloud.databricks.com/dashboardsv3/01f1c3a763e6120386df84ff0202cf62/published?w=7474650939745946)
  - [Pipeline monitoring](https://dbc-367bc80b-b0c3.cloud.databricks.com/dashboardsv3/01f1c3a7643715bba7aeececf1756b40/published?w=7474650939745946)
  - [Data governance](https://dbc-367bc80b-b0c3.cloud.databricks.com/dashboardsv3/01f1c3a763f91075a3811843c1fdf6f7/published?w=7474650939745946)
  - Use "Reset all to default" to clear chart-click filters.
- **SQL editor:**
  ```sql
  SELECT * FROM workspace.met_audit.run_log            ORDER BY start_time DESC;
  SELECT * FROM workspace.met_audit.run_log_dtl      ORDER BY start_time DESC;
  SELECT * FROM workspace.met_audit.schema_change_log  ORDER BY detected_at DESC;
  SELECT * FROM workspace.met_quarantine.data_quarantine ORDER BY quarantined_at DESC;
  SELECT entity_id, extraction_type, request_page, raw_file_path FROM workspace.met_raw.weather_raw ORDER BY ingestion_timestamp DESC;
  LIST '/Volumes/workspace/met_raw/raw_landing/source=open_meteo_weather/';
  ```
- **More queries:** `sql/dashboard_queries.sql` has ad-hoc versions of the dashboard queries.

## 8. Onboard a new API (configuration only)
See `docs/onboarding.md`:
1. Write `sources/<name>.yml`.
2. Save fixtures to `tests/fixtures/<name>/`.
3. Write `sql/gold/<name>.sql`.
4. Run `pytest`. The onboarding tests discover the new source automatically.
5. Run `bundle deploy`, then `bundle run ... --params source=<name>`.

For API keys, use `databricks secrets put-secret <scope> <key> -p Atul` and reference the key in `request.auth`.

## 9. Change dashboards
1. Edit `scripts/build_dashboards.py`, not the JSON.
2. Run `build_dashboards.py --validate`.
3. Run `pytest tests/test_dashboards.py`.
4. Run `bundle deploy`.

## 10. 20-minute demo order
| Time | Content |
|---|---|
| 0–3 min | slides: architecture, layers, framework, AI use (`docs/ai_use_log.md`) |
| 3–7 min | code walkthrough: `api_client.py`, `transforms.py`, `data_quality.py`, `persistence.merge_silver`, `pipeline.py` |
| 7–10 min | live weather run (step 4) |
| 10–15 min | bad_request + 3 edge cases (step 6), shown on the monitoring dashboard |
| 15–17 min | Business insights + Governance dashboards |
| 17–20 min | production: schedules, envs, DAB, CI, UC governance, alerts, scaling (README §13) |

## Verification
- Step 2 passes locally: pytest 206 test cases, smoke test PASSED.
- Steps 4–6:
  - every job run ends TERMINATED SUCCESS, with the validation task green
  - the run audit shows the expected SUCCESS / PARTIAL_SUCCESS statuses
  - the dashboards render across the full width
