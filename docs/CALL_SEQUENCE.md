# Call sequence of one pipeline run

`databricks bundle run weather_pipeline_job -t dev -p Atul --params source=open_meteo_weather`

## 0 · The command
| Piece | Purpose |
|---|---|
| `databricks bundle run` | start a job defined in the bundle (it does **not** deploy; `bundle deploy` uploads code first) |
| `weather_pipeline_job` | resource key in `resources/weather_job.yml`; resolves to the deployed job `[dev atulpanthriinbox] api_ingest_job` |
| `-t dev` | bundle target (`databricks.yml`); dev mode prefixes names |
| `-p Atul` | CLI profile in `~/.databrickscfg` (workspace host + OAuth login) |
| `--params source=open_meteo_weather` | overrides the job parameter `source`; the other parameters keep their defaults (`catalog=workspace`, `schema_suffix=""`, `source_mode=live`, `test_mode=none`, `fault_entity_id=""`, `replay_run_id=""`, `window=""`) |

The CLI calls the Jobs API (run-now with job parameters) and polls until the run ends.

## 1 · Job DAG (serverless, `max_concurrent_runs: 1`)
```
setup → run_pipeline → validation → refresh_meteorological_dashboard
                                  → refresh_monitoring_dashboard        (run_if ALL_DONE)
                                  → refresh_governance_dashboard
```
Typical durations: setup ~43 s, run_pipeline ~45 s, validation ~31 s.

## 2 · Task `setup` — `notebooks/00_setup.py`
| # | Call | Purpose / usage |
|---|---|---|
| 1 | `%run ./_bootstrap` | find repo root, add `src/` to `sys.path`, set Spark time zone to UTC, define `widget()`, `get_config()`, `common_widgets()` |
| 2 | `common_widgets()` | declare widgets `source`, `catalog`, `schema_suffix`; job parameters fill them |
| 3 | `get_config()` → `load_source()` | read `framework.yml` (`load_framework`) and `sources/open_meteo_weather.yml`, apply catalog / suffix overrides |
| 4 | `build_source()` → `_parse()` → `validate_source()` | build the typed `SourceConfig`; fail fast with every problem listed (`ConfigError`) before any API call |
| 5 | `persistence.create_all()` | `CREATE SCHEMA IF NOT EXISTS` for the 5 layer schemas, then `create_table()` for `run_log`, `run_log_dtl`, `schema_change_log`, `data_quarantine`, bronze, silver (DDL from `tables.table_ddl`: partitions, generated columns), then `create_gold_table()` (schema from the gold SQL + lineage columns; upgrades older tables) |
| 6 | `create_landing_volume()` | `CREATE VOLUME IF NOT EXISTS met_raw.raw_landing` |
| 7 | `apply_governance()` | UC tags (layer, source, owner, steward, classification, retention_days), column / table / schema comments; changes only differences |

## 3 · Task `run_pipeline` — `notebooks/01_run_pipeline.py`
Same bootstrap / `get_config()` / `create_all()` (idempotent), then `run_pipeline()` in `pipeline.py`:

| # | Call | Purpose / writes |
|---|---|---|
| 1 | `normalize_test_mode()`, `resolve_target()`, `resolve_window()` | validate fault mode (none here), pick fault target, resolve window (`forecast_days=3`) |
| 2 | `persistence.start_run_log()` | append status `RUNNING` row to `met_audit.run_log` (run_id = new UUID) |
| 3 | `requests.Session()` → `build_session()` | HTTP session (live); wrapped by the fault injector only if `test_mode` is set |
| 4 | `ingest_entities()` | per city, isolated: `get_extractor()` → `ApiExtractor.extract()` |
| 4a | `api_client.fetch_entity()` | `build_request()` (render `{entity.*}`/`{window.*}`, auth), `first_page_request()`, loop |
| 4b | `fetch_once()` | one request with retries: timeout / connection / 429 / 5xx retried, `backoff_delay`, `parse_retry_after`; 400 fails at once |
| 4c | `next_request()` | pagination (none for weather; offset for USGS), capped by `max_pages` |
| 5 | `persistence.write_landing_files()` | write each raw body to `/Volumes/workspace/met_raw/raw_landing/source=…/ingest_date=…/run_id=…/<city>.json` (write-once) |
| 6 | `persistence.append_rows(BRONZE)` | append raw responses + metadata to `met_raw.weather_raw` |
| 7 | `persistence.read_bronze_run()` | read this run's bronze rows back (source of truth, also enables replay) |
| 8 | `process_bronze()` → `process_entity_records()` | per city: `parse_payload` → `check_payload_schema` (contract, drift events) → `utc_offset` → `extract_rows` (array-length check, flatten) → `validate_rows` / `validate_row` / `cast_value` (types, ranges, duplicates) |
| 9 | `append_rows(SCHEMA_CHANGE_LOG)`, `append_rows(QUARANTINE)` | schema events; rejected rows / payloads with reason codes |
| 10 | `silver_source_rows()` → `merge_silver()` | typed rows → dedupe (`row_number`) → temp view → `MERGE INTO met_silver.weather_hourly` on (location_id, timestamp_utc); counts from the MERGE result |
| 11 | `merge_gold()` | affected (location, date) keys → `gold_select_sql()` → `gold_merge_sql()` → `MERGE INTO met_gold.weather_daily` (created_* kept, last_updated_* on change) |
| 12 | `finally`: `overall_status()`, `run_log_dtl_rows()` → `append_rows(RUN_LOG_DTL)`, `finalize_run_log()` | SUCCESS / PARTIAL_SUCCESS / FAILED; one detail row per city; MERGE final counts into `run_log` |
| 13 | notebook tail | print summary, display audit rows, raise only if the run is FAILED |

## 4 · Task `validation` — `notebooks/02_validation.py`
`get_config()` → build checks from the YAML (duplicate silver / gold keys, null required values, min/max rule
violations, gold rows without `created_run_id`, silver keys missing from gold, run_log vs detail row count) →
each must return 0 rows; the task fails otherwise. Then displays counts and audit tables.

## 5 · Tasks `refresh_*_dashboard` (`dashboard_task`)
Re-run the published dashboards' datasets on the SQL warehouse so cached results show this run. `run_if: ALL_DONE`
means they run even if the pipeline failed.

## 6 · What one run leaves behind
- 5 landing files + 5 bronze rows (one per city)
- silver / gold rows inserted or updated only where values changed
- 1 `run_log` row, 5 `run_log_dtl` rows, plus any `schema_change_log` / `data_quarantine` rows
- three dashboards refreshed
