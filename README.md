# Config-driven API ingestion on Databricks (Open-Meteo weather + USGS earthquakes)

> Confidential take-home. Do not push to a public repository.

## 1. Problem
The assignment: pull 3–7 days of hourly weather (temperature, relative humidity, precipitation, wind speed) for
≥5 configured locations from `https://api.open-meteo.com/v1/forecast`. Persist it through Delta bronze, silver
and gold layers without duplicates on rerun, and make failures, data quality and schema changes visible through
audit tables and a dashboard.

The solution is a small **configuration-driven framework**. The weather pipeline is one YAML file, and a second
API (USGS earthquakes: records response, offset pagination) was onboarded **with configuration only**, as a proof.

## 2. Architecture
```text
sources/<name>.yml ──► validate config + window ──► RUNNING row in met_audit.run_log
                              │
           Extractor per extraction.type: API (implemented: templated request, API-key auth from
           secret, bounded retries, pagination) · FILE · DB (planned); one entity at a time, isolated
                              ▼
source ──► landing files  /Volumes/<cat>/met_raw/raw_landing/source=…/ingest_date=…/run_id=…/<entity>[_pN].json
       └─► met_raw.<bronze_table>  (append; raw data as received + extraction_type, source_uri, raw_file_path)
                              │  read back this run_id (or replay_run_id)
                              ▼
     contract check → (columnar: array-length check) → extract → controlled casts → DQ rules (from YAML)
            │                         │                                       │
  met_audit.schema_change_log   met_quarantine.data_quarantine (ROW/PAYLOAD)  │
                                                                              ▼
              met_silver.<silver_table>  MERGE on silver.keys (only changed values, never older data)
                                                                              ▼
              met_gold.<gold_table>      sql/gold/<name>.sql for affected keys only, MERGE on gold.keys
                                                                              ▼
                                                              Databricks AI/BI dashboards
Audit (all sources): met_audit.run_log (one row per run) · met_audit.run_log_dtl (one row per entity
per run, with extraction_type) · met_audit.schema_change_log. Demo runs: schema_suffix=_edge -> met_*_edge.
```

## 3. Repository
```text
framework.yml              shared: catalog, layer schemas (met_raw/silver/gold/audit/quarantine), landing volume
sources/                   one YAML per API source (the only thing a new pipeline needs, plus 2 below)
  open_meteo_weather.yml   columnar response, 5 cities, demo fault scenarios
  usgs_earthquakes.yml     records response, offset pagination, 5 regions
sql/gold/<source>.sql      gold SELECT per source ({silver}, {affected_keys}, :params)
src/ingest_framework/
  source_config.py         YAML load + full validation, window resolution
  api_client.py            templated requests, API-key auth + redaction, retry/backoff/Retry-After, pagination
  schema_validation.py     contract check: NEW_FIELD / MISSING_FIELD / TYPE_CHANGE / UNIT_CHANGE
  transforms.py            parse, dot-path access, columnar/records extraction, controlled casts
  data_quality.py          config-driven row rules, reason codes, duplicate policy
  fault_injection.py       opt-in faults: built-ins + per-source scenarios (off by default)
  fixture_source.py        offline API stand-in serving saved real responses
  processing.py            Spark-free orchestration, statuses, output rows
  tables.py                ops/bronze contracts, generated silver DDL
  persistence.py           landing files, Delta appends, silver/gold MERGE, audit finalize
  pipeline.py              run_pipeline(): end-to-end with audit in `finally`
notebooks/                 00_setup, 01_run_pipeline, 02_validation, 03_rerun_proof (+ _bootstrap)
sql/dashboard_queries.sql  dashboard datasets (business, pipeline health, data health)
tests/                     pytest suite + fixtures/<source>/ (real API responses)
scripts/local_spark_smoke.py   local Spark+Delta end-to-end check for every source
databricks.yml, resources/  Asset Bundle: one serverless job (`source` parameter) + 3 dashboards
docs/onboarding.md         add a new API source in 5 steps
dashboards/                generated AI/BI dashboard files (business, monitoring, governance)
scripts/build_dashboards.py  dashboard generator (edit this, not the JSON)
docs/ai_use_log.md         AI usage, prompts, mistakes found
```

## 4. Setup and run
Step-by-step runbook (setup, tests, deploy, runs, edge cases, dashboards, demo order): [docs/RUNBOOK.md](docs/RUNBOOK.md).

### Local checks
```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements-dev.txt
.\.venv\Scripts\python -m pytest -q
```
Local Spark + Delta (optional; see the script header for the Windows/Python 3.10 note):
```powershell
py -3.10 -m venv .venv-spark
.\.venv-spark\Scripts\python -m pip install "pyspark==3.5.3" "delta-spark==3.2.1" requests pyyaml
Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue
$env:PYSPARK_PYTHON = "$PWD\.venv-spark\Scripts\python.exe"
.\.venv-spark\Scripts\python scripts\local_spark_smoke.py .local_spark
```

### Deploy to Databricks Free Edition (Asset Bundle)
```powershell
databricks bundle validate -t dev -p Atul
databricks bundle deploy   -t dev -p Atul
databricks bundle run weather_pipeline_job -t dev -p Atul                                   # weather, live
databricks bundle run weather_pipeline_job -t dev -p Atul --params source=usgs_earthquakes  # earthquakes
```
Job parameters: `source`, `catalog`, `schema_suffix` (e.g. `_edge` → `met_raw_edge`, …; empty = main schemas), `source_mode`
(`live`|`fixture`), `test_mode`, `fault_entity_id`, `replay_run_id`, `window` (JSON, e.g. `{"forecast_days":5}`).

## 5. Tables
| Table | Grain / key | Write |
|---|---|---|
| `met_raw` volume `raw_landing` | one file per received raw unit/page | write-once |
| `met_raw.weather_raw`, `met_raw.earthquakes_raw` | one row per received raw unit/page; **partitioned by `ingestion_date`** | append only |
| `met_silver.weather_hourly`, `met_silver.earthquake_events` | `silver.keys` (weather: `location_id, timestamp_utc`); **partitioned by `silver.partition_by`** (`weather_date_utc` / `event_date_utc`) | MERGE |
| `met_gold.weather_daily`, `met_gold.earthquakes_daily` | `gold.keys` (weather: `location_id, weather_date_utc`); lineage `created_run_id`/`created_at` (set once) and `last_updated_run_id`/`updated_at` (on change) | MERGE of affected keys |
| `met_audit.run_log` | `run_id` (status, duration, `job_success`/`job_failed` entity counts, row counts) | insert RUNNING, MERGE final |
| `met_audit.run_log_dtl` | `run_dtl_id` = `<run_id>-<entity>`; `extraction_type` API/FILE/DB, `parameters` JSON, `start_time`/`end_time`; **partitioned by `start_date`** | append |
| `met_audit.schema_change_log` | one row per detected difference | append |
| `met_quarantine.data_quarantine` | rejected row (`ROW`) or payload (`PAYLOAD`), `raw_values` map | append |

**Partitioning.** Delta cannot partition on an expression, so the date partitions are generated columns:
- `ingestion_date = CAST(ingestion_timestamp AS DATE)` on the raw tables
- `start_date = CAST(start_time AS DATE)` on `run_log_dtl`

Both are UTC dates, because the session time zone is UTC. Silver partitions on a real date column chosen per
source (`silver.partition_by`). Existing tables were rebuilt without data loss by
`scripts/migrate_partitioning.py`, which prints a dry run by default and applies with `--execute`; row counts
are checked before the old table is dropped.

Extraction types: `extraction.type` in each source YAML selects an Extractor (`src/ingest_framework/extractors.py`).
`api` is implemented. `file` and `db` are registered, but config validation rejects them until they are
implemented. A new type only needs a new Extractor class that returns `RawUnit`s; the rest of the pipeline
is shared.

Weather units are in column names (`temperature_c`, `relative_humidity_pct`, `precipitation_mm`,
`wind_speed_kmh`) and checked against `hourly_units` in every payload. Precipitation is total precipitation
(rain + showers + snow water equivalent), not rainfall only. Daily grouping uses the **UTC** date.

**Raw data, industry pattern.** Each response is kept twice, unmodified: as a file in a UC Volume
landing zone (partitioned by source, ingest date and run), and as `raw_response` in bronze with
`raw_file_path` pointing to the file. Bronze is the system of record for processing and replay. The
files give an immutable, browsable copy with cheap retention.

## 6. Data quality (from the YAML)
Per silver column: `required`, `min`/`max` (custom reason codes), `allowed_values`, `on_the_hour`,
type (`double/int/bigint/boolean/string/timestamp/date`; timestamps as strptime format, `iso8601`,
`epoch_s` or `epoch_ms`).

Reason codes:
- `NULL_<NAME>`
- `INVALID_<NAME>_TYPE`
- `INVALID_<NAME>` (timestamps)
- range reasons
- `NOT_ALLOWED_<NAME>`
- the duplicate reason

The weather config keeps the original codes (`HUMIDITY_ABOVE_100`, `TEMPERATURE_OUT_OF_RANGE`,
`NEGATIVE_PRECIPITATION`, `DUPLICATE_TIMESTAMP`, ...).

Payload-level rejections (bronze kept, nothing invented):
- `EMPTY_RESPONSE`
- `INVALID_JSON`
- `API_ERROR`
- `MISSING_FIELD`
- container `TYPE_CHANGE`
- `UNIT_CHANGE`
- `ARRAY_LENGTH_MISMATCH`

Policies:
- **Numeric strings:** converted (and logged) or rejected, per source (`silver.numeric_strings`).
- **Duplicates within an entity:** first occurrence wins (page, then index). **Across pulls:** the latest
  ingestion wins.

## 7. Retry, pagination and error policy
| Outcome | Retry? | Entity result |
|---|---|---|
| 200 | – | processed |
| status in `retry.retry_statuses` (default 429/500/502/503/504) | yes; 429/503 honor `Retry-After` (capped) | SUCCESS if a later attempt works |
| timeout / connection error | yes | FAILED after `max_attempts`; no bronze row (no body exists) |
| other 4xx | no | FAILED; error body kept in bronze and landing |
| more pages than `max_pages` | – | FAILED (`PAGINATION_LIMIT`), never silently truncated |
| unexpected exception | no | FAILED (`UNEXPECTED_ERROR`); other entities continue |

**Statuses.**
- A run is SUCCESS if every entity succeeds, FAILED if none do or an essential write fails (landing,
  bronze, silver, gold), and PARTIAL_SUCCESS otherwise.
- An entity is SUCCESS when all its pages are 200, every payload passes the contract, and it has at least one
  valid row (or none, when `allow_empty`).
- A FAILED entity loads nothing; its pages stay in bronze.
- Audit-write failures are raised. Only a FAILED run fails the job.

**Secrets.** `auth: {type: api_key, ...}` reads the key from a Databricks secret scope at run time. It is
masked as `***` in bronze, landing metadata, audits and errors.

## 8. Idempotency
- **Bronze/landing:** grow on every run, by design.
- **Silver:** dedup per key, then MERGE.
  - A same-window rerun inserts 0 rows; updates happen only where values changed.
  - A replayed older pull cannot overwrite newer data.
- **Gold:** the source's SQL recomputes only the keys present in this run's valid rows, then a MERGE.
  - Like silver, an update lists its columns explicitly: metrics plus `last_updated_run_id`/`updated_at`.
    `created_run_id`/`created_at` keep the run that first created the row.
  - Why MERGE, not overwrite: a run where one entity fails must leave that entity's gold rows intact.
- **Counts:** `silver_rows_inserted/updated` come from the MERGE result, not from submitted rows.

## 9. Schema drift
The explicit contract (`response.contract`) is compared with every payload before typed parsing:
- **`NEW_FIELD`:** logged, kept in bronze; the load continues. `ignore_fields` lists known fields that are
  deliberately not loaded.
- **`MISSING_FIELD` / container `TYPE_CHANGE` / `UNIT_CHANGE`:** logged; that entity's payload is rejected.
- **Element `TYPE_CHANGE`:** logged; values are converted or quarantined per policy.

`mergeSchema` is not used.

## 10. Dashboards (deployed with the bundle)
Three AI/BI dashboards are generated by `scripts/build_dashboards.py` into `dashboards/*.lvdash.json` and
deployed by `resources/dashboards.yml`. The warehouse is looked up by name, so no ID is hard-coded.

| Dashboard | Pages | Content |
|---|---|---|
| **Meteorological Data** | Weather · Earthquakes | city filter, **data last updated (UTC)** (gold/silver last change, last successful load), KPI counters, hourly temperature, daily precipitation/wind/average temperature, city comparison, alert days; quakes per day, largest magnitude by region, strongest events |
| **Pipeline monitoring** | Pipeline health · Edge-case runs | nine sections: **1 execution** (total, successful, partial, failed, retries, skipped/cancelled job runs) · **2 performance** (avg/max runtime, extraction latency, duration trend, task duration by stage) · **3 ingestion** (source vs target records, throughput) · **4 data quality** (valid, rejected, duplicates, null %, rejection %, reasons) · **5 SLA** (expected vs actual completion, breaches; targets in `framework.yml`) · **6 freshness** (last successful load per entity, source-to-target latency, stale entities) · **7 infrastructure** (DBUs from `system.billing.usage`; CPU/memory not exposed on serverless) · **8 errors** (codes, stages, exception messages) · **9 audit & reconciliation** (source = valid + rejected, valid = silver input, inserted/updated/unchanged). Edge-case page from `met_*_edge` |
| **Data governance** | 3 pages x 3 sections | nine sections: **ownership** (owner, steward, datasets without either) · **metadata** (registered tables, % described, missing descriptions, schema changes) · **lineage** (declared Source API → Bronze → Silver → Gold, observed Unity Catalog edges, upstream/downstream) · **security & access** (principals, failed / unauthorized requests, grants, sensitive datasets) · **compliance** (retention policy vs data age, classification coverage, policy violations) · **audit & monitoring** (run history, governed-asset changes, job runs, access log) · **data quality** (quality score, pass/fail, nulls, duplicates, invalid) · **freshness** (dataset and entity, stale, SLA breaches) · **quarantine** (rejected, reasons, pending reprocessing = not yet replayed) |

**Refresh:** published dashboards serve cached results. The job ends with three `dashboard_task`s (`run_if: ALL_DONE`) that refresh all three dashboards after every run, including failed ones.

Regenerate and check the dashboards before deploying:
```powershell
$env:DBX_WAREHOUSE = "<warehouse id>"   # databricks warehouses list -p Atul
.\.venv\Scripts\python scripts\build_dashboards.py --validate
```
`--validate` runs every dataset on the warehouse and fails if a widget uses an unsupported (nested) expression,
references a column the dataset does not return, has an encoding without a query field, or overlaps another
widget. `tests/test_dashboards.py` runs the offline part and checks that the committed JSON is up to date.

Steward and per-layer `retention_days` are part of each source's `governance:` block; the shared audit and quarantine tables are owned via `governance:` in `framework.yml`.

Governance metadata comes from each source YAML (`governance:` block, column `description`s, and
`gold.column_descriptions`). The setup task applies it as Unity Catalog tags and column comments, changing
only what differs.

The ad-hoc versions of the monitoring queries are also in `sql/dashboard_queries.sql`. One dataset per visual:
- **Business:** weather B1–B4, earthquakes B5.
- **Pipeline health:** latest run per source, history, failures by entity, retries/pages, stuck runs.
- **Data health:** loaded vs rejected and rejection rate, ingestion freshness per entity, forecast horizon,
  quarantine reasons, recent quarantine, schema changes.

All ops visuals filter on `source_name`.

## 11. Edge-case demo
Edge runs use separate schemas so injected data never reaches the main tables (except the deliberate real failure):
```powershell
# deliberate real failure: the real API returns 400 for London -> PARTIAL_SUCCESS
databricks bundle run weather_pipeline_job -t dev -p Atul --params test_mode=bad_request
# fixture-based edge cases
databricks bundle run weather_pipeline_job -t dev -p Atul --params "schema_suffix=_edge,source_mode=fixture,test_mode=invalid_humidity"
```
| `test_mode` | Expected evidence |
|---|---|
| `bad_request` | target FAILED, `http_status=400`, `attempt_count=1`; run PARTIAL_SUCCESS |
| `unreachable_url` / `timeout` / `server_error` | FAILED after 3 attempts (`CONNECTION_ERROR` / `TIMEOUT` / `HTTP_5XX`) |
| `rate_limit` | 429, then success: `attempt_count=2` |
| `invalid_humidity` | quarantine `HUMIDITY_ABOVE_100`, `raw_values.relative_humidity_2m=130` |
| `bad_temperature_type` | quarantine `INVALID_TEMPERATURE_TYPE` (`"ABC"`); schema `TYPE_CHANGE` |
| `numeric_string` | value converted; schema `TYPE_CHANGE`; `numeric_strings_converted=1` |
| `null_values` / `invalid_timestamp` | quarantine `NULL_WIND_SPEED` / `INVALID_TIMESTAMP` |
| `duplicate_record` | one silver row; quarantine `DUPLICATE_TIMESTAMP` (conflicting) |
| `new_field` | schema `NEW_FIELD`; load continues |
| `missing_field` | schema `MISSING_FIELD`; PAYLOAD quarantine; target FAILED |
| `unequal_arrays` | PAYLOAD quarantine `ARRAY_LENGTH_MISMATCH` with all lengths |
| `empty_response` / `truncated_response` | `EMPTY_RESPONSE` / `INVALID_JSON`; body kept |
| USGS: `invalid_magnitude`, `bad_magnitude_type`, `missing_field`, `new_field`, `bad_request` | the same patterns for a records source |

Same-window rerun: `notebooks/03_rerun_proof.py` asserts that bronze grows and silver/gold key counts don't.

## 12. Onboarding a new API
See [docs/onboarding.md](docs/onboarding.md). The steps are:
1. Write `sources/<name>.yml`.
2. Save fixture responses.
3. Write `sql/gold/<name>.sql`.
4. Run `pytest`. The onboarding tests discover the new source automatically.
5. Run `bundle deploy`, then `bundle run --params source=<name>`.

## 13. Path to production
- **Scheduling:** one schedule per source, or a `for_each_task` over sources in one job.
- **Environments:** dev/test/prod bundle targets with separate catalogs.
- **CI:** `pytest` (including the onboarding contract) + `bundle validate` before `bundle deploy`.
- **Governance:** Unity Catalog grants (writer service principal; read-only analysts on gold, restricted
  bronze/landing) and lineage.
- **Alerts:** SQL alerts on FAILED/PARTIAL_SUCCESS, staleness and rejection-rate spikes.
- **Storage:** landing-file lifecycle rules, VACUUM/OPTIMIZE.
- **Scale (thousands of entities):** bounded concurrency per source with rate limits, incremental windows,
  and a dead-letter list.
- **Future:** Auto Loader over the landing volume as an alternative bronze loader.

## 14. AI usage
See [docs/ai_use_log.md](docs/ai_use_log.md).
Slides: [docs/Config_driven_API_ingestion.pptx](docs/Config_driven_API_ingestion.pptx) (4 slides, speaker notes included).

## 15. Verification status and known limitations
**Verified locally:**
- `pytest`: 211 test cases pass, from 111 test functions parametrized over edge-case tables (including dashboard structure checks). They cover config validation, templating, auth/redaction, every pagination type,
  the retry policy, schema drift for both shapes, DQ, every weather fault, USGS scenarios, the onboarding
  contract for every source, and landing files.
- `scripts/local_spark_smoke.py` (Spark 3.5.3 + Delta 3.2.1, Python 3.10), for both sources: first run,
  rerun (0 inserted/0 updated, no new silver/gold rows), a fault run and a replay. Landing files are identical
  to bronze `raw_response`.
  - Weather: 360 silver / 15 gold.
  - USGS: 16 pages, 134 events, 33 gold rows.

**Workspace:** see "Workspace evidence" below. Only runs that were actually executed are listed.

**Limitations:**
- API calls are sequential per source.
- Only the final retry attempt's body is kept per page; the attempt history is in the entity audit.
- The HTTP-date form of `Retry-After` falls back to backoff.
- Payload processing runs on the driver (small data by design).
- Fixture matching for `next_url`/`cursor` pagination uses a per-entity counter.
- Gold days at the window edges may be partial (`is_complete_day` for weather).
- Alert thresholds are demo values.
- The pre-framework tables (`weather_demo*` schemas) are left in the workspace, untouched.

### Workspace evidence
**Layered model (2026-10-10).** The 8 pre-`met_*` project schemas were dropped (`scripts/drop_legacy_schemas.sql`).
`bookstore_eng_pro` and `default` were not touched. Job runs on serverless, all tasks (including validation)
SUCCESS:

| Run | Result |
|---|---|
| weather, live | SUCCESS; `job_success=5`; met_raw 5 rows → met_silver 360 inserted → met_gold 15 inserted |
| USGS, live | SUCCESS; 18 pages → 151 events → 33 gold rows |
| weather, live rerun | SUCCESS; 0 inserted, 39 updated (forecast revised); silver 360, gold 15 |
| weather `bad_request` | PARTIAL_SUCCESS; `job_failed=1`; `run_log_dtl`: London API / FAILED / HTTP 400 / 1 attempt |
| edge (`_edge`) `invalid_humidity` | `met_quarantine_edge`: ROW `HUMIDITY_ABOVE_100` |
| edge `missing_field` | PARTIAL_SUCCESS; `met_audit_edge.schema_change_log` MISSING_FIELD; PAYLOAD quarantine |
| edge `timeout` | PARTIAL_SUCCESS; London `TIMEOUT` |
| edge USGS `invalid_magnitude` / `new_field` | ROW `MAGNITUDE_OUT_OF_RANGE` / `NEW_FIELD features[].properties.new_metric` |

Other facts checked in the workspace:
- **Schemas:** `met_raw`, `met_silver`, `met_gold`, `met_audit` (3 tables), `met_quarantine` and their `_edge`
  copies.
- **Landing:** volume `raw_landing` in `met_raw`.
- **Governance:** tags (layer, source, classification, data_owner, extraction_type) on all raw/silver/gold tables.
- **Dashboards:**
  - live-validated: every dataset runs and every widget column exists
  - redeployed and republished
  - visual rendering should be checked in the UI
