# Execution Steps — Weather Pipeline on Databricks Free Edition

> **Update 2026-10-09:** the weather-only package (`src/weather_pipeline`, `config/*.json`) was replaced by the
> config-driven framework (`src/ingest_framework`, `framework.yml`, `sources/*.yml`), and raw responses are also
> written to a UC Volume landing zone. Table names moved to `workspace.weather.*` plus shared
> `workspace.ingest_ops.*`. The phases below describe the original build; README.md describes the current design and docs/RUNBOOK.md has the current execution steps.

Source documents, in order of authority:
1. `../DE_Takehome_Weather_API_Databricks.pdf` (assignment, authoritative)
2. `../PROJECT_CONTEXT.md` (assignment summary + candidate decisions of 2026-10-07)
3. `DATABRICKS_WEATHER_IMPLEMENTATION.md` (implementation spec)

Strictly confidential: no public repo, no publishing, no upload anywhere except the candidate's own workspace.

---

## 0. Current state (inspected 2026-10-07)

| Item | Status |
|---|---|
| Repository | Only `DATABRICKS_WEATHER_IMPLEMENTATION.md`; no code, not a git repo |
| Python | 3.14 is the default `python`; 3.12 and 3.10 also installed (`py -3.12`). `pip` on PATH belongs to 3.12 — mismatch |
| PySpark locally | Not installed |
| Java | 11 (OK for local PySpark 3.5 if needed) |
| Databricks CLI | v1.19.0, `bundle` command available |
| CLI profiles | `Atul` → `https://dbc-367bc80b-b0c3.cloud.databricks.com` (valid); `DEFAULT` profile is invalid — always pass `-p Atul` |

## 1. Decisions that resolve conflicts between the documents

| Topic | Spec says | Decision for execution | Why |
|---|---|---|---|
| DAB | Optional, do last | **Core deliverable**, built in Phase 3 right after the first working end-to-end path | Candidate decision in PROJECT_CONTEXT.md |
| Location key | `location` (city name) | `location_id` (stable slug, e.g. `tokyo`) + `location_name` | Names can change; IDs stay stable (CLAUDE.md) |
| Silver key | `(location, timestamp)` | `(location_id, timestamp_utc)` | Same semantics, explicit UTC |
| Gold key / write | MERGE on `(location, date)` | MERGE on `(location_id, weather_date)`, recomputed from Silver **only for location/dates touched in this run** | Rerun-safe; a partial run never removes other cities' gold rows |
| Daily grouping timezone | not specified | UTC | Documented in README; API called with `timezone=UTC` |
| Layout | flat `src/*.py` | `src/weather_pipeline/` package + `config/locations.json` | Importable from notebooks and testable locally |
| Window | `FORECAST_DAYS = 5` | `forecast_days = 3` default (configurable 3–7); optional fixed `start_date`/`end_date` | Smaller demo; fixed dates make reruns exactly repeatable. Verify parameter names in Open-Meteo docs before coding |
| Edge-case runs | not specified | Fixture-based edge cases write to a separate schema `weather_demo_edge`; real runs and the deliberate bad-parameter failure go to `weather_demo` | Keeps demo junk out of real Silver/Gold while the real failure still shows on the dashboard |
| Table names | spec §3 names | Use spec §3 names (`bronze_weather_raw`, `pipeline_run_audit`, …) | Spec is the most detailed, latest document |

Record any change to these in README "Design decisions".

---

## Phase 0 — Environment checks (≈20 min)

**0.1 Local Python environment** (use 3.12, PySpark does not support 3.14 reliably):
```powershell
cd D:\learning\projects\JERA\weather_analysis
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install requests pytest
# optional, only if local Spark tests are wanted:
pip install "pyspark==3.5.*" delta-spark
```

**0.2 Workspace access**
```powershell
databricks -p Atul current-user me
databricks -p Atul catalogs list
```
Confirm the catalog name (expected `workspace`). Create the schema later via `00_setup`.

**0.3 Blocking check — can serverless reach Open-Meteo?**
In the workspace, create a scratch notebook on serverless compute and run:
```python
import requests
r = requests.get("https://api.open-meteo.com/v1/forecast",
                 params={"latitude": 35.68, "longitude": 139.65,
                         "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
                         "forecast_days": 3, "timezone": "UTC"}, timeout=10)
print(r.status_code, list(r.json().get("hourly", {}).keys()), r.json().get("hourly_units"))
```
- **Works** → record status code, field names and units in `docs/ai_use_log.md`/README evidence.
- **Blocked** → document it and run the pipeline in fixture mode (`SOURCE_MODE=fixture`) using saved real responses captured locally.

**0.4 Save a real response as the base fixture** (locally):
```powershell
mkdir tests\fixtures
curl.exe -s "https://api.open-meteo.com/v1/forecast?latitude=35.6762&longitude=139.6503&hourly=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m&forecast_days=3&timezone=UTC" -o tests\fixtures\tokyo_ok.json
```
All edge-case fixtures are derived from this real file.

**0.5** `git init` locally (no remote) so changes are reversible.

✅ Exit criteria: venv works, `-p Atul` authenticates, API reachability known, `tokyo_ok.json` saved.

---

## Phase 1 — Pure-Python core + unit tests (≈1.5 h, no Spark needed)

Create files in this order; after each, write its tests and run `pytest -q`.

| # | File | Contents | Tests |
|---|---|---|---|
| 1 | `config/locations.json` | 5 cities: `location_id, location_name, latitude, longitude` | — |
| 2 | `src/weather_pipeline/config.py` | Load JSON + settings (endpoint, hourly fields, window, catalog/schema, timeout, max_attempts, backoff cap). `validate_config()` rejects <5 locations, bad lat/lon, window outside 3–7, duplicate IDs | `test_config.py` |
| 3 | `api_client.py` | `build_params()`, `fetch_location()` returning a `FetchResult` dataclass (`status, http_status, attempts, body_text, error_type, error_message, request_url`). Retry: timeout/ConnectionError/429/500/502/503/504; capped exponential backoff + jitter; honor `Retry-After` up to cap; 400 and other 4xx = no retry; max 3 attempts. Injectable `session` and `sleep` for tests | `test_api_client.py` with fake session: 200, 400, 429→200, 503×3, timeout×3, invalid JSON |
| 4 | `schema_validation.py` | `EXPECTED_CONTRACT` (hourly field → expected type). `detect_schema_changes(payload)` → list of `NEW_FIELD / MISSING_FIELD / TYPE_CHANGE` events | `test_schema_validation.py` |
| 5 | `transforms.py` | `check_array_lengths()` (no `zip` before it), `flatten_payload()` → list of dicts with `*_raw` values kept as strings, `safe_float()` with documented numeric-string policy | `test_transforms.py`: normal row count = 72, unequal arrays rejected, numeric string `"21.5"` converted + event, `"ABC"` rejected |
| 6 | `data_quality.py` | `validate_row()` → list of reason codes (spec §13 list + `NULL_<FIELD>`, `DUPLICATE_TIMESTAMP_CONFLICT`). Temperature bounds −90..60 °C (documented). `resolve_duplicates()` deterministic policy | `test_data_quality.py`: humidity 0/100/100.1/−1, precip −0.1, wind −1, bad timestamp, duplicates |
| 7 | `fault_injection.py` | `apply_test_mode(mode, location_id, body)` — only active when `TEST_MODE` is set; modes from spec §20 | `test_fault_injection.py` |

Run:
```powershell
pytest -q
```
✅ Exit criteria: all tests green; record the actual pass count in README "Checks run".

---

## Phase 2 — Spark/Delta layer + notebooks (≈1.5 h)

**2.1 `sql/ddl.sql`** — explicit `CREATE TABLE IF NOT EXISTS ... USING DELTA` for all 7 tables (spec §4, §7, §9, §12, §14, §17). No `mergeSchema`.

**2.2 `src/weather_pipeline/persistence.py`**
- `append_bronze(rows)` — append only.
- `append_quarantine`, `append_schema_events`, `write_location_audit`.
- `start_run_audit()` writes `RUNNING`; `finalize_run_audit()` MERGEs the final row by `run_id`.
- `merge_silver(df)` — dedup with `row_number()` over `(location_id, timestamp_utc)` ordered by `ingestion_timestamp DESC`, then MERGE; read inserted/updated counts from `DeltaTable.history(1)` `operationMetrics` (`numTargetRowsInserted`, `numTargetRowsUpdated`) — never report submitted rows as inserted.
- `merge_gold(affected_keys)` — recompute aggregates from Silver for affected `(location_id, weather_date)` only, then MERGE.

**2.3 `src/weather_pipeline/pipeline.py`** — `run_pipeline(spark, settings, test_mode=None)`:
```
validate config → run_id → RUNNING audit
for each location (try/except per location):
    fetch → (fault injection if enabled) → append bronze if a body exists
    schema events → array check → flatten → DQ → collect valid/invalid
    location audit row (always, including failures)
quarantine append → silver MERGE → gold MERGE (affected keys)
final status SUCCESS / PARTIAL_SUCCESS / FAILED; FAILED also if an essential write fails
finalize run audit in `finally`; re-raise audit-write failures
```
Silver/Gold read only the current `run_id` from Bronze (replay via an explicit `replay_run_id` parameter).

**2.4 Notebooks** (`notebooks/`, Databricks source format `# Databricks notebook source`), thin wrappers using widgets `catalog`, `schema`, `test_mode`, `source_mode`:
- `00_setup.py` — create schema + run `sql/ddl.sql`
- `01_run_pipeline.py` — `run_pipeline(...)` (one entry point is simpler to demo than 3 notebooks; spec notebooks 01–03 can be sections inside it)
- `04_monitoring_tests.py` — spec §21 validation queries + idempotency check

Import code with `sys.path.append(<bundle root>/src)`.

✅ Exit criteria: code imports cleanly; local Spark test optional (`test_idempotency.py` with local PySpark + delta-spark if installed, otherwise mark "workspace-only").

---

## Phase 3 — Asset bundle + first workspace run (≈45 min)

**3.1 `databricks.yml`** — target `dev` (mode `development`, host from profile `Atul`), variables `catalog`, `schema`. Job `weather_pipeline_job` with serverless tasks:
```
setup → run_pipeline → validation
```
(≤5 concurrent tasks; no cluster definitions — serverless only on Free Edition.) Check the current bundle schema with `databricks bundle schema` before writing YAML.

**3.2 Deploy and run**
```powershell
databricks bundle validate -t dev -p Atul
databricks bundle deploy   -t dev -p Atul
databricks bundle run weather_pipeline_job -t dev -p Atul
```

**3.3 Verify in a SQL editor / notebook**
```sql
SELECT * FROM workspace.weather_demo.pipeline_run_audit ORDER BY start_time DESC;
SELECT location_id, status, http_status, retry_count, records_valid FROM workspace.weather_demo.location_run_audit;
SELECT COUNT(*) FROM workspace.weather_demo.bronze_weather_raw;      -- expect 5
SELECT COUNT(*) FROM workspace.weather_demo.silver_weather_hourly;   -- expect 5 × 72 = 360
SELECT COUNT(*) FROM workspace.weather_demo.gold_weather_daily;      -- expect 5 × 3 = 15
```
✅ Exit criteria: run status `SUCCESS`; counts recorded as evidence. If the bundle fails on Free Edition, record the error and run the notebooks manually (fallback), then fix the bundle.

---

## Phase 4 — Idempotency + edge cases (≈1 h)

**4.1 Same-window rerun** (main schema):
```powershell
databricks bundle run weather_pipeline_job -t dev -p Atul
```
Expect Bronze 5 → 10, Silver stays 360, Gold stays 15, duplicate-key query returns 0 rows, `silver_updated` > 0 only if forecasts changed. Save a before/after table for the demo.

**4.2 Deliberate real failure** (main schema): run with `test_mode=bad_request` for one city (invalid latitude sent to the real API → real HTTP 400). Expect `PARTIAL_SUCCESS`, 4 cities loaded, failed city audited with `http_status=400`, `retry_count=0`.

**4.3 Fixture edge cases** (schema `weather_demo_edge`, `source_mode=fixture`). One run per mode, record result:

| test_mode | Expected evidence |
|---|---|
| `invalid_humidity` (130) | Quarantine `HUMIDITY_ABOVE_100`; row absent from Silver |
| `bad_temperature_type` ("ABC") | Quarantine `INVALID_TEMPERATURE_TYPE`, `temperature_raw='ABC'`; schema event `TYPE_CHANGE` |
| `duplicate_timestamp` | Silver has one row for that key; conflict recorded |
| `missing_field` | Schema event `MISSING_FIELD`; location `FAILED`; others continue |
| `new_field` (`cloud_cover`) | Schema event `NEW_FIELD`; load continues; field kept in Bronze |
| `unequal_arrays` | `ARRAY_LENGTH_MISMATCH` payload rejection; Bronze kept; no rows invented |
| `empty_response` | Location `FAILED` with clear error; Bronze keeps the empty body |
| timeout / 429 / 5xx | Covered by local unit tests (fake session) — show `pytest` output in the demo |

✅ Exit criteria: each row above observed in tables; screenshots/queries saved in `docs/demo_script.md`.

---

## Phase 5 — Monitoring SQL + dashboard (≈45 min)

**5.1 `sql/dashboard_queries.sql`** (spec §19): latest run status/duration, location success/failure per run, retries/errors, valid vs rejected rows, rejection rate with `NULLIF(total,0)`, freshness = `now() - max(ingestion_timestamp)` (ingestion freshness) vs `max(timestamp_utc)` (forecast horizon) — labelled separately.

**5.2 Build the dashboard in the UI** (AI/BI dashboard on the single 2X-Small warehouse):
- Business: temperature by city/day, daily total precipitation, max wind, city comparison table.
- Ops: latest status, duration, successful/failed locations, loaded/rejected rows, freshness.

Optional: export the dashboard JSON (`.lvdash.json`) into the bundle only if export/import works on Free Edition — verify, don't assume.

✅ Exit criteria: dashboard renders with both a SUCCESS and a PARTIAL_SUCCESS run visible.

---

## Phase 6 — Documentation, slides, demo prep (≈45 min)

- `README.md` — the 14 sections in spec §24, plus "Checks run" (local, with real results) vs "Workspace evidence" (only what was actually observed).
- `docs/ai_use_log.md` — real prompts and a **real** mistake observed during this build (do not reuse the spec's `zip()` example unless it actually happened).
- `docs/demo_script.md` — 20-minute timed script (spec §28) with exact widget values and queries.
- `docs/slides_outline.md` — 6 slides: problem & architecture; layers & keys; reliability (retry/isolation/audit); DQ & schema drift; dashboard & results; production path (scheduling, envs, DAB/CI-CD, UC governance, alerts, scaling, testing).

---

## Phase 7 — Dry run (≈20 min)

1. Reset demo state (only demo schemas; confirm before dropping anything).
2. Run the full demo script once with a timer.
3. Tick the Definition of Done (spec §31) and the PROJECT_CONTEXT acceptance checklist; leave unticked anything not actually verified.

---

## Requirement → evidence tracker

| Requirement | Where implemented | Evidence | Status |
|---|---|---|---|
| ≥5 configurable locations, 3–7 day hourly window | `config/locations.json`, `config.py` | `test_config.py` | ☐ |
| 4 measures retrieved | `api_client.py` | Phase 0.3 output, Silver columns | ☐ |
| Bronze append-only, raw body + metadata | `persistence.append_bronze` | Bronze 5 → 10 on rerun | ☐ |
| Silver MERGE, no duplicate keys | `merge_silver` | Duplicate query = 0 rows | ☐ |
| Gold rerun-safe | `merge_gold` | Gold count unchanged on rerun | ☐ |
| Retries/timeout/429/5xx/400 | `api_client.py` | `pytest` + real 400 run | ☐ |
| One-location isolation | `pipeline.py` | PARTIAL_SUCCESS run | ☐ |
| Quarantine with reasons | `data_quality.py` | Edge runs | ☐ |
| Schema drift logged | `schema_validation.py` | `schema_change_audit` rows | ☐ |
| Run + location audit | `persistence.py` | Audit queries | ☐ |
| Dashboard (gold + health + freshness) | workspace | Screenshot | ☐ |
| DAB deploy + run | `databricks.yml` | `bundle deploy/run` output | ☐ |
| README, AI log, slides, demo script | `README.md`, `docs/` | Files | ☐ |

Total estimate: ≈6.5 h including the DAB. If time is short, cut in this order: local Spark tests, dashboard JSON in bundle, alert table.
