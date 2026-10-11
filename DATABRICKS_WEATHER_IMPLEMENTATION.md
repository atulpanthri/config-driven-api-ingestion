# Databricks Weather Data Engineering Take-Home — Claude Code Implementation Spec

## Goal
Build a complete, demo-ready Databricks Free Edition solution for the supplied weather API take-home. Keep it simple enough to explain in 20 minutes, but implement the reliability requirements cleanly.

## 1. Mandatory requirements
- Call `https://api.open-meteo.com/v1/forecast` for at least 5 configurable locations.
- Retrieve 3–7 days of hourly data.
- Include temperature, relative humidity, precipitation, and wind speed.
- Use Delta Bronze, Silver, and Gold layers.
- Bronze is append-only and keeps every raw API pull.
- Silver has one typed row per `(location, timestamp)` and uses MERGE.
- Same-window reruns must not duplicate Silver or Gold.
- Invalid records go to quarantine with reasons.
- One location failure must not affect other locations.
- Maintain run-level and location-level audit data.
- Detect/log new fields, missing fields, and changed data types.
- Expose pipeline health and data health for a Databricks dashboard.
- Prepare controlled failure/edge-case scenarios.
- DAB is optional and should be done only after core requirements work.

## 2. Repository structure
```text
weather-pipeline/
├── README.md
├── requirements.txt
├── src/
│   ├── config.py
│   ├── api_client.py
│   ├── schemas.py
│   ├── audit.py
│   ├── data_quality.py
│   ├── schema_validation.py
│   └── transforms.py
├── notebooks/
│   ├── 00_setup.py
│   ├── 01_ingest_bronze.py
│   ├── 02_bronze_to_silver.py
│   ├── 03_silver_to_gold.py
│   └── 04_monitoring_tests.py
├── tests/
│   ├── test_data_quality.py
│   ├── test_schema_validation.py
│   ├── test_transforms.py
│   └── test_idempotency.py
└── resources/
    └── dashboard_queries.sql
```
Optional later: `databricks.yml` and job resource YAML.

## 3. Configuration
Use at least:
```python
LOCATIONS = [
    {"location":"Tokyo", "latitude":35.6762, "longitude":139.6503},
    {"location":"Singapore", "latitude":1.3521, "longitude":103.8198},
    {"location":"London", "latitude":51.5072, "longitude":-0.1276},
    {"location":"New York", "latitude":40.7128, "longitude":-74.0060},
    {"location":"Sydney", "latitude":-33.8688, "longitude":151.2093},
]
FORECAST_DAYS = 5
HOURLY_FIELDS = ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"]
TIMEZONE = "UTC"
```
Centralize table names:
```python
CATALOG = "workspace"
SCHEMA = "weather_demo"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_weather_raw"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_weather_hourly"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_weather_daily"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.weather_quarantine"
RUN_AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.pipeline_run_audit"
LOCATION_AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.location_run_audit"
SCHEMA_AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.schema_change_audit"
```
Make catalog/schema easy to change for the actual Free Edition workspace.

## 4. Pipeline run and auditing
Generate one UUID per complete execution:
```python
run_id = str(uuid.uuid4())
run_start_time = datetime.now(timezone.utc)
```
Create `pipeline_run_audit` with approximately:
```text
run_id, start_time, end_time, status, parameters,
locations_requested, locations_successful, locations_failed,
bronze_rows, silver_valid_rows, silver_inserted, silver_updated,
quarantine_rows, gold_rows_affected, duration_seconds, error_message
```
Statuses: `RUNNING`, `SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`.

Create `location_run_audit` with:
```text
run_id, location, request_start_time, request_end_time, status,
http_status, retry_count, records_received, records_valid,
records_rejected, error_type, error_message
```
Every requested location gets an audit row.

## 5. API client
```python
BASE_URL = "https://api.open-meteo.com/v1/forecast"

def build_params(location):
    return {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "hourly": ",".join(HOURLY_FIELDS),
        "forecast_days": FORECAST_DAYS,
        "timezone": "UTC",
    }
```
Use `requests.get(..., timeout=10)`.

Retry policy:
- 200: process.
- 400: fail location, normally no retry.
- 429: retry; honor `Retry-After` if present, else exponential backoff.
- 5xx: bounded retry.
- timeout/transient network error: bounded retry.
- invalid JSON: fail and audit.
- unexpected exception: isolate and audit.
- Maximum ~3 attempts for the take-home.

Return structured results instead of uncontrolled exceptions.

## 6. Location isolation
Process each city independently. One exception must never terminate all cities.
Expected example:
```text
Tokyo SUCCESS
Singapore SUCCESS
London FAILED
New York SUCCESS
Sydney SUCCESS
Overall = PARTIAL_SUCCESS
```
Successful cities continue downstream.

## 7. Bronze
Schema should include:
```text
run_id STRING
ingestion_timestamp TIMESTAMP
location STRING
latitude DOUBLE
longitude DOUBLE
request_url STRING
request_params STRING
http_status INT
retry_count INT
raw_response STRING
```
Rules:
- raw API response as received;
- append only;
- never update/merge/overwrite/deduplicate Bronze;
- same-window rerun creates another raw pull;
- preserve request metadata.

Write using Delta append.

## 8. Process current run
Normally read only Bronze rows where `run_id == current run_id`. Historical replay should remain possible explicitly.

## 9. Source contract and schema drift
Required hourly fields:
```text
time
temperature_2m
relative_humidity_2m
precipitation
wind_speed_10m
```
Before restrictive parsing, inspect raw JSON keys.
```python
actual_fields = set(payload.get("hourly", {}).keys())
expected_fields = {"time","temperature_2m","relative_humidity_2m","precipitation","wind_speed_10m"}
new_fields = actual_fields - expected_fields
missing_fields = expected_fields - actual_fields
```
Create `schema_change_audit`:
```text
run_id, location, detected_at, change_type, field_name,
expected_type, actual_type, details
```
Change types: `NEW_FIELD`, `MISSING_FIELD`, `TYPE_CHANGE`.

Policy:
- additive unknown field: log and continue if core contract is valid;
- required missing field: log and reject/fail that location payload;
- compatible numeric string: safe conversion allowed, optionally log drift;
- incompatible type: quarantine affected data.

Do not treat `mergeSchema` as a complete drift strategy.

## 10. Parallel array validation
Before flattening, compare lengths of all required hourly arrays.
```python
lengths = {
  "time": len(hourly["time"]),
  "temperature": len(hourly["temperature_2m"]),
  "humidity": len(hourly["relative_humidity_2m"]),
  "precipitation": len(hourly["precipitation"]),
  "wind": len(hourly["wind_speed_10m"]),
}
if len(set(lengths.values())) != 1:
    # ARRAY_LENGTH_MISMATCH
```
Never silently `zip()` unequal arrays. Log/reject malformed location payload while allowing other cities to continue.

## 11. Flatten and type
Transform parallel arrays to one row per location/hour:
```text
location | timestamp | temperature | humidity | precipitation | wind_speed
```
For this small take-home, Python normalization followed by Spark DataFrame creation is acceptable.

Preserve raw values during casting where useful:
```text
timestamp_raw
temperature_raw
humidity_raw
precipitation_raw
wind_speed_raw
```
This distinguishes genuine source nulls from conversion failures such as `"ABC" -> NULL`.

## 12. Silver schema
Suggested:
```text
location STRING
latitude DOUBLE
longitude DOUBLE
timestamp TIMESTAMP
temperature DOUBLE
humidity DOUBLE
precipitation DOUBLE
wind_speed DOUBLE
created_run_id STRING
last_updated_run_id STRING
created_at TIMESTAMP
updated_at TIMESTAMP
```
Business key: `(location, timestamp)`.

## 13. Data quality
Implement explicit validation reasons. Minimum rules:
- location non-null;
- timestamp valid/non-null;
- humidity between 0 and 100;
- precipitation >= 0;
- wind speed >= 0;
- temperature within a documented plausible range;
- numeric conversions successful.

Suggested reasons:
```text
MISSING_LOCATION
INVALID_TIMESTAMP
HUMIDITY_BELOW_ZERO
HUMIDITY_ABOVE_100
NEGATIVE_PRECIPITATION
NEGATIVE_WIND_SPEED
TEMPERATURE_OUT_OF_RANGE
INVALID_TEMPERATURE_TYPE
INVALID_HUMIDITY_TYPE
ARRAY_LENGTH_MISMATCH
```
Create `dq_errors ARRAY<STRING>` or equivalent. Split valid and invalid rows. Never silently drop invalid rows.

## 14. Quarantine
Create `weather_quarantine` with approximately:
```text
run_id
location
raw_timestamp
timestamp
temperature_raw
humidity_raw
precipitation_raw
wind_speed_raw
dq_errors ARRAY<STRING>
raw_record
quarantined_at
```
Append invalid records and preserve reasons.

## 15. Deterministic dedup before MERGE
Before Silver MERGE, guarantee one source row per `(location,timestamp)`.
Use a window ordered by latest ingestion timestamp:
```python
w = Window.partitionBy("location", "timestamp").orderBy(F.col("ingestion_timestamp").desc())
deduped_df = valid_df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
```
Rule: latest ingestion wins. Avoid arbitrary `dropDuplicates()` when duplicate values disagree.

## 16. Silver MERGE
Use Delta MERGE keyed by:
```text
target.location = source.location
AND target.timestamp = source.timestamp
```
On insert set created and updated metadata to current run/time. On update preserve created metadata and change last-updated metadata.

Same-window rerun must not add duplicate Silver business keys.

## 17. Gold
Create `gold_weather_daily`:
```text
location STRING
date DATE
min_temperature DOUBLE
max_temperature DOUBLE
avg_temperature DOUBLE
total_precipitation DOUBLE
max_wind_speed DOUBLE
avg_humidity DOUBLE
rainy_hours BIGINT
last_updated_run_id STRING
updated_at TIMESTAMP
```
Aggregate Silver by location/date using min/max/avg temperature, total precipitation, max wind, avg humidity, and rainy-hour count.

Use `(location,date)` as the logical Gold key and MERGE for clear rerun safety. A targeted partition replacement is acceptable only if well documented.

Optional `gold_weather_alerts` may use demo thresholds for heat/high wind/heavy rain; clearly label thresholds as demo business rules, not authoritative meteorological standards.

## 18. Finalize audits
At completion compute successful/failed locations, Bronze rows, valid/rejected rows, Silver/Gold affected rows, duration, and errors. Final status:
```python
if success_count == len(LOCATIONS): status = "SUCCESS"
elif success_count == 0: status = "FAILED"
else: status = "PARTIAL_SUCCESS"
```

## 19. Monitoring/dashboard queries
Provide simple SQL/views for:
- latest pipeline status and duration;
- successful/failed locations;
- retry/error information;
- rows processed/valid/rejected;
- rejection rate (guard division by zero);
- latest weather timestamp per location;
- documented freshness metric;
- recent quarantine/schema changes.

Dashboard business visuals:
1. temperature comparison/trend;
2. daily precipitation;
3. maximum wind;
4. city comparison.

Operational tiles:
5. latest run status;
6. duration;
7. successful/failed locations;
8. loaded/rejected rows;
9. freshness.

## 20. Controlled test mode
Implement clearly separated test injection, e.g.:
```text
TEST_MODE = None
bad_request
invalid_humidity
bad_temperature_type
duplicate_timestamp
unequal_arrays
missing_field
new_field
empty_response
```
Do not depend on the public API randomly failing during the demo.

### Required scenarios
**One location failure:** four cities continue; failed city is audited; run becomes PARTIAL_SUCCESS.

**Humidity 130:** absent from Silver, present in quarantine with `HUMIDITY_ABOVE_100`.

**Bad temperature type `"ABC"`:** preserve raw value, safe cast fails, quarantine with type reason.

**Duplicate timestamp:** resolve deterministically before MERGE; Silver ends with one row.

**Missing field:** schema audit records `MISSING_FIELD`; malformed location does not break others.

**New field `cloud_cover`:** schema audit records `NEW_FIELD`; continue if required contract remains valid.

**Unequal arrays:** record `ARRAY_LENGTH_MISMATCH`; never truncate silently.

**Empty/invalid response:** create clear audit/error outcome rather than cryptic downstream Spark failure.

## 21. Validation notebook
Include demo-friendly queries:
```sql
-- duplicate keys: expect zero rows
SELECT location, timestamp, COUNT(*) cnt
FROM silver_weather_hourly
GROUP BY location, timestamp
HAVING COUNT(*) > 1;

-- invalid humidity: expect zero rows
SELECT * FROM silver_weather_hourly
WHERE humidity < 0 OR humidity > 100;

-- null timestamps: expect zero rows
SELECT * FROM silver_weather_hourly
WHERE timestamp IS NULL;

SELECT * FROM pipeline_run_audit ORDER BY start_time DESC;
SELECT * FROM location_run_audit ORDER BY request_start_time DESC;
SELECT * FROM weather_quarantine ORDER BY quarantined_at DESC;
SELECT * FROM schema_change_audit ORDER BY detected_at DESC;
```

## 22. Idempotency proof
Demonstrate the same forecast window twice.
```text
FIRST RUN
Bronze = 5 pulls
Silver = X logical rows
Gold   = Y logical rows

SAME-WINDOW RERUN
Bronze = 10 pulls   <- grows
Silver = X          <- unchanged logically
Gold   = Y          <- unchanged logically
```
This must be easy to show during the interview.

## 23. Automated tests
Where feasible test pure functions for:
- request parameter construction;
- retry/error classification;
- expected/new/missing/type-change schema cases;
- unequal arrays;
- humidity boundaries;
- negative precipitation;
- invalid timestamps/types;
- normal payload row count;
- duplicate resolution;
- same-input idempotency.

## 24. README requirements
Create a polished README containing:
1. problem statement;
2. architecture;
3. repository structure;
4. setup/run instructions;
5. table descriptions;
6. DQ rules;
7. retry/error policy;
8. idempotency strategy;
9. schema-drift strategy;
10. monitoring/dashboard;
11. edge-case demo instructions;
12. productionization plan;
13. AI usage disclosure;
14. known limitations.

Architecture:
```text
Configuration
     |
Open-Meteo API
     |
     | retry / timeout / isolation
     v
Bronze Delta (raw append)
     |
     | schema checks + flatten + type + DQ
     +-------------> Quarantine
     |
     v
Silver Delta
MERGE(location,timestamp)
     |
     v
Gold Delta
     |
Databricks Dashboard

Cross-cutting: Run Audit | Location Audit | Schema Audit | Monitoring
```

## 25. AI usage documentation
Document AI usage transparently. Suggested wording:
> AI was used to accelerate boilerplate generation, review retry/error scenarios, generate initial test cases, and review PySpark transformations. All generated code was reviewed against the API contract and validated with controlled failure scenarios.

Good prompt example:
> Generate a PySpark validation function that separates valid weather observations from invalid observations, preserving rejected rows with all applicable validation reasons. Do not silently drop invalid records.

Good AI mistake example:
> An initial implementation used Python `zip()` directly over hourly arrays. Unequal lengths could silently truncate records, so explicit array-length validation was added before flattening.

## 26. Productionization notes
Document, but do not overbuild:
- Databricks Workflows/Jobs scheduling;
- Dev/Test/Prod separation;
- CI/CD;
- optional Databricks Asset Bundles;
- Unity Catalog governance/access/lineage;
- secure secrets;
- alerts on FAILED/PARTIAL_SUCCESS/staleness/rejection spikes;
- retention/cost controls;
- testing gates;
- API rate limiting and bounded concurrency.

For thousands of locations, do not create uncontrolled API concurrency. Respect provider limits; use bounded concurrency, rate limiting, retry/dead-letter patterns, and incremental processing.

## 27. Optional DAB
Only after the core path works. Desired dependency:
```text
ingest_bronze -> bronze_to_silver -> silver_to_gold -> validation
```
Create bundle resources compatible with the actual workspace/CLI. Do not invent unsupported Free Edition features. Validate before claiming it works.

## 28. Demo flow
- **0–3 min:** architecture, layers, keys, audits, AI usage.
- **3–7 min:** config, API/retry, Bronze, schema checks, DQ/quarantine, Silver MERGE, audits.
- **7–10 min:** successful five-location run.
- **10–15 min:** deliberate failure + at least three edge cases; show audit/quarantine outcomes.
- **15–17 min:** Gold/dashboard + pipeline/data health.
- **17–20 min:** scheduling, environments, DAB/CI-CD, governance, alerting, scaling, testing.

## 29. Implementation priority
Implement in this exact order:
```text
1. Workspace/schema setup
2. Configuration
3. Run ID
4. RUNNING audit
5. API client
6. Retry/error handling
7. Location isolation
8. Bronze append
9. Location audit
10. Raw schema inspection
11. Schema-change audit
12. Array-length validation
13. Flatten
14. Safe casting
15. DQ rules
16. Quarantine
17. Deterministic source dedup
18. Silver MERGE
19. Gold aggregation
20. Gold rerun-safe write
21. Final audit
22. Monitoring SQL/views
23. Dashboard
24. Controlled failures
25. Edge-case tests
26. Same-window idempotency proof
27. README
28. Demo/slides
29. Optional DAB
```

## 30. Constraints for Claude Code
- Do not replace Delta architecture with pandas-only processing.
- Do not flatten before Bronze.
- Do not overwrite Bronze.
- Do not blindly append Silver or Gold.
- Do not silently drop invalid records.
- Do not use `zip()` before array-length validation.
- Do not let one city exception terminate ingestion.
- Do not automatically accept arbitrary schema changes.
- Do not use unlimited retries.
- Do not over-engineer.
- Do not fabricate unsupported Databricks Free Edition features.
- Keep execution understandable for a live demo.
- Add comments for important engineering decisions.
- Prefer deterministic behavior and testable functions.
- Keep failure injection clearly separate from normal execution.

## 31. Definition of done
- [ ] 5+ configurable locations.
- [ ] 3–7 days hourly weather.
- [ ] Temperature/humidity/precipitation/wind included.
- [ ] Timeout and retry handling.
- [ ] Per-location failure isolation.
- [ ] Raw append-only Bronze.
- [ ] Schema drift detection/logging.
- [ ] Array mismatch handling.
- [ ] Typed Silver + explicit DQ.
- [ ] Quarantine with reasons.
- [ ] Deterministic source dedup.
- [ ] Silver Delta MERGE on `(location,timestamp)`.
- [ ] No duplicate Silver business keys.
- [ ] Gold daily metrics.
- [ ] Gold rerun safety.
- [ ] Run audit.
- [ ] Location audit.
- [ ] Schema audit.
- [ ] Dashboard business + pipeline + data health.
- [ ] Freshness visible.
- [ ] Same-window rerun proof.
- [ ] Controlled location failure.
- [ ] Invalid humidity scenario.
- [ ] Datatype-change scenario.
- [ ] At least three assignment edge cases demo-ready.
- [ ] README.
- [ ] AI usage documented.
- [ ] Production plan documented.
- [ ] Optional DAB only after core solution works.

## Expected outcome
The finished project should clearly demonstrate:

**Reliability + idempotency + failure isolation + data quality + schema handling + auditability + monitoring + useful Gold outputs + production awareness**, while remaining simple enough for the candidate to explain every important line and trade-off during the interview.
