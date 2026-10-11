# Onboarding a new API source (configuration only)

A new pipeline needs **no Python changes**. You add three things, run the tests, and deploy.

## 1. Write `sources/<source_name>.yml`
Copy the closer of the two examples:
- [`sources/open_meteo_weather.yml`](../sources/open_meteo_weather.yml) — *columnar* response (parallel arrays), no pagination
- [`sources/usgs_earthquakes.yml`](../sources/usgs_earthquakes.yml) — *records* response (list of objects), offset pagination

| Section | What to set |
|---|---|
| `extraction` | `type: api` (implemented); `file` / `db` are planned and rejected by validation for now |
| `target` | bronze/silver/gold table names; they are created in `met_raw` / `met_silver` / `met_gold` (from `framework.yml`) |
| `request` | `base_url`, `params` with `{entity.x}` / `{window.x}` templates, `auth`, `pagination` |
| `retry` | attempts, backoff, which HTTP statuses are retryable |
| `window` | time window values (`int`, `date`, `string`, or `relative_date` with `offset_days`) |
| `entities` | the list of things to call the API for (cities, regions, accounts, ...) and their typed attributes |
| `response` | `format` (`columnar`/`records`), `data_path`, the field **contract**, `ignore_fields`, `allow_empty` |
| `silver` | merge `keys`, typed `columns` with DQ rules (`required`, `min`, `max`, `allowed_values`, custom reason codes), `derived` columns |
| `gold` | `sql_file`, `keys`, and `params` for the SQL |
| `fault_injection` | demo scenarios for this source (optional) |

**Auth.** Never put a key in the YAML (validation rejects credential-looking params). Store it in a
Databricks secret scope and reference it:
```yaml
auth: {type: api_key, in: header, name: X-API-Key, secret_scope: my_scope, secret_key: my_api_key}
```
```bash
databricks secrets create-scope my_scope -p Atul
databricks secrets put-secret my_scope my_api_key -p Atul
```
The key is read at run time with `dbutils.secrets.get` and masked (`***`) in bronze, audits and errors.

**Pagination types:** `none`, `offset` (`offset_param`, `limit_param`, `limit`, `start`),
`page_number` (`page_param`, `start`, optional `total_pages_path`), `cursor` (`cursor_path`, `cursor_param`),
`next_url` (`next_url_path`). Every type is bounded by `max_pages`; hitting the cap fails the entity
(`PAGINATION_LIMIT`) instead of silently truncating.

## 2. Save sample responses as fixtures
Save real responses to `tests/fixtures/<source_name>/<entity_id>.json` (page 1) and
`<entity_id>_p<N>.json` (later pages). The easiest way is the engine itself:
```python
from ingest_framework.source_config import load_source, resolve_window
from ingest_framework.api_client import fetch_entity
import requests
cfg = load_source(".", "<source_name>")
for e in cfg.entities:
    pages, _ = fetch_entity(requests.Session(), cfg, e, resolve_window(cfg, {}))
    for p in pages:
        (cfg.fixtures_dir / (f"{e.id}.json" if p.page == 1 else f"{e.id}_p{p.page}.json")).write_text(p.body_text)
```

## 3. Write `sql/gold/<source_name>.sql`
A `SELECT` over `{silver}` joined to `{affected_keys}` on the gold keys, using `:param` values from
`gold.params`. The engine adds `last_updated_run_id`/`updated_at`, creates the gold table from the
query's output schema, and MERGEs only the affected keys. See the two existing files.

## 4. Run the tests
```powershell
.\.venv\Scripts\python -m pytest -q
```
`tests/test_onboarding.py` automatically picks up every `sources/*.yml` and checks that it validates,
runs end-to-end on its fixtures, produces unique silver keys, matches the declared column types, and
that its fault scenarios only affect the target entity.

## 5. Deploy and run
```powershell
databricks bundle deploy -t dev -p Atul
databricks bundle run weather_pipeline_job -t dev -p Atul --params source=<source_name>
```
The setup task creates the source's schema, tables and landing volume. The shared ops tables and
dashboard health tiles pick the new source up through `source_name`. If serverless compute cannot reach
the API host, run with `source_mode=fixture` and note it.
