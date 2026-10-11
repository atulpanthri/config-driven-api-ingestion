# AI use log

Truthful record of how AI was used. Add entries as work continues; do not invent history.

## Tools
* Claude Code (model: Claude Opus 5.5) in VS Code: execution plan, code, tests, README.
* The candidate reviews, runs and owns all output.

## Prompts that worked (2026-10-07)
* "read @DATABRICKS_WEATHER_IMPLEMENTATION.md and prepare the execution steps in current folder"
  → produced `EXECUTION_STEPS.md`, after checking the actual environment (Python versions, Databricks CLI
  profile) and resolving conflicts between the spec and project context (e.g. DAB moved from "optional, last"
  to core work, per the candidate's decision).
* "create the folder structure and the code" → produced the package, notebooks, bundle, SQL and tests.
  Before coding, the assistant called the real API to confirm parameter names, units, the 400 error body and
  `start_date`/`end_date` support, and saved real responses as fixtures.

## Mistakes found and corrected
1. **Fault-injection default target was inconsistent (real, 2026-10-07).**
   The generated code defaulted the fault target to "the second configured location" (Singapore), while the
   notebook widget and documentation said London.
   *How found:* the local Spark smoke test output showed `singapore: HTTP_4XX` in the run audit where London
   was expected. The unit tests had not caught it because they always passed an explicit target.
   *Fix:* explicit `fault_location_id` in `config/pipeline_config.json` (validated against configured IDs),
   used by code, notebook and job; a unit test now covers the default.
2. **Placeholder line left in a notebook (real, 2026-10-07).**
   The first version of `notebooks/01_run_pipeline.py` ended with a nonsense import placeholder
   (`from weather_pipeline_faults import ...`). *How found:* reviewing the file right after it was written.
   *Fix:* the notebook was rewritten before any run.

3. **Fault injection mutated every page of a paginated response (real, 2026-10-09).**
   After generalising fault injection for the framework, payload faults were applied to every response of the
   target entity. For USGS Japan, page 2 is an empty `features` list, so `set_value index 0` raised
   `IndexError` and the entity failed with `UNEXPECTED_ERROR` instead of quarantining one row.
   *How found:* 3 failing tests in `tests/test_onboarding.py::test_usgs_scenarios`.
   *Fix:* the target's first successful response (page 1) is mutated once; later pages pass through.
4. **YAML integers sent into a DOUBLE column (real, 2026-10-09).**
   Entity attributes such as `min_latitude: 30` were read by YAML as `int` and passed unchanged to Spark,
   which rejected them for a `DOUBLE` column (`CANNOT_ACCEPT_OBJECT_IN_TYPE`). Every USGS run became FAILED.
   *How found:* the local Spark smoke test; the pure-Python tests did not catch it.
   *Fix:* entity attributes are coerced to their declared types when the config loads, and a new onboarding
   test checks every silver value's Python type against its declared column type, so this class of bug is now
   caught without Spark.

5. **Dashboard widgets the renderer rejected (real, 2026-10-10).**
   The first generated dashboards were written from one example file. After deployment, the candidate reported
   "invalid widget / unable to render visualization" errors in the UI.
   *How found:* the candidate opened the dashboards. The AI then compared its JSON with 27 published Databricks
   example dashboards and found these deviations:
   - a counter used a nested expression (`ROUND(SUM(...), 1)`); no example ever nests widget expressions
   - the governance "Schema" filter targeted a dataset without a `schema_name` column
   - pages lacked `pageType`/`layoutVersion`, text widgets used the rarer `textbox_spec`, and tables used a
     hand-built v1 column spec
   *Fix:*
   - nested expressions removed (computed in dataset SQL instead)
   - the missing column added
   - v2 tables and `multilineTextboxSpec` used
   - a validator added to `scripts/build_dashboards.py` (`--validate` checks allowed expressions, grid
     overlaps, encodings, and every widget column against the real dataset schema from the warehouse)
   - a pytest check added
   Visual rendering is still confirmed by opening the dashboards in the UI.

6. **Dashboards filled only the left half of the page (real, 2026-10-10).**
   When fixing item 5, the AI added `layoutVersion: GRID_V1` (copied from the examples) but kept 6-column
   positions. GRID_V1 pages are 12 columns wide.
   *How found:* the candidate's screenshot showed every page squeezed into the left half. The examples confirmed
   it: GRID_V1 pages reach column 12, pages without a layoutVersion reach column 6.
   *Fix:* the generator doubles x/width for GRID_V1, and the validator now fails if a page does not span all
   12 columns.

7. **Gold tables had no created_run_id (real, 2026-10-11).**
   Silver kept `created_run_id`/`created_at` and `last_updated_run_id`/`updated_at`, but the AI-written gold
   MERGE used `UPDATE SET *` and only added the two `last_updated` columns. A "created" column would have been
   overwritten on every update.
   *How found:* the candidate asked why `met_gold.weather_daily` had no `created_run_id`.
   *Fix:*
   - gold gets all four lineage columns
   - the MERGE update lists its columns explicitly, as silver does
   - existing gold tables are upgraded in place, with `created_*` backfilled from the first silver load of each key
   - a unit test checks the MERGE never updates `created_*`; the smoke test checks the creator survives reruns

## Prompts that worked (framework, 2026-10-09)
* "what is the industry standard about this" (raw API responses as files vs. in a table) → an explanation of
  the landing-zone and raw-payload-in-bronze patterns, which led to adding Volume landing files.
* Plan feedback: "also add boiler plate coding, which allow to onboard new API based pipeline by configuration
  only" → the config-driven framework; design choices (table layout, YAML, API capabilities, gold as SQL) were
  confirmed with the candidate before coding.

## Environment issue found while testing (not an AI mistake)
* Local PySpark 3.5 Python workers crashed on Windows with Python 3.12 ("Python worker exited unexpectedly")
  and a system `SPARK_HOME` pointed to a different Spark build. The smoke test runs with Python 3.10 and
  `SPARK_HOME` unset. Databricks serverless is unaffected.
