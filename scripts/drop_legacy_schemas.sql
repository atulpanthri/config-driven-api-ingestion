-- One-off cleanup (2026-10-10): drop this project's pre-"met_*" schemas.
-- Removes their tables and landing volumes (raw files) permanently.
-- Not touched: bookstore_eng_pro, default (not created by this project).
DROP SCHEMA IF EXISTS workspace.weather            CASCADE;
DROP SCHEMA IF EXISTS workspace.weather_edge       CASCADE;
DROP SCHEMA IF EXISTS workspace.earthquakes        CASCADE;
DROP SCHEMA IF EXISTS workspace.earthquakes_edge   CASCADE;
DROP SCHEMA IF EXISTS workspace.ingest_ops         CASCADE;
DROP SCHEMA IF EXISTS workspace.ingest_ops_edge    CASCADE;
DROP SCHEMA IF EXISTS workspace.weather_demo       CASCADE;
DROP SCHEMA IF EXISTS workspace.weather_demo_edge  CASCADE;
