-- Gold: daily weather per location and UTC date.
-- Contract for gold SQL files (applies to every source):
--   * {silver}        -> this source's silver table (identifier, filled in by the engine)
--   * {affected_keys} -> a view with the gold key columns touched by the current run
--   * :name           -> values from gold.params in the source YAML
--   * output must contain the gold.keys columns; the engine adds last_updated_run_id/updated_at
--     and MERGEs the result on gold.keys, so only affected keys are recomputed.
SELECT
    s.location_id,
    max_by(s.location_name, s.updated_at)        AS location_name,
    s.weather_date_utc,
    ROUND(MIN(s.temperature_c), 2)               AS min_temperature_c,
    ROUND(MAX(s.temperature_c), 2)               AS max_temperature_c,
    ROUND(AVG(s.temperature_c), 2)               AS avg_temperature_c,
    ROUND(SUM(s.precipitation_mm), 2)            AS total_precipitation_mm,
    ROUND(MAX(s.wind_speed_kmh), 2)              AS max_wind_speed_kmh,
    ROUND(AVG(s.relative_humidity_pct), 2)       AS avg_relative_humidity_pct,
    COUNT_IF(s.precipitation_mm >= :rainy_mm)    AS precipitation_hours,
    COUNT(*)                                     AS valid_hours,
    COUNT(*) = 24                                AS is_complete_day,
    MAX(s.temperature_c) >= :heat_c              AS heat_alert,
    MAX(s.wind_speed_kmh) >= :wind_kmh           AS high_wind_alert,
    SUM(s.precipitation_mm) >= :heavy_mm         AS heavy_precipitation_alert
FROM {silver} AS s
JOIN {affected_keys} AS k
  ON s.location_id = k.location_id AND s.weather_date_utc = k.weather_date_utc
GROUP BY s.location_id, s.weather_date_utc
