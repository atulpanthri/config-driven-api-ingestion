-- Gold: daily earthquake activity per region (UTC date). See sql/gold/open_meteo_weather.sql
-- for the placeholder/parameter contract shared by all gold SQL files.
SELECT
    s.region_id,
    max_by(s.region_name, s.updated_at)                 AS region_name,
    s.event_date_utc,
    COUNT(*)                                            AS quake_count,
    ROUND(MAX(s.magnitude), 2)                          AS max_magnitude,
    ROUND(AVG(s.magnitude), 2)                          AS avg_magnitude,
    max_by(s.place, s.magnitude)                        AS strongest_place,
    ROUND(AVG(s.depth_km), 1)                           AS avg_depth_km,
    COUNT_IF(s.magnitude >= :significant_mag)           AS significant_quakes
FROM {silver} AS s
JOIN {affected_keys} AS k
  ON s.region_id = k.region_id AND s.event_date_utc = k.event_date_utc
GROUP BY s.region_id, s.event_date_utc
