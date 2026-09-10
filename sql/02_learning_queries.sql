-- A: deliberately scans all partitions. Record "Data scanned" in Athena.
SELECT count(*) AS rows_all_periods,
       round(avg(temperature_c), 2) AS avg_temperature_c
FROM weather_learning_lab.fact_weather_hourly;

-- B: partition pruning. Compare "Data scanned" with query A.
SELECT count(*) AS rows_one_day,
       round(avg(temperature_c), 2) AS avg_temperature_c
FROM weather_learning_lab.fact_weather_hourly
WHERE year = 2025 AND month = 1 AND day = 15;

-- Fact + small dimension lookup.
SELECT d.city,
       round(avg(f.temperature_c), 1) AS avg_temperature_c,
       round(sum(f.precipitation_mm), 1) AS precipitation_mm
FROM weather_learning_lab.fact_weather_hourly f
JOIN weather_learning_lab.dim_location d USING (location_id)
WHERE year = 2025 AND month = 1
GROUP BY d.city
ORDER BY avg_temperature_c DESC;

-- Ad hoc validation: duplicates should be zero.
SELECT location_id, observed_at, count(*) AS occurrences
FROM weather_learning_lab.fact_weather_hourly
GROUP BY location_id, observed_at
HAVING count(*) > 1;
