-- Replace ${BUCKET} before running in Athena. Explicit DDL was chosen instead
-- of a crawler so the schema and metadata contract are visible and versioned.
CREATE EXTERNAL TABLE IF NOT EXISTS weather_learning_lab.fact_weather_hourly (
  location_id string,
  observed_at timestamp,
  temperature_c double,
  humidity_pct int,
  precipitation_mm double,
  weather_code int,
  wind_speed_kmh double,
  ingested_at timestamp,
  temperature_band string
)
PARTITIONED BY (year int, month int, day int)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/curated/fact_weather_hourly/';

CREATE EXTERNAL TABLE IF NOT EXISTS weather_learning_lab.dim_location (
  location_id string,
  city string,
  state string,
  latitude double,
  longitude double
)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/curated/dim_location/';

MSCK REPAIR TABLE weather_learning_lab.fact_weather_hourly;

