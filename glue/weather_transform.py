"""AWS Glue/PySpark job: RAW Open-Meteo JSON -> CURATED Parquet."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

HOURLY_FIELDS = [
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--RAW_PATH", required=True)
    parser.add_argument("--CURATED_PATH", required=True)
    parser.add_argument("--QUALITY_PATH", required=True)
    known, _ = parser.parse_known_args()
    return known


def require_raw_schema(df: DataFrame) -> None:
    required = {"_ingestion", "hourly"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Schema RAW inesperado; campos ausentes: {sorted(missing)}")
    hourly_fields = set(df.schema["hourly"].dataType.fieldNames())
    missing_hourly = set(HOURLY_FIELDS) - hourly_fields
    if missing_hourly:
        raise ValueError(f"Schema horário inesperado; campos ausentes: {sorted(missing_hourly)}")


def normalize(raw: DataFrame) -> DataFrame:
    require_raw_schema(raw)
    zipped = F.arrays_zip(*[F.col(f"hourly.{name}") for name in HOURLY_FIELDS])
    return (
        raw.select("_ingestion", F.explode(zipped).alias("hour"))
        .select(
            F.col("_ingestion.location_id").alias("location_id"),
            F.col("_ingestion.city").alias("city"),
            F.col("_ingestion.state").alias("state"),
            F.col("_ingestion.latitude").cast(T.DoubleType()).alias("latitude"),
            F.col("_ingestion.longitude").cast(T.DoubleType()).alias("longitude"),
            F.to_timestamp("hour.time").alias("observed_at"),
            F.col("hour.temperature_2m").cast(T.DoubleType()).alias("temperature_c"),
            F.col("hour.relative_humidity_2m").cast(T.IntegerType()).alias("humidity_pct"),
            F.col("hour.precipitation").cast(T.DoubleType()).alias("precipitation_mm"),
            F.col("hour.weather_code").cast(T.IntegerType()).alias("weather_code"),
            F.col("hour.wind_speed_10m").cast(T.DoubleType()).alias("wind_speed_kmh"),
            F.to_timestamp("_ingestion.fetched_at").alias("ingested_at"),
        )
        .withColumn(
            "temperature_band",
            F.when(F.col("temperature_c") < 15, "cold")
            .when(F.col("temperature_c") < 25, "mild")
            .otherwise("hot"),
        )
    )


def quality_metrics(df: DataFrame) -> dict[str, int]:
    invalid_condition = (
        F.col("location_id").isNull()
        | F.col("observed_at").isNull()
        | F.col("temperature_c").isNull()
        | F.col("humidity_pct").isNull()
        | F.col("precipitation_mm").isNull()
        | F.col("weather_code").isNull()
        | F.col("wind_speed_kmh").isNull()
        | ~F.col("humidity_pct").between(0, 100)
        | (F.col("precipitation_mm") < 0)
        | (F.col("wind_speed_kmh") < 0)
        | ~F.col("weather_code").between(0, 99)
    )
    total = df.count()
    invalid = df.filter(invalid_condition).count()
    duplicates = total - df.select("location_id", "observed_at").distinct().count()
    return {"row_count": total, "invalid_count": invalid, "duplicate_count": duplicates}


def deduplicate(df: DataFrame) -> DataFrame:
    latest = Window.partitionBy("location_id", "observed_at").orderBy(F.col("ingested_at").desc())
    return df.withColumn("_row_number", F.row_number().over(latest)).filter("_row_number = 1").drop("_row_number")


def transform(raw: DataFrame) -> tuple[DataFrame, DataFrame, dict[str, int]]:
    normalized = normalize(raw)
    metrics = quality_metrics(normalized)
    if metrics["invalid_count"]:
        raise ValueError(f"Data Quality falhou: {metrics}")
    fact = (
        deduplicate(normalized)
        .withColumn("year", F.year("observed_at"))
        .withColumn("month", F.month("observed_at"))
        .withColumn("day", F.dayofmonth("observed_at"))
    )
    dim_location = fact.select("location_id", "city", "state", "latitude", "longitude").distinct()
    metrics["output_row_count"] = fact.count()
    return fact.drop("city", "state", "latitude", "longitude"), dim_location, metrics


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("weather-learning-lab").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    raw = spark.read.option("multiline", "true").option("recursiveFileLookup", "true").json(args.RAW_PATH)
    fact, dim_location, metrics = transform(raw)
    fact.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
        f"{args.CURATED_PATH.rstrip('/')}/fact_weather_hourly"
    )
    dim_location.coalesce(1).write.mode("overwrite").parquet(
        f"{args.CURATED_PATH.rstrip('/')}/dim_location"
    )
    report = [{**metrics, "checked_at": datetime.now(timezone.utc).isoformat()}]
    spark.createDataFrame(report).coalesce(1).write.mode("overwrite").json(args.QUALITY_PATH)
    print(json.dumps(metrics, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
