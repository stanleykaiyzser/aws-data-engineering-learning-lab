import json
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from glue.weather_transform import transform


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[2]").appName("lab-tests").getOrCreate()
    yield session
    session.stop()


def raw_row(fetched_at="2025-02-01T00:00:00+00:00", humidity=70):
    return {
        "_ingestion": {
            "location_id": "belo_horizonte",
            "city": "Belo Horizonte",
            "state": "MG",
            "latitude": -19.9,
            "longitude": -43.9,
            "fetched_at": fetched_at,
        },
        "hourly": {
            "time": ["2025-01-01T00:00"],
            "temperature_2m": [21.5],
            "relative_humidity_2m": [humidity],
            "precipitation": [0.0],
            "weather_code": [1],
            "wind_speed_10m": [8.0],
        },
    }


def test_transform_deduplicates_reruns(spark):
    raw = spark.read.json(spark.sparkContext.parallelize([json.dumps(raw_row()), json.dumps(raw_row("2025-02-02T00:00:00+00:00"))]))
    fact, dim, metrics = transform(raw)
    assert metrics["duplicate_count"] == 1
    assert fact.count() == 1
    assert dim.count() == 1


def test_quality_failure_is_controlled(spark):
    raw = spark.read.json(spark.sparkContext.parallelize([json.dumps(raw_row(humidity=150))]))
    with pytest.raises(ValueError, match="Data Quality falhou"):
        transform(raw)

