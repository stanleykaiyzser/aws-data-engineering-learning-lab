"""Reference DAG: Airflow orchestrates; Python/Glue/Athena do the work."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator


def ingest(**context):
    from src.ingest import run

    day = context["ds"]
    run(day, day, context["params"]["bucket"], None)


default_args = {"retries": 2, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="weather_learning_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    params={"bucket": "replace-me", "database": "weather_learning_lab"},
    tags=["learning", "aws"],
) as dag:
    ingest_task = PythonOperator(task_id="ingest", python_callable=ingest)
    validate_raw = S3KeySensor(
        task_id="validate_raw",
        bucket_name="{{ params.bucket }}",
        bucket_key="raw/open_meteo/period={{ ds }}_{{ ds }}/*/open_meteo.json",
        wildcard_match=True,
        timeout=300,
    )
    glue_job = GlueJobOperator(task_id="glue_job", job_name="weather-learning-transform")
    validate_curated = S3KeySensor(
        task_id="validate_curated",
        bucket_name="{{ params.bucket }}",
        bucket_key="curated/fact_weather_hourly/year={{ ds[:4] }}/month={{ ds[5:7] | int }}/day={{ ds[8:10] | int }}/*.parquet",
        wildcard_match=True,
        timeout=300,
    )
    athena_check = AthenaOperator(
        task_id="athena_check",
        query="SELECT count(*) FROM fact_weather_hourly WHERE year={{ ds[:4] }} AND month={{ ds[5:7] | int }} AND day={{ ds[8:10] | int }}",
        database="{{ params.database }}",
        output_location="s3://{{ params.bucket }}/athena-results/",
    )

    ingest_task >> validate_raw >> glue_job >> validate_curated >> athena_check
