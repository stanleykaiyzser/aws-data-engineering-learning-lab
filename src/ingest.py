"""Fetch a small real weather dataset and preserve the source response in RAW.

The stable object key makes a rerun for the same location and period idempotent:
the object is replaced instead of creating another logical batch.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

API_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_FIELDS = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
)


@dataclass(frozen=True)
class Location:
    location_id: str
    city: str
    state: str
    latitude: float
    longitude: float


LOCATIONS = (
    Location("belo_horizonte", "Belo Horizonte", "MG", -19.9167, -43.9345),
    Location("sao_paulo", "São Paulo", "SP", -23.5505, -46.6333),
    Location("rio_de_janeiro", "Rio de Janeiro", "RJ", -22.9068, -43.1729),
    Location("blumenau", "Blumenau", "SC", -26.9194, -49.0661),
)


def build_url(location: Location, start_date: str, end_date: str) -> str:
    params = {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_FIELDS),
        "timezone": "UTC",
    }
    return f"{API_URL}?{urlencode(params)}"


def fetch_json(url: str, attempts: int = 3, timeout_seconds: int = 30) -> dict[str, Any]:
    """Retry transient failures, but fail clearly after a small bounded wait."""
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(url, timeout=timeout_seconds) as response:  # noqa: S310
                payload = json.load(response)
            if "hourly" not in payload:
                raise ValueError("Resposta da fonte sem o objeto obrigatório 'hourly'.")
            return payload
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            last_error = exc
            logging.warning("Tentativa %s/%s falhou: %s", attempt, attempts, exc)
            if attempt < attempts:
                time.sleep(2 ** (attempt - 1))
    raise RuntimeError(f"Fonte indisponível ou resposta inválida após {attempts} tentativas") from last_error


def raw_key(location: Location, start_date: str, end_date: str) -> str:
    return (
        "raw/open_meteo/"
        f"period={start_date}_{end_date}/"
        f"location_id={location.location_id}/open_meteo.json"
    )


def envelope(
    payload: dict[str, Any], location: Location, start_date: str, end_date: str
) -> dict[str, Any]:
    return {
        "_ingestion": {
            **asdict(location),
            "source": "open_meteo_historical_weather_api",
            "start_date": start_date,
            "end_date": end_date,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        },
        **payload,
    }


def write_local(payload: dict[str, Any], base_dir: Path, key: str) -> Path:
    target = base_dir / key
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def write_s3(payload: dict[str, Any], bucket: str, key: str, s3_client: Any = None) -> str:
    if s3_client is None:
        import boto3  # imported only for the AWS execution path

        s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        Metadata={"idempotency-key": key.replace("/", "_")[:1024]},
    )
    return f"s3://{bucket}/{key}"


def run(start_date: str, end_date: str, bucket: str | None, local_dir: Path | None) -> list[str]:
    if bool(bucket) == bool(local_dir):
        raise ValueError("Informe exatamente um destino: --bucket ou --local-dir.")
    outputs: list[str] = []
    for location in LOCATIONS:
        url = build_url(location, start_date, end_date)
        raw = envelope(fetch_json(url), location, start_date, end_date)
        key = raw_key(location, start_date, end_date)
        destination = (
            write_s3(raw, bucket, key)
            if bucket
            else str(write_local(raw, local_dir, key))  # type: ignore[arg-type]
        )
        logging.info("RAW escrita: %s", destination)
        outputs.append(destination)
    return outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--bucket")
    destination.add_argument("--local-dir", type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    run(args.start_date, args.end_date, args.bucket, args.local_dir)

