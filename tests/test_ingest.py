import json
from pathlib import Path

import pytest

from src.ingest import LOCATIONS, envelope, raw_key, write_local, write_s3
from src.run_athena import split_sql


def sample_payload():
    return {"hourly": {"time": ["2025-01-01T00:00"], "temperature_2m": [20.0]}}


def test_raw_key_is_stable_for_same_logical_extract():
    location = LOCATIONS[0]
    assert raw_key(location, "2025-01-01", "2025-01-02") == raw_key(
        location, "2025-01-01", "2025-01-02"
    )


def test_local_rerun_overwrites_instead_of_creating_duplicate(tmp_path: Path):
    location = LOCATIONS[0]
    key = raw_key(location, "2025-01-01", "2025-01-01")
    first = envelope(sample_payload(), location, "2025-01-01", "2025-01-01")
    second = envelope(sample_payload(), location, "2025-01-01", "2025-01-01")
    path = write_local(first, tmp_path, key)
    write_local(second, tmp_path, key)
    assert len(list(tmp_path.rglob("*.json"))) == 1
    assert json.loads(path.read_text(encoding="utf-8"))["_ingestion"]["location_id"] == location.location_id


class FakeS3:
    def __init__(self):
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)


def test_s3_write_sets_content_type_and_idempotency_metadata():
    fake = FakeS3()
    uri = write_s3(sample_payload(), "lab-bucket", "raw/test.json", fake)
    assert uri == "s3://lab-bucket/raw/test.json"
    assert fake.calls[0]["ContentType"] == "application/json"
    assert "idempotency-key" in fake.calls[0]["Metadata"]


def test_split_sql_ignores_comments_and_empty_statements():
    sql = "-- comment\nSELECT 1;\n\n-- another\nSELECT 2;;"
    assert split_sql(sql) == ["SELECT 1", "SELECT 2"]
