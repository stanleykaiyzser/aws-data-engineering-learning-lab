import importlib.util
import io
import json
from pathlib import Path

import pytest

module_path = Path(__file__).parents[1] / "lambda" / "raw_event_validator.py"
spec = importlib.util.spec_from_file_location("raw_event_validator", module_path)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)
handler = module.handler


class FakeS3:
    def get_object(self, **kwargs):
        body = json.dumps({"hourly": {"time": []}}).encode()
        return {"Body": io.BytesIO(body), "ContentLength": len(body)}


def event_for(key):
    return {"Records": [{"s3": {"bucket": {"name": "lab"}, "object": {"key": key}}}]}


def test_accepts_raw_json():
    result = handler(event_for("raw/open_meteo/x.json"), object(), FakeS3())
    assert result["statusCode"] == 200


def test_rejects_non_raw_object():
    with pytest.raises(ValueError, match="fora do contrato"):
        handler(event_for("curated/x.parquet"), object(), FakeS3())
