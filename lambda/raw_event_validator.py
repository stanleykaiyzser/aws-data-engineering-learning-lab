"""Tiny S3-event Lambda: validate that a RAW object looks like JSON.

This is intentionally not the transformation engine. It demonstrates a short,
event-driven boundary and can be left undeployed to avoid unnecessary moving parts.
"""

from __future__ import annotations

import json
import os
import urllib.parse


def handler(event, context, s3_client=None):
    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")
    checked = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        if not key.startswith("raw/") or not key.endswith(".json"):
            raise ValueError(f"Objeto fora do contrato RAW JSON: s3://{bucket}/{key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        json.loads(response["Body"].read())
        checked.append(f"s3://{bucket}/{key}")
    return {"statusCode": 200, "checked": checked, "requestId": getattr(context, "aws_request_id", None)}
