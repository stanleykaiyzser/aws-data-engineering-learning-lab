"""Execute the lab SQL in Athena and persist small, inspectable evidence."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def split_sql(text: str) -> list[str]:
    without_comments = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("--")
    )
    return [statement.strip() for statement in without_comments.split(";") if statement.strip()]


def execute(client: Any, query: str, database: str, workgroup: str) -> dict[str, Any]:
    query_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )["QueryExecutionId"]

    while True:
        execution = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = execution["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELLED"}:
            reason = execution["Status"].get("StateChangeReason", "sem detalhe")
            raise RuntimeError(f"Athena {state}: {reason}\nSQL: {query}")
        time.sleep(2)

    statistics = execution.get("Statistics", {})
    result: dict[str, Any] = {
        "query_id": query_id,
        "sql": query,
        "data_scanned_bytes": statistics.get("DataScannedInBytes", 0),
        "engine_time_ms": statistics.get("EngineExecutionTimeInMillis", 0),
    }
    if query.lstrip().upper().startswith("SELECT"):
        rows = client.get_query_results(QueryExecutionId=query_id, MaxResults=10)["ResultSet"]["Rows"]
        result["sample_rows"] = [
            [column.get("VarCharValue") for column in row.get("Data", [])] for row in rows
        ]
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--database", default="weather_learning_lab")
    parser.add_argument("--workgroup", default="weather-learning-lab")
    parser.add_argument("--project-dir", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()

    import boto3

    client = boto3.client("athena", region_name=args.region)
    executed: list[dict[str, Any]] = []
    for relative_path in ("sql/01_catalog.sql", "sql/02_learning_queries.sql"):
        sql_text = (args.project_dir / relative_path).read_text(encoding="utf-8")
        for index, statement in enumerate(split_sql(sql_text.replace("${BUCKET}", args.bucket)), 1):
            print(f"ATHENA {relative_path} statement={index}")
            executed.append(execute(client, statement, args.database, args.workgroup))

    evidence = {
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "region": args.region,
        "bucket": args.bucket,
        "queries": executed,
    }
    evidence_dir = args.project_dir / "evidence"
    evidence_dir.mkdir(exist_ok=True)
    (evidence_dir / "athena_run.json").write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    for item in executed:
        print(
            "ATHENA_OK",
            f"scanned_bytes={item['data_scanned_bytes']}",
            f"engine_ms={item['engine_time_ms']}",
            f"query_id={item['query_id']}",
        )


if __name__ == "__main__":
    main()
