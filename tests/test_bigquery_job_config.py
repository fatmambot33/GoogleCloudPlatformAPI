import json
from datetime import date
from pathlib import Path

import pytest

from GoogleCloudPlatformAPI.BigQuery import BigQuery
from google.cloud import bigquery


def _write_schema(folder: Path, *, source_format: str = "CSV") -> None:
    folder.mkdir(parents=True, exist_ok=True)
    schema = {
        "allow_jagged_rows": True,
        "allow_quoted_newlines": True,
        "ignore_unknown_values": True,
        "source_format": source_format,
        "field_delimiter": ";",
        "skip_leading_rows": 1,
        "table_schema": [
            {"name": "col1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "col2", "type": "INTEGER", "mode": "REQUIRED"},
        ],
    }
    (folder / "schema.json").write_text(json.dumps(schema))


def test_build_job_config_csv(tmp_path: Path):
    folder = tmp_path / "my_table"
    _write_schema(folder, source_format="CSV")
    # Ensure partition folder exists so copy operation can succeed
    (folder / "2023-01-01").mkdir(parents=True, exist_ok=True)

    cfg, uri = BigQuery.build_job_config(
        table_name="ignored",
        bucket_name="bucket",
        data_path=str(folder) + "/",
        partition_date=date(2023, 1, 1),
    )

    # Validates basic properties of the generated LoadJobConfig
    assert isinstance(cfg, bigquery.LoadJobConfig)
    assert cfg.source_format == bigquery.SourceFormat.CSV
    assert cfg.field_delimiter == ";"
    assert cfg.skip_leading_rows == 1
    assert cfg.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    # URI should point to the partitioned folder and CSV pattern
    assert uri == "gs://bucket/my_table/2023-01-01/*.csv.gz"


def test_build_job_config_json(tmp_path: Path):
    folder = tmp_path / "my_table"
    _write_schema(folder, source_format="JSON")
    (folder / "2024-02-02").mkdir(parents=True, exist_ok=True)

    cfg, uri = BigQuery.build_job_config(
        table_name="ignored",
        bucket_name="bucket",
        data_path=str(folder) + "/",
        partition_date=date(2024, 2, 2),
    )

    assert isinstance(cfg, bigquery.LoadJobConfig)
    assert cfg.source_format == bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    assert cfg.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    assert uri == "gs://bucket/my_table/2024-02-02/*.json.gz"
