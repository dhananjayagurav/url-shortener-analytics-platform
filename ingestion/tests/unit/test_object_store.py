from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from url_shortener_analytics.exceptions import ObjectStoreWriteError
from url_shortener_analytics.object_store import build_bronze_key, write_bronze

RUN_DATE = datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC)


def test_build_bronze_key_is_deterministic_per_table_and_day() -> None:
    key_1 = build_bronze_key("clicks", RUN_DATE)
    key_2 = build_bronze_key("clicks", datetime(2026, 9, 19, 23, 59, tzinfo=UTC))
    assert key_1 == key_2 == "bronze/clicks/ingestion_date=2026-09-19/clicks.parquet"


def test_build_bronze_key_differs_by_table_and_by_day() -> None:
    assert build_bronze_key("clicks", RUN_DATE) != build_bronze_key("urls", RUN_DATE)
    assert build_bronze_key("clicks", RUN_DATE) != build_bronze_key(
        "clicks", datetime(2026, 9, 20, 0, 0, tzinfo=UTC)
    )


def test_write_bronze_calls_put_object_with_the_deterministic_key() -> None:
    s3_client = MagicMock()
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})

    key = write_bronze(df, "widgets", RUN_DATE, s3_client, bucket="test-bucket")

    assert key == "bronze/widgets/ingestion_date=2026-09-19/widgets.parquet"
    s3_client.put_object.assert_called_once()
    call_kwargs = s3_client.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "test-bucket"
    assert call_kwargs["Key"] == key
    assert len(call_kwargs["Body"]) > 0  # real Parquet bytes, not empty


def test_write_bronze_retries_then_succeeds() -> None:
    s3_client = MagicMock()
    error = ClientError({"Error": {"Code": "SlowDown", "Message": "throttled"}}, "PutObject")
    s3_client.put_object.side_effect = [error, error, None]  # fails twice, succeeds on 3rd
    df = pd.DataFrame({"id": [1]})

    key = write_bronze(df, "widgets", RUN_DATE, s3_client, bucket="test-bucket", backoff_seconds=0)

    assert key == "bronze/widgets/ingestion_date=2026-09-19/widgets.parquet"
    assert s3_client.put_object.call_count == 3


def test_write_bronze_raises_after_exhausting_retries() -> None:
    s3_client = MagicMock()
    error = ClientError({"Error": {"Code": "SlowDown", "Message": "throttled"}}, "PutObject")
    s3_client.put_object.side_effect = error
    df = pd.DataFrame({"id": [1]})

    with pytest.raises(ObjectStoreWriteError):
        write_bronze(df, "widgets", RUN_DATE, s3_client, bucket="test-bucket", max_attempts=2, backoff_seconds=0)

    assert s3_client.put_object.call_count == 2
