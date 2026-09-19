from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from url_shortener_analytics.exceptions import ObjectStoreWriteError
from url_shortener_analytics.object_store import (
    build_bronze_incremental_key,
    build_bronze_key,
    head_object,
    list_bronze_keys,
    write_bronze,
    write_bronze_incremental,
)

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


def test_build_bronze_incremental_key_is_deterministic_per_watermark_range() -> None:
    key_1 = build_bronze_incremental_key("clicks", 100, 250)
    key_2 = build_bronze_incremental_key("clicks", 100, 250)
    assert key_1 == key_2
    assert key_1 == (
        "bronze/clicks/incremental/watermark_start=000000000100/watermark_end=000000000250/clicks.parquet"
    )


def test_build_bronze_incremental_key_differs_by_table_and_by_range() -> None:
    assert build_bronze_incremental_key("clicks", 100, 250) != build_bronze_incremental_key("urls", 100, 250)
    assert build_bronze_incremental_key("clicks", 100, 250) != build_bronze_incremental_key("clicks", 100, 300)
    # Adjacent, non-overlapping batches (250->250 then 250->400) get different
    # keys -- this is deliberate, see the function's own docstring for when
    # this guarantee does and doesn't give a clean overwrite on retry.
    assert build_bronze_incremental_key("clicks", 0, 250) != build_bronze_incremental_key("clicks", 250, 400)


def test_write_bronze_incremental_calls_put_object_with_the_deterministic_key() -> None:
    s3_client = MagicMock()
    df = pd.DataFrame({"id": [101, 102, 103], "value": ["a", "b", "c"]})

    key = write_bronze_incremental(df, "clicks", 100, 103, s3_client, bucket="test-bucket")

    assert key == "bronze/clicks/incremental/watermark_start=000000000100/watermark_end=000000000103/clicks.parquet"
    s3_client.put_object.assert_called_once()
    call_kwargs = s3_client.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "test-bucket"
    assert call_kwargs["Key"] == key
    assert len(call_kwargs["Body"]) > 0


def test_write_bronze_incremental_retries_then_succeeds() -> None:
    s3_client = MagicMock()
    error = ClientError({"Error": {"Code": "SlowDown", "Message": "throttled"}}, "PutObject")
    s3_client.put_object.side_effect = [error, None]
    df = pd.DataFrame({"id": [101]})

    key = write_bronze_incremental(df, "clicks", 100, 101, s3_client, bucket="test-bucket", backoff_seconds=0)

    assert key == "bronze/clicks/incremental/watermark_start=000000000100/watermark_end=000000000101/clicks.parquet"
    assert s3_client.put_object.call_count == 2


def test_head_object_returns_metadata_when_the_object_exists() -> None:
    s3_client = MagicMock()
    s3_client.head_object.return_value = {"ContentLength": 123}

    result = head_object(s3_client, "test-bucket", "bronze/clicks/x.parquet")

    assert result == {"ContentLength": 123}
    s3_client.head_object.assert_called_once_with(Bucket="test-bucket", Key="bronze/clicks/x.parquet")


def test_head_object_returns_none_when_the_object_is_missing() -> None:
    s3_client = MagicMock()
    s3_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
    )

    assert head_object(s3_client, "test-bucket", "bronze/clicks/missing.parquet") is None


def test_head_object_reraises_unrelated_client_errors() -> None:
    s3_client = MagicMock()
    s3_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "HeadObject"
    )

    with pytest.raises(ClientError):
        head_object(s3_client, "test-bucket", "bronze/clicks/x.parquet")


def test_list_bronze_keys_returns_every_key_under_the_prefix() -> None:
    s3_client = MagicMock()
    s3_client.list_objects_v2.return_value = {
        "Contents": [{"Key": "bronze/clicks/a.parquet"}, {"Key": "bronze/clicks/b.parquet"}]
    }

    keys = list_bronze_keys(s3_client, "test-bucket")

    assert keys == ["bronze/clicks/a.parquet", "bronze/clicks/b.parquet"]
    s3_client.list_objects_v2.assert_called_once_with(Bucket="test-bucket", Prefix="bronze/")


def test_list_bronze_keys_returns_empty_list_when_the_prefix_has_no_objects() -> None:
    s3_client = MagicMock()
    s3_client.list_objects_v2.return_value = {}  # no "Contents" key at all -- an empty bucket/prefix

    assert list_bronze_keys(s3_client, "test-bucket") == []
