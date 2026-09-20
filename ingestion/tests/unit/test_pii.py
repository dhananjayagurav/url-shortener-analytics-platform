from __future__ import annotations

from pathlib import Path

import pytest

from url_shortener_analytics.exceptions import ContractError
from url_shortener_analytics.pii import PiiColumn, classify_all_contracts, classify_table

VALID_CONTRACT = {
    "table": "widgets",
    "columns": [
        {"name": "id", "type": "integer", "nullable": False, "pii": "none", "description": "PK"},
        {"name": "owner_email", "type": "text", "nullable": False, "pii": "direct", "description": "owner's email"},
        {"name": "owner_id", "type": "integer", "nullable": True, "pii": "pseudonymized", "description": "FK"},
    ],
}


def test_classify_table_returns_only_non_none_columns() -> None:
    result = classify_table(VALID_CONTRACT)
    assert len(result) == 2
    assert {c.column for c in result} == {"owner_email", "owner_id"}


def test_classify_table_reports_the_correct_category_per_column() -> None:
    result = classify_table(VALID_CONTRACT)
    by_column = {c.column: c for c in result}
    assert by_column["owner_email"].category == "direct"
    assert by_column["owner_id"].category == "pseudonymized"
    assert by_column["owner_email"].table == "widgets"


def test_classify_table_returns_pii_column_dataclass_instances() -> None:
    result = classify_table(VALID_CONTRACT)
    assert all(isinstance(c, PiiColumn) for c in result)


def test_classify_table_returns_an_empty_list_when_every_column_is_none() -> None:
    contract = {
        "table": "widgets",
        "columns": [{"name": "id", "type": "integer", "nullable": False, "pii": "none"}],
    }
    assert classify_table(contract) == []


def test_classify_table_raises_when_a_column_is_missing_the_pii_field() -> None:
    # Real gap this guards against (Section 23): a column added to a
    # contract without anyone making an explicit PII call must fail
    # loudly, not silently default to "none".
    contract = {
        "table": "widgets",
        "columns": [
            {"name": "id", "type": "integer", "nullable": False, "pii": "none"},
            {"name": "secret", "type": "text", "nullable": True},  # no `pii` key
        ],
    }
    with pytest.raises(ContractError, match="secret"):
        classify_table(contract)


def test_classify_table_raises_when_a_column_has_an_invalid_pii_value() -> None:
    contract = {
        "table": "widgets",
        "columns": [
            {"name": "id", "type": "integer", "nullable": False, "pii": "sort-of"},
        ],
    }
    with pytest.raises(ContractError, match="sort-of"):
        classify_table(contract)


def test_classify_all_contracts_against_the_real_contracts_directory() -> None:
    """The actual contracts/source/*.yaml files, classified for real --
    proves the real, committed contracts all declare a valid `pii` value
    for every column, and that the known PII columns are found."""
    real_contracts_dir = Path(__file__).resolve().parents[3] / "contracts" / "source"

    results = classify_all_contracts(real_contracts_dir)
    found = {(c.table, c.column): c.category for c in results}

    assert found == {
        ("users", "email"): "direct",
        ("urls", "user_id"): "pseudonymized",
        ("clicks", "hashed_ip"): "pseudonymized",
        ("clicks", "user_id"): "pseudonymized",
    }
