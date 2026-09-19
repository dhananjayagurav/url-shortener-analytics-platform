from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import Engine, text

from url_shortener_analytics.contracts import (
    load_contract,
    validate_all_contracts,
    validate_contract,
)
from url_shortener_analytics.exceptions import ContractError

VALID_CONTRACT = {
    "table": "widgets",
    "version": 1,
    "columns": [
        {"name": "id", "type": "integer", "nullable": False},
        {"name": "name", "type": "text", "nullable": True},
    ],
}


@pytest.fixture
def widgets_table(sqlite_engine: Engine) -> Engine:
    with sqlite_engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE widgets (id INTEGER NOT NULL, name TEXT, created_at TIMESTAMP)"
        ))
    return sqlite_engine


def test_validate_contract_passes_for_a_matching_table(widgets_table: Engine) -> None:
    contract = {**VALID_CONTRACT, "columns": VALID_CONTRACT["columns"]}
    result = validate_contract(widgets_table, contract)
    assert result.passed
    assert result.violations == []


def test_validate_contract_warns_but_does_not_fail_on_an_extra_column(widgets_table: Engine) -> None:
    # widgets_table has created_at, which VALID_CONTRACT doesn't declare.
    result = validate_contract(widgets_table, VALID_CONTRACT)
    assert result.passed  # extra columns are warnings, not violations
    assert len(result.warnings) == 1
    assert result.warnings[0].kind == "extra_column"
    assert result.warnings[0].column == "created_at"


def test_validate_contract_fails_on_a_missing_column(widgets_table: Engine) -> None:
    contract = {
        "table": "widgets",
        "columns": [
            *VALID_CONTRACT["columns"],
            {"name": "price", "type": "numeric", "nullable": False},
        ],
    }
    result = validate_contract(widgets_table, contract)
    assert not result.passed
    assert len(result.violations) == 1
    assert result.violations[0].kind == "missing_column"
    assert result.violations[0].column == "price"


def test_validate_contract_fails_on_a_type_mismatch(widgets_table: Engine) -> None:
    contract = {
        "table": "widgets",
        "columns": [
            {"name": "id", "type": "text", "nullable": False},  # wrong -- id is actually integer
            {"name": "name", "type": "text", "nullable": True},
        ],
    }
    result = validate_contract(widgets_table, contract)
    assert not result.passed
    kinds = [v.kind for v in result.violations]
    assert "type_mismatch" in kinds


def test_validate_contract_fails_on_a_nullability_mismatch(widgets_table: Engine) -> None:
    contract = {
        "table": "widgets",
        "columns": [
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "name", "type": "text", "nullable": False},  # wrong -- name is actually nullable
        ],
    }
    result = validate_contract(widgets_table, contract)
    assert not result.passed
    assert result.violations[0].kind == "nullability_mismatch"
    assert result.violations[0].column == "name"


def test_validate_contract_raises_for_a_nonexistent_table(sqlite_engine: Engine) -> None:
    with pytest.raises(ContractError):
        validate_contract(sqlite_engine, {"table": "does_not_exist", "columns": []})


def test_load_contract_reads_a_real_contract_file(tmp_path: Path) -> None:
    contract_path = tmp_path / "widgets.yaml"
    contract_path.write_text(
        "table: widgets\nversion: 1\ncolumns:\n  - name: id\n    type: integer\n    nullable: false\n"
    )
    contract = load_contract(contract_path)
    assert contract["table"] == "widgets"
    assert contract["columns"][0]["name"] == "id"


def test_load_contract_raises_for_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ContractError):
        load_contract(tmp_path / "does_not_exist.yaml")


def test_load_contract_raises_for_malformed_yaml(tmp_path: Path) -> None:
    bad_path = tmp_path / "bad.yaml"
    bad_path.write_text("table: widgets\ncolumns: [this is not valid: yaml: at all:")
    with pytest.raises(ContractError):
        load_contract(bad_path)


def test_load_contract_raises_when_required_keys_are_missing(tmp_path: Path) -> None:
    incomplete_path = tmp_path / "incomplete.yaml"
    incomplete_path.write_text("table: widgets\n")  # missing 'columns'
    with pytest.raises(ContractError):
        load_contract(incomplete_path)


def test_validate_all_contracts_against_the_real_contracts_directory(sqlite_engine: Engine) -> None:
    """The actual contracts/source/*.yaml files, validated against a
    SQLite schema built to match them exactly -- proves the real,
    committed contracts are internally well-formed and loadable, without
    requiring real Postgres."""
    with sqlite_engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE urls (id INTEGER NOT NULL, short_code TEXT, original_url TEXT NOT NULL, "
            "created_at TIMESTAMP NOT NULL, expires_at TIMESTAMP, user_id INTEGER, "
            "is_active BOOLEAN NOT NULL, deleted_at TIMESTAMP)"
        ))
        conn.execute(text(
            "CREATE TABLE users (id INTEGER NOT NULL, email TEXT NOT NULL, "
            "plan_type TEXT NOT NULL, created_at TIMESTAMP NOT NULL)"
        ))
        conn.execute(text(
            "CREATE TABLE clicks (id INTEGER NOT NULL, short_code TEXT NOT NULL, "
            "occurred_at TIMESTAMP NOT NULL, device_type TEXT NOT NULL, "
            "hashed_ip TEXT, user_id INTEGER)"
        ))

    real_contracts_dir = Path(__file__).resolve().parents[3] / "contracts" / "source"
    results = validate_all_contracts(sqlite_engine, real_contracts_dir)

    assert {r.table for r in results} == {"urls", "users", "clicks"}
    failed = [r for r in results if not r.passed]
    assert failed == [], f"contract(s) failed against a matching schema: {failed}"
