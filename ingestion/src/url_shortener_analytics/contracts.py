"""Data contract validation: compare a table's real, live shape against a
committed contract file in contracts/source/*.yaml.

See docs/analytics-engineering-guide.md, Section 12, for the concept and
this module's two central design decisions:

1. Type comparison is by coarse CATEGORY (integer / text / boolean /
   timestamp / numeric), not exact SQL type string. Contracts here are
   validated against SQLite in unit tests and real Postgres in
   integration tests / production, and those two dialects don't share a
   common exact-type vocabulary after SQLAlchemy reflection (Postgres
   BIGINT and SQLite's affinity-inferred INTEGER are both "integer" in
   spirit; comparing their exact type strings would either force
   Postgres-only contracts or produce false failures under SQLite). See
   `_categorize_type` below and Section 12's Trade-offs table.

2. Validating a contract returns a ContractValidationResult (a report
   listing every violation and warning found), rather than raising on the
   first mismatch. A real contract check should tell you everything that's
   wrong in one run, the same way a test suite reports every failing test
   rather than stopping at the first -- see Section 12's Design Decision
   for the full reasoning. ContractError is reserved for cases where
   validation couldn't even be attempted (a malformed contract file, or a
   table that doesn't exist at all).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import Engine, inspect
from sqlalchemy import types as sqltypes

from url_shortener_analytics.exceptions import ContractError

DEFAULT_CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts" / "source"


@dataclass
class ContractViolation:
    """A breaking mismatch between a contract and the real table -- a
    missing column, a type-category mismatch, or a nullability mismatch."""

    kind: str
    column: str
    detail: str


@dataclass
class ContractWarning:
    """A non-breaking observation -- currently only "the table has a
    column the contract doesn't declare". Not a violation: see Section
    12's Design Decision for why an additive, backward-compatible schema
    change is treated as tolerable, not broken."""

    kind: str
    column: str
    detail: str


@dataclass
class ContractValidationResult:
    table: str
    violations: list[ContractViolation] = field(default_factory=list)
    warnings: list[ContractWarning] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.violations


def load_contract(path: Path) -> dict[str, Any]:
    """Parse one contract YAML file. Raises ContractError on any I/O or
    parse failure -- a malformed contract is a hard stop, not something
    to report as a "violation" of itself."""
    try:
        with path.open() as f:
            contract = yaml.safe_load(f)
    except (OSError, yaml.YAMLError) as err:
        raise ContractError(f"failed to load contract file '{path}'") from err

    if not contract or "table" not in contract or "columns" not in contract:
        raise ContractError(f"contract file '{path}' is missing required keys ('table', 'columns')")
    return contract


def _categorize_type(sa_type: sqltypes.TypeEngine) -> str:
    """Map a reflected SQLAlchemy column type to one of this project's
    coarse contract categories. Order matters: Boolean and Integer are
    both checked before Numeric, since some dialects' integer types are
    also technically Numeric subtypes in SQLAlchemy's hierarchy."""
    if isinstance(sa_type, sqltypes.Boolean):
        return "boolean"
    if isinstance(sa_type, sqltypes.Integer):
        return "integer"
    if isinstance(sa_type, sqltypes.DateTime):
        return "timestamp"
    if isinstance(sa_type, sqltypes.Numeric):
        return "numeric"
    if isinstance(sa_type, sqltypes.String):
        return "text"
    return "unknown"


def validate_contract(engine: Engine, contract: dict[str, Any]) -> ContractValidationResult:
    """Validate one already-loaded contract dict against the real,
    connected database. Returns a result object -- never raises for a
    structural mismatch (see module docstring point 2); raises
    ContractError only if the contracted table doesn't exist at all."""
    table_name = contract["table"]
    inspector = inspect(engine)

    if not inspector.has_table(table_name):
        raise ContractError(f"table '{table_name}' does not exist -- cannot validate its contract")

    actual_columns = {c["name"]: c for c in inspector.get_columns(table_name)}
    contract_columns = {c["name"]: c for c in contract["columns"]}

    violations: list[ContractViolation] = []
    warnings: list[ContractWarning] = []

    for name, spec in contract_columns.items():
        if name not in actual_columns:
            violations.append(ContractViolation(
                "missing_column", name,
                f"contract declares '{name}' but it does not exist in table '{table_name}'",
            ))
            continue

        actual = actual_columns[name]
        actual_category = _categorize_type(actual["type"])
        if actual_category != spec["type"]:
            violations.append(ContractViolation(
                "type_mismatch", name,
                f"contract expects type category '{spec['type']}', "
                f"actual column is '{actual_category}' ({actual['type']})",
            ))

        expected_nullable = spec["nullable"]
        actual_nullable = actual["nullable"]
        if expected_nullable != actual_nullable:
            violations.append(ContractViolation(
                "nullability_mismatch", name,
                f"contract expects nullable={expected_nullable}, actual column has nullable={actual_nullable}",
            ))

    for name in actual_columns:
        if name not in contract_columns:
            warnings.append(ContractWarning(
                "extra_column", name,
                f"column '{name}' exists in table '{table_name}' but is not declared in the contract",
            ))

    return ContractValidationResult(table=table_name, violations=violations, warnings=warnings)


def validate_all_contracts(
    engine: Engine, contracts_dir: Path = DEFAULT_CONTRACTS_DIR
) -> list[ContractValidationResult]:
    """Load and validate every *.yaml contract in `contracts_dir`, in
    alphabetical order. Used by `cli.py validate-contracts`."""
    results = []
    for path in sorted(contracts_dir.glob("*.yaml")):
        contract = load_contract(path)
        results.append(validate_contract(engine, contract))
    return results
