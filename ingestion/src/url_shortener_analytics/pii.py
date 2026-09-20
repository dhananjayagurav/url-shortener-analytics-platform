"""PII classification: read the `pii` field every contracts/source/*.yaml
column now declares, and report which source columns carry personal data.

See docs/analytics-engineering-guide.md, Section 23, for the concept and
this module's central design decision: PII classification lives inside
Section 12's existing contract files, as one more required field per
column, instead of a separate registry file tracked on its own. A
contract already enumerates every column a table has. Adding `pii` to
that same list means the classification can never drift out of sync with
the schema it describes -- there is no second file that could go stale.

Three categories, chosen to match language this project already uses
elsewhere (Section 10.4's `dim_user`/email decision, and the `hashed_ip`
column comment in sql/source/002_hypothetical_users_and_clicks.sql):

- "none" -- the column carries no personal data.
- "pseudonymized" -- the column stands in for a real identity or
  attribute, but does not reveal it by itself (a hashed IP, a bare
  foreign-key integer). Still personal data under a strict reading (see
  Section 23.1) -- pseudonymizing a value is not the same as anonymizing
  it, because the same input always produces the same output.
- "direct" -- the column identifies one natural person on its own, with
  no other data needed (an email address).

Every contract column MUST declare `pii`. This is enforced by
`_require_pii_declared`, which raises ContractError -- the same
exception `contracts.py` raises for a contract that can't be loaded at
all -- rather than silently treating an undeclared column as "none".
Silently defaulting to "none" would be the one behavior this whole
module exists to prevent: a new column added to a contract without
anyone having to make an explicit PII call.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from url_shortener_analytics.contracts import DEFAULT_CONTRACTS_DIR, load_contract
from url_shortener_analytics.exceptions import ContractError

VALID_PII_CATEGORIES = {"none", "pseudonymized", "direct"}


@dataclass
class PiiColumn:
    """One column a contract classifies as carrying personal data.
    Columns classified "none" are not represented by this class at all
    -- see `classify_table`."""

    table: str
    column: str
    category: str  # "pseudonymized" | "direct"
    description: str


def _require_pii_declared(contract: dict[str, Any]) -> None:
    """Raise ContractError if any column in `contract` is missing `pii`,
    or declares a value outside VALID_PII_CATEGORIES. Called before any
    classification is read, so a contract that hasn't been updated yet
    fails loudly instead of being silently treated as "no PII here"."""
    table = contract["table"]

    undeclared = [c["name"] for c in contract["columns"] if "pii" not in c]
    if undeclared:
        raise ContractError(
            f"contract '{table}' is missing a 'pii' classification for "
            f"column(s): {', '.join(undeclared)}"
        )

    invalid = {c["name"]: c["pii"] for c in contract["columns"] if c["pii"] not in VALID_PII_CATEGORIES}
    if invalid:
        raise ContractError(
            f"contract '{table}' has invalid 'pii' value(s) {invalid} -- "
            f"must be one of {sorted(VALID_PII_CATEGORIES)}"
        )


def classify_table(contract: dict[str, Any]) -> list[PiiColumn]:
    """Return every column in `contract` classified as `pii != "none"`.
    Raises ContractError first if the contract's `pii` declarations are
    missing or invalid (see `_require_pii_declared`)."""
    _require_pii_declared(contract)
    return [
        PiiColumn(
            table=contract["table"],
            column=c["name"],
            category=c["pii"],
            description=c.get("description", ""),
        )
        for c in contract["columns"]
        if c["pii"] != "none"
    ]


def classify_all_contracts(contracts_dir: Path = DEFAULT_CONTRACTS_DIR) -> list[PiiColumn]:
    """Load and classify every *.yaml contract in `contracts_dir`, in
    alphabetical order. Used by `cli.py pii-report`."""
    results: list[PiiColumn] = []
    for path in sorted(contracts_dir.glob("*.yaml")):
        contract = load_contract(path)
        results.extend(classify_table(contract))
    return results
