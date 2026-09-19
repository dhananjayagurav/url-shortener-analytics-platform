"""Package-specific exceptions.

Raising these instead of letting raw driver/SDK exceptions (psycopg errors,
botocore ClientError, ...) propagate unchanged gives callers -- and the
`ingestion_metadata` failure record in metadata.py -- one predictable
exception family to catch, while `__cause__` still chains back to the real
underlying error for debugging (see the `raise ... from err` pattern used
throughout this package).
"""

from __future__ import annotations


class IngestionError(Exception):
    """Base class for all errors raised by this package."""


class ExtractionError(IngestionError):
    """Raised when reading from the source OLTP database fails."""


class ObjectStoreWriteError(IngestionError):
    """Raised when writing a Bronze object to object storage fails."""


class MetadataError(IngestionError):
    """Raised when reading or writing ingestion_metadata fails."""


class ContractError(IngestionError):
    """Raised when a data contract file can't be loaded/parsed, or when the
    table it describes can't be reached at all (e.g. it doesn't exist).
    NOT raised for a structural mismatch between a contract and the real
    table -- see contracts.py's module docstring for why those are
    reported as data (ContractValidationResult), not exceptions."""
