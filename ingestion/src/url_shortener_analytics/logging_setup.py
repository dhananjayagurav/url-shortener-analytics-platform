"""Structured logging setup.

A minimal structured formatter -- timestamp, level, logger name, message,
plus any `extra={...}` fields -- rendered as key=value pairs on one line.
This is deliberately not a dependency on structlog or python-json-logger:
at this project's size, a ~30-line formatter gives the real benefit
(fields are greppable and parseable, not buried in a free-text sentence)
without adding a dependency whose full feature set this package doesn't
need yet. Revisit if/when logs need to ship to a real log aggregation
system that expects JSON specifically.
"""

from __future__ import annotations

import logging
import sys

_RESERVED = frozenset(logging.LogRecord(
    "", 0, "", 0, "", (), None
).__dict__.keys())


class KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = (
            f"ts={self.formatTime(record, '%Y-%m-%dT%H:%M:%S%z')} "
            f"level={record.levelname} logger={record.name} "
            f"msg=\"{record.getMessage()}\""
        )
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in _RESERVED and not k.startswith("_")
        }
        if extras:
            base += " " + " ".join(f"{k}={v!r}" for k, v in sorted(extras.items()))
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger once, at process startup (cli.py's entrypoint)."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(KeyValueFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
