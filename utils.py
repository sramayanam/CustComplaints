"""
utils.py – Shared utilities for the CustComplaints pipeline.
"""

from __future__ import annotations


def mask_key(key: object) -> str:
    """Return a masked representation of a Kafka message key for safe logging.

    Reveals at most the first ``min(4, len // 2)`` characters of the key so
    that log messages contain enough context to correlate errors without
    exposing entity identifiers in plain text.

    Examples
    --------
    >>> mask_key(b"1")
    '****'
    >>> mask_key(b"42")
    '4****'
    >>> mask_key("complaints")
    'comp****'
    >>> mask_key(None)
    '<null>'
    """
    if key is None:
        return "<null>"
    if isinstance(key, (bytes, bytearray)):
        key = key.decode("utf-8", errors="replace")
    s = str(key)
    n = min(4, max(0, len(s) // 2))
    return (s[:n] + "****") if n > 0 else "****"
