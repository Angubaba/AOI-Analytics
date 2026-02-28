"""
Shared low-level helpers for all AOI parsers.
Kept in a separate module to avoid circular imports with __init__.py.
"""

import re

# Pre-compiled patterns used by multiple parsers
_DATE_YYYYMMDD = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATE_DDMMYYYY = re.compile(r"^\d{2}-\d{2}-\d{4}$")
_TIME_HHMMSS   = re.compile(r"^\d{1,2}:\d{2}:\d{2}$")
_AMPM          = re.compile(r"^(AM|PM)$", re.IGNORECASE)


def _extract_uname_from_tokens(tokens: list) -> str:
    """
    Find uname by scanning for TB marker (12 or 13).
    Returns the token immediately before the first TB marker found,
    or empty string if not found.
    Used by line2_parser and line4_parser.
    """
    for i, t in enumerate(tokens):
        if t in ("12", "13") and i >= 1:
            return tokens[i - 1]
    return ""


def find_datetimes_in_tokens(tokens: list) -> list:
    """
    Scan a token list for date+time patterns regardless of position.

    Supported formats:
      - YYYY-MM-DD HH:MM:SS           (2 tokens)
      - YYYY-MM-DD HH:MM:SS AM/PM     (3 tokens)
      - DD-MM-YYYY HH:MM:SS           (2 tokens)

    Returns a list of (start_index, datetime_string, n_tokens_consumed) tuples
    in the order they appear in the token list.
    """
    results = []
    i = 0
    while i < len(tokens):
        t = tokens[i]
        is_date = _DATE_YYYYMMDD.match(t) or _DATE_DDMMYYYY.match(t)
        if is_date and i + 1 < len(tokens) and _TIME_HHMMSS.match(tokens[i + 1]):
            if i + 2 < len(tokens) and _AMPM.match(tokens[i + 2]):
                results.append((i, " ".join(tokens[i : i + 3]), 3))
                i += 3
            else:
                results.append((i, " ".join(tokens[i : i + 2]), 2))
                i += 2
        else:
            i += 1
    return results
