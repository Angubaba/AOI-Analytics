"""
Shared low-level helpers for all AOI parsers.
Kept in a separate module to avoid circular imports with __init__.py.
"""


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
