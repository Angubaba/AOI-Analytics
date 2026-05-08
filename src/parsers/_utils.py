"""
Shared low-level helpers for all AOI parsers.
Kept in a separate module to avoid circular imports with __init__.py.
"""


def _extract_uname_from_tokens(tokens: list) -> str:
    """
    Find defect name by scanning for PackageTypeGroup marker (12 or 13).
    Returns the token immediately before the first marker found.
    Used by line2_parser and line4_parser.
    """
    for i, t in enumerate(tokens):
        if t in ("12", "13") and i >= 1:
            return tokens[i - 1]
    return ""


def _extract_component_from_tokens(tokens: list) -> str:
    """
    Find component name (uname CSV column) by scanning for PackageTypeGroup marker (12 or 13).
    The component appears 7 positions before the marker:
      [component, PackageName, PackageType, PartNumber, unameAngle, ArrayIndex, InspType, 12/13]
    Returns empty string if not found or index out of range.
    """
    for i, t in enumerate(tokens):
        if t in ("12", "13") and i >= 7:
            return tokens[i - 7]
    return ""
