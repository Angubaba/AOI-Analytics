import os

from .line1_parser import parse_line1
from .line2_parser import parse_line2
from .line4_parser import parse_line4


def load_any_aoi(file_path: str):
    """
    Detects which parser to use based on first header token order.
    Assumes utf-16 text.
    """
    try:
        fh = open(file_path, encoding="utf-16")
    except (FileNotFoundError, PermissionError, OSError) as e:
        raise IOError(f"Cannot open AOI file: {file_path!r} — {e}") from e

    with fh:
        header = fh.readline().strip().replace("\ufeff", "")

    h = header.split()

    # line4 starts with PCBID MachineID JobFileIDShare...
    if len(h) >= 3 and h[0] == "PCBID" and h[1] == "MachineID":
        return parse_line4(file_path)

    # line2 starts with StartDateTime JobFileIDShare AllBarCode...
    if len(h) >= 3 and h[0] == "StartDateTime" and "AllBarCode" in h[:5]:
        return parse_line2(file_path)

    # line1 starts with BarCode AllBarCode JobFileIDShare...
    if len(h) >= 2 and h[0] == "BarCode" and h[1] == "AllBarCode":
        return parse_line1(file_path)

    raise ValueError(f"Unknown AOI format header: {header[:120]}")


def detect_line_key(file_path: str) -> str:
    """
    Read only the header row and return 'line1' | 'line2' | 'line4'.
    Raises ValueError for unknown formats.
    """
    try:
        with open(file_path, encoding="utf-16") as fh:
            header = fh.readline().strip().replace("\ufeff", "")
    except (FileNotFoundError, PermissionError, OSError) as e:
        raise IOError(f"Cannot open AOI file: {file_path!r} — {e}") from e

    h = header.split()
    if len(h) >= 3 and h[0] == "PCBID" and h[1] == "MachineID":
        return "line4"
    if len(h) >= 3 and h[0] == "StartDateTime" and "AllBarCode" in h[:5]:
        return "line2"
    if len(h) >= 2 and h[0] == "BarCode" and h[1] == "AllBarCode":
        return "line1"
    raise ValueError(f"Unknown AOI format: {header[:80]}")
