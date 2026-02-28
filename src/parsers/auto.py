from .line1_parser import parse_line1
from .line2_parser import parse_line2
from .line4_parser import parse_line4


def _read_header(file_path: str) -> tuple[str, str]:
    """
    Try multiple encodings and return (header_line, encoding_used).
    Raises IOError if no encoding works.
    """
    for enc in ("utf-16", "utf-8-sig", "cp1252", "latin-1"):
        try:
            with open(file_path, encoding=enc) as fh:
                hdr = fh.readline().strip().replace("\ufeff", "")
            if hdr:
                return hdr, enc
        except Exception:
            continue
    raise IOError(f"Cannot read header of AOI file: {file_path!r}")


def _detect_format(header: str) -> str:
    """
    Identify format from the header string.
    Detection is CASE-INSENSITIVE and POSITION-INDEPENDENT — it doesn't care
    what order the columns are in, only which column names are present.

    Returns: 'line1', 'line2', or 'line4'
    Raises:  ValueError for unrecognised headers.
    """
    tokens = header.split()
    lower  = [t.lower() for t in tokens]
    lset   = set(lower)

    # ── line4: contains both 'pcbid' and 'machineid' ──
    # (original layout starts with PCBID MachineID, but we don't require that)
    if "pcbid" in lset and "machineid" in lset:
        return "line4"

    # ── line2: contains 'startdatetime' and 'allbarcode' ──
    if "startdatetime" in lset and "allbarcode" in lset:
        return "line2"

    # ── line1: contains 'barcode' AND 'allbarcode' (both present, barcode before allbarcode) ──
    if "barcode" in lset and "allbarcode" in lset:
        barcode_pos    = next((i for i, x in enumerate(lower) if x == "barcode"), -1)
        allbarcode_pos = next((i for i, x in enumerate(lower) if x == "allbarcode"), -1)
        if 0 <= barcode_pos < allbarcode_pos:
            return "line1"

    raise ValueError(f"Unknown AOI format — header: {header[:120]!r}")


def load_any_aoi(file_path: str):
    """
    Auto-detect which AOI parser to use, then parse and return a DataFrame.

    Detection strategy (column-order agnostic, case-insensitive):
      • 'pcbid' + 'machineid' in header → line4
      • 'startdatetime' + 'allbarcode'  → line2
      • 'barcode' + 'allbarcode'        → line1

    Multiple encodings are tried automatically (utf-16, utf-8-sig, cp1252, latin-1).
    """
    header, _ = _read_header(file_path)
    fmt = _detect_format(header)

    if fmt == "line4":
        return parse_line4(file_path)
    if fmt == "line2":
        return parse_line2(file_path)
    if fmt == "line1":
        return parse_line1(file_path)

    raise ValueError(f"Unknown AOI format: {header[:120]}")
