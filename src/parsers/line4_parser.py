import re
import pandas as pd
from ._utils import _extract_uname_from_tokens, find_datetimes_in_tokens

# Pre-compiled at module level
_NUMERIC_RE = re.compile(r"\d{3,}")


def parse_line4(file_path: str) -> pd.DataFrame:
    """
    Line4:
    Typical layout: PCBID MachineID JobFileIDShare StartDateTime EndDateTime UserID ... AllBarCode

    Field extraction is COLUMN-ORDER AGNOSTIC:
      - PCBID:         first pure-numeric (3+ digit) token before .KYJOB
      - MachineID:     other non-numeric, non-path token before .KYJOB (e.g. AL-SL-04527)
      - JobFileIDShare: token(s) ending with .KYJOB
      - StartDateTime: first date+time pattern in the full row
      - EndDateTime:   second date+time pattern in the full row
      - AllBarCode:    last non-datetime, non-kyjob, non-numeric token (heuristic)
      - uname/TB:      token before TB marker (12 or 13)

    Works even when PCBID and MachineID are swapped or other columns shift position.
    """
    rows = []
    try:
        fh = open(file_path, encoding="utf-16")
    except (FileNotFoundError, PermissionError, OSError) as e:
        raise IOError(f"Line4 parser cannot open file: {file_path!r} — {e}") from e

    with fh:
        header = fh.readline().strip().replace("\ufeff", "")
        if not header:
            raise ValueError("Empty file/header not found.")

        for raw in fh:
            line = raw.strip().strip('"')
            if not line:
                continue

            try:
                tokens = line.split()

                # ── .KYJOB anchor ──
                kyjob_idx = next(
                    i for i, t in enumerate(tokens) if t.endswith(".KYJOB")
                )
                jobfile = " ".join(tokens[2 : kyjob_idx + 1]).strip()

                # ── Tokens before KYJOB: find PCBID and MachineID ──
                before = tokens[:kyjob_idx]
                # PCBID: first pure-numeric (3+ digit) token
                pcbid = next(
                    (t for t in before if _NUMERIC_RE.fullmatch(t)), None
                )
                # MachineID: first non-numeric, non-path (no backslash) token before KYJOB
                machine = next(
                    (
                        t for t in before
                        if not _NUMERIC_RE.fullmatch(t)
                        and "\\" not in t
                        and re.search(r"[A-Za-z]", t)
                    ),
                    None,
                )

                # ── Datetimes: scan full row (position-independent) ──
                dts = find_datetimes_in_tokens(tokens)
                start_raw = dts[0][1] if len(dts) >= 1 else None
                end_raw   = dts[1][1] if len(dts) >= 2 else None

                # ── Build the "after datetimes" section for uname / AllBarCode ──
                # Collect token indices consumed by the two datetimes and kyjob range
                used = set()
                for di, _, dn in dts[:2]:
                    used.update(range(di, di + dn))
                used.update(range(0, kyjob_idx + 1))

                after = [t for i, t in enumerate(tokens) if i not in used]

                # uname: token before TB (12/13)
                uname_guess = _extract_uname_from_tokens(after) or None

                # AllBarCode: last token of the full row (heuristic for line4)
                allbarcode = tokens[-1] if tokens else None

                rows.append({
                    "PCBID": pcbid,
                    "MachineID": machine,
                    "JobFileIDShare": jobfile,
                    "StartDateTime_raw": start_raw,
                    "EndDateTime_raw": end_raw,
                    "AllBarCode": allbarcode,
                    "uname": uname_guess,
                    "ParseOK": True,
                    "ParseError": "",
                })

            except Exception as e:
                # Fallback: pure pattern scan — works on any column order
                tokens = line.split()

                # PCBID: first pure-numeric 3+ digit token
                pcbid_guess = next(
                    (t for t in tokens if _NUMERIC_RE.fullmatch(t)), None
                )

                # Datetime: first date+time pattern found
                dts = find_datetimes_in_tokens(tokens)
                dt_guess = dts[0][1] if dts else None

                uname_guess = _extract_uname_from_tokens(tokens) or None

                rows.append({
                    "PCBID": pcbid_guess,
                    "MachineID": None,
                    "JobFileIDShare": None,
                    "StartDateTime_raw": dt_guess,
                    "EndDateTime_raw": None,
                    "AllBarCode": None,
                    "uname": uname_guess,
                    "ParseOK": False,
                    "ParseError": str(e),
                })

    df = pd.DataFrame(rows)
    print(f"✅ Line4 parsed {len(df)} rows (skipped 0)")
    return df
