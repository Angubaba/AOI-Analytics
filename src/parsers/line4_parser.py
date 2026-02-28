import re
import pandas as pd
from ._utils import _extract_uname_from_tokens

# Pre-compiled at module level — avoids recompiling inside per-row exception handler
_NUMERIC_RE = re.compile(r"\d{3,}")
_DATE_RE    = re.compile(r"\d{2}-\d{2}-\d{4}")
_TIME_RE    = re.compile(r"\d{2}:\d{2}:\d{2}")


def parse_line4(file_path: str) -> pd.DataFrame:
    """
    Line4:
    PCBID MachineID JobFileIDShare StartDateTime EndDateTime UserID ... AllBarCode
    Start/End are dd-mm-yyyy HH:MM:SS (2 tokens each)
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

                pcbid = tokens[0]
                machine = tokens[1]

                kyjob_idx = next(i for i, t in enumerate(tokens) if t.endswith(".KYJOB"))
                jobfile = " ".join(tokens[2:kyjob_idx + 1]).strip()

                rest = tokens[kyjob_idx + 1:]
                start_raw = " ".join(rest[0:2])
                end_raw = " ".join(rest[2:4])

                after_dt = rest[4:]

                # uname: token before TB (12/13)
                uname_guess = _extract_uname_from_tokens(after_dt) or None

                # AllBarCode usually last token (may contain commas, no spaces)
                allbarcode = after_dt[-1] if after_dt else None

                rows.append({
                    "PCBID": pcbid,
                    "MachineID": machine,
                    "JobFileIDShare": jobfile,
                    "StartDateTime_raw": start_raw,
                    "EndDateTime_raw": end_raw,
                    "AllBarCode": allbarcode,
                    "uname": uname_guess,
                    "ParseOK": True,
                    "ParseError": ""
                })

            except Exception as e:
                tokens = line.split()
                pcbid_guess = next((t for t in tokens if _NUMERIC_RE.fullmatch(t)), None)

                dt_guess = None
                for i in range(len(tokens) - 1):
                    if _DATE_RE.fullmatch(tokens[i]) and _TIME_RE.fullmatch(tokens[i + 1]):
                        dt_guess = tokens[i] + " " + tokens[i + 1]
                        break

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
                    "ParseError": str(e)
                })

    df = pd.DataFrame(rows)
    print(f"✅ Line4 parsed {len(df)} rows (skipped 0)")
    return df
