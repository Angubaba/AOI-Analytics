import re
import pandas as pd


# Anchored extractor:
# 1) JobFile: starts with "\" and ends with ".KYJOB"
# 2) StartDT: dd-mm-yyyy HH:MM:SS  (1 or 2 digit day/month/hour — handles non-zero-padded)
# 3) PCBID: digits
# 4) MachineID: non-space token (AL-SL-xxxxx)
# 5) EndDT: dd-mm-yyyy HH:MM:SS
LINE1_RE = re.compile(
    r"""
    (?P<JobFile>\\.*?\.KYJOB)\s+
    (?P<StartDT>\d{1,2}-\d{1,2}-\d{4}\s+\d{1,2}:\d{2}:\d{2})\s+
    (?P<PCBID>\d+)\s+
    (?P<MachineID>\S+)\s+
    (?P<EndDT>\d{1,2}-\d{1,2}-\d{4}\s+\d{1,2}:\d{2}:\d{2})
    """,
    re.VERBOSE | re.IGNORECASE,
)

TB_RE = re.compile(r"\s(12|13)\s")


def parse_line1(file_path: str, encoding: str = "utf-16") -> pd.DataFrame:
    """
    LINE 1: space-delimited log (header present), but AllBarCode may contain spaces.
    We NEVER skip lines. We use regex anchors to reliably extract:
      JobFile, StartDateTime_raw, PCBID, MachineID, EndDateTime_raw
    uname is best-effort: token immediately before TB(12/13).
    """
    rows = []

    try:
        fh = open(file_path, "r", encoding=encoding, errors="ignore")
    except (FileNotFoundError, PermissionError, OSError) as e:
        raise IOError(f"Line1 parser cannot open file: {file_path!r} — {e}") from e

    with fh:
        _ = fh.readline()  # header

        for raw in fh:
            line = raw.strip().strip('"')
            if not line:
                continue

            rec = {
                "Format": "line1",
                "JobFile": None,
                "StartDateTime_raw": None,
                "EndDateTime_raw": None,
                "PCBID": None,
                "MachineID": None,
                "uname": None,
                "ParseOK": False,
                "ParseError": "",
            }

            try:
                m = LINE1_RE.search(line)
                if m:
                    rec["JobFile"] = m.group("JobFile")
                    rec["StartDateTime_raw"] = m.group("StartDT")
                    rec["PCBID"] = m.group("PCBID")
                    rec["MachineID"] = m.group("MachineID")
                    rec["EndDateTime_raw"] = m.group("EndDT")
                    rec["ParseOK"] = True
                else:
                    rec["ParseError"] = "Regex anchor not found"

                tbm = TB_RE.search(line)
                if tbm:
                    left = line[: tbm.start()].strip()
                    if left:
                        rec["uname"] = left.split()[-1]

            except Exception as e:
                rec["ParseError"] = str(e)

            rows.append(rec)

    df = pd.DataFrame(rows)
    ok = int(df["ParseOK"].sum()) if "ParseOK" in df.columns else 0
    fail = len(df) - ok
    print(f"Line1 parsed {len(df)} rows -- {ok} OK, {fail} failed regex")
    return df
