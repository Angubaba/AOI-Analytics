import re
import pandas as pd
from ._utils import _extract_uname_from_tokens

# Pre-compiled at module level — avoids recompiling inside the per-token inner loop
_NUMERIC_RE  = re.compile(r"\d{3,}")
_OLD_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD  (old format)
_NEW_DATE_RE = re.compile(r"^\d{2}-\d{2}-\d{4}$")  # DD-MM-YYYY  (new format)


def parse_line2(file_path: str) -> pd.DataFrame:
    """
    Line-2 format (file extension is .csv but rows are whitespace-separated).
    Supports two date formats — auto-detected per row:
      Old: YYYY-MM-DD h:mm:ss AM/PM  (3 tokens)
      New: DD-MM-YYYY HH:MM:SS       (2 tokens, 24-hour)

    Output columns at minimum:
      StartDateTime_raw, EndDateTime_raw, JobFileIDShare, AllBarCode, PCBID, MachineID, uname, TB
    """
    rows = []
    skipped = 0

    # Try common encodings
    encodings = ["utf-8-sig", "utf-16", "cp1252"]
    last_err = None

    for enc in encodings:
        try:
            with open(file_path, "r", encoding=enc) as f:
                header = f.readline().strip().replace("\ufeff", "")
                # header is whitespace-separated, not comma-separated
                if not header:
                    raise ValueError("Empty header")

                for line in f:
                    line = line.strip().strip('"')
                    if not line:
                        continue

                    tokens = line.split()
                    if len(tokens) < 10:
                        skipped += 1
                        continue

                    # Auto-detect date format from first token
                    if _OLD_DATE_RE.match(tokens[0]):
                        dt_tokens = 3   # YYYY-MM-DD h:mm:ss AM/PM
                    elif _NEW_DATE_RE.match(tokens[0]):
                        dt_tokens = 2   # DD-MM-YYYY HH:MM:SS
                    else:
                        skipped += 1
                        continue

                    if len(tokens) < dt_tokens + 1:
                        skipped += 1
                        continue
                    start_raw = " ".join(tokens[0:dt_tokens])

                    # Find end of jobfile (token ending with .KYJOB)
                    try:
                        kyjob_idx = next(i for i, t in enumerate(tokens) if t.upper().endswith(".KYJOB"))
                    except StopIteration:
                        skipped += 1
                        continue

                    # JobFileIDShare starts after StartDateTime tokens up to kyjob_idx inclusive
                    if kyjob_idx < dt_tokens:
                        skipped += 1
                        continue
                    job_file = " ".join(tokens[dt_tokens:kyjob_idx + 1])

                    rest = tokens[kyjob_idx + 1:]
                    if len(rest) < 4:
                        skipped += 1
                        continue

                    # PCBID is the first *pure numeric* token after AllBarCode
                    pcbid_pos = None
                    for i, t in enumerate(rest):
                        if _NUMERIC_RE.fullmatch(t):
                            pcbid_pos = i
                            break
                    if pcbid_pos is None:
                        skipped += 1
                        continue

                    allbarcode = " ".join(rest[:pcbid_pos]).strip()
                    pcbid = rest[pcbid_pos]

                    # MachineID follows PCBID
                    machine = rest[pcbid_pos + 1] if pcbid_pos + 1 < len(rest) else ""

                    # EndDateTime: same token count as StartDateTime
                    end_start = pcbid_pos + 2
                    end_raw = ""
                    if end_start + dt_tokens <= len(rest):
                        end_raw = " ".join(rest[end_start:end_start + dt_tokens])

                    # After end datetime, there are many fields, but may have blanks (multiple spaces)
                    tail = rest[end_start + dt_tokens:] if end_start + dt_tokens < len(rest) else []

                    # uname extraction: anchor TB (12 or 13). uname is token right before TB.
                    tb = next((t for t in tail if t in ("12", "13")), None)
                    uname = _extract_uname_from_tokens(tail)

                    rows.append({
                        "StartDateTime_raw": start_raw,
                        "EndDateTime_raw": end_raw,
                        "JobFileIDShare": job_file,
                        "AllBarCode": allbarcode,
                        "PCBID": pcbid,
                        "MachineID": machine,
                        "uname": uname,
                        "TB": tb,
                    })

            df = pd.DataFrame(rows)
            print(f"✅ Line2 parsed {len(df)} rows (skipped {skipped})")
            return df

        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"Line2 parser failed for all encodings. Last error: {last_err}")
