import re
import pandas as pd
from ._utils import _extract_uname_from_tokens

# Pre-compiled at module level — avoids recompiling inside the per-token inner loop
_NUMERIC_RE = re.compile(r"\d{3,}")


def parse_line2(file_path: str) -> pd.DataFrame:
    """
    Line-2 format (file extension is .csv but rows are whitespace-separated):
    - StartDateTime is 2 tokens: DD-MM-YYYY HH:MM:SS  (24-hour, no AM/PM)
    - JobFileIDShare may contain spaces; anchor ends at token ending with .KYJOB
    - AllBarCode may contain spaces; it ends right before PCBID (first pure-numeric token)
    - EndDateTime is 2 tokens: DD-MM-YYYY HH:MM:SS

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

                    # StartDateTime is 2 tokens: DD-MM-YYYY HH:MM:SS
                    # Example: 06-04-2026 12:28:55
                    if len(tokens) < 3:
                        skipped += 1
                        continue
                    start_raw = " ".join(tokens[0:2])

                    # Find end of jobfile (token ending with .KYJOB)
                    try:
                        kyjob_idx = next(i for i, t in enumerate(tokens) if t.upper().endswith(".KYJOB"))
                    except StopIteration:
                        skipped += 1
                        continue

                    # JobFileIDShare starts after StartDateTime tokens (index 2) up to kyjob_idx inclusive
                    if kyjob_idx < 2:
                        skipped += 1
                        continue
                    job_file = " ".join(tokens[2:kyjob_idx + 1])

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

                    # EndDateTime is next 3 tokens after MachineID
                    end_raw = ""
                    if pcbid_pos + 4 < len(rest):
                        end_raw = " ".join(rest[pcbid_pos + 2:pcbid_pos + 5])

                    # After end datetime, there are many fields, but may have blanks (multiple spaces)
                    tail = rest[pcbid_pos + 5:] if pcbid_pos + 5 < len(rest) else []

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
