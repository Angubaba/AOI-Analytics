import re
import pandas as pd
from ._utils import _extract_uname_from_tokens, find_datetimes_in_tokens

# Pre-compiled at module level — avoids recompiling inside the per-token inner loop
_NUMERIC_RE = re.compile(r"\d{3,}")


def parse_line2(file_path: str) -> pd.DataFrame:
    """
    Line-2 format (file extension is .csv but rows are whitespace-separated).

    Field extraction is COLUMN-ORDER AGNOSTIC:
      - StartDateTime: first date+time pattern found anywhere in the row
      - EndDateTime:   second date+time pattern found anywhere in the row
      - JobFileIDShare: token(s) ending with .KYJOB (anchor)
      - AllBarCode:    tokens between .KYJOB and the first pure-numeric PCBID
      - PCBID:         first pure-numeric (3+ digit) token after .KYJOB
      - MachineID:     token immediately after PCBID (best-effort)
      - uname/TB:      token before TB marker (12 or 13)

    Compatible with both the original "21 2r" layout and any variant where
    StartDateTime does not start at column 0.
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
                if not header:
                    raise ValueError("Empty header")

                for line in f:
                    line = line.strip().strip('"')
                    if not line:
                        continue

                    tokens = line.split()
                    if len(tokens) < 6:
                        skipped += 1
                        continue

                    # ── StartDateTime: first date+time pattern (position-independent) ──
                    dts = find_datetimes_in_tokens(tokens)
                    if not dts:
                        skipped += 1
                        continue
                    start_raw = dts[0][1]

                    # ── .KYJOB anchor ──
                    try:
                        kyjob_idx = next(
                            i for i, t in enumerate(tokens)
                            if t.upper().endswith(".KYJOB")
                        )
                    except StopIteration:
                        skipped += 1
                        continue

                    # JobFileIDShare: tokens from after StartDateTime up to .KYJOB
                    # (handles multi-token paths with spaces)
                    job_file = tokens[kyjob_idx]
                    # Scan backward for path fragments that belong to the job path
                    start_j = kyjob_idx
                    while (
                        start_j > 0
                        and (
                            tokens[start_j - 1].startswith("\\")
                            or "\\" in tokens[start_j - 1]
                        )
                    ):
                        start_j -= 1
                    job_file = " ".join(tokens[start_j : kyjob_idx + 1])

                    # ── Everything after .KYJOB ──
                    rest = tokens[kyjob_idx + 1 :]
                    if len(rest) < 2:
                        skipped += 1
                        continue

                    # PCBID: first pure-numeric (3+ digit) token in rest
                    pcbid_pos = None
                    for i, t in enumerate(rest):
                        if _NUMERIC_RE.fullmatch(t):
                            pcbid_pos = i
                            break
                    if pcbid_pos is None:
                        skipped += 1
                        continue

                    # AllBarCode: everything between .KYJOB and PCBID
                    # (may be multi-word if the barcode contains spaces)
                    allbarcode = " ".join(rest[:pcbid_pos]).strip()
                    pcbid = rest[pcbid_pos]

                    # MachineID: token right after PCBID (best-effort)
                    machine = rest[pcbid_pos + 1] if pcbid_pos + 1 < len(rest) else ""

                    # EndDateTime: second datetime pattern found in the full line,
                    # or fall back to positional extraction from rest
                    end_raw = ""
                    if len(dts) >= 2:
                        end_raw = dts[1][1]
                    elif pcbid_pos + 4 < len(rest):
                        end_raw = " ".join(rest[pcbid_pos + 2 : pcbid_pos + 5])

                    # Tail after MachineID: used for TB / uname
                    tail = rest[pcbid_pos + 2 :] if pcbid_pos + 2 < len(rest) else []

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
            rows = []
            skipped = 0
            continue

    raise RuntimeError(f"Line2 parser failed for all encodings. Last error: {last_err}")
