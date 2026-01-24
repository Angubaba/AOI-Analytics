# src/analysis.py
import os
import re
import pandas as pd


def ensure_outputs_dir(path: str = "outputs") -> None:
    os.makedirs(path, exist_ok=True)


# -----------------------------
# Defect (event rows) analysis
# -----------------------------

def _clean_defect_labels(series: pd.Series) -> pd.Series:
    """
    Keep only meaningful defect labels:
      - must contain at least one alphabet (filters out '1', '2300', '13', '12000000', etc.)
      - no empty strings
    Does NOT replace with UNKNOWN.
    """
    s = series.fillna("").astype(str).str.strip()
    s = s[s != ""]
    s = s[s.str.contains(r"[A-Za-z]", regex=True)]
    return s


def top_defects(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """
    Top defect types flagged by AOI (event rows).
    Uses column 'uname' (already normalized by parsers/clean_data).
    Filters numeric junk like '1', '2300'.

    Returns columns: Defect, Count
    """
    if "uname" not in df.columns:
        raise ValueError("Column 'uname' not found.")

    labels = _clean_defect_labels(df["uname"])
    vc = labels.value_counts()

    out = vc.reset_index()
    out.columns = ["Defect", "Count"]
    out["Count"] = out["Count"].astype(int)

    if top_n is None:
        return out
    return out.head(int(top_n))


# -----------------------------
# Card name extraction helpers
# -----------------------------

def _jobfile_series(df: pd.DataFrame) -> pd.Series:
    """
    Best available series for extracting card name.
    Line1/Line4 typically: JobFileIDShare exists
    Some parsers may store in JobFile
    """
    if "JobFileIDShare" in df.columns:
        return df["JobFileIDShare"]
    if "JobFile" in df.columns:
        return df["JobFile"]
    return pd.Series([""] * len(df), index=df.index)


def _extract_card_name(jobfile: str) -> str:
    """
    Extract card name from job path before .KYJOB and remove token 'NEW'.

    Examples:
      \\CDA51 TOP\\CDA51 TOP.KYJOB -> CDA51 TOP
      \\CDD41 BOTTOM NEW\\CDD41 BOTTOM NEW.KYJOB -> CDD41 BOTTOM
    """
    if jobfile is None:
        return "UNKNOWN_CARD"

    s = str(jobfile).strip().strip('"')
    if s == "" or s.lower() == "nan":
        return "UNKNOWN_CARD"

    m = re.search(r"([^\\\/]+)\.KYJOB", s, flags=re.IGNORECASE)
    if m:
        name = m.group(1).strip()
    else:
        parts = re.split(r"[\\\/]+", s)
        parts = [p for p in parts if p.strip()]
        name = parts[-1].strip() if parts else "UNKNOWN_CARD"

    toks = [t for t in name.split() if t.upper() != "NEW"]
    name = " ".join(toks).strip()

    return name if name else "UNKNOWN_CARD"


# -----------------------------
# New counting key (rescans-safe)
# -----------------------------

def _make_scan_key(df: pd.DataFrame) -> pd.Series:
    """
    Build a rescan-safe unique key:
      - Line2 requirement: unique pair (StartDateTime, PCBID)
      - For Line1/Line4 we ALSO include Card to avoid PCBID collisions across different cards:
            Card | PCBID | StartDateTime
    """
    if "StartDateTime" not in df.columns:
        raise ValueError("StartDateTime missing.")
    if "PCBID" not in df.columns:
        # keep safe; if missing, everything becomes empty -> counts 0 later
        pcb = pd.Series([""] * len(df), index=df.index)
    else:
        pcb = df["PCBID"].fillna("").astype(str).str.strip()

    # keep only rows with valid start + pcbid
    dt = df["StartDateTime"]
    dt_str = dt.dt.strftime("%Y-%m-%d %H:%M:%S")

    line = df.attrs.get("line", "")

    if line == "line2":
        # strictly what you asked: unique pair StartDateTime + PCBID
        return pcb + "|" + dt_str

    # line1/line4: protect collisions by adding CardName too
    card = _jobfile_series(df).apply(_extract_card_name).astype(str)
    return card + "|" + pcb + "|" + dt_str


# -----------------------------
# Per-card breakdown (matches total count)
# -----------------------------

def pcbs_flagged_by_card(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns: Card, Count

    Count definition matches cards_scanned_over_time() totals:
      - Line2: unique (StartDateTime, PCBID) grouped by CardName
      - Line1/Line4: unique (Card, PCBID, StartDateTime) grouped by Card
    """
    if "StartDateTime" not in df.columns:
        return pd.DataFrame(columns=["Card", "Count"])

    d = df[df["StartDateTime"].notna()].copy()
    if d.empty:
        return pd.DataFrame(columns=["Card", "Count"])

    # normalize required cols
    d["PCBID"] = d.get("PCBID", "").fillna("").astype(str).str.strip()
    d = d[d["PCBID"] != ""].copy()
    if d.empty:
        return pd.DataFrame(columns=["Card", "Count"])

    d["Card"] = _jobfile_series(d).apply(_extract_card_name)

    # key built using df.attrs['line']
    d.attrs["line"] = df.attrs.get("line", "")
    d["ScanKey"] = _make_scan_key(d)

    out = (
        d.groupby("Card", as_index=False)["ScanKey"]
         .nunique()
         .rename(columns={"ScanKey": "Count"})
    )
    out["Count"] = out["Count"].fillna(0).astype(int)
    return out.sort_values("Count", ascending=False)


# -----------------------------
# Time series (PCBs flagged over time)
# -----------------------------

def cards_scanned_over_time(df: pd.DataFrame, hour_to_day_threshold_days: int = 3):
    """
    Returns (ts_df, grain, total_flagged)

    Updated counting:
      - Line2: unique pair (StartDateTime, PCBID)  → rescans counted when StartDateTime changes
      - Line1/Line4: unique (Card, PCBID, StartDateTime)  → avoids cross-card PCBID collisions + counts rescans
    """
    if "StartDateTime" not in df.columns:
        raise ValueError("StartDateTime missing.")

    d = df[df["StartDateTime"].notna()].copy()
    if d.empty:
        return pd.DataFrame(columns=["TimeTS", "Count"]), "hour", 0

    # decide grain
    tmin = d["StartDateTime"].min()
    tmax = d["StartDateTime"].max()
    span_days = (tmax - tmin).total_seconds() / 86400.0
    grain = "hour" if span_days <= hour_to_day_threshold_days else "day"
    freq = "h" if grain == "hour" else "D"

    # normalize PCBID
    d["PCBID"] = d.get("PCBID", "").fillna("").astype(str).str.strip()
    d = d[d["PCBID"] != ""].copy()
    if d.empty:
        return pd.DataFrame(columns=["TimeTS", "Count"]), grain, 0

    # preserve line attribute for key-building
    d.attrs["line"] = df.attrs.get("line", "")

    # build scan key (line2 uses StartDateTime|PCBID, others use Card|PCBID|StartDateTime)
    d["ScanKey"] = _make_scan_key(d)

    ts = (
        d.set_index("StartDateTime")
         .groupby(pd.Grouper(freq=freq))["ScanKey"]
         .nunique()
         .rename("Count")
         .reset_index()
         .rename(columns={"StartDateTime": "TimeTS"})
    )
    ts["Count"] = ts["Count"].fillna(0).astype(int)
    total_flagged = int(ts["Count"].sum()) if not ts.empty else 0
    return ts, grain, total_flagged
