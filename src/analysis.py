# src/analysis.py
import os
import re
import pandas as pd

# Module-level constants — avoids recompiling on every function call
_UNKNOWN   = "UNKNOWN_CARD"
_ALPHA_RE  = re.compile(r"[A-Za-z]")
_CARD_RE   = re.compile(r"([^\\\/]+)\.KYJOB", re.IGNORECASE)


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
    s = s[s.str.contains(_ALPHA_RE)]
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
    """
    if jobfile is None:
        return _UNKNOWN

    s = str(jobfile).strip().strip('"')
    if s == "" or s.lower() == "nan":
        return _UNKNOWN

    m = _CARD_RE.search(s)
    if m:
        name = m.group(1).strip()
    else:
        parts = re.split(r"[\\\/]+", s)
        parts = [p for p in parts if p.strip()]
        name = parts[-1].strip() if parts else _UNKNOWN

    toks = [t for t in name.split() if t.upper() != "NEW"]
    name = " ".join(toks).strip()

    return name if name else _UNKNOWN


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
        pcb = pd.Series([""] * len(df), index=df.index)
    else:
        pcb = df["PCBID"].fillna("").astype(str).str.strip()

    dt = df["StartDateTime"]
    # If StartDateTime isn't datetime, this will throw. That’s fine: caller should clean first.
    dt_str = dt.dt.strftime("%Y-%m-%d %H:%M:%S")

    line = df.attrs.get("line", "")

    if line == "line2":
        # ✅ DO NOT CHANGE (line2 counting was correct)
        return pcb + "|" + dt_str

    card = _jobfile_series(df).apply(_extract_card_name).astype(str)
    return card + "|" + pcb + "|" + dt_str


# -----------------------------
# Per-card breakdown (matches total count)
# -----------------------------

def pcbs_flagged_by_card(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns: Card, Count
    Count definition matches cards_scanned_over_time() totals.
    """
    if "StartDateTime" not in df.columns:
        return pd.DataFrame(columns=["Card", "Count"])

    pcbid_col = df["PCBID"].fillna("").astype(str).str.strip() if "PCBID" in df.columns else ""
    d = df[df["StartDateTime"].notna() & (pcbid_col != "")].copy()
    if d.empty:
        return pd.DataFrame(columns=["Card", "Count"])

    d["PCBID"] = d["PCBID"].fillna("").astype(str).str.strip()
    d["Card"] = _jobfile_series(d).apply(_extract_card_name)

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
# 7am → 7am helpers
# -----------------------------

def _window_start_7am(ts: pd.Timestamp) -> pd.Timestamp:
    """
    For a timestamp, return the 07:00 boundary that starts the 7am→7am "day".
    If ts time is before 07:00, the window starts at 07:00 of previous day.
    """
    ts = pd.Timestamp(ts)
    base_date = ts.normalize()
    seven = base_date + pd.Timedelta(hours=7)
    if ts < seven:
        return (base_date - pd.Timedelta(days=1)) + pd.Timedelta(hours=7)
    return seven


def _full_hour_index_from_window(wstart: pd.Timestamp) -> pd.DatetimeIndex:
    """07:00..06:00 next day (24 hours), hourly."""
    if wstart is None or pd.isna(wstart):
        return pd.DatetimeIndex([])
    wstart = pd.Timestamp(wstart)
    return pd.date_range(start=wstart, periods=24, freq="h")


def _trim_to_dominant_7to7_window(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Timestamp | None]:
    """
    Robust outlier trimming for one CSV:
    - Compute shift-day for each row: (StartDateTime - 7h).date
    - Pick the most frequent shift-day (mode)
    - Keep only timestamps in that 07:00->07:00 window

    Returns: (trimmed_df, window_start)
    """
    if df is None or df.empty:
        return df, None
    if "StartDateTime" not in df.columns:
        return df, None

    d = df[df["StartDateTime"].notna()].copy()
    if d.empty:
        return df, None

    # Ensure datetime just in case
    if not pd.api.types.is_datetime64_any_dtype(d["StartDateTime"]):
        d["StartDateTime"] = pd.to_datetime(d["StartDateTime"], errors="coerce")
        d = d[d["StartDateTime"].notna()].copy()
        if d.empty:
            return df, None

    shifted = d["StartDateTime"] - pd.to_timedelta(7, unit="h")
    shift_day = shifted.dt.date

    md = shift_day.mode()
    if md is None or len(md) == 0:
        return df, None

    dominant_day = md.iloc[0]
    wstart = pd.Timestamp(dominant_day) + pd.Timedelta(hours=7)
    wend = wstart + pd.Timedelta(hours=24)

    out = d[(d["StartDateTime"] >= wstart) & (d["StartDateTime"] < wend)].copy()
    if out.empty:
        # fail-safe: do not wipe all data
        return d, _window_start_7am(d["StartDateTime"].max())

    return out, wstart


# -----------------------------
# Time series (PCBs flagged over time)
# -----------------------------

def cards_scanned_over_time(
    df: pd.DataFrame,
    hour_to_day_threshold_days: int = 3,
    force_7to7_when_hourly: bool = False
):
    """
    Returns (ts_df, grain, total_flagged)

    Counting (DO NOT CHANGE line2 logic):
      - Line2: unique pair (StartDateTime, PCBID)
      - Line1/Line4: unique (Card, PCBID, StartDateTime)

    If force_7to7_when_hourly=True and grain==hour:
      - trims outlier timestamps to the dominant 07:00→07:00 window
      - output is reindexed to full 07:00→07:00 hourly bins (missing hours as 0)
    """
    if "StartDateTime" not in df.columns:
        raise ValueError("StartDateTime missing.")

    d = df[df["StartDateTime"].notna()].copy()
    if d.empty:
        return pd.DataFrame(columns=["TimeTS", "Count"]), "hour", 0

    tmin = d["StartDateTime"].min()
    tmax = d["StartDateTime"].max()
    span_days = (tmax - tmin).total_seconds() / 86400.0
    grain = "hour" if span_days <= hour_to_day_threshold_days else "day"
    freq = "h" if grain == "hour" else "D"

    # ✅ OUTLIER TRIM: only for HOURLY view
    window_start = None
    if grain == "hour" and force_7to7_when_hourly:
        d, window_start = _trim_to_dominant_7to7_window(d)

    pcbid_col = d["PCBID"].fillna("").astype(str).str.strip() if "PCBID" in d.columns else ""
    d = d[pcbid_col != ""].copy()
    if d.empty:
        return pd.DataFrame(columns=["TimeTS", "Count"]), grain, 0
    d["PCBID"] = d["PCBID"].fillna("").astype(str).str.strip()

    d.attrs["line"] = df.attrs.get("line", "")
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

    # ✅ Force 7→7 only for HOURLY view
    if grain == "hour" and force_7to7_when_hourly:
        # If we trimmed, use that window; otherwise fall back to computed window from tmax
        if window_start is None and not d.empty:
            window_start = _window_start_7am(d["StartDateTime"].max())

        full_idx = _full_hour_index_from_window(window_start)
        if len(full_idx) > 0:
            ts2 = ts.set_index("TimeTS").reindex(full_idx)
            ts2.index.name = "TimeTS"
            ts2["Count"] = ts2["Count"].fillna(0).astype(int)
            ts = ts2.reset_index()

    total_flagged = int(ts["Count"].sum()) if not ts.empty else 0
    return ts, grain, total_flagged


# -----------------------------
# Minute drilldown (strict 0–60 axis)
# -----------------------------

def pcbs_flagged_by_minute(df: pd.DataFrame, hour_ts):
    """
    Returns 0..59 minutes for the chosen hour.
    Minutes with 0 will still exist in the dataframe.

    IMPORTANT:
    - Count definition matches cards_scanned_over_time() logic.
      • line2: unique (StartDateTime, PCBID)
      • line1/line4: unique (Card, PCBID, StartDateTime)
    """
    if df is None or df.empty or "StartDateTime" not in df.columns:
        start = pd.Timestamp(hour_ts)
        full_minutes = pd.date_range(start=start, periods=60, freq="min")
        return pd.DataFrame({"TimeTS": full_minutes, "Count": [0] * 60})

    start = pd.Timestamp(hour_ts)
    end = start + pd.Timedelta(hours=1)

    d = df[
        (df["StartDateTime"] >= start) &
        (df["StartDateTime"] < end)
    ].copy()

    full_minutes = pd.date_range(start=start, periods=60, freq="min")

    if d.empty:
        return pd.DataFrame({"TimeTS": full_minutes, "Count": [0] * 60})

    # Ensure datetime
    if not pd.api.types.is_datetime64_any_dtype(d["StartDateTime"]):
        d["StartDateTime"] = pd.to_datetime(d["StartDateTime"], errors="coerce")
        d = d[d["StartDateTime"].notna()].copy()
        if d.empty:
            return pd.DataFrame({"TimeTS": full_minutes, "Count": [0] * 60})

    pcbid_col = d["PCBID"].fillna("").astype(str).str.strip() if "PCBID" in d.columns else ""
    d = d[pcbid_col != ""].copy()
    if d.empty:
        return pd.DataFrame({"TimeTS": full_minutes, "Count": [0] * 60})
    d["PCBID"] = d["PCBID"].fillna("").astype(str).str.strip()

    # Minute bucket (still preserves scan event uniqueness via StartDateTime in ScanKey)
    d["Minute"] = d["StartDateTime"].dt.floor("min")

    # Use the same scan-key logic as hourly/day totals
    d.attrs["line"] = df.attrs.get("line", "")
    d["ScanKey"] = _make_scan_key(d)

    out = (
        d.groupby("Minute")["ScanKey"]
         .nunique()
         .reindex(full_minutes, fill_value=0)
         .reset_index()
         .rename(columns={"index": "TimeTS", "ScanKey": "Count"})
    )

    # Some pandas versions name the first column "Minute" not "index"
    if "Minute" in out.columns and "TimeTS" not in out.columns:
        out = out.rename(columns={"Minute": "TimeTS"})

    out["Count"] = out["Count"].fillna(0).astype(int)
    return out


# -----------------------------
# Defect timing over 7am→7am (hourly)
# -----------------------------

def defect_occurs_over_time_7to7(df: pd.DataFrame, defect_name: str):
    """
    For a given defect label (uname), return hourly counts (event rows) in the latest 07:00→07:00 window.

    Output columns: TimeTS, Count
    - Always 24 rows (07:00..06:00 next day) if there is any data in df.
    - Counts are number of event rows where uname == defect_name in each hour.
    """
    if "StartDateTime" not in df.columns:
        return pd.DataFrame(columns=["TimeTS", "Count"])
    if "uname" not in df.columns:
        return pd.DataFrame(columns=["TimeTS", "Count"])

    d = df[df["StartDateTime"].notna()].copy()
    if d.empty:
        return pd.DataFrame(columns=["TimeTS", "Count"])

    # latest window based on max timestamp (best match to "that day")
    tmax = pd.Timestamp(d["StartDateTime"].max())
    wstart = _window_start_7am(tmax)
    wend = wstart + pd.Timedelta(hours=24)

    # filter window
    d = d[(d["StartDateTime"] >= wstart) & (d["StartDateTime"] < wend)].copy()

    full_hours = pd.date_range(start=wstart, periods=24, freq="h")

    if d.empty:
        return pd.DataFrame({"TimeTS": full_hours, "Count": [0] * 24})

    # exact match on defect label (same as shown in pareto)
    d["uname"] = d["uname"].fillna("").astype(str).str.strip()
    d = d[d["uname"] == str(defect_name).strip()].copy()

    if d.empty:
        return pd.DataFrame({"TimeTS": full_hours, "Count": [0] * 24})

    d["Hour"] = d["StartDateTime"].dt.floor("h")
    out = (
        d.groupby("Hour")["uname"]
         .size()
         .reindex(full_hours, fill_value=0)
         .reset_index()
         .rename(columns={"index": "TimeTS", "uname": "Count"})
    )

    # Some pandas versions name the first column "Hour" not "index"
    if "Hour" in out.columns and "TimeTS" not in out.columns:
        out = out.rename(columns={"Hour": "TimeTS"})

    out["Count"] = out["Count"].fillna(0).astype(int)
    return out


# =========================================================
# NEW: Combined (multi-line) helpers for ALL-LINES analytics
# =========================================================

def combine_defects_dfs(defects_dfs, top_n: int = 20) -> pd.DataFrame:
    """
    Combine multiple defect pareto dataframes (Defect, Count) by summing counts per defect.
    Returns columns: Defect, Count (sorted desc).
    """
    frames = [d for d in (defects_dfs or []) if d is not None and not d.empty]
    if not frames:
        return pd.DataFrame(columns=["Defect", "Count"])

    all_df = pd.concat(frames, ignore_index=True)
    all_df["Defect"] = all_df["Defect"].fillna("").astype(str).str.strip()
    all_df = all_df[all_df["Defect"] != ""].copy()
    all_df["Count"] = pd.to_numeric(all_df["Count"], errors="coerce").fillna(0).astype(int)

    out = (
        all_df.groupby("Defect", as_index=False)["Count"]
        .sum()
        .sort_values("Count", ascending=False)
        .reset_index(drop=True)
    )
    if top_n is None:
        return out
    return out.head(int(top_n))


def sum_time_series_dfs_on_time(ts_dfs) -> pd.DataFrame:
    """
    Sum multiple time-series dataframes with columns:
      - TimeTS
      - Count
    Output: TimeTS, Count
    """
    frames = [t for t in (ts_dfs or []) if t is not None and not t.empty]
    if not frames:
        return pd.DataFrame(columns=["TimeTS", "Count"])

    norm = []
    for t in frames:
        d = t.copy()
        if "TimeTS" not in d.columns:
            continue
        d["TimeTS"] = pd.to_datetime(d["TimeTS"], errors="coerce")
        d = d[d["TimeTS"].notna()].copy()

        if "Count" not in d.columns:
            # best-effort fallback
            for c in d.columns:
                if str(c).lower() in ("count", "counts", "value"):
                    d["Count"] = d[c]
                    break
        d["Count"] = pd.to_numeric(d.get("Count"), errors="coerce").fillna(0).astype(int)

        norm.append(d[["TimeTS", "Count"]])

    if not norm:
        return pd.DataFrame(columns=["TimeTS", "Count"])

    all_df = pd.concat(norm, ignore_index=True)
    out = (
        all_df.groupby("TimeTS", as_index=False)["Count"]
        .sum()
        .sort_values("TimeTS", ascending=True)
        .reset_index(drop=True)
    )
    out["Count"] = out["Count"].fillna(0).astype(int)
    return out


def pcbs_flagged_by_minute_multi(dfs_by_line: dict, hour_ts):
    """
    Combined minute drilldown:
      - runs pcbs_flagged_by_minute() per line (preserves line rules)
      - sums counts per minute
    Returns TimeTS, Count (60 rows).
    """
    start = pd.Timestamp(hour_ts)
    full_minutes = pd.date_range(start=start, periods=60, freq="min")

    if not dfs_by_line:
        return pd.DataFrame({"TimeTS": full_minutes, "Count": [0] * 60})

    series = []
    for _, df in dfs_by_line.items():
        if df is None or df.empty:
            continue
        try:
            ts_min = pcbs_flagged_by_minute(df, hour_ts)
            if ts_min is None or ts_min.empty:
                continue
            series.append(ts_min.copy())
        except Exception:
            continue

    if not series:
        return pd.DataFrame({"TimeTS": full_minutes, "Count": [0] * 60})

    summed = sum_time_series_dfs_on_time(series)
    summed = summed.set_index("TimeTS").reindex(full_minutes, fill_value=0).reset_index()
    summed = summed.rename(columns={"index": "TimeTS"})
    summed["Count"] = pd.to_numeric(summed["Count"], errors="coerce").fillna(0).astype(int)
    return summed


def defect_occurs_over_time_7to7_multi(dfs_by_line: dict, defect_name: str):
    """
    Combined defect timing (hourly 7→7):
      - runs defect_occurs_over_time_7to7() per line
      - sums counts per hour
    Returns 24 rows TimeTS, Count.
    """
    if not dfs_by_line:
        return pd.DataFrame(columns=["TimeTS", "Count"])

    series = []
    for _, df in dfs_by_line.items():
        if df is None or df.empty:
            continue
        try:
            ts = defect_occurs_over_time_7to7(df, defect_name)
            if ts is None or ts.empty:
                continue
            series.append(ts.copy())
        except Exception:
            continue

    if not series:
        return pd.DataFrame(columns=["TimeTS", "Count"])

    summed = sum_time_series_dfs_on_time(series)

    # Ensure we output 24 rows aligned to latest 7→7 window of the combined data
    # We'll build the window from the max timestamp across all dfs that have StartDateTime.
    tmax = None
    for _, df in dfs_by_line.items():
        if df is None or df.empty or "StartDateTime" not in df.columns:
            continue
        d = df[df["StartDateTime"].notna()]
        if d.empty:
            continue
        mx = pd.Timestamp(d["StartDateTime"].max())
        tmax = mx if tmax is None else max(tmax, mx)

    if tmax is None:
        return summed

    wstart = _window_start_7am(tmax)
    full_hours = pd.date_range(start=wstart, periods=24, freq="h")

    summed = summed.set_index("TimeTS").reindex(full_hours, fill_value=0).reset_index()
    summed = summed.rename(columns={"index": "TimeTS"})
    summed["Count"] = pd.to_numeric(summed["Count"], errors="coerce").fillna(0).astype(int)
    return summed
