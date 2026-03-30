# src/chatbot_db.py
"""
AOI Chatbot — Knowledge Base

Stores pre-processed knowledge from uploaded historical CSVs in a separate
SQLite database (aoi_chatbot.db), kept next to aoi_logs.db.

Tables:
  card_defect_history   — defect occurrence counts per card/line (cumulative)
  component_registry    — component/package names seen per card/line
  daily_card_summaries  — per-date per-card flagged counts + top defects
  ingestion_log         — tracks which files have been ingested
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.log_db import get_db_path
from src.analysis import (
    top_defects,
    _extract_card_name,
    _jobfile_series,
)

_UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# DB path
# ---------------------------------------------------------------------------

def get_chatbot_db_path() -> str:
    return str(Path(get_db_path()).parent / "aoi_chatbot.db")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS card_defect_history (
    card_name        TEXT NOT NULL,
    defect_type      TEXT NOT NULL,
    line             TEXT NOT NULL,
    occurrence_count INTEGER DEFAULT 0,
    last_seen        TEXT,
    PRIMARY KEY (card_name, defect_type, line)
);

CREATE TABLE IF NOT EXISTS component_registry (
    component_name   TEXT NOT NULL,
    package_type     TEXT,
    line             TEXT NOT NULL,
    card_name        TEXT NOT NULL,
    occurrence_count INTEGER DEFAULT 0,
    PRIMARY KEY (component_name, line, card_name)
);

CREATE TABLE IF NOT EXISTS daily_card_summaries (
    log_date         TEXT NOT NULL,
    line             TEXT NOT NULL,
    card_name        TEXT NOT NULL,
    flagged_count    INTEGER DEFAULT 0,
    total_rows       INTEGER DEFAULT 0,
    top_defects_json TEXT,
    PRIMARY KEY (log_date, line, card_name)
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    filename     TEXT NOT NULL,
    line         TEXT NOT NULL,
    row_count    INTEGER,
    ingested_at  TEXT,
    PRIMARY KEY (filename, line)
);
"""


def init_chatbot_db(db_path: str = None):
    path = db_path or get_chatbot_db_path()
    con = sqlite3.connect(path)
    con.executescript(_SCHEMA)
    con.commit()
    con.close()


def reset_chatbot_db(db_path: str = None):
    """Wipe all learned data and ingestion history so files can be re-ingested."""
    path = db_path or get_chatbot_db_path()
    con = sqlite3.connect(path)
    con.executescript("""
        DELETE FROM card_defect_history;
        DELETE FROM component_registry;
        DELETE FROM daily_card_summaries;
        DELETE FROM ingestion_log;
    """)
    con.commit()
    con.close()


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def already_ingested(filename: str, line: str, db_path: str = None) -> bool:
    path = db_path or get_chatbot_db_path()
    try:
        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute(
            "SELECT 1 FROM ingestion_log WHERE filename=? AND line=?",
            (filename, line),
        )
        found = cur.fetchone() is not None
        con.close()
        return found
    except Exception:
        return False


def ingest_csv(df, line_key: str, filename: str, db_path: str = None) -> dict:
    """
    Process a cleaned DataFrame and add its knowledge to aoi_chatbot.db.

    Args:
        df:        Cleaned AOI DataFrame (from clean_aoi_data).
        line_key:  "line1" | "line2" | "line4"
        filename:  Original CSV filename (used to prevent double-ingestion).
        db_path:   Override DB path (defaults to standard location).

    Returns dict with keys: rows, cards, defects, skipped (True if duplicate).
    """
    path = db_path or get_chatbot_db_path()
    init_chatbot_db(path)

    if already_ingested(filename, line_key, path):
        return {"rows": 0, "cards": 0, "defects": 0, "skipped": True}

    if df is None or df.empty:
        return {"rows": 0, "cards": 0, "defects": 0, "skipped": False}

    con = sqlite3.connect(path)
    cur = con.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- assign card names per row ---
    jf = _jobfile_series(df)
    card_col = jf.apply(_extract_card_name)

    # --- extract shift date (7am→7am) from StartDateTime ---
    # Subtract 7 h so that 00:00-06:59 rows map to the *previous* shift day,
    # matching the 7am-to-7am window used in the Analysis tab.
    date_col = None
    if "StartDateTime" in df.columns and pd.api.types.is_datetime64_any_dtype(df["StartDateTime"]):
        shifted = df["StartDateTime"] - pd.Timedelta(hours=7)
        date_col = shifted.dt.strftime("%Y-%m-%d").fillna("")

    unique_cards = set()
    unique_defects = set()

    # ---- per-card aggregation ----
    for card_name in card_col.unique():
        if card_name == _UNKNOWN or not card_name:
            continue
        unique_cards.add(card_name)
        mask = card_col == card_name
        card_df = df[mask]

        # 1. card_defect_history — cumulative defect counts
        try:
            defects = top_defects(card_df, top_n=None)
        except Exception:
            defects = None

        if defects is not None and not defects.empty:
            for _, row in defects.iterrows():
                dname = str(row["Defect"])
                cnt   = int(row["Count"])
                unique_defects.add(dname)
                cur.execute(
                    """
                    INSERT INTO card_defect_history
                        (card_name, defect_type, line, occurrence_count, last_seen)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(card_name, defect_type, line) DO UPDATE SET
                        occurrence_count = occurrence_count + excluded.occurrence_count,
                        last_seen        = excluded.last_seen
                    """,
                    (card_name, dname, line_key, cnt, now[:10]),
                )

        # 2. component_registry — unique components/packages
        if "uname" in card_df.columns and "PackageName" in card_df.columns:
            comp_group = (
                card_df[["uname", "PackageName"]]
                .dropna(subset=["uname"])
                .groupby(["uname", "PackageName"])
                .size()
                .reset_index(name="cnt")
            )
            for _, row in comp_group.iterrows():
                comp  = str(row["uname"]).strip()
                pkg   = str(row["PackageName"]).strip() if row["PackageName"] else ""
                cnt   = int(row["cnt"])
                if not comp:
                    continue
                cur.execute(
                    """
                    INSERT INTO component_registry
                        (component_name, package_type, line, card_name, occurrence_count)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(component_name, line, card_name) DO UPDATE SET
                        occurrence_count = occurrence_count + excluded.occurrence_count
                    """,
                    (comp, pkg, line_key, card_name, cnt),
                )

        # 3. daily_card_summaries — per date
        if date_col is not None:
            card_dates = date_col[mask]
            for log_date in card_dates.unique():
                if not log_date or log_date == "NaT":
                    continue
                day_mask  = mask & (date_col == log_date)
                day_df    = df[day_mask]
                flagged = 0
                if "PCBID" in day_df.columns and "StartDateTime" in day_df.columns:
                    valid = (
                        day_df["StartDateTime"].notna() &
                        (day_df["PCBID"].fillna("").astype(str).str.strip() != "")
                    )
                    fdf = day_df[valid]
                    if not fdf.empty:
                        sk = (
                            fdf["PCBID"].fillna("").astype(str).str.strip() + "|" +
                            fdf["StartDateTime"].dt.strftime("%Y-%m-%d %H:%M:%S")
                        )
                        flagged = int(sk.nunique())
                total_rows = len(day_df)
                try:
                    all_d = top_defects(day_df, top_n=None)
                    top5_json = json.dumps(
                        [{"defect": r["Defect"], "count": int(r["Count"])}
                         for _, r in all_d.iterrows()]
                    )
                except Exception:
                    top5_json = "[]"

                cur.execute(
                    """
                    INSERT INTO daily_card_summaries
                        (log_date, line, card_name, flagged_count, total_rows, top_defects_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(log_date, line, card_name) DO UPDATE SET
                        flagged_count    = flagged_count    + excluded.flagged_count,
                        total_rows       = total_rows       + excluded.total_rows,
                        top_defects_json = excluded.top_defects_json
                    """,
                    (log_date, line_key, card_name, flagged, total_rows, top5_json),
                )

    # record ingestion
    cur.execute(
        "INSERT OR REPLACE INTO ingestion_log (filename, line, row_count, ingested_at) VALUES (?,?,?,?)",
        (filename, line_key, len(df), now),
    )

    con.commit()
    con.close()

    return {
        "rows":    len(df),
        "cards":   len(unique_cards),
        "defects": len(unique_defects),
        "skipped": False,
    }


# ---------------------------------------------------------------------------
# Query helpers (used by chatbot.py for context building)
# ---------------------------------------------------------------------------

def _connect_ro(db_path: str):
    """Open chatbot DB read-only."""
    uri = "file:{}?mode=ro".format(db_path.replace("\\", "/"))
    return sqlite3.connect(uri, uri=True)


def get_known_cards(db_path: str = None) -> list:
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        rows = con.execute(
            "SELECT DISTINCT card_name FROM card_defect_history ORDER BY card_name"
        ).fetchall()
        con.close()
        return [r[0] for r in rows]
    except Exception:
        return []


def get_known_defects(db_path: str = None) -> list:
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        rows = con.execute(
            "SELECT DISTINCT defect_type FROM card_defect_history ORDER BY defect_type"
        ).fetchall()
        con.close()
        return [r[0] for r in rows]
    except Exception:
        return []


def query_card_defects(card_name: str, line: str = None,
                       top_n: int = 10, db_path: str = None) -> list:
    """Return [{defect_type, occurrence_count, line}, ...] for a card."""
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        if line:
            rows = con.execute(
                """SELECT defect_type, SUM(occurrence_count) AS cnt, line
                   FROM card_defect_history
                   WHERE card_name = ? AND line = ?
                   GROUP BY defect_type, line
                   ORDER BY cnt DESC LIMIT ?""",
                (card_name, line, top_n),
            ).fetchall()
        else:
            rows = con.execute(
                """SELECT defect_type, SUM(occurrence_count) AS cnt, GROUP_CONCAT(DISTINCT line)
                   FROM card_defect_history
                   WHERE card_name = ?
                   GROUP BY defect_type
                   ORDER BY cnt DESC LIMIT ?""",
                (card_name, top_n),
            ).fetchall()
        con.close()
        return [{"defect_type": r[0], "count": r[1], "line": r[2]} for r in rows]
    except Exception:
        return []


def query_component_info(card_name: str = None, line: str = None,
                         db_path: str = None) -> list:
    """Return unique components seen for a card/line."""
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        conditions, params = [], []
        if card_name:
            conditions.append("card_name = ?"); params.append(card_name)
        if line:
            conditions.append("line = ?"); params.append(line)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        rows = con.execute(
            f"""SELECT component_name, package_type, SUM(occurrence_count)
                FROM component_registry {where}
                GROUP BY component_name, package_type
                ORDER BY SUM(occurrence_count) DESC LIMIT 30""",
            params,
        ).fetchall()
        con.close()
        return [{"component": r[0], "package": r[1], "count": r[2]} for r in rows]
    except Exception:
        return []


def query_daily_card_summary(log_date: str, line: str = None,
                             db_path: str = None) -> list:
    """Return per-card summary for a specific date."""
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        if line:
            rows = con.execute(
                """SELECT card_name, flagged_count, total_rows, top_defects_json
                   FROM daily_card_summaries
                   WHERE log_date = ? AND line = ?
                   ORDER BY flagged_count DESC""",
                (log_date, line),
            ).fetchall()
            con.close()
            return [
                {"card": r[0], "flagged": r[1], "total_rows": r[2],
                 "top_defects": json.loads(r[3]) if r[3] else []}
                for r in rows
            ]
        else:
            # Fetch all lines for this date and aggregate per card in Python
            # (avoids MAX(top_defects_json) which picks arbitrary line data)
            rows = con.execute(
                """SELECT card_name, flagged_count, total_rows, top_defects_json
                   FROM daily_card_summaries WHERE log_date = ?""",
                (log_date,),
            ).fetchall()
            con.close()
            card_data: dict = {}
            for card_name, flagged, total_rows, json_str in rows:
                if card_name not in card_data:
                    card_data[card_name] = {"flagged": 0, "total_rows": 0, "defects": {}}
                card_data[card_name]["flagged"] += flagged or 0
                card_data[card_name]["total_rows"] += total_rows or 0
                if json_str:
                    try:
                        for d in json.loads(json_str):
                            n, c = d.get("defect", ""), int(d.get("count", 0))
                            if n:
                                card_data[card_name]["defects"][n] = (
                                    card_data[card_name]["defects"].get(n, 0) + c
                                )
                    except Exception:
                        pass
            result = []
            for cname, data in sorted(card_data.items(),
                                      key=lambda x: x[1]["flagged"], reverse=True):
                top_d = sorted(data["defects"].items(),
                               key=lambda x: x[1], reverse=True)[:5]
                result.append({
                    "card": cname,
                    "flagged": data["flagged"],
                    "total_rows": data["total_rows"],
                    "top_defects": [{"defect": n, "count": c} for n, c in top_d],
                })
            return result
    except Exception:
        return []


def query_worst_card(line: str = None, start_iso: str = None, end_iso: str = None,
                     top_n: int = 10, db_path: str = None) -> list:
    """Return cards ranked by total flagged PCBs, with optional date/line filters."""
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        conditions, params = [], []
        if line:
            conditions.append("line = ?"); params.append(line)
        if start_iso:
            conditions.append("log_date >= ?"); params.append(start_iso)
        if end_iso:
            conditions.append("log_date <= ?"); params.append(end_iso)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(top_n)
        rows = con.execute(
            f"""SELECT card_name, SUM(flagged_count) AS total_flagged
               FROM daily_card_summaries {where}
               GROUP BY card_name ORDER BY total_flagged DESC LIMIT ?""",
            params,
        ).fetchall()
        con.close()
        return [{"card": r[0], "total_flagged": r[1]} for r in rows]
    except Exception:
        return []


def query_best_card(line: str = None, start_iso: str = None, end_iso: str = None,
                    top_n: int = 10, db_path: str = None) -> list:
    """Return cards ranked by fewest flagged PCBs (ascending), with optional date/line filters."""
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        conditions, params = [], []
        if line:
            conditions.append("line = ?"); params.append(line)
        if start_iso:
            conditions.append("log_date >= ?"); params.append(start_iso)
        if end_iso:
            conditions.append("log_date <= ?"); params.append(end_iso)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(top_n)
        rows = con.execute(
            f"""SELECT card_name, SUM(flagged_count) AS total_flagged
               FROM daily_card_summaries {where}
               GROUP BY card_name ORDER BY total_flagged ASC LIMIT ?""",
            params,
        ).fetchall()
        con.close()
        return [{"card": r[0], "total_flagged": r[1]} for r in rows]
    except Exception:
        return []


def query_defect_cards(defect_type: str, line: str = None,
                       top_n: int = 10, db_path: str = None) -> list:
    """Return which cards have a specific defect most often."""
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        if line:
            rows = con.execute(
                """SELECT card_name, SUM(occurrence_count) AS cnt
                   FROM card_defect_history
                   WHERE defect_type = ? AND line = ?
                   GROUP BY card_name ORDER BY cnt DESC LIMIT ?""",
                (defect_type, line, top_n),
            ).fetchall()
        else:
            rows = con.execute(
                """SELECT card_name, SUM(occurrence_count) AS cnt
                   FROM card_defect_history
                   WHERE defect_type = ?
                   GROUP BY card_name ORDER BY cnt DESC LIMIT ?""",
                (defect_type, top_n),
            ).fetchall()
        con.close()
        return [{"card": r[0], "count": r[1]} for r in rows]
    except Exception:
        return []


def query_defects_by_date(start_iso: str, end_iso: str, line: str = None,
                          card: str = None, top_n: int = 20,
                          db_path: str = None) -> list:
    """
    Return defect totals for a date (or date range), optionally filtered by
    line and/or card. Aggregates top_defects_json from daily_card_summaries.
    """
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        conditions = ["log_date >= ?", "log_date <= ?"]
        params: list = [start_iso, end_iso]
        if line:
            conditions.append("line = ?")
            params.append(line)
        if card:
            conditions.append("card_name = ?")
            params.append(card)
        where = " AND ".join(conditions)
        rows = con.execute(
            f"SELECT top_defects_json FROM daily_card_summaries WHERE {where}",
            params,
        ).fetchall()
        con.close()

        totals: dict = {}
        for (json_str,) in rows:
            if not json_str:
                continue
            try:
                for d in json.loads(json_str):
                    name = d.get("defect", "")
                    count = int(d.get("count", 0))
                    if name:
                        totals[name] = totals.get(name, 0) + count
            except Exception:
                continue

        sorted_defects = sorted(totals.items(), key=lambda x: x[1], reverse=True)
        return [{"defect_type": name, "count": cnt}
                for name, cnt in sorted_defects[:top_n]]
    except Exception:
        return []


def query_all_defects(line: str = None, top_n: int = 15,
                      db_path: str = None) -> list:
    """Return all-time defect totals across all cards from card_defect_history."""
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        if line:
            rows = con.execute(
                """SELECT defect_type, SUM(occurrence_count) AS cnt
                   FROM card_defect_history WHERE line = ?
                   GROUP BY defect_type ORDER BY cnt DESC LIMIT ?""",
                (line, top_n),
            ).fetchall()
        else:
            rows = con.execute(
                """SELECT defect_type, SUM(occurrence_count) AS cnt
                   FROM card_defect_history
                   GROUP BY defect_type ORDER BY cnt DESC LIMIT ?""",
                (top_n,),
            ).fetchall()
        con.close()
        return [{"defect_type": r[0], "count": r[1]} for r in rows]
    except Exception:
        return []


def query_defect_trend(defect_type: str, start_iso: str, end_iso: str,
                       line: str = None, card: str = None,
                       db_path: str = None) -> list:
    """
    Return per-day occurrence counts for a specific defect type.
    Aggregates top_defects_json across all cards (and optionally a single card)
    for each day in the date range.
    """
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        conditions = ["log_date >= ?", "log_date <= ?"]
        params: list = [start_iso, end_iso]
        if line:
            conditions.append("line = ?")
            params.append(line)
        if card:
            conditions.append("card_name = ?")
            params.append(card)
        where = " AND ".join(conditions)
        rows = con.execute(
            f"SELECT log_date, top_defects_json FROM daily_card_summaries "
            f"WHERE {where} ORDER BY log_date",
            params,
        ).fetchall()
        con.close()

        daily: dict = {}
        target = defect_type.upper()
        for log_date, json_str in rows:
            if not json_str:
                continue
            try:
                for d in json.loads(json_str):
                    if d.get("defect", "").upper() == target:
                        daily[log_date] = daily.get(log_date, 0) + int(d.get("count", 0))
            except Exception:
                continue

        return [{"log_date": dt, "count": cnt}
                for dt, cnt in sorted(daily.items())]
    except Exception:
        return []


def query_card_stats(card_name: str, line: str = None,
                     start_iso: str = None, end_iso: str = None,
                     db_path: str = None) -> dict:
    """
    Return summary stats for a specific card: flagged PCBs, defect events, days.
    Optional date filter via start_iso/end_iso.
    """
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        conditions = ["card_name = ?"]
        params: list = [card_name]
        if line:
            conditions.append("line = ?")
            params.append(line)
        if start_iso:
            conditions.append("log_date >= ?")
            params.append(start_iso)
        if end_iso:
            conditions.append("log_date <= ?")
            params.append(end_iso)
        where = " AND ".join(conditions)
        row = con.execute(
            f"""SELECT SUM(flagged_count), SUM(total_rows), COUNT(DISTINCT log_date)
               FROM daily_card_summaries WHERE {where}""",
            params,
        ).fetchone()
        con.close()
        if not row or row[0] is None:
            return {}
        return {
            "flagged": int(row[0] or 0),
            "defect_events": int(row[1] or 0),
            "days": int(row[2] or 0),
        }
    except Exception:
        return {}


def query_all_card_names(line: str = None, db_path: str = None) -> list:
    """Return all distinct cards with total flagged counts, sorted by most flagged."""
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        if line:
            rows = con.execute(
                """SELECT card_name, SUM(flagged_count) AS total_flagged
                   FROM daily_card_summaries WHERE line = ?
                   GROUP BY card_name ORDER BY total_flagged DESC""",
                (line,),
            ).fetchall()
        else:
            rows = con.execute(
                """SELECT card_name, SUM(flagged_count) AS total_flagged
                   FROM daily_card_summaries
                   GROUP BY card_name ORDER BY total_flagged DESC"""
            ).fetchall()
        con.close()
        return [{"card": r[0], "total_flagged": r[1]} for r in rows]
    except Exception:
        return []


def query_all_defect_types(line: str = None, db_path: str = None) -> list:
    """Return all distinct defect types with total occurrence counts."""
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        if line:
            rows = con.execute(
                """SELECT defect_type, SUM(occurrence_count) AS cnt
                   FROM card_defect_history WHERE line = ?
                   GROUP BY defect_type ORDER BY cnt DESC""",
                (line,),
            ).fetchall()
        else:
            rows = con.execute(
                """SELECT defect_type, SUM(occurrence_count) AS cnt
                   FROM card_defect_history
                   GROUP BY defect_type ORDER BY cnt DESC"""
            ).fetchall()
        con.close()
        return [{"defect_type": r[0], "count": r[1]} for r in rows]
    except Exception:
        return []


def query_card_daily_flagged(card_name: str, start_iso: str, end_iso: str,
                             line: str = None, db_path: str = None) -> list:
    """Return per-day flagged PCB counts for a specific card."""
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        conditions = ["card_name = ?", "log_date >= ?", "log_date <= ?"]
        params: list = [card_name, start_iso, end_iso]
        if line:
            conditions.append("line = ?")
            params.append(line)
        where = " AND ".join(conditions)
        rows = con.execute(
            f"""SELECT log_date, SUM(flagged_count) AS flagged
               FROM daily_card_summaries WHERE {where}
               GROUP BY log_date ORDER BY log_date ASC""",
            params,
        ).fetchall()
        con.close()
        return [{"log_date": r[0], "flagged": r[1]} for r in rows]
    except Exception:
        return []


def query_daily_top_cards(start_iso: str, end_iso: str, line: str = None,
                          top_n: int = 5, db_path: str = None) -> list:
    """
    Return per-day top-N card rankings for a date range.
    Returns [{"log_date": ..., "cards": [{"card": ..., "flagged": ...}, ...]}, ...]
    """
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        conditions = ["log_date >= ?", "log_date <= ?"]
        params: list = [start_iso, end_iso]
        if line:
            conditions.append("line = ?")
            params.append(line)
        where = " AND ".join(conditions)
        rows = con.execute(
            f"""SELECT log_date, card_name, SUM(flagged_count) AS flagged
               FROM daily_card_summaries WHERE {where}
               GROUP BY log_date, card_name
               ORDER BY log_date ASC, flagged DESC""",
            params,
        ).fetchall()
        con.close()
        daily: dict = {}
        for log_date, card_name, flagged in rows:
            if log_date not in daily:
                daily[log_date] = []
            daily[log_date].append({"card": card_name, "flagged": flagged})
        return [
            {"log_date": dt, "cards": cards[:top_n]}
            for dt, cards in sorted(daily.items())
        ]
    except Exception:
        return []


def query_range_card_summary(start_iso: str, end_iso: str,
                              line: str = None, db_path: str = None) -> list:
    """Return per-card totals over a date range (for multi-day summaries)."""
    path = db_path or get_chatbot_db_path()
    try:
        con = _connect_ro(path)
        if line:
            rows = con.execute(
                """SELECT card_name, SUM(flagged_count), SUM(total_rows)
                   FROM daily_card_summaries
                   WHERE log_date >= ? AND log_date <= ? AND line = ?
                   GROUP BY card_name ORDER BY SUM(flagged_count) DESC LIMIT 10""",
                (start_iso, end_iso, line),
            ).fetchall()
        else:
            rows = con.execute(
                """SELECT card_name, SUM(flagged_count), SUM(total_rows)
                   FROM daily_card_summaries
                   WHERE log_date >= ? AND log_date <= ?
                   GROUP BY card_name ORDER BY SUM(flagged_count) DESC LIMIT 10""",
                (start_iso, end_iso),
            ).fetchall()
        con.close()
        return [{"card": r[0], "flagged": r[1], "total_rows": r[2]} for r in rows]
    except Exception:
        return []
