import os
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict


def get_db_path() -> str:
    """
    Persistent per-user DB path so logs survive packaging and restarts.
    Windows: %APPDATA%\\AOI_Analytics\\aoi_logs.db
    Others:  ~/.aoi_analytics/aoi_logs.db
    """
    if os.name == "nt":
        base = os.environ.get("APPDATA") or str(Path.home() / "AppData" / "Roaming")
        folder = Path(base) / "AOI_Analytics"
    else:
        folder = Path.home() / ".aoi_analytics"

    folder.mkdir(parents=True, exist_ok=True)
    return str(folder / "aoi_logs.db")


def _connect():
    return sqlite3.connect(get_db_path())


def init_db():
    con = _connect()
    cur = con.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_logs (
            log_date TEXT NOT NULL,
            line TEXT NOT NULL,

            detected_line TEXT,
            window_start TEXT,
            window_end TEXT,

            total_rows INTEGER,
            pcbs_flagged INTEGER,

            -- ✅ NEW: manual total checked
            pcbs_checked INTEGER,

            -- kept for backward compatibility; you can ignore it now
            ratio_rows_per_pcb REAL,

            source_file_name TEXT,
            created_at TEXT DEFAULT (datetime('now')),

            PRIMARY KEY (log_date, line)
        )
        """
    )

    # ✅ migration (existing DBs)
    try:
        cur.execute("ALTER TABLE daily_logs ADD COLUMN pcbs_checked INTEGER")
    except sqlite3.OperationalError:
        pass  # already exists

    con.commit()
    con.close()


def log_exists(log_date: str, line: str) -> bool:
    con = _connect()
    cur = con.cursor()
    cur.execute(
        "SELECT 1 FROM daily_logs WHERE log_date=? AND line=? LIMIT 1",
        (log_date, line),
    )
    row = cur.fetchone()
    con.close()
    return row is not None


def delete_log(log_date: str, line: str) -> bool:
    con = _connect()
    cur = con.cursor()
    cur.execute("DELETE FROM daily_logs WHERE log_date=? AND line=?", (log_date, line))
    con.commit()
    deleted = cur.rowcount > 0
    con.close()
    return deleted


def upsert_log(
    log_date: str,
    line: str,
    detected_line: str,
    window_start: str,
    window_end: str,
    total_rows: int,
    pcbs_flagged: int,
    pcbs_checked: Optional[int],
    ratio_rows_per_pcb: Optional[float],
    source_file_name: str,
    replace: bool = False,
):
    con = _connect()
    cur = con.cursor()

    if replace:
        cur.execute(
            "DELETE FROM daily_logs WHERE log_date=? AND line=?",
            (log_date, line),
        )

    cur.execute(
        """
        INSERT OR REPLACE INTO daily_logs
        (log_date, line, detected_line, window_start, window_end,
         total_rows, pcbs_flagged, pcbs_checked, ratio_rows_per_pcb, source_file_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            log_date,
            line,
            detected_line,
            window_start,
            window_end,
            int(total_rows),
            int(pcbs_flagged),
            None if pcbs_checked is None else int(pcbs_checked),
            None if ratio_rows_per_pcb is None else float(ratio_rows_per_pcb),
            source_file_name,
        ),
    )

    con.commit()
    con.close()


def fetch_pcbs_flagged_trend(line: str, start_date: str, end_date: str) -> List[Dict]:
    """
    Returns list of dicts:
      [{"log_date": "YYYY-MM-DD", "pcbs_flagged": 123, "pcbs_checked": 1000}, ...]
    Inclusive range: start_date <= log_date <= end_date
    """
    con = _connect()
    cur = con.cursor()
    cur.execute(
        """
        SELECT log_date,
               COALESCE(pcbs_flagged, 0),
               COALESCE(pcbs_checked, 0)
        FROM daily_logs
        WHERE line = ?
          AND log_date >= ?
          AND log_date <= ?
        ORDER BY log_date ASC
        """,
        (line, start_date, end_date),
    )
    rows = cur.fetchall()
    con.close()

    return [{"log_date": r[0], "pcbs_flagged": int(r[1]), "pcbs_checked": int(r[2])} for r in rows]
