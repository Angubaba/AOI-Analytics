import os
import sys
import sqlite3
import shutil
from pathlib import Path
from typing import Optional, List, Dict


def _legacy_appdata_db_path() -> Path:
    # OLD location you previously used
    if os.name == "nt":
        base = os.environ.get("APPDATA") or str(Path.home() / "AppData" / "Roaming")
        folder = Path(base) / "AOI_Analytics"
        return folder / "aoi_logs.db"
    return Path.home() / ".aoi_analytics" / "aoi_logs.db"


def _portable_app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(os.path.dirname(sys.executable))
    return Path(__file__).resolve().parent.parent


def get_db_path() -> str:
    """
    Portable DB:
      - Packaged EXE: <EXE_FOLDER>/aoi_logs.db
      - Source run:   <PROJECT_ROOT>/aoi_logs.db

    Migration:
      - If portable DB doesn't exist but legacy APPDATA DB exists -> copy it once.
    """
    folder = _portable_app_dir()
    folder.mkdir(parents=True, exist_ok=True)

    new_path = folder / "aoi_logs.db"
    legacy = _legacy_appdata_db_path()

    # ✅ one-time migration/copy so old logs are not lost
    try:
        if (not new_path.exists()) and legacy.exists():
            shutil.copy2(str(legacy), str(new_path))
    except Exception:
        # never block app start if copy fails
        pass

    return str(new_path)


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

            pcbs_checked INTEGER,

            ratio_rows_per_pcb REAL,

            source_file_name TEXT,
            created_at TEXT DEFAULT (datetime('now')),

            PRIMARY KEY (log_date, line)
        )
        """
    )

    # migration for older DBs
    try:
        cur.execute("ALTER TABLE daily_logs ADD COLUMN pcbs_checked INTEGER")
    except sqlite3.OperationalError:
        pass

    con.commit()
    con.close()


def log_exists(log_date: str, line: str) -> bool:
    con = _connect()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM daily_logs WHERE log_date=? AND line=? LIMIT 1", (log_date, line))
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
        cur.execute("DELETE FROM daily_logs WHERE log_date=? AND line=?", (log_date, line))

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
