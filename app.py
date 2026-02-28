# app.py
import os
import re
import sys
import threading
import traceback
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk
from datetime import datetime, date, timedelta
import calendar
import sqlite3
import subprocess
import importlib

from PIL import Image, ImageTk

from src.parsers import load_any_aoi
from src.clean_data import clean_aoi_data
from src.analysis import (
    ensure_outputs_dir,
    top_defects,
    cards_scanned_over_time,
    pcbs_flagged_by_card,
    pcbs_flagged_by_minute,
    defect_occurs_over_time_7to7,
    # multi-line helpers
    combine_defects_dfs,
    sum_time_series_dfs_on_time,
    pcbs_flagged_by_minute_multi,
    defect_occurs_over_time_7to7_multi,
)
from src.plots import (
    plot_top_defects_bars,
    plot_time_series_counts_bar,
    plot_pcbs_flagged_trend,
    plot_pcbs_flagged_by_minute,
)

from src.log_db import (
    init_db,
    get_db_path,
    log_exists,
    upsert_log,
    fetch_pcbs_flagged_trend,
)

import src.report as report

# matplotlib for FPY trend plots
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ---------------- Simple English fixes (operator friendly) ----------------
DEFECT_FIXES_SIMPLE = {
    "Solderfillet": [
        "Clean the stencil and check for blockage.",
        "Check solder paste quantity (not too less/too much).",
        "Check reflow heating settings and paste expiry.",
    ],
    "PadOverhang": [
        "Check placement is centered on the pad.",
        "Check stencil alignment.",
        "Check nozzle and camera calibration.",
    ],
    "Coplanarity": [
        "Check component is sitting flat (not tilted).",
        "Avoid bent/warped parts.",
        "Check placement pressure / support pins.",
    ],
    "Polarity": [
        "Verify + / - direction on board and part.",
        "Confirm feeder/reel orientation.",
        "Compare AOI image with polarity marking.",
    ],
    "Missing": [
        "Check feeder is not empty or jammed.",
        "Check nozzle vacuum / pick failure.",
        "Check reel end / part supply.",
    ],
    "Bridging": [
        "Check for extra paste / paste smearing.",
        "Clean stencil apertures.",
        "Check reflow settings (too much spread).",
    ],
    "UpsideDown": [
        "Check component orientation (top/bottom).",
        "Check feeder pocket orientation.",
        "Verify part library/teaching.",
    ],
    "Part(OCV/OCR)": [
        "Confirm correct reel and part number.",
        "Verify AOI recognition image (may be mismatch).",
        "Check program / part library mapping.",
    ],
    "Part(Dimen.)": [
        "Confirm correct part size/value.",
        "Check AOI tolerance/teaching values.",
        "Check placement position on pads.",
    ],
    "Part(Absence)": [
        "Check feeder jam/empty.",
        "Check nozzle vacuum leak.",
        "Check pickup height / vision pickup.",
    ],
    "OCR_OCV": [
        "Confirm correct reel and part number.",
        "Verify AOI recognition image.",
        "Check program / part library mapping.",
    ],
    "OCROCV": [
        "Confirm correct reel and part number.",
        "Verify AOI recognition image.",
        "Check program / part library mapping.",
    ],
}


# ---------------- Card extraction (for click-a-card filtering) ----------------
def _jobfile_series(df):
    if "JobFileIDShare" in df.columns:
        return df["JobFileIDShare"]
    if "JobFile" in df.columns:
        return df["JobFile"]
    return None


def _extract_card_name(jobfile: str) -> str:
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


def _safe_folder_name(s: str) -> str:
    s = (s or "UNKNOWN_CARD").strip()
    s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:120] if len(s) > 120 else s


def _normalize_defect_name(s: str) -> str:
    if s is None:
        return ""
    x = str(s).strip()
    if x.upper() in ("OCR_OCV", "OCROCV"):
        return "OCROCV"
    return x


def _parse_ddmmyyyy(s: str) -> date:
    return datetime.strptime(s.strip(), "%d/%m/%Y").date()


def _fmt_ddmmyyyy(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def _ym_to_range(year: int, month: int):
    first = date(year, month, 1)
    last = date(year, month, calendar.monthrange(year, month)[1])
    return first, last


# ---------------- Local DB helpers (for Trends UI) ----------------
def _db_connect():
    return sqlite3.connect(get_db_path())


def _db_fetch_all(line: str):
    con = _db_connect()
    cur = con.cursor()
    cur.execute(
        """
        SELECT log_date, COALESCE(pcbs_flagged, 0), pcbs_checked, COALESCE(total_rows, 0)
        FROM daily_logs
        WHERE line = ?
        ORDER BY log_date ASC
        """,
        (line,),
    )
    rows = cur.fetchall()
    con.close()
    out = []
    for r in rows:
        out.append(
            {
                "log_date": r[0],
                "pcbs_flagged": int(r[1]) if r[1] is not None else 0,
                "pcbs_checked": None if r[2] is None else int(r[2]),
                "total_rows": int(r[3]) if r[3] is not None else 0,
            }
        )
    return out


def _db_fetch_all_lines(start_date: str, end_date: str):
    """
    Combined (line1+line2+line4) by day:
      - flagged = SUM(pcbs_flagged)
      - checked = SUM(pcbs_checked) but NULL treated as 0
      - total_rows = SUM(total_rows)
    """
    con = _db_connect()
    cur = con.cursor()
    cur.execute(
        """
        SELECT log_date,
               SUM(COALESCE(pcbs_flagged, 0)) AS flagged_sum,
               SUM(COALESCE(pcbs_checked, 0)) AS checked_sum,
               SUM(COALESCE(total_rows, 0)) AS rows_sum
        FROM daily_logs
        WHERE log_date >= ?
          AND log_date <= ?
          AND line IN ('line1','line2','line4')
        GROUP BY log_date
        ORDER BY log_date ASC
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    con.close()
    return [
        {
            "log_date": r[0],
            "pcbs_flagged": int(r[1] or 0),
            "pcbs_checked": int(r[2] or 0),
            "total_rows": int(r[3] or 0),
        }
        for r in rows
    ]


def _db_fetch_all_lines_alltime():
    con = _db_connect()
    cur = con.cursor()
    cur.execute(
        """
        SELECT log_date,
               SUM(COALESCE(pcbs_flagged, 0)) AS flagged_sum,
               SUM(COALESCE(pcbs_checked, 0)) AS checked_sum,
               SUM(COALESCE(total_rows, 0)) AS rows_sum
        FROM daily_logs
        WHERE line IN ('line1','line2','line4')
        GROUP BY log_date
        ORDER BY log_date ASC
        """
    )
    rows = cur.fetchall()
    con.close()
    return [
        {
            "log_date": r[0],
            "pcbs_flagged": int(r[1] or 0),
            "pcbs_checked": int(r[2] or 0),
            "total_rows": int(r[3] or 0),
        }
        for r in rows
    ]


def _db_list_years(line: str):
    con = _db_connect()
    cur = con.cursor()

    if line == "all":
        cur.execute(
            """
            SELECT DISTINCT substr(log_date, 1, 4) AS y
            FROM daily_logs
            WHERE line IN ('line1','line2','line4')
            ORDER BY y ASC
            """
        )
    else:
        cur.execute(
            """
            SELECT DISTINCT substr(log_date, 1, 4) AS y
            FROM daily_logs
            WHERE line = ?
            ORDER BY y ASC
            """,
            (line,),
        )

    years = []
    for r in cur.fetchall():
        try:
            if r[0] is not None and str(r[0]).strip() != "":
                years.append(int(r[0]))
        except Exception:
            pass
    con.close()
    return years


def _db_list_months_for_year(line: str, year: int):
    con = _db_connect()
    cur = con.cursor()

    if line == "all":
        cur.execute(
            """
            SELECT DISTINCT substr(log_date, 6, 2) AS m
            FROM daily_logs
            WHERE substr(log_date, 1, 4) = ?
              AND line IN ('line1','line2','line4')
            ORDER BY m ASC
            """,
            (f"{year:04d}",),
        )
    else:
        cur.execute(
            """
            SELECT DISTINCT substr(log_date, 6, 2) AS m
            FROM daily_logs
            WHERE line = ?
              AND substr(log_date, 1, 4) = ?
            ORDER BY m ASC
            """,
            (line, f"{year:04d}"),
        )

    months = []
    for r in cur.fetchall():
        try:
            if r[0] is not None and str(r[0]).strip() != "":
                months.append(int(r[0]))
        except Exception:
            pass
    con.close()
    return months


# ---------------- Date picker ----------------
class DatePicker(tk.Toplevel):
    def __init__(self, master, initial: date | None = None, on_done=None):
        super().__init__(master)
        self.title("Select date (dd/mm/yyyy)")
        self.resizable(False, False)
        self.on_done = on_done
        self.initial = initial or date.today()

        self.var_day = tk.StringVar(value=f"{self.initial.day:02d}")
        self.var_month = tk.StringVar(value=f"{self.initial.month:02d}")
        self.var_year = tk.StringVar(value=f"{self.initial.year:04d}")

        frm = tk.Frame(self, padx=12, pady=12)
        frm.pack()

        tk.Label(frm, text="Day").grid(row=0, column=0, sticky="w")
        tk.Label(frm, text="Month").grid(row=0, column=1, sticky="w")
        tk.Label(frm, text="Year").grid(row=0, column=2, sticky="w")

        days = [f"{i:02d}" for i in range(1, 32)]
        months = [f"{i:02d}" for i in range(1, 13)]

        now_y = date.today().year
        years = [str(y) for y in range(now_y - 5, now_y + 6)]

        ttk.Combobox(frm, textvariable=self.var_day, values=days, width=5, state="readonly").grid(
            row=1, column=0, padx=6, pady=6
        )
        ttk.Combobox(frm, textvariable=self.var_month, values=months, width=5, state="readonly").grid(
            row=1, column=1, padx=6, pady=6
        )
        ttk.Combobox(frm, textvariable=self.var_year, values=years, width=7, state="readonly").grid(
            row=1, column=2, padx=6, pady=6
        )

        btns = tk.Frame(frm)
        btns.grid(row=2, column=0, columnspan=3, pady=(8, 0), sticky="e")

        tk.Button(btns, text="Cancel", command=self.destroy, width=10).pack(side="right", padx=6)
        tk.Button(btns, text="OK", command=self._ok, width=10).pack(side="right")

        self.grab_set()
        self.transient(master)

    def _ok(self):
        try:
            d = int(self.var_day.get())
            m = int(self.var_month.get())
            y = int(self.var_year.get())
            chosen = date(y, m, d)
        except Exception:
            messagebox.showerror("Invalid date", "Please pick a valid date.")
            return

        if self.on_done:
            self.on_done(chosen)
        self.destroy()


# ---------------- Main app ----------------
class AOIApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("AOI Analytics")
        self.geometry("1500x860")

        init_db()

        self.main_frame = tk.Frame(self)
        self.main_frame.pack(fill="both", expand=True)

        # Modern look on Windows (falls back gracefully on other platforms)
        try:
            style = ttk.Style(self)
            style.theme_use("vista")
        except Exception:
            pass

        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill="both", expand=True)

        self.analysis_tab = tk.Frame(self.notebook)
        self.log_tab = tk.Frame(self.notebook)
        self.trends_tab = tk.Frame(self.notebook)
        self.formats_tab = tk.Frame(self.notebook)
        self.report_tab = tk.Frame(self.notebook)

        self.notebook.add(self.analysis_tab, text="Analysis")
        self.notebook.add(self.log_tab, text="Log Data")
        self.notebook.add(self.trends_tab, text="Trends")
        self.notebook.add(self.formats_tab, text="Formats")
        self.notebook.add(self.report_tab, text="Report")

        self.notebook.select(self.analysis_tab)

        # images
        self._img_defects = None
        self._img_cards = None
        self._img_trend = None
        # Actual displayed image dimensions → used by _x_to_index for centering correction
        self._img_display_size = {}   # which -> (w_px, h_px)

        # store last line-level outputs (single-file analysis)
        self._line_defects_png = None
        self._line_cards_png = None
        self._line_summary = ""
        self._line_breakdown = ""
        self._line_top2 = ("", "", "", "")

        # keep df in memory (currently displayed analysis df)
        self._df_current = None
        self._current_out_dir = None

        # drilldown state (hour -> minute) for currently displayed df
        self._drill_active = False
        self._drill_hour_index = None
        self._drill_times = []
        self._drill_df = None          # df OR dict[str,df] for combined
        self._drill_hour_png = None
        self._drill_out_dir = None
        self._drill_title_prefix = ""

        # defect click state (pareto -> defect hourly)
        self._defect_active = False
        self._defect_prev_png = None
        self._defect_prev_status = ""

        # plot margins used in plots.py (must match fig.subplots_adjust)
        self._plot_left_frac = 0.08
        self._plot_right_frac = 0.98

        # Analysis state (single file)
        self.input_path = tk.StringVar(value="")
        self.output_dir = tk.StringVar(value=os.path.join(os.getcwd(), "outputs"))
        self.status_text = tk.StringVar(value="Select a file to begin.")

        self.total_cards_text = tk.StringVar(value="Total PCBs flagged: -")
        self.card_breakdown_text = tk.StringVar(value="")

        self.defects_hint_text = tk.StringVar(value="Top defects and fixes will appear here after you run analysis.")
        self.top2_left_title = tk.StringVar(value="")
        self.top2_left_body = tk.StringVar(value="")
        self.top2_right_title = tk.StringVar(value="")
        self.top2_right_body = tk.StringVar(value="")

        self._last_defects_labels = []

        # --- 3-line analysis state ---
        self.multi_line1_path = tk.StringVar(value="")
        self.multi_line2_path = tk.StringVar(value="")
        self.multi_line4_path = tk.StringVar(value="")
        self.multi_view_line = tk.StringVar(value="combined")
        self._multi_results = {}
        self._multi_out_root = None
        self.multi_enabled = tk.BooleanVar(value=False)

        # Log Data state
        self.log_line = tk.StringVar(value="line4")
        self.log_date_start = tk.StringVar(value=_fmt_ddmmyyyy(date.today()))
        self.log_date_end = tk.StringVar(value=_fmt_ddmmyyyy(date.today() + timedelta(days=1)))
        self.log_file_path = tk.StringVar(value="")
        self.log_status = tk.StringVar(value="Select dates + line, then upload a CSV. Preview stats before saving.")
        self.log_stats = tk.StringVar(value="")
        self._pending_log = None
        self.log_pcbs_checked = tk.StringVar(value="")

        # Trends state
        self.trend_line = tk.StringVar(value="line4")        # line1/line2/line4/all
        self.trend_mode = tk.StringVar(value="month")        # month | range | all
        self.trend_metric = tk.StringVar(value="Counts")     # Counts | FPY %
        self.trend_year = tk.StringVar(value="")
        self.trend_month = tk.StringVar(value="")
        self.range_from = tk.StringVar(value=_fmt_ddmmyyyy(date.today() - timedelta(days=14)))
        self.range_to = tk.StringVar(value=_fmt_ddmmyyyy(date.today()))
        self.trend_stats = tk.StringVar(value="")
        self.trend_selected_stats = tk.StringVar(value="")
        self.trend_png_path = os.path.join(os.getcwd(), "outputs", "trend.png")
        self.trend_csv_path = os.path.join(os.getcwd(), "outputs", "trend_data.csv")
        self.trend_summary_csv_path = os.path.join(os.getcwd(), "outputs", "trend_summary.csv")

        # click-to-select day state
        self._trend_df_current = None
        self._trend_dates = []
        self._trend_selected_idx = None

        # Report tab state
        self.report_line1_path = tk.StringVar(value="")
        self.report_line2_path = tk.StringVar(value="")
        self.report_line4_path = tk.StringVar(value="")
        self.report_date = tk.StringVar(value=datetime.today().strftime("%d/%m/%Y"))
        self.report_status = tk.StringVar(value="Select files and click Generate & Save Report.")


        # build tabs
        self._build_analysis_ui()
        self._build_log_ui()
        self._build_trends_ui()
        self._build_formats_ui()
        self._build_report_ui()

        self._refresh_years_months()

    # ===================== Trends plot: CHECKED behind FLAGGED + stacked numeric labels =====================
    def _plot_trend_checked_flagged(self, rows, out_png: str, title: str, metric: str = "Counts"):
        """
        Counts mode:
          - Plot CHECKED bars BEHIND FLAGGED bars (different colors).
          - Only plot CHECKED bar for a day when checked is present AND checked != flagged.
          - Label: show checked on first line and flagged under it (no 'checked:'/'flagged:' text).
            If checked bar not shown, label is just flagged.

        FPY % mode:
          - Plot FPY% bars.
          - If checked present, label as:
              checked
              flagged
            else label FPY%.
        """
        if not rows:
            raise ValueError("No rows to plot.")

        dates = [str(r.get("log_date", "")) for r in rows]
        flagged = [int(r.get("pcbs_flagged", 0) or 0) for r in rows]

        checked = []
        for r in rows:
            c = r.get("pcbs_checked", None)
            if c is None:
                checked.append(None)
            else:
                try:
                    checked.append(int(c))
                except Exception:
                    checked.append(None)

        x = list(range(len(dates)))

        fig = plt.figure(figsize=(14, 6))
        ax = fig.add_subplot(111)

        # keep consistent margins with click mapping; top=0.92 gives bar-top labels extra room
        fig.subplots_adjust(left=self._plot_left_frac, right=self._plot_right_frac, bottom=0.22, top=0.92)

        if metric == "FPY %":
            y = []
            for i in range(len(rows)):
                c = checked[i]
                f = flagged[i]
                if c is None or c <= 0:
                    y.append(0.0)
                else:
                    y.append((max(0, c - f) / c) * 100.0)

            ax.bar(x, y)  # default color
            ax.set_xlim(-0.5, len(x) - 0.5)   # pin xlim so click mapping is exact
            ax.set_ylabel("FPY %")
            ax.set_ylim(0, 118)  # 18 % headroom so bar-top labels never clip
            ax.set_title(title)
            ax.set_xticks(x)

            for i, val in enumerate(y):
                c = checked[i]
                f = flagged[i]
                if c is not None and c > 0:
                    lab = f"{c}\n{f}"
                else:
                    lab = f"{val:.1f}%"
                ax.text(i, val, lab, ha="center", va="bottom", fontsize=8)

        else:
            # CHECKED behind FLAGGED, only where checked != flagged
            show_idx = []
            show_x = []
            show_h = []
            for i in range(len(x)):
                c = checked[i]
                if c is None:
                    continue
                if c == flagged[i]:
                    continue
                show_idx.append(i)
                show_x.append(x[i])
                show_h.append(c)

            # Behind bar (checked)
            if show_x:
                ax.bar(show_x, show_h, alpha=0.35, zorder=1, label="Checked")

            # Front bar (flagged)
            ax.bar(x, flagged, zorder=2, label="Flagged")
            ax.set_xlim(-0.5, len(x) - 0.5)   # pin xlim so click mapping is exact

            ax.set_ylabel("PCBs")
            ax.set_title(title)
            ax.set_xticks(x)

            # labels
            for i in range(len(x)):
                f = flagged[i]
                c = checked[i]
                if c is not None and c != f:
                    lab = f"{c}\n{f}"
                    y_text = max(c, f)
                else:
                    lab = f"{f}"
                    y_text = f
                ax.text(i, y_text, lab, ha="center", va="bottom", fontsize=8)

            # ylim headroom: 22 % above peak so two-line "checked\nflagged" labels never clip
            peak = max(max(flagged, default=0),
                       max((c for c in checked if c is not None), default=0), 1)
            ax.set_ylim(0, peak * 1.22)

        # Reduce tick crowding for long ranges
        if len(dates) > 31:
            step = max(1, len(dates) // 16)
            shown = [(d if (i % step == 0) else "") for i, d in enumerate(dates)]
            ax.set_xticklabels(shown, rotation=45, ha="right")
        else:
            ax.set_xticklabels(dates, rotation=45, ha="right")

        # Legend only makes sense for Counts (checked/flagged)
        if metric != "FPY %":
            ax.legend(loc="best", fontsize=9, framealpha=0.85)

        fig.savefig(out_png, dpi=160)
        plt.close(fig)
    # ===================== END Trends plot =====================

    # ---------------- UI-thread safe dialogs ----------------
    def _ask_yesno_sync(self, title: str, msg: str) -> bool:
        var = tk.BooleanVar(value=False)
        done = threading.Event()

        def _show():
            try:
                res = messagebox.askyesno(title, msg)
                var.set(bool(res))
            finally:
                done.set()

        self.after(0, _show)
        done.wait()
        return bool(var.get())

    # ---------------- 3-line helpers ----------------
    def _toggle_multi_panel(self):
        if self.multi_enabled.get():
            self.multi_panel_container.grid()
        else:
            self.multi_panel_container.grid_remove()

    def _detect_line_from_file_quick(self, path: str) -> str:
        name = os.path.basename(path).lower()
        if "line1" in name or "l1" in name:
            return "line1"
        if "line2" in name or "l2" in name:
            return "line2"
        if "line4" in name or "l4" in name:
            return "line4"

        try:
            df_raw = load_any_aoi(path)
            detected = (df_raw.attrs.get("line") or "").strip()
            if detected in ("line1", "line2", "line4"):
                return detected
        except Exception:
            pass
        return ""

    def browse_three_csvs(self):
        paths = filedialog.askopenfilenames(
            title="Select 3 AOI CSV files (Line1, Line2, Line4)",
            filetypes=[("CSV", "*.csv"), ("All Files", "*.*")]
        )
        if not paths:
            return

        paths = list(paths)
        mapped = {"line1": None, "line2": None, "line4": None}
        unknown = []

        for p in paths:
            line = self._detect_line_from_file_quick(p)
            if line in mapped and mapped[line] is None:
                mapped[line] = p
            else:
                unknown.append(p)

        for line in ("line1", "line2", "line4"):
            if mapped[line] is None and unknown:
                mapped[line] = unknown.pop(0)

        self.multi_line1_path.set(mapped["line1"] or "")
        self.multi_line2_path.set(mapped["line2"] or "")
        self.multi_line4_path.set(mapped["line4"] or "")

        self.multi_enabled.set(True)
        self._toggle_multi_panel()
        self.status_text.set("3-Line files selected. Click Run 3-Line Analysis.")

    # ---------------- Analysis UI ----------------
    def _build_analysis_ui(self):
        top = tk.Frame(self.analysis_tab, padx=12, pady=10)
        top.pack(fill="x")

        tk.Label(top, text="Input file (single line):").grid(row=0, column=0, sticky="w")
        tk.Entry(top, textvariable=self.input_path, width=95).grid(row=0, column=1, padx=8)
        tk.Button(top, text="Browse", command=self.browse_file, width=12).grid(row=0, column=2)

        tk.Label(top, text="Output folder:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        tk.Entry(top, textvariable=self.output_dir, width=95).grid(row=1, column=1, padx=8, pady=(8, 0))
        tk.Button(top, text="Choose", command=self.choose_output_dir, width=12).grid(row=1, column=2, pady=(8, 0))

        tk.Button(top, text="Run Analysis", command=self.run_analysis, width=16).grid(row=2, column=1, pady=10, sticky="e")
        tk.Button(top, text="Open Output Folder", command=self.open_output_folder, width=18).grid(row=2, column=2, pady=10)

        toggle_row = tk.Frame(top)
        toggle_row.grid(row=3, column=0, columnspan=3, sticky="we", pady=(4, 0))
        toggle_row.grid_columnconfigure(0, weight=1)

        tk.Checkbutton(
            toggle_row,
            text="Enable 3-Line Analysis (Line1 + Line2 + Line4)",
            variable=self.multi_enabled,
            command=self._toggle_multi_panel
        ).pack(side="left")

        tk.Button(
            toggle_row,
            text="Select 3 CSVs...",
            command=self.browse_three_csvs,
            width=16
        ).pack(side="right")

        self.multi_panel_container = tk.Frame(top)
        self.multi_panel_container.grid(row=4, column=0, columnspan=3, sticky="we", pady=(6, 0))
        self.multi_panel_container.grid_columnconfigure(0, weight=1)

        multi = tk.LabelFrame(self.multi_panel_container, text="3-Line Analysis", padx=10, pady=10)
        multi.pack(fill="x", expand=True)
        multi.grid_columnconfigure(1, weight=1)

        tk.Label(multi, text="Line1 CSV:").grid(row=0, column=0, sticky="w")
        tk.Entry(multi, textvariable=self.multi_line1_path, width=92).grid(row=0, column=1, padx=8, sticky="we")
        tk.Button(multi, text="Browse", width=12, command=lambda: self._browse_multi_file("line1")).grid(row=0, column=2)

        tk.Label(multi, text="Line2 CSV:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        tk.Entry(multi, textvariable=self.multi_line2_path, width=92).grid(row=1, column=1, padx=8, pady=(6, 0), sticky="we")
        tk.Button(multi, text="Browse", width=12, command=lambda: self._browse_multi_file("line2")).grid(row=1, column=2, pady=(6, 0))

        tk.Label(multi, text="Line4 CSV:").grid(row=2, column=0, sticky="w", pady=(6, 0))
        tk.Entry(multi, textvariable=self.multi_line4_path, width=92).grid(row=2, column=1, padx=8, pady=(6, 0), sticky="we")
        tk.Button(multi, text="Browse", width=12, command=lambda: self._browse_multi_file("line4")).grid(row=2, column=2, pady=(6, 0))

        actions = tk.Frame(multi)
        actions.grid(row=3, column=0, columnspan=3, sticky="we", pady=(10, 0))
        actions.grid_columnconfigure(0, weight=1)

        tk.Label(actions, text="View:").pack(side="left")
        self.multi_view_cb = ttk.Combobox(
            actions,
            textvariable=self.multi_view_line,
            values=["combined", "line1", "line2", "line4"],
            width=12,
            state="readonly"
        )
        self.multi_view_cb.pack(side="left", padx=(6, 10))
        self.multi_view_cb.bind("<<ComboboxSelected>>", lambda e: self._show_multi_line(self.multi_view_line.get().strip()))
        tk.Button(actions, text="Run 3-Line Analysis", command=self.run_multi_analysis, width=20).pack(side="right")

        self.multi_panel_container.grid_remove()

        status = tk.Frame(self.analysis_tab, padx=12, pady=0)
        status.pack(fill="x")
        tk.Label(status, textvariable=self.status_text, fg="#0b5394").pack(anchor="w")

        body = tk.Frame(self.analysis_tab, padx=12, pady=10)
        body.pack(fill="both", expand=True)

        left = tk.LabelFrame(body, text="Top Defects  — click a bar to see defect timing", padx=8, pady=8)
        left.pack(side="left", fill="both", expand=True, padx=(0, 6))

        right = tk.LabelFrame(body, text="PCBs Flagged  — click a bar to drill into minutes", padx=8, pady=8)
        right.pack(side="left", fill="both", expand=True, padx=(6, 0))

        top2_frame = tk.Frame(left)
        top2_frame.pack(fill="x", pady=(0, 6))

        tk.Label(
            top2_frame,
            textvariable=self.defects_hint_text,
            fg="#444",
            font=("Segoe UI", 10, "italic")
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 6))

        col_left = tk.Frame(top2_frame)
        col_right = tk.Frame(top2_frame)
        col_left.grid(row=1, column=0, sticky="nw", padx=(0, 16))
        col_right.grid(row=1, column=1, sticky="nw")

        tk.Label(col_left, textvariable=self.top2_left_title, font=("Segoe UI", 10, "bold")).pack(anchor="w")
        tk.Label(col_left, textvariable=self.top2_left_body, font=("Segoe UI", 10), justify="left", wraplength=520).pack(anchor="w")

        tk.Label(col_right, textvariable=self.top2_right_title, font=("Segoe UI", 10, "bold")).pack(anchor="w")
        tk.Label(col_right, textvariable=self.top2_right_body, font=("Segoe UI", 10), justify="left", wraplength=520).pack(anchor="w")

        self.defects_canvas = tk.Label(left, cursor="hand2")
        self.defects_canvas.pack(fill="both", expand=True)
        self.defects_canvas.bind("<Button-1>", self._on_defect_chart_click)

        summary = tk.Frame(right)
        summary.pack(fill="x", pady=(0, 4))
        tk.Label(summary, textvariable=self.total_cards_text, font=("Segoe UI", 10, "bold")).pack(anchor="w")

        self.card_ui_row = tk.Frame(right)
        self.card_ui_row.pack(fill="x", pady=(4, 2))
        tk.Label(self.card_ui_row, text="Cards (click to filter):", font=("Segoe UI", 9, "bold")).pack(side="left")
        self.btn_show_line = tk.Button(self.card_ui_row, text="Show Line Summary", command=self._show_line_summary, width=18)
        self.btn_show_line.pack(side="right")

        # Merged: shows "CardName: count" — count visible + clickable in one widget
        self.card_list = tk.Listbox(right, height=4, cursor="hand2")
        self.card_list.pack(fill="x", pady=(0, 4))
        self.card_list.bind("<<ListboxSelect>>", self._on_card_click)

        self.cards_canvas = tk.Label(right, cursor="hand2")
        self.cards_canvas.pack(fill="both", expand=True)
        self.cards_canvas.bind("<Button-1>", self._on_cards_chart_click)

        self.drill_bar = tk.Frame(right)
        self.drill_bar.pack(fill="x", pady=(8, 0))

        self.btn_prev_hour = tk.Button(self.drill_bar, text="⬅ Prev hour", command=self._drill_prev_hour, width=14, state="disabled")
        self.btn_back_hour = tk.Button(self.drill_bar, text="⬆ Back", command=self._drill_back_to_hour_or_defect, width=10, state="disabled")
        self.btn_next_hour = tk.Button(self.drill_bar, text="Next hour ➡", command=self._drill_next_hour, width=14, state="disabled")

        self.btn_prev_hour.pack(side="left", padx=4)
        self.btn_back_hour.pack(side="left", padx=4)
        self.btn_next_hour.pack(side="left", padx=4)

    # ---------------- Analysis methods ----------------
    def browse_file(self):
        path = filedialog.askopenfilename(
            title="Select AOI CSV file",
            filetypes=[("CSV", "*.csv"), ("All Files", "*.*")]
        )
        if path:
            self.input_path.set(path)
            self.status_text.set("Ready. Click Run Analysis.")

    def _browse_multi_file(self, line: str):
        path = filedialog.askopenfilename(
            title=f"Select {line.upper()} AOI CSV file",
            filetypes=[("CSV", "*.csv"), ("All Files", "*.*")]
        )
        if not path:
            return
        if line == "line1":
            self.multi_line1_path.set(path)
        elif line == "line2":
            self.multi_line2_path.set(path)
        else:
            self.multi_line4_path.set(path)

        self.multi_enabled.set(True)
        self._toggle_multi_panel()
        self.status_text.set("3-Line files selected. Click Run 3-Line Analysis.")

    def choose_output_dir(self):
        path = filedialog.askdirectory(title="Choose output folder")
        if path:
            self.output_dir.set(path)

    def open_output_folder(self):
        out_dir = self.output_dir.get().strip()
        if not out_dir:
            messagebox.showwarning("Output folder", "Please choose an output folder first.")
            return
        if not os.path.exists(out_dir):
            messagebox.showwarning("Output folder", "Output folder does not exist yet. Run analysis first.")
            return

        try:
            if sys.platform.startswith("win"):
                os.startfile(out_dir)  # type: ignore[attr-defined]
            elif sys.platform.startswith("darwin"):
                subprocess.run(["open", out_dir], check=False)
            else:
                subprocess.run(["xdg-open", out_dir], check=False)
        except Exception as e:
            messagebox.showerror("Open folder failed", str(e))

    def run_analysis(self):
        in_path = self.input_path.get().strip()
        out_dir = self.output_dir.get().strip()

        if not in_path:
            messagebox.showwarning("Input file", "Please select a CSV file.")
            return
        if not os.path.exists(in_path):
            messagebox.showerror("Input file", f"File not found:\n{in_path}")
            return
        if not out_dir:
            messagebox.showwarning("Output folder", "Please choose an output folder.")
            return

        self.status_text.set("Running analysis...")
        self._clear_analysis_images()
        self._reset_drill_state()
        self._reset_defect_state()

        self._set_card_ui_visible(True)

        t = threading.Thread(target=self._run_analysis_safe, args=(in_path, out_dir), daemon=True)
        t.start()

    def run_multi_analysis(self):
        if not self.multi_enabled.get():
            messagebox.showwarning("3-Line Analysis", "Enable 3-Line Analysis first.")
            return

        p1 = self.multi_line1_path.get().strip()
        p2 = self.multi_line2_path.get().strip()
        p4 = self.multi_line4_path.get().strip()
        out_dir = self.output_dir.get().strip()

        if not p1 or not p2 or not p4:
            messagebox.showwarning("3-Line Analysis", "Please select ALL three files (Line1, Line2, Line4).")
            return
        for p in (p1, p2, p4):
            if not os.path.exists(p):
                messagebox.showerror("3-Line Analysis", f"File not found:\n{p}")
                return
        if not out_dir:
            messagebox.showwarning("Output folder", "Please choose an output folder.")
            return

        self.status_text.set("Running 3-Line analysis...")
        self._clear_analysis_images()
        self._reset_drill_state()
        self._reset_defect_state()
        self._multi_results = {}

        t = threading.Thread(target=self._run_multi_analysis_safe, args=(p1, p2, p4, out_dir), daemon=True)
        t.start()

    def _format_fixes(self, defect_name: str):
        key = _normalize_defect_name(defect_name)
        fixes = DEFECT_FIXES_SIMPLE.get(key) or DEFECT_FIXES_SIMPLE.get(defect_name) or [
            "Check the AOI image and placement.",
            "Confirm the correct reel/part.",
            "Check program/settings and rerun.",
        ]
        return "\n".join([f"• {x}" for x in fixes])

    # ------------------------------------------------------------------
    # Shared display helpers (eliminate repeated 10-15 line blocks)
    # ------------------------------------------------------------------

    def _get_top2_info(self, defects_df) -> tuple:
        """Return (left_title, left_body, right_title, right_body) for top-2 defects."""
        lt = lb = rt = rb = ""
        if defects_df is None or defects_df.empty:
            return lt, lb, rt, rb
        top2 = defects_df.head(2)
        if len(top2) >= 1:
            d1 = str(top2.iloc[0]["Defect"])
            lt = f"1) {d1}  (Count: {int(top2.iloc[0]['Count'])})"
            lb = self._format_fixes(d1)
        if len(top2) >= 2:
            d2 = str(top2.iloc[1]["Defect"])
            rt = f"2) {d2}  (Count: {int(top2.iloc[1]['Count'])})"
            rb = self._format_fixes(d2)
        return lt, lb, rt, rb

    def _format_card_breakdown(self, by_card) -> tuple:
        """Return ("", display_items) where display_items are "CardName: count" strings.
        The first element is kept for API compat but the breakdown label has been removed;
        the listbox now shows counts inline so both info and click live in one widget."""
        if by_card is None or by_card.empty:
            return "", []
        display = [f"{r['Card']}: {int(r['Count'])}" for _, r in by_card.iterrows()]
        return "", display

    def _show_chart_pair(self, defects_png: str, cards_png: str):
        """Load both chart canvases at once."""
        self._load_image(self.defects_canvas, defects_png, which="defects")
        self._load_image(self.cards_canvas, cards_png, which="cards")

    def _reset_drill_state(self):
        self._drill_active = False
        self._drill_hour_index = None
        self._drill_times = []
        self._drill_df = None
        self._drill_hour_png = None
        self._drill_out_dir = None
        self._drill_title_prefix = ""
        self.btn_prev_hour.configure(state="disabled")
        self.btn_back_hour.configure(state="disabled")
        self.btn_next_hour.configure(state="disabled")

    def _reset_defect_state(self):
        self._defect_active = False
        self._defect_prev_png = None
        self._defect_prev_status = ""

    def _set_card_ui_visible(self, visible: bool):
        try:
            if visible:
                if not self.card_ui_row.winfo_ismapped():
                    self.card_ui_row.pack(fill="x", pady=(8, 6))
                if not self.card_list.winfo_ismapped():
                    self.card_list.pack(fill="x", pady=(0, 8))
            else:
                if self.card_list.winfo_ismapped():
                    self.card_list.pack_forget()
                if self.card_ui_row.winfo_ismapped():
                    self.card_ui_row.pack_forget()
        except Exception:
            pass

    def _run_analysis_safe(self, in_path: str, out_dir: str):
        try:
            ensure_outputs_dir(out_dir)

            df_raw = load_any_aoi(in_path)
            df = clean_aoi_data(df_raw)

            self._df_current = df.copy()
            self._current_out_dir = out_dir

            defects = top_defects(df, top_n=20)
            defects_png = os.path.join(out_dir, "defect_pareto.png")
            defects.to_csv(os.path.join(out_dir, "defect_pareto.csv"), index=False)
            plot_top_defects_bars(defects, defects_png, title="Top defect types flagged by AOI (event rows)")

            left_title, left_body, right_title, right_body = self._get_top2_info(defects)

            ts, grain, total_flagged = cards_scanned_over_time(
                df,
                hour_to_day_threshold_days=3,
                force_7to7_when_hourly=True
            )

            cards_png = os.path.join(out_dir, "pcbs_flagged_by_hour.png")
            title = "PCBs flagged per Hour (07:00 → 07:00)" if grain == "hour" else "PCBs flagged per Day"

            plot_time_series_counts_bar(
                ts,
                cards_png,
                title=title,
                y_label="Flagged count (line logic)",
                grain=grain,
            )

            if grain == "hour":
                self._drill_times = ts["TimeTS"].tolist()
                self._drill_df = df.copy()
                self._drill_hour_png = cards_png
                self._drill_out_dir = out_dir
                self._drill_title_prefix = ""

            by_card = pcbs_flagged_by_card(df)
            by_card.to_csv(os.path.join(out_dir, "pcbs_flagged_by_card.csv"), index=False)

            breakdown_text, card_names = self._format_card_breakdown(by_card)
            summary = f"Total PCBs flagged in period: {total_flagged:,}  ({'Hourly' if grain=='hour' else 'Daily'} view)"

            self._line_defects_png = defects_png
            self._line_cards_png = cards_png
            self._line_summary = summary
            self._line_breakdown = breakdown_text
            self._line_top2 = (left_title, left_body, right_title, right_body)

            self.after(
                0,
                lambda: self._on_analysis_success(
                    defects_png, cards_png, out_dir, summary, breakdown_text,
                    left_title, left_body, right_title, right_body, card_names,
                    defects
                ),
            )

        except Exception as e:
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            self.after(0, lambda: self._on_error(err))

    def _run_multi_analysis_safe(self, p1: str, p2: str, p4: str, out_dir: str):
        try:
            ensure_outputs_dir(out_dir)

            multi_root = os.path.join(out_dir, "multi_line")
            os.makedirs(multi_root, exist_ok=True)
            self._multi_out_root = multi_root

            dfs = {}
            defects_by_line = {}
            hourly_ts_by_line = {}
            total_flagged_by_line = {}

            for line, path in (("line1", p1), ("line2", p2), ("line4", p4)):
                df_raw = load_any_aoi(path)
                df = clean_aoi_data(df_raw)
                if "StartDateTime" not in df.columns:
                    raise ValueError(f"{line}: StartDateTime missing after cleaning.")
                if df["StartDateTime"].notna().sum() == 0:
                    raise ValueError(f"{line}: No valid StartDateTime rows found.")
                dfs[line] = df

            results = {}

            for line, df in dfs.items():
                line_dir = os.path.join(multi_root, line)
                os.makedirs(line_dir, exist_ok=True)

                defects = top_defects(df, top_n=20)
                defects_by_line[line] = defects.copy()
                defects_png = os.path.join(line_dir, "defect_pareto.png")
                defects.to_csv(os.path.join(line_dir, "defect_pareto.csv"), index=False)
                plot_top_defects_bars(defects, defects_png, title=f"{line.upper()} - Top defects (event rows)")

                ts, grain, total_flagged = cards_scanned_over_time(
                    df,
                    hour_to_day_threshold_days=3,
                    force_7to7_when_hourly=True
                )
                total_flagged_by_line[line] = int(total_flagged)

                cards_png = os.path.join(line_dir, "pcbs_flagged_by_hour.png")
                title = f"{line.upper()} - PCBs flagged per Hour (07:00 → 07:00)" if grain == "hour" else f"{line.upper()} - PCBs flagged per Day"
                plot_time_series_counts_bar(ts, cards_png, title=title, y_label="Flagged count (line logic)", grain=grain)

                ts_h, _, _ = cards_scanned_over_time(
                    df,
                    hour_to_day_threshold_days=10**9,
                    force_7to7_when_hourly=True
                )
                hourly_ts_by_line[line] = ts_h.copy()

                by_card = pcbs_flagged_by_card(df)
                by_card.to_csv(os.path.join(line_dir, "pcbs_flagged_by_card.csv"), index=False)

                breakdown_text, card_names = self._format_card_breakdown(by_card)
                lt, lb, rt, rb = self._get_top2_info(defects)

                summary = f"{line.upper()} | Total PCBs flagged: {int(total_flagged):,}  ({'Hourly' if grain=='hour' else 'Daily'} view)"

                drill_payload = None
                if grain == "hour" and ts is not None and not ts.empty and "TimeTS" in ts.columns:
                    drill_payload = {
                        "times": ts["TimeTS"].tolist(),
                        "df": df.copy(),
                        "hour_png": cards_png,
                        "out_dir": line_dir,
                        "title_prefix": f"{line.upper()} - ",
                        "mode": "single",
                    }

                results[line] = {
                    "df": df.copy(),
                    "out_dir": line_dir,
                    "defects_png": defects_png,
                    "cards_png": cards_png,
                    "summary": summary,
                    "breakdown": breakdown_text,
                    "top2": (lt, lb, rt, rb),
                    "cards": card_names,
                    "defects_df": defects,
                    "drill": drill_payload,
                }

            combined_dir = os.path.join(multi_root, "combined")
            os.makedirs(combined_dir, exist_ok=True)

            combined_defects = combine_defects_dfs(
                [defects_by_line["line1"], defects_by_line["line2"], defects_by_line["line4"]],
                top_n=20
            )
            combined_defects_png = os.path.join(combined_dir, "defect_pareto.png")
            combined_defects.to_csv(os.path.join(combined_dir, "defect_pareto.csv"), index=False)
            plot_top_defects_bars(combined_defects, combined_defects_png, title="ALL LINES - Top defects (event rows)")

            combined_ts = sum_time_series_dfs_on_time(
                [hourly_ts_by_line["line1"], hourly_ts_by_line["line2"], hourly_ts_by_line["line4"]]
            )
            combined_cards_png = os.path.join(combined_dir, "pcbs_flagged_by_hour.png")
            plot_time_series_counts_bar(
                combined_ts,
                combined_cards_png,
                title="ALL LINES - PCBs flagged per Hour (07:00 → 07:00)",
                y_label="Flagged count (summed line logic)",
                grain="hour",
            )

            lt, lb, rt, rb = self._get_top2_info(combined_defects)

            combined_total = sum(int(total_flagged_by_line.get(k, 0)) for k in ("line1", "line2", "line4"))
            combined_summary = (
                f"ALL LINES | Total PCBs flagged: {combined_total:,}  (Hourly view)\n"
                f"  • LINE1: {total_flagged_by_line.get('line1', 0):,}\n"
                f"  • LINE2: {total_flagged_by_line.get('line2', 0):,}\n"
                f"  • LINE4: {total_flagged_by_line.get('line4', 0):,}"
            )

            combined_drill = None
            if combined_ts is not None and not combined_ts.empty and "TimeTS" in combined_ts.columns:
                combined_drill = {
                    "times": combined_ts["TimeTS"].tolist(),
                    "df": {k: v.copy() for k, v in dfs.items()},
                    "hour_png": combined_cards_png,
                    "out_dir": combined_dir,
                    "title_prefix": "ALL LINES - ",
                    "mode": "combined",
                }

            results["combined"] = {
                "df": None,
                "out_dir": combined_dir,
                "defects_png": combined_defects_png,
                "cards_png": combined_cards_png,
                "summary": combined_summary,
                "breakdown": "",
                "top2": (lt, lb, rt, rb),
                "cards": [],
                "defects_df": combined_defects,
                "drill": combined_drill,
                "dfs_by_line": {k: v.copy() for k, v in dfs.items()},
            }

            self._multi_results = results
            self.after(0, lambda: self.status_text.set(f"3-Line analysis done. Outputs saved to: {self._multi_out_root}"))

            preferred = (self.multi_view_line.get().strip() or "combined")
            if preferred not in self._multi_results:
                preferred = "combined"
            self.after(0, lambda: self._show_multi_line(preferred))

        except Exception as e:
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            self.after(0, lambda: self._on_error(err))
            self.after(0, lambda: self.status_text.set("3-Line analysis failed."))

    def _show_multi_line(self, line: str):
        line = (line or "").strip()
        if line not in self._multi_results:
            return

        payload = self._multi_results[line]

        if line == "combined":
            self._set_card_ui_visible(False)
        else:
            self._set_card_ui_visible(True)

        if line == "combined":
            self._df_current = None
            self._current_out_dir = payload["out_dir"]
        else:
            self._df_current = payload["df"].copy()
            self._current_out_dir = payload["out_dir"]

        self._reset_drill_state()
        self._reset_defect_state()

        ltitle, lbody, rtitle, rbody = payload["top2"]
        self.defects_hint_text.set("")
        self.top2_left_title.set(ltitle)
        self.top2_left_body.set(lbody)
        self.top2_right_title.set(rtitle)
        self.top2_right_body.set(rbody)

        defects_df = payload.get("defects_df")
        self._last_defects_labels = []
        if defects_df is not None and not defects_df.empty and "Defect" in defects_df.columns:
            self._last_defects_labels = defects_df["Defect"].astype(str).tolist()

        self.card_list.delete(0, tk.END)
        for c in payload.get("cards", []):
            self.card_list.insert(tk.END, c)

        self.total_cards_text.set(payload.get("summary", "Total PCBs flagged: -"))
        self.card_breakdown_text.set(payload.get("breakdown", ""))

        self._show_chart_pair(payload["defects_png"], payload["cards_png"])

        drill = payload.get("drill")
        if drill:
            self._drill_times = drill["times"]
            self._drill_df = drill["df"]
            self._drill_hour_png = drill["hour_png"]
            self._drill_out_dir = drill["out_dir"]
            self._drill_title_prefix = drill["title_prefix"]

        self.status_text.set(f"Showing 3-Line results: {line.upper()}")

    def _on_analysis_success(self, defects_png, cards_png, out_dir, summary, breakdown,
                            ltitle, lbody, rtitle, rbody, card_names, defects_df):
        self._set_card_ui_visible(True)

        self.status_text.set(f"Done. Outputs saved to: {out_dir}  |  Click a chart bar to drill down.")

        self.total_cards_text.set(summary)
        self.card_breakdown_text.set(breakdown)

        self.defects_hint_text.set("")
        self.top2_left_title.set(ltitle)
        self.top2_left_body.set(lbody)
        self.top2_right_title.set(rtitle)
        self.top2_right_body.set(rbody)

        self._last_defects_labels = []
        if defects_df is not None and not defects_df.empty and "Defect" in defects_df.columns:
            self._last_defects_labels = defects_df["Defect"].astype(str).tolist()

        self.card_list.delete(0, tk.END)
        for c in card_names:
            self.card_list.insert(tk.END, c)

        self._show_chart_pair(defects_png, cards_png)

    def _clear_analysis_images(self):
        self.defects_canvas.configure(image="", text="")
        self.cards_canvas.configure(image="", text="")
        self._img_defects = None
        self._img_cards = None

        self.total_cards_text.set("Total PCBs flagged: -")
        self.card_breakdown_text.set("")

        self.defects_hint_text.set("Top defects and fixes will appear here after you run analysis.")
        self.top2_left_title.set("")
        self.top2_left_body.set("")
        self.top2_right_title.set("")
        self.top2_right_body.set("")

        self.card_list.delete(0, tk.END)
        self._last_defects_labels = []

        self._set_card_ui_visible(True)

    def _on_card_click(self, event=None):
        if self._df_current is None:
            return
        sel = self.card_list.curselection()
        if not sel:
            return
        # Listbox items are "CardName: count" — extract name from the left side
        item = self.card_list.get(sel[0])
        card = item.rsplit(": ", 1)[0] if ": " in item else item
        if not card or not self._current_out_dir:
            return

        self.status_text.set(f"Generating card view: {card} ...")
        self._reset_drill_state()
        self._reset_defect_state()
        t = threading.Thread(target=self._build_card_view_safe, args=(card,), daemon=True)
        t.start()

    def _build_card_view_safe(self, card: str):
        try:
            df = self._df_current
            out_dir = self._current_out_dir

            d = df.copy()
            js = _jobfile_series(d)
            if js is None:
                raise ValueError("JobFileIDShare/JobFile not found; cannot build card view.")
            d["_CardName"] = js.apply(_extract_card_name)

            d_card = d[d["_CardName"] == card].copy()
            if d_card.empty:
                raise ValueError(f"No rows found for card: {card}")

            card_folder = os.path.join(out_dir, "cardwise", _safe_folder_name(card))
            os.makedirs(card_folder, exist_ok=True)

            defects = top_defects(d_card, top_n=20)
            defects_png = os.path.join(card_folder, "defect_pareto.png")
            defects.to_csv(os.path.join(card_folder, "defect_pareto.csv"), index=False)
            plot_top_defects_bars(defects, defects_png, title=f"{card} - Top defects (event rows)")

            ts, grain, total_flagged = cards_scanned_over_time(
                d_card,
                hour_to_day_threshold_days=3,
                force_7to7_when_hourly=True
            )

            cards_png = os.path.join(card_folder, "pcbs_flagged_by_hour.png")
            title = f"{card} - PCBs flagged per Hour (07:00 → 07:00)" if grain == "hour" else f"{card} - PCBs flagged per Day"
            plot_time_series_counts_bar(ts, cards_png, title=title, y_label="Flagged count (line logic)", grain=grain)

            if grain == "hour":
                self._drill_times = ts["TimeTS"].tolist()
                self._drill_df = d_card.copy()
                self._drill_hour_png = cards_png
                self._drill_out_dir = card_folder
                self._drill_title_prefix = f"{card} - "

            by_card = pcbs_flagged_by_card(d_card)
            by_card.to_csv(os.path.join(card_folder, "pcbs_flagged_by_card.csv"), index=False)

            lt, lb, rt, rb = self._get_top2_info(defects)
            breakdown = f"• {card}: {total_flagged:,}"
            summary = f"Card view: {card} | Total PCBs flagged: {total_flagged:,}  ({'Hourly' if grain=='hour' else 'Daily'} view)"

            self.after(
                0,
                lambda: self._show_card_outputs(
                    card, defects_png, cards_png, summary, breakdown,
                    lt, lb, rt, rb, defects
                ),
            )
        except Exception as e:
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            self.after(0, lambda: self._on_error(err))
            self.after(0, lambda: self.status_text.set("Card view failed."))

    def _show_card_outputs(self, card, defects_png, cards_png, summary, breakdown,
                           ltitle, lbody, rtitle, rbody, defects_df):
        self._set_card_ui_visible(True)

        self.status_text.set(f"Showing card view: {card}")
        self.total_cards_text.set(summary)
        self.card_breakdown_text.set(breakdown)

        self.defects_hint_text.set("")
        self.top2_left_title.set(ltitle)
        self.top2_left_body.set(lbody)
        self.top2_right_title.set(rtitle)
        self.top2_right_body.set(rbody)

        self._last_defects_labels = []
        if defects_df is not None and not defects_df.empty and "Defect" in defects_df.columns:
            self._last_defects_labels = defects_df["Defect"].astype(str).tolist()

        self._show_chart_pair(defects_png, cards_png)

    def _show_line_summary(self):
        if not self._line_defects_png or not self._line_cards_png:
            return
        self.status_text.set("Showing line summary.")
        self.total_cards_text.set(self._line_summary or "Total PCBs flagged: -")
        self.card_breakdown_text.set(self._line_breakdown or "")

        self._set_card_ui_visible(True)

        ltitle, lbody, rtitle, rbody = self._line_top2
        self.defects_hint_text.set("")
        self.top2_left_title.set(ltitle)
        self.top2_left_body.set(lbody)
        self.top2_right_title.set(rtitle)
        self.top2_right_body.set(rbody)
        self._show_chart_pair(self._line_defects_png, self._line_cards_png)
        self._reset_defect_state()

    def _x_to_index(self, x_px: int, widget_w: int, n: int, which: str = "") -> int:
        """Map a click x-pixel to a bar index.

        Corrects for the image being centered inside the tk.Label widget
        (centering offset = (widget_w - img_w) // 2), then maps within the
        plot area using the stored left/right margin fractions.
        """
        if n <= 0 or widget_w <= 1:
            return 0

        img_size = self._img_display_size.get(which)
        if img_size:
            img_w = img_size[0]
            h_offset = max(0, (widget_w - img_w) // 2)
            x_in_img = x_px - h_offset
            if x_in_img < 0 or x_in_img > img_w:
                return 0
        else:
            img_w = widget_w
            x_in_img = x_px

        x0 = int(self._plot_left_frac * img_w)
        x1 = int(self._plot_right_frac * img_w)
        if x1 <= x0:
            x0, x1 = 0, img_w
        x = max(x0, min(x_in_img, x1 - 1))
        frac = (x - x0) / max(1, x1 - x0)
        return max(0, min(int(frac * n), n - 1))

    def _on_cards_chart_click(self, event):
        if self._defect_active:
            return
        if not self._drill_times or self._drill_df is None or self._drill_out_dir is None:
            return
        if self._drill_active:
            return

        w = max(self.cards_canvas.winfo_width(), 1)
        n = len(self._drill_times)
        idx = self._x_to_index(event.x, w, n, which="cards")
        self._drill_show_hour_index(idx)

    def _drill_show_hour_index(self, idx: int):
        try:
            if not self._drill_times or idx < 0 or idx >= len(self._drill_times):
                return

            hour_ts = self._drill_times[idx]

            if isinstance(self._drill_df, dict):
                ts_min = pcbs_flagged_by_minute_multi(self._drill_df, hour_ts)
            else:
                ts_min = pcbs_flagged_by_minute(self._drill_df, hour_ts)

            drill_dir = os.path.join(self._drill_out_dir, "minute_drilldown")
            os.makedirs(drill_dir, exist_ok=True)

            safe_name = hour_ts.strftime("%Y%m%d_%H00")
            out_png = os.path.join(drill_dir, f"pcbs_per_minute_{safe_name}.png")

            title = f"{self._drill_title_prefix}PCBs flagged per Minute ({hour_ts.strftime('%d-%b %H:00')})"
            plot_pcbs_flagged_by_minute(ts_min, out_png, title=title, y_label="Flagged count (line logic)")

            self._drill_active = True
            self._drill_hour_index = idx

            self._load_image(self.cards_canvas, out_png, which="cards")

            self.btn_back_hour.configure(state="normal")
            self.btn_prev_hour.configure(state="normal" if idx > 0 else "disabled")
            self.btn_next_hour.configure(state="normal" if idx < len(self._drill_times) - 1 else "disabled")

            self.status_text.set("Minute view. Use Back / Prev / Next.")

        except Exception as e:
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            self._on_error(err)

    def _drill_back_to_hour_or_defect(self):
        if self._defect_active:
            self._defect_active = False
            if self._defect_prev_png:
                self._load_image(self.cards_canvas, self._defect_prev_png, which="cards")
            self.status_text.set(self._defect_prev_status or "Back.")
            self.btn_back_hour.configure(state="disabled")
            self.btn_prev_hour.configure(state="disabled")
            self.btn_next_hour.configure(state="disabled")
            return

        if not self._drill_active or not self._drill_hour_png:
            return
        self._drill_active = False
        self._drill_hour_index = None
        self._load_image(self.cards_canvas, self._drill_hour_png, which="cards")

        self.btn_back_hour.configure(state="disabled")
        self.btn_prev_hour.configure(state="disabled")
        self.btn_next_hour.configure(state="disabled")
        self.status_text.set("Back to hourly view. Click an hour bar to drill down.")

    def _drill_prev_hour(self):
        if not self._drill_active or self._drill_hour_index is None:
            return
        self._drill_show_hour_index(self._drill_hour_index - 1)

    def _drill_next_hour(self):
        if not self._drill_active or self._drill_hour_index is None:
            return
        self._drill_show_hour_index(self._drill_hour_index + 1)

    def _on_defect_chart_click(self, event):
        if not getattr(self, "_last_defects_labels", None):
            return
        # No _drill_active guard here: defect timing can be viewed at any time,
        # even while the right panel is showing a minute drill-down.

        w = max(self.defects_canvas.winfo_width(), 1)
        n = len(self._last_defects_labels)
        idx = self._x_to_index(event.x, w, n, which="defects")
        defect = self._last_defects_labels[idx]

        # Capture before spawning thread (state could change)
        prev_png = self._drill_hour_png or self._line_cards_png
        prev_status = self.status_text.get()
        self.status_text.set(f"Loading defect timing: {defect}…")

        t = threading.Thread(target=self._defect_timing_safe,
                             args=(defect, prev_png, prev_status), daemon=True)
        t.start()

    def _defect_timing_safe(self, defect: str, prev_png: str, prev_status: str):
        try:
            out_dir = self._current_out_dir or os.path.join(os.getcwd(), "outputs")
            drill_dir = os.path.join(out_dir, "defect_timing")
            os.makedirs(drill_dir, exist_ok=True)

            if self._df_current is None and self._multi_results.get("combined"):
                dfs_by_line = self._multi_results["combined"].get("dfs_by_line") or {}
                ts_def = defect_occurs_over_time_7to7_multi(dfs_by_line, defect)
            else:
                ts_def = defect_occurs_over_time_7to7(self._df_current, defect)

            safe_def = re.sub(r"[^A-Za-z0-9._ -]+", "_", defect).strip()[:80] or "DEFECT"
            out_png = os.path.join(drill_dir, f"defect_{safe_def}_hourly_7to7.png")

            plot_time_series_counts_bar(
                ts_def,
                out_png,
                title=f"Defect timing (07:00 → 07:00): {defect}",
                y_label="Occurrences (event rows)",
                grain="hour",
            )

            def _show():
                self._defect_prev_png = prev_png
                self._defect_prev_status = prev_status
                self._defect_active = True
                self._drill_active = False   # exit any minute drill-down
                self._drill_hour_index = None
                self._load_image(self.cards_canvas, out_png, which="cards")
                self.btn_back_hour.configure(state="normal")
                self.btn_prev_hour.configure(state="disabled")
                self.btn_next_hour.configure(state="disabled")
                self.status_text.set(f"Defect timing: {defect}  |  Press Back to return.")

            self.after(0, _show)

        except Exception as e:
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            self.after(0, lambda: self._on_error(err))

    # ---------------- Log Data UI ----------------
    def _build_log_ui(self):
        wrap = tk.Frame(self.log_tab, padx=12, pady=12)
        wrap.pack(fill="both", expand=True)

        cfg = tk.LabelFrame(wrap, text="Log daily data (7am → 7am)", padx=10, pady=10)
        cfg.pack(fill="x")

        tk.Label(cfg, text="Line to log:").grid(row=0, column=0, sticky="w")
        ttk.Combobox(cfg, textvariable=self.log_line, values=["line1", "line2", "line4"], width=10, state="readonly").grid(
            row=0, column=1, padx=(6, 14), sticky="w"
        )

        tk.Label(cfg, text="Start date (dd/mm/yyyy):").grid(row=0, column=2, sticky="w")
        tk.Entry(cfg, textvariable=self.log_date_start, width=14).grid(row=0, column=3, padx=(6, 6), sticky="w")
        tk.Button(cfg, text="📅", command=lambda: self._pick_date(self.log_date_start)).grid(row=0, column=4, padx=(0, 14))

        tk.Label(cfg, text="End date (dd/mm/yyyy):").grid(row=0, column=5, sticky="w")
        tk.Entry(cfg, textvariable=self.log_date_end, width=14).grid(row=0, column=6, padx=(6, 6), sticky="w")
        tk.Button(cfg, text="📅", command=lambda: self._pick_date(self.log_date_end)).grid(row=0, column=7)

        tk.Label(cfg, text="Total PCBs checked (manual):").grid(row=1, column=0, sticky="w", pady=(10, 0))
        tk.Entry(cfg, textvariable=self.log_pcbs_checked, width=16).grid(row=1, column=1, sticky="w", pady=(10, 0))

        hint = tk.Frame(wrap)
        hint.pack(fill="x", pady=(10, 6))
        tk.Label(
            hint,
            text="⚠ Important:\n"
                 "• Select the correct LINE before uploading.\n"
                 "• Keep the file between 07:00 of start date and 07:00 of end date.\n"
                 "• If the file extends beyond these boundaries, the app can trim it (you will be asked).",
            fg="#333",
            justify="left"
        ).pack(anchor="w")

        actions = tk.Frame(wrap)
        actions.pack(fill="x", pady=(6, 6))

        tk.Button(actions, text="Upload CSV for preview", command=self.log_upload_preview, width=22).pack(side="left")
        tk.Button(actions, text="Confirm & Save Log", command=self.log_confirm_save, width=18).pack(side="left", padx=10)
        tk.Button(actions, text="Undo / Reset", command=self.log_undo, width=14).pack(side="left")

        st = tk.Frame(wrap)
        st.pack(fill="x", pady=(6, 6))
        tk.Label(st, textvariable=self.log_status, fg="#0b5394").pack(anchor="w")
        tk.Label(st, textvariable=self.log_stats, fg="#222", justify="left").pack(anchor="w")

        dbline = tk.Frame(wrap)
        dbline.pack(fill="x", pady=(4, 10))
        tk.Label(dbline, text=f"DB location: {get_db_path()}", fg="#666", font=("Segoe UI", 8)).pack(anchor="w")

    def _pick_date(self, target_var: tk.StringVar):
        try:
            current = _parse_ddmmyyyy(target_var.get())
        except Exception:
            current = date.today()

        def set_it(d: date):
            target_var.set(_fmt_ddmmyyyy(d))

        DatePicker(self, initial=current, on_done=set_it)

    def log_upload_preview(self):
        path = filedialog.askopenfilename(
            title="Select AOI CSV file to log",
            filetypes=[("CSV", "*.csv"), ("All Files", "*.*")]
        )
        if not path:
            return

        self.log_file_path.set(path)
        self.log_status.set("Reading file and preparing preview...")
        self.log_stats.set("")
        self._pending_log = None

        t = threading.Thread(target=self._log_preview_safe, args=(path,), daemon=True)
        t.start()

    def _log_preview_safe(self, path: str):
        try:
            d_start = _parse_ddmmyyyy(self.log_date_start.get())
            d_end = _parse_ddmmyyyy(self.log_date_end.get())

            window_start = datetime(d_start.year, d_start.month, d_start.day, 7, 0, 0)
            window_end = datetime(d_end.year, d_end.month, d_end.day, 7, 0, 0)

            if window_end <= window_start:
                raise ValueError("End date must be after start date (7am → 7am window).")

            selected_line = self.log_line.get().strip()

            df_raw = load_any_aoi(path)
            df = clean_aoi_data(df_raw)

            detected_line = (df.attrs.get("line") or "").strip()
            if detected_line and detected_line != selected_line:
                ok = self._ask_yesno_sync(
                    "Line mismatch",
                    f"You selected: {selected_line}\n"
                    f"File looks like: {detected_line}\n\n"
                    "Continue anyway?"
                )
                if not ok:
                    self.after(0, lambda: self.log_status.set("Preview cancelled (line mismatch)."))
                    return

            if "StartDateTime" not in df.columns:
                raise ValueError("StartDateTime missing after cleaning.")

            d = df[df["StartDateTime"].notna()].copy()
            if d.empty:
                raise ValueError("No valid StartDateTime rows in file.")

            file_min = d["StartDateTime"].min()
            file_max = d["StartDateTime"].max()

            if file_max < window_start or file_min > window_end:
                raise ValueError(
                    "Selected dates do not match file timestamps.\n"
                    f"File range: {file_min} → {file_max}\n"
                    f"Window: {window_start} → {window_end}"
                )

            trimmed = False
            if file_min < window_start or file_max > window_end:
                ok = self._ask_yesno_sync(
                    "Trim to window?",
                    "File extends beyond selected 7am → 7am window.\n\nTrim data to this window?"
                )
                if not ok:
                    self.after(0, lambda: self.log_status.set("Preview cancelled by user."))
                    return

                d = d[(d["StartDateTime"] >= window_start) & (d["StartDateTime"] < window_end)].copy()
                trimmed = True

            if d.empty:
                raise ValueError("No data left after trimming.")

            _, _, total_flagged = cards_scanned_over_time(
                d,
                hour_to_day_threshold_days=3,
                force_7to7_when_hourly=True
            )

            checked_raw = self.log_pcbs_checked.get().strip()
            pcbs_checked = None
            if checked_raw:
                pcbs_checked = int(checked_raw)

            self._pending_log = {
                "log_date": d_start.strftime("%Y-%m-%d"),
                "line": selected_line,
                "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S"),
                "window_end": window_end.strftime("%Y-%m-%d %H:%M:%S"),
                "total_rows": int(len(d)),
                "pcbs_flagged": int(total_flagged),
                "pcbs_checked": pcbs_checked,
                "source_file_name": os.path.basename(path),
            }

            stats = [
                f"File: {os.path.basename(path)}",
                f"Line: {selected_line}",
                f"Window: {window_start} → {window_end}",
                f"Rows (event rows): {len(d):,}",
                f"PCBs flagged: {int(total_flagged):,}",
                f"PCBs checked: {pcbs_checked if pcbs_checked is not None else '-'}",
                f"Trimmed: {'YES' if trimmed else 'NO'}",
            ]

            self.after(0, lambda: self.log_stats.set("\n".join(stats)))
            self.after(0, lambda: self.log_status.set("Preview ready. Click 'Confirm & Save Log'."))

        except Exception as e:
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            self.after(0, lambda: self._on_error(err))
            self.after(0, lambda: self.log_status.set("Preview failed."))
            self._pending_log = None

    def log_confirm_save(self):
        if not self._pending_log:
            messagebox.showwarning("Nothing to save", "Upload a CSV for preview first.")
            return

        p = self._pending_log

        replace = False
        if log_exists(p["log_date"], p["line"]):
            replace = messagebox.askyesno(
                "Replace existing log?",
                f"A log already exists for {p['log_date']} ({p['line']}).\nReplace it?"
            )
            if not replace:
                self.log_status.set("Save cancelled.")
                return

        sure = messagebox.askyesno(
            "Confirm save",
            f"Save this daily log?\n\n"
            f"Date: {p['log_date']}\n"
            f"Line: {p['line']}\n"
            f"Flagged: {p['pcbs_flagged']}\n"
            f"Checked: {p['pcbs_checked'] if p['pcbs_checked'] is not None else '-'}"
        )
        if not sure:
            return

        upsert_log(**p, detected_line="", ratio_rows_per_pcb=None, replace=replace)

        self.log_status.set(f"Saved log for {p['log_date']} ({p['line']}).")
        self._pending_log = None
        self._refresh_years_months()

    def log_undo(self):
        self._pending_log = None
        self.log_file_path.set("")
        self.log_stats.set("")
        self.log_pcbs_checked.set("")
        self.log_status.set("Reset done. Upload a CSV to preview again.")

    # ---------------- Trends UI ----------------
    def _build_trends_ui(self):
        wrap = tk.Frame(self.trends_tab, padx=12, pady=12)
        wrap.pack(fill="both", expand=True)

        cfg = tk.LabelFrame(wrap, text="Trends (from logged daily data)", padx=10, pady=10)
        cfg.pack(fill="x")

        tk.Label(cfg, text="Line:").grid(row=0, column=0, sticky="w")
        self.cb_trend_line = ttk.Combobox(cfg, textvariable=self.trend_line, values=["line1", "line2", "line4", "all"], width=10, state="readonly")
        self.cb_trend_line.grid(row=0, column=1, padx=(6, 14), sticky="w")
        self.cb_trend_line.bind("<<ComboboxSelected>>", lambda e: self._refresh_years_months())

        tk.Label(cfg, text="Mode:").grid(row=0, column=2, sticky="w")
        self.cb_trend_mode = ttk.Combobox(cfg, textvariable=self.trend_mode, values=["month", "range", "all"], width=10, state="readonly")
        self.cb_trend_mode.grid(row=0, column=3, padx=(6, 14), sticky="w")
        self.cb_trend_mode.bind("<<ComboboxSelected>>", lambda e: self._update_trend_mode_ui())

        tk.Label(cfg, text="Metric:").grid(row=0, column=4, sticky="w")
        ttk.Combobox(cfg, textvariable=self.trend_metric, values=["Counts", "FPY %"], width=10, state="readonly").grid(
            row=0, column=5, padx=(6, 14), sticky="w"
        )

        tk.Label(cfg, text="Year:").grid(row=1, column=0, sticky="w", pady=(10, 0))
        self.cb_trend_year = ttk.Combobox(cfg, textvariable=self.trend_year, values=[], width=10, state="readonly")
        self.cb_trend_year.grid(row=1, column=1, padx=(6, 14), sticky="w", pady=(10, 0))
        self.cb_trend_year.bind("<<ComboboxSelected>>", lambda e: self._refresh_months_only())

        tk.Label(cfg, text="Month:").grid(row=1, column=2, sticky="w", pady=(10, 0))
        self.cb_trend_month = ttk.Combobox(cfg, textvariable=self.trend_month, values=[], width=10, state="readonly")
        self.cb_trend_month.grid(row=1, column=3, padx=(6, 14), sticky="w", pady=(10, 0))

        tk.Label(cfg, text="From (dd/mm/yyyy):").grid(row=2, column=0, sticky="w", pady=(10, 0))
        self.ent_range_from = tk.Entry(cfg, textvariable=self.range_from, width=14)
        self.ent_range_from.grid(row=2, column=1, padx=(6, 6), sticky="w", pady=(10, 0))
        self.btn_range_from = tk.Button(cfg, text="📅", command=lambda: self._pick_date(self.range_from))
        self.btn_range_from.grid(row=2, column=2, sticky="w", pady=(10, 0))

        tk.Label(cfg, text="To (dd/mm/yyyy):").grid(row=2, column=3, sticky="w", pady=(10, 0))
        self.ent_range_to = tk.Entry(cfg, textvariable=self.range_to, width=14)
        self.ent_range_to.grid(row=2, column=4, padx=(6, 6), sticky="w", pady=(10, 0))
        self.btn_range_to = tk.Button(cfg, text="📅", command=lambda: self._pick_date(self.range_to))
        self.btn_range_to.grid(row=2, column=5, sticky="w", pady=(10, 0))

        btn_row = tk.Frame(cfg)
        btn_row.grid(row=3, column=0, columnspan=6, sticky="w", pady=(10, 4))
        tk.Button(btn_row, text="Generate Trend", command=self._trend_generate, width=18).pack(side="left")
        tk.Button(btn_row, text="Export Trend CSV", command=self._trend_export_csv, width=18).pack(side="left", padx=10)

        st = tk.Frame(wrap)
        st.pack(fill="x", pady=(4, 10))
        tk.Label(st, textvariable=self.trend_stats, fg="#0b5394", justify="left").pack(anchor="w")
        tk.Label(st, textvariable=self.trend_selected_stats, fg="#222", justify="left").pack(anchor="w")

        chart = tk.LabelFrame(wrap, text="Trend chart (click a bar/day)", padx=8, pady=8)
        chart.pack(fill="both", expand=True)

        self.trend_canvas = tk.Label(chart, cursor="hand2")
        self.trend_canvas.pack(fill="both", expand=True)
        self.trend_canvas.bind("<Button-1>", self._on_trend_click)

        self._update_trend_mode_ui()

    def _update_trend_mode_ui(self):
        mode = (self.trend_mode.get() or "month").strip().lower()

        try:
            self.cb_trend_year.configure(state="readonly" if mode == "month" else "disabled")
            self.cb_trend_month.configure(state="readonly" if mode == "month" else "disabled")
        except Exception:
            pass

        range_state = "normal" if mode == "range" else "disabled"
        try:
            self.ent_range_from.configure(state=range_state)
            self.ent_range_to.configure(state=range_state)
            self.btn_range_from.configure(state=range_state)
            self.btn_range_to.configure(state=range_state)
        except Exception:
            pass

        if mode == "all":
            try:
                self.cb_trend_year.configure(state="disabled")
                self.cb_trend_month.configure(state="disabled")
                self.ent_range_from.configure(state="disabled")
                self.ent_range_to.configure(state="disabled")
                self.btn_range_from.configure(state="disabled")
                self.btn_range_to.configure(state="disabled")
            except Exception:
                pass

    def _refresh_years_months(self):
        try:
            line = (self.trend_line.get() or "line4").strip()
            years = _db_list_years(line)
            years_str = [str(y) for y in years]

            if hasattr(self, "cb_trend_year"):
                self.cb_trend_year.configure(values=years_str)

            if years:
                y = years[-1]
                self.trend_year.set(str(y))
                months = _db_list_months_for_year(line, y)
                months_str = [f"{m:02d}" for m in months]
                if hasattr(self, "cb_trend_month"):
                    self.cb_trend_month.configure(values=months_str)
                if months:
                    self.trend_month.set(f"{months[-1]:02d}")
                else:
                    self.trend_month.set("")
            else:
                self.trend_year.set("")
                self.trend_month.set("")
                if hasattr(self, "cb_trend_month"):
                    self.cb_trend_month.configure(values=[])

        except Exception:
            self.trend_year.set("")
            self.trend_month.set("")
            try:
                if hasattr(self, "cb_trend_year"):
                    self.cb_trend_year.configure(values=[])
                if hasattr(self, "cb_trend_month"):
                    self.cb_trend_month.configure(values=[])
            except Exception:
                pass

    def _refresh_months_only(self):
        try:
            line = (self.trend_line.get() or "line4").strip()
            y_raw = (self.trend_year.get() or "").strip()
            if not y_raw:
                return
            y = int(y_raw)
            months = _db_list_months_for_year(line, y)
            months_str = [f"{m:02d}" for m in months]
            self.cb_trend_month.configure(values=months_str)
            if months and (self.trend_month.get() not in months_str):
                self.trend_month.set(f"{months[-1]:02d}")
        except Exception:
            pass

    def _trend_generate(self):
        self.trend_stats.set("Generating trend...")
        self.trend_selected_stats.set("")
        self._trend_df_current = None
        self._trend_dates = []
        self._trend_selected_idx = None
        t = threading.Thread(target=self._trend_generate_safe, daemon=True)
        t.start()

    def _trend_generate_safe(self):
        try:
            out_dir = self.output_dir.get().strip() or os.path.join(os.getcwd(), "outputs")
            ensure_outputs_dir(out_dir)
            self.trend_png_path = os.path.join(out_dir, "trend.png")
            self.trend_csv_path = os.path.join(out_dir, "trend_data.csv")
            self.trend_summary_csv_path = os.path.join(out_dir, "trend_summary.csv")

            line = (self.trend_line.get() or "line4").strip()
            mode = (self.trend_mode.get() or "month").strip().lower()
            metric = (self.trend_metric.get() or "Counts").strip()

            rows = []
            if mode == "all":
                if line == "all":
                    rows = _db_fetch_all_lines_alltime()
                else:
                    rows = _db_fetch_all(line)
            elif mode == "month":
                y_raw = (self.trend_year.get() or "").strip()
                m_raw = (self.trend_month.get() or "").strip()
                if not y_raw or not m_raw:
                    raise ValueError("Pick Year + Month first (or change mode).")
                y = int(y_raw)
                m = int(m_raw)
                d0, d1 = _ym_to_range(y, m)
                start_s = d0.strftime("%Y-%m-%d")
                end_s = d1.strftime("%Y-%m-%d")
                if line == "all":
                    rows = _db_fetch_all_lines(start_s, end_s)
                else:
                    allr = _db_fetch_all(line)
                    rows = [r for r in allr if start_s <= r["log_date"] <= end_s]
            elif mode == "range":
                d_from = _parse_ddmmyyyy(self.range_from.get())
                d_to = _parse_ddmmyyyy(self.range_to.get())
                if d_to < d_from:
                    raise ValueError("Range 'To' date must be after 'From' date.")
                start_s = d_from.strftime("%Y-%m-%d")
                end_s = d_to.strftime("%Y-%m-%d")
                if line == "all":
                    rows = _db_fetch_all_lines(start_s, end_s)
                else:
                    allr = _db_fetch_all(line)
                    rows = [r for r in allr if start_s <= r["log_date"] <= end_s]
            else:
                raise ValueError("Unknown trend mode.")

            if not rows:
                raise ValueError("No logged data found for this selection.")

            total_flagged = sum(int(r.get("pcbs_flagged", 0) or 0) for r in rows)
            total_rows = sum(int(r.get("total_rows", 0) or 0) for r in rows)
            n_days = len(rows)

            checked_vals = []
            daily_fpy_vals = []
            total_rows_on_checked_days = 0
            total_checked_for_ratio = 0

            for r in rows:
                c = r.get("pcbs_checked")
                f = int(r.get("pcbs_flagged", 0) or 0)
                tr = int(r.get("total_rows", 0) or 0)

                if c is None:
                    continue
                try:
                    c = int(c)
                except Exception:
                    continue

                checked_vals.append(c)

                if c > 0:
                    daily_fpy = (max(0, c - f) / c) * 100.0
                    daily_fpy_vals.append(daily_fpy)

                    total_rows_on_checked_days += tr
                    total_checked_for_ratio += c

            total_checked = sum(checked_vals)
            n_checked_days = len(checked_vals)

            avg_flagged_per_day = (total_flagged / n_days) if n_days > 0 else 0.0
            avg_checked_per_day = (total_checked / n_checked_days) if n_checked_days > 0 else None
            avg_fpy = (sum(daily_fpy_vals) / len(daily_fpy_vals)) if daily_fpy_vals else None

            overall_fpy = None
            if total_checked > 0:
                overall_fpy = (max(0, total_checked - total_flagged) / total_checked) * 100.0

            avg_defects_per_board = None
            if total_checked_for_ratio > 0:
                avg_defects_per_board = total_rows_on_checked_days / total_checked_for_ratio

            checked_str = f"{total_checked:,}" if n_checked_days > 0 else "-"
            avg_checked_str = f"{avg_checked_per_day:,.2f}" if avg_checked_per_day is not None else "-"
            avg_fpy_str = f"{avg_fpy:.2f}%" if avg_fpy is not None else "-"
            overall_fpy_str = f"{overall_fpy:.2f}%" if overall_fpy is not None else "-"
            avg_defects_per_board_str = f"{avg_defects_per_board:.4f}" if avg_defects_per_board is not None else "-"

            if metric == "FPY %":
                headline = (
                    f"FPY (overall): {overall_fpy_str} | Avg FPY: {avg_fpy_str} | Days: {n_days} | "
                    f"Checked: {checked_str} | Flagged: {total_flagged:,}\n"
                    f"Avg flagged/day: {avg_flagged_per_day:,.2f} | Avg checked/day: {avg_checked_str} | "
                    f"Avg defects/board: {avg_defects_per_board_str}"
                )
            else:
                headline = (
                    f"Total PCBs flagged: {total_flagged:,} | Days: {n_days} | Total PCBs checked: {checked_str}\n"
                    f"Avg flagged/day: {avg_flagged_per_day:,.2f} | Avg checked/day: {avg_checked_str} | "
                    f"Avg FPY: {avg_fpy_str} | Avg defects/board: {avg_defects_per_board_str}"
                )

            title = "PCBs trend (daily logs)"
            if line != "all":
                title = f"{line.upper()} - PCBs trend (daily logs)"
            else:
                title = "ALL LINES - PCBs trend (daily logs)"

            # --- UPDATED: colored checked behind flagged + numeric stacked labels ---
            self._plot_trend_checked_flagged(rows, self.trend_png_path, title=title, metric=metric)

            self._trend_dates = [str(r["log_date"]) for r in rows]
            self._trend_df_current = rows
            self._trend_selected_idx = None

            # export CSVs (best-effort)
            try:
                import csv
                with open(self.trend_csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=sorted(rows[0].keys()))
                    w.writeheader()
                    w.writerows(rows)

                with open(self.trend_summary_csv_path, "w", encoding="utf-8") as f:
                    f.write("metric,value\n")
                    f.write(f"days,{n_days}\n")
                    f.write(f"total_flagged,{total_flagged}\n")
                    f.write(f"total_checked,{total_checked}\n")
                    f.write(f"total_rows,{total_rows}\n")
                    if avg_defects_per_board is not None:
                        f.write(f"avg_defects_per_board,{avg_defects_per_board}\n")
            except Exception:
                pass

            self.after(0, lambda: self.trend_stats.set(headline))
            self.after(0, lambda: self._load_image(self.trend_canvas, self.trend_png_path, which="trend"))

        except Exception as e:
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            self.after(0, lambda: self._on_error(err))
            self.after(0, lambda: self.trend_stats.set("Trend generation failed."))

    def _on_trend_click(self, event):
        if not self._trend_df_current or not self._trend_dates:
            return

        w = max(self.trend_canvas.winfo_width(), 1)
        n = len(self._trend_dates)
        idx = self._x_to_index(event.x, w, n, which="trend")
        idx = max(0, min(idx, n - 1))
        self._trend_selected_idx = idx

        try:
            day = self._trend_dates[idx]
            row = self._trend_df_current[idx]

            flagged = int(row.get("pcbs_flagged", 0) or 0)
            total_rows = int(row.get("total_rows", 0) or 0)

            checked = row.get("pcbs_checked", None)
            checked_s = "-" if checked is None else f"{int(checked):,}"

            msg = [
                f"Selected day: {day}",
                f"  • PCBs flagged: {flagged:,}",
                f"  • PCBs checked: {checked_s}",
                f"  • Rows (event rows): {total_rows:,}",
            ]

            if checked is not None and int(checked) > 0:
                c = int(checked)
                fpy = (max(0, c - flagged) / c) * 100.0
                defects_per_board = (total_rows / c) if c > 0 else 0.0
                msg.append(f"  • FPY: {fpy:.2f}%")
                msg.append(f"  • Defects/board: {defects_per_board:.4f}")

            self.trend_selected_stats.set("\n".join(msg))
        except Exception:
            pass

    def _trend_export_csv(self):
        out_dir = os.path.dirname(self.trend_csv_path) if self.trend_csv_path else (self.output_dir.get().strip() or os.getcwd())
        try:
            if sys.platform.startswith("win"):
                os.startfile(out_dir)  # type: ignore[attr-defined]
            elif sys.platform.startswith("darwin"):
                subprocess.run(["open", out_dir], check=False)
            else:
                subprocess.run(["xdg-open", out_dir], check=False)
        except Exception as e:
            messagebox.showerror("Open folder failed", str(e))

    # ---------------- Formats UI ----------------
    def _build_formats_ui(self):
        wrap = tk.Frame(self.formats_tab, padx=12, pady=12)
        wrap.pack(fill="both", expand=True)

        box = tk.LabelFrame(wrap, text="Supported AOI CSV headers (exact order)", padx=12, pady=12)
        box.pack(fill="both", expand=True)

        formats_text = (
            "LINE1\n"
            "BarCode AllBarCode JobFileIDShare StartDateTime PCBID MachineID EndDateTime UserID "
            "PCBResultBefore PCBResultAfter PCBRepair TB Checksum uname PackageName PackageType \n"
            "PartNumber unameAngle ArrayIndex InspType PackageTypeGroup Failure Defect Repair LeadName LeadID\n\n"
            "LINE2\n"
            "StartDateTime JobFileIDShare AllBarCode PCBID MachineID EndDateTime UserID "
            "PCBResultBefore PCBResultAfter PCBRepair BarCode TB Checksum uname PackageName PackageType\n "
            "PartNumber unameAngle ArrayIndex InspType PackageTypeGroup Failure Defect Repair LeadName LeadID\n\n"
            "LINE4\n"
            "PCBID MachineID JobFileIDShare StartDateTime EndDateTime UserID PCBResultBefore PCBResultAfter "
            "PCBRepair BarCode TB Checksum uname PackageName PackageType PartNumber unameAngle ArrayIndex\n "
            "InspType PackageTypeGroup Failure Defect Repair LeadName LeadID AllBarCode"
        )

        lbl = tk.Label(box, text=formats_text, justify="left", anchor="nw")
        lbl.pack(fill="both", expand=True)

    # ---------------- shared helpers ----------------
    def _load_image(self, widget: tk.Label, path: str, which: str):
        if not os.path.exists(path):
            widget.configure(image="", text=f"Image not found:\n{path}")
            return

        # Use main-window dimensions (stable across clicks) — widget.winfo_width() grows
        # after each image load creating a resize feedback loop, so we avoid it here.
        win_w = max(self.winfo_width(), 1500)
        win_h = max(self.winfo_height(), 860)

        if which in ("defects", "cards"):
            t_w = max(int(win_w * 0.46), 520)
            t_h = max(int(win_h * 0.56), 380)
        else:  # trend: single full-width panel
            t_w = max(int(win_w * 0.91), 900)
            t_h = max(int(win_h * 0.56), 380)

        img = Image.open(path)
        img.thumbnail((t_w, t_h))
        disp_w, disp_h = img.size
        # Store actual displayed size for click-coordinate correction
        self._img_display_size[which] = (disp_w, disp_h)

        tk_img = ImageTk.PhotoImage(img)
        # Pin widget size to displayed image — prevents geometry manager from
        # propagating size changes to parent frames on each successive image load.
        widget.configure(image=tk_img, text="")

        if which == "defects":
            self._img_defects = tk_img
        elif which == "cards":
            self._img_cards = tk_img
        else:
            self._img_trend = tk_img

    def _on_error(self, err: str):
        messagebox.showerror("Error", err)

    # ===================== Report Tab =====================

    def _build_report_ui(self):
        wrap = tk.Frame(self.report_tab, padx=12, pady=12)
        wrap.pack(fill="both", expand=True)

        box = tk.LabelFrame(wrap, text="PDF Report Generator", padx=12, pady=12)
        box.pack(fill="x")

        # Date row
        tk.Label(box, text="Date (DD/MM/YYYY):").grid(row=0, column=0, sticky="w", pady=4)
        tk.Entry(box, textvariable=self.report_date, width=16).grid(row=0, column=1, sticky="w", padx=6)

        # File picker rows
        for row_idx, (label, var) in enumerate(
            [("Line 1 CSV:", self.report_line1_path),
             ("Line 2 CSV:", self.report_line2_path),
             ("Line 4 CSV:", self.report_line4_path)],
            start=1,
        ):
            tk.Label(box, text=label).grid(row=row_idx, column=0, sticky="w", pady=4)
            tk.Entry(box, textvariable=var, width=52).grid(row=row_idx, column=1, sticky="ew", padx=6)
            tk.Button(
                box, text="Browse",
                command=lambda v=var: v.set(
                    filedialog.askopenfilename(
                        title="Select AOI CSV",
                        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                    ) or v.get()
                ),
            ).grid(row=row_idx, column=2, padx=4)

        box.columnconfigure(1, weight=1)

        # Generate button
        btn_frame = tk.Frame(wrap, pady=10)
        btn_frame.pack(fill="x")
        tk.Button(
            btn_frame, text="Generate & Save Report",
            font=("", 11, "bold"), bg="#0078D4", fg="white",
            activebackground="#005A9E", activeforeground="white",
            padx=14, pady=6,
            command=self._on_generate_report,
        ).pack(side="left")

        # Status label
        tk.Label(wrap, textvariable=self.report_status, justify="left",
                 anchor="w", wraplength=800).pack(fill="x", pady=4)

    def _on_generate_report(self):
        date_str = self.report_date.get().strip()
        if not date_str:
            self.report_status.set("Error: please enter a date.")
            return

        p1 = self.report_line1_path.get().strip()
        p2 = self.report_line2_path.get().strip()
        p4 = self.report_line4_path.get().strip()

        if not any([p1, p2, p4]):
            self.report_status.set("Error: select at least one CSV file.")
            return

        out_path = filedialog.asksaveasfilename(
            title="Save PDF Report As",
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            initialfile=f"AOI_REPORT_{date_str.replace('/', '')}.pdf",
        )
        if not out_path:
            return  # user cancelled

        self.report_status.set("Generating report, please wait…")
        self.update_idletasks()

        threading.Thread(
            target=self._run_report_safe,
            args=(p1, p2, p4, date_str, out_path),
            daemon=True,
        ).start()

    def _run_report_safe(self, p1, p2, p4, date_str, out_path):
        try:
            def _load(path):
                if not path:
                    return None
                df_raw = load_any_aoi(path)
                df = clean_aoi_data(df_raw)
                return df if df is not None and not df.empty else None

            df1 = _load(p1)
            df2 = _load(p2)
            df4 = _load(p4)

            report.generate_pdf(df1, df2, df4, date_str, out_path)

            short = os.path.basename(out_path)
            self.after(0, lambda: self.report_status.set(f"Done — saved: {short}"))
        except Exception as exc:
            msg = f"Error: {exc}"
            self.after(0, lambda m=msg: self.report_status.set(m))


if __name__ == "__main__":
    AOIApp().mainloop()