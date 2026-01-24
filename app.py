# app.py
import os
import re
import threading
import traceback
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk
from datetime import datetime, date, timedelta
import calendar
import sqlite3

from PIL import Image, ImageTk

from src.parsers import load_any_aoi
from src.clean_data import clean_aoi_data
from src.analysis import (
    ensure_outputs_dir,
    top_defects,
    cards_scanned_over_time,
    pcbs_flagged_by_card,
)
from src.plots import (
    plot_top_defects_bars,
    plot_time_series_counts_bar,
    plot_pcbs_flagged_trend,
)

# ✅ log_db (persistent across packaging)
from src.log_db import (
    init_db,
    get_db_path,
    log_exists,
    delete_log,
    upsert_log,
    fetch_pcbs_flagged_trend,
)

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

# ---------------- Local DB helpers (avoid missing imports) ----------------
def _db_connect():
    return sqlite3.connect(get_db_path())

def _db_fetch_all(line: str):
    con = _db_connect()
    cur = con.cursor()
    cur.execute(
        """
        SELECT log_date, COALESCE(pcbs_flagged, 0), pcbs_checked
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
            }
        )
    return out

def _db_list_years(line: str):
    con = _db_connect()
    cur = con.cursor()
    cur.execute(
        """
        SELECT DISTINCT substr(log_date, 1, 4) AS y
        FROM daily_logs
        WHERE line = ?
        ORDER BY y ASC
        """,
        (line,),
    )
    years = [int(r[0]) for r in cur.fetchall() if r[0] is not None]
    con.close()
    return years

def _db_list_months_for_year(line: str, year: int):
    con = _db_connect()
    cur = con.cursor()
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
    months = [int(r[0]) for r in cur.fetchall() if r[0] is not None]
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
        years = [str(y) for y in range(date.today().year - 5, date.today().year + 2)]

        ttk.Combobox(frm, textvariable=self.var_day, values=days, width=5, state="readonly").grid(row=1, column=0, padx=6, pady=6)
        ttk.Combobox(frm, textvariable=self.var_month, values=months, width=5, state="readonly").grid(row=1, column=1, padx=6, pady=6)
        ttk.Combobox(frm, textvariable=self.var_year, values=years, width=7, state="readonly").grid(row=1, column=2, padx=6, pady=6)

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

        # ✅ Main app boots straight into notebook (no boot screen)
        self.main_frame = tk.Frame(self)
        self.main_frame.pack(fill="both", expand=True)

        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill="both", expand=True)

        self.analysis_tab = tk.Frame(self.notebook)
        self.log_tab = tk.Frame(self.notebook)
        self.trends_tab = tk.Frame(self.notebook)
        self.formats_tab = tk.Frame(self.notebook)   # ✅ RESTORED (4th tab)

        self.notebook.add(self.analysis_tab, text="Analysis")
        self.notebook.add(self.log_tab, text="Log Data")
        self.notebook.add(self.trends_tab, text="Trends")
        self.notebook.add(self.formats_tab, text="Formats")  # ✅ RESTORED

        # start on Analysis tab
        self.notebook.select(self.analysis_tab)

        # images
        self._img_defects = None
        self._img_cards = None
        self._img_trend = None

        # store last line-level outputs so we can revert
        self._line_defects_png = None
        self._line_cards_png = None
        self._line_summary = ""
        self._line_breakdown = ""
        self._line_top2 = ("", "", "", "")

        # keep df in memory for card click
        self._df_current = None
        self._current_out_dir = None

        # ---------- Analysis state ----------
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

        # ---------- Log Data state ----------
        self.log_line = tk.StringVar(value="line4")
        self.log_date_start = tk.StringVar(value=_fmt_ddmmyyyy(date.today()))
        self.log_date_end = tk.StringVar(value=_fmt_ddmmyyyy(date.today() + timedelta(days=1)))
        self.log_file_path = tk.StringVar(value="")

        self.log_status = tk.StringVar(value="Select dates + line, then upload a CSV. Preview stats before saving.")
        self.log_stats = tk.StringVar(value="")
        self._pending_log = None

        # ✅ NEW: total PCBs checked (manual)
        self.log_pcbs_checked = tk.StringVar(value="")

        # ---------- Trends state ----------
        self.trend_line = tk.StringVar(value="line4")
        self.trend_mode = tk.StringVar(value="month")  # month | range | all
        self.trend_year = tk.StringVar(value="")
        self.trend_month = tk.StringVar(value="")
        self.range_from = tk.StringVar(value="")
        self.range_to = tk.StringVar(value="")
        self.trend_stats = tk.StringVar(value="")
        self.trend_png_path = os.path.join(os.getcwd(), "outputs", "trend.png")

        # build tabs
        self._build_analysis_ui()
        self._build_log_ui()
        self._build_trends_ui()
        self._build_formats_ui()  # ✅ RESTORED

        # populate trends dropdowns
        self._refresh_years_months()

    # ---------------- Analysis UI ----------------
    def _build_analysis_ui(self):
        top = tk.Frame(self.analysis_tab, padx=12, pady=10)
        top.pack(fill="x")

        tk.Label(top, text="Input file (Line1/Line2/Line4 CSV logs):").grid(row=0, column=0, sticky="w")
        tk.Entry(top, textvariable=self.input_path, width=95).grid(row=0, column=1, padx=8)
        tk.Button(top, text="Browse", command=self.browse_file, width=12).grid(row=0, column=2)

        tk.Label(top, text="Output folder:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        tk.Entry(top, textvariable=self.output_dir, width=95).grid(row=1, column=1, padx=8, pady=(8, 0))
        tk.Button(top, text="Choose", command=self.choose_output_dir, width=12).grid(row=1, column=2, pady=(8, 0))

        tk.Button(top, text="Run Analysis", command=self.run_analysis, width=16).grid(row=2, column=1, pady=12, sticky="e")
        tk.Button(top, text="Open Output Folder", command=self.open_output_folder, width=18).grid(row=2, column=2, pady=12)

        status = tk.Frame(self.analysis_tab, padx=12, pady=0)
        status.pack(fill="x")
        tk.Label(status, textvariable=self.status_text, fg="#0b5394").pack(anchor="w")

        body = tk.Frame(self.analysis_tab, padx=12, pady=10)
        body.pack(fill="both", expand=True)

        left = tk.LabelFrame(body, text="Top Defects (PNG)", padx=8, pady=8)
        left.pack(side="left", fill="both", expand=True, padx=(0, 6))

        right = tk.LabelFrame(body, text="PCBs Flagged (PNG)", padx=8, pady=8)
        right.pack(side="left", fill="both", expand=True, padx=(6, 0))

        # Top-2 defects (two columns)
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

        self.defects_canvas = tk.Label(left)
        self.defects_canvas.pack(fill="both", expand=True)

        summary = tk.Frame(right)
        summary.pack(fill="x", pady=(0, 6))
        tk.Label(summary, textvariable=self.total_cards_text, font=("Segoe UI", 10, "bold")).pack(anchor="w")
        tk.Label(summary, textvariable=self.card_breakdown_text, font=("Segoe UI", 9), fg="#333", justify="left").pack(anchor="w")

        # Card click UI (small + optional; does nothing until analysis ran)
        card_ui = tk.Frame(right)
        card_ui.pack(fill="x", pady=(8, 6))
        tk.Label(card_ui, text="Click a card:", font=("Segoe UI", 9, "bold")).pack(side="left")
        self.btn_show_line = tk.Button(card_ui, text="Show Line Summary", command=self._show_line_summary, width=18)
        self.btn_show_line.pack(side="right")

        self.card_list = tk.Listbox(right, height=6)
        self.card_list.pack(fill="x", pady=(0, 8))
        self.card_list.bind("<<ListboxSelect>>", self._on_card_click)

        self.cards_canvas = tk.Label(right)
        self.cards_canvas.pack(fill="both", expand=True)

    def browse_file(self):
        path = filedialog.askopenfilename(
            title="Select AOI CSV file",
            filetypes=[("CSV", "*.csv"), ("All Files", "*.*")]
        )
        if path:
            self.input_path.set(path)
            self.status_text.set("Ready. Click Run Analysis.")

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
        os.startfile(out_dir)

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

        t = threading.Thread(target=self._run_analysis_safe, args=(in_path, out_dir), daemon=True)
        t.start()

    def _format_fixes(self, defect_name: str):
        key = _normalize_defect_name(defect_name)
        fixes = DEFECT_FIXES_SIMPLE.get(key) or DEFECT_FIXES_SIMPLE.get(defect_name) or [
            "Check the AOI image and placement.",
            "Confirm the correct reel/part.",
            "Check program/settings and rerun.",
        ]
        return "\n".join([f"• {x}" for x in fixes])

    def _run_analysis_safe(self, in_path: str, out_dir: str):
        try:
            ensure_outputs_dir(out_dir)

            df_raw = load_any_aoi(in_path)
            df = clean_aoi_data(df_raw)

            # store df for card clicks
            self._df_current = df.copy()
            self._current_out_dir = out_dir

            defects = top_defects(df, top_n=20)
            defects_csv = os.path.join(out_dir, "defect_pareto.csv")
            defects_png = os.path.join(out_dir, "defect_pareto.png")
            defects.to_csv(defects_csv, index=False)

            plot_top_defects_bars(defects, defects_png, title="Top defect types flagged by AOI (event rows)")

            top2 = defects.head(2).copy()
            left_title = left_body = right_title = right_body = ""

            if len(top2) >= 1:
                d1 = str(top2.iloc[0]["Defect"])
                c1 = int(top2.iloc[0]["Count"])
                left_title = f"1) {d1}  (Count: {c1})"
                left_body = self._format_fixes(d1)

            if len(top2) >= 2:
                d2 = str(top2.iloc[1]["Defect"])
                c2 = int(top2.iloc[1]["Count"])
                right_title = f"2) {d2}  (Count: {c2})"
                right_body = self._format_fixes(d2)

            ts, grain, total_flagged = cards_scanned_over_time(df, hour_to_day_threshold_days=3)
            cards_png = os.path.join(out_dir, "pcbs_scanned_by_hour.png")
            title = "PCBs flagged per Hour" if grain == "hour" else "PCBs flagged per Day"

            plot_time_series_counts_bar(
                ts,
                cards_png,
                title=title,
                y_label="Unique PCBIDs flagged",
                grain=grain,
            )

            by_card = pcbs_flagged_by_card(df)
            by_card_csv = os.path.join(out_dir, "pcbs_flagged_by_card.csv")
            by_card.to_csv(by_card_csv, index=False)

            breakdown_lines = []
            card_names = []
            if not by_card.empty:
                for _, r in by_card.iterrows():
                    card_names.append(str(r["Card"]))
                for _, r in by_card.head(6).iterrows():
                    breakdown_lines.append(f"• {r['Card']}: {int(r['Count'])}")
                if len(by_card) > 6:
                    breakdown_lines.append(f"• ... +{len(by_card) - 6} more")
            breakdown_text = "\n".join(breakdown_lines)

            summary = f"Total PCBs flagged in period: {total_flagged:,}  ({'Hourly' if grain=='hour' else 'Daily'} view)"

            # remember line-level outputs for quick revert
            self._line_defects_png = defects_png
            self._line_cards_png = cards_png
            self._line_summary = summary
            self._line_breakdown = breakdown_text
            self._line_top2 = (left_title, left_body, right_title, right_body)

            self.after(
                0,
                lambda: self._on_analysis_success(
                    defects_png, cards_png, out_dir, summary, breakdown_text,
                    left_title, left_body, right_title, right_body, card_names
                ),
            )

        except Exception as e:
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            self.after(0, lambda: self._on_error(err))

    def _on_analysis_success(self, defects_png, cards_png, out_dir, summary, breakdown, ltitle, lbody, rtitle, rbody, card_names):
        self.status_text.set(f"Done. Outputs saved to: {out_dir}")

        self.total_cards_text.set(summary)
        self.card_breakdown_text.set(breakdown)

        self.defects_hint_text.set("")
        self.top2_left_title.set(ltitle)
        self.top2_left_body.set(lbody)
        self.top2_right_title.set(rtitle)
        self.top2_right_body.set(rbody)

        # populate card list
        self.card_list.delete(0, tk.END)
        for c in card_names:
            self.card_list.insert(tk.END, c)

        self._load_image(self.defects_canvas, defects_png, which="defects")
        self._load_image(self.cards_canvas, cards_png, which="cards")

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

    # ---- Card click behavior: generate cardwise outputs + show them ----
    def _on_card_click(self, event=None):
        if not self._df_current is not None:
            return
        sel = self.card_list.curselection()
        if not sel:
            return
        card = self.card_list.get(sel[0])
        if not card:
            return
        if not self._current_out_dir:
            return

        self.status_text.set(f"Generating card view: {card} ...")
        t = threading.Thread(target=self._build_card_view_safe, args=(card,), daemon=True)
        t.start()

    def _build_card_view_safe(self, card: str):
        try:
            df = self._df_current
            out_dir = self._current_out_dir

            # build a Card column for filtering (does NOT modify analysis module)
            d = df.copy()
            js = _jobfile_series(d)
            if js is None:
                raise ValueError("JobFileIDShare/JobFile not found; cannot build card view.")
            d["_CardName"] = js.apply(_extract_card_name)

            d_card = d[d["_CardName"] == card].copy()
            if d_card.empty:
                raise ValueError(f"No rows found for card: {card}")

            # cardwise output folder
            card_folder = os.path.join(out_dir, "cardwise", _safe_folder_name(card))
            os.makedirs(card_folder, exist_ok=True)

            # generate same outputs but for this card
            defects = top_defects(d_card, top_n=20)
            defects_csv = os.path.join(card_folder, "defect_pareto.csv")
            defects_png = os.path.join(card_folder, "defect_pareto.png")
            defects.to_csv(defects_csv, index=False)
            plot_top_defects_bars(defects, defects_png, title=f"{card} - Top defect types (event rows)")

            ts, grain, total_flagged = cards_scanned_over_time(d_card, hour_to_day_threshold_days=3)
            cards_png = os.path.join(card_folder, "pcbs_scanned_by_hour.png")
            title = f"{card} - PCBs flagged per Hour" if grain == "hour" else f"{card} - PCBs flagged per Day"
            plot_time_series_counts_bar(
                ts,
                cards_png,
                title=title,
                y_label="Unique PCBIDs flagged",
                grain=grain,
            )

            by_card = pcbs_flagged_by_card(d_card)
            by_card_csv = os.path.join(card_folder, "pcbs_flagged_by_card.csv")
            by_card.to_csv(by_card_csv, index=False)

            # top2 fixes for this card
            top2 = defects.head(2).copy()
            left_title = left_body = right_title = right_body = ""
            if len(top2) >= 1:
                d1 = str(top2.iloc[0]["Defect"])
                c1 = int(top2.iloc[0]["Count"])
                left_title = f"1) {d1}  (Count: {c1})"
                left_body = self._format_fixes(d1)
            if len(top2) >= 2:
                d2 = str(top2.iloc[1]["Defect"])
                c2 = int(top2.iloc[1]["Count"])
                right_title = f"2) {d2}  (Count: {c2})"
                right_body = self._format_fixes(d2)

            # breakdown for this card view: still show only this card count
            breakdown = f"• {card}: {total_flagged:,}"

            summary = f"Card view: {card} | Total PCBs flagged: {total_flagged:,}  ({'Hourly' if grain=='hour' else 'Daily'} view)"

            self.after(
                0,
                lambda: self._show_card_outputs(
                    card, defects_png, cards_png, summary, breakdown, left_title, left_body, right_title, right_body
                ),
            )
        except Exception as e:
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            self.after(0, lambda: self._on_error(err))
            self.after(0, lambda: self.status_text.set("Card view failed."))

    def _show_card_outputs(self, card, defects_png, cards_png, summary, breakdown, ltitle, lbody, rtitle, rbody):
        self.status_text.set(f"Showing card view: {card}")
        self.total_cards_text.set(summary)
        self.card_breakdown_text.set(breakdown)

        self.defects_hint_text.set("")
        self.top2_left_title.set(ltitle)
        self.top2_left_body.set(lbody)
        self.top2_right_title.set(rtitle)
        self.top2_right_body.set(rbody)

        self._load_image(self.defects_canvas, defects_png, which="defects")
        self._load_image(self.cards_canvas, cards_png, which="cards")

    def _show_line_summary(self):
        # revert to the last computed line-level plots
        if not self._line_defects_png or not self._line_cards_png:
            return
        self.status_text.set("Showing line summary.")
        self.total_cards_text.set(self._line_summary or "Total PCBs flagged: -")
        self.card_breakdown_text.set(self._line_breakdown or "")
        ltitle, lbody, rtitle, rbody = self._line_top2
        self.defects_hint_text.set("")
        self.top2_left_title.set(ltitle)
        self.top2_left_body.set(lbody)
        self.top2_right_title.set(rtitle)
        self.top2_right_body.set(rbody)
        self._load_image(self.defects_canvas, self._line_defects_png, which="defects")
        self._load_image(self.cards_canvas, self._line_cards_png, which="cards")

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

        # ✅ NEW: total PCBs checked input
        tk.Label(cfg, text="Total PCBs checked (manual):").grid(row=1, column=0, sticky="w", pady=(10, 0))
        tk.Entry(cfg, textvariable=self.log_pcbs_checked, width=16).grid(row=1, column=1, sticky="w", pady=(10, 0))

        hint = tk.Frame(wrap)
        hint.pack(fill="x", pady=(10, 6))
        tk.Label(
            hint,
            text="Keep the file between 07:00 of start date and 07:00 of end date.\n"
                 "If the file extends beyond these boundaries, the app can trim it (you will be asked).",
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
                raise ValueError("End date must be after start date (for 7am→7am window).")

            df_raw = load_any_aoi(path)
            df = clean_aoi_data(df_raw)

            if "StartDateTime" not in df.columns:
                raise ValueError("StartDateTime missing after cleaning.")

            d = df[df["StartDateTime"].notna()].copy()
            if d.empty:
                raise ValueError("No valid StartDateTime rows found in this file.")

            file_min = d["StartDateTime"].min()
            file_max = d["StartDateTime"].max()

            extends_before = file_min < window_start
            extends_after = file_max > window_end

            if file_max < window_start or file_min > window_end:
                raise ValueError(
                    "Selected dates do not match file timestamps.\n"
                    f"File range: {file_min} → {file_max}\n"
                    f"Selected window: {window_start} → {window_end}"
                )

            trimmed = False
            if extends_before or extends_after:
                msg = (
                    "File extends beyond selected 7am→7am window.\n\n"
                    f"File range: {file_min} → {file_max}\n"
                    f"Window:     {window_start} → {window_end}\n\n"
                    "App will trim data inside window.\n"
                    "Continue?"
                )
                ok = messagebox.askyesno("Trim to window?", msg)
                if not ok:
                    self.after(0, lambda: self.log_status.set("Preview cancelled by user (no trimming applied)."))
                    return

                d = d[(d["StartDateTime"] >= window_start) & (d["StartDateTime"] <= window_end)].copy()
                trimmed = True

                if d.empty:
                    raise ValueError("After trimming, no rows remain inside the selected window.")

            defects_total_rows = int(len(d))
            ts, grain, total_flagged = cards_scanned_over_time(d, hour_to_day_threshold_days=3)

            detected_line = d.attrs.get("line", df.attrs.get("line", ""))
            selected_line = self.log_line.get().strip()

            # read manual checked value (optional)
            checked_raw = self.log_pcbs_checked.get().strip()
            pcbs_checked = None
            if checked_raw != "":
                try:
                    pcbs_checked = int(checked_raw)
                except Exception:
                    raise ValueError("Total PCBs checked must be a whole number (or leave blank).")

            stats = []
            stats.append(f"Source file: {os.path.basename(path)}")
            stats.append(f"Detected format: {detected_line or 'unknown'}  |  Selected line: {selected_line}")
            stats.append(f"Window: {window_start}  →  {window_end}")
            stats.append(f"File rows in window: {defects_total_rows:,}  (trimmed: {'YES' if trimmed else 'NO'})")
            stats.append(f"Total PCBs flagged in window: {total_flagged:,}")
            stats.append(f"Total PCBs checked (manual): {pcbs_checked if pcbs_checked is not None else '-'}")

            d.attrs["line"] = df.attrs.get("line", "")
            by_card = pcbs_flagged_by_card(d)
            if not by_card.empty:
                stats.append("\nTop cards (flagged):")
                shown = 0
                for _, r in by_card.iterrows():
                    if shown >= 6:
                        break
                    stats.append(f"  • {str(r['Card'])}: {int(r['Count'])}")
                    shown += 1
                if len(by_card) > 6:
                    stats.append(f"  • ... +{len(by_card) - 6} more")
                stats.append(f"\nCheck: sum(by card) = {int(by_card['Count'].sum()):,} (should match total PCBs flagged)")

            payload = {
                "log_date": d_start.strftime("%Y-%m-%d"),
                "line": selected_line,
                "detected_line": detected_line or "",
                "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S"),
                "window_end": window_end.strftime("%Y-%m-%d %H:%M:%S"),
                "total_rows": defects_total_rows,
                "pcbs_flagged": int(total_flagged),
                "pcbs_checked": pcbs_checked,
                "ratio_rows_per_pcb": None,
                "source_file_name": os.path.basename(path),
            }

            self._pending_log = payload
            self.after(0, lambda: self.log_status.set("Preview ready. If correct, click 'Confirm & Save Log'."))
            self.after(0, lambda: self.log_stats.set("\n".join(stats)))

        except Exception as e:
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            self.after(0, lambda: self._on_error(err))
            self.after(0, lambda: self.log_status.set("Preview failed. Fix inputs and try again."))
            self._pending_log = None

    def log_confirm_save(self):
        if not self._pending_log:
            messagebox.showwarning("Nothing to save", "Upload a CSV for preview first. Then confirm & save.")
            return

        p = self._pending_log
        log_date = p["log_date"]
        line = p["line"]

        # extra confirmation for manual checked value (if provided)
        if p.get("pcbs_checked") is not None:
            ok_num = messagebox.askyesno(
                "Confirm checked count",
                f"Are you sure this is the correct 'Total PCBs checked'?\n\n{p['pcbs_checked']}"
            )
            if not ok_num:
                self.log_status.set("Save cancelled (checked count not confirmed).")
                return

        if log_exists(log_date, line):
            msg = f"A log already exists for {log_date} ({line}).\nReplace it?"
            replace = messagebox.askyesno("Replace existing log?", msg)
            if not replace:
                self.log_status.set("Save cancelled (existing log not replaced).")
                return
        else:
            replace = False

        sure = messagebox.askyesno(
            "Confirm save",
            f"Save this daily log?\n\nDate: {log_date}\nLine: {line}\n"
            f"PCBs flagged: {p['pcbs_flagged']}\nRows: {p['total_rows']}\n"
            f"PCBs checked: {p['pcbs_checked'] if p.get('pcbs_checked') is not None else '-'}"
        )
        if not sure:
            self.log_status.set("Save cancelled by user.")
            return

        upsert_log(
            log_date=log_date,
            line=line,
            detected_line=p["detected_line"],
            window_start=p["window_start"],
            window_end=p["window_end"],
            total_rows=p["total_rows"],
            pcbs_flagged=p["pcbs_flagged"],
            pcbs_checked=p.get("pcbs_checked"),
            ratio_rows_per_pcb=p["ratio_rows_per_pcb"],
            source_file_name=p["source_file_name"],
            replace=replace,
        )

        self.log_status.set(f"Saved log for {log_date} ({line}). Trends updated.")
        self._refresh_years_months()

    def log_undo(self):
        self._pending_log = None
        self.log_file_path.set("")
        self.log_stats.set("")
        self.log_status.set("Reset done. Select dates + line, then upload a CSV. Preview stats before saving.")

    # ---------------- Trends UI ----------------
    def _build_trends_ui(self):
        wrap = tk.Frame(self.trends_tab, padx=12, pady=12)
        wrap.pack(fill="both", expand=True)

        top = tk.LabelFrame(wrap, text="Filters", padx=10, pady=10)
        top.pack(fill="x")

        tk.Label(top, text="Line:").grid(row=0, column=0, sticky="w")
        line_cb = ttk.Combobox(top, textvariable=self.trend_line, values=["line1", "line2", "line4"],
                               width=10, state="readonly")
        line_cb.grid(row=0, column=1, padx=(6, 14), sticky="w")
        line_cb.bind("<<ComboboxSelected>>", lambda e: self._refresh_years_months())

        tk.Label(top, text="View:").grid(row=0, column=2, sticky="w")
        mode_cb = ttk.Combobox(top, textvariable=self.trend_mode, values=["month", "range", "all"],
                               width=10, state="readonly")
        mode_cb.grid(row=0, column=3, padx=(6, 14), sticky="w")
        mode_cb.bind("<<ComboboxSelected>>", lambda e: self._on_mode_change())

        tk.Label(top, text="Year:").grid(row=0, column=4, sticky="w")
        self.year_cb = ttk.Combobox(top, textvariable=self.trend_year, values=[], width=8, state="readonly")
        self.year_cb.grid(row=0, column=5, padx=(6, 14), sticky="w")
        self.year_cb.bind("<<ComboboxSelected>>", lambda e: self._refresh_months_only())

        tk.Label(top, text="Month:").grid(row=0, column=6, sticky="w")
        self.month_cb = ttk.Combobox(top, textvariable=self.trend_month, values=[], width=8, state="readonly")
        self.month_cb.grid(row=0, column=7, padx=(6, 14), sticky="w")

        tk.Label(top, text="From (dd/mm/yyyy):").grid(row=1, column=0, sticky="w", pady=(10, 0))
        tk.Entry(top, textvariable=self.range_from, width=14).grid(row=1, column=1, sticky="w", pady=(10, 0))
        tk.Button(top, text="📅", command=lambda: self._pick_date(self.range_from)).grid(row=1, column=1, sticky="e", padx=(0, 2), pady=(10, 0))

        tk.Label(top, text="To (dd/mm/yyyy):").grid(row=1, column=2, sticky="w", pady=(10, 0))
        tk.Entry(top, textvariable=self.range_to, width=14).grid(row=1, column=3, sticky="w", pady=(10, 0))
        tk.Button(top, text="📅", command=lambda: self._pick_date(self.range_to)).grid(row=1, column=3, sticky="e", padx=(0, 2), pady=(10, 0))

        tk.Button(top, text="Load Trend", command=self.load_trend).grid(row=1, column=7, sticky="e", pady=(10, 0))

        info = tk.Frame(wrap)
        info.pack(fill="x", pady=(10, 6))
        tk.Label(info, textvariable=self.trend_stats, font=("Segoe UI", 10, "bold")).pack(anchor="w")
        tk.Label(info, text=f"DB: {get_db_path()}", fg="#666", font=("Segoe UI", 8)).pack(anchor="w")

        chart = tk.LabelFrame(wrap, text="PCBs per day (Flagged + Checked)", padx=8, pady=8)
        chart.pack(fill="both", expand=True)

        self.trend_canvas = tk.Label(chart)
        self.trend_canvas.pack(fill="both", expand=True)

        self._on_mode_change()

    def _on_mode_change(self):
        mode = self.trend_mode.get()
        if mode == "month":
            self.year_cb.configure(state="readonly")
            self.month_cb.configure(state="readonly")
        else:
            self.year_cb.configure(state="disabled")
            self.month_cb.configure(state="disabled")

    def _refresh_years_months(self):
        line = self.trend_line.get()
        years = _db_list_years(line)
        self.year_cb["values"] = [str(y) for y in years]
        if years:
            self.trend_year.set(str(years[-1]))
        else:
            self.trend_year.set("")
        self._refresh_months_only()

    def _refresh_months_only(self):
        line = self.trend_line.get()
        y = self.trend_year.get().strip()
        if not y:
            self.month_cb["values"] = []
            self.trend_month.set("")
            return
        months = _db_list_months_for_year(line, int(y))
        self.month_cb["values"] = [f"{m:02d}" for m in months]
        if months:
            self.trend_month.set(f"{months[-1]:02d}")
        else:
            self.trend_month.set("")

    def load_trend(self):
        try:
            line = self.trend_line.get().strip()
            mode = self.trend_mode.get().strip()

            if mode == "all":
                rows = _db_fetch_all(line)

            elif mode == "month":
                y = int(self.trend_year.get())
                m = int(self.trend_month.get())
                d1, d2 = _ym_to_range(y, m)
                rows = fetch_pcbs_flagged_trend(line, d1.strftime("%Y-%m-%d"), d2.strftime("%Y-%m-%d"))

            else:  # range
                d1 = _parse_ddmmyyyy(self.range_from.get())
                d2 = _parse_ddmmyyyy(self.range_to.get())
                if d2 < d1:
                    d1, d2 = d2, d1
                rows = fetch_pcbs_flagged_trend(line, d1.strftime("%Y-%m-%d"), d2.strftime("%Y-%m-%d"))

            import pandas as pd
            df = pd.DataFrame(rows)

            os.makedirs(os.path.dirname(self.trend_png_path), exist_ok=True)
            title = f"{line.upper()} - PCBs per day (Flagged + Checked)"
            plot_pcbs_flagged_trend(df, self.trend_png_path, title=title)

            if df.empty:
                self.trend_stats.set("Days: 0 | Flagged: 0 | Checked: - | Avg flagged/day: 0")
            else:
                days = len(df)
                flagged_total = int(df["pcbs_flagged"].astype(int).sum())
                avg = flagged_total / days if days else 0

                checked_total = None
                if "pcbs_checked" in df.columns:
                    try:
                        checked_total = int(pd.to_numeric(df["pcbs_checked"], errors="coerce").dropna().sum())
                    except Exception:
                        checked_total = None

                if checked_total is not None:
                    self.trend_stats.set(f"Days: {days} | Flagged: {flagged_total:,} | Checked: {checked_total:,} | Avg flagged/day: {avg:.1f}")
                else:
                    self.trend_stats.set(f"Days: {days} | Flagged: {flagged_total:,} | Checked: - | Avg flagged/day: {avg:.1f}")

            self._load_image(self.trend_canvas, self.trend_png_path, which="trend")

        except Exception as e:
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            self._on_error(err)

    # ---------------- Formats UI (RESTORED) ----------------
    def _build_formats_ui(self):
        wrap = tk.Frame(self.formats_tab, padx=14, pady=14)
        wrap.pack(fill="both", expand=True)

        tk.Label(
            wrap,
            text="Supported file formats (space-separated CSV logs)",
            font=("Segoe UI", 14, "bold")
        ).pack(anchor="w", pady=(0, 10))

        info = (
            "Line 1 header:\n"
            "BarCode AllBarCode JobFileIDShare StartDateTime PCBID MachineID EndDateTime UserID "
            "PCBResultBefore PCBResultAfter PCBRepair TB Checksum uname PackageName PackageType "
            "PartNumber unameAngle ArrayIndex InspType PackageTypeGroup Failure Defect Repair LeadName LeadID\n\n"
            "Line 2 header:\n"
            "StartDateTime JobFileIDShare AllBarCode PCBID MachineID EndDateTime UserID "
            "PCBResultBefore PCBResultAfter PCBRepair BarCode TB Checksum uname PackageName PackageType "
            "PartNumber unameAngle ArrayIndex InspType PackageTypeGroup Failure Defect Repair LeadName LeadID\n\n"
            "Line 4 header:\n"
            "PCBID MachineID JobFileIDShare StartDateTime EndDateTime UserID PCBResultBefore PCBResultAfter "
            "PCBRepair BarCode TB Checksum uname PackageName PackageType PartNumber unameAngle ArrayIndex "
            "InspType PackageTypeGroup Failure Defect Repair LeadName LeadID AllBarCode"
        )

        box = tk.Text(wrap, height=18, wrap="word", font=("Consolas", 10))
        box.pack(fill="both", expand=True)
        box.insert("1.0", info)
        box.configure(state="disabled")

    # ---------------- shared helpers ----------------
    def _load_image(self, widget: tk.Label, path: str, which: str):
        if not os.path.exists(path):
            widget.configure(text=f"Image not found:\n{path}")
            return

        img = Image.open(path)
        w = max(widget.winfo_width(), 520)
        h = max(widget.winfo_height(), 420)
        img.thumbnail((w, h))
        tk_img = ImageTk.PhotoImage(img)

        widget.configure(image=tk_img)
        if which == "defects":
            self._img_defects = tk_img
        elif which == "cards":
            self._img_cards = tk_img
        else:
            self._img_trend = tk_img

    def _on_error(self, err: str):
        messagebox.showerror("Error", err)


if __name__ == "__main__":
    AOIApp().mainloop()
