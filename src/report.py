# src/report.py
"""
PDF report generator for AOI Analytics.

Structure:
  - Title header  (AOI REPORT + date)
  - All Lines summary section  (7-day history table + combined charts)
  - Per-line sections  (Line 1, Line 2, Line 4):
        section header → 7-day history table (today highlighted) →
        card count table →
        overall line hourly chart → per-card hourly charts →
        defect count table (or "Unavailable") →
        overall line defect chart → per-card defect charts
  - Page numbers in footer on every page

History table columns: Date | FPY% | Defects/Board | Cards Scanned | Cards Flagged
  - Last 7 days pulled entirely from aoi_logs.db (all rows including today)
  - Today's row highlighted yellow if data is logged; shows "—" if not yet logged
  - Bottom row: 7-day cumulative totals
"""

import io
import sqlite3
from datetime import datetime, timedelta, date as date_type

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Table, TableStyle,
    Image as RLImage, Spacer,
)

from src.analysis import (
    top_defects,
    cards_scanned_over_time,
    pcbs_flagged_by_card,
    combine_defects_dfs,
    sum_time_series_dfs_on_time,
    _extract_card_name,
    _jobfile_series,
)
from src.log_db import get_db_path

# ---------------------------------------------------------------------------
# Page geometry
# ---------------------------------------------------------------------------
_PAGE_W, _PAGE_H = A4          # 595.28 x 841.89 pts
_MARGIN = inch
_USABLE_W = _PAGE_W - 2 * _MARGIN

# ---------------------------------------------------------------------------
# Paragraph styles
# ---------------------------------------------------------------------------
_TITLE_STYLE = ParagraphStyle(
    "ReportTitle",
    fontSize=24,
    fontName="Helvetica-Bold",
    alignment=TA_CENTER,
    spaceAfter=6,
    leading=30,
)

_SECTION_STYLE = ParagraphStyle(
    "SectionHeader",
    fontSize=18,
    fontName="Helvetica",
    alignment=TA_LEFT,
    spaceAfter=4,
    spaceBefore=10,
    leading=24,
)

_UNAVAIL_STYLE = ParagraphStyle(
    "UnavailableText",
    fontSize=16,
    fontName="Helvetica-Oblique",
    alignment=TA_LEFT,
    spaceAfter=4,
    spaceBefore=10,
    leading=22,
)

# ---------------------------------------------------------------------------
# History table colours
# ---------------------------------------------------------------------------
_COL_HEADER_BG  = colors.HexColor("#4472C4")   # blue header
_COL_HEADER_FG  = colors.white
_COL_ROW_ALT    = colors.HexColor("#EEF2FA")    # light-blue alternating rows
_COL_TODAY_BG   = colors.HexColor("#FFF2CC")    # yellow  – today highlight
_COL_CUMUL_BG   = colors.HexColor("#D6E4BC")    # light-green – cumulative row
_COL_GRID       = colors.HexColor("#AAAAAA")

# History table column widths (must sum ≤ _USABLE_W = 4.77 inch at 1" margins)
_HIST_COLS = [0.80*inch, 0.80*inch, 1.10*inch, 1.25*inch, 1.25*inch]  # = 5.20"

# ---------------------------------------------------------------------------
# Chart constants
# ---------------------------------------------------------------------------
_CHART_DPI   = 130
_CHART_FIG_W = 13.0
_CHART_FIG_H = 5.8
_PDF_CHART_W = _USABLE_W



# ===========================================================================
# Footer
# ===========================================================================

def _draw_footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 9)
    canvas.setFillColor(colors.HexColor("#555555"))
    canvas.drawCentredString(_PAGE_W / 2, 0.45 * inch, f"Page {canvas.getPageNumber()}")
    canvas.restoreState()


# ===========================================================================
# Chart helpers
# ===========================================================================

def _fig_to_rl_image(fig) -> RLImage:
    fig_w_in, fig_h_in = fig.get_size_inches()
    aspect = fig_h_in / fig_w_in
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=_CHART_DPI, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return RLImage(buf, width=_PDF_CHART_W, height=_PDF_CHART_W * aspect)


def _bar_chart_fig(xlabels, counts, title, xlabel, ylabel):
    fig, ax = plt.subplots(figsize=(_CHART_FIG_W, _CHART_FIG_H))
    if not counts:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", fontsize=14)
        ax.axis("off")
        ax.set_title(title, fontsize=14, fontweight="bold")
        fig.subplots_adjust(left=0.08, right=0.98, bottom=0.18, top=0.90)
        return fig

    bars = ax.bar(range(len(xlabels)), counts, color="steelblue")
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_ylabel(ylabel, fontsize=11)
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_xticks(range(len(xlabels)))

    if len(xlabels) <= 28:
        ax.set_xticklabels(xlabels, rotation=40, ha="right", fontsize=9)
    else:
        step = max(1, len(xlabels) // 18)
        shown = ["" if i % step != 0 else xlabels[i] for i in range(len(xlabels))]
        ax.set_xticklabels(shown, rotation=40, ha="right", fontsize=9)

    ymax = max(counts)
    ax.set_ylim(0, max(1.0, ymax * 1.35 + 0.5))
    pad = max(0.05 * ymax, 0.12)
    for b, v in zip(bars, counts):
        ax.text(b.get_x() + b.get_width() / 2, v + pad, str(v),
                ha="center", va="bottom", fontsize=9, fontweight="bold", clip_on=True)

    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.22, top=0.90)
    return fig


def _make_hourly_fig(df: pd.DataFrame, title: str):
    try:
        ts_df, grain, _ = cards_scanned_over_time(df, force_7to7_when_hourly=True)
    except Exception:
        ts_df, grain = pd.DataFrame(columns=["TimeTS", "Count"]), "hour"
    return _make_hourly_fig_from_ts(ts_df, grain, title)


def _make_hourly_fig_from_ts(ts_df, grain, title: str):
    if ts_df is None or ts_df.empty or "TimeTS" not in ts_df.columns:
        fig, ax = plt.subplots(figsize=(_CHART_FIG_W, _CHART_FIG_H))
        ax.text(0.5, 0.5, "No data", ha="center", va="center", fontsize=14)
        ax.axis("off")
        ax.set_title(title, fontsize=14, fontweight="bold")
        fig.subplots_adjust(left=0.08, right=0.98, bottom=0.18, top=0.90)
        return fig

    times  = ts_df["TimeTS"].tolist()
    counts = ts_df["Count"].astype(int).tolist()
    if grain == "hour":
        xlabels = [t.strftime("%H:%M") if hasattr(t, "strftime") else str(t) for t in times]
        xlabel  = "Hour"
    else:
        xlabels = [t.strftime("%d-%b") if hasattr(t, "strftime") else str(t) for t in times]
        xlabel  = "Day"
    return _bar_chart_fig(xlabels, counts, title, xlabel, "Unique PCBIDs flagged")


def _make_defect_fig(defects_df: pd.DataFrame, title: str):
    if defects_df is None or defects_df.empty:
        fig, ax = plt.subplots(figsize=(_CHART_FIG_W, _CHART_FIG_H))
        ax.text(0.5, 0.5, "No data", ha="center", va="center", fontsize=14)
        ax.axis("off")
        ax.set_title(title, fontsize=14, fontweight="bold")
        fig.subplots_adjust(left=0.08, right=0.98, bottom=0.18, top=0.90)
        return fig
    xlabels = defects_df["Defect"].astype(str).tolist()
    counts  = defects_df["Count"].astype(int).tolist()
    return _bar_chart_fig(xlabels, counts, title, "Defect type", "Event rows")


# ===========================================================================
# Table helpers
# ===========================================================================

def _two_col_table(data: list, col_widths=None) -> Table:
    if col_widths is None:
        col_widths = [4.0 * inch, 1.5 * inch]
    t = Table(data, colWidths=col_widths)
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  colors.HexColor("#D9D9D9")),
        ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, -1), 11),
        ("GRID",          (0, 0), (-1, -1), 0.5, colors.grey),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, colors.HexColor("#F5F5F5")]),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 6),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return t


def _rl_card_table(card_counts_df: pd.DataFrame) -> Table:
    data = [["Card", "Count"]]
    for _, row in card_counts_df.iterrows():
        data.append([str(row["Card"]), str(int(row["Count"]))])
    return _two_col_table(data)


def _rl_defect_table(defects_df: pd.DataFrame) -> Table:
    data = [["Defect", "Count"]]
    for _, row in defects_df.iterrows():
        data.append([str(row["Defect"]), str(int(row["Count"]))])
    return _two_col_table(data)


# ===========================================================================
# 7-day history: DB query
# ===========================================================================

def _fetch_line_history(line_db_key: str, report_date_iso: str) -> dict:
    """
    Query aoi_logs.db for the 7 days ending on report_date_iso ("YYYY-MM-DD").

    line_db_key: "line1" | "line2" | "line4" | "all"

    Returns dict keyed by "YYYY-MM-DD":
      { scanned: int|None, flagged: int, total_rows: int,
        fpy_pct: float|None, dpb: float|None }

    scanned / fpy_pct are None when pcbs_checked was not logged.
    """
    try:
        rep = datetime.strptime(report_date_iso, "%Y-%m-%d").date()
    except ValueError:
        return {}

    start_iso = (rep - timedelta(days=6)).isoformat()
    end_iso   = report_date_iso

    try:
        # Open in read-only mode (uri=True + mode=ro) so the report never
        # writes to or creates journal/WAL files in aoi_logs.db.
        db_uri = "file:{}?mode=ro".format(get_db_path().replace("\\", "/"))
        con = sqlite3.connect(db_uri, uri=True)
        cur = con.cursor()

        if line_db_key == "all":
            # Sum across all lines per day.
            # SUM(pcbs_checked) is NULL only if every contributing row is NULL.
            cur.execute(
                """
                SELECT log_date,
                       SUM(COALESCE(total_rows,    0))  AS total_rows,
                       SUM(COALESCE(pcbs_flagged,  0))  AS flagged,
                       SUM(pcbs_checked)                AS scanned
                FROM   daily_logs
                WHERE  log_date >= ? AND log_date <= ?
                GROUP  BY log_date
                ORDER  BY log_date ASC
                """,
                (start_iso, end_iso),
            )
        else:
            cur.execute(
                """
                SELECT log_date,
                       COALESCE(total_rows,   0),
                       COALESCE(pcbs_flagged, 0),
                       pcbs_checked
                FROM   daily_logs
                WHERE  line = ? AND log_date >= ? AND log_date <= ?
                ORDER  BY log_date ASC
                """,
                (line_db_key, start_iso, end_iso),
            )

        rows = cur.fetchall()
        con.close()
    except Exception:
        rows = []

    result = {}
    for log_date, total_rows, flagged, scanned in rows:
        # scanned (pcbs_checked) may be NULL → None
        fpy_pct = (
            (scanned - flagged) / scanned * 100
            if (scanned is not None and scanned > 0)
            else None
        )
        # Match Trends page: defects/board = total_rows / pcbs_checked (scanned), not flagged
        dpb = total_rows / scanned if (scanned is not None and scanned > 0) else None
        result[log_date] = {
            "scanned":    scanned,    # int or None
            "flagged":    int(flagged),
            "total_rows": int(total_rows),
            "fpy_pct":    fpy_pct,    # float or None
            "dpb":        dpb,        # float or None
        }

    return result


# ===========================================================================
# 7-day history table builder
# ===========================================================================

def _build_history_table(
    db_rows: dict,          # from _fetch_line_history()
    report_date_iso: str,   # "YYYY-MM-DD" — the day being reported (highlighted yellow)
) -> Table:
    """
    Return a reportlab Table showing the last 7 days, sourced entirely from the DB.

    Columns : Date | FPY% | Defects/Board | Cards Scanned | Cards Flagged
    Rows    : 7 daily rows (oldest → newest) + 1 cumulative row
    Highlight: today's row in yellow (if data exists for it in the DB)
    """
    try:
        rep = datetime.strptime(report_date_iso, "%Y-%m-%d").date()
    except ValueError:
        rep = date_type.today()

    # Ordered list of 7 ISO date strings: oldest first
    dates_7 = [(rep - timedelta(days=i)).isoformat() for i in range(6, -1, -1)]

    # ------------------------------------------------------------------
    # Build rows
    # ------------------------------------------------------------------
    header = ["Date", "FPY%", "Defects/Board", "Cards Scanned", "Cards Flagged"]
    data   = [header]

    today_row_idx = None          # 1-based index in `data` for today's row

    # Accumulators for the cumulative row
    cum_scanned     = 0
    cum_flagged     = 0
    cum_defect_rows = 0
    cum_has_scanned = False       # True once at least one day has a valid scanned value

    for idx, date_iso in enumerate(dates_7):
        data_row_idx = idx + 1    # +1 because data[0] is the header

        if date_iso == report_date_iso:
            today_row_idx = data_row_idx

        # --- source: database (all days including today) ---
        if date_iso in db_rows:
            db_row      = db_rows[date_iso]
            scanned     = db_row["scanned"]     # int or None
            flagged     = db_row["flagged"]
            defect_rows = db_row["total_rows"]
            fpy_pct     = db_row["fpy_pct"]     # float or None
            dpb         = db_row["dpb"]         # float or None

            fpy_str     = f"{fpy_pct:.2f}%" if fpy_pct  is not None else "\u2014"
            dpb_str     = f"{dpb:.4f}"      if dpb      is not None else "\u2014"
            scanned_str = str(scanned)      if scanned  is not None else "\u2014"
            flagged_str = str(flagged)

            if scanned is not None:
                cum_scanned     += scanned
                cum_has_scanned  = True
            cum_flagged     += flagged
            cum_defect_rows += defect_rows

        # --- no data logged for this day ---
        else:
            fpy_str = dpb_str = scanned_str = flagged_str = "\u2014"

        # Display date as DD/MM
        disp_date = f"{date_iso[8:10]}/{date_iso[5:7]}"
        data.append([disp_date, fpy_str, dpb_str, scanned_str, flagged_str])

    # ------------------------------------------------------------------
    # Cumulative row
    # ------------------------------------------------------------------
    cum_fpy_str = (
        f"{(cum_scanned - cum_flagged) / cum_scanned * 100:.2f}%"
        if (cum_has_scanned and cum_scanned > 0)
        else "\u2014"
    )
    cum_dpb_str = (
        f"{cum_defect_rows / cum_flagged:.4f}"
        if cum_flagged > 0
        else "\u2014"
    )
    cum_scanned_str = str(cum_scanned) if cum_has_scanned else "\u2014"
    cum_row_idx = len(data)    # will be appended next
    data.append([
        "7-day total",
        cum_fpy_str,
        cum_dpb_str,
        cum_scanned_str,
        str(cum_flagged),
    ])

    # ------------------------------------------------------------------
    # Table style
    # ------------------------------------------------------------------
    style_cmds = [
        # Header
        ("BACKGROUND",    (0, 0),             (-1, 0),             _COL_HEADER_BG),
        ("FONTNAME",      (0, 0),             (-1, 0),             "Helvetica-Bold"),
        ("FONTCOLOR",     (0, 0),             (-1, 0),             _COL_HEADER_FG),
        # All cells
        ("FONTSIZE",      (0, 0),             (-1, -1),            10),
        ("GRID",          (0, 0),             (-1, -1),            0.5, _COL_GRID),
        ("ALIGN",         (0, 0),             (-1, -1),            "CENTER"),
        ("VALIGN",        (0, 0),             (-1, -1),            "MIDDLE"),
        ("LEFTPADDING",   (0, 0),             (-1, -1),            5),
        ("RIGHTPADDING",  (0, 0),             (-1, -1),            5),
        ("TOPPADDING",    (0, 0),             (-1, -1),            4),
        ("BOTTOMPADDING", (0, 0),             (-1, -1),            4),
        # Alternating data rows
        ("ROWBACKGROUNDS",(0, 1),             (-1, cum_row_idx-1), [colors.white, _COL_ROW_ALT]),
        # Cumulative row
        ("BACKGROUND",    (0, cum_row_idx),   (-1, cum_row_idx),   _COL_CUMUL_BG),
        ("FONTNAME",      (0, cum_row_idx),   (-1, cum_row_idx),   "Helvetica-Bold"),
    ]

    # Today highlight (applied after alternating so it overrides)
    if today_row_idx is not None:
        style_cmds += [
            ("BACKGROUND", (0, today_row_idx), (-1, today_row_idx), _COL_TODAY_BG),
            ("FONTNAME",   (0, today_row_idx), (-1, today_row_idx), "Helvetica-Bold"),
        ]

    t = Table(data, colWidths=_HIST_COLS)
    t.setStyle(TableStyle(style_cmds))
    return t


# ===========================================================================
# Card filter
# ===========================================================================

def _filter_by_card(df: pd.DataFrame, card_name: str) -> pd.DataFrame:
    jf   = _jobfile_series(df)
    mask = jf.apply(_extract_card_name) == card_name
    sub  = df[mask].copy()
    sub.attrs.update(df.attrs)
    return sub


# ===========================================================================
# Section builders
# ===========================================================================

def _build_combined_section(
    story: list,
    active_dfs: list,
    active_labels: list,
    report_date_iso: str,
):
    """All-lines combined summary section."""
    label = " + ".join(active_labels)
    story.append(Paragraph(label, _SECTION_STYLE))

    db_rows = _fetch_line_history("all", report_date_iso)
    story.append(_build_history_table(db_rows, report_date_iso))
    story.append(Spacer(1, 0.16 * inch))

    # Combined hourly chart
    ts_list, grain_list = [], []
    for df in active_dfs:
        try:
            ts_df, grain, _ = cards_scanned_over_time(df, force_7to7_when_hourly=True)
            ts_list.append(ts_df)
            grain_list.append(grain)
        except Exception:
            pass
    if ts_list:
        combined_ts = sum_time_series_dfs_on_time(ts_list)
        grain = "day" if "day" in grain_list else "hour"
        fig   = _make_hourly_fig_from_ts(
            combined_ts, grain,
            "ALL LINES \u2013 PCBs flagged per Hour (07:00 \u2192 07:00)",
        )
        story.append(_fig_to_rl_image(fig))
        story.append(Spacer(1, 0.10 * inch))

    # Combined defect pareto
    defect_dfs = [
        d for df in active_dfs
        for d in [top_defects(df, top_n=None)]
        if d is not None and not d.empty
    ]
    if defect_dfs:
        combined_defects = combine_defects_dfs(defect_dfs, top_n=20)
        fig = _make_defect_fig(combined_defects, "ALL LINES \u2013 Top defects (event rows)")
        story.append(_fig_to_rl_image(fig))
        story.append(Spacer(1, 0.14 * inch))


def _build_line_section(
    story: list,
    df: pd.DataFrame,
    line_label: str,
    report_date_iso: str,
):
    """One line's full report section."""
    # Map label → DB key
    _db_key = {"Line 1": "line1", "Line 2": "line2", "Line 4": "line4"}
    line_db_key = _db_key.get(line_label, line_label.lower().replace(" ", ""))

    # 1. Section header
    story.append(Paragraph(line_label, _SECTION_STYLE))

    # 2. 7-day history table (DB data for all days including today)
    db_rows = _fetch_line_history(line_db_key, report_date_iso)
    story.append(_build_history_table(db_rows, report_date_iso))
    story.append(Spacer(1, 0.14 * inch))

    # 3. Card count table
    card_counts  = pcbs_flagged_by_card(df)
    cards_ordered = card_counts["Card"].tolist() if not card_counts.empty else []
    if not card_counts.empty:
        story.append(_rl_card_table(card_counts))
        story.append(Spacer(1, 0.12 * inch))

    # 4. Overall line hourly chart (above card-wise)
    fig = _make_hourly_fig(
        df,
        f"{line_label} \u2013 PCBs flagged per Hour (07:00 \u2192 07:00)",
    )
    story.append(_fig_to_rl_image(fig))
    story.append(Spacer(1, 0.06 * inch))

    # 5. Per-card hourly charts
    for card_name in cards_ordered:
        card_df = _filter_by_card(df, card_name)
        if card_df.empty:
            continue
        fig = _make_hourly_fig(
            card_df,
            f"{card_name} \u2013 PCBs flagged per Hour (07:00 \u2192 07:00)",
        )
        story.append(_fig_to_rl_image(fig))
        story.append(Spacer(1, 0.06 * inch))

    story.append(Spacer(1, 0.08 * inch))

    # 6–8. Defects section
    defects_all = top_defects(df, top_n=None)
    if defects_all is None or defects_all.empty:
        story.append(Paragraph("Defects Data Unavailable", _UNAVAIL_STYLE))
        story.append(Spacer(1, 0.14 * inch))
    else:
        story.append(_rl_defect_table(defects_all))
        story.append(Spacer(1, 0.12 * inch))

        # Overall line defect chart (above card-wise)
        defects_top20 = top_defects(df, top_n=20)
        if defects_top20 is not None and not defects_top20.empty:
            fig = _make_defect_fig(defects_top20, f"{line_label} \u2013 Top defects (event rows)")
            story.append(_fig_to_rl_image(fig))
            story.append(Spacer(1, 0.06 * inch))

        # Per-card defect charts
        for card_name in cards_ordered:
            card_df      = _filter_by_card(df, card_name)
            card_defects = top_defects(card_df, top_n=20)
            if card_df.empty or card_defects is None or card_defects.empty:
                continue
            fig = _make_defect_fig(card_defects, f"{card_name} \u2013 Top defects (event rows)")
            story.append(_fig_to_rl_image(fig))
            story.append(Spacer(1, 0.06 * inch))

        story.append(Spacer(1, 0.08 * inch))


# ===========================================================================
# Public API
# ===========================================================================

def generate_pdf(df1, df2, df4, date_str: str, output_path: str):
    """Generate the AOI PDF report.

    Args:
        df1, df2, df4:  Cleaned DataFrames (None to skip a line).
        date_str:       Report date in "DD/MM/YYYY" format.
        output_path:    Destination .pdf file path.
    """
    # Convert user-facing date to ISO for DB queries
    try:
        report_date_iso = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        report_date_iso = date_type.today().isoformat()

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=_MARGIN,
        rightMargin=_MARGIN,
        topMargin=_MARGIN,
        bottomMargin=_MARGIN + 0.2 * inch,
    )

    story = []

    # Title
    story.append(Paragraph("AOI REPORT", _TITLE_STYLE))
    story.append(Paragraph(date_str,     _TITLE_STYLE))
    story.append(Spacer(1, 0.25 * inch))

    # Collect non-empty lines
    line_pairs = [(df1, "Line 1"), (df2, "Line 2"), (df4, "Line 4")]
    active     = [(df, lbl) for df, lbl in line_pairs if df is not None and not df.empty]

    # Combined section (only when ≥2 lines present)
    if len(active) > 1:
        active_dfs, active_labels = zip(*active)
        _build_combined_section(
            story, list(active_dfs), list(active_labels), report_date_iso
        )

    # Per-line sections
    for df, label in active:
        _build_line_section(story, df, label, report_date_iso)

    doc.build(
        story,
        onFirstPage=_draw_footer,
        onLaterPages=_draw_footer,
    )
