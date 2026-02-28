# src/plots.py
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

# ---------------------------------------------------------------------------
# Module-level plot constants — single source of truth for all chart styling
# ---------------------------------------------------------------------------
_FIG_W       = 13.0
_FIG_H       = 5.8
_FIG_H_BLANK = 5.4   # used for empty/error figures
_FIG_H_TREND = 5.2
_DPI         = 260
_MARGINS      = dict(left=0.08, right=0.98, bottom=0.22, top=0.90)
_MARGINS_BLANK = dict(left=0.08, right=0.98, bottom=0.18, top=0.90)
_SAVE_KW     = dict(dpi=_DPI)  # exact figsize margins — no tight-crop distortion (keeps click mapping accurate)


def _apply_margins(fig, margins: dict):
    try:
        fig.subplots_adjust(**margins)
    except (ValueError, TypeError):
        pass


def _label_bars(ax, bars, counts, pad, fontsize=14):
    """Draw count labels above each bar."""
    for b, v in zip(bars, counts):
        ax.text(
            b.get_x() + b.get_width() / 2,
            float(v) + pad,
            f"{v}",
            ha="center", va="bottom",
            fontsize=fontsize, fontweight="bold",
            clip_on=True,
        )


def _annotate_trend_bar(ax, i, bar, flg: int, pad: float, is_small: bool):
    """Label a single trend bar — above if small, centred inside if large."""
    if flg <= 0:
        ax.text(i, pad, "0",
                ha="center", va="bottom",
                fontsize=12, fontweight="bold",
                color="black", zorder=3, clip_on=True)
    elif is_small:
        ax.text(bar.get_x() + bar.get_width() / 2, flg + pad,
                str(int(flg)),
                ha="center", va="bottom",
                fontsize=12, fontweight="bold",
                color="black", zorder=3, clip_on=True)
    else:
        ax.text(bar.get_x() + bar.get_width() / 2, flg / 2,
                str(int(flg)),
                ha="center", va="center",
                fontsize=13, fontweight="bold",
                color="white", zorder=3)


def _save_or_blank(fig, out_path: str, title: str, msg: str = "No data"):
    ax = fig.gca()
    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.text(0.5, 0.5, msg, ha="center", va="center", fontsize=16)
    ax.axis("off")
    _apply_margins(fig, _MARGINS_BLANK)
    fig.savefig(out_path, **_SAVE_KW)
    plt.close(fig)


def plot_top_defects_bars(defects_df, out_path, title="Top defect types flagged by AOI (event rows)"):
    if defects_df is None or defects_df.empty:
        fig = plt.figure(figsize=(_FIG_W, _FIG_H_BLANK), dpi=_DPI)
        _save_or_blank(fig, out_path, title)
        return

    if not {"Defect", "Count"}.issubset(set(defects_df.columns)):
        fig = plt.figure(figsize=(_FIG_W, _FIG_H_BLANK), dpi=_DPI)
        _save_or_blank(fig, out_path, title, msg="Bad data format")
        return

    labels = defects_df["Defect"].astype(str).tolist()
    counts = defects_df["Count"].astype(int).tolist()

    fig = plt.figure(figsize=(_FIG_W, _FIG_H), dpi=_DPI)
    ax = plt.gca()

    bars = ax.bar(range(len(labels)), counts)
    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.set_ylabel("Event rows", fontsize=13)
    ax.set_xlabel("Defect type", fontsize=13)

    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=40, ha="right", fontsize=11)

    ymax = float(max(counts)) if counts else 0.0
    ax.set_ylim(0, max(1.0, ymax * 1.35 + 0.5))
    pad = max(0.05 * ymax, 0.12) if ymax > 0 else 0.12

    _label_bars(ax, bars, counts, pad, fontsize=14)

    ax.margins(y=0.20)
    _apply_margins(fig, _MARGINS)
    fig.savefig(out_path, **_SAVE_KW)
    plt.close(fig)


def plot_time_series_counts_bar(
    ts_df,
    out_path,
    title="PCBs flagged over time",
    y_label="Unique PCBIDs flagged",
    grain="hour"
):
    if ts_df is None or ts_df.empty:
        fig = plt.figure(figsize=(_FIG_W, _FIG_H_BLANK), dpi=_DPI)
        _save_or_blank(fig, out_path, title)
        return

    if not {"TimeTS", "Count"}.issubset(set(ts_df.columns)):
        fig = plt.figure(figsize=(_FIG_W, _FIG_H_BLANK), dpi=_DPI)
        _save_or_blank(fig, out_path, title, msg="Bad data format")
        return

    times = ts_df["TimeTS"].tolist()
    counts = ts_df["Count"].astype(int).tolist()

    if grain == "hour":
        xlabels = [t.strftime("%d-%b %H:%M") if hasattr(t, "strftime") else str(t) for t in times]
        xlabel = "Hour"
    else:
        xlabels = [t.strftime("%d-%b") if hasattr(t, "strftime") else str(t) for t in times]
        xlabel = "Day"

    fig = plt.figure(figsize=(_FIG_W, _FIG_H), dpi=_DPI)
    ax = plt.gca()

    bars = ax.bar(range(len(xlabels)), counts)
    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.set_ylabel(y_label, fontsize=13)
    ax.set_xlabel(xlabel, fontsize=13)

    ax.set_xticks(range(len(xlabels)))

    # keep readable: show fewer labels if too many
    if len(xlabels) <= 28:
        ax.set_xticklabels(xlabels, rotation=40, ha="right", fontsize=11)
    else:
        step = max(1, len(xlabels) // 18)
        shown = [lbl if (i % step == 0) else "" for i, lbl in enumerate(xlabels)]
        ax.set_xticklabels(shown, rotation=40, ha="right", fontsize=11)

    ymax = float(max(counts)) if counts else 0.0
    ax.set_ylim(0, max(1.0, ymax * 1.35 + 0.5))
    pad = max(0.05 * ymax, 0.12) if ymax > 0 else 0.12

    _label_bars(ax, bars, counts, pad, fontsize=14)

    ax.margins(y=0.20)
    _apply_margins(fig, _MARGINS)
    fig.savefig(out_path, **_SAVE_KW)
    plt.close(fig)


def plot_pcbs_flagged_by_minute(
    ts_df: pd.DataFrame,
    out_path: str,
    title: str = "PCBs flagged per Minute",
    y_label: str = "Unique PCBIDs flagged"
):
    """
    Strict 0–60 minute axis:
      - Always spans 60 minutes (00..59).
      - Bars are drawn only where Count > 0 (zero minutes show no bar).
    """
    if ts_df is None or ts_df.empty:
        fig = plt.figure(figsize=(_FIG_W, _FIG_H_BLANK), dpi=_DPI)
        _save_or_blank(fig, out_path, title, msg="No data in this hour")
        return

    if not {"TimeTS", "Count"}.issubset(set(ts_df.columns)):
        fig = plt.figure(figsize=(_FIG_W, _FIG_H_BLANK), dpi=_DPI)
        _save_or_blank(fig, out_path, title, msg="Bad data format")
        return

    d = ts_df.copy()
    d["Count"] = pd.to_numeric(d["Count"], errors="coerce").fillna(0).astype(int)
    d = d.sort_values("TimeTS")

    times = d["TimeTS"].tolist()
    if not times:
        fig = plt.figure(figsize=(_FIG_W, _FIG_H_BLANK), dpi=_DPI)
        _save_or_blank(fig, out_path, title, msg="No data")
        return

    start = pd.Timestamp(times[0]).floor("h")
    d["m"] = (pd.to_datetime(d["TimeTS"]) - start).dt.total_seconds().div(60).astype(int)

    # keep only 0..59
    d = d[(d["m"] >= 0) & (d["m"] <= 59)].copy()

    fig = plt.figure(figsize=(_FIG_W, _FIG_H), dpi=_DPI)
    ax = plt.gca()

    # draw only nonzero bars
    dnz = d[d["Count"] > 0]
    ax.bar(dnz["m"].tolist(), dnz["Count"].tolist())

    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.set_ylabel(y_label, fontsize=13)
    ax.set_xlabel("Minute (0–60)", fontsize=13)

    ax.set_xlim(-0.5, 59.5)

    # ticks every 5 minutes
    ticks = list(range(0, 60, 5))
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{t:02d}" for t in ticks], fontsize=10)

    ymax = float(d["Count"].max()) if len(d) else 0.0
    ax.set_ylim(0, max(1.0, ymax * 1.35 + 0.5))
    pad = max(0.05 * ymax, 0.12) if ymax > 0 else 0.12

    # labels only for nonzero bars (x is minute index, not bar object)
    for m, v in zip(dnz["m"].tolist(), dnz["Count"].tolist()):
        ax.text(
            m, float(v) + pad, f"{int(v)}",
            ha="center", va="bottom",
            fontsize=12, fontweight="bold",
            clip_on=True,
        )

    ax.margins(y=0.20)
    _apply_margins(fig, dict(left=0.08, right=0.98, bottom=0.18, top=0.90))
    fig.savefig(out_path, **_SAVE_KW)
    plt.close(fig)


def plot_pcbs_flagged_trend(df: pd.DataFrame, out_path: str, title: str = "PCBs flagged trend"):
    fig = plt.figure(figsize=(_FIG_W, _FIG_H_TREND), dpi=_DPI)
    ax = fig.add_subplot(111)

    if df is None or df.empty:
        _save_or_blank(fig, out_path, title, msg="No logged data in this range")
        return

    if not {"log_date", "pcbs_flagged"}.issubset(set(df.columns)):
        _save_or_blank(fig, out_path, title, msg="Bad logged data format")
        return

    d = df.copy()
    d["log_date"] = pd.to_datetime(d["log_date"], errors="coerce").dt.date
    d = d.dropna(subset=["log_date"]).copy()
    if d.empty:
        _save_or_blank(fig, out_path, title, msg="No valid dates")
        return

    d["pcbs_flagged"] = pd.to_numeric(d["pcbs_flagged"], errors="coerce").fillna(0).astype(int)

    has_checked = "pcbs_checked" in d.columns
    if has_checked:
        d["pcbs_checked"] = pd.to_numeric(d["pcbs_checked"], errors="coerce")
        d.loc[d["pcbs_checked"].isna(), "pcbs_checked"] = pd.NA

    d = d.sort_values("log_date")

    xlabels = [pd.to_datetime(x).strftime("%d/%m") for x in d["log_date"].tolist()]
    y_flagged = d["pcbs_flagged"].tolist()
    x = list(range(len(xlabels)))

    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.set_ylabel("PCBs", fontsize=13)
    ax.set_xlabel("Day", fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels(xlabels, rotation=40, ha="right", fontsize=11)
    ax.tick_params(axis="y", labelsize=11)

    ymax = max(y_flagged) if y_flagged else 0
    SMALL_FRAC = 0.12
    small_thresh = max(1, int(ymax * SMALL_FRAC)) if ymax else 1

    if has_checked:
        y_checked = []
        for v in d["pcbs_checked"].tolist():
            y_checked.append(None if pd.isna(v) else int(v))

        any_red = False
        for chk, flg in zip(y_checked, y_flagged):
            if chk is None:
                continue
            ymax = max(ymax, chk)
            if chk > flg:
                any_red = True

        small_thresh = max(1, int(ymax * SMALL_FRAC)) if ymax else 1
        pad = max(0.05 * (ymax if ymax else 1), 0.12)

        if any_red:
            ax.bar(x, [0 if v is None else v for v in y_checked],
                   color="red", alpha=0.35, zorder=1, label="Checked")

        bars_blue = ax.bar(x, y_flagged, color="blue", alpha=0.90, zorder=2, label="Flagged")

        for i, (bar, flg) in enumerate(zip(bars_blue, y_flagged)):
            _annotate_trend_bar(ax, i, bar, flg, pad, flg < small_thresh)

        for i, (chk, flg) in enumerate(zip(y_checked, y_flagged)):
            if chk is None or chk <= flg:
                continue
            if flg < small_thresh:
                ax.text(i, chk + pad, f"{int(flg)}/{int(chk)}",
                        ha="center", va="bottom",
                        fontsize=12, fontweight="bold",
                        color="black", zorder=3, clip_on=True)
            else:
                ax.text(i, chk + pad, str(int(chk)),
                        ha="center", va="bottom",
                        fontsize=12, fontweight="bold",
                        color="black", zorder=3, clip_on=True)

        if any_red:
            ax.legend(loc="upper left", fontsize=10)

    else:
        bars = ax.bar(x, y_flagged, color="blue", alpha=0.90, zorder=2)
        ymax = max(y_flagged) if y_flagged else 0
        small_thresh = max(1, int(ymax * SMALL_FRAC)) if ymax else 1
        pad = max(0.05 * (ymax if ymax else 1), 0.12)

        for i, (bar, flg) in enumerate(zip(bars, y_flagged)):
            _annotate_trend_bar(ax, i, bar, flg, pad, flg < small_thresh)

    ax.margins(y=0.20)
    _apply_margins(fig, _MARGINS)
    fig.savefig(out_path, **_SAVE_KW)
    plt.close(fig)
