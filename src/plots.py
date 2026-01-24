# src/plots.py
import matplotlib
matplotlib.use("Agg")  # ✅ important: avoids GUI/thread issues

import matplotlib.pyplot as plt
import pandas as pd


def _save_or_blank(fig, out_path: str, title: str, msg: str = "No data"):
    ax = fig.gca()
    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.text(0.5, 0.5, msg, ha="center", va="center", fontsize=16)
    ax.axis("off")
    # ✅ avoid tight_layout warnings
    try:
        fig.subplots_adjust(left=0.08, right=0.98, bottom=0.18, top=0.90)
    except Exception:
        pass
    fig.savefig(out_path, dpi=260, bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)


def plot_top_defects_bars(defects_df, out_path, title="Top defect types flagged by AOI (event rows)"):
    """
    defects_df columns expected: Defect, Count
    High-quality PNG + larger labels/values.
    """
    if defects_df is None or defects_df.empty:
        fig = plt.figure(figsize=(12.5, 5.6), dpi=260)
        _save_or_blank(fig, out_path, title)
        return

    if not {"Defect", "Count"}.issubset(set(defects_df.columns)):
        fig = plt.figure(figsize=(12.5, 5.6), dpi=260)
        _save_or_blank(fig, out_path, title, msg="Bad data format")
        return

    labels = defects_df["Defect"].astype(str).tolist()
    counts = defects_df["Count"].astype(int).tolist()

    fig = plt.figure(figsize=(13.0, 5.8), dpi=260)
    ax = plt.gca()

    bars = ax.bar(range(len(labels)), counts)
    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.set_ylabel("Event rows", fontsize=13)
    ax.set_xlabel("Defect type", fontsize=13)

    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=40, ha="right", fontsize=11)

    ymax = max(counts) if counts else 0
    pad = max(2, int(ymax * 0.02))

    # ✅ bigger numbers on bars
    for b, v in zip(bars, counts):
        ax.text(
            b.get_x() + b.get_width() / 2,
            v + pad,
            f"{v}",
            ha="center",
            va="bottom",
            fontsize=14,
            fontweight="bold"
        )

    ax.margins(y=0.20)
    # ✅ avoid tight_layout warnings
    try:
        fig.subplots_adjust(left=0.08, right=0.98, bottom=0.22, top=0.90)
    except Exception:
        pass
    fig.savefig(out_path, dpi=260, bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)


def plot_time_series_counts_bar(
    ts_df,
    out_path,
    title="PCBs flagged over time",
    y_label="Unique PCBIDs flagged",
    grain="hour"
):
    """
    ts_df columns expected: TimeTS, Count
    High-quality PNG + larger labels/values.
    """
    if ts_df is None or ts_df.empty:
        fig = plt.figure(figsize=(12.5, 5.4), dpi=260)
        _save_or_blank(fig, out_path, title)
        return

    if not {"TimeTS", "Count"}.issubset(set(ts_df.columns)):
        fig = plt.figure(figsize=(12.5, 5.4), dpi=260)
        _save_or_blank(fig, out_path, title, msg="Bad data format")
        return

    times = ts_df["TimeTS"].tolist()
    counts = ts_df["Count"].astype(int).tolist()

    # readable x labels
    if grain == "hour":
        xlabels = [t.strftime("%d-%b %H:%M") if hasattr(t, "strftime") else str(t) for t in times]
        xlabel = "Hour"
    else:
        xlabels = [t.strftime("%d-%b") if hasattr(t, "strftime") else str(t) for t in times]
        xlabel = "Day"

    fig = plt.figure(figsize=(13.0, 5.8), dpi=260)
    ax = plt.gca()

    bars = ax.bar(range(len(xlabels)), counts)
    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.set_ylabel(y_label, fontsize=13)
    ax.set_xlabel(xlabel, fontsize=13)

    ax.set_xticks(range(len(xlabels)))
    ax.set_xticklabels(xlabels, rotation=40, ha="right", fontsize=11)

    ymax = max(counts) if counts else 0

    # ✅ add headroom (helps tiny-bar days + labels)
    ax.set_ylim(0, max(1, int(ymax * 1.25) + 1))

    pad = max(2, int((ymax if ymax else 1) * 0.02))

    for b, v in zip(bars, counts):
        ax.text(
            b.get_x() + b.get_width() / 2,
            v + pad,
            f"{v}",
            ha="center",
            va="bottom",
            fontsize=14,
            fontweight="bold"
        )

    ax.margins(y=0.20)

    # ✅ avoid tight_layout warnings (especially hourly view)
    try:
        fig.subplots_adjust(left=0.08, right=0.98, bottom=0.22, top=0.90)
    except Exception:
        pass
    fig.savefig(out_path, dpi=260, bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)


def plot_pcbs_flagged_trend(df: pd.DataFrame, out_path: str, title: str = "PCBs flagged trend"):
    """
    df columns required: log_date (YYYY-MM-DD), pcbs_flagged (int)
    Optional: pcbs_checked (int)  -> plotted in RED behind BLUE flagged bars

    ✅ For small blue bars: move label above bar
    ✅ If small day AND checked > flagged: show "flagged/checked" above (single label)
    ✅ Avoid label overlaps
    """
    fig = plt.figure(figsize=(13.0, 5.2), dpi=260)
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

    # threshold to decide "small bar" (relative)
    # if bar height < 12% of ymax, treat as small
    SMALL_FRAC = 0.12
    small_thresh = max(1, int(ymax * SMALL_FRAC)) if ymax else 1

    def _is_small(v: int) -> bool:
        return v < small_thresh

    # ---- Checked (red behind) + Flagged (blue front) ----
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

        # recompute after ymax updated
        small_thresh = max(1, int(ymax * SMALL_FRAC)) if ymax else 1

        if any_red:
            ax.bar(
                x,
                [0 if v is None else v for v in y_checked],
                color="red",
                alpha=0.35,
                zorder=1,
                label="Checked"
            )

        bars_blue = ax.bar(x, y_flagged, color="blue", alpha=0.90, zorder=2, label="Flagged")

        pad = max(2, int((ymax if ymax else 1) * 0.02))

        # 1) Flagged labels: inside if big, above if small
        for i, (bar, flg) in enumerate(zip(bars_blue, y_flagged)):
            if flg <= 0:
                ax.text(i, 0 + pad, "0",
                        ha="center", va="bottom",
                        fontsize=12, fontweight="bold",
                        color="black", zorder=3)
                continue

            if _is_small(flg):
                # put above blue bar for small bars
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    flg + pad,
                    str(int(flg)),
                    ha="center", va="bottom",
                    fontsize=12, fontweight="bold",
                    color="black",
                    zorder=3
                )
            else:
                # inside blue bar
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    flg / 2,
                    str(int(flg)),
                    ha="center", va="center",
                    fontsize=13, fontweight="bold",
                    color="white",
                    zorder=3
                )

        # 2) Checked labels: only when checked > flagged
        #    If flagged bar is SMALL on that day, show combined "flagged/checked" above (single label)
        for i, (chk, flg) in enumerate(zip(y_checked, y_flagged)):
            if chk is None:
                continue
            if chk > flg:
                if _is_small(flg):
                    # combined label above the red bar
                    ax.text(
                        i,
                        chk + pad,
                        f"{int(flg)}/{int(chk)}",
                        ha="center", va="bottom",
                        fontsize=12, fontweight="bold",
                        color="black",
                        zorder=3
                    )
                else:
                    # normal: show checked above red
                    ax.text(
                        i,
                        chk + pad,
                        str(int(chk)),
                        ha="center", va="bottom",
                        fontsize=12, fontweight="bold",
                        color="black",
                        zorder=3
                    )

        if any_red:
            ax.legend(loc="upper left", fontsize=10)

    # ---- Only flagged (blue) ----
    else:
        bars = ax.bar(x, y_flagged, color="blue", alpha=0.90, zorder=2)
        ymax = max(y_flagged) if y_flagged else 0
        small_thresh = max(1, int(ymax * SMALL_FRAC)) if ymax else 1
        pad = max(2, int((ymax if ymax else 1) * 0.02))

        for i, (bar, flg) in enumerate(zip(bars, y_flagged)):
            if flg <= 0:
                ax.text(i, 0 + pad, "0",
                        ha="center", va="bottom",
                        fontsize=12, fontweight="bold",
                        color="black", zorder=3)
                continue

            if _is_small(flg):
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    flg + pad,
                    str(int(flg)),
                    ha="center", va="bottom",
                    fontsize=12, fontweight="bold",
                    color="black",
                    zorder=3
                )
            else:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    flg / 2,
                    str(int(flg)),
                    ha="center", va="center",
                    fontsize=13, fontweight="bold",
                    color="white",
                    zorder=3
                )

    ax.margins(y=0.20)

    # ✅ avoid tight_layout warnings
    try:
        fig.subplots_adjust(left=0.08, right=0.98, bottom=0.22, top=0.90)
    except Exception:
        pass
    fig.savefig(out_path, dpi=260, bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)
