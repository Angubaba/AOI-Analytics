# src/chatbot.py
"""
AOI Chatbot — Rule-Based Engine

Detects query intent from natural language, extracts entities (line, card,
date, defect), queries aoi_chatbot.db / aoi_logs.db, and returns a
plain-text response.  No AI model, no internet, no extra dependencies.
"""

import re
import sqlite3
from datetime import date, datetime, timedelta
from calendar import monthrange

import src.chatbot_db as cdb

# ---------------------------------------------------------------------------
# Intent patterns  (checked in order — first match wins)
# ---------------------------------------------------------------------------

_INTENTS = [
    # ── meta ──────────────────────────────────────────────────────────────
    ("help",           r"\bhelp\b|what can you|what do you know|example questions?|"
                       r"commands?|how to use|what do you do|instructions|"
                       r"show examples?|what queries|what can i ask|"
                       r"available queries|list commands?|show commands?|"
                       r"how does this work|what.{0,10}ask"),

    # ── list all / enumerate ──────────────────────────────────────────────
    ("list_defects",   r"list.{0,10}(?:all )?defects?|show.{0,10}(?:all )?defect|"
                       r"all defect (?:types?|names?|kinds?)|defect (?:types?|names?)\b|"
                       r"what defects? (?:are there|exist|do you have)|available defects?|"
                       r"enumerate defects?|defects? available"),

    ("list_cards",     r"list.{0,10}(?:all )?cards?|show.{0,10}(?:all )?cards?|"
                       r"all card names?|card names?\b|"
                       r"what cards? (?:are there|exist|do you have)|available cards?|"
                       r"enumerate cards?|show me (?:all )?cards?"),

    # ── defect-centric ────────────────────────────────────────────────────
    ("defect_info",    r"tell me about|which cards? (?:has|have|with).{0,25}defect|"
                       r"where (?:does|do).{0,20}occur|defect.{0,15}info|"
                       r"info (?:on|about).{0,20}defect|about the defect|"
                       r"defect.{0,10}where|which.{0,10}card.{0,10}(?:has|have|shows?)|"
                       r"details? (?:on|about|for).{0,20}defect|"
                       r"explain.{0,15}defect|describe.{0,15}defect"),

    ("top_defects",    r"top defects?|most common defects?|defect types?|"
                       r"defect breakdown|worst defects?|frequent defects?|"
                       r"all defects?|list (?:all )?defects?|"
                       r"defects? (?:yesterday|today|for|on|last|this|in)|"
                       r"defect summary|defect data|defect report|"
                       r"common issues?|what.{0,15}failing|what.{0,15}wrong|"
                       r"failure modes?|fault types?|error types?|reject types?|"
                       r"most.{0,10}fails?|issues? (?:with|on)|"
                       r"problems? (?:with|on)|what defects?|show defects?|"
                       r"defect list|rejects? on|failing on|errors? on|"
                       r"what.{0,10}going wrong|causes? of|defect (?:count|report|rate)|"
                       r"which defects?|recurring defects?|repeat defects?|"
                       r"failing components?|what.{0,10}issues?"),

    # ── card-centric ──────────────────────────────────────────────────────
    ("best_card",      r"best card|fewest defects?|least defects?|lowest defects?|"
                       r"best performing card|cleanest card|least.{0,10}flagged|"
                       r"least.{0,10}fail|lowest.{0,10}flag|top performing|"
                       r"most reliable card|highest quality card|"
                       r"lowest reject.{0,10}card|best board type|"
                       r"card.{0,10}best|card.{0,10}lowest"),

    # ── per-day card rankings ─────────────────────────────────────────────
    ("daily_top_cards", r"top\s*\d*\s*cards?.{0,30}(?:per day|each day|every day|by day)|"
                        r"(?:per day|each day|every day|by day).{0,30}cards?|"
                        r"daily.{0,15}card.{0,10}(?:rank|top|breakdown|summary)|"
                        r"card.{0,15}rank(?:ing)?.{0,15}(?:per day|each day|by day|daily)|"
                        r"which cards?.{0,10}(?:each|every|per) day"),

    ("card_breakdown", r"worst card|which card|by card|per card|card breakdown|"
                       r"card comparison|card ranking|most defects|card performance|"
                       r"compare cards?|card stats?|card issues?|card flags?|"
                       r"problematic card|all cards|each card|cards? on line|"
                       r"card.{0,10}fail|card.{0,10}reject|card.{0,10}flagged|"
                       r"card summary|card list|card overview|card results?|"
                       r"board type.{0,15}breakdown|by board type|per board type|"
                       r"which boards?.{0,15}failing|cards?.{0,10}most.{0,10}issue|"
                       r"card defect summary|rank.{0,10}cards?|card.{0,10}rank|"
                       r"top\s+\d+\s+(?:cards?|boards?)"),

    # ── card-specific stats ───────────────────────────────────────────────
    ("card_stats",     r"\bstats?\b.{0,25}(?:for|of|about|on)\b|"
                       r"(?:for|about|on).{0,25}\bstats?\b|"
                       r"card.{0,20}(?:detail|metric|number|data)\b|"
                       r"(?:detail|metric|number|data).{0,20}(?:for|of|about)\b|"
                       r"performance (?:for|of|on)\b"),

    # ── volume / throughput ───────────────────────────────────────────────
    ("scanned_count",  r"how many.{0,20}(?:scanned|checked|inspected|boards?|pcbs?|run)|"
                       r"boards? (?:scanned|checked|inspected|produced|run)|"
                       r"pcbs? (?:scanned|checked|inspected)|"
                       r"cards? (?:scanned|checked|inspected|produced|run)|"
                       r"total.{0,15}(?:scanned|checked|inspected|boards?|pcbs?|cards?|units?)|"
                       r"throughput|production (?:count|volume|output|number)|"
                       r"how much.{0,10}produc|boards? (?:today|yesterday|this week|last week)|"
                       r"units? (?:produced|inspected|scanned|run|checked)|"
                       r"(?:number|count) of (?:boards?|pcbs?|cards?|units?)|"
                       r"\boutput count\b|\boutput volume\b|volume (?:scanned|produced|inspected)|"
                       r"how much.{0,10}(?:scanned|run|produced)|"
                       r"total.{0,10}output|production numbers?|boards? per day|"
                       r"\bproduction\b"),

    # ── FPY / quality ─────────────────────────────────────────────────────
    ("fpy_trend",      r"\bfpy\b|first[- ]pass yield|pass rate|\byield\b|"
                       r"line.{0,15}efficiency|quality rate|"
                       r"how.{0,10}(?:good|well).{0,10}line|"
                       r"line.{0,10}perform|defect rate|reject rate|\bquality\b|"
                       r"failure rate|rejection rate|pass percentage|fail percentage|"
                       r"\bdpb\b|\bdpu\b|\bppm\b|defects? per (?:board|unit|pcb)|"
                       r"defects?/(?:board|unit|pcb)|quality (?:score|metric|number)|"
                       r"line quality|quality performance|"
                       r"what.{0,10}reject rate|what.{0,10}pass rate|"
                       r"efficiency|(?:pass|fail) ratio"),

    # ── flagged count ──────────────────────────────────────────────────────
    ("flagged_count",  r"how many.{0,25}(?:flagged|rejected|failed?|fail)|"
                       r"flagged pcbs?|rejected boards?|total flagged|pcbs? flagged|"
                       r"total.{0,10}rejects?|rejection count|fail count|failure count|"
                       r"boards? (?:failed?|rejected)|rejects? (?:today|yesterday|this|last)|"
                       r"failures? (?:today|yesterday|this|last)|"
                       r"how many.{0,10}(?:issues?|problems?|errors?)|"
                       r"number of (?:rejects?|failures?|failed|flagged)|"
                       r"(?:bad|defective|failed?) boards?|"
                       r"total.{0,10}failures?|count of (?:rejects?|failures?)|"
                       r"flagged (?:today|yesterday|this|last)|"
                       r"rejected (?:today|yesterday|this|last)|"
                       r"flagged (?:per day|each day|every day|by day|daily)|"
                       r"(?:per day|each day|by day|daily).{0,15}flagged|"
                       r"daily.{0,10}(?:flagged|rejects?|failures?)"),

    # ── summary / report ──────────────────────────────────────────────────
    ("daily_summary",  r"summary|overview|how did|how was|what happened|"
                       r"production report|shift report|daily report|line status|"
                       r"shift summary|what.{0,10}produc|how.{0,10}line.{0,10}doing|"
                       r"line.{0,10}status|today.{0,20}(?:line|prod|result)|"
                       r"yesterday.{0,20}(?:line|prod|result)|report for|"
                       r"today.{0,10}(?:number|stat|data|count|result)|"
                       r"(?:number|stat|data|result).{0,10}today|"
                       r"production status|status update|line update|"
                       r"daily (?:number|stat|data|result|update)|"
                       r"shift (?:number|stat|data|result)|"
                       r"inspection results?|what.{0,10}count|"
                       r"how.{0,10}(?:line|production).{0,10}(?:going|doing)|"
                       r"(?:line|production).{0,10}update|"
                       r"numbers? for (?:today|yesterday)|"
                       r"production overview|line overview|"
                       r"what.{0,10}status|current (?:status|output|numbers?)"),

    # ── trend / history ───────────────────────────────────────────────────
    ("trend",          r"trend|over time|history|by day|daily counts?|"
                       r"week.{0,10}count|month.{0,10}count|production trend|"
                       r"flagged.{0,10}trend|scanned.{0,10}trend|"
                       r"weekly|monthly|show.*over|"
                       r"day by day|daily breakdown|historical|"
                       r"over (?:the )?(?:past|last)|day.{0,5}to.{0,5}day|"
                       r"week.{0,5}to.{0,5}week|daily stats?|weekly stats?|monthly stats?|"
                       r"output trend|rejection trend|pass.{0,5}trend|"
                       r"daily (?:chart|graph|plot)|production over|"
                       r"flagged over|scanned over|output over|"
                       r"boards? per day|units? per day|"
                       r"defect.{0,5}(?:trend|history|over)|"
                       r"(?:past|last).{0,5}\d+.{0,5}(?:days?|weeks?|months?).*(?:trend|history|data)"),

    ("unknown",        r""),   # catch-all
]


def _detect_intent(q: str) -> str:
    q_lower = q.lower()
    for intent, pattern in _INTENTS:
        if pattern and re.search(pattern, q_lower):
            return intent
    return "unknown"


# ---------------------------------------------------------------------------
# Entity extraction
# ---------------------------------------------------------------------------

_LINE_RE    = re.compile(r"\bline\s*([124])\b|\bl([124])\b", re.IGNORECASE)
_DPB_RE     = re.compile(
    r"\bdpb\b|\bdpu\b|\bppm\b|defects?\s*/\s*(?:board|unit|pcb)|"
    r"defects?\s+per\s+(?:board|unit|pcb)",
    re.IGNORECASE,
)
_NDAYS_RE   = re.compile(r"(?:last|past)\s+(\d+)\s+days?", re.IGNORECASE)
_NWEEKS_RE  = re.compile(r"(?:last|past)\s+(\d+)\s+weeks?", re.IGNORECASE)
_NMONTHS_RE = re.compile(r"(?:last|past)\s+(\d+)\s+months?", re.IGNORECASE)
_TOPN_RE    = re.compile(r"\btop\s*(\d+)\b", re.IGNORECASE)
# DD/MM/YYYY  or  DD-MM-YYYY
_DATE_DMY_RE = re.compile(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b")
# YYYY-MM-DD
_DATE_ISO_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")

_MONTH_NAMES = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6,
    "july": 7, "jul": 7, "august": 8, "aug": 8, "september": 9, "sep": 9,
    "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
}

# "3 march", "3march", "march 3", "3 march 2025", "march 3 2025"
_MON_PAT = "|".join(_MONTH_NAMES)   # shared pattern piece (longest names first)
_DATE_DM_RE = re.compile(
    r"\b(\d{1,2})\s*(" + _MON_PAT + r")(?:\s+(\d{4}))?\b",
    re.IGNORECASE,
)
_DATE_MD_RE = re.compile(
    r"\b(" + _MON_PAT + r")\s+(\d{1,2})(?:\s+(\d{4}))?\b",
    re.IGNORECASE,
)
# "from 1feb to 28feb", "from 1 feb to 28 feb 2026", etc.
_RANGE_DM_RE = re.compile(
    r"\bfrom\s+(\d{1,2})\s*(" + _MON_PAT + r")(?:\s+(\d{4}))?"
    r"\s+to\s+(\d{1,2})\s*(" + _MON_PAT + r")(?:\s+(\d{4}))?",
    re.IGNORECASE,
)


def _extract_line(q: str):
    """Returns "line1" | "line2" | "line4" | None."""
    m = _LINE_RE.search(q)
    if m:
        n = m.group(1) or m.group(2)
        return f"line{n}"
    return None


def _extract_date_range(q: str):
    """
    Returns (start_iso, end_iso) strings or (None, None).
    Covers: today, yesterday, last/past N days/weeks/months,
    this/last week/month/year, named months, DD/MM/YYYY, YYYY-MM-DD,
    "3 march [year]", "march 3 [year]".
    """
    q_lower = q.lower()
    today = date.today()

    # ── single named days ────────────────────────────────────────────────
    if "today" in q_lower:
        d = today.isoformat()
        return d, d

    if "yesterday" in q_lower:
        d = (today - timedelta(days=1)).isoformat()
        return d, d

    # ── explicit "from D1mon to D2mon" range ──────────────────────────────
    m = _RANGE_DM_RE.search(q_lower)
    if m:
        try:
            year1 = int(m.group(3)) if m.group(3) else today.year
            year2 = int(m.group(6)) if m.group(6) else today.year
            d1 = date(year1, _MONTH_NAMES[m.group(2).lower()], int(m.group(1)))
            d2 = date(year2, _MONTH_NAMES[m.group(5).lower()], int(m.group(4)))
            return d1.isoformat(), d2.isoformat()
        except (ValueError, KeyError):
            pass

    # ── specific explicit dates ───────────────────────────────────────────
    m = _DATE_ISO_RE.search(q)
    if m:
        try:
            d = date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
            return d, d
        except ValueError:
            pass

    m = _DATE_DMY_RE.search(q)
    if m:
        try:
            d = date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
            return d, d
        except ValueError:
            pass

    m = _DATE_DM_RE.search(q_lower)
    if m:
        try:
            mnum = _MONTH_NAMES[m.group(2).lower()]
            year = int(m.group(3)) if m.group(3) else today.year
            d = date(year, mnum, int(m.group(1))).isoformat()
            return d, d
        except (ValueError, KeyError):
            pass

    m = _DATE_MD_RE.search(q_lower)
    if m:
        try:
            mnum = _MONTH_NAMES[m.group(1).lower()]
            year = int(m.group(3)) if m.group(3) else today.year
            d = date(year, mnum, int(m.group(2))).isoformat()
            return d, d
        except (ValueError, KeyError):
            pass

    # ── relative N-unit ranges ────────────────────────────────────────────
    m = _NDAYS_RE.search(q)
    if m:
        n = int(m.group(1))
        return (today - timedelta(days=n - 1)).isoformat(), today.isoformat()

    m = _NWEEKS_RE.search(q)
    if m:
        n = int(m.group(1))
        return (today - timedelta(weeks=n)).isoformat(), today.isoformat()

    m = _NMONTHS_RE.search(q)
    if m:
        n = int(m.group(1))
        return (today - timedelta(days=n * 30)).isoformat(), today.isoformat()

    # ── named ranges ──────────────────────────────────────────────────────
    if re.search(r"last\s+week|past\s+week", q_lower):
        return (today - timedelta(days=7)).isoformat(), (today - timedelta(days=1)).isoformat()

    if re.search(r"this\s+week", q_lower):
        monday = today - timedelta(days=today.weekday())
        return monday.isoformat(), today.isoformat()

    if re.search(r"last\s+month", q_lower):
        first_this = today.replace(day=1)
        last_prev  = first_this - timedelta(days=1)
        first_prev = last_prev.replace(day=1)
        return first_prev.isoformat(), last_prev.isoformat()

    if re.search(r"this\s+month", q_lower):
        return today.replace(day=1).isoformat(), today.isoformat()

    if re.search(r"last\s+year", q_lower):
        y = today.year - 1
        return f"{y}-01-01", f"{y}-12-31"

    if re.search(r"this\s+year", q_lower):
        return f"{today.year}-01-01", today.isoformat()

    # ── named month (± year) ──────────────────────────────────────────────
    for mname, mnum in _MONTH_NAMES.items():
        pattern = rf"\b(?:in|for|of|during)\s+{mname}|\b{mname}\s+(\d{{4}})"
        mm = re.search(pattern, q_lower)
        if mm:
            year_str = mm.group(1) if mm.lastindex and mm.group(1) else None
            year = int(year_str) if year_str else today.year
            last_day = monthrange(year, mnum)[1]
            return f"{year:04d}-{mnum:02d}-01", f"{year:04d}-{mnum:02d}-{last_day:02d}"

    return None, None


def _norm_name(s: str) -> str:
    """Strip spaces, hyphens, underscores and uppercase for fuzzy matching."""
    return re.sub(r"[\s\-_]", "", s).upper()


def _extract_card(q: str, known_cards: list) -> str | None:
    q_upper = q.upper()
    q_norm  = _norm_name(q)
    by_len  = sorted(known_cards, key=len, reverse=True)

    # Pass 1: exact substring match (e.g. "CDA62TOP" in query)
    for card in by_len:
        if card.upper() in q_upper:
            return card

    # Pass 2: full normalized match — strips spaces/hyphens both sides
    # e.g. "CDA 62 TOP" → "CDA62TOP" matches card "CDA62TOP"
    for card in by_len:
        if _norm_name(card) in q_norm:
            return card

    # Pass 3: query prefix inside card — e.g. "CDC22" matches "CDC22 NEW5"
    # Require ≥4 chars to avoid accidental short-token matches
    if len(q_norm) >= 4:
        for card in by_len:
            if q_norm in _norm_name(card):
                return card

    return None


def _extract_defect(q: str, known_defects: list) -> str | None:
    q_upper = q.upper()
    for d in sorted(known_defects, key=len, reverse=True):
        if d.upper() in q_upper:
            return d
    return None


def _extract_top_n(q: str, default: int = 5) -> int:
    """Extract 'top N' number from query, e.g. 'top 3 cards' → 3."""
    m = _TOPN_RE.search(q)
    return int(m.group(1)) if m else default


# ---------------------------------------------------------------------------
# DB query — aoi_logs.db (FPY / flagged trends)
# ---------------------------------------------------------------------------

def _query_logs(logs_db_path: str, line: str, start_iso: str, end_iso: str) -> list:
    """
    Returns list of dicts: log_date, pcbs_checked, pcbs_flagged, total_rows.
    DB opened read-only.
    """
    try:
        uri = "file:{}?mode=ro".format(logs_db_path.replace("\\", "/"))
        con = sqlite3.connect(uri, uri=True)
        if line and line != "all":
            rows = con.execute(
                """SELECT log_date,
                          COALESCE(pcbs_checked, 0) AS scanned,
                          COALESCE(pcbs_flagged, 0) AS flagged,
                          COALESCE(total_rows,   0) AS total_rows
                   FROM daily_logs
                   WHERE line=? AND log_date>=? AND log_date<=?
                   ORDER BY log_date ASC""",
                (line, start_iso, end_iso),
            ).fetchall()
        else:
            rows = con.execute(
                """SELECT log_date,
                          SUM(COALESCE(pcbs_checked, 0)),
                          SUM(COALESCE(pcbs_flagged, 0)),
                          SUM(COALESCE(total_rows,   0))
                   FROM daily_logs
                   WHERE log_date>=? AND log_date<=?
                   GROUP BY log_date ORDER BY log_date ASC""",
                (start_iso, end_iso),
            ).fetchall()
        con.close()
        return [
            {"log_date": r[0], "scanned": r[1],
             "flagged": r[2], "total_rows": r[3]}
            for r in rows
        ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Response formatters
# ---------------------------------------------------------------------------

def _fmt_date(iso: str) -> str:
    """YYYY-MM-DD → DD/MM/YYYY"""
    try:
        return f"{iso[8:10]}/{iso[5:7]}/{iso[:4]}"
    except Exception:
        return iso


def _fpy(scanned, flagged) -> str:
    if scanned and scanned > 0:
        return f"{(scanned - flagged) / scanned * 100:.1f}%"
    return "—"


def _fmt_logs_table(rows: list, label: str) -> str:
    if not rows:
        return f"No logged data found for {label}."
    lines = [f"{label}:"]
    total_s, total_f = 0, 0
    for r in rows:
        s, f = r["scanned"], r["flagged"]
        total_s += s; total_f += f
        lines.append(
            f"  {_fmt_date(r['log_date'])}  "
            f"FPY {_fpy(s, f)}  "
            f"({f:,} flagged{(' / ' + f'{s:,} scanned') if s else ''})"
        )
    if len(rows) > 1:
        lines.append(f"  {'─' * 46}")
        lines.append(
            f"  Total  FPY {_fpy(total_s, total_f)}  "
            f"({total_f:,} flagged{(' / ' + f'{total_s:,} scanned') if total_s else ''})"
        )
    return "\n".join(lines)


def _dpb(total_rows, scanned) -> str:
    if scanned and scanned > 0:
        return f"{total_rows / scanned:.2f}"
    return "—"


def _fmt_dpb_table(rows: list, label: str) -> str:
    """Format defects-per-board (DPB) table."""
    if not rows:
        return f"No logged data found for {label}."
    lines = [f"{label}:"]
    total_tr, total_s = 0, 0
    for r in rows:
        tr, s = r.get("total_rows", 0), r["scanned"]
        total_tr += tr; total_s += s
        lines.append(
            f"  {_fmt_date(r['log_date'])}  "
            f"DPB {_dpb(tr, s)}  "
            f"({tr:,} defect events / {s:,} boards)"
        )
    if len(rows) > 1:
        lines.append(f"  {'─' * 50}")
        lines.append(
            f"  Total  DPB {_dpb(total_tr, total_s)}  "
            f"({total_tr:,} defect events / {total_s:,} boards)"
        )
    return "\n".join(lines)


def _line_label(line: str) -> str:
    return line.replace("line", "Line ") if line else "All Lines"


def _date_label(start_iso: str, end_iso: str) -> str:
    if start_iso == end_iso:
        return _fmt_date(start_iso)
    return f"{_fmt_date(start_iso)} – {_fmt_date(end_iso)}"


# ---------------------------------------------------------------------------
# Intent handlers
# ---------------------------------------------------------------------------

def _handle_help(**_) -> str:
    return (
        "I can answer questions about AOI production data.\n\n"
        "Defects:\n"
        "  • top defects for CDA62TOP\n"
        "  • top defects for CDA 62 TOP last week          (fuzzy card name)\n"
        "  • all defects yesterday on line 1\n"
        "  • what's failing on line 1 last week?\n"
        "  • most common defects on line 4 this month\n"
        "  • defect summary for 24/01/2026\n"
        "  • tell me about SOT23_SOLDER\n"
        "  • which cards have the BRIDGE defect?\n"
        "  • list all defect types\n\n"
        "Defect Trends:\n"
        "  • Coplanarity trend last 7 days\n"
        "  • BRIDGE trend on line 1 this month\n"
        "  • SOT23_SOLDER trend last 2 weeks for CDA62TOP\n\n"
        "Cards — Overview:\n"
        "  • worst card on line 2\n"
        "  • worst card last week on line 1\n"
        "  • card breakdown for line 1\n"
        "  • top 5 cards last month on line 4\n"
        "  • compare all cards on line 4\n"
        "  • best performing card on line 1\n"
        "  • best card last 7 days\n"
        "  • which boards are failing most?\n"
        "  • list all cards\n\n"
        "Cards — Per-Day Rankings:\n"
        "  • top 5 cards per day last week\n"
        "  • top 3 cards per day this month on line 1\n"
        "  • daily card breakdown last 7 days\n"
        "  • card ranking by day last 2 weeks on line 4\n\n"
        "Card Stats (single card):\n"
        "  • CDA62TOP stats\n"
        "  • CDA 67 TOP last month                         (fuzzy name ok)\n"
        "  • cdc22 stats last week                         (partial match ok)\n"
        "  • CDA62TOP defects/board last 7 days\n\n"
        "Counts & Throughput:\n"
        "  • how many boards were scanned yesterday on line 1?\n"
        "  • total output this week on line 4\n"
        "  • units inspected last 7 days\n"
        "  • cda67top production last month\n\n"
        "Flagged / Rejects:\n"
        "  • how many PCBs were flagged last 7 days?\n"
        "  • total rejects today on line 2\n"
        "  • number of failures yesterday\n\n"
        "Quality / FPY / DPB:\n"
        "  • line 2 FPY this month\n"
        "  • rejection rate last 2 weeks\n"
        "  • defects per board on line 1\n"
        "  • DPB / DPU / PPM last week\n"
        "  • failure rate this month on line 4\n\n"
        "Summary & Trends:\n"
        "  • summary for today on line 1\n"
        "  • production status yesterday\n"
        "  • how was line 4 last week?\n"
        "  • trend for line 1 last month\n"
        "  • daily stats last 2 weeks\n\n"
        "Date formats understood:\n"
        "  today / yesterday / last 7 days / last 2 weeks / last 3 months /\n"
        "  this week / last week / last month / this month / in march /\n"
        "  march 2025 / 3 march 2026 / 03/03/2026 / 2026-03-03\n\n"
        "Card name formats understood:\n"
        "  CDA62TOP / CDA 62 TOP / CDA-62-TOP / cda62top / CDC22 (→ CDC22 NEW5)\n\n"
        "Tip: upload CSVs in 'Upload Training Data' to enable card & defect queries."
    )


def _handle_top_defects(card, line, start_iso, end_iso, known_cards, chatbot_db, **_) -> str:
    # ── Date specified → date-based query (with or without card) ─────────────
    if start_iso:
        e_iso = end_iso or start_iso
        results = cdb.query_defects_by_date(
            start_iso, e_iso, line, card=card, top_n=20, db_path=chatbot_db)
        if not results:
            card_str = f" for {card}" if card else ""
            line_str = f" on {_line_label(line)}" if line else ""
            return (
                f"No defect data found{card_str}{line_str} "
                f"for {_date_label(start_iso, e_iso)}.\n"
                "Make sure CSVs covering this date have been ingested."
            )
        card_str = f" for {card}" if card else ""
        line_str = f" — {_line_label(line)}" if line else " — all lines"
        title = f"Defects{card_str}{line_str} — {_date_label(start_iso, e_iso)}:"
        body = "\n".join(
            f"  {i+1:2d}. {r['defect_type']:<30s} {r['count']:>6,} occurrences"
            for i, r in enumerate(results)
        )
        return f"{title}\n{body}"

    # ── No date, specific card → all-time defects for that card ──────────────
    if card:
        results = cdb.query_card_defects(card, line, top_n=10, db_path=chatbot_db)
        if not results:
            line_str = f" on {_line_label(line)}" if line else ""
            return (
                f"No defect data found for {card}{line_str}.\n"
                "Try uploading historical CSVs in the Upload section."
            )
        line_str = f" on {_line_label(line)}" if line else " (all lines)"
        title = f"Top defects for {card}{line_str} — all time:"
        body = "\n".join(
            f"  {i+1:2d}. {r['defect_type']:<30s} {r['count']:>6,} occurrences"
            for i, r in enumerate(results)
        )
        return f"{title}\n{body}"

    # ── No date, no card → all-time defects across all cards ─────────────────
    if not known_cards:
        return (
            "Please specify a card (e.g. 'top defects for CDA62TOP') or a date,\n"
            "or upload CSVs first so I know which cards to look up."
        )
    results = cdb.query_all_defects(line, top_n=15, db_path=chatbot_db)
    if not results:
        line_str = f" on {_line_label(line)}" if line else ""
        return (
            f"No defect data found{line_str}.\n"
            "Try uploading historical CSVs in the Upload section."
        )
    line_str = f" — {_line_label(line)}" if line else " — all lines"
    title = f"Top defects{line_str} — all time:"
    body = "\n".join(
        f"  {i+1:2d}. {r['defect_type']:<30s} {r['count']:>6,} occurrences"
        for i, r in enumerate(results)
    )
    return f"{title}\n{body}"


def _handle_defect_info(defect, card, line, known_defects, chatbot_db, **_) -> str:
    if not defect:
        if known_defects:
            sample = ", ".join(sorted(known_defects)[:12])
            return (
                "Which defect would you like info on?\n"
                f"Known defects include: {sample}...\n"
                "Try: 'tell me about SOT23_SOLDER'"
            )
        return (
            "Specify a defect name, e.g. 'tell me about BRIDGE'.\n"
            "Upload CSVs first to populate the defect list."
        )

    results = cdb.query_defect_cards(defect, line, top_n=10, db_path=chatbot_db)
    if not results:
        line_str = f" on {_line_label(line)}" if line else ""
        return f"No data found for defect '{defect}'{line_str}. Try uploading more CSVs."

    line_str = f" on {_line_label(line)}" if line else " (all lines)"
    total = sum(r["count"] for r in results)
    title = f"Defect '{defect}'{line_str} — {total:,} total occurrences, by card:"
    body = "\n".join(
        f"  {i+1:2d}. {r['card']:<20s} {r['count']:>6,} occurrences"
        for i, r in enumerate(results)
    )
    return f"{title}\n{body}"


def _handle_card_breakdown(line, start_iso, end_iso, question, chatbot_db, **_) -> str:
    top_n = _extract_top_n(question, default=10)
    e_iso = end_iso or start_iso
    results = cdb.query_worst_card(line, start_iso=start_iso, end_iso=e_iso,
                                   top_n=top_n, db_path=chatbot_db)
    if not results:
        line_str = f" on {_line_label(line)}" if line else ""
        date_str = f" for {_date_label(start_iso, e_iso)}" if start_iso else ""
        return (
            f"No card data found{line_str}{date_str}.\n"
            "Upload historical CSVs to enable card comparisons."
        )
    line_str = f" — {_line_label(line)}" if line else " — all lines"
    date_str = f" — {_date_label(start_iso, e_iso)}" if start_iso else ""
    title = f"Cards by flagged PCBs{line_str}{date_str}:"
    body = "\n".join(
        f"  {i+1:2d}. {r['card']:<20s} {r['total_flagged']:>8,} flagged PCBs"
        for i, r in enumerate(results)
    )
    return f"{title}\n{body}"


def _handle_best_card(line, start_iso, end_iso, question, chatbot_db, **_) -> str:
    top_n = _extract_top_n(question, default=10)
    e_iso = end_iso or start_iso
    results = cdb.query_best_card(line, start_iso=start_iso, end_iso=e_iso,
                                  top_n=top_n, db_path=chatbot_db)
    if not results:
        line_str = f" on {_line_label(line)}" if line else ""
        date_str = f" for {_date_label(start_iso, e_iso)}" if start_iso else ""
        return (
            f"No card data found{line_str}{date_str}.\n"
            "Upload historical CSVs to enable card comparisons."
        )
    line_str = f" — {_line_label(line)}" if line else " — all lines"
    date_str = f" — {_date_label(start_iso, e_iso)}" if start_iso else ""
    title = f"Cards by fewest flagged PCBs{line_str}{date_str}:"
    body = "\n".join(
        f"  {i+1:2d}. {r['card']:<20s} {r['total_flagged']:>8,} flagged PCBs"
        for i, r in enumerate(results)
    )
    return f"{title}\n{body}"


def _handle_component_info(card, line, chatbot_db, **_) -> str:
    results = cdb.query_component_info(card, line, db_path=chatbot_db)
    if not results:
        return (
            "No component data found.\n"
            "Upload historical CSVs to populate component information."
        )
    scope_parts = []
    if card:  scope_parts.append(card)
    if line:  scope_parts.append(_line_label(line))
    scope = " — " + " / ".join(scope_parts) if scope_parts else " — all"
    title = f"Components seen{scope}:"
    body = "\n".join(
        f"  {r['component']:<30s}  pkg: {r['package'] or '—':<18s}  seen {r['count']:,}×"
        for r in results
    )
    return f"{title}\n{body}"


def _handle_fpy_trend(line, start_iso, end_iso, logs_db, question, **_) -> str:
    if not start_iso:
        start_iso = (date.today() - timedelta(days=6)).isoformat()
        end_iso   = date.today().isoformat()
    rows  = _query_logs(logs_db, line, start_iso, end_iso)
    label = f"{_line_label(line)} — {_date_label(start_iso, end_iso)}"
    if _DPB_RE.search(question):
        return _fmt_dpb_table(rows, f"{label} — Defects per Board")
    return _fmt_logs_table(rows, f"{label} — FPY")


def _handle_scanned_count(line, start_iso, end_iso, logs_db, **_) -> str:
    if not start_iso:
        start_iso = (date.today() - timedelta(days=6)).isoformat()
        end_iso   = date.today().isoformat()
    rows = _query_logs(logs_db, line, start_iso, end_iso)
    if not rows:
        return f"No logged data for {_line_label(line)} — {_date_label(start_iso, end_iso)}."
    total_s = sum(r["scanned"] for r in rows)
    total_f = sum(r["flagged"] for r in rows)
    detail = "\n".join(
        f"  {_fmt_date(r['log_date'])}  {r['scanned']:>6,} scanned  ({r['flagged']:,} flagged)"
        for r in rows
    )
    return (
        f"{_line_label(line)} — boards scanned ({_date_label(start_iso, end_iso)}):\n"
        f"{detail}\n"
        f"  {'─' * 46}\n"
        f"  Total  {total_s:>6,} scanned  "
        f"({total_f:,} flagged  /  FPY {_fpy(total_s, total_f)})"
    )


def _handle_flagged_count(line, card, start_iso, end_iso, logs_db, chatbot_db, **_) -> str:
    if not start_iso:
        start_iso = (date.today() - timedelta(days=6)).isoformat()
        end_iso   = date.today().isoformat()
    e_iso = end_iso or start_iso

    # Card specified → per-day breakdown from chatbot DB
    if card:
        rows = cdb.query_card_daily_flagged(card, start_iso, e_iso, line,
                                            db_path=chatbot_db)
        if not rows:
            line_str = f" on {_line_label(line)}" if line else ""
            return (
                f"No flagged data found for {card}{line_str} "
                f"during {_date_label(start_iso, e_iso)}.\n"
                "Make sure CSVs for this card have been ingested."
            )
        line_str = f" — {_line_label(line)}" if line else " — all lines"
        title = f"{card}{line_str} — flagged PCBs per day ({_date_label(start_iso, e_iso)}):"
        total = sum(r["flagged"] for r in rows)
        body = "\n".join(
            f"  {_fmt_date(r['log_date'])}  {r['flagged']:>6,} flagged"
            for r in rows
        )
        return f"{title}\n{body}\n  {'─' * 32}\n  Total  {total:>6,} flagged"

    # Line-level (original behavior)
    rows = _query_logs(logs_db, line, start_iso, e_iso)
    if not rows:
        return f"No logged data for {_line_label(line)} — {_date_label(start_iso, e_iso)}."
    total = sum(r["flagged"] for r in rows)
    detail = "\n".join(
        f"  {_fmt_date(r['log_date'])}  {r['flagged']:>6,} flagged"
        for r in rows
    )
    return (
        f"{_line_label(line)} — PCBs flagged ({_date_label(start_iso, e_iso)}):\n"
        f"{detail}\n"
        f"  {'─' * 25}\n"
        f"  Total  {total:>6,} flagged"
    )


def _handle_daily_summary(line, start_iso, end_iso, chatbot_db, logs_db, **_) -> str:
    if not start_iso:
        start_iso = end_iso = date.today().isoformat()

    log_rows  = _query_logs(logs_db, line, start_iso, end_iso)
    multi_day = start_iso != end_iso

    label_line = _line_label(line)
    out = [f"Summary — {label_line} — {_date_label(start_iso, end_iso)}"]

    # ── overall log numbers ───────────────────────────────────────────────
    if log_rows:
        if multi_day:
            total_s = sum(r["scanned"] for r in log_rows)
            total_f = sum(r["flagged"] for r in log_rows)
            out.append(f"  Total flagged : {total_f:,} / {total_s:,} scanned  "
                       f"(FPY {_fpy(total_s, total_f)})")
            out.append(f"  Days logged   : {len(log_rows)}")
        else:
            r = log_rows[0]
            out.append(
                f"  Flagged  : {r['flagged']:,}"
                + (f"  /  Scanned: {r['scanned']:,}  FPY {_fpy(r['scanned'], r['flagged'])}"
                   if r["scanned"] else "")
            )

    # ── per-card breakdown ────────────────────────────────────────────────
    if multi_day:
        card_rows = cdb.query_range_card_summary(
            start_iso, end_iso, line, db_path=chatbot_db)
    else:
        card_rows = cdb.query_daily_card_summary(
            start_iso, line, db_path=chatbot_db)
        # flatten to same shape as range query
        card_rows = [{"card": c["card"], "flagged": c["flagged"],
                      "top_defects": c.get("top_defects", [])}
                     for c in card_rows]

    if card_rows:
        out.append("  Card breakdown:")
        for c in card_rows[:10]:
            out.append(f"    {c['card']:<20s}  {c['flagged']:>5,} flagged")
            top_d = c.get("top_defects", [])
            if top_d:
                top = top_d[0]
                out.append(f"      top defect: {top['defect']} ({top['count']}×)")

    if not log_rows and not card_rows:
        out.append("  No data found. Make sure the date is logged and CSVs are ingested.")

    return "\n".join(out)


def _handle_trend(line, start_iso, end_iso, logs_db, **_) -> str:
    if not start_iso:
        start_iso = (date.today() - timedelta(days=29)).isoformat()
        end_iso   = date.today().isoformat()
    rows = _query_logs(logs_db, line, start_iso, end_iso)
    label = f"{_line_label(line)} trend — {_date_label(start_iso, end_iso)}"
    return _fmt_logs_table(rows, label)


def _handle_unknown(question, known_cards, known_defects, **_) -> str:
    hints = []
    if known_cards:
        hints.append(f"  • top defects for {known_cards[0]}")
    if known_defects:
        hints.append(f"  • tell me about {known_defects[0]}")
    hints += [
        "  • line 1 FPY last week",
        "  • how many boards scanned yesterday on line 4",
        "  • which card has the most issues on line 2",
    ]
    return (
        "I'm not sure what you're asking. Type 'help' to see example questions.\n\n"
        "You could try:\n" + "\n".join(hints)
    )


def _handle_defect_trend(defect, line, card, start_iso, end_iso,
                         known_defects, chatbot_db, **_) -> str:
    if not defect:
        if known_defects:
            return (
                f"Which defect trend? Try: '{known_defects[0]} trend last 7 days'\n"
                "or: 'Coplanarity trend this month on line 2'"
            )
        return "Specify a defect name, e.g. 'Coplanarity trend last week'."

    if not start_iso:
        start_iso = (date.today() - timedelta(days=6)).isoformat()
        end_iso   = date.today().isoformat()
    e_iso = end_iso or start_iso

    rows = cdb.query_defect_trend(defect, start_iso, e_iso, line, card,
                                  db_path=chatbot_db)
    if not rows:
        card_str = f" for {card}" if card else ""
        line_str = f" on {_line_label(line)}" if line else ""
        return (
            f"No trend data for '{defect}'{card_str}{line_str} — "
            f"{_date_label(start_iso, e_iso)}.\n"
            "Make sure CSVs covering this period have been ingested."
        )

    card_str = f" for {card}" if card else ""
    line_str = f" — {_line_label(line)}" if line else " — all lines"
    title = f"'{defect}'{card_str} trend{line_str} — {_date_label(start_iso, e_iso)}:"
    total = sum(r["count"] for r in rows)
    body  = "\n".join(
        f"  {_fmt_date(r['log_date'])}  {r['count']:>6,} occurrences"
        for r in rows
    )
    return f"{title}\n{body}\n  {'─' * 35}\n  Total  {total:>6,} occurrences"


def _handle_card_stats(card, line, start_iso, end_iso, defect, chatbot_db, **_) -> str:
    if not card:
        # No card — fall back to full card breakdown
        return _handle_card_breakdown(line=line, chatbot_db=chatbot_db)

    stats = cdb.query_card_stats(card, line, start_iso, end_iso, db_path=chatbot_db)
    if not stats:
        line_str = f" on {_line_label(line)}" if line else ""
        date_str = f" for {_date_label(start_iso, end_iso or start_iso)}" if start_iso else ""
        return (
            f"No data found for {card}{line_str}{date_str}.\n"
            "Make sure CSVs for this card have been ingested."
        )

    line_str = f" — {_line_label(line)}" if line else " — all lines"
    date_str = f" — {_date_label(start_iso, end_iso or start_iso)}" if start_iso else " — all time"
    flagged = stats["flagged"]
    events  = stats["defect_events"]
    days    = stats["days"]
    dpb     = f"{events / flagged:.2f}" if flagged else "—"

    out = [f"{card}{line_str}{date_str}:"]
    out.append(f"  Flagged PCBs          : {flagged:>8,}")
    out.append(f"  Defect events         : {events:>8,}")
    out.append(f"  Defects/flagged PCB   : {dpb:>8}")
    if not start_iso:
        out.append(f"  Days with data        : {days:>8,}")
    out.append("  Note: FPY needs total scanned — use 'line X FPY' for line-level FPY")

    # Top defects for this card
    if start_iso:
        defect_rows = cdb.query_defects_by_date(
            start_iso, end_iso or start_iso, line, card=card, top_n=10, db_path=chatbot_db)
        defect_list = defect_rows  # [{"defect_type": ..., "count": ...}]
    else:
        defect_list = cdb.query_card_defects(card, line, top_n=10, db_path=chatbot_db)
        # normalize key: query_card_defects returns "defect_type" too

    if defect_list:
        out.append("\n  Top defects:")
        for i, r in enumerate(defect_list):
            dname = r.get("defect_type", "")
            cnt   = r.get("count", 0)
            mark  = " ◄" if defect and defect.upper() == dname.upper() else ""
            out.append(f"    {i+1:2d}. {dname:<30s} {cnt:>6,} events{mark}")

    return "\n".join(out)


def _handle_daily_top_cards(line, start_iso, end_iso, question, chatbot_db, **_) -> str:
    top_n = _extract_top_n(question, default=5)
    if not start_iso:
        start_iso = (date.today() - timedelta(days=6)).isoformat()
        end_iso   = date.today().isoformat()
    e_iso = end_iso or start_iso
    rows = cdb.query_daily_top_cards(start_iso, e_iso, line, top_n=top_n,
                                     db_path=chatbot_db)
    if not rows:
        line_str = f" on {_line_label(line)}" if line else ""
        return (
            f"No card data found{line_str} for {_date_label(start_iso, e_iso)}.\n"
            "Make sure CSVs covering this period have been ingested."
        )
    line_str = f" — {_line_label(line)}" if line else " — all lines"
    title = f"Top {top_n} cards per day{line_str} — {_date_label(start_iso, e_iso)}:"
    parts = [title]
    for day_entry in rows:
        parts.append(f"\n  {_fmt_date(day_entry['log_date'])}:")
        for i, c in enumerate(day_entry["cards"]):
            parts.append(f"    {i+1}. {c['card']:<20s} {c['flagged']:>6,} flagged")
    return "\n".join(parts)


def _handle_list_cards(line, chatbot_db, **_) -> str:
    results = cdb.query_all_card_names(line, db_path=chatbot_db)
    if not results:
        line_str = f" on {_line_label(line)}" if line else ""
        return (
            f"No cards found{line_str}.\n"
            "Upload historical CSVs to populate card data."
        )
    line_str = f" — {_line_label(line)}" if line else " — all lines"
    title = f"All cards{line_str} — {len(results)} total:"
    body = "\n".join(
        f"  {i+1:2d}. {r['card']:<25s} {r['total_flagged']:>8,} flagged PCBs"
        for i, r in enumerate(results)
    )
    return f"{title}\n{body}"


def _handle_list_defects(line, chatbot_db, **_) -> str:
    results = cdb.query_all_defect_types(line, db_path=chatbot_db)
    if not results:
        line_str = f" on {_line_label(line)}" if line else ""
        return (
            f"No defects found{line_str}.\n"
            "Upload historical CSVs to populate defect data."
        )
    line_str = f" — {_line_label(line)}" if line else " — all lines"
    title = f"All defect types{line_str} — {len(results)} total:"
    body = "\n".join(
        f"  {i+1:2d}. {r['defect_type']:<30s} {r['count']:>8,} occurrences"
        for i, r in enumerate(results)
    )
    return f"{title}\n{body}"


_HANDLERS = {
    "help":             _handle_help,
    "list_defects":     _handle_list_defects,
    "list_cards":       _handle_list_cards,
    "top_defects":      _handle_top_defects,
    "defect_info":      _handle_defect_info,
    "daily_top_cards":  _handle_daily_top_cards,
    "card_breakdown":   _handle_card_breakdown,
    "best_card":        _handle_best_card,
    "card_stats":       _handle_card_stats,
    "component_info":   _handle_component_info,
    "fpy_trend":        _handle_fpy_trend,
    "scanned_count":    _handle_scanned_count,
    "flagged_count":    _handle_flagged_count,
    "daily_summary":    _handle_daily_summary,
    "defect_trend":     _handle_defect_trend,
    "trend":            _handle_trend,
    "unknown":          _handle_unknown,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def answer(
    question: str,
    chatbot_db_path: str,
    logs_db_path: str,
    known_cards: list,
    known_defects: list,
) -> str:
    """
    Answer a natural-language AOI question.

    Args:
        question:        User's raw question string.
        chatbot_db_path: Path to aoi_chatbot.db (knowledge base).
        logs_db_path:    Path to aoi_logs.db (production history).
        known_cards:     List of card names from chatbot_db (for entity matching).
        known_defects:   List of defect type strings from chatbot_db.

    Returns a plain-text answer string.
    """
    if not question.strip():
        return ""

    intent              = _detect_intent(question)
    line                = _extract_line(question)
    card                = _extract_card(question, known_cards)
    defect              = _extract_defect(question, known_defects)
    start_iso, end_iso  = _extract_date_range(question)

    # ── smart routing ────────────────────────────────────────────────────
    # "worst card for CDA62TOP" → user wants that card's top defects
    if intent == "card_breakdown" and card:
        intent = "top_defects"
    # defect_info with no defect but card present → top defects for that card
    if intent == "defect_info" and not defect and card:
        intent = "top_defects"
    # defect mentioned with trend query → per-day defect trend
    if intent == "trend" and defect:
        intent = "defect_trend"
    # trend for a specific card (no defect) → per-day flagged breakdown
    if intent == "trend" and card and not defect:
        intent = "flagged_count"
    # metric intents with a specific card → card-level stats
    # (line-level handlers can't filter by card)
    if intent in ("fpy_trend", "scanned_count", "daily_summary") and card:
        intent = "card_stats"
    # flagged_count + card: summary (card_stats) unless date present → per-day breakdown
    if intent == "flagged_count" and card and not start_iso:
        intent = "card_stats"

    handler = _HANDLERS.get(intent, _handle_unknown)
    try:
        return handler(
            question=question,
            intent=intent,
            line=line,
            card=card,
            defect=defect,
            start_iso=start_iso,
            end_iso=end_iso,
            known_cards=known_cards,
            known_defects=known_defects,
            chatbot_db=chatbot_db_path,
            logs_db=logs_db_path,
        )
    except Exception as exc:
        return f"Error generating response: {exc}"
