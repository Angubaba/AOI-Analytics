# AOI Analytics

A desktop application for analysing **Automated Optical Inspection (AOI)** data from PCB manufacturing lines. Built with Python / Tkinter, it turns raw AOI CSV exports into actionable defect statistics, trends, and reports — with a built-in rule-based chatbot for natural-language queries.

---

## Features

| Module | Description |
|---|---|
| **Multi-line analysis** | Load CSV data from AOI Lines 1, 2, and 4 simultaneously or individually |
| **Defect pareto** | Bar chart of top defect types ranked by occurrence count |
| **PCBs flagged by card** | Per-board-type breakdown of flagged PCB counts |
| **Hourly scan trend** | Time-series bar chart of PCBs scanned per hour across a shift |
| **FPY trend** | First-Pass Yield trend plotted over multiple days |
| **Minute-level drilldown** | Per-minute scan rate chart for pinpointing throughput issues |
| **Defect timing** | When each defect type tends to occur across the shift |
| **Card-wise output** | Per-card subdirectory of charts and CSVs auto-generated on each run |
| **PDF / CSV export** | One-click report export for sharing with team leads |
| **AI Chatbot** | Natural-language Q&A over historical data (no internet required) |
| **Production log DB** | Persistent SQLite history of every run for trend analysis across days |

---

## Project Structure

```
AOI_analytics/
├── app.py                  # Main Tkinter application entry point
├── main.py                 # Alternate launcher
├── requirements.txt        # Python dependencies
├── AOI_Analytics.spec      # PyInstaller build spec
│
├── src/
│   ├── analysis.py         # Core analytics logic (defect stats, FPY, trend)
│   ├── clean_data.py       # Data normalisation and column standardisation
│   ├── plots.py            # Matplotlib chart generators
│   ├── log_db.py           # aoi_logs.db read/write (production history)
│   ├── report.py           # PDF and CSV report export
│   ├── chatbot.py          # Rule-based NL chatbot engine
│   ├── chatbot_db.py       # aoi_chatbot.db read/write (chatbot knowledge base)
│   └── parsers/
│       ├── auto.py         # Auto-detects AOI line from CSV header
│       ├── line1_parser.py # Line 1 CSV parser
│       ├── line2_parser.py # Line 2 CSV parser
│       └── line4_parser.py # Line 4 CSV parser
│
├── outputs/                # Auto-generated charts and CSVs (gitignored)
│   └── cardwise/           # Per-card subdirectories
│
└── dist/AOI_Analytics/     # PyInstaller build output (gitignored)
```

---

## How It Works

### Shift day definition
A "production day" runs **7 am → 7 am** the next morning. All timestamps are shifted back 7 hours before date-bucketing, so night-shift scans are correctly grouped with the production day they belong to rather than the calendar day they physically occurred on.

### Flagged PCB counting
Flagged PCB counts use a **ScanKey** (`PCBID|StartDateTime` unique pairs) to deduplicate — a single board that triggers multiple defect events is counted only once as flagged.

### Data flow

```
AOI CSV export
      │
      ▼
Parser (auto-detects line)
      │
      ▼
clean_data  →  normalise columns, strip junk rows
      │
      ▼
analysis.py →  defect pareto, FPY, trend, flagged counts
      │
      ├──▶  plots.py    →  PNG charts
      ├──▶  log_db.py   →  persist to aoi_logs.db
      ├──▶  chatbot_db  →  ingest into aoi_chatbot.db
      └──▶  report.py   →  PDF / CSV export
```

### Two databases

| Database | Purpose |
|---|---|
| `aoi_logs.db` | Raw per-run production history; source for FPY trend charts |
| `aoi_chatbot.db` | Pre-aggregated daily/card summaries; queried by the chatbot |

---

## Chatbot

The built-in chatbot answers questions about historical AOI data using a **fully offline, rule-based engine** — no API key, no internet connection, no external model.

### Example queries

```
line 2 FPY last 7 days
CDE8X.EPM 88_COPY summary for february
top defects on line 1 in march
CDA63TOP trend from 1feb to 28feb
how many PCBs were flagged yesterday?
line 4 defects per board last 30 days
Solderfillet trend on line 2
list all cards
```

### Supported intents

| Intent | Example |
|---|---|
| FPY (line or all lines) | `line 1 FPY last 14 days` |
| Card summary | `CDA63TOP summary for february` |
| Defect trend over time | `Coplanarity trend on line 2` |
| Flagged count per day | `CDE8X.EPM 88_COPY trend from 1feb to 28feb` |
| Defects per board (DPB) | `line 2 defects per board last week` |
| Top defects | `top defects in march` |
| List cards / lines | `list all cards`, `which lines have data?` |
| Help | `help`, `what can I ask?` |

---

## Installation

### From source

```bash
git clone https://github.com/Angubaba/AOI-Analytics.git
cd AOI-Analytics
pip install -r requirements.txt
python app.py
```

**Requirements:** Python 3.10+

```
pandas
matplotlib
pillow
reportlab
```

### Pre-built executable (Windows)

Build with PyInstaller:

```bash
pip install pyinstaller
pyinstaller AOI_Analytics.spec
```

The output lands in `dist/AOI_Analytics/`. Run `AOI_Analytics.exe` — keep the entire folder together (one-folder build, not a single-file exe).

---

## Usage

1. **Load data** — click *Browse* and select one or more AOI CSV export files. The app auto-detects which line each file came from.
2. **Run analysis** — click *Analyse*. Charts and CSVs are written to `outputs/`.
3. **View charts** — switch between the *Analysis*, *Trends*, and *Card View* tabs.
4. **Export report** — use *Export PDF* or *Export CSV* to save a shareable summary.
5. **Query chatbot** — type a question in the Chatbot tab using plain English.

---

## Supported AOI Lines

| Line | Parser |
|---|---|
| Line 1 | `src/parsers/line1_parser.py` |
| Line 2 | `src/parsers/line2_parser.py` |
| Line 4 | `src/parsers/line4_parser.py` |

Parsers are column-order resilient — columns are matched by name rather than position, so minor changes to the AOI export format do not break parsing.

---

## License

Internal tool developed during an internship at **Deltron**. Not licensed for external distribution.
