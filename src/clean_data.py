import pandas as pd


def clean_aoi_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parses StartDateTime_raw / EndDateTime_raw into StartDateTime / EndDateTime
    Supports ONLY 3 formats:
      - Line1/Line4: DD-MM-YYYY HH:MM:SS
      - Line2:       YYYY-MM-DD h:mm:ss AM/PM
    """
    df = df.copy()

    s = df.get("StartDateTime_raw")
    e = df.get("EndDateTime_raw")

    s1 = pd.to_datetime(s, format="%d-%m-%Y %H:%M:%S", errors="coerce")
    s2 = pd.to_datetime(s, format="%Y-%m-%d %I:%M:%S %p", errors="coerce")
    df["StartDateTime"] = s1.fillna(s2)

    e1 = pd.to_datetime(e, format="%d-%m-%Y %H:%M:%S", errors="coerce")
    e2 = pd.to_datetime(e, format="%Y-%m-%d %I:%M:%S %p", errors="coerce")
    df["EndDateTime"] = e1.fillna(e2)

    ok = df["StartDateTime"].notna().sum()
    print(f"🧼 Datetime parsed for {ok} / {len(df)} rows")

    df["uname"] = df.get("uname", "").fillna("").astype(str).str.strip()
    df["PCBID"] = df.get("PCBID", "").fillna("").astype(str).str.strip()

    return df
