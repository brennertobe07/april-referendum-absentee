"""
build_april_absentee_json.py
Queries absentee.dbo on INSTANCE-1 and writes JSON files
for the April Referendum Absentee Dashboard hosted on GitHub Pages.

Run: py -3.12 build_april_absentee_json.py
"""

import pyodbc
import pandas as pd
import json
import os
from datetime import datetime

# ── CONFIG ──────────────────────────────────────────────────────────────────
SERVER      = r"INSTANCE-1"
DATABASE    = "absentee"
ODBC_DRIVER = "ODBC Driver 17 for SQL Server"

# Set this to the local path of your GitHub repo
# e.g. r"C:\Users\YourName\Documents\GitHub\april-referendum-absentee"
REPO_ROOT = r"C:\Scripts\Python\Python_Absentee\April\april-referendum-absentee"
DATA_DIR    = os.path.join(REPO_ROOT, "data")
# ────────────────────────────────────────────────────────────────────────────

QUERY = """
SELECT
    van.CountyName,
    van.PrecinctName,
    COUNT(van.VoterFileVANID) AS TotalMatchedVoters,

    -- Voted by party
    SUM(CASE 
            WHEN dal.BALLOT_RECEIPT_DATE IS NOT NULL
                 AND (
                     van.likelyparty IN ('sd', 'ld')
                     OR (van.likelyparty IN ('nd', 'U', 'I') AND van.Clarity_DemSupport_26 >= 60)
                 )
            THEN 1 ELSE 0 END) AS DemVotedCount,

    SUM(CASE 
            WHEN dal.BALLOT_RECEIPT_DATE IS NOT NULL
                 AND (
                     van.likelyparty IN ('sr', 'lr')
                     OR (van.likelyparty IN ('nd', 'U', 'I') AND van.Clarity_DemSupport_26 <= 40)
                 )
            THEN 1 ELSE 0 END) AS RepVotedCount,

    SUM(CASE 
            WHEN dal.BALLOT_RECEIPT_DATE IS NOT NULL
                 AND (
                     (van.likelyparty IN ('nd', 'U', 'I') AND van.Clarity_DemSupport_26 BETWEEN 41 AND 59)
                     OR van.likelyparty IS NULL
                     OR van.likelyparty NOT IN ('sd', 'ld', 'sr', 'lr', 'nd', 'U', 'I')
                 )
            THEN 1 ELSE 0 END) AS UnknownVotedCount,

    -- Still out (ONGOING, no receipt date) by party
    SUM(CASE 
            WHEN dal.BALLOT_RECEIPT_DATE IS NULL AND dal.ONGOING = 'True'
                 AND (
                     van.likelyparty IN ('sd', 'ld')
                     OR (van.likelyparty IN ('nd', 'U', 'I') AND van.Clarity_DemSupport_26 >= 60)
                 )
            THEN 1 ELSE 0 END) AS DemOutCount,

    SUM(CASE 
            WHEN dal.BALLOT_RECEIPT_DATE IS NULL AND dal.ONGOING = 'True'
                 AND (
                     van.likelyparty IN ('sr', 'lr')
                     OR (van.likelyparty IN ('nd', 'U', 'I') AND van.Clarity_DemSupport_26 <= 40)
                 )
            THEN 1 ELSE 0 END) AS RepOutCount,

    SUM(CASE 
            WHEN dal.BALLOT_RECEIPT_DATE IS NULL AND dal.ONGOING = 'True'
                 AND (
                     (van.likelyparty IN ('nd', 'U', 'I') AND van.Clarity_DemSupport_26 BETWEEN 41 AND 59)
                     OR van.likelyparty IS NULL
                     OR van.likelyparty NOT IN ('sd', 'ld', 'sr', 'lr', 'nd', 'U', 'I')
                 )
            THEN 1 ELSE 0 END) AS UnknownOutCount

FROM van
INNER JOIN Daily_Absentee_List dal
    ON van.StateFileID = dal.IDENTIFICATION_NUMBER

GROUP BY
    van.CountyName,
    van.PrecinctName

ORDER BY
    van.CountyName,
    van.PrecinctName;
"""

NUM_COLS = [
    "TotalMatchedVoters",
    "DemVotedCount", "RepVotedCount", "UnknownVotedCount",
    "DemOutCount",   "RepOutCount",   "UnknownOutCount",
]

def connect():
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

def add_totals(df):
    """Add derived VotedCount, OutCount columns."""
    df = df.copy()
    df["VotedCount"] = df["DemVotedCount"] + df["RepVotedCount"] + df["UnknownVotedCount"]
    df["OutCount"]   = df["DemOutCount"]   + df["RepOutCount"]   + df["UnknownOutCount"]
    return df

def df_to_records(df):
    """Convert DataFrame to JSON-safe list of dicts."""
    return json.loads(df.to_json(orient="records"))

def aggregate_counties(df):
    grp_cols = NUM_COLS + ["VotedCount", "OutCount"]
    county_df = (
        df.groupby("CountyName")[grp_cols]
        .sum()
        .reset_index()
        .sort_values("CountyName")
    )
    return county_df

def aggregate_statewide(df):
    grp_cols = NUM_COLS + ["VotedCount", "OutCount"]
    totals = df[grp_cols].sum()
    return totals.to_dict()

def write_json(obj, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    print(f"  wrote {path}")

def main():
    print(f"Connecting to {SERVER}\\{DATABASE} ...")
    conn = connect()

    print("Running query ...")
    df = pd.read_sql(QUERY, conn)
    conn.close()

    print(f"Rows returned: {len(df):,}")

    # Derived totals
    df = add_totals(df)

    # Ensure data dir exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # ── precincts.json ──────────────────────────────────────────────────────
    write_json(df_to_records(df), os.path.join(DATA_DIR, "precincts.json"))

    # ── counties.json ───────────────────────────────────────────────────────
    county_df = aggregate_counties(df)
    write_json(df_to_records(county_df), os.path.join(DATA_DIR, "counties.json"))

    # ── summary.json ────────────────────────────────────────────────────────
    statewide = aggregate_statewide(df)
    statewide["CountyCount"]   = int(df["CountyName"].nunique())
    statewide["PrecinctCount"] = int(len(df))
    write_json(statewide, os.path.join(DATA_DIR, "summary.json"))

    # ── metadata.json ───────────────────────────────────────────────────────
    meta = {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "source":       f"{SERVER} / {DATABASE}",
    }
    write_json(meta, os.path.join(DATA_DIR, "metadata.json"))

    print("\nDone. Files written to:", DATA_DIR)
    print("Commit and push with GitHub Desktop to update the live dashboard.")

if __name__ == "__main__":
    main()
