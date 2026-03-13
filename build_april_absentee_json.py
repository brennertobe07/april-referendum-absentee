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

# ── CONFIG ───────────────────────────────────────────────────────────────────
SERVER      = r"INSTANCE-1"
DATABASE    = "absentee"
ODBC_DRIVER = "ODBC Driver 17 for SQL Server"

REPO_ROOT = r"C:\Scripts\Python\Python_Absentee\April\april-referendum-absentee"
DATA_DIR  = os.path.join(REPO_ROOT, "data")
# ─────────────────────────────────────────────────────────────────────────────

QUERY_SUMMARY = """
SELECT
    van.CountyName,
    van.PrecinctName,
    COUNT(van.VoterFileVANID) AS TotalMatchedVoters,

    SUM(CASE WHEN dal.BALLOT_RECEIPT_DATE IS NOT NULL
             AND dal.BALLOT_STATUS NOT IN ('Deleted','Not Issued')
             AND (van.likelyparty IN ('sd','ld')
                 OR (van.likelyparty IN ('nd','U','I') AND van.Clarity_DemSupport_26 >= 60))
             THEN 1 ELSE 0 END) AS DemVotedCount,

    SUM(CASE WHEN dal.BALLOT_RECEIPT_DATE IS NOT NULL
             AND dal.BALLOT_STATUS NOT IN ('Deleted','Not Issued')
             AND (van.likelyparty IN ('sr','lr')
                 OR (van.likelyparty IN ('nd','U','I') AND van.Clarity_DemSupport_26 <= 40))
             THEN 1 ELSE 0 END) AS RepVotedCount,

    SUM(CASE WHEN dal.BALLOT_RECEIPT_DATE IS NOT NULL
             AND dal.BALLOT_STATUS NOT IN ('Deleted','Not Issued')
             AND ((van.likelyparty IN ('nd','U','I') AND van.Clarity_DemSupport_26 BETWEEN 41 AND 59)
                 OR van.likelyparty IS NULL
                 OR van.likelyparty NOT IN ('sd','ld','sr','lr','nd','U','I'))
             THEN 1 ELSE 0 END) AS UnknownVotedCount,

    SUM(CASE WHEN dal.BALLOT_RECEIPT_DATE IS NULL AND dal.ONGOING = 'True'
             AND (van.likelyparty IN ('sd','ld')
                 OR (van.likelyparty IN ('nd','U','I') AND van.Clarity_DemSupport_26 >= 60))
             THEN 1 ELSE 0 END) AS DemOutCount,

    SUM(CASE WHEN dal.BALLOT_RECEIPT_DATE IS NULL AND dal.ONGOING = 'True'
             AND (van.likelyparty IN ('sr','lr')
                 OR (van.likelyparty IN ('nd','U','I') AND van.Clarity_DemSupport_26 <= 40))
             THEN 1 ELSE 0 END) AS RepOutCount,

    SUM(CASE WHEN dal.BALLOT_RECEIPT_DATE IS NULL AND dal.ONGOING = 'True'
             AND ((van.likelyparty IN ('nd','U','I') AND van.Clarity_DemSupport_26 BETWEEN 41 AND 59)
                 OR van.likelyparty IS NULL
                 OR van.likelyparty NOT IN ('sd','ld','sr','lr','nd','U','I'))
             THEN 1 ELSE 0 END) AS UnknownOutCount

FROM van
INNER JOIN Daily_Absentee_List dal
    ON van.StateFileID = dal.IDENTIFICATION_NUMBER

GROUP BY van.CountyName, van.PrecinctName
ORDER BY van.CountyName, van.PrecinctName;
"""

QUERY_DAILY = """
SELECT
    CASE
        WHEN CAST(dal.BALLOT_RECEIPT_DATE AS DATE) < '2026-03-06'
        THEN CAST('2026-03-06' AS DATE)
        ELSE CAST(dal.BALLOT_RECEIPT_DATE AS DATE)
    END AS ReturnDate,

    DATENAME(WEEKDAY,
        CASE
            WHEN CAST(dal.BALLOT_RECEIPT_DATE AS DATE) < '2026-03-06'
            THEN CAST('2026-03-06' AS DATE)
            ELSE CAST(dal.BALLOT_RECEIPT_DATE AS DATE)
        END
    ) AS DayOfWeek,

    COUNT(*) AS TotalReturned,

    SUM(CASE WHEN dal.BALLOT_STATUS = 'On Machine' THEN 1 ELSE 0 END) AS InPersonTotal,
    SUM(CASE WHEN dal.BALLOT_STATUS = 'On Machine'
             AND (van.likelyparty IN ('sd','ld')
                 OR (van.likelyparty IN ('nd','U','I') AND van.Clarity_DemSupport_26 >= 60))
             THEN 1 ELSE 0 END) AS InPersonDem,
    SUM(CASE WHEN dal.BALLOT_STATUS = 'On Machine'
             AND (van.likelyparty IN ('sr','lr')
                 OR (van.likelyparty IN ('nd','U','I') AND van.Clarity_DemSupport_26 <= 40))
             THEN 1 ELSE 0 END) AS InPersonRep,
    SUM(CASE WHEN dal.BALLOT_STATUS = 'On Machine'
             AND ((van.likelyparty IN ('nd','U','I') AND van.Clarity_DemSupport_26 BETWEEN 41 AND 59)
                 OR van.likelyparty IS NULL
                 OR van.likelyparty NOT IN ('sd','ld','sr','lr','nd','U','I'))
             THEN 1 ELSE 0 END) AS InPersonUnknown,

    SUM(CASE WHEN dal.BALLOT_STATUS IN ('Marked','Pre-Processed') THEN 1 ELSE 0 END) AS MailTotal,
    SUM(CASE WHEN dal.BALLOT_STATUS IN ('Marked','Pre-Processed')
             AND (van.likelyparty IN ('sd','ld')
                 OR (van.likelyparty IN ('nd','U','I') AND van.Clarity_DemSupport_26 >= 60))
             THEN 1 ELSE 0 END) AS MailDem,
    SUM(CASE WHEN dal.BALLOT_STATUS IN ('Marked','Pre-Processed')
             AND (van.likelyparty IN ('sr','lr')
                 OR (van.likelyparty IN ('nd','U','I') AND van.Clarity_DemSupport_26 <= 40))
             THEN 1 ELSE 0 END) AS MailRep,
    SUM(CASE WHEN dal.BALLOT_STATUS IN ('Marked','Pre-Processed')
             AND ((van.likelyparty IN ('nd','U','I') AND van.Clarity_DemSupport_26 BETWEEN 41 AND 59)
                 OR van.likelyparty IS NULL
                 OR van.likelyparty NOT IN ('sd','ld','sr','lr','nd','U','I'))
             THEN 1 ELSE 0 END) AS MailUnknown

FROM van
INNER JOIN Daily_Absentee_List dal
    ON van.StateFileID = dal.IDENTIFICATION_NUMBER

WHERE dal.BALLOT_RECEIPT_DATE IS NOT NULL
  AND dal.BALLOT_STATUS NOT IN ('Deleted','Not Issued')
  AND YEAR(dal.BALLOT_RECEIPT_DATE) = 2026

GROUP BY
    CASE
        WHEN CAST(dal.BALLOT_RECEIPT_DATE AS DATE) < '2026-03-06'
        THEN CAST('2026-03-06' AS DATE)
        ELSE CAST(dal.BALLOT_RECEIPT_DATE AS DATE)
    END,
    DATENAME(WEEKDAY,
        CASE
            WHEN CAST(dal.BALLOT_RECEIPT_DATE AS DATE) < '2026-03-06'
            THEN CAST('2026-03-06' AS DATE)
            ELSE CAST(dal.BALLOT_RECEIPT_DATE AS DATE)
        END
    )

ORDER BY ReturnDate;
"""

NUM_COLS = [
    "TotalMatchedVoters",
    "DemVotedCount","RepVotedCount","UnknownVotedCount",
    "DemOutCount","RepOutCount","UnknownOutCount",
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
    df = df.copy()
    df["VotedCount"] = df["DemVotedCount"] + df["RepVotedCount"] + df["UnknownVotedCount"]
    df["OutCount"]   = df["DemOutCount"]   + df["RepOutCount"]   + df["UnknownOutCount"]
    return df

def add_cumulative(df):
    df = df.copy()
    df = df.sort_values("ReturnDate").reset_index(drop=True)
    cum_cols = ["TotalReturned","InPersonTotal","MailTotal",
                "InPersonDem","InPersonRep","InPersonUnknown",
                "MailDem","MailRep","MailUnknown"]
    for col in cum_cols:
        df[f"Cum{col}"] = df[col].cumsum().astype(int)
    df["ReturnDate"] = df["ReturnDate"].astype(str)
    return df

def df_to_records(df):
    return json.loads(df.to_json(orient="records"))

def aggregate_counties(df):
    grp_cols = NUM_COLS + ["VotedCount","OutCount"]
    return (
        df.groupby("CountyName")[grp_cols]
        .sum()
        .reset_index()
        .sort_values("CountyName")
    )

def aggregate_statewide(df):
    grp_cols = NUM_COLS + ["VotedCount","OutCount"]
    return df[grp_cols].sum().to_dict()

def write_json(obj, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    print(f"  wrote {path}")

def main():
    print(f"Connecting to {SERVER}\\{DATABASE} ...")
    conn = connect()

    print("Running summary query ...")
    df = pd.read_sql(QUERY_SUMMARY, conn)
    print(f"  {len(df):,} precinct rows")
    df = add_totals(df)

    print("Running daily trend query ...")
    daily_df = pd.read_sql(QUERY_DAILY, conn)
    print(f"  {len(daily_df):,} daily rows")
    conn.close()

    os.makedirs(DATA_DIR, exist_ok=True)

    write_json(df_to_records(df),                                os.path.join(DATA_DIR, "precincts.json"))
    write_json(df_to_records(aggregate_counties(df)),            os.path.join(DATA_DIR, "counties.json"))

    statewide = aggregate_statewide(df)
    statewide["CountyCount"]   = int(df["CountyName"].nunique())
    statewide["PrecinctCount"] = int(len(df))
    write_json(statewide,                                        os.path.join(DATA_DIR, "summary.json"))

    daily_df = add_cumulative(daily_df)
    write_json(df_to_records(daily_df),                          os.path.join(DATA_DIR, "daily.json"))

    write_json({
        "last_updated":  datetime.now().strftime("%Y-%m-%d %H:%M"),
        "source":        f"{SERVER} / {DATABASE}",
        "election_day":  "2026-04-21",
    },                                                           os.path.join(DATA_DIR, "metadata.json"))

    print("\nDone. Commit and push with GitHub Desktop to update the live dashboard.")

if __name__ == "__main__":
    main()
