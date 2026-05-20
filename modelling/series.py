import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import DB


def between_dates(df, sd, ed):
    return df.loc[(df["date"] >= sd) & (df["date"] <= ed)].copy(deep=True)


# --- Tier-1 / region helpers (SQLite-backed) ---

def get_tier1(df, con=None):
    close = con is None
    if con is None:
        con = sqlite3.connect(DB)
    tier1 = pd.read_sql("SELECT id FROM teams", con)["id"].tolist()
    if close:
        con.close()
    return df.loc[df["t1"].isin(tier1) & df["t2"].isin(tier1)].copy()


def get_region(team, con=None):
    close = con is None
    if con is None:
        con = sqlite3.connect(DB)
    row = pd.read_sql(
        "SELECT region FROM teams WHERE id=? LIMIT 1", con, params=(int(team),)
    )
    if close:
        con.close()
    return row["region"].iloc[0] if len(row) > 0 else "unknown"


def get_international(df, con=None):
    close = con is None
    if con is None:
        con = sqlite3.connect(DB)
    return_df = df.copy()
    return_df["t1_region"] = return_df["t1"].apply(lambda t: get_region(t, con))
    return_df["t2_region"] = return_df["t2"].apply(lambda t: get_region(t, con))
    return_df = return_df.loc[return_df["t1_region"] != return_df["t2_region"]]
    return_df = return_df.drop(columns=["t1_region", "t2_region"])
    if close:
        con.close()
    return return_df.copy()


def get_regional(df, con=None):
    close = con is None
    if con is None:
        con = sqlite3.connect(DB)
    return_df = df.copy()
    return_df["t1_region"] = return_df["t1"].apply(lambda t: get_region(t, con))
    return_df["t2_region"] = return_df["t2"].apply(lambda t: get_region(t, con))
    return_df = return_df.loc[return_df["t1_region"] == return_df["t2_region"]]
    return_df = return_df.drop(columns=["t1_region", "t2_region"])
    if close:
        con.close()
    return return_df.copy()


def remove_cn(df, con=None):
    close = con is None
    if con is None:
        con = sqlite3.connect(DB)
    cn = pd.read_sql("SELECT id FROM teams WHERE region='cn'", con)["id"].tolist()
    if close:
        con.close()
    return df.loc[~(df["t1"].isin(cn) | df["t2"].isin(cn))].copy()
