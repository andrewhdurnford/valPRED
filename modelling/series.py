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


# --- Rolling team-form features ---

ROLLING_FEATURES = ["rating_diff", "acs_diff", "fkpm_diff", "fdpm_diff", "winrate_diff"]


def compute_rolling_features(df, window=10, min_periods=3):
    """Append rolling team-form diff features to df.

    For each series, looks at each team's prior `window` series (closed='left',
    so no leakage from the current row). Counts are normalised to per-map rates
    using t1_mapwins + t2_mapwins so BO3/BO5 are comparable.

    Required cols on df: match_id, t1, t2, date, winner, t1_mapwins, t2_mapwins,
    and per-team t{1,2}_{rating,acs,kills,deaths,assists,fks,fds}. To compute
    features for rows not yet played (e.g. upcoming matches), concatenate them
    onto the historical series df with stats columns as NaN — `closed='left'`
    means a row's own stats never enter its own feature.

    Adds: rating_diff, acs_diff, fkpm_diff, fdpm_diff, winrate_diff.
    Rows where a team has fewer than `min_periods` prior series get NaN diffs.
    """
    df = df.copy()
    df["match_id"] = df["match_id"].astype(str)
    df["date"] = pd.to_datetime(df["date"])

    total_maps = df["t1_mapwins"].fillna(0) + df["t2_mapwins"].fillna(0)
    total_maps = total_maps.where(total_maps > 0)  # zeros -> NaN so division is NaN, not inf

    def team_long(prefix):
        win = df["winner"].astype("Int64") if prefix == "t1" else (1 - df["winner"].astype("Int64"))
        return pd.DataFrame({
            "match_id": df["match_id"].values,
            "date": df["date"].values,
            "team": df[prefix].values,
            "rating": df[f"{prefix}_rating"].values,
            "acs": df[f"{prefix}_acs"].values,
            "kpm": (df[f"{prefix}_kills"] / total_maps).values,
            "dpm": (df[f"{prefix}_deaths"] / total_maps).values,
            "apm": (df[f"{prefix}_assists"] / total_maps).values,
            "fkpm": (df[f"{prefix}_fks"] / total_maps).values,
            "fdpm": (df[f"{prefix}_fds"] / total_maps).values,
            "win": win.values,
        })

    long = pd.concat([team_long("t1"), team_long("t2")], ignore_index=True)
    long = long.sort_values(["team", "date"], kind="stable").reset_index(drop=True)

    metric_cols = ["rating", "acs", "kpm", "dpm", "apm", "fkpm", "fdpm", "win"]
    rolling = (
        long.groupby("team", group_keys=False)[metric_cols]
        .apply(lambda g: g.rolling(window=window, min_periods=min_periods, closed="left").mean())
    )
    rolling.columns = [f"r_{c}" for c in metric_cols]
    long_roll = pd.concat(
        [long[["match_id", "team"]].reset_index(drop=True), rolling.reset_index(drop=True)],
        axis=1,
    )

    t1_roll = long_roll.rename(columns={"team": "t1", **{f"r_{c}": f"t1_r_{c}" for c in metric_cols}})
    t2_roll = long_roll.rename(columns={"team": "t2", **{f"r_{c}": f"t2_r_{c}" for c in metric_cols}})
    df = df.merge(t1_roll, on=["match_id", "t1"], how="left")
    df = df.merge(t2_roll, on=["match_id", "t2"], how="left")

    df["rating_diff"] = df["t1_r_rating"] - df["t2_r_rating"]
    df["acs_diff"] = df["t1_r_acs"] - df["t2_r_acs"]
    df["fkpm_diff"] = df["t1_r_fkpm"] - df["t2_r_fkpm"]
    df["fdpm_diff"] = df["t1_r_fdpm"] - df["t2_r_fdpm"]
    df["winrate_diff"] = df["t1_r_win"] - df["t2_r_win"]

    return df
