import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from joblib import load

from paths import DB, MODELS
from elo import get_current_ratings
from series import get_tier1, remove_cn, compute_rolling_features, ROLLING_FEATURES
from training import FEATURES


# Per-team stat columns the rolling-feature engineer expects on its input.
# Upcoming rows don't have these — they're padded with NaN before stitching.
_STAT_COLS = [
    "winner", "t1_mapwins", "t2_mapwins",
    "t1_rating", "t1_acs", "t1_kills", "t1_deaths", "t1_assists", "t1_fks", "t1_fds",
    "t2_rating", "t2_acs", "t2_kills", "t2_deaths", "t2_assists", "t2_fks", "t2_fds",
]


def _attach_rolling_to_upcoming(upcoming, con):
    """Compute rolling features for upcoming rows by stitching them onto the
    historical series df and running the standard rolling engine.
    """
    history = pd.read_sql("SELECT * FROM series", con)
    # Pad upcoming with NaN stat columns so it has the same schema as history.
    upc = upcoming.copy()
    for col in _STAT_COLS:
        if col not in upc.columns:
            upc[col] = pd.NA
    combined = pd.concat([history, upc[history.columns.intersection(upc.columns).tolist()]], ignore_index=True, sort=False)
    # Reattach any history columns missing from the intersection (defensive).
    for col in history.columns:
        if col not in combined.columns:
            combined[col] = pd.NA
    combined = compute_rolling_features(combined)
    # Pull rolling cols back for the upcoming rows only.
    upc_ids = set(upcoming["match_id"].astype(str))
    combined["match_id"] = combined["match_id"].astype(str)
    upc_with_roll = combined[combined["match_id"].isin(upc_ids)][["match_id"] + ROLLING_FEATURES]
    upcoming = upcoming.copy()
    upcoming["match_id"] = upcoming["match_id"].astype(str)
    return upcoming.merge(upc_with_roll, on="match_id", how="left")


def predict(con=None):
    """Load upcoming matches, predict t1 win probability, compute EV, and save to results table."""
    close = con is None
    if con is None:
        con = sqlite3.connect(DB)

    upcoming = pd.read_sql("SELECT * FROM upcoming", con)
    upcoming = get_tier1(upcoming, con)
    upcoming = remove_cn(upcoming, con)

    if len(upcoming) == 0:
        print("No upcoming tier-1 matches found.")
        if close:
            con.close()
        return pd.DataFrame()

    # Elo
    ratings = get_current_ratings(con)
    upcoming["t1_elo"] = upcoming["t1"].apply(lambda t: ratings.get(t, 1500))
    upcoming["t2_elo"] = upcoming["t2"].apply(lambda t: ratings.get(t, 1500))
    upcoming["elo_diff"] = upcoming["t1_elo"] - upcoming["t2_elo"]
    upcoming["past_diff"] = upcoming["t1_past"].fillna(0) - upcoming["t2_past"].fillna(0)
    upcoming["net_h2h"] = upcoming["net_h2h"].fillna(0)

    # Rolling team-form features from historical series
    upcoming = _attach_rolling_to_upcoming(upcoming, con)

    # Team names for display
    teams = pd.read_sql("SELECT id, fullname FROM teams", con)
    teams_map = dict(zip(teams["id"], teams["fullname"]))

    model = load(MODELS / "series_winner.joblib")

    upcoming["pred_win%"] = model.predict_proba(upcoming[FEATURES].fillna(0))[:, 0]

    upcoming["ev_t1"] = upcoming["pred_win%"] * upcoming["t1_odds"] - (1 - upcoming["pred_win%"])
    upcoming["ev_t2"] = (1 - upcoming["pred_win%"]) * upcoming["t2_odds"] - upcoming["pred_win%"]
    upcoming["bet"] = upcoming.apply(
        lambda r: "t1" if r["ev_t1"] > 0 else ("t2" if r["ev_t2"] > 0 else None),
        axis=1,
    )

    results = upcoming[["match_id", "t1", "t2", "date", "pred_win%", "t1_odds", "t2_odds", "ev_t1", "ev_t2", "bet"]].copy()
    results["t1"] = results["t1"].map(teams_map).fillna(results["t1"])
    results["t2"] = results["t2"].map(teams_map).fillna(results["t2"])

    results.to_sql("results", con, if_exists="replace", index=False)

    if close:
        con.close()

    return results.sort_values("date").reset_index(drop=True)


if __name__ == "__main__":
    df = predict()
    print(df.to_string(index=False))
