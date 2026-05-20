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
from series import get_tier1, remove_cn


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

    # Team names for display
    teams = pd.read_sql("SELECT id, fullname FROM teams", con)
    teams_map = dict(zip(teams["id"], teams["fullname"]))

    model = load(MODELS / "series_winner.joblib")

    features = ["elo_diff", "net_h2h", "past_diff"]
    upcoming["pred_win%"] = model.predict_proba(upcoming[features])[:, 0]

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
