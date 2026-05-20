import sqlite3
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from joblib import dump, load

from paths import DB, MODELS, CONFIG
from elo import compute_elo
from series import between_dates, get_tier1, remove_cn, get_regional, compute_rolling_features, ROLLING_FEATURES
from training import train_series_winner_model
from testing import simulate_bets, simulate_bets_best, test_series_winner_model, predict_series_outcomes


def init():
    """Compute elo ratings for all series and write them to the series table."""
    with sqlite3.connect(DB) as con:
        compute_elo(con)
    print("Elo ratings computed.")


def _load_series_with_features():
    """Load full series table, filter to tier-1 regional non-CN, and attach
    rolling features. Returns the dataframe with all date rows intact so the
    caller can window after features are computed.
    """
    with sqlite3.connect(DB) as con:
        df = pd.read_sql("SELECT * FROM series", con)
        df = get_tier1(df, con)
        df = remove_cn(df, con)
        df = get_regional(df, con)

    df["past_diff"] = df["t1_past"].fillna(0) - df["t2_past"].fillna(0)
    df = compute_rolling_features(df)
    return df


def train_series_win_model(start_date, end_date):
    """Train the series winner model on regional tier-1 (non-CN) data in [start_date, end_date]."""
    df = _load_series_with_features()
    df = between_dates(df, start_date, end_date)
    df = df.dropna(subset=["elo_diff", "net_h2h"] + ROLLING_FEATURES)

    MODELS.mkdir(exist_ok=True)
    model = train_series_winner_model(df)
    dump(model, MODELS / "series_winner.joblib")
    print(f"Series winner model saved to {MODELS / 'series_winner.joblib'}")


def test_series_winner(start_date, end_date):
    """Backtest the series winner model on regional tier-1 (non-CN) data in [start_date, end_date]."""
    model = load(MODELS / "series_winner.joblib")

    df = _load_series_with_features()
    df = between_dates(df, start_date, end_date)
    df = df.dropna(subset=["elo_diff", "net_h2h"] + ROLLING_FEATURES)

    predictions = predict_series_outcomes(df, model)
    accuracy = test_series_winner_model(predictions)
    print(f"Accuracy: {accuracy:.2%}")
    simulate_bets(predictions, 1000)
    simulate_bets_best(predictions, 1000)


if __name__ == "__main__":
    with open(CONFIG, "rb") as f:
        cfg = tomllib.load(f)

    init()
    train_series_win_model(
        cfg["modelling"]["vct_2023_start"],
        cfg["modelling"]["vct_2024_start"],
    )
    test_series_winner(
        cfg["modelling"]["vct_2024_start"],
        cfg["modelling"]["vct_2024_end"],
    )
