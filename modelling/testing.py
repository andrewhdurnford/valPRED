import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market import vig_opposite_probability


MIN_MARKET_PROBABILITY = 0.1


def _valid_market_probability(probability):
    return probability is not None and pd.notna(probability) and 0 < probability < 1


def predict_series_outcomes(sds, series_winner_model):
    from training import get_series_model_features

    return_df = sds.copy(deep=True)
    features = get_series_model_features(series_winner_model)
    missing = [c for c in features if c not in return_df.columns]
    if missing:
        raise ValueError(f"Prediction data is missing required series features: {missing}")
    X = return_df[features].fillna(0)
    return_df["pred_win%"] = series_winner_model.predict_proba(X)[:, 1]
    return_df = return_df[["match_id", "t1", "t2", "date", "winner", "pred_win%", "odds", "best_odds", "worst_odds"]]
    return return_df.copy(deep=True)


def simulate_bets(predictions, bankroll):
    bets = 0
    won = 0
    lost = 0
    betsize = 50
    start = bankroll
    dog = 0
    predictions = predictions.sort_values(by="date", ascending=True)
    cols = ["match_id", "t1", "t2", "correct", "date", "bankroll", "betsize", "$win", "$lose", "best", "worst"]
    data = []
    for _, row in predictions.iterrows():
        t1_odds = row["odds"]
        t2_odds = vig_opposite_probability(t1_odds)

        if _valid_market_probability(t1_odds) and row["pred_win%"] > t1_odds and t1_odds > MIN_MARKET_PROBABILITY:
            bets += 1
            if t1_odds < 0.5:
                dog += 1
            if row["winner"]:
                bankroll += (betsize * 1 / t1_odds) - betsize
                won += 1
                data.append([row["match_id"], row["t1"], row["t2"], True, row["date"], bankroll, betsize, (betsize * 1 / t1_odds) - betsize, betsize, t1_odds, t2_odds])
            else:
                bankroll -= betsize
                lost += 1
                data.append([row["match_id"], row["t1"], row["t2"], False, row["date"], bankroll, betsize, (betsize * 1 / t1_odds) - betsize, betsize, t1_odds, t2_odds])

        elif _valid_market_probability(t2_odds) and (1 - row["pred_win%"]) > t2_odds and t2_odds > MIN_MARKET_PROBABILITY:
            bets += 1
            if t2_odds < 0.5:
                dog += 1
            if row["winner"]:
                bankroll -= betsize
                lost += 1
                data.append([row["match_id"], row["t1"], row["t2"], False, row["date"], bankroll, betsize, (betsize * 1 / t2_odds) - betsize, betsize, t1_odds, t2_odds])
            else:
                bankroll += (betsize * 1 / t2_odds) - betsize
                won += 1
                data.append([row["match_id"], row["t1"], row["t2"], True, row["date"], bankroll, betsize, (betsize * 1 / t2_odds) - betsize, betsize, t1_odds, t2_odds])

    accuracy = round(won / (won + lost) * 100, 2) if (won + lost) > 0 else 0
    expected_value = round((bankroll - start) / bets / betsize, 2) if bets > 0 else 0
    dog_pct = round(dog / bets * 100, 2) if bets > 0 else 0
    print(
        f"Bets placed: {bets}  Ending bankroll: ${round(bankroll, 2)}"
        f"  Accuracy: {accuracy}%  EV: {expected_value}  Dog: {dog_pct}%"
    )
    df = pd.DataFrame(data=data, columns=cols)
    return df


def simulate_bets_best(predictions, bankroll):
    bets = 0
    won = 0
    lost = 0
    betsize = 50
    start = bankroll
    dog = 0
    predictions = predictions.sort_values(by="date", ascending=True)
    data = []
    for _, row in predictions.iterrows():
        t1_odds = row["worst_odds"]
        t2_odds = vig_opposite_probability(row["best_odds"])

        if _valid_market_probability(t1_odds) and row["pred_win%"] > t1_odds and t1_odds > MIN_MARKET_PROBABILITY:
            bets += 1
            if t1_odds < 0.5:
                dog += 1
            if row["winner"]:
                bankroll += (betsize * 1 / t1_odds) - betsize
                won += 1
                data.append([row["match_id"], row["t1"], row["t2"], True, row["date"], bankroll, betsize, (betsize * 1 / t1_odds) - betsize, betsize, row["best_odds"], row["worst_odds"]])
            else:
                bankroll -= betsize
                lost += 1
                data.append([row["match_id"], row["t1"], row["t2"], False, row["date"], bankroll, betsize, (betsize * 1 / t1_odds) - betsize, betsize, row["best_odds"], row["worst_odds"]])

        elif _valid_market_probability(t2_odds) and (1 - row["pred_win%"]) > t2_odds and t2_odds > MIN_MARKET_PROBABILITY:
            bets += 1
            if t2_odds < 0.5:
                dog += 1
            if row["winner"]:
                bankroll -= betsize
                lost += 1
                data.append([row["match_id"], row["t1"], row["t2"], False, row["date"], bankroll, betsize, (betsize * 1 / t2_odds) - betsize, betsize, row["best_odds"], row["worst_odds"]])
            else:
                bankroll += (betsize * 1 / t2_odds) - betsize
                won += 1
                data.append([row["match_id"], row["t1"], row["t2"], True, row["date"], bankroll, betsize, (betsize * 1 / t2_odds) - betsize, betsize, row["best_odds"], row["worst_odds"]])

    accuracy = round(won / (won + lost) * 100, 2) if (won + lost) > 0 else 0
    expected_value = round((bankroll - start) / bets / betsize, 2) if bets > 0 else 0
    dog_pct = round(dog / bets * 100, 2) if bets > 0 else 0
    print(
        f"Bets placed: {bets}  Ending bankroll: ${round(bankroll, 2)}"
        f"  Accuracy: {accuracy}%  EV: {expected_value}  Dog: {dog_pct}%"
    )
    df = pd.DataFrame(
        data=data,
        columns=["match_id", "t1", "t2", "correct", "date", "bankroll", "betsize", "earnings", "bet_size", "best_odds", "worst_odds"],
    )
    return df


def test_series_winner_model(sp):
    df = sp.copy(deep=True)
    correct = len(
        df.loc[
            ((df["pred_win%"] > 0.5) & (df["winner"] == True))
            | ((df["pred_win%"] < 0.5) & (df["winner"] == False))
        ].index
    )
    return correct / len(df.index)
