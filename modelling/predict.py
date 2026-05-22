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
from training import get_series_model_features
from market import vig_opposite_probability
from map_expectations import (
    MAP_EXPECTATION_AUDIT_COLUMNS,
    MAP_EXPECTATION_FEATURES,
    SIMPLE_MAP_ROLLING_FEATURES,
    build_map_expectation_features,
    build_simple_map_summary_features,
    load_map_expectation_models,
)
from maps import build_candidate_map_rows


# Per-team stat columns the rolling-feature engineer expects on its input.
# Upcoming rows don't have these — they're padded with NaN before stitching.
_STAT_COLS = [
    "winner", "t1_mapwins", "t2_mapwins",
    "t1_rating", "t1_acs", "t1_kills", "t1_deaths", "t1_assists", "t1_fks", "t1_fds",
    "t2_rating", "t2_acs", "t2_kills", "t2_deaths", "t2_assists", "t2_fks", "t2_fds",
]

_VETO_COLS = [
    "t1_ban1", "t1_ban2", "t2_ban1", "t2_ban2",
    "t1_pick", "t2_pick", "remaining",
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


def _attach_map_expectations_to_upcoming(upcoming, con):
    """Build Phase 8 map expectation features for upcoming rows.

    Candidate-map features are built from full historical series/maps plus the
    upcoming rows padded as pre-match, no-veto series. The Phase 3 helpers use
    prior-only date lookups, so history after an upcoming row's date does not
    leak into that row.
    """
    history = pd.read_sql("SELECT * FROM series", con)
    maps = pd.read_sql("SELECT * FROM maps", con)
    map_play_model, map_win_model = load_map_expectation_models()

    upc = upcoming.copy()
    upc["match_id"] = upc["match_id"].astype(str)
    for col in _VETO_COLS + _STAT_COLS:
        if col not in upc.columns:
            upc[col] = pd.NA
    for col in history.columns:
        if col not in upc.columns:
            upc[col] = pd.NA

    combined = pd.concat(
        [history, upc.reindex(columns=history.columns)],
        ignore_index=True,
        sort=False,
    )
    candidates = build_candidate_map_rows(combined, maps)
    upcoming_ids = set(upc["match_id"])
    candidates = candidates[candidates["match_id"].astype(str).isin(upcoming_ids)].copy()

    map_features = build_map_expectation_features(
        candidates,
        map_play_model=map_play_model,
        map_win_model=map_win_model,
    )

    merge_cols = [
        "match_id",
        *MAP_EXPECTATION_FEATURES,
        *[c for c in MAP_EXPECTATION_AUDIT_COLUMNS if c in map_features.columns],
    ]
    map_features["match_id"] = map_features["match_id"].astype(str)
    upcoming = upcoming.copy()
    upcoming["match_id"] = upcoming["match_id"].astype(str)
    upcoming = upcoming.merge(map_features[merge_cols], on="match_id", how="left")
    upcoming["map_winshare"] = upcoming["map_winshare"].fillna(0.5)
    upcoming["map_edge"] = upcoming["map_edge"].fillna(0)
    return upcoming


def _attach_simple_map_features_to_upcoming(upcoming, con):
    """Build simple map-history summary features for upcoming rows.

    Same pre-match-safe construction pattern as the map-expectation path: pad
    upcoming as no-veto series, concatenate with historical series, build
    Phase 3 candidate rows (prior-date-only), then aggregate.
    """
    history = pd.read_sql("SELECT * FROM series", con)
    maps = pd.read_sql("SELECT * FROM maps", con)

    upc = upcoming.copy()
    upc["match_id"] = upc["match_id"].astype(str)
    for col in _VETO_COLS + _STAT_COLS:
        if col not in upc.columns:
            upc[col] = pd.NA
    for col in history.columns:
        if col not in upc.columns:
            upc[col] = pd.NA

    combined = pd.concat(
        [history, upc.reindex(columns=history.columns)],
        ignore_index=True,
        sort=False,
    )
    candidates = build_candidate_map_rows(combined, maps)
    upcoming_ids = set(upc["match_id"])
    candidates = candidates[candidates["match_id"].astype(str).isin(upcoming_ids)].copy()

    simple_features = build_simple_map_summary_features(candidates)
    simple_features["match_id"] = simple_features["match_id"].astype(str)

    upcoming = upcoming.copy()
    upcoming["match_id"] = upcoming["match_id"].astype(str)
    upcoming = upcoming.merge(
        simple_features[["match_id"] + SIMPLE_MAP_ROLLING_FEATURES],
        on="match_id",
        how="left",
    )
    for col in SIMPLE_MAP_ROLLING_FEATURES:
        upcoming[col] = upcoming[col].fillna(0)
    return upcoming


def _market_probability(value):
    """Convert stored implied-probability or decimal odds to market probability."""
    if value is None or pd.isna(value):
        return None
    value = float(value)
    if value <= 0:
        return None
    probability = (1 / value) if value > 1 else value
    if not 0 < probability < 1:
        return None
    return probability


def _market_payout(raw_odds, market_probability):
    if raw_odds is not None and pd.notna(raw_odds):
        raw_odds = float(raw_odds)
        if raw_odds > 1:
            return raw_odds - 1
    if market_probability is None:
        return None
    return (1 / market_probability) - 1


def _ev(model_probability, market_probability, payout):
    if market_probability is None or payout is None:
        return pd.NA
    return (model_probability * payout) - (1 - model_probability)


def _attach_market_ev(upcoming):
    """Compute vig-aware market probabilities and EV for both sides."""
    df = upcoming.copy()
    t1_market = []
    t2_market = []
    t1_payout = []
    t2_payout = []

    for _, row in df.iterrows():
        t1_prob = _market_probability(row.get("t1_odds"))
        t2_prob = _market_probability(row.get("t2_odds"))
        if t1_prob is None and t2_prob is not None:
            t1_prob = vig_opposite_probability(t2_prob)
        if t2_prob is None and t1_prob is not None:
            t2_prob = vig_opposite_probability(t1_prob)

        t1_market.append(t1_prob)
        t2_market.append(t2_prob)
        t1_payout.append(_market_payout(row.get("t1_odds"), t1_prob))
        t2_payout.append(_market_payout(row.get("t2_odds"), t2_prob))

    df["t1_market_prob"] = t1_market
    df["t2_market_prob"] = t2_market
    df["ev_t1"] = [
        _ev(model_prob, market_prob, payout)
        for model_prob, market_prob, payout in zip(df["pred_win%"], t1_market, t1_payout)
    ]
    df["ev_t2"] = [
        _ev(1 - model_prob, market_prob, payout)
        for model_prob, market_prob, payout in zip(df["pred_win%"], t2_market, t2_payout)
    ]
    df["bet"] = df.apply(
        lambda r: "t1"
        if pd.notna(r["ev_t1"]) and r["ev_t1"] > 0
        else ("t2" if pd.notna(r["ev_t2"]) and r["ev_t2"] > 0 else None),
        axis=1,
    )
    return df


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

    model = load(MODELS / "series_winner.joblib")
    features = get_series_model_features(model)

    if any(feature in MAP_EXPECTATION_FEATURES for feature in features):
        try:
            upcoming = _attach_map_expectations_to_upcoming(upcoming, con)
        except FileNotFoundError as e:
            raise FileNotFoundError(
                "Upcoming prediction needs Phase 8 map expectation features, "
                f"but map model artifacts are incomplete: {e}"
            ) from e
    if any(feature in SIMPLE_MAP_ROLLING_FEATURES for feature in features):
        upcoming = _attach_simple_map_features_to_upcoming(upcoming, con)

    # Team names for display
    teams = pd.read_sql("SELECT id, fullname FROM teams", con)
    teams_map = dict(zip(teams["id"], teams["fullname"]))

    missing = [c for c in features if c not in upcoming.columns]
    if missing:
        raise ValueError(
            "Upcoming prediction rows are missing required series features: "
            f"{missing}."
        )
    upcoming["pred_win%"] = model.predict_proba(upcoming[features].fillna(0))[:, 1]
    upcoming = _attach_market_ev(upcoming)

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
