"""Collapse map-play and map-win probabilities into series-level features.

Phase 6 of the map-by-map reintroduction plan. The functions here keep map
state as pre-match expectations:

    expected_t1_maps = sum(P(map played) * P(t1 wins map))
    expected_t2_maps = sum(P(map played) * P(t2 wins map))

The collapsed features are intended for the Phase 7 series model extension.
They do not use actual played maps, veto labels, or same-match map outcomes as
features.
"""

import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import load

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import DB
try:
    from map_play import MAP_PLAY_MODEL_PATH, predict_map_play_probabilities
    from map_win import MAP_WIN_MODEL_PATH, predict_map_win_probabilities
    from maps import build_candidate_map_rows
except ImportError:
    from modelling.map_play import MAP_PLAY_MODEL_PATH, predict_map_play_probabilities
    from modelling.map_win import MAP_WIN_MODEL_PATH, predict_map_win_probabilities
    from modelling.maps import build_candidate_map_rows


MAP_PLAY_PROB_COL = "map_play_prob"
MAP_WIN_PROB_COL = "map_win_prob"

MAP_EXPECTATION_FEATURES = [
    "map_winshare",
    "map_edge",
]

SIMPLE_MAP_ROLLING_FEATURES = [
    "simple_map_wr_diff",
    "simple_round_share_diff",
    "simple_pistol_diff",
    "simple_map_rating_diff",
    "simple_map_acs_diff",
    "simple_map_fk_net_diff",
    "simple_sample_size_diff",
    "simple_h2h_map_edge",
]

MAP_PLAY_SUMMARY_FEATURES = [
    "map_play_mass",
    "map_play_top3_mass",
    "map_play_prob_std",
]

MAP_EXPECTATION_AUDIT_COLUMNS = [
    "expected_t1_maps",
    "expected_t2_maps",
    "expected_total_maps",
    "candidate_maps",
    "map_play_mass",
]

MAP_EXPECTATION_COLUMNS = MAP_EXPECTATION_FEATURES + MAP_EXPECTATION_AUDIT_COLUMNS

_SERIES_ID_COLUMNS = ["match_id", "date", "t1", "t2"]


def _coerce_probability(df, col):
    values = pd.to_numeric(df[col], errors="coerce")
    if values.isna().any():
        missing = int(values.isna().sum())
        raise ValueError(f"{col} contains {missing} missing/non-numeric probabilities")
    invalid = (values < 0) | (values > 1)
    if invalid.any():
        raise ValueError(f"{col} contains probabilities outside [0, 1]")
    return values.astype(float)


def score_candidate_map_expectations(
    candidate_df,
    map_play_model=None,
    map_win_model=None,
    map_play_prob_col=MAP_PLAY_PROB_COL,
    map_win_prob_col=MAP_WIN_PROB_COL,
):
    """Return candidate rows with map-play/map-win probabilities and EV pieces.

    If ``map_play_prob_col`` or ``map_win_prob_col`` already exist, those
    values are used. Otherwise the corresponding model must be supplied.
    This makes the aggregation easy to test with pre-scored synthetic rows
    while still supporting the real Phase 4/5 artifacts.
    """
    df = candidate_df.copy()
    if df.empty:
        for col in [
            map_play_prob_col,
            map_win_prob_col,
            "expected_t1_map_contribution",
            "expected_t2_map_contribution",
            "candidate_map_edge",
        ]:
            df[col] = pd.Series(dtype=float)
        return df

    required = {"match_id", "map"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"candidate_df missing required columns: {sorted(missing)}")

    if map_play_prob_col not in df.columns:
        if map_play_model is None:
            raise ValueError("map_play_model is required when map_play_prob is absent")
        df[map_play_prob_col] = predict_map_play_probabilities(map_play_model, df)

    if map_win_prob_col not in df.columns:
        if map_win_model is None:
            raise ValueError("map_win_model is required when map_win_prob is absent")
        df[map_win_prob_col] = predict_map_win_probabilities(map_win_model, df)

    map_play_prob = _coerce_probability(df, map_play_prob_col)
    map_win_prob = _coerce_probability(df, map_win_prob_col)

    df[map_play_prob_col] = map_play_prob
    df[map_win_prob_col] = map_win_prob
    df["expected_t1_map_contribution"] = map_play_prob * map_win_prob
    df["expected_t2_map_contribution"] = map_play_prob * (1.0 - map_win_prob)
    df["candidate_map_edge"] = (
        df["expected_t1_map_contribution"] - df["expected_t2_map_contribution"]
    )

    return df


def collapse_map_expectations(
    scored_candidate_df,
    map_play_prob_col=MAP_PLAY_PROB_COL,
):
    """Aggregate scored candidate-map rows into one feature row per series."""
    df = scored_candidate_df.copy()
    if df.empty:
        return pd.DataFrame(columns=_SERIES_ID_COLUMNS + MAP_EXPECTATION_COLUMNS)

    required = {
        "match_id",
        "expected_t1_map_contribution",
        "expected_t2_map_contribution",
        "candidate_map_edge",
        map_play_prob_col,
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"scored_candidate_df missing required columns: {sorted(missing)}")

    group_cols = [c for c in _SERIES_ID_COLUMNS if c in df.columns]
    if "match_id" not in group_cols:
        raise ValueError("scored_candidate_df must include match_id")

    agg = (
        df.groupby(group_cols, dropna=False, sort=False)
        .agg(
            expected_t1_maps=("expected_t1_map_contribution", "sum"),
            expected_t2_maps=("expected_t2_map_contribution", "sum"),
            map_edge=("candidate_map_edge", "sum"),
            candidate_maps=("map", "count"),
            map_play_mass=(map_play_prob_col, "sum"),
        )
        .reset_index()
    )
    agg["expected_total_maps"] = agg["expected_t1_maps"] + agg["expected_t2_maps"]
    denom = agg["expected_total_maps"].where(agg["expected_total_maps"] > 0)
    agg["map_winshare"] = (agg["expected_t1_maps"] / denom).fillna(0.5)

    ordered = group_cols + [
        "map_winshare",
        "map_edge",
        "expected_t1_maps",
        "expected_t2_maps",
        "expected_total_maps",
        "candidate_maps",
        "map_play_mass",
    ]
    return agg[ordered]


def build_map_expectation_features(
    candidate_df,
    map_play_model=None,
    map_win_model=None,
    map_play_prob_col=MAP_PLAY_PROB_COL,
    map_win_prob_col=MAP_WIN_PROB_COL,
):
    """Score candidate maps and collapse them to series-level map features."""
    scored = score_candidate_map_expectations(
        candidate_df,
        map_play_model=map_play_model,
        map_win_model=map_win_model,
        map_play_prob_col=map_play_prob_col,
        map_win_prob_col=map_win_prob_col,
    )
    return collapse_map_expectations(
        scored,
        map_play_prob_col=map_play_prob_col,
    )


def build_simple_map_summary_features(candidate_df):
    """Aggregate raw Phase 3 map-history features to one row per series.

    These are deliberately simple unweighted means across candidate maps. They
    support Phase 9's "baseline plus simple map rolling features" ablation
    without depending on trained map-play or map-win artifacts.
    """
    df = candidate_df.copy()
    if df.empty:
        return pd.DataFrame(columns=_SERIES_ID_COLUMNS + SIMPLE_MAP_ROLLING_FEATURES)

    if "match_id" not in df.columns:
        raise ValueError("candidate_df must include match_id")

    for col in [
        "map_wr_diff",
        "round_share_diff",
        "pistol_diff",
        "map_rating_diff",
        "map_acs_diff",
        "map_fk_net_diff",
        "sample_size_diff",
        "h2h_map_t1_winrate",
    ]:
        if col not in df.columns:
            df[col] = np.nan

    df["simple_h2h_map_edge_source"] = df["h2h_map_t1_winrate"] - 0.5
    group_cols = [c for c in _SERIES_ID_COLUMNS if c in df.columns]
    agg = (
        df.groupby(group_cols, dropna=False, sort=False)
        .agg(
            simple_map_wr_diff=("map_wr_diff", "mean"),
            simple_round_share_diff=("round_share_diff", "mean"),
            simple_pistol_diff=("pistol_diff", "mean"),
            simple_map_rating_diff=("map_rating_diff", "mean"),
            simple_map_acs_diff=("map_acs_diff", "mean"),
            simple_map_fk_net_diff=("map_fk_net_diff", "mean"),
            simple_sample_size_diff=("sample_size_diff", "mean"),
            simple_h2h_map_edge=("simple_h2h_map_edge_source", "mean"),
        )
        .reset_index()
    )
    return agg[group_cols + SIMPLE_MAP_ROLLING_FEATURES]


def build_map_play_summary_features(
    candidate_df,
    map_play_model=None,
    map_play_prob_col=MAP_PLAY_PROB_COL,
):
    """Aggregate map-play probabilities to one row per series.

    This supports Phase 9's map-play-only ablation. It intentionally does not
    require map-win probabilities, so it can run before the full maps backfill
    produces a reliable Phase 5 artifact.
    """
    df = candidate_df.copy()
    if df.empty:
        return pd.DataFrame(columns=_SERIES_ID_COLUMNS + MAP_PLAY_SUMMARY_FEATURES)

    if "match_id" not in df.columns:
        raise ValueError("candidate_df must include match_id")

    if map_play_prob_col not in df.columns:
        if map_play_model is None:
            raise ValueError("map_play_model is required when map_play_prob is absent")
        df[map_play_prob_col] = predict_map_play_probabilities(map_play_model, df)

    df[map_play_prob_col] = _coerce_probability(df, map_play_prob_col)
    group_cols = [c for c in _SERIES_ID_COLUMNS if c in df.columns]

    def _top3_mass(values):
        return float(pd.Series(values).nlargest(3).sum())

    agg = (
        df.groupby(group_cols, dropna=False, sort=False)
        .agg(
            map_play_mass=(map_play_prob_col, "sum"),
            map_play_top3_mass=(map_play_prob_col, _top3_mass),
            map_play_prob_std=(map_play_prob_col, "std"),
        )
        .reset_index()
    )
    return agg[group_cols + MAP_PLAY_SUMMARY_FEATURES]


def build_map_expectation_features_from_tables(series_df, maps_df, map_play_model,
                                               map_win_model):
    """Build Phase 3 candidate rows from tables, then return Phase 6 features."""
    candidate_df = build_candidate_map_rows(series_df, maps_df)
    return build_map_expectation_features(
        candidate_df,
        map_play_model=map_play_model,
        map_win_model=map_win_model,
    )


def load_map_play_model(map_play_path=MAP_PLAY_MODEL_PATH):
    """Load the Phase 4 map-play artifact."""
    if not Path(map_play_path).exists():
        raise FileNotFoundError(f"Required map-play model file missing: {map_play_path}")
    return load(map_play_path)


def load_map_win_model(map_win_path=MAP_WIN_MODEL_PATH):
    """Load the Phase 5 map-win artifact."""
    if not Path(map_win_path).exists():
        raise FileNotFoundError(f"Required map-win model file missing: {map_win_path}")
    return load(map_win_path)


def load_map_expectation_models(map_play_path=MAP_PLAY_MODEL_PATH,
                                map_win_path=MAP_WIN_MODEL_PATH):
    """Load the Phase 4 and Phase 5 artifacts needed for real aggregation."""
    return load_map_play_model(map_play_path), load_map_win_model(map_win_path)


def load_candidate_rows_from_db():
    """Load current DB tables and build Phase 3 candidate rows."""
    with sqlite3.connect(DB) as con:
        series_df = pd.read_sql("SELECT * FROM series", con)
        maps_df = pd.read_sql("SELECT * FROM maps", con)
    return build_candidate_map_rows(series_df, maps_df)


if __name__ == "__main__":
    try:
        play_model, win_model = load_map_expectation_models()
    except FileNotFoundError as e:
        print(f"Map expectation features not built: {e}")
        print("Train map_play.py and map_win.py after the maps table is backfilled, then rerun.")
    else:
        candidates = load_candidate_rows_from_db()
        features = build_map_expectation_features(
            candidates,
            map_play_model=play_model,
            map_win_model=win_model,
        )
        print(f"Candidate rows scored: {len(candidates)}")
        print(f"Series feature rows built: {len(features)}")
        print(features.head(20).to_string(index=False))
