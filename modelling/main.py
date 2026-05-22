import argparse
import sqlite3
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from joblib import dump, load
from sklearn.metrics import brier_score_loss, log_loss

from paths import DB, MODELS, CONFIG
from elo import compute_elo
from series import between_dates, get_tier1, remove_cn, get_regional, compute_rolling_features, ROLLING_FEATURES
from training import BASELINE_FEATURES, EXTENDED_FEATURES, SIMPLE_FEATURES, train_series_winner_model
from testing import simulate_bets, simulate_bets_best, test_series_winner_model, predict_series_outcomes
from map_expectations import (
    MAP_EXPECTATION_FEATURES,
    MAP_PLAY_SUMMARY_FEATURES,
    SIMPLE_MAP_ROLLING_FEATURES,
    build_map_expectation_features,
    build_map_play_summary_features,
    build_simple_map_summary_features,
    load_map_expectation_models,
    load_map_play_model,
)
from maps import build_candidate_map_rows


_VALID_FEATURE_SETS = ("baseline", "map", "simple")


def init():
    """Compute elo ratings for all series and write them to the series table."""
    with sqlite3.connect(DB) as con:
        compute_elo(con)
    print("Elo ratings computed.")


def _series_features(feature_set="baseline"):
    if feature_set == "map":
        return EXTENDED_FEATURES
    if feature_set == "simple":
        return SIMPLE_FEATURES
    return BASELINE_FEATURES


def _baseline_required_features():
    return ["elo_diff", "net_h2h"] + ROLLING_FEATURES


def _build_map_expectation_features(series_df, maps_df):
    map_play_model, map_win_model = load_map_expectation_models()

    candidates = build_candidate_map_rows(series_df, maps_df)
    return build_map_expectation_features(
        candidates,
        map_play_model=map_play_model,
        map_win_model=map_win_model,
    )


def _build_simple_map_features(series_df, maps_df):
    candidates = build_candidate_map_rows(series_df, maps_df)
    return build_simple_map_summary_features(candidates)


def _load_series_with_features(feature_set="baseline"):
    """Load full series table, compute rolling features on all data, then filter
    to tier-1 regional non-CN rows for training/testing.

    ``feature_set`` selects which (optional) map-derived features are merged
    onto the base series frame before tier-1 filtering. Rolling features and
    Elo are computed on the full dataset (including T2 matches) so that a
    team's form window is populated from all their games; filtering happens
    after so T2 rows don't end up in the training set.
    """
    if feature_set not in _VALID_FEATURE_SETS:
        raise ValueError(f"Unknown feature_set: {feature_set!r}")

    with sqlite3.connect(DB) as con:
        series_df = pd.read_sql("SELECT * FROM series", con)
        series_df["past_diff"] = series_df["t1_past"].fillna(0) - series_df["t2_past"].fillna(0)
        maps_df = (
            pd.read_sql("SELECT * FROM maps", con)
            if feature_set in ("map", "simple")
            else None
        )

        df = compute_rolling_features(series_df)

        if feature_set == "map":
            map_features = _build_map_expectation_features(series_df, maps_df)
            df["match_id"] = df["match_id"].astype(str)
            map_features["match_id"] = map_features["match_id"].astype(str)
            df = df.merge(
                map_features[["match_id"] + MAP_EXPECTATION_FEATURES],
                on="match_id",
                how="left",
            )
            df["map_winshare"] = df["map_winshare"].fillna(0.5)
            df["map_edge"] = df["map_edge"].fillna(0)
        elif feature_set == "simple":
            simple_features = _build_simple_map_features(series_df, maps_df)
            df["match_id"] = df["match_id"].astype(str)
            simple_features["match_id"] = simple_features["match_id"].astype(str)
            df = df.merge(
                simple_features[["match_id"] + SIMPLE_MAP_ROLLING_FEATURES],
                on="match_id",
                how="left",
            )
            for col in SIMPLE_MAP_ROLLING_FEATURES:
                df[col] = df[col].fillna(0)

        df = get_tier1(df, con)
        df = remove_cn(df, con)
        df = get_regional(df, con)

    return df


def _series_frame_for_dates(start_date, end_date, feature_set="baseline"):
    df = _load_series_with_features(feature_set=feature_set)
    df = between_dates(df, start_date, end_date)
    return df.dropna(subset=_baseline_required_features())


def train_series_win_model(start_date, end_date, feature_set="baseline", model_path=None):
    """Train the series winner model on regional tier-1 (non-CN) data in [start_date, end_date]."""
    df = _series_frame_for_dates(start_date, end_date, feature_set=feature_set)
    features = _series_features(feature_set)

    MODELS.mkdir(exist_ok=True)
    model = train_series_winner_model(df, features=features)
    if model_path is None:
        model_path = MODELS / "series_winner.joblib"
    dump(model, model_path)
    print(f"Series winner model saved to {model_path}")
    return model


def test_series_winner(start_date, end_date, feature_set="baseline", model_path=None):
    """Backtest the series winner model on regional tier-1 (non-CN) data in [start_date, end_date]."""
    if model_path is None:
        model_path = MODELS / "series_winner.joblib"
    model = load(model_path)

    df = _series_frame_for_dates(start_date, end_date, feature_set=feature_set)

    predictions = predict_series_outcomes(df, model)
    accuracy = test_series_winner_model(predictions)
    print(f"Accuracy: {accuracy:.2%}")
    simulate_bets(predictions, 1000)
    simulate_bets_best(predictions, 1000)
    return predictions


def _expected_calibration_error(y_true, pred, n_bins=10):
    y = pd.Series(y_true).astype(float)
    p = pd.Series(pred).astype(float)
    ece = 0.0
    for i in range(n_bins):
        lo = i / n_bins
        hi = (i + 1) / n_bins
        mask = (p >= lo) & (p <= hi) if i == n_bins - 1 else (p >= lo) & (p < hi)
        if not mask.any():
            continue
        ece += mask.mean() * abs(p[mask].mean() - y[mask].mean())
    return float(ece)


def _summarize_bets(bets_df, start=1000, betsize=50):
    if bets_df.empty:
        return {
            "bets": 0,
            "bankroll": float(start),
            "bet_accuracy": 0.0,
            "ev": 0.0,
            "min_bankroll": float(start),
            "max_drawdown": 0.0,
        }
    bankroll = float(bets_df["bankroll"].iloc[-1])
    path = pd.concat(
        [pd.Series([float(start)]), bets_df["bankroll"].astype(float)],
        ignore_index=True,
    )
    drawdown = path.cummax() - path
    return {
        "bets": int(len(bets_df)),
        "bankroll": bankroll,
        "bet_accuracy": float(bets_df["correct"].mean()),
        "ev": float((bankroll - start) / len(bets_df) / betsize),
        "min_bankroll": float(path.min()),
        "max_drawdown": float(drawdown.max()),
    }


def _evaluate_series_feature_set(name, train_start, train_end, test_start, test_end,
                                 feature_set="baseline"):
    features = _series_features(feature_set)
    train_df = _series_frame_for_dates(train_start, train_end, feature_set=feature_set)
    test_df = _series_frame_for_dates(test_start, test_end, feature_set=feature_set)
    model = train_series_winner_model(train_df, features=features)
    predictions = predict_series_outcomes(test_df, model)
    y = predictions["winner"].astype(int)
    pred = predictions["pred_win%"]

    print(f"\n{name}")
    avg_bets = simulate_bets(predictions, 1000)
    best_bets = simulate_bets_best(predictions, 1000)

    avg_summary = _summarize_bets(avg_bets)
    best_summary = _summarize_bets(best_bets)
    return {
        "feature_set": name,
        "rows": int(len(predictions)),
        "features": len(features),
        "accuracy": float(test_series_winner_model(predictions)),
        "brier": float(brier_score_loss(y, pred)),
        "log_loss": float(log_loss(y, pred, labels=[0, 1])),
        "calibration_ece": _expected_calibration_error(y, pred),
        "avg_bets": avg_summary["bets"],
        "avg_bankroll": avg_summary["bankroll"],
        "avg_min_bankroll": avg_summary["min_bankroll"],
        "avg_max_drawdown": avg_summary["max_drawdown"],
        "avg_ev": avg_summary["ev"],
        "best_bets": best_summary["bets"],
        "best_bankroll": best_summary["bankroll"],
        "best_min_bankroll": best_summary["min_bankroll"],
        "best_max_drawdown": best_summary["max_drawdown"],
        "best_ev": best_summary["ev"],
    }


def run_phase7_ablations(train_start, train_end, test_start, test_end):
    """Compare the baseline series model against Phase 6 map expectations."""
    results = []
    results.append(_evaluate_series_feature_set(
        "baseline",
        train_start,
        train_end,
        test_start,
        test_end,
        feature_set="baseline",
    ))

    try:
        results.append(_evaluate_series_feature_set(
            "baseline + map expectations",
            train_start,
            train_end,
            test_start,
            test_end,
            feature_set="map",
        ))
    except FileNotFoundError as e:
        print(f"\nSkipping map-expectation ablation: {e}")

    summary = pd.DataFrame(results)
    print("\nPhase 7 ablation summary")
    print(summary.to_string(index=False))
    return summary


def _merge_feature_frame(df, feature_df, feature_cols, fill_values=None):
    """Merge one feature frame by match_id and apply neutral fills."""
    fill_values = fill_values or {}
    missing = [c for c in feature_cols if c not in feature_df.columns]
    if missing:
        raise ValueError(f"Feature frame is missing required columns: {missing}")

    out = df.copy()
    out["match_id"] = out["match_id"].astype(str)
    features = feature_df[["match_id"] + feature_cols].copy()
    features["match_id"] = features["match_id"].astype(str)
    out = out.merge(features, on="match_id", how="left")
    for col in feature_cols:
        out[col] = out[col].fillna(fill_values.get(col, 0))
    return out


def _filter_model_frame(df, con):
    df = get_tier1(df, con)
    df = remove_cn(df, con)
    df = get_regional(df, con)
    return df


def _phase9_eval_from_frame(name, df, train_start, train_end, test_start, test_end,
                            features):
    train_df = between_dates(df, train_start, train_end)
    train_df = train_df.dropna(subset=_baseline_required_features())
    test_df = between_dates(df, test_start, test_end)
    test_df = test_df.dropna(subset=_baseline_required_features())

    model = train_series_winner_model(train_df, features=features)
    predictions = predict_series_outcomes(test_df, model)
    y = predictions["winner"].astype(int)
    pred = predictions["pred_win%"]

    print(f"\n{name}")
    avg_bets = simulate_bets(predictions, 1000)
    best_bets = simulate_bets_best(predictions, 1000)

    avg_summary = _summarize_bets(avg_bets)
    best_summary = _summarize_bets(best_bets)
    return {
        "feature_set": name,
        "rows": int(len(predictions)),
        "features": len(features),
        "accuracy": float(test_series_winner_model(predictions)),
        "brier": float(brier_score_loss(y, pred)),
        "log_loss": float(log_loss(y, pred, labels=[0, 1])),
        "calibration_ece": _expected_calibration_error(y, pred),
        "avg_bets": avg_summary["bets"],
        "avg_bankroll": avg_summary["bankroll"],
        "avg_min_bankroll": avg_summary["min_bankroll"],
        "avg_max_drawdown": avg_summary["max_drawdown"],
        "avg_ev": avg_summary["ev"],
        "best_bets": best_summary["bets"],
        "best_bankroll": best_summary["bankroll"],
        "best_min_bankroll": best_summary["min_bankroll"],
        "best_max_drawdown": best_summary["max_drawdown"],
        "best_ev": best_summary["ev"],
    }


def run_phase9_ablations(train_start, train_end, test_start, test_end):
    """Run the Phase 9 chronological ablation matrix.

    Missing map artifacts are reported as skipped variants so the baseline and
    artifact-independent checks can still run before a full map backfill.
    """
    with sqlite3.connect(DB) as con:
        series_df = pd.read_sql("SELECT * FROM series", con)
        series_df["past_diff"] = series_df["t1_past"].fillna(0) - series_df["t2_past"].fillna(0)
        maps_df = pd.read_sql("SELECT * FROM maps", con)
        base_df = compute_rolling_features(series_df)
        candidates = build_candidate_map_rows(series_df, maps_df)

        print(
            "Phase 9 candidate rows: "
            f"{len(candidates)} across {candidates['match_id'].nunique() if not candidates.empty else 0} series"
        )

        variants = [
            {
                "name": "baseline",
                "features": BASELINE_FEATURES,
                "frame": base_df,
            },
        ]
        skips = []

        simple_features = build_simple_map_summary_features(candidates)
        simple_df = _merge_feature_frame(
            base_df,
            simple_features,
            SIMPLE_MAP_ROLLING_FEATURES,
            fill_values={c: 0 for c in SIMPLE_MAP_ROLLING_FEATURES},
        )
        variants.append({
            "name": "baseline + simple map rolling",
            "features": BASELINE_FEATURES + SIMPLE_MAP_ROLLING_FEATURES,
            "frame": simple_df,
        })

        try:
            map_play_model = load_map_play_model()
        except FileNotFoundError as e:
            skips.append(("baseline + map-play expectation", str(e)))
        else:
            map_play_features = build_map_play_summary_features(
                candidates,
                map_play_model=map_play_model,
            )
            map_play_df = _merge_feature_frame(
                base_df,
                map_play_features,
                MAP_PLAY_SUMMARY_FEATURES,
                fill_values={c: 0 for c in MAP_PLAY_SUMMARY_FEATURES},
            )
            variants.append({
                "name": "baseline + map-play expectation",
                "features": BASELINE_FEATURES + MAP_PLAY_SUMMARY_FEATURES,
                "frame": map_play_df,
            })

        try:
            map_play_model, map_win_model = load_map_expectation_models()
        except FileNotFoundError as e:
            skips.append(("baseline + map-win expectation", str(e)))
            skips.append(("final selected feature set", str(e)))
        else:
            expectation_features = build_map_expectation_features(
                candidates,
                map_play_model=map_play_model,
                map_win_model=map_win_model,
            )
            map_win_df = _merge_feature_frame(
                base_df,
                expectation_features,
                ["map_winshare"],
                fill_values={"map_winshare": 0.5},
            )
            final_df = _merge_feature_frame(
                base_df,
                expectation_features,
                MAP_EXPECTATION_FEATURES,
                fill_values={"map_winshare": 0.5, "map_edge": 0},
            )
            variants.extend([
                {
                    "name": "baseline + map-win expectation",
                    "features": BASELINE_FEATURES + ["map_winshare"],
                    "frame": map_win_df,
                },
                {
                    "name": "final selected feature set",
                    "features": BASELINE_FEATURES + MAP_EXPECTATION_FEATURES,
                    "frame": final_df,
                },
            ])

        results = []
        for variant in variants:
            model_df = _filter_model_frame(variant["frame"], con)
            results.append(_phase9_eval_from_frame(
                variant["name"],
                model_df,
                train_start,
                train_end,
                test_start,
                test_end,
                variant["features"],
            ))

    if skips:
        print("\nSkipped Phase 9 variants")
        for name, reason in skips:
            print(f"- {name}: {reason}")

    summary = pd.DataFrame(results)
    print("\nPhase 9 ablation summary")
    print(summary.to_string(index=False))
    return summary


def _parse_args():
    parser = argparse.ArgumentParser(description="Train and backtest the series winner model.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--with-map-features",
        action="store_true",
        help="Train/test the series model with Phase 6 map expectation features.",
    )
    group.add_argument(
        "--with-simple-map-features",
        action="store_true",
        help=(
            "Train/test the series model with the raw aggregated map-history "
            "features. No map-play / map-win artifacts required."
        ),
    )
    parser.add_argument(
        "--phase7-ablations",
        action="store_true",
        help="Run baseline vs map-expectation series-model ablations without saving a model.",
    )
    parser.add_argument(
        "--phase9-ablations",
        action="store_true",
        help="Run the Phase 9 chronological ablation matrix without saving a model.",
    )
    parser.add_argument(
        "--skip-init",
        action="store_true",
        help="Skip recomputing Elo before training/backtesting.",
    )
    return parser.parse_args()


def _feature_set_from_args(args):
    if args.with_map_features:
        return "map"
    if args.with_simple_map_features:
        return "simple"
    return "baseline"


if __name__ == "__main__":
    args = _parse_args()
    with open(CONFIG, "rb") as f:
        cfg = tomllib.load(f)

    if not args.skip_init:
        init()

    if args.phase7_ablations:
        run_phase7_ablations(
            cfg["modelling"]["vct_2023_start"],
            cfg["modelling"]["vct_2025_end"],
            cfg["modelling"]["vct_2026_start"],
            cfg["modelling"]["vct_2026_end"],
        )
        raise SystemExit(0)

    if args.phase9_ablations:
        run_phase9_ablations(
            cfg["modelling"]["vct_2023_start"],
            cfg["modelling"]["vct_2025_end"],
            cfg["modelling"]["vct_2026_start"],
            cfg["modelling"]["vct_2026_end"],
        )
        raise SystemExit(0)

    feature_set = _feature_set_from_args(args)

    try:
        train_series_win_model(
            cfg["modelling"]["vct_2023_start"],
            cfg["modelling"]["vct_2026_start"],
            feature_set=feature_set,
        )
        test_series_winner(
            cfg["modelling"]["vct_2026_start"],
            cfg["modelling"]["vct_2026_end"],
            feature_set=feature_set,
        )
    except FileNotFoundError as e:
        if feature_set == "baseline":
            raise
        if feature_set == "map":
            print(f"Map-feature series model not trained: {e}")
            print("Backfill maps and train map_play.py/map_win.py before using --with-map-features.")
        else:
            print(f"Simple-map-feature series model not trained: {e}")
        raise SystemExit(1)
