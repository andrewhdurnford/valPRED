"""Train and evaluate the Phase 4 map-play model.

The model estimates P(candidate map is played) from pre-match candidate-map
features. It intentionally does not use same-match veto labels such as
``picked_by_t1``, ``banned_by_t2``, or ``decider`` as features; those columns
exist only to label and audit historical rows.
"""

import sqlite3
import sys
import tomllib
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import dump
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import CONFIG, DB, MODELS
try:
    from maps import build_candidate_map_rows
except ImportError:
    from modelling.maps import build_candidate_map_rows


MAP_PLAY_MODEL_PATH = MODELS / "map_play.joblib"

MAP_PLAY_CATEGORICAL_FEATURES = ["map"]

MAP_PLAY_NUMERIC_FEATURES = [
    "in_pool",
    "elo_diff", "net_h2h", "past_diff",
    "t1_pick_rate", "t2_pick_rate",
    "t1_ban_rate", "t2_ban_rate",
    "t1_play_rate", "t2_play_rate",
    "pick_rate_sum", "pick_rate_diff",
    "ban_rate_sum", "ban_rate_diff",
    "play_rate_sum", "play_rate_diff",
    "veto_history_n_t1", "veto_history_n_t2",
    "veto_history_n_min", "veto_history_n_diff",
    "h2h_map_count", "h2h_map_t1_edge",
    "map_wr_diff", "round_share_diff", "pistol_diff",
    "map_rating_diff", "map_acs_diff", "map_fk_net_diff",
    "map_kpm_diff", "map_dpm_diff", "sample_size_diff",
    "t1_sm_count", "t2_sm_count",
]

MAP_PLAY_FEATURES = MAP_PLAY_CATEGORICAL_FEATURES + MAP_PLAY_NUMERIC_FEATURES


def _with_derived_map_play_features(candidate_df):
    """Return candidate rows with Phase 4 model features added.

    Missing history is kept as NaN here and handled by the model pipeline's
    imputer. Derived edge features are neutral at 0 when history is missing.
    """
    df = candidate_df.copy()
    for col in MAP_PLAY_CATEGORICAL_FEATURES + MAP_PLAY_NUMERIC_FEATURES:
        if col not in df.columns:
            df[col] = np.nan
    if "h2h_map_t1_winrate" not in df.columns:
        df["h2h_map_t1_winrate"] = np.nan

    df["pick_rate_sum"] = df["t1_pick_rate"] + df["t2_pick_rate"]
    df["pick_rate_diff"] = df["t1_pick_rate"] - df["t2_pick_rate"]
    df["ban_rate_sum"] = df["t1_ban_rate"] + df["t2_ban_rate"]
    df["ban_rate_diff"] = df["t1_ban_rate"] - df["t2_ban_rate"]
    df["play_rate_sum"] = df["t1_play_rate"] + df["t2_play_rate"]
    df["play_rate_diff"] = df["t1_play_rate"] - df["t2_play_rate"]
    df["veto_history_n_min"] = df[["veto_history_n_t1", "veto_history_n_t2"]].min(axis=1)
    df["veto_history_n_diff"] = df["veto_history_n_t1"] - df["veto_history_n_t2"]
    df["h2h_map_t1_edge"] = df["h2h_map_t1_winrate"] - 0.5

    return df


def prepare_map_play_dataset(candidate_df, require_veto_known=True):
    """Build chronological X/y data for map-play modelling.

    Rows with unknown vetoes are excluded by default because Phase 4 needs a
    reliable played/not-played label for every candidate map in the series.
    """
    df = candidate_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["match_id"] = df["match_id"].astype(str)

    if require_veto_known:
        if "veto_known" not in df.columns:
            raise ValueError("candidate_df must include veto_known when require_veto_known=True")
        df = df[df["veto_known"].eq(1)].copy()

    df = df[df["played"].notna()].copy()
    df["played"] = df["played"].astype(int)
    df = df.sort_values(["date", "match_id", "map"], kind="stable").reset_index(drop=True)
    if df.empty:
        raise ValueError("No map-play training rows after filtering")
    if df["played"].nunique() < 2:
        raise ValueError("Map-play target has only one class after filtering")

    df = _with_derived_map_play_features(df)
    return df[MAP_PLAY_FEATURES], df["played"], df


def make_map_play_pipeline():
    """Create the simple Phase 4 model: one-hot map id + scaled logistic regression."""
    numeric = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value=0, keep_empty_features=True)),
        ("scaler", StandardScaler()),
    ])
    categorical = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value=-1, keep_empty_features=True)),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    preprocess = ColumnTransformer([
        ("map", categorical, MAP_PLAY_CATEGORICAL_FEATURES),
        ("num", numeric, MAP_PLAY_NUMERIC_FEATURES),
    ])
    return Pipeline([
        ("preprocess", preprocess),
        ("model", LogisticRegression(max_iter=2000)),
    ])


def train_map_play_model(candidate_df, require_veto_known=True):
    """Fit a map-play model from candidate rows and return the sklearn pipeline."""
    X, y, _ = prepare_map_play_dataset(candidate_df, require_veto_known=require_veto_known)
    model = make_map_play_pipeline()
    model.fit(X, y)
    return model


def predict_map_play_probabilities(model, candidate_df):
    """Return P(map is played) for candidate rows using a trained map-play model."""
    df = _with_derived_map_play_features(candidate_df)
    return model.predict_proba(df[MAP_PLAY_FEATURES])[:, 1]


def _between_dates(df, start_date, end_date):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    return df[(df["date"] >= start) & (df["date"] <= end)].copy()


def _chronological_holdout(candidate_df, test_fraction=0.2):
    """Split candidate rows by whole match, preserving chronological order."""
    _, _, df = prepare_map_play_dataset(candidate_df, require_veto_known=True)
    matches = (
        df[["match_id", "date"]]
        .drop_duplicates("match_id")
        .sort_values(["date", "match_id"], kind="stable")
        .reset_index(drop=True)
    )
    n_test = max(1, int(len(matches) * test_fraction))
    split_at = max(1, len(matches) - n_test)
    train_ids = set(matches.iloc[:split_at]["match_id"])
    test_ids = set(matches.iloc[split_at:]["match_id"])
    return df[df["match_id"].isin(train_ids)].copy(), df[df["match_id"].isin(test_ids)].copy()


def _expected_calibration_error(y_true, pred, n_bins=10):
    """Simple equal-width expected calibration error."""
    y = np.asarray(y_true)
    p = np.asarray(pred)
    ece = 0.0
    for i in range(n_bins):
        lo = i / n_bins
        hi = (i + 1) / n_bins
        if i == n_bins - 1:
            mask = (p >= lo) & (p <= hi)
        else:
            mask = (p >= lo) & (p < hi)
        if not mask.any():
            continue
        ece += mask.mean() * abs(p[mask].mean() - y[mask].mean())
    return float(ece)


def evaluate_map_play_model(train_df, test_df, model=None):
    """Evaluate a map-play model on a chronological test set."""
    if model is None:
        model = train_map_play_model(train_df)

    X_test, y_test, prepared_test = prepare_map_play_dataset(test_df, require_veto_known=True)
    pred = model.predict_proba(X_test)[:, 1]

    metrics = {
        "rows": int(len(y_test)),
        "series": int(prepared_test["match_id"].nunique()),
        "positive_rate": float(y_test.mean()),
        "accuracy": float(accuracy_score(y_test, pred >= 0.5)),
        "brier": float(brier_score_loss(y_test, pred)),
        "log_loss": float(log_loss(y_test, pred, labels=[0, 1])),
        "calibration_ece": _expected_calibration_error(y_test, pred),
        "roc_auc": float(roc_auc_score(y_test, pred)),
        "average_precision": float(average_precision_score(y_test, pred)),
    }

    # Series-level sanity check: if a BO3 has three played maps, do the top
    # three probabilities recover them? Uses the actual number of played maps
    # per series so BO2/BO5 rows are handled without special cases.
    scored = prepared_test[["match_id", "played"]].copy()
    scored["map_play_prob"] = pred
    hits = []
    for _, group in scored.groupby("match_id", sort=False):
        k = int(group["played"].sum())
        if k <= 0:
            continue
        top = group.nlargest(k, "map_play_prob")
        hits.append(top["played"].sum() / k)
    metrics["top_k_played_recall"] = float(np.mean(hits)) if hits else np.nan
    return metrics, model


def load_candidate_rows():
    """Load current DB tables and build Phase 3 candidate map rows."""
    with sqlite3.connect(DB) as con:
        series_df = pd.read_sql("SELECT * FROM series", con)
        maps_df = pd.read_sql("SELECT * FROM maps", con)
    return build_candidate_map_rows(series_df, maps_df)


def train_and_evaluate_from_db(train_start, train_end, test_start=None, test_end=None,
                               save=True):
    """Build candidate rows, run chronological validation, and optionally save."""
    cand = load_candidate_rows()
    if test_start is None or test_end is None:
        train_df, test_df = _chronological_holdout(cand)
    else:
        train_df = _between_dates(cand, train_start, train_end)
        test_df = _between_dates(cand, test_start, test_end)

    model = train_map_play_model(train_df)
    metrics, model = evaluate_map_play_model(train_df, test_df, model=model)

    if save:
        MODELS.mkdir(exist_ok=True)
        dump(model, MAP_PLAY_MODEL_PATH)

    return metrics, model, cand


def _print_metrics(metrics):
    print("Map-play model validation")
    print(f"Rows: {metrics['rows']}  Series: {metrics['series']}  Positive rate: {metrics['positive_rate']:.2%}")
    print(
        f"Accuracy: {metrics['accuracy']:.2%}  "
        f"Brier: {metrics['brier']:.4f}  "
        f"Log loss: {metrics['log_loss']:.4f}  "
        f"ECE: {metrics['calibration_ece']:.4f}"
    )
    print(
        f"ROC AUC: {metrics['roc_auc']:.4f}  "
        f"Avg precision: {metrics['average_precision']:.4f}  "
        f"Top-k recall: {metrics['top_k_played_recall']:.2%}"
    )


if __name__ == "__main__":
    with open(CONFIG, "rb") as f:
        cfg = tomllib.load(f)

    metrics, _, cand = train_and_evaluate_from_db(
        cfg["modelling"]["vct_2023_start"],
        cfg["modelling"]["vct_2025_end"],
        cfg["modelling"]["vct_2026_start"],
        cfg["modelling"]["vct_2026_end"],
        save=True,
    )
    _print_metrics(metrics)
    print(f"Candidate rows built: {len(cand)}")
    print(f"Map-play model saved to {MAP_PLAY_MODEL_PATH}")
