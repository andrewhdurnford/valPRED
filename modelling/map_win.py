"""Train and evaluate the Phase 5 map-win model.

The model estimates P(t1 wins a played map) from pre-match candidate-map
features. It intentionally uses only prior map/team context from Phase 3;
same-match map stats such as rounds, rating, ACS, and pistol counts are labels
or outcomes only and must not enter the feature matrix.
"""

import sqlite3
import sys
import tomllib
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import dump
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import CONFIG, DB, MODELS
try:
    from maps import build_candidate_map_rows
except ImportError:
    from modelling.maps import build_candidate_map_rows


MAP_WIN_MODEL_PATH = MODELS / "map_win.joblib"

MAP_WIN_CATEGORICAL_FEATURES = ["map"]

MAP_WIN_NUMERIC_FEATURES = [
    "elo_diff", "net_h2h", "past_diff",
    "h2h_map_count", "h2h_map_t1_edge",
    "map_wr_diff", "round_share_diff", "pistol_diff",
    "map_rating_diff", "map_acs_diff", "map_fk_net_diff",
    "map_kpm_diff", "map_dpm_diff", "sample_size_diff",
    "t1_sm_count", "t2_sm_count", "sm_count_min", "sm_count_diff",
]

MAP_WIN_FEATURES = MAP_WIN_CATEGORICAL_FEATURES + MAP_WIN_NUMERIC_FEATURES

_CALIBRATION_FRACTION = 0.2
_MIN_BASE_TRAIN_ROWS = 50
_MIN_CALIBRATION_ROWS = 20


def _with_derived_map_win_features(df):
    """Return rows with Phase 5 model features added.

    Missing history is left as NaN and handled by the model pipeline's imputer.
    Derived edge/count features are neutral at 0 after imputation.
    """
    df = df.copy()
    for col in MAP_WIN_CATEGORICAL_FEATURES + MAP_WIN_NUMERIC_FEATURES:
        if col not in df.columns:
            df[col] = np.nan
    if "h2h_map_t1_winrate" not in df.columns:
        df["h2h_map_t1_winrate"] = np.nan

    df["h2h_map_t1_edge"] = df["h2h_map_t1_winrate"] - 0.5
    df["sm_count_min"] = df[["t1_sm_count", "t2_sm_count"]].min(axis=1)
    df["sm_count_diff"] = df["t1_sm_count"] - df["t2_sm_count"]

    return df


def build_map_win_rows(series_df, maps_df, candidate_df=None):
    """Build labelled actual-played rows for map-win modelling.

    Labels come from the ``maps`` table: ``t1_win = 1`` when ``winner == 0``.
    Features come from Phase 3 candidate rows and are computed as of the
    series date, before any maps in that series are considered.
    """
    required = {"map_id", "t1", "t2", "date", "winner", "map"}
    missing = required - set(maps_df.columns)
    if missing:
        raise ValueError(f"maps_df missing required columns: {sorted(missing)}")

    if maps_df.empty:
        cols = ["map_id", "match_id", "date", "t1", "t2", "map", "winner", "t1_win"]
        return pd.DataFrame(columns=cols + MAP_WIN_FEATURES)

    actual = maps_df[list(required)].copy()
    actual["date"] = pd.to_datetime(actual["date"])
    actual["winner"] = pd.to_numeric(actual["winner"], errors="coerce")
    actual = actual[actual["winner"].isin([0, 1])].copy()
    actual["t1_win"] = (actual["winner"].astype(int) == 0).astype(int)

    if candidate_df is None:
        candidate_df = build_candidate_map_rows(series_df, maps_df)
    cand = candidate_df.copy()
    if cand.empty:
        actual["match_id"] = "map-" + actual["map_id"].astype(str)
        return _with_derived_map_win_features(actual)

    cand["date"] = pd.to_datetime(cand["date"])
    source_cols = [
        "match_id", "t1", "t2", "date", "map",
        "elo_diff", "net_h2h", "past_diff",
        "h2h_map_count", "h2h_map_t1_winrate",
        "map_wr_diff", "round_share_diff", "pistol_diff",
        "map_rating_diff", "map_acs_diff", "map_fk_net_diff",
        "map_kpm_diff", "map_dpm_diff", "sample_size_diff",
        "t1_sm_count", "t2_sm_count",
    ]
    source_cols = [c for c in source_cols if c in cand.columns]
    cand = cand[source_cols].drop_duplicates(["t1", "t2", "date", "map"], keep="first")

    df = actual.merge(cand, on=["t1", "t2", "date", "map"], how="left")
    if "match_id" not in df.columns:
        df["match_id"] = np.nan
    missing_match = df["match_id"].isna()
    df.loc[missing_match, "match_id"] = (
        "map-" + df.loc[missing_match, "map_id"].astype(str)
    )
    return _with_derived_map_win_features(df)


def prepare_map_win_dataset(map_win_df):
    """Build chronological X/y data for map-win modelling."""
    df = map_win_df.copy()
    if "t1_win" not in df.columns:
        if "winner" not in df.columns:
            raise ValueError("map_win_df must include t1_win or winner")
        df["winner"] = pd.to_numeric(df["winner"], errors="coerce")
        df = df[df["winner"].isin([0, 1])].copy()
        df["t1_win"] = (df["winner"].astype(int) == 0).astype(int)

    df["date"] = pd.to_datetime(df["date"])
    df["match_id"] = df["match_id"].astype(str)
    df = df[df["t1_win"].notna()].copy()
    df["t1_win"] = df["t1_win"].astype(int)
    df = _with_derived_map_win_features(df)
    df = df.sort_values(["date", "match_id", "map_id"], kind="stable").reset_index(drop=True)

    if df.empty:
        raise ValueError("No map-win training rows after filtering")
    if df["t1_win"].nunique() < 2:
        raise ValueError("Map-win target has only one class after filtering")

    return df[MAP_WIN_FEATURES], df["t1_win"], df


def make_map_win_pipeline():
    """Create the Phase 5 base model: one-hot map id + GBM classifier."""
    numeric = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value=0, keep_empty_features=True)),
    ])
    categorical = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value=-1, keep_empty_features=True)),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    preprocess = ColumnTransformer([
        ("map", categorical, MAP_WIN_CATEGORICAL_FEATURES),
        ("num", numeric, MAP_WIN_NUMERIC_FEATURES),
    ])
    return Pipeline([
        ("preprocess", preprocess),
        ("model", GradientBoostingClassifier(
            n_estimators=200,
            learning_rate=0.05,
            max_depth=3,
            subsample=0.9,
            random_state=42,
        )),
    ])


def _split_for_calibration(prepared_df, calibration_fraction=_CALIBRATION_FRACTION):
    """Split by whole match, using the most recent rows for calibration."""
    matches = (
        prepared_df[["match_id", "date"]]
        .drop_duplicates("match_id")
        .sort_values(["date", "match_id"], kind="stable")
        .reset_index(drop=True)
    )
    if len(matches) < 3:
        raise ValueError("Need at least 3 matches for chronological calibration")

    n_cal_matches = max(1, int(len(matches) * calibration_fraction))
    split_at = max(1, len(matches) - n_cal_matches)

    while split_at > 0:
        train_ids = set(matches.iloc[:split_at]["match_id"])
        cal_ids = set(matches.iloc[split_at:]["match_id"])
        base_df = prepared_df[prepared_df["match_id"].isin(train_ids)].copy()
        cal_df = prepared_df[prepared_df["match_id"].isin(cal_ids)].copy()
        enough_rows = (
            len(base_df) >= _MIN_BASE_TRAIN_ROWS
            and len(cal_df) >= _MIN_CALIBRATION_ROWS
        )
        enough_classes = (
            base_df["t1_win"].nunique() == 2
            and cal_df["t1_win"].nunique() == 2
        )
        if enough_rows and enough_classes:
            return base_df, cal_df
        split_at -= 1

    raise ValueError(
        "Not enough chronological map-win rows/classes for calibrated training "
        f"(need >= {_MIN_BASE_TRAIN_ROWS} base rows and >= {_MIN_CALIBRATION_ROWS} "
        "calibration rows, both with two classes)"
    )


def _fit_sigmoid_calibrator(base_model, X_cal, y_cal):
    """Fit a sigmoid calibrator around an already-fitted base model.

    Uses ``FrozenEstimator`` when available (newer scikit-learn), with the
    older ``cv='prefit'`` path retained for the pinned 1.5.x dependency.
    """
    try:
        from sklearn.frozen import FrozenEstimator
    except ImportError:
        calibrator = CalibratedClassifierCV(
            estimator=base_model,
            method="sigmoid",
            cv="prefit",
        )
    else:
        calibrator = CalibratedClassifierCV(
            estimator=FrozenEstimator(base_model),
            method="sigmoid",
        )
    calibrator.fit(X_cal, y_cal)
    return calibrator


def train_map_win_model(map_win_df):
    """Fit a calibrated pooled map-win model and return the sklearn estimator."""
    _, _, prepared = prepare_map_win_dataset(map_win_df)
    base_df, cal_df = _split_for_calibration(prepared)

    X_base = base_df[MAP_WIN_FEATURES]
    y_base = base_df["t1_win"]
    X_cal = cal_df[MAP_WIN_FEATURES]
    y_cal = cal_df["t1_win"]

    base = make_map_win_pipeline()
    base.fit(X_base, y_base)
    return _fit_sigmoid_calibrator(base, X_cal, y_cal)


def _class_one_probability(model, X):
    proba = model.predict_proba(X)
    class_index = list(model.classes_).index(1)
    return proba[:, class_index]


def predict_map_win_probabilities(model, candidate_df):
    """Return P(t1 wins map) for candidate rows using a trained map-win model."""
    df = _with_derived_map_win_features(candidate_df)
    return _class_one_probability(model, df[MAP_WIN_FEATURES])


def _between_dates(df, start_date, end_date):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    return df[(df["date"] >= start) & (df["date"] <= end)].copy()


def _chronological_holdout(map_win_df, test_fraction=0.2):
    """Split map rows by whole match, preserving chronological order."""
    _, _, df = prepare_map_win_dataset(map_win_df)
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


def evaluate_map_win_model(train_df, test_df, model=None):
    """Evaluate a calibrated map-win model on a chronological test set."""
    if model is None:
        model = train_map_win_model(train_df)

    X_test, y_test, prepared_test = prepare_map_win_dataset(test_df)
    pred = _class_one_probability(model, X_test)

    metrics = {
        "rows": int(len(y_test)),
        "series": int(prepared_test["match_id"].nunique()),
        "positive_rate": float(y_test.mean()),
        "accuracy": float(accuracy_score(y_test, pred >= 0.5)),
        "brier": float(brier_score_loss(y_test, pred)),
        "log_loss": float(log_loss(y_test, pred, labels=[0, 1])),
        "calibration_ece": _expected_calibration_error(y_test, pred),
    }
    if y_test.nunique() == 2:
        metrics["roc_auc"] = float(roc_auc_score(y_test, pred))
        metrics["average_precision"] = float(average_precision_score(y_test, pred))
    else:
        metrics["roc_auc"] = np.nan
        metrics["average_precision"] = np.nan
    return metrics, model


def load_map_win_rows():
    """Load current DB tables and build Phase 5 labelled map-win rows."""
    with sqlite3.connect(DB) as con:
        series_df = pd.read_sql("SELECT * FROM series", con)
        maps_df = pd.read_sql("SELECT * FROM maps", con)
    return build_map_win_rows(series_df, maps_df)


def train_and_evaluate_from_db(train_start, train_end, test_start=None, test_end=None,
                               save=True):
    """Build map-win rows, run chronological validation, and optionally save."""
    rows = load_map_win_rows()
    if test_start is None or test_end is None:
        train_df, test_df = _chronological_holdout(rows)
    else:
        train_df = _between_dates(rows, train_start, train_end)
        test_df = _between_dates(rows, test_start, test_end)

    model = train_map_win_model(train_df)
    metrics, model = evaluate_map_win_model(train_df, test_df, model=model)

    if save:
        MODELS.mkdir(exist_ok=True)
        dump(model, MAP_WIN_MODEL_PATH)

    return metrics, model, rows


def _print_metrics(metrics):
    print("Map-win model validation")
    print(f"Rows: {metrics['rows']}  Series: {metrics['series']}  Positive rate: {metrics['positive_rate']:.2%}")
    print(
        f"Accuracy: {metrics['accuracy']:.2%}  "
        f"Brier: {metrics['brier']:.4f}  "
        f"Log loss: {metrics['log_loss']:.4f}  "
        f"ECE: {metrics['calibration_ece']:.4f}"
    )
    print(
        f"ROC AUC: {metrics['roc_auc']:.4f}  "
        f"Avg precision: {metrics['average_precision']:.4f}"
    )


if __name__ == "__main__":
    with open(CONFIG, "rb") as f:
        cfg = tomllib.load(f)

    try:
        metrics, _, rows = train_and_evaluate_from_db(
            cfg["modelling"]["vct_2023_start"],
            cfg["modelling"]["vct_2025_end"],
            cfg["modelling"]["vct_2026_start"],
            cfg["modelling"]["vct_2026_end"],
            save=True,
        )
    except ValueError as e:
        rows = load_map_win_rows()
        print(f"Map-win model not trained: {e}")
        print(f"Map-win rows available: {len(rows)}")
        print("Backfill the maps table with a full scrape, then rerun this script.")
    else:
        _print_metrics(metrics)
        print(f"Map-win rows built: {len(rows)}")
        print(f"Map-win model saved to {MAP_WIN_MODEL_PATH}")
