import hashlib
import pickle
import sys
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from sklearn.ensemble import GradientBoostingClassifier

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import MODELS
# SIMPLE_MAP_ROLLING_FEATURES is the canonical list; mirrored here so callers
# can build SIMPLE_FEATURES without an extra import chain at predict time.
from map_expectations import SIMPLE_MAP_ROLLING_FEATURES as _SIMPLE_MAP_ROLLING_FEATURES

_PARAM_GRID = {
    "n_estimators": [100, 200, 300],
    "learning_rate": [0.01, 0.1, 0.2],
    "max_depth": [3, 4, 5],
    "min_samples_split": [2, 5, 10],
    "min_samples_leaf": [1, 2, 4],
    "subsample": [0.8, 0.9, 1.0],
    "max_features": ["sqrt", "log2"],
}


BASELINE_FEATURES = [
    "elo_diff", "net_h2h", "past_diff",
    "rating_diff", "acs_diff", "fk_net_diff", "winrate_diff",
]

MAP_SERIES_FEATURES = [
    "map_winshare", "map_edge",
]

EXTENDED_FEATURES = BASELINE_FEATURES + MAP_SERIES_FEATURES

# Simple aggregated map-history features (no map-play/map-win models required).
SIMPLE_MAP_SERIES_FEATURES = list(_SIMPLE_MAP_ROLLING_FEATURES)
SIMPLE_FEATURES = BASELINE_FEATURES + SIMPLE_MAP_SERIES_FEATURES

# Backwards-compatible default: the original seven-feature series model.
FEATURES = BASELINE_FEATURES

# Fraction of training data held back (chronologically) for Platt scaling.
_CALIBRATION_FRACTION = 0.2


def get_series_model_features(model=None, feature_set="baseline"):
    """Return the feature list a series model expects.

    Older saved artifacts do not carry feature names, so callers fall back to
    a named feature set. ``feature_set`` may be ``"baseline"``, ``"map"``
    (Phase 7 map-expectation extension), or ``"simple"`` (raw aggregated
    map-history features). ``include_map_features=True`` is accepted via the
    legacy keyword path below for backwards compatibility.
    """
    if model is not None and hasattr(model, "feature_names"):
        return list(model.feature_names)
    if feature_set == "map":
        return EXTENDED_FEATURES.copy()
    if feature_set == "simple":
        return SIMPLE_FEATURES.copy()
    return FEATURES.copy()


def _params_path(features):
    """Cache hyperparameters per feature shape.

    Baseline keeps the historical ``series.pkl`` filename so the existing
    cache is reused. Other feature sets get a stable hashed filename so the
    cache does not collide across shapes and a search re-runs the first time
    a new shape is trained.
    """
    feature_list = list(features)
    if feature_list == BASELINE_FEATURES:
        return MODELS / "params" / "series.pkl"
    key = hashlib.md5("|".join(feature_list).encode()).hexdigest()[:8]
    return MODELS / "params" / f"series_{len(feature_list)}f_{key}.pkl"


class _PlattScaledModel:
    """GBM + Platt scaling (logistic regression on raw GBM scores).

    Joblib-serialisable. predict_proba returns calibrated probabilities.
    """
    def __init__(self, base, platt, feature_names=None):
        self.base = base
        self.platt = platt
        self.feature_names = list(feature_names) if feature_names is not None else None

    def _prepare_X(self, X):
        if self.feature_names is not None and hasattr(X, "loc"):
            missing = [c for c in self.feature_names if c not in X.columns]
            if missing:
                raise ValueError(f"Input is missing required series features: {missing}")
            return X.loc[:, self.feature_names].fillna(0)
        return X

    def predict_proba(self, X):
        X = self._prepare_X(X)
        raw = self.base.predict_proba(X)[:, 1].reshape(-1, 1)
        p = self.platt.predict_proba(raw)[:, 1]
        return np.column_stack([1 - p, p])

    def predict(self, X):
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)


def train_series_winner_model(sds, features=None):
    features = get_series_model_features(feature_set="baseline") if features is None else list(features)
    missing = [c for c in features if c not in sds.columns]
    if missing:
        raise ValueError(f"Training data is missing required series features: {missing}")

    # Sort chronologically so TimeSeriesSplit folds and calibration split both
    # respect time order.
    sds = sds.sort_values("date").reset_index(drop=True)
    X = sds[features].fillna(0)
    Y = sds["winner"]

    # Hold back the most recent fraction as a calibration set. Platt scaling
    # needs fewer samples than isotonic regression, making it appropriate here.
    n_cal = max(20, int(len(X) * _CALIBRATION_FRACTION))
    X_train, X_cal = X.iloc[:-n_cal], X.iloc[-n_cal:]
    y_train, y_cal = Y.iloc[:-n_cal], Y.iloc[-n_cal:]

    params_file = _params_path(features)
    if params_file.exists():
        with open(params_file, "rb") as f:
            best_params = pickle.load(f)
        model = GradientBoostingClassifier(**best_params, random_state=42)
        model.fit(X_train, y_train)
    else:
        search = RandomizedSearchCV(
            estimator=GradientBoostingClassifier(random_state=42),
            param_distributions=_PARAM_GRID,
            n_iter=50,
            scoring="accuracy",
            cv=TimeSeriesSplit(n_splits=5),
            verbose=1,
            random_state=42,
            n_jobs=-1,
        )
        search.fit(X_train, y_train)
        best_params = search.best_params_
        params_file.parent.mkdir(parents=True, exist_ok=True)
        with open(params_file, "wb") as f:
            pickle.dump(best_params, f)
        model = search.best_estimator_

    raw_cal = model.predict_proba(X_cal)[:, 1].reshape(-1, 1)
    platt = LogisticRegression()
    platt.fit(raw_cal, y_cal)
    return _PlattScaledModel(model, platt, feature_names=features)
