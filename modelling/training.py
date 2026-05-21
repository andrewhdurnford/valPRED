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

_PARAM_GRID = {
    "n_estimators": [100, 200, 300],
    "learning_rate": [0.01, 0.1, 0.2],
    "max_depth": [3, 4, 5],
    "min_samples_split": [2, 5, 10],
    "min_samples_leaf": [1, 2, 4],
    "subsample": [0.8, 0.9, 1.0],
    "max_features": ["sqrt", "log2"],
}


FEATURES = [
    "elo_diff", "net_h2h", "past_diff",
    "rating_diff", "acs_diff", "fk_net_diff", "winrate_diff",
]

# Fraction of training data held back (chronologically) for Platt scaling.
_CALIBRATION_FRACTION = 0.2


class _PlattScaledModel:
    """GBM + Platt scaling (logistic regression on raw GBM scores).

    Joblib-serialisable. predict_proba returns calibrated probabilities.
    """
    def __init__(self, base, platt):
        self.base = base
        self.platt = platt

    def predict_proba(self, X):
        raw = self.base.predict_proba(X)[:, 1].reshape(-1, 1)
        p = self.platt.predict_proba(raw)[:, 1]
        return np.column_stack([1 - p, p])

    def predict(self, X):
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)


def train_series_winner_model(sds):
    # Sort chronologically so TimeSeriesSplit folds and calibration split both
    # respect time order.
    sds = sds.sort_values("date").reset_index(drop=True)
    X = sds[FEATURES].fillna(0)
    Y = sds["winner"]

    # Hold back the most recent fraction as a calibration set. Platt scaling
    # needs fewer samples than isotonic regression, making it appropriate here.
    n_cal = max(20, int(len(X) * _CALIBRATION_FRACTION))
    X_train, X_cal = X.iloc[:-n_cal], X.iloc[-n_cal:]
    y_train, y_cal = Y.iloc[:-n_cal], Y.iloc[-n_cal:]

    params_file = MODELS / "params" / "series.pkl"
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
    return _PlattScaledModel(model, platt)
