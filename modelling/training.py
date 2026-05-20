import pickle
import sys
from pathlib import Path

import pandas as pd
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split, GridSearchCV, RandomizedSearchCV
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
    "rating_diff", "acs_diff", "fkpm_diff", "fdpm_diff", "winrate_diff",
]


def train_series_winner_model(sds):
    X = sds[FEATURES].fillna(0)
    Y = sds["winner"]

    X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

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
            n_iter=500,
            scoring="accuracy",
            cv=5,
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

    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy of the Series Winner model: {accuracy:.2f}")
    return model
