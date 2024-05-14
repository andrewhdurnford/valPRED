import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split
from IPython.display import display
from joblib import dump, load

tier1_tmd = pd.read_csv('data/tier1/tier1_normalized_tmd_nomap.csv', index_col=False)
tier1_teams = pd.read_csv('data/tier1/tier1_teams.csv').iloc[:,0].tolist()
series_wr_diff = pd.read_csv('data/tier1/tier1_series_wr_diff.csv', index_col=False)
map_diff_model = load('models/current/wr_diff.joblib')

def train_map_model(tmd):
    # wr_diff_features = [f'{i}_wr_diff' for i in range(10)]
    # swr_diff = series_wr_diff.copy()
    # swr_diff['map_diff'] = 0
    # swr_diff['map_diff'] = map_diff_model.predict_proba(swr_diff[wr_diff_features])
    # tmd['map_diff'] = swr_diff['map_diff']

    features = ['net_h2h', 'past_diff', 'round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 
                'pistol_wr_diff', 'eco_wr_diff', 'fullbuy_wr_diff', 'acs_diff', 'kills_diff', 
                'assists_diff', 'deaths_diff', 'kdr_diff', 'kast_diff', 'rating_diff', 'clutch_diff', 'econ_diff']

    X = tmd[features]
    Y = tmd['winner']
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
    model = LogisticRegression()
    model.fit(X_train, Y_train)
    predictions = model.predict(X_test)
    report = classification_report(Y_test, predictions, output_dict=False)
    print(report)
    print(model.coef_)

def train_series_wr_diff_model(tmd):
    features = [f'{i}_wr_diff' for i in range(10)]

    X = tmd[features]
    Y = tmd['winner']
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
    model = LogisticRegression()
    model.fit(X_train, Y_train)
    predictions = model.predict(X_test)
    report = classification_report(Y_test, predictions, output_dict=False)
    print(report)
    print(model.coef_)
    return model

train_map_model(tier1_tmd)