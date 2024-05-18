import pandas as pd
import numpy as np
from sklearn.discriminant_analysis import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split
from IPython.display import display
from joblib import dump, load

def train_map_model(tmd, map):
    temp_tmd = tmd.copy()
    temp_tmd['winner'] = (temp_tmd['winner'] == temp_tmd['t1'])
    temp_tmd = temp_tmd.loc[temp_tmd['map'] == map]

    features = ['round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 'pistol_wr_diff', 
                'eco_wr_diff', 'antieco_wr_diff', 'fullbuy_wr_diff', 'acs_diff', 'kills_diff', 
                'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff', 'kast_diff', 'rating_diff', 'mks_diff', 'clutch_diff', 'econ_diff']

    X = temp_tmd[features]
    Y = temp_tmd['winner'].astype(int)

    # Split the data into training and testing sets
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

    model = LogisticRegression()
    model.fit(X_train, Y_train)
    predictions = model.predict(X_test)
    report = classification_report(Y_test, predictions, output_dict=False)
    print(report)
    print(model.coef_)
    dump(model, f'models/current/maps/{map}_model.joblib')