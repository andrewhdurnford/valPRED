import pandas as pd
from IPython.display import display
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score
from sklearn.model_selection import train_test_split

def train_map_pick_model(vetos):
    features = ['t1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%']

    for col in features:
        vetos[col] = vetos[col] / vetos[col].abs().max() 

    X = vetos[features]
    Y = vetos['played']

    model = LogisticRegression()
    model.fit(X, Y)
    return model

def train_series_winner_model(sds):
    features = ['net_h2h', 'past_diff', 'winshare']
    X = sds[features]
    Y = sds['winner']

    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
    model = LogisticRegression()
    model.fit(X_train, Y_train)
    predictions = model.predict(X_test)
    report = classification_report(Y_test, predictions, output_dict=False)
    print(report)
    return model

def predict_series_outcomes(sds, series_winner_model):
    return_df = sds.copy(deep=True)
    
    def series_win_row(row):
        cols = ['net_h2h', 'past_diff', 'winshare']
        df = pd.DataFrame(data=[[row[5], row[6], row[9]]], columns=cols)
        row[10] = series_winner_model.predict_proba(df)[0][0]
        return row
    
    return_df['pred_win%'] = 0
    return_df = return_df.apply(series_win_row, raw=True, axis=1)
    return_df = return_df[['match_id', 't1', 't2', 'date', 'winner', 'pred_win%', 'best_odds', 'worst_odds']]
    return return_df.copy(deep=True)
