import pandas as pd
from IPython.display import display
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score
from sklearn.model_selection import train_test_split
from joblib import dump, load

# Load data
exploded_vetos = pd.read_csv("data/exploded_vetos.csv", index_col=False)
map_pick_model = load('models/current/map_pick_model.joblib')
series_winner_model = load('models/current/series_winner.joblib')
maps_id = range(10)
models = {}
for m in maps_id:
    models[m] = load(f'models/current/maps/{m}_model.joblib')

def train_map_pick_model(vetos_df):
    features = ['t1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%', 'h2h_played']
    for col in features:
        vetos_df[col] = vetos_df[col] / vetos_df[col].abs().max() 

    X = vetos_df[features]
    Y = vetos_df['played']

    # Train model
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
    model = LogisticRegression()
    model.fit(X_train, Y_train)
    predictions = model.predict(X_test)
    accuracy = accuracy_score(Y_test, predictions)
    report = classification_report(Y_test, predictions, output_dict=False)
    print(accuracy)
    print(report)
    dump(model, 'models/current/map_pick_model.joblib')

def get_mapwin_row(row):
    model = models[row[4]]
    map_features = ['round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 'pistol_wr_diff', 
                'eco_wr_diff', 'antieco_wr_diff', 'fullbuy_wr_diff', 'acs_diff', 'kills_diff', 
                'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff', 'kast_diff', 'rating_diff', 'mks_diff', 'clutch_diff', 'econ_diff']
    df = pd.DataFrame(data=[row[20:39]], columns=map_features)
    map_pick_features = ['t1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%', 'h2h_played']
    pick_df = pd.DataFrame(data=[row[11:20]], columns=map_pick_features)
    row[39] = map_pick_model.predict_proba(pick_df)[0][0]
    row[40] = model.predict_proba(df)[0][0] * row[39]
    row[41] = model.predict_proba(df)[0][1] * row[39]
    return row

def transform_data_stats(sds):
    sds = sds.fillna(0)

    # Get map play chance, and each team's winrate
    sds[['play%', 't1_winchance', 't2_winchance']] = 0
    sds = sds.apply(get_mapwin_row, raw=True, axis=1)

    # Compress matches
    sds = sds[['match_id', 't1', 't2', 'date', 'winner', 'net_h2h', 'past_diff', 'best_odds', 'worst_odds', 't1_winchance', 't2_winchance']]
    matches = sds['match_id'].unique()
    cols = ['match_id', 't1', 't2', 'date', 'winner', 'net_h2h', 'past_diff', 'best_odds', 'worst_odds', 'winshare']
    data = []
    for match in matches:
        df = sds.loc[sds['match_id'] == match]
        match_data = df.iloc[0].tolist()[:9]
        pred_win = df['t1_winchance'].sum() / (df['t1_winchance'].sum() + df['t2_winchance'].sum()) 
        match_data.append(pred_win)
        data.append(match_data)
    sds = pd.DataFrame(data=data, columns=cols)
    sds.to_csv('data/transformed_pred_data.csv')
    return sds

def train_pred_model(sds):
    sds = transform_data_stats(sds)
    features = ['net_h2h', 'past_diff', 'winshare']
    X = sds[features]
    Y = sds['winner']

    # Train model
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
    model = LogisticRegression()
    model.fit(X_train, Y_train)
    predictions = model.predict(X_test)
    accuracy = accuracy_score(Y_test, predictions)
    report = classification_report(Y_test, predictions, output_dict=False)
    dump(model, 'models/current/series_winner.joblib')
    print(accuracy)
    print(report)

def series_win_row(row):
    cols = ['net_h2h', 'past_diff', 'winshare']
    df = pd.DataFrame(data=[[row[6], row[7], row[10]]], columns=cols)
    row[11] = series_winner_model.predict_proba(df)[0][0]
    return row

def pred_sds(sds):
    sds['pred_win%'] = 0
    sds = sds.apply(series_win_row, raw=True, axis=1)
    sds = sds[['match_id', 't1', 't2', 'date', 'winner', 'pred_win%', 'best_odds', 'worst_odds']]
    return sds
