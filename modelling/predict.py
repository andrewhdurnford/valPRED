import pandas as pd, operator
from joblib import load
from series import get_team_wr_by_map, get_team_pbrate_by_map, get_map_in_pool, get_tier1
from maps import get_team_map_stats, normalize_training_data
from IPython.display import display

cn = pd.read_csv('data/tier1/teams/cn.csv').iloc[:,0].tolist()
upcoming = get_tier1(pd.read_csv('data/raw/upcoming.csv', index_col=False))
upcoming = upcoming.loc[~(upcoming['t1'].isin(cn) | upcoming['t2'].isin(cn))]
maps = pd.read_csv('data/tier1/maps.csv')
series = pd.read_csv('data/tier1/series.csv')
map_pick_model = load('models/map_pick.joblib')
models = {}
for i in range(10):
    models[i] = load(f'models/maps/{i}.joblib')
series_winner_model = load('models/series_winner.joblib')

def explode_preds(pred_df):
    def get_team_data_by_map_row(row, count=20):
        t1_win = get_team_wr_by_map(maps, row[1], row[4], row[3], count)
        t1_pick, t1_ban, t1_play = get_team_pbrate_by_map(series, row[1], row[4], row[3], count)
        t2_win = get_team_wr_by_map(maps, row[2], row[4], row[3], count)
        t2_pick, t2_ban, t2_play = get_team_pbrate_by_map(series, row[2], row[4], row[3], count)
        row[9:17] = [t1_win, t1_pick, t1_ban, t1_play, t2_win, t2_pick, t2_ban, t2_play]
        return row

    sdf = pred_df.copy()
    sdf['net_h2h'] = sdf['net_h2h'].fillna(0)
    data = []
    for map in range(10):
        temp_df = sdf.apply(lambda row: pd.Series({
            'match_id': row['match_id'],
            't1': row['t1'],
            't2': row['t2'],
            'date': row['date'],
            'map': map,
            'net_h2h': row['net_h2h'],
            'past_diff': (row['t1_past'] - row['t2_past']),
            't1_odds': row['t1_odds'],
            't2_odds': row['t2_odds']
        }), axis=1)
        data.append(temp_df)

    exploded_df = pd.concat(data)
    exploded_df['inpool'] = True
    for _, row in exploded_df.iterrows():
        row['inpool'] = get_map_in_pool(row['date'], row['map'])
    exploded_df = exploded_df.loc[exploded_df['inpool']]
    exploded_df = exploded_df.drop('inpool', axis=1)

    # Get win, pick, ban, and playrates
    exploded_df[['t1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%']] = 0
    exploded_df = exploded_df.apply(get_team_data_by_map_row, raw=True, axis=1)
    return exploded_df.copy(deep=True)

def get_preds_map_stats_df(df, maps_df):
    def get_map_stats_row(row, count=20):
        t1_stats = get_team_map_stats(row[1], row[4], maps_df, row[3], count)  # team, map, maps_df, date
        t2_stats = get_team_map_stats(row[2], row[4], maps_df, row[3], count)
        stats_diff = row[0:17].tolist()
        stats_diff.extend(map(operator.sub, t1_stats, t2_stats))
        stats_diff_list.append(stats_diff)

    columns = ['match_id', 't1', 't2', 'date', 'map', 'net_h2h', 'past_diff', 't1_odds', 't2_odds',
               't1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%', 
               'round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 'pistol_wr_diff', 'eco_wr_diff', 'antieco_wr_diff', 'fullbuy_wr_diff', 
               'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff', 'kast_diff', 'rating_diff', 'mks_diff', 'clutch_diff', 'econ_diff']

    stats_diff_list = []
    df = df.apply(get_map_stats_row, raw=True, axis=1)
    return pd.DataFrame(data=stats_diff_list, columns=columns)

def transform_preds_stats(preds, models, map_pick_model):
    # Fill Nan values
    preds = preds.fillna(0)
    
    def get_mapwin_row(row):
        map_features = ['round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 'pistol_wr_diff', 
                    'eco_wr_diff', 'antieco_wr_diff', 'fullbuy_wr_diff', 'acs_diff', 'kills_diff', 
                    'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff', 'kast_diff', 'rating_diff', 'mks_diff', 'clutch_diff', 'econ_diff']
        df = pd.DataFrame(data=[row[17:36]], columns=map_features)
        map_pick_features = ['t1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%']
        pick_df = pd.DataFrame(data=[row[9:17]], columns=map_pick_features)
        row[36] = map_pick_model.predict_proba(pick_df)[0][0]

        model = models[row[4]]
        row[37] = model.predict_proba(df)[0][0] if model is not None else None
        row[38] = model.predict_proba(df)[0][1] if model is not None else None
        return row

    # Get map play chance, and each team's winrate
    preds[['play%', 't1_winchance', 't2_winchance']] = 0
    preds = preds.apply(get_mapwin_row, raw=True, axis=1)
    preds = preds.dropna()

    # Compress matches
    preds = preds[['match_id', 't1', 't2', 'date', 'net_h2h', 'past_diff', 't1_odds', 't2_odds', 'play%', 't1_winchance', 't2_winchance']]
    preds = preds.sort_values(by='play%', ascending=True)
    matches = preds['match_id'].unique()
    cols = ['match_id', 't1', 't2', 'date', 'net_h2h', 'past_diff', 't1_odds', 't2_odds', 'winshare']
    data = []
    for match in matches:
        df = preds.copy().loc[preds['match_id'] == match]
        df.drop(df.head(2).index, inplace=True) # Drop least likely maps
        match_data = df.iloc[0].tolist()[:8]
        pred_win = df['t1_winchance'].sum() / (df['t1_winchance'].sum() + df['t2_winchance'].sum()) 
        match_data.append(pred_win)
        data.append(match_data)
    preds = pd.DataFrame(data=data, columns=cols)
    return preds.copy(deep=True)

def predict_series_outcomes(sds, series_winner_model):
    return_df = sds.copy(deep=True)
    
    def series_win_row(row):
        cols = ['net_h2h', 'past_diff', 'winshare']
        df = pd.DataFrame(data=[[row[4], row[5], row[8]]], columns=cols)
        row[9] = series_winner_model.predict_proba(df)[0][0]
        return row
    
    return_df['pred_win%'] = 0
    return_df = return_df.apply(series_win_row, raw=True, axis=1)
    return_df = return_df[['match_id', 't1', 't2', 'date', 'pred_win%', 't1_odds', 't2_odds']]
    return_df['ev_t1'] = return_df['pred_win%'] * return_df['t1_odds'] - (1 - return_df['pred_win%'])
    return_df['ev_t2'] = (1 - return_df['pred_win%']) * return_df['t2_odds'] - return_df['pred_win%']
    return_df['t1_odds'] = 1/return_df['t1_odds']
    return_df['t2_odds'] = 1/return_df['t2_odds']

    return return_df.copy(deep=True)

def process_pred_stats():
    return predict_series_outcomes(transform_preds_stats(normalize_training_data(get_preds_map_stats_df(explode_preds(upcoming), maps), True), models, map_pick_model), series_winner_model).copy(deep=True)

preds = process_pred_stats()
display(preds.head(20))