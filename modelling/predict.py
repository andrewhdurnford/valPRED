import pandas as pd, operator
from joblib import load
from series import get_team_wr_by_map, get_team_pbrate_by_map, get_map_in_pool, get_tier1
from maps import get_team_map_stats, normalize_training_data, between_dates
from IPython.display import display

cn = pd.read_csv('data/tier1/teams/cn.csv').iloc[:,0].tolist()
upcoming = get_tier1(pd.read_csv('data/raw/upcoming.csv', index_col=False))
upcoming = upcoming.loc[~(upcoming['t1'].isin(cn) | upcoming['t2'].isin(cn))]
maps = between_dates(pd.read_csv('data/tier1/maps.csv'), '2024-01-01', '2025-01-01')
series = pd.read_csv('data/tier1/series.csv')
teams = pd.read_csv('data/tier1/teams.csv')

map_pick_model = load('models/map_pick.joblib')
# models = {}
# for i in range(10):
#     models[i] = load(f'models/maps/{i}.joblib')
model = load('models/map_win.joblib')
series_winner_model = load('models/series_winner.joblib')

def get_team_fullname(row):
    t1 = teams[teams['id'] == row['t1']]
    t2 = teams[teams['id'] == row['t2']]
    row['t1'] = t1['fullname'].values[0]
    row['t2'] = t2['fullname'].values[0]
    return row

def explode_preds(pred_df):
    def get_team_data_by_map_row(row, count=20):
        t1_win = get_team_wr_by_map(maps, row['t1'], row['map'], row['date'], count) #maps_df, team, map, date, count
        t1_pick, t1_ban, t1_play = get_team_pbrate_by_map(series, row['t1'], row['map'], row['date'], count) # team, map, date, count
        t2_win = get_team_wr_by_map(maps, row['t2'], row['map'], row['date'], count) 
        t2_pick, t2_ban, t2_play = get_team_pbrate_by_map(series, row['t2'], row['map'], row['date'], count)
        row.loc[['avg_win%', 'avg_pick%', 'avg_ban%', 'avg_play%']] = [(t1_win + t2_win) / 2, (t1_pick + t2_pick) / 2, (t1_play + t2_play) / 2, (t1_ban + t2_ban) / 2]
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
    exploded_df[['avg_win%', 'avg_pick%', 'avg_ban%', 'avg_play%']] = 0
    exploded_df = exploded_df.apply(get_team_data_by_map_row, axis=1)
    return exploded_df.copy(deep=True)

def get_preds_map_stats_df(df, maps_df):
    def get_map_stats_row(row, count=20):
        t1_stats = get_team_map_stats(row['t1'], row['map'], maps_df, row['date'], count) # team, map, maps_df, date=datetime.today().strftime('%Y-%m-%d'), count
        t2_stats = get_team_map_stats(row['t2'], row['map'], maps_df, row['date'], count)
        stats_diff = row.iloc[0:13].tolist()
        stats_diff.extend(map(operator.sub, t1_stats, t2_stats))
        stats_diff_list.append(stats_diff)
        return row
    
    columns = ['match_id', 't1', 't2', 'date', 'map', 'net_h2h', 'past_diff', 't1_odds', 't2_odds',
               'avg_win%', 'avg_pick%', 'avg_ban%', 'avg_play%',
               'round_wr_diff', 'fk_percent_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff']

    stats_diff_list = []
    df = df.apply(get_map_stats_row, axis=1)
    return pd.DataFrame(data=stats_diff_list, columns=columns)

def transform_preds_stats(preds, models, map_pick_model):
    # Fill Nan values
    preds = preds.fillna(0)

    def get_mapwin_row(row):
        map_features = ['round_wr_diff', 'fk_percent_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff']
        df = pd.DataFrame(data=[[row['round_wr_diff'], row['fk_percent_diff'], row['acs_diff'], row['kills_diff'], row['assists_diff'], row['deaths_diff'], row['kdr_diff']]], columns=map_features)

        map_pick_features = ['avg_play%', 'avg_pick%', 'avg_ban%', 'avg_win%']
        pick_df = pd.DataFrame(data=[[row['avg_play%'], row['avg_pick%'], row['avg_ban%'], row['avg_win%']]], index=map_pick_features)

        row['play%'] = map_pick_model.predict_proba(pick_df)[0][0]

        row['t1_winchance'] = model.predict_proba(df)[0][0] if model is not None else None
        row['t2_winchance'] = model.predict_proba(df)[0][1] if model is not None else None
        return row
    
    # Get map play chance, and each team's winrate
    preds[['play%', 't1_winchance', 't2_winchance']] = 0
    preds = preds.apply(get_mapwin_row, axis=1)
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
        # print('t1: ' + str(df['t1_winchance'].sum()) + ' t2: ' + str(df['t2_winchance'].sum()))
        pred_win = df['t1_winchance'].sum() / (df['t1_winchance'].sum() + df['t2_winchance'].sum()) 
        match_data.append(pred_win)
        data.append(match_data)
    preds = pd.DataFrame(data=data, columns=cols)
    return preds.copy(deep=True)

def predict_series_outcomes(sds, series_winner_model):
    return_df = sds.copy(deep=True)
    
    def series_win_row(row):
        cols = ['past_diff', 'winshare']
        df = pd.DataFrame(data=[[row['past_diff'], row['winshare']]], columns=cols)
        row['pred_win%'] = series_winner_model.predict_proba(df)[0][0]
        return row
    
    def bet(row):
        if row['ev_t1'] > 1:
            return 't1'
        elif row['ev_t2'] > 1:
            return 't2'
        else:
            return None
    
    return_df['pred_win%'] = 0
    return_df = return_df.apply(series_win_row, axis=1)
    return_df = return_df[['match_id', 't1', 't2', 'date', 'pred_win%', 't1_odds', 't2_odds']]
    return_df['ev_t1'] = return_df['pred_win%'] * return_df['t1_odds'] - (1 - return_df['pred_win%'])
    return_df['ev_t2'] = (1 - return_df['pred_win%']) * return_df['t2_odds'] - return_df['pred_win%']
    return_df['bet'] = return_df.apply(bet, axis=1)
    return_df = return_df.apply(get_team_fullname, axis=1)

    return return_df.copy(deep=True)

def process_pred_stats():
    return predict_series_outcomes(transform_preds_stats(normalize_training_data(get_preds_map_stats_df(explode_preds(upcoming), maps)), model, map_pick_model), series_winner_model).sort_values(by='date', ascending=True).copy(deep=True)

preds = process_pred_stats()
display(preds)