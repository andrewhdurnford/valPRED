import pandas as pd, operator
from datetime import datetime

from maps import rename_team_cols

def get_team_map_stats(team, map, maps_df, date=datetime.today().strftime('%Y-%m-%d'), count=5):
    # Filter by team
    maps_df = maps_df.loc[((maps_df['t1'] == team) | (maps_df['t2'] == team))]

    maps_df = maps_df.loc[(maps_df['date'] < date)]
    if len(maps_df.index) == 0:
        return [0,] * 8
    
    # Check if team has played map
    if len(maps_df.loc[(maps_df['map'] == map)].index) != 0:
        maps_df = maps_df.loc[(maps_df['map'] == map)]

    # Check count flag and if team has played enough games
    maps_df = maps_df.sort_values(by='date', ascending=False)
    if count > 0 and len(maps_df.index) > count:
        maps_df = maps_df.head(count)
    
    maps_df = rename_team_cols(maps_df, team)
    maps_df = maps_df.fillna(0)
    if len(maps_df.index) == 0:
        return [0,] * 19
    
    round_wr = maps_df['t1_rds'].sum() / (maps_df['t2_rds'].sum() + maps_df['t1_rds'].sum())
    fk_percent = (maps_df['t1_atk_fks'].sum() + maps_df['t1_def_fks'].sum()) / (maps_df['t1_atk_fks'].sum() + maps_df['t1_def_fks'].sum() + maps_df['t2_atk_fks'].sum() + maps_df['t2_def_fks'].sum()) 
    acs = maps_df['t1_avg_acs'].sum() / len(maps_df.index)
    kills = maps_df['t1_kills'].sum() / (maps_df['t1_rds'].sum() + maps_df['t2_rds'].sum())
    assists = maps_df['t1_assists'].sum() / (maps_df['t1_rds'].sum() + maps_df['t2_rds'].sum())
    deaths = maps_df['t1_deaths'].sum() / (maps_df['t1_rds'].sum() + maps_df['t2_rds'].sum())
    kdr = maps_df['t1_kills'].sum() / maps_df['t1_deaths'].sum()
    kadr = (maps_df['t1_kills'].sum() + maps_df['t1_assists'].sum()) / maps_df['t1_deaths'].sum()
    
    return [round_wr, fk_percent, acs, kills, assists, deaths, kdr, kadr]

def get_team_map_stats_df(maps_df):
    def get_map_stats_row(row, count=5):
        t1_stats = get_team_map_stats(row[1], row[4], maps_df, row[19], count)
        t2_stats = get_team_map_stats(row[2], row[4], maps_df, row[19], count)
        stats_diff = row[0:5].tolist()
        stats_diff.append(row[19])
        stats_diff.extend(map(operator.sub, t1_stats, t2_stats))
        stats_diff_list.append(stats_diff)
        return row

    stats_diff_list = []
    columns = ['map_id', 't1', 't2', 'winner', 'map', 'date', 'round_wr_diff', 'fk_percent_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff']
    maps_df.apply(get_map_stats_row, raw=True, axis=1)
    df = pd.DataFrame(data=stats_diff_list, columns=columns)
    return df.copy(deep=True)

def normalize_training_data(df, vetos=False):
    numeric = ['round_wr_diff', 'fk_percent_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff']

    for col in numeric:
        df[col] = df[col] / df[col].abs().max() 
    return df

def format_map_data_nd(maps_df):
    maps_df = maps_df[['map_id', 't1', 't2', 'winner', 'map', 't1_rds', 't2_rds', 't1_atk_fks', 't1_def_fks', 't2_atk_fks', 't2_def_fks', 
                       't1_avg_acs', 't2_avg_acs', 't1_kills', 't1_assists', 't1_deaths', 't2_kills', 't2_assists', 't2_deaths', 'date']]
    maps_df = maps_df.dropna()
    tmd = get_team_map_stats_df(maps_df)
    return normalize_training_data(tmd).copy(deep=True)

def get_series_map_stats_df(df, maps_df): # match_id,t1,t2,date,map,winner,played,net_h2h,past_diff,best_odds,worst_odds,t1_win%,t1_pick%,t1_ban%,t1_play%,t2_win%,t2_pick%,t2_ban%,t2_play%
    def get_map_stats_row(row, count=5):
        t1_stats = get_team_map_stats(row[1], row[4], maps_df, row[3], count) 
        t2_stats = get_team_map_stats(row[2], row[4], maps_df, row[3], count)
        stats_diff = row[0:19].tolist()
        stats_diff.extend(map(operator.sub, t1_stats, t2_stats))
        stats_diff_list.append(stats_diff)

    columns = ['match_id', 't1', 't2', 'date', 'map', 'winner', 'played', 'net_h2h', 'past_diff', 'best_odds', 'worst_odds',
               't1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%', 
               'round_wr_diff', 'fk_percent_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff']

    stats_diff_list = []
    df = df.apply(get_map_stats_row, raw=True, axis=1)
    return pd.DataFrame(data=stats_diff_list, columns=columns)

def format_veto_data_nd(vetos_df, maps_df):
    tmd = get_series_map_stats_df(vetos_df, maps_df)
    return normalize_training_data(tmd, True).copy(deep=True)

def transform_series_stats_nd(sds, models, map_pick_model):
    # Fill Nan values
    sds = sds.fillna(0)
    
    def get_mapwin_row(row):
        map_features = ['round_wr_diff', 'fk_percent_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff']
        df = pd.DataFrame(data=[row[19:26]], columns=map_features)
        # map_pick_features = ['t1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%']

        map_pick_features = ['avg_play%', 'avg_pick%', 'avg_ban%', 'avg_win%']
        pickdata = [(row[14] + row[18]) / 2, (row[12] + row[16]) / 2, (row[13] + row[17]) / 2, (row[11] + row[15]) / 2]
        pick_df = pd.DataFrame(data=[pickdata], columns=map_pick_features)

        row[27] = map_pick_model.predict_proba(pick_df)[0][0]

        # model = models[row[4]]
        model = models
        row[28] = model.predict_proba(df)[0][0] if model is not None else None
        row[29] = model.predict_proba(df)[0][1] if model is not None else None
        return row

    # Get map play chance, and each team's winrate
    sds[['play%', 't1_winchance', 't2_winchance']] = 0
    sds = sds.apply(get_mapwin_row, raw=True, axis=1)
    sds = sds.dropna()

    # Compress matches
    sds = sds[['match_id', 't1', 't2', 'date', 'winner', 'net_h2h', 'past_diff', 'best_odds', 'worst_odds', 'play%', 't1_winchance', 't2_winchance']]
    sds = sds.sort_values(by='play%', ascending=True)
    matches = sds['match_id'].unique()
    cols = ['match_id', 't1', 't2', 'date', 'winner', 'net_h2h', 'past_diff', 'best_odds', 'worst_odds', 'winshare']
    data = []
    for match in matches:
        df = sds.copy().loc[sds['match_id'] == match]
        df.drop(df.head(2).index, inplace=True) # Drop least likely maps
        match_data = df.iloc[0].tolist()[:9]
        pred_win = df['t1_winchance'].sum() / (df['t1_winchance'].sum() + df['t2_winchance'].sum()) 
        match_data.append(pred_win)
        data.append(match_data)
    sds = pd.DataFrame(data=data, columns=cols)
    return sds.copy(deep=True)