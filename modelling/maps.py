import pandas as pd, operator
from datetime import datetime
import warnings

warnings.filterwarnings("ignore", message="X does not have valid feature names")

# Functions to retrieve game data
def save_map_pool(out_of_pool):
    with open("data/game_data/map_pool.txt", "w") as f:
        for k, v in out_of_pool.items():
            f.write(f"{k};" + ",".join(map(str, v)) + "\n")

def get_map_pool(file):
    out_of_pool = {}
    with open(file, "r") as f:
        for line in f:
            item = line.split(";")
            date = item[0]
            maps = [int(i) for i in item[1].strip("\n").split(",")]
            out_of_pool[date] = maps
    return out_of_pool

def save_maps(maps):
    with open("data/game_data/maps.txt", "w") as f:
        for map in maps:
            f.write(f"{map}\n")

def get_maps(file):
    maps = []
    with open (file, "r") as f:
        for line in f:
            maps.append(line.strip("\n"))
    return maps

def rename_team_cols(df, team):
    # Separate columns
    t1 = df.loc[(df['t1'] == team)]
    t2 = df.loc[(df['t2'] == team)]

    # List of columns that begin with 't1' or 't2'
    t1_columns = [col for col in t2.columns if col.startswith('t1')]
    t2_columns = [col for col in t2.columns if col.startswith('t2')]

    # Create a mapping of t1 to t2 and t2 to t1 columns
    column_map = {t1: t2 for t1, t2 in zip(t1_columns, t2_columns)}
    column_map.update({t2: t1 for t1, t2 in zip(t1_columns, t2_columns)})

    # Rename the columns according to the mapping
    t2_renamed = t2.rename(columns=column_map)  
    df = pd.concat([t1, t2_renamed])

    return df


def between_dates(df, sd, ed):
    return_df = df.copy(deep=True)
    return_df = return_df.loc[((return_df['date'] >= sd) & (return_df['date'] <= ed))]
    return return_df.copy(deep=True)

def rename_team_cols(df, team):
    # Separate columns
    t1 = df.loc[(df['t1'] == team)]
    t2 = df.loc[(df['t2'] == team)]

    # List of columns that begin with 't1' or 't2'
    t1_columns = [col for col in t2.columns if col.startswith('t1')]
    t2_columns = [col for col in t2.columns if col.startswith('t2')]

    # Create a mapping of t1 to t2 and t2 to t1 columns
    column_map = {t1: t2 for t1, t2 in zip(t1_columns, t2_columns)}
    column_map.update({t2: t1 for t1, t2 in zip(t1_columns, t2_columns)})

    # Rename the columns according to the mapping
    t2_renamed = t2.rename(columns=column_map)  
    df = pd.concat([t1, t2_renamed])

    return df

def get_team_map_stats(team, map, maps_df, date=datetime.today().strftime('%Y-%m-%d'), count=20):
    # Filter by team
    maps_df = maps_df.loc[((maps_df['t1'] == team) | (maps_df['t2'] == team))]

    maps_df = maps_df.loc[(maps_df['date'] < date)]
    if len(maps_df.index) == 0:
        return [0,] * 7
    
    # Check if team has played map
    if len(maps_df.loc[(maps_df['map'] == map)].index) != 0:
        maps_df = maps_df.loc[(maps_df['map'] == map)]

    # Check count flag and if team has played enough games
    maps_df = maps_df.sort_values(by='date', ascending=False)
    if count > 0 and len(maps_df.index) > count:
        maps_df = maps_df.head(count)
    
    maps_df = rename_team_cols(maps_df, team)
    maps_df = maps_df.fillna(0)
    if len(maps_df.index) == 0 or maps_df['t1_fks'].sum() + maps_df['t2_fks'].sum() == 0:
        return [0,] * 7
    
    round_wr = maps_df['t1_rds'].sum() / (maps_df['t2_rds'].sum() + maps_df['t1_rds'].sum())
    fk_percent = maps_df['t1_fks'].sum() / (maps_df['t1_fks'].sum() + maps_df['t2_fks'].sum()) 
    acs = maps_df['t1_acs'].sum() / len(maps_df.index)
    kills = maps_df['t1_kills'].sum() / (maps_df['t1_rds'].sum() + maps_df['t2_rds'].sum())
    assists = maps_df['t1_assists'].sum() / (maps_df['t1_rds'].sum() + maps_df['t2_rds'].sum())
    deaths = maps_df['t1_deaths'].sum() / (maps_df['t1_rds'].sum() + maps_df['t2_rds'].sum())
    kdr = maps_df['t1_kills'].sum() / maps_df['t1_deaths'].sum()
    
    return [round_wr, fk_percent, acs, kills, assists, deaths, kdr]

def get_team_map_stats_df(maps_df):
    def get_map_stats_row(row, count=20):
        t1_stats = get_team_map_stats(row['t1'], row['map'], maps_df, row['date'], count)
        t2_stats = get_team_map_stats(row['t2'], row['map'], maps_df, row['date'], count)
        stats_diff = row.loc[['map_id', 't1', 't2', 'winner', 'date', 'map']].tolist()
        stats_diff.extend(map(operator.sub, t1_stats, t2_stats))
        stats_diff_list.append(stats_diff)
        return row

    stats_diff_list = []
    columns = ['map_id', 't1', 't2', 'winner', 'date', 'map', 'round_wr_diff', 'fk_percent_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff']
    maps_df.apply(get_map_stats_row, axis=1)
    df = pd.DataFrame(data=stats_diff_list, columns=columns)
    return df.copy(deep=True)

def normalize_training_data(df):
    numeric = ['round_wr_diff', 'fk_percent_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff']

    for col in numeric:
        df[col] = df[col] / df[col].abs().max() 
    return df

def format_map_data_nd(maps_df):
    maps_df = maps_df[['map_id', 't1', 't2', 'winner', 'map', 't1_rds', 't2_rds', 't1_fks', 't2_fks', 
                       't1_acs', 't2_acs', 't1_kills', 't1_assists', 't1_deaths', 't2_kills', 't2_assists', 't2_deaths', 'date']]
    maps_df = maps_df.dropna()
    tmd = get_team_map_stats_df(maps_df)
    return normalize_training_data(tmd).copy(deep=True)

def get_series_map_stats_df(df, maps_df): 
    def get_map_stats_row(row, count=20):
        t1_stats = get_team_map_stats(row['t1'], row['map'], maps_df, row['date'], count) 
        t2_stats = get_team_map_stats(row['t2'], row['map'], maps_df, row['date'], count)
        stats_diff = row.tolist()[0:17]
        stats_diff.extend(map(operator.sub, t1_stats, t2_stats))
        stats_diff_list.append(stats_diff)

    columns = ['match_id', 't1', 't2', 'date', 'elo_diff', 'map', 'winner', 'played', 'net_h2h', 'past_diff', 'odds', 'best_odds', 'worst_odds',
               'avg_win%', 'avg_pick%', 'avg_ban%', 'avg_play%', 
               'round_wr_diff', 'fk_percent_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff']

    stats_diff_list = []
    df = df.apply(get_map_stats_row, axis=1)
    return pd.DataFrame(data=stats_diff_list, columns=columns)

def format_veto_data_nd(vetos_df, maps_df):
    tmd = get_series_map_stats_df(vetos_df, maps_df)
    return normalize_training_data(tmd).copy(deep=True)

def transform_series_stats_nd(sds, model, map_pick_model):
    # Fill Nan values
    sds = sds.fillna(0)
    
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
    sds[['play%', 't1_winchance', 't2_winchance']] = 0
    sds = sds.apply(get_mapwin_row, axis=1)
    sds = sds.dropna()

    # Compress matches
    sds = sds[['match_id', 't1', 't2', 'date', 'elo_diff', 'winner', 'net_h2h', 'past_diff', 'odds', 'best_odds', 'worst_odds', 'play%', 't1_winchance', 't2_winchance']]
    sds = sds.sort_values(by='play%', ascending=True)
    matches = sds['match_id'].unique()
    cols = ['match_id', 't1', 't2', 'date', 'elo_diff', 'winner', 'net_h2h', 'past_diff', 'odds', 'best_odds', 'worst_odds', 'winshare']
    data = []
    for match in matches:
        df = sds.copy().loc[sds['match_id'] == match]
        df.drop(df.head(2).index, inplace=True) # Drop least likely maps
        match_data = df.iloc[0].tolist()[:11]
        pred_win = df['t1_winchance'].sum() / (df['t1_winchance'].sum() + df['t2_winchance'].sum()) 
        match_data.append(pred_win)
        data.append(match_data)
    sds = pd.DataFrame(data=data, columns=cols)
    sds['elo_diff'] = sds['elo_diff'] / sds['elo_diff'].abs().max() 
    return sds.copy(deep=True)