import pandas as pd, ast, operator, numpy as np, math, time
from IPython.display import display
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split
from joblib import dump, load
from functools import partial
from map_data_parsing import get_map_pool, get_maps, get_agents, rename_team_cols

# Load data
maps_df = pd.read_csv("data/maps.csv")
series_df = pd.read_csv("data/series.csv", index_col=False)

# Map, Agents data
out_of_pool = get_map_pool("data/game_data/map_pool.txt")
maps = get_maps("data/game_data/maps.txt")
maps_id = list(range(len(maps)))
agents = get_agents("data/game_data/agents.txt")
agents_id = list(range(len(agents)))

def get_map_in_pool(date, index):
    maps = [1,]*10
    for key, value in out_of_pool.items():
        if date >= key:
            continue
        else:
            for value in value:
                maps[value] = 0
        return maps[index]

# Get tier1 version of a dataframe
def get_tier1(df):
    tier1 = pd.read_csv('data/tier1/teams.csv').iloc[:,0].tolist()
    df = df.loc[(df['t1'].isin(tier1) | df['t2'].isin(tier1))]
    return df.copy(deep=True)

def between_dates(df, sd, ed):
    return_df = df.copy(deep=True)
    return_df = return_df.loc[((return_df['date'] > sd) & (return_df['date'] < ed))]
    return return_df.copy(deep=True)

# Get winrate of a team on a specific map, before a date over a count of maps
def get_team_wr_by_map(maps_df, team, map, date, count):
    
    # Sort and Filter
    maps_df.sort_values(by="date", ascending=True, inplace=True)
    maps_df = maps_df.loc[((maps_df["map"] == map) & ((maps_df["t1"] == team) | (maps_df["t2"] == team)))]

    if len(maps_df.index) == 0:
        return 0
    elif count == 0 or maps_df[maps_df["date"] < date].shape[0] < count:
        maps_df = maps_df[maps_df["date"] < date]
    else:
        maps_df = maps_df[maps_df["date"] < date].tail(count)

    # Count wins and return
    wins = maps_df["winner"].value_counts()[team] if team in maps_df["winner"].values else 0
    games = len(maps_df)
    win_rate = wins / games if games > 0 else 0
    return win_rate

# Get winrate of a team on all maps, before a date over a count of maps
def get_team_wr_by_all_maps(maps_df, team, date, count, format):
    # Set column headers and initialize DataFrame
    team_map_winrate_headers = ["team"]
    team_map_winrate_headers.extend(maps_id)

    # Iterate over maps and get winrates
    map_winrates = [int(team)]
    for map in maps_id:
        map_winrates.append(get_team_wr_by_map(maps_df, team, map, date, count))

    # Return DataFrame or list
    if format == "list":
        return map_winrates
    else:
        return pd.DataFrame([map_winrates], columns=team_map_winrate_headers)

# Get winrate of all teams on all maps, before a date over a count of maps
# def get_all_team_wr_by_maps(maps_df, date, count):

#     # Set column headers and initialize DataFrame
#     team_map_winrate_headers = ["team"]
#     team_map_winrate_headers.extend(maps_id)

#     # Iterate over teams and get winrates
#     map_winrates_data = []
#     for team in teams:
#         map_winrates = get_team_wr_by_all_maps(maps_df, team, date, count, format="list")
#         map_winrates_data.append(map_winrates)
#     team_map_winrate_df = pd.DataFrame(columns=team_map_winrate_headers, data=map_winrates_data)
#     return team_map_winrate_df

# Add map pool on match date to series dataframe
def add_map_pool_to_series(series_df):
    # Get map pool for each map
    for map in maps_id:
        series_df[f'{map}_in_pool'] = series_df['date'].apply(lambda x: get_map_in_pool(x, map))
    return series_df

# Explode vetos to a column
# def explode_vetos(series_df):
#     # Set instance variables, add map pool to df
#     series_df = add_map_pool_to_series(series_df)
#     sdf_headers = list(series_df.columns)[0:91]
#     headers = list(series_df.columns)  + ["team", "action", "map"]
#     exploded_rows = []

#     # Iterate over rows
#     for _, row in series_df.iterrows():
#         map_pool = row.iloc[91:101].tolist()
#         # Iterate through pick/ban order
#         for i in [0, 2, 4, 5, 1, 3, 6]:
#             exploded_data = row.iloc[0:91].tolist() + map_pool

#             # Get team
#             if i == 6:
#                 team = 0
#             else:
#                 team = int(row[sdf_headers[i + 3][:2]])
#             exploded_data.append(team)

#             # Get action
#             if "ban" in sdf_headers[i + 3]:
#                 exploded_data.append("ban")
#             elif "pick" in sdf_headers[i + 3]:
#                 exploded_data.append("pick")
#             else:
#                 exploded_data.append("remaining")
            
#             # Get map
#             exploded_data.append(row.iloc[i + 3])

#             # Append to data
#             exploded_rows.append(exploded_data)

#             # Remove map from pool
#             map_pool[row.iloc[i + 3]] = 0

#     # Separate by picks & bans
#     exploded_df = pd.DataFrame(columns=headers, data=exploded_rows)
#     exploded_df = exploded_df.astype({"team": "int64"})
#     exploded_df.to_csv('data/vetos.csv', index=False)
#     return exploded_df

# Get pick, ban, and play rates of a team on a specific map, before a date over a count of series
def get_team_pbrate_by_map(series_df, team, map, date, count):

    series_df = add_map_pool_to_series(series_df)
    series_df = series_df.loc[((series_df[f'{map}_in_pool'] == 1) & ((series_df['t1'] == team) | (series_df['t2'] == team)))]
    series_df = rename_team_cols(series_df, team)

    # Sort by date
    if len(series_df.index) == 0:
        return 0, 0, 0
    elif count == 0 or series_df[series_df["date"] < date].shape[0] < count:
        series_df = series_df[series_df["date"] < date]
    else:
        series_df = series_df[series_df["date"] < date].tail(count)

    # Get playrate
    total = len(series_df.index)
    played = len(series_df.loc[((series_df['t1_pick'] == map) | (series_df['t2_pick'] == map) | (series_df['remaining'] == map))].index)

    picked = len(series_df.loc[(series_df['t1_pick'] == map)].index)
    banned = len(series_df.loc[((series_df['t1_ban1'] == map) | (series_df['t1_ban2'] == map))].index)

    # Calculate rates
    banrate = banned / total if total > 0 else 0
    pickrate = picked / total if total > 0 else 0
    playrate = played / total if total > 0 else 0
    return pickrate, banrate, playrate

# Get pick, ban, and play rates of a team on all maps, before a date over a count of series
def get_team_pbrate_by_all_maps(series_df, team, date, count):
    # Set column headers
    headers = ["team"]
    headers.extend([header for map in maps_id for header in (f"{map}_pickrate", f"{map}_banrate", f"{map}_playrate")])

    # Iterate over maps and get pick, ban, and play rates
    pb_rates = [team]
    for map in maps_id:
        pb_rates.extend(get_team_pbrate_by_map(series_df, team, map, date, count))

    # Return df
    return pd.DataFrame([pb_rates], columns=headers)

# Get playrate of map between teams
def get_h2h_map_history(series_df, t1, t2, map, date):
    series_df = series_df.loc[(((series_df['t1'] == t1) |  (series_df['t2'] == t1)) & ((series_df['t1'] == t2) |  (series_df['t2'] == t2)) & (series_df['date'] < date))]
    times_played = len(series_df.loc[((series_df['t1_pick'] == map) | (series_df['t2_pick'] == map) | (series_df['remaining'] == map))].index)
    return 1 if times_played > 0 else 0

# Get win, pick, ban, and play rate of a team on a map before a date
def get_team_data_by_map_row(row, count=5):
    t1_win = get_team_wr_by_map(maps_df, row[1], row[4], row[3], count)
    t1_pick, t1_ban, t1_play = get_team_pbrate_by_map(series_df, row[1], row[4], row[3], count)
    t2_win = get_team_wr_by_map(maps_df, row[2], row[4], row[3], count)
    t2_pick, t2_ban, t2_play = get_team_pbrate_by_map(series_df, row[2], row[4], row[3], count)
    row[11:19] = [t1_win, t1_pick, t1_ban, t1_play, t2_win, t2_pick, t2_ban, t2_play]
    return row

# Get pick, ban, play, and win rates of a team on all maps, before a date over a count of series
def get_team_data(team, date, count, format):
    print("Getting team data for team " + str(team))
    pb_data = get_team_pbrate_by_all_maps(series_df, team, date, count).reset_index(drop=True)
    pb_data.drop(columns=["team"], inplace=True)
    map_data = get_team_wr_by_all_maps(maps_df, team, date, count, format="df").reset_index(drop=True)
    map_data.drop(columns=["team"], inplace=True)
    if format == "list":
        return pb_data.values.flatten().tolist() + map_data.values.flatten().tolist()
    else:
        return pd.concat([pb_data, map_data], axis=1)
    
# Vectorized team data function
def get_team_data_row(row, count):
    t1_data = get_team_data(row[1], row[10], count, "list")
    t2_data = get_team_data(row[2], row[10], count, "list")
    row[11:51] = t1_data
    row[51:91] = t2_data
    return row

def get_team_wr_by_all_maps_row(row):

    # Iterate over maps and get winrates
    map_winrates = []
    for map in maps_id:
        map_winrates.append(get_team_wr_by_map(maps_df, row[1], map, row[15], 0) - get_team_wr_by_map(maps_df, row[2], map, row[15], 0))

    row[17:27] = map_winrates
    return row

def get_winrate_diff_df(series_df):
    series_df['winner'] = False
    series_df.loc[series_df['t1_mapwins'] > series_df['t2_mapwins'], 'winner'] = True
    new_cols = [f'{i}_wr_diff' for i in range(10)]
    series_df[new_cols] = 0
    series_df = series_df.apply(get_team_wr_by_all_maps_row, axis=1, raw=True)
    series_df = series_df[['match_id', 't1', 't2', 'winner', 'date'] + new_cols]
    return series_df

# Set map played flag
def map_played_status(row, map_id):
    # Maps in pick or remaining columns are played
    if map_id in [row['t1_pick'], row['t2_pick'], row['remaining']]:
        return True
    # Maps in ban columns are not played
    elif map_id in [row['t1_ban1'], row['t1_ban2'], row['t2_ban1'], row['t2_ban2']]:
        return False
    # If map is neither picked nor banned
    return None

def explode_map_choices(series_df):
    sdf = series_df.copy()
    sdf['net_h2h'] = sdf['net_h2h'].fillna(0)
    data = []
    for map in maps_id:
        temp_df = sdf.apply(lambda row: pd.Series({
            'match_id': row['match_id'],
            't1': row['t1'],
            't2': row['t2'],
            'date': row['date'],
            'map': map,
            'winner': row['winner'],
            'played': map_played_status(row, map),
            'net_h2h': row['net_h2h'],
            'past_diff': (row['t1_past'] - row['t2_past']),
            'best_odds': row['best_odds'],
            'worst_odds': row['worst_odds']
        }), axis=1)
        data.append(temp_df)

    exploded_df = pd.concat(data)
    
    # Drop rows where map status is None (map not involved in the match at all)
    exploded_df = exploded_df.dropna(subset=['played'])
    exploded_df.sort_values(by="match_id", ascending=True, inplace=True)

    # Get win, pick, ban, and playrates
    exploded_df[['t1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%']] = 0
    exploded_df = exploded_df.apply(get_team_data_by_map_row, raw=True, axis=1)
    exploded_df = exploded_df.loc[~((exploded_df['t1_win%'] == 0) & (exploded_df['t1_pick%'] == 0) & (exploded_df['t1_ban%'] == 0) & (exploded_df['t1_play%'] == 0) 
                    & (exploded_df['t2_win%'] == 0) & (exploded_df['t2_pick%'] == 0) & (exploded_df['t2_ban%'] == 0) & (exploded_df['t2_play%'] == 0))]
    return exploded_df.copy(deep=True)

def transform_series_stats(sds, models, map_pick_model):
    # Fill Nan values
    sds = sds.fillna(0)
    
    def get_mapwin_row(row):
        map_features = ['round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 'pistol_wr_diff', 
                    'eco_wr_diff', 'antieco_wr_diff', 'fullbuy_wr_diff', 'acs_diff', 'kills_diff', 
                    'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff', 'kast_diff', 'rating_diff', 'mks_diff', 'clutch_diff', 'econ_diff']
        df = pd.DataFrame(data=[row[19:38]], columns=map_features)
        map_pick_features = ['t1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%']
        pick_df = pd.DataFrame(data=[row[11:19]], columns=map_pick_features)
        row[38] = map_pick_model.predict_proba(pick_df)[0][0]

        model = models[row[4]]
        row[39] = model.predict_proba(df)[0][0] * row[38] if model is not None else None
        row[40] = model.predict_proba(df)[0][1] * row[38] if model is not None else None
        return row

    # Get map play chance, and each team's winrate
    sds[['play%', 't1_winchance', 't2_winchance']] = 0
    sds = sds.apply(get_mapwin_row, raw=True, axis=1)
    sds = sds.dropna()

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
    return sds.copy(deep=True)