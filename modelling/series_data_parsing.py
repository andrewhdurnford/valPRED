import pandas as pd, ast, operator, numpy as np, math, time
from IPython.display import display
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split
from joblib import dump, load
from functools import partial
from map_data_parsing import get_map_pool, get_maps, get_agents

# Load data
maps_df = pd.read_csv("data/maps.csv")
series_df = pd.read_csv("data/series.csv", index_col=False)
# teams = pd.read_csv("data/teams.csv", header=None)[0].to_list()
vetos_df = pd.read_csv("data/vetos.csv")
# tvd = pd.read_csv("data/tvd_inp.csv")

# Map, Agents data
out_of_pool = get_map_pool("data/game_data/map_pool.txt")
maps = get_maps("data/game_data/maps.txt")
maps_id = list(range(len(maps)))
agents = get_agents("data/game_data/agents.txt")
agents_id = list(range(len(agents)))

def concat_df(df1, df2):
    return (df2.copy() if df1.empty else pd.concat([df1, df2], ignore_index=True, sort=False))

def get_tier1_series(series_df):
    tier1_teams = []
    with open("data/tier1_teams.csv", "r") as file:
        for line in file:
            tier1_teams.append(int(line.strip().split(",")[0]))
    series_df = series_df[(series_df["t1"].isin(tier1_teams))]
    series_df = series_df[(series_df["t2"].isin(tier1_teams))]
    series_df.to_csv("data/tier1_series_encoded.csv", index=False)

def get_map_pool(date, index):
    maps = [1,]*10
    for key, value in out_of_pool.items():
        if date >= key:
            continue
        else:
            for value in value:
                maps[value] = 0
        return maps[index]

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
def get_all_team_wr_by_maps(maps_df, date, count):

    # Set column headers and initialize DataFrame
    team_map_winrate_headers = ["team"]
    team_map_winrate_headers.extend(maps_id)

    # Iterate over teams and get winrates
    map_winrates_data = []
    for team in teams:
        map_winrates = get_team_wr_by_all_maps(maps_df, team, date, count, format="list")
        map_winrates_data.append(map_winrates)
    team_map_winrate_df = pd.DataFrame(columns=team_map_winrate_headers, data=map_winrates_data)
    return team_map_winrate_df

# Add map pool on match date to series dataframe
def add_map_pool_to_series(series_df):
    # Get map pool for each map
    for map in maps_id:
        series_df[f'{map}_in_pool'] = series_df['date'].apply(lambda x: get_map_pool(x, map))
    return series_df

# Explode vetos to a column
def explode_vetos(series_df):
    # Set instance variables, add map pool to df
    series_df = add_map_pool_to_series(series_df)
    sdf_headers = list(series_df.columns)[0:91]
    headers = list(series_df.columns)  + ["team", "action", "map"]
    exploded_rows = []

    # Iterate over rows
    for _, row in series_df.iterrows():
        map_pool = row.iloc[91:101].tolist()
        # Iterate through pick/ban order
        for i in [0, 2, 4, 5, 1, 3, 6]:
            exploded_data = row.iloc[0:91].tolist() + map_pool

            # Get team
            if i == 6:
                team = 0
            else:
                team = int(row[sdf_headers[i + 3][:2]])
            exploded_data.append(team)

            # Get action
            if "ban" in sdf_headers[i + 3]:
                exploded_data.append("ban")
            elif "pick" in sdf_headers[i + 3]:
                exploded_data.append("pick")
            else:
                exploded_data.append("remaining")
            
            # Get map
            exploded_data.append(row.iloc[i + 3])

            # Append to data
            exploded_rows.append(exploded_data)

            # Remove map from pool
            map_pool[row.iloc[i + 3]] = 0

    

    # Separate by picks & bans
    exploded_df = pd.DataFrame(columns=headers, data=exploded_rows)
    exploded_df = exploded_df.astype({"team": "int64"})
    return exploded_df

# Get pick, ban, and play rates of a team on a specific map, before a date over a count of series
def get_team_pbrate_by_map(vdf, team, map, date, count):

    # vdf = vdf.astype({f'{map}_in_pool': 'bool'})
    vdf = vdf.loc[((vdf[f'{map}_in_pool'] == 1) & ((vdf['t1'] == team) | (vdf['t2'] == team)))]

    # Sort by date
    if len(vdf.index) == 0:
        return 0, 0, 0
    elif count == 0 or vdf[vdf["date"] < date].shape[0] < count:
        vdf = vdf[vdf["date"] < date]
    else:
        vdf = vdf[vdf["date"] < date].tail(count)

    # Get playrate
    total = vdf['match_id'].nunique()
    played = len(vdf.loc[((vdf['map'] == map) & (vdf['action'] != 'ban'))].index)

    # Get pick and ban rate
    vdf = vdf.loc[((vdf[f'{map}_in_pool']) & (vdf['team'] == team))]
    picked_df = vdf.loc[vdf['action'] == 'pick']
    banned_df = vdf.loc[vdf['action'] == 'ban']

    if not len(picked_df.index) == 0:
        picked = picked_df['map'].value_counts()[map] if map in picked_df['map'].values else 0
        ct_pick = len(picked_df.index)
    else:
        picked = 0
        ct_pick = 0
    
    if not len(banned_df.index) == 0:
        banned = banned_df['map'].value_counts()[map] if map in banned_df['map'].values else 0
        ct_ban = len(banned_df.index)
    else:
        banned = 0
        ct_ban = 0

    # Calculate rates
    banrate = banned / ct_ban if ct_ban > 0 else 0
    pickrate = picked / ct_pick if ct_pick > 0 else 0
    playrate = played / total if total > 0 else 0
    return pickrate, banrate, playrate

# Get pick, ban, and play rates of a team on all maps, before a date over a count of series
def get_team_pbrate_by_all_maps(vdf, team, date, count):
    # Set column headers
    headers = ["team"]
    headers.extend([header for map in maps_id for header in (f"{map}_pickrate", f"{map}_banrate", f"{map}_playrate")])

    # Iterate over maps and get pick, ban, and play rates
    pb_rates = [team]
    for map in maps_id:
        pb_rates.extend(get_team_pbrate_by_map(vdf, team, map, date, count))

    # Return df
    return pd.DataFrame([pb_rates], columns=headers)

# Get pick, ban, play, and win rates of a team on all maps, before a date over a count of series
def get_team_data(team, date, count, format):
    print("Getting team data for team " + str(team))
    pb_data = get_team_pbrate_by_all_maps(vetos_df, team, date, count).reset_index(drop=True)
    pb_data.drop(columns=["team"], inplace=True)
    map_data = get_team_wr_by_all_maps(maps_df, team, date, count, format="df").reset_index(drop=True)
    map_data.drop(columns=["team"], inplace=True)
    if format == "list":
        return pb_data.values.flatten().tolist() + map_data.values.flatten().tolist()
    else:
        return pd.concat([pb_data, map_data], axis=1)
    
def get_team_data_row(row, count):
    t1_data = get_team_data(row[1], row[10], count, "list")
    t2_data = get_team_data(row[2], row[10], count, "list")
    row[11:51] = t1_data
    row[51:91] = t2_data
    return row

def transform_series_data(sdf, count):
    sdf = sdf.drop(columns=['t1_mapwins', 't2_mapwins', 'net_h2h', 't1_past', 't2_past'], axis=1)
    columns = list(sdf.columns)
    team_data_columns = []
    for team_suffix in ['t1', 't2']:
        team_data_columns.extend([f"{map}_{team_suffix}_{rate}" for map in maps_id for rate in ['pickrate', 'banrate', 'playrate']])
        team_data_columns.extend([f"{map}_{team_suffix}_winrate" for map in maps_id])
    columns.extend(team_data_columns)
    sdf = sdf.reindex(columns=columns)
    sdf = sdf.apply(get_team_data_row, count=count, axis=1, raw=True)
    sdf = explode_vetos(sdf)
    sdf_cols = ['match_id', 't1', 't2', 'date', 'team', 'action', 'map'] + [f'{map}_in_pool' for map in maps_id] + team_data_columns
    sdf = sdf[sdf_cols]
    sdf = sdf.loc[(sdf['action'] != 'remaining')]
    return sdf

def concatenate_row(row):
    if row[2] == row[3]:
        row[16:56] = row[56:96]
    return row

def concatenate_tvd(tvd):
    tvd.drop(columns=['date'], inplace=True)
    tvd = tvd.apply(concatenate_row, axis=1, raw=True)
    tvd.drop(tvd.columns[56:96], axis=1, inplace=True)
    for i in maps_id:
        tvd = tvd.loc[((tvd[f'{i}_t1_playrate'] != 0))] #& (tvd[f'{i}_in_pool'] == 1))]
        tvd.rename(columns={f'{i}_t1_playrate': f'{i}_playrate', f'{i}_t1_pickrate': f'{i}_pickrate', f'{i}_t1_banrate': f'{i}_banrate', f'{i}_t1_winrate': f'{i}_winrate'}, inplace=True)
        tvd = tvd.astype({f'{i}_in_pool': 'bool'})
    tvd.drop(['t1','t2'], inplace=True, axis=1)
    tvd.reset_index(drop=True, inplace=True)
    tvd = tvd.astype({'match_id': 'int32', 'team': 'int32', 'map': 'int32'})
    return tvd

def concatenate_tvd_inp(tvd):
    # tvd.drop(columns=['date'], inplace=True)
    # tvd = tvd.apply(concatenate_row, axis=1, raw=True)
    # tvd.drop(tvd.columns[56:96], axis=1, inplace=True)

    for i in maps_id:
        tvd = tvd.loc[(((tvd[f'{i}_t1_playrate'] != 0) | (tvd[f'{i}_t2_playrate'] != 0)) | (tvd[f'{i}_in_pool'] == 0))]
        # tvd.rename(columns={f'{i}_t1_playrate': f'{i}_playrate', f'{i}_t1_pickrate': f'{i}_pickrate', f'{i}_t1_banrate': f'{i}_banrate', f'{i}_t1_winrate': f'{i}_winrate'}, inplace=True)
        tvd = tvd.astype({f'{i}_in_pool': 'bool'})
    tvd.drop(['t1','t2'], inplace=True, axis=1)
    tvd.reset_index(drop=True, inplace=True)
    tvd = tvd.astype({'match_id': 'int32', 'team': 'int32', 'map': 'int32'})
    return tvd

def get_tier1_tvd(tvd):
    tier1 = pd.read_csv("data/tier1_teams.csv", index_col=False, header=None)[0].tolist()
    tvd = tvd[tvd['team'].isin(tier1)]
    return tvd

# for i in [0, 5, 10, 20]:
#     tvd = transform_series_data(series_df, i)
#     tvd = concatenate_tvd_inp(tvd)
#     tvd = get_tier1_tvd(tvd)
#     tvd.to_csv(f'data/tier1_tvd{i}.csv', index=False)