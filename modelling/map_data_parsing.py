import pandas as pd, math, ast, json, operator
from IPython.display import display
from datetime import datetime
from sklearn import preprocessing

# Load data
maps_df = pd.read_csv("data/maps.csv")
series_df = pd.read_csv("data/series.csv", index_col=False)
teams = pd.read_csv('data/teams.csv').iloc[:,0].tolist()
tier1_teams = pd.read_csv('data/tier1/tier1_teams.csv').iloc[:,0].tolist()
tier1_maps = pd.read_csv('data/tier1/tier1_maps.csv')
tier1_maps = tier1_maps.drop(columns=tier1_maps.columns[0], axis=1)
agent_wrs = pd.read_csv('data/agent_wrs.csv', index_col=False)

# Map, Agents data
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

def save_agents(agents):
    with open("data/game_data/agents.txt", "w") as f:
        for agent in agents:
            f.write(f"{agent}\n")

def get_agents(file):
    agents = []
    with open(file, "r") as f:
        for line in f:
            agents.append(line.strip("\n"))
    return agents

# Load game data
out_of_pool = get_map_pool("data/game_data/map_pool.txt")
maps = get_maps("data/game_data/maps.txt")
maps_id = list(range(len(maps)))
agents = get_agents("data/game_data/agents.txt")
agents_id = list(range(len(agents)))

# Get winrate of an agent on a specific map
def get_agent_wr_by_map(maps_df, agent, map):  
    # Filter row by map and agent
    maps_df = maps_df.loc[maps_df["map"] == map]
    t1_games = maps_df.loc[((maps_df['t1_agent1'] == agent) | (maps_df['t1_agent2'] == agent) | (maps_df['t1_agent3'] == agent) | (maps_df['t1_agent4'] == agent) | (maps_df['t1_agent5'] == agent))]
    t2_games = maps_df.loc[((maps_df['t2_agent1'] == agent) | (maps_df['t2_agent2'] == agent) | (maps_df['t2_agent3'] == agent) | (maps_df['t2_agent4'] == agent) | (maps_df['t2_agent5'] == agent))]

    # Count wins and total games
    wins = len(t1_games.loc[(t1_games['winner'] == t1_games['t1'])].index) + len(t2_games.loc[(t2_games['winner'] == t2_games['t1'])].index)
    total_games = len(t1_games.index) + len(t2_games.index)
    return wins/total_games if total_games > 0 else 0

# Get winrate of all agents on all maps
def get_all_agent_wr_by_maps(maps_df):

    # Set column headers and initialize DataFrame
    agent_map_winrate_headers = ["agent"]
    agent_map_winrate_headers.extend(maps)
    agent_map_winrate_df = pd.DataFrame(columns=agent_map_winrate_headers)

    # Iterate over maps and get winrates
    for agent in agents_id:
        map_winrates = [agents[agent]]
        for map in maps_id:
            map_winrates.append(get_agent_wr_by_map(maps_df, agent, map))
        map_winrates_df = pd.DataFrame([map_winrates], columns=agent_map_winrate_headers)
        agent_map_winrate_df = (map_winrates_df.copy() if agent_map_winrate_df.empty else pd.concat([agent_map_winrate_df, map_winrates_df], ignore_index=True, sort=False))

    return agent_map_winrate_df

def get_all_agent_wr(maps_df):
    agent_map_winrate_headers = ["agent", "winrate"]
    agent_wrs = []
    for agent in agents_id:
        maps_df = maps_df.loc[~(
            ((maps_df['t1_agent1'] == agent) | (maps_df['t1_agent2'] == agent) | (maps_df['t1_agent3'] == agent) | 
            (maps_df['t1_agent4'] == agent) | (maps_df['t1_agent5'] == agent)) & 
            ((maps_df['t2_agent1'] == agent) | (maps_df['t2_agent2'] == agent) | (maps_df['t2_agent3'] == agent) | 
            (maps_df['t2_agent4'] == agent) | (maps_df['t2_agent5'] == agent))
        )]        
        t1_games = maps_df.loc[((maps_df['t1_agent1'] == agent) | (maps_df['t1_agent2'] == agent) | (maps_df['t1_agent3'] == agent) | (maps_df['t1_agent4'] == agent) | (maps_df['t1_agent5'] == agent))]
        t2_games = maps_df.loc[((maps_df['t2_agent1'] == agent) | (maps_df['t2_agent2'] == agent) | (maps_df['t2_agent3'] == agent) | (maps_df['t2_agent4'] == agent) | (maps_df['t2_agent5'] == agent))]
        # Count wins and total games
        wins = len(t1_games.loc[(t1_games['winner'] == t1_games['t1'])].index) + len(t2_games.loc[(t2_games['winner'] == t2_games['t1'])].index)
        total_games = len(t1_games.index) + len(t2_games.index)
        winrate = wins/total_games if total_games > 0 else 0
        agent_wrs.extend([[agents[agent], winrate]])
    agent_wr_df = pd.DataFrame(data=agent_wrs, columns=agent_map_winrate_headers)
    agent_wr_df.to_csv('data/agent_wrs_nomap.csv', index=False)


def get_agent_wr_from_df(agent, map, agent_wrs):
    wr = agent_wrs.iloc[[int(agent)]][map].sum()
    return wr

def get_comp_wrs_from_row(row):
    map = maps[int(row[4])]
    row[67] = (get_agent_wr_from_df(row[5], map, agent_wrs) 
                         + get_agent_wr_from_df(row[6], map, agent_wrs) 
                         + get_agent_wr_from_df(row[7], map, agent_wrs)
                         + get_agent_wr_from_df(row[8], map, agent_wrs)
                         + get_agent_wr_from_df(row[9], map, agent_wrs))/5
    row[68] = (get_agent_wr_from_df(row[10], map, agent_wrs) 
                         + get_agent_wr_from_df(row[11], map, agent_wrs) 
                         + get_agent_wr_from_df(row[12], map, agent_wrs)
                         + get_agent_wr_from_df(row[13], map, agent_wrs)
                         + get_agent_wr_from_df(row[14], map, agent_wrs))/5
    return row

def rename_team_cols(maps_df, team):
    # Separate columns
    t1 = maps_df.loc[(maps_df['t1'] == team)]
    t2 = maps_df.loc[(maps_df['t2'] == team)]

    # List of columns that begin with 't1' or 't2'
    t1_columns = [col for col in t2.columns if col.startswith('t1')]
    t2_columns = [col for col in t2.columns if col.startswith('t2')]

    # Create a mapping of t1 to t2 and t2 to t1 columns
    column_map = {t1: t2 for t1, t2 in zip(t1_columns, t2_columns)}
    column_map.update({t2: t1 for t1, t2 in zip(t1_columns, t2_columns)})

    # Rename the columns according to the mapping
    t2_renamed = t2.rename(columns=column_map)  
    maps_df = pd.concat([t1, t2_renamed])

    return maps_df

def get_comp_wrs_df(maps_df):
    maps_df['t1_comp_wr'] = pd.Series(dtype='float')
    maps_df['t2_comp_wr'] = pd.Series(dtype='float')
    maps_df = maps_df.apply(get_comp_wrs_from_row, raw=True, axis=1)
    return maps_df

def get_team_map_stats(team, map, maps_df=maps_df, date=datetime.today().strftime('%Y-%m-%d'), count=0):
    # Filter by team and map
    maps_df = maps_df.loc[((maps_df['t1'] == team) | (maps_df['t2'] == team))]

    # Check if team has played before date
    if len(maps_df.loc[(maps_df['date'] < date)].index) != 0:
        maps_df = maps_df.loc[(maps_df['date'] < date)]
    
    # Check map flag and if team has played map
    if map is not None and len(maps_df.loc[(maps_df['map'] == map)].index) != 0:
        maps_df = maps_df.loc[((maps_df['map'] == map) & (maps_df['date'] < date))]

    # Check count flag and if team has played enough games
    maps_df = maps_df.sort_values(by='date', ascending=False)
    if count > 0 and len(maps_df.index) > count:
        maps_df = maps_df.head(10)
    
    maps_df = rename_team_cols(maps_df, team)
    maps_df = maps_df.fillna(0)
    maps_df = get_comp_wrs_df(maps_df)

    comp_wr = maps_df['t1_comp_wr'].sum() / len(maps_df.index)
    round_wr = maps_df['t1_rds'].sum() / (maps_df['t2_rds'].sum() + maps_df['t1_rds'].sum())
    retake_wr = maps_df['t1_retakes_won'].sum() / (maps_df['t1_retakes_lost'].sum() + maps_df['t1_retakes_won'].sum()) if (maps_df['t1_retakes_lost'].sum() + maps_df['t1_retakes_won'].sum()) > 0 else 0
    postplant_wr = maps_df['t1_postplants_won'].sum() / (maps_df['t1_postplants_won'].sum() + maps_df['t1_postplants_lost'].sum()) if (maps_df['t1_postplants_won'].sum() + maps_df['t1_postplants_lost'].sum()) > 0 else 0
    fk_percent = (maps_df['t1_atk_fks'].sum() + maps_df['t1_def_fks'].sum()) / (maps_df['t1_atk_fks'].sum() + maps_df['t1_def_fks'].sum() + maps_df['t2_atk_fks'].sum() + maps_df['t2_def_fks'].sum()) if (maps_df['t1_atk_fks'].sum() + maps_df['t1_def_fks'].sum() + maps_df['t2_atk_fks'].sum() + maps_df['t2_def_fks'].sum()) > 0 else 0
    pistol_wr = maps_df['t1_pistols'].sum() / (maps_df['t1_pistols'].sum() + maps_df['t2_pistols'].sum()) if (maps_df['t1_pistols'].sum() + maps_df['t2_pistols'].sum()) > 0 else 0
    eco_wr = maps_df['t1_ecos_won'].sum() / (maps_df['t1_ecos_won'].sum() + maps_df['t1_ecos_lost'].sum()) if (maps_df['t1_ecos_won'].sum() + maps_df['t1_ecos_lost'].sum()) > 0 else 0
    antieco_wr = maps_df['t2_ecos_lost'].sum() / (maps_df['t2_ecos_won'].sum() + maps_df['t2_ecos_lost'].sum()) if (maps_df['t2_ecos_won'].sum() + maps_df['t2_ecos_lost'].sum()) > 0 else 0
    fullbuy_wr = maps_df['t1_fullbuys_won'].sum() / (maps_df['t1_fullbuys_won'].sum() + maps_df['t1_fullbuys_lost'].sum()) if (maps_df['t1_fullbuys_won'].sum() + maps_df['t1_fullbuys_lost'].sum()) > 0 else 0
    acs = maps_df['t1_avg_acs'].sum() / len(maps_df.index)
    kills = maps_df['t1_kills'].sum() / (maps_df['t1_rds'].sum() + maps_df['t2_rds'].sum())
    assists = maps_df['t1_assists'].sum() / (maps_df['t1_rds'].sum() + maps_df['t2_rds'].sum())
    deaths = maps_df['t1_deaths'].sum() / (maps_df['t1_rds'].sum() + maps_df['t2_rds'].sum())
    kdr = maps_df['t1_kills'].sum() / maps_df['t1_deaths'].sum()
    kadr = (maps_df['t1_kills'].sum() + maps_df['t1_assists'].sum()) / maps_df['t1_deaths'].sum()
    kast = maps_df['t1_kast'].sum() / len(maps_df.index)
    rating = (maps_df['t1_atk_rating'].sum() + maps_df['t1_def_rating'].sum()) / (len(maps_df.index) * 2)
    mks = maps_df['t1_mks'].sum() / len(maps_df.index)
    clutches = maps_df['t1_clutches'].sum() / (maps_df['t1_clutches'].sum() + maps_df['t2_clutches'].sum()) if (maps_df['t1_clutches'].sum() + maps_df['t2_clutches'].sum()) > 0 else 0
    econ = maps_df['t1_econ'].sum() / len(maps_df.index)
    
    return [comp_wr, round_wr, retake_wr, postplant_wr, fk_percent, pistol_wr, eco_wr, antieco_wr, fullbuy_wr, acs, kills, assists, deaths, kdr, kadr, kast, rating, mks, clutches, econ]

def get_team_map_stats_df(maps_df, maps, series_df=None, count=0):
    if series_df is None:
        new_columns = ['map_id', 't1', 't2', 'winner', 'map', 'date', 
                    'comp_wr_diff', 'round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 'pistol_wr_diff', 
                    'eco_wr_diff', 'antieco_wr_diff', 'fullbuy_wr_diff', 'acs_diff', 'kills_diff', 
                    'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff', 'kast_diff', 'rating_diff', 'clutch_diff', 'econ_diff']
        maps_df_copy = maps_df
        stats_diff_list = []

        for _, row in maps_df.iterrows():
            row_map = row['map'] if maps else None
            t1_stats = get_team_map_stats(row['t1'], row_map, maps_df_copy, row['date'])
            t2_stats = get_team_map_stats(row['t2'], row_map, maps_df_copy, row['date'])
            stats_diff = [row['map_id'], row['t1'], row['t2'], row['winner'], row['map'], row['date']]
            stats_diff.extend(map(operator.sub, t1_stats, t2_stats))
            stats_diff_list.append(stats_diff)

        stats_diff_df = pd.DataFrame(data=stats_diff_list, columns=new_columns)
    else:
        columns = ['match_id', 't1', 't2', 'winner', 'date', 'net_h2h', 'past_diff', 'comp_wr_diff', 'round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 
                   'pistol_wr_diff', 'eco_wr_diff', 'antieco_wr_diff', 'fullbuy_wr_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff', 
                   'kast_diff', 'rating_diff', 'mks_diff', 'clutch_diff', 'econ_diff']

        stats_diff_list = []
        for _, row in series_df.iterrows():
            t1_stats = get_team_map_stats(row['t1'], None, maps_df, row['date'], count)
            t2_stats = get_team_map_stats(row['t2'], None, maps_df, row['date'], count)
            stats_diff = [row['match_id'], row['t1'], row['t2'], row['winner'], row['date'], row['net_h2h'], row['past_diff']]
            stats_diff.extend(map(operator.sub, t1_stats, t2_stats))
            stats_diff_list.append(stats_diff)
        stats_diff_df = pd.DataFrame(data=stats_diff_list, columns=columns)
    return stats_diff_df

def get_tier1_maps(maps_df):
    maps_df = maps_df.loc[((maps_df['t1'].isin(tier1_teams)) | (maps_df['t2'].isin(tier1_teams)))]
    return maps_df

def normalized_map_tmd(maps_tmd, series=False):
    numeric = ['comp_wr_diff', 'round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 'pistol_wr_diff', 
                'eco_wr_diff', 'antieco_wr_diff', 'fullbuy_wr_diff', 'acs_diff', 'kills_diff', 
                'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff', 'kast_diff', 'rating_diff', 'clutch_diff', 'econ_diff']
    if series:
        numeric.extend(['net_h2h', 'past_diff'])
    for col in numeric:
        maps_tmd[col] = maps_tmd[col] / maps_tmd[col].abs().max() 
    return maps_tmd

def normalize_tier1_tmd(tier1, maps_df, maps, series_df=None, count=0):
    prefix = ''
    if tier1:
        maps_df = maps_df.loc[((maps_df['t1'].isin(tier1_teams)) | (maps_df['t2'].isin(tier1_teams)))]
        prefix = 'data/tier1/tier1_'
    if maps:
        tier1_tmd = get_team_map_stats_df(tier1_maps, True)
        normalized_map_tmd(tier1_tmd).to_csv(f'{prefix}normalized_tmd.csv', index=False)
    else:
        series_df = series_df.loc[((series_df['t1'].isin(tier1_teams)) | (series_df['t2'].isin(tier1_teams)))].copy()
        series_df['winner'] = False
        series_df.loc[series_df['t1_mapwins'] > series_df['t2_mapwins'], 'winner'] = True
        series_df = series_df.fillna(0)
        series_df['past_diff'] = series_df['t1_past'] - series_df['t2_past']
        series_df.drop(["t1_ban1", "t1_ban2","t2_ban1", "t2_ban2", "t1_pick", "t2_pick", "remaining", "t1_mapwins", "t2_mapwins", "t1_past", "t2_past"], axis=1, inplace=True)
        series_df = series_df[['match_id', 't1', 't2', 'winner', 'date', 'net_h2h', 'past_diff']]

        tier1_tmd = get_team_map_stats_df(maps_df, False, series_df, count)
        if count > 0:
            normalized_map_tmd(tier1_tmd, True).to_csv(f'{prefix}normalized_tmd_nomap_{count}.csv', index=False)
        else:
            normalized_map_tmd(tier1_tmd, True).to_csv(f'{prefix}normalized_tmd_nomap.csv', index=False)

# normalize_tier1_tmd(False, maps_df, False, series_df, 0)