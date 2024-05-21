import pandas as pd, operator
from datetime import datetime
from dateutil.relativedelta import relativedelta
from IPython.display import display

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

def get_date_before(date_str):
    # Convert the string to a datetime object
    date = datetime.strptime(date_str, "%Y-%m-%d")
    
    # Subtract three months from the date
    date_three_months_before = date - relativedelta(months=3)
    
    # Convert the datetime object back to a string in the same format
    return date_three_months_before.strftime("%Y-%m-%d")

# Load game data
maps = get_maps("data/game_data/maps.txt")
maps_id = list(range(len(maps)))
agents = get_agents("data/game_data/agents.txt")
agents_id = list(range(len(agents)))

# # Get winrate of an agent on a specific map within the last 3 months
# def get_agent_wr_by_map(maps_df, agent, map, date):  
#     # Filter row by map and agent
#     maps_df = maps_df.loc[maps_df["map"] == map]
#     t1_games = maps_df.loc[((maps_df['t1_agent1'] == agent) | (maps_df['t1_agent2'] == agent) | (maps_df['t1_agent3'] == agent) | (maps_df['t1_agent4'] == agent) | (maps_df['t1_agent5'] == agent))]
#     t2_games = maps_df.loc[((maps_df['t2_agent1'] == agent) | (maps_df['t2_agent2'] == agent) | (maps_df['t2_agent3'] == agent) | (maps_df['t2_agent4'] == agent) | (maps_df['t2_agent5'] == agent))]

#     if len(t1_games.index) == 0 and len(t2_games.index) == 0:
#         return 0
    
#     before = get_date_before(date)

#     if len(maps_df.loc[((maps_df['date'] < date) & (maps_df['date'] > before))].index) != 0:
#         maps_df = maps_df.loc[(maps_df['date'] < date)]

#     # Count wins and total games
#     wins = len(t1_games.loc[(t1_games['winner'] == t1_games['t1'])].index) + len(t2_games.loc[(t2_games['winner'] == t2_games['t1'])].index)
#     total_games = len(t1_games.index) + len(t2_games.index)
#     return wins/total_games if total_games > 0 else 0

# # Get winrate of all agents on all maps
# def get_all_agent_wr_by_maps(maps_df):

#     # Set column headers and initialize DataFrame
#     agent_map_winrate_headers = ["agent"]
#     agent_map_winrate_headers.extend(maps)
#     agent_map_winrate_df = pd.DataFrame(columns=agent_map_winrate_headers)

#     # Iterate over maps and get winrates
#     for agent in agents_id:
#         map_winrates = [agents[agent]]
#         for map in maps_id:
#             map_winrates.append(get_agent_wr_by_map(maps_df, agent, map))
#         map_winrates_df = pd.DataFrame([map_winrates], columns=agent_map_winrate_headers)
#         agent_map_winrate_df = (map_winrates_df.copy() if agent_map_winrate_df.empty else pd.concat([agent_map_winrate_df, map_winrates_df], ignore_index=True, sort=False))

#     return agent_map_winrate_df

# def get_all_agent_wr(maps_df):
#     agent_map_winrate_headers = ["agent", "winrate"]
#     agent_wrs = []
#     for agent in agents_id:
#         maps_df = maps_df.loc[~(
#             ((maps_df['t1_agent1'] == agent) | (maps_df['t1_agent2'] == agent) | (maps_df['t1_agent3'] == agent) | 
#             (maps_df['t1_agent4'] == agent) | (maps_df['t1_agent5'] == agent)) & 
#             ((maps_df['t2_agent1'] == agent) | (maps_df['t2_agent2'] == agent) | (maps_df['t2_agent3'] == agent) | 
#             (maps_df['t2_agent4'] == agent) | (maps_df['t2_agent5'] == agent))
#         )]        
#         t1_games = maps_df.loc[((maps_df['t1_agent1'] == agent) | (maps_df['t1_agent2'] == agent) | (maps_df['t1_agent3'] == agent) | (maps_df['t1_agent4'] == agent) | (maps_df['t1_agent5'] == agent))]
#         t2_games = maps_df.loc[((maps_df['t2_agent1'] == agent) | (maps_df['t2_agent2'] == agent) | (maps_df['t2_agent3'] == agent) | (maps_df['t2_agent4'] == agent) | (maps_df['t2_agent5'] == agent))]
#         # Count wins and total games
#         wins = len(t1_games.loc[(t1_games['winner'] == t1_games['t1'])].index) + len(t2_games.loc[(t2_games['winner'] == t2_games['t1'])].index)
#         total_games = len(t1_games.index) + len(t2_games.index)
#         winrate = wins/total_games if total_games > 0 else 0
#         agent_wrs.extend([[agents[agent], winrate]])
#     agent_wr_df = pd.DataFrame(data=agent_wrs, columns=agent_map_winrate_headers)
#     agent_wr_df.to_csv('data/agent_wrs_nomap.csv', index=False)

# def get_agent_wr_from_df(agent, map, agent_wrs):
#     wr = agent_wrs.iloc[[int(agent)]][map].sum()
#     return wr

# def get_comp_wrs_df(maps_df):
#     def get_comp_wrs_from_row(row):
#         row[67] = (get_agent_wr_by_map(maps_df, row[5], row[4], row[66]) 
#                             + get_agent_wr_by_map(maps_df, row[6], row[4], row[66]) 
#                             + get_agent_wr_by_map(maps_df, row[7], row[4], row[66])
#                             + get_agent_wr_by_map(maps_df, row[8], row[4], row[66])
#                             + get_agent_wr_by_map(maps_df, row[9], row[4], row[66]))/5
#         row[68] = (get_agent_wr_by_map(maps_df, row[10], row[4], row[66]) 
#                             + get_agent_wr_by_map(maps_df, row[11], row[4], row[66]) 
#                             + get_agent_wr_by_map(maps_df, row[12], row[4], row[66])
#                             + get_agent_wr_by_map(maps_df, row[13], row[4], row[66])
#                             + get_agent_wr_by_map(maps_df, row[14], row[4], row[66]))/5
#         return row
#     maps_df['t1_comp_wr'] = pd.Series(dtype='float')
#     maps_df['t2_comp_wr'] = pd.Series(dtype='float')
#     maps_df = maps_df.apply(get_comp_wrs_from_row, raw=True, axis=1)
#     return maps_df

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

def get_team_map_stats(team, map, maps_df, date=datetime.today().strftime('%Y-%m-%d'), count=5):
    # Filter by team
    maps_df = maps_df.loc[((maps_df['t1'] == team) | (maps_df['t2'] == team))]

    maps_df = maps_df.loc[(maps_df['date'] < date)]
    if len(maps_df.index) == 0:
        return [0,] * 19
    
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
    retake_wr = maps_df['t1_retakes_won'].sum() / (maps_df['t1_retakes_lost'].sum() + maps_df['t1_retakes_won'].sum()) if (maps_df['t1_retakes_lost'].sum() + maps_df['t1_retakes_won'].sum()) > 0 else 0
    postplant_wr = maps_df['t1_postplants_won'].sum() / (maps_df['t1_postplants_won'].sum() + maps_df['t1_postplants_lost'].sum()) if (maps_df['t1_postplants_won'].sum() + maps_df['t1_postplants_lost'].sum()) > 0 else 0
    fk_percent = (maps_df['t1_atk_fks'].sum() + maps_df['t1_def_fks'].sum()) / (maps_df['t1_atk_fks'].sum() + maps_df['t1_def_fks'].sum() + maps_df['t2_atk_fks'].sum() + maps_df['t2_def_fks'].sum()) 
    pistol_wr = maps_df['t1_pistols'].sum() / (maps_df['t1_pistols'].sum() + maps_df['t2_pistols'].sum()) 
    eco_wr = maps_df['t1_ecos_won'].sum() / (maps_df['t1_ecos_won'].sum() + maps_df['t1_ecos_lost'].sum()) 
    antieco_wr = maps_df['t2_ecos_lost'].sum() / (maps_df['t2_ecos_won'].sum() + maps_df['t2_ecos_lost'].sum())
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
    
    return [round_wr, retake_wr, postplant_wr, fk_percent, pistol_wr, eco_wr, antieco_wr, fullbuy_wr, acs, kills, assists, deaths, kdr, kadr, kast, rating, mks, clutches, econ]

def get_team_map_stats_df(maps_df, count=10):
    
    def get_map_stats_row(row, count=10):
        t1_stats = get_team_map_stats(row[1], row[4], maps_df, row[66], count)
        t2_stats = get_team_map_stats(row[2], row[4], maps_df, row[66], count)
        stats_diff = [row[0], row[1], row[2], row[66], row[4], row[3]]
        stats_diff.extend(map(operator.sub, t1_stats, t2_stats))
        stats_diff_list.append(stats_diff)

    columns = ['map_id', 't1', 't2', 'date', 'map', 'winner', 'round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 
                'pistol_wr_diff', 'eco_wr_diff', 'antieco_wr_diff', 'fullbuy_wr_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff', 
                'kast_diff', 'rating_diff', 'mks_diff', 'clutch_diff', 'econ_diff']

    stats_diff_list = []
    maps_df.apply(get_map_stats_row, raw=True, axis=1)
    return pd.DataFrame(data=stats_diff_list, columns=columns)

def get_series_map_stats_df(df, maps_df):
    def get_map_stats_row(row, count=10):
        t1_stats = get_team_map_stats(row[1], row[4], maps_df, row[3], count)
        t2_stats = get_team_map_stats(row[2], row[4], maps_df, row[3], count)
        stats_diff = row[0:19].tolist()
        stats_diff.extend(map(operator.sub, t1_stats, t2_stats))
        stats_diff_list.append(stats_diff)

    columns = ['match_id', 't1', 't2', 'date', 'map', 'winner', 'played', 'net_h2h', 'past_diff', 'best_odds', 'worst_odds',
               't1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%', 
               'round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 'pistol_wr_diff', 'eco_wr_diff', 'antieco_wr_diff', 'fullbuy_wr_diff', 
               'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff', 'kast_diff', 'rating_diff', 'mks_diff', 'clutch_diff', 'econ_diff']

    stats_diff_list = []
    df = df.apply(get_map_stats_row, raw=True, axis=1)
    return pd.DataFrame(data=stats_diff_list, columns=columns)

def normalize_training_data(df, vetos=False):
    numeric = ['round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 'pistol_wr_diff', 
                'eco_wr_diff', 'antieco_wr_diff', 'fullbuy_wr_diff', 'acs_diff', 'kills_diff', 
                'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff', 'kast_diff', 'rating_diff', 'mks_diff', 'clutch_diff', 'econ_diff']
    if vetos:
        numeric.extend(['net_h2h', 'past_diff'])
    for col in numeric:
        df[col] = df[col] / df[col].abs().max() 
    return df

def format_map_data(maps_df, vetos_df=None):
    if vetos_df is not None:
        tmd = get_series_map_stats_df(vetos_df, maps_df)
        return normalize_training_data(tmd, True).copy(deep=True)
    else:
        tmd = get_team_map_stats_df(maps_df)
        return normalize_training_data(tmd).copy(deep=True)