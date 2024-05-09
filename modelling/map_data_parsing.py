import pandas as pd, math, ast, json
from IPython.display import display

# Load data
maps_df = pd.read_csv("data/maps.csv")
maps_sample = pd.read_csv("data/maps_sample.csv")
series_df = pd.read_csv("data/series.csv", index_col=False)
# teams = pd.read_csv("data/teams.csv", header=None)[0].to_list()

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

