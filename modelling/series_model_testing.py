from functools import partial
import random
from joblib import load
import numpy as np
from series_data_parsing import get_team_data, get_map_pool
import pandas as pd
from IPython.display import display

# Map, Agents data
maps = ["Ascent", "Bind", "Breeze", "Fracture", "Haven", "Icebox", "Lotus", "Pearl", "Split", "Sunset"]
maps_id = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
out_of_pool = {"2023-01-10": [6, 8, 9], "2023-04-25": [1, 2, 9], "2023-08-24": [2, 5, 9], "06-09-23": [3, 5, 7, 9], "2024-01-09": [3, 5, 7], "2024-11-12": [3, 4, 7]}
agents = ["Astra", "Breach", "Brimstone", "Chamber", "Clove", "Cypher", "Deadlock", "Fade", "Gekko", "Harbor", "Iso", 
          "Jett", "Kayo", "Killjoy", "Neon", "Omen", "Phoenix", "Raze", "Reyna", "Sage", "Skye", "Sova", "Viper", "Yoru"]
agents_id = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]

# Load models
pick_models={}
ban_models={}
for m in maps_id:
    pick_models[m] = load(f'models/current/0/{maps[m].lower()}_map_pick.joblib')
    ban_models[m] = load(f'models/current/0/{maps[m].lower()}_map_ban.joblib')
def rename_columns(t, old_name):
    if isinstance(old_name, str):
        map_number = old_name.split('_')[0]
        if 'pickrate' in old_name or 'banrate' in old_name or 'playrate' in old_name:
            return f"{map_number}_{t}_{old_name.split('_')[1]}"
        else:
            return f"{map_number}_{t}_winrate" 
    else:
        return f"{old_name}_{t}_winrate"

rename_t1 = partial(rename_columns, "t1")
rename_t2 = partial(rename_columns, "t2")

class BinaryTreeNode:
    def __init__(self, map_id, decision, played, probability, depth, parent=None):
        self.map_id = map_id
        self.decision = decision
        self.played = played
        self.probability = probability
        self.depth = depth
        self.parent = parent
        self.left = None
        self.right = None

class Node: 
    def __init__(self, map_id, decision, played, probability, depth, parent=None):
        self.map_id = map_id
        self.decision = decision
        self.played = played
        self.probability = probability
        self.depth = depth
        self.parent = parent
        self.children = []

def get_action(turn, action):
    if turn and action:
        return True
    elif turn and not action:
        return False
    elif not turn and action:
        return False
    elif not turn and not action:
        return True
    
def pool_size(map_pool):
    s = 0
    maps = []
    for map in map_pool:
        if map == 1:
            maps.append(map_pool.index(map))
            s += 1
    return [s, maps[0]]

def maps_in_pool(map_pool):
    maps = []
    for map in map_pool:
        if map == 1:
            maps.append(map_pool.index(map))
    return maps

def calculate_BT_play_likelihoods(node, likelihoods=None, path_probability=1):
    if likelihoods is None:
        likelihoods = {}

    # When reaching a leaf node which determines a map remains, add its probability
    if node.played:
        if node.map_id in likelihoods:
            likelihoods[node.map_id] += path_probability * node.probability
        else:
            likelihoods[node.map_id] = path_probability * node.probability

    # Recursively call the function for the left child if it exists
    if node.left:
        calculate_BT_play_likelihoods(node.left, likelihoods, path_probability * node.left.probability)

    # Recursively call the function for the right child if it exists
    if node.right:
        calculate_BT_play_likelihoods(node.right, likelihoods, path_probability * node.right.probability)

    return likelihoods

def calculate_T_play_likelihoods(node, likelihoods=None, path_probability=1):
    if likelihoods is None:
        likelihoods = {}

    if node.played:
        if node.map_id in likelihoods:
            likelihoods[node.map_id] += path_probability * node.probability
        else:
            likelihoods[node.map_id] = path_probability * node.probability
    
    if len(node.children) > 0:
        for child in node.children:
            calculate_T_play_likelihoods(child, likelihoods, path_probability * child.probability)
    
    return likelihoods

def predict_map_selection(team1, team2, binary, count, date):
    def simulate_binary_tree(map, turn, action, maps_pool, maps_choice_pool, depth=0, parent=None):
        
        choice = "picks" if action else "bans"
        team_num = 1 if turn else 2

        if pool_size(maps_pool)[0] == 1:
            node = BinaryTreeNode(map, "map_remains", True, 1, depth, parent)
            if parent.left is not None:
                parent.right = node
            else: 
                parent.left = node
            return
        elif pool_size(maps_choice_pool)[0] == 1:
            node = BinaryTreeNode(map, f"team{team_num}_{choice}", action, 1, depth, parent)
            if parent.left is not None:
                parent.right = node
            else: 
                parent.left = node
            return
        
        # Calculate the probability of the current map being picked/banned
        tdf = t1_data if turn == True else t2_data
        tdf['action'] = action
        map_features = maps_id.copy()
        map_features.remove(map)
        for i in range(10):
            tdf[f'{i}_in_pool'] = maps_choice_pool[i]
        features = [f"{m}_in_pool" for m in map_features] + [f"{map}_t1_pickrate", f"{map}_t1_banrate", f"{map}_t1_playrate", f"{map}_t1_winrate",
            f"{map}_t2_pickrate", f"{map}_t2_banrate", f"{map}_t2_playrate", f"{map}_t2_winrate"]
        
        if action:
            action_prob = pick_models[map].predict_proba(tdf.iloc[[0]][features])[0][1]
        elif not action:
            action_prob = ban_models[map].predict_proba(tdf.iloc[[0]][features])[0][1]
        # Node for the map being chosen
        choice_node = BinaryTreeNode(map, f"team{team_num}_{choice}", action, action_prob, depth, parent)
        parent.left = choice_node

        # Node for the map not being chosen
        not_choice_node = BinaryTreeNode(map, f"team{team_num}_not_{choice}", action, (1 - action_prob), depth, parent)
        parent.right = not_choice_node

        # Simulate the scenario where the map is chosen (remove from pool)
        new_map_pool = maps_pool.copy()
        new_map_pool[map] = 0
        simulate_binary_tree(random.choice(maps_in_pool(new_map_pool)), (not turn), get_action(turn, action), new_map_pool, new_map_pool, depth + 1, choice_node)

        # Simulate the scenario where the map is not chosen (keep in pool)
        new_choice_pool = maps_choice_pool.copy()
        new_choice_pool[map] = 0
        simulate_binary_tree(random.choice(maps_in_pool(new_choice_pool)), turn, action, maps_pool, new_choice_pool, depth + 1, not_choice_node)
    
    # FIXME: Ascent being favoured way too heavily compared to the rest of the maps
    def simulate_tree(turn, action, maps_pool, depth=0, parent=None):
        choice = "picks" if action else "bans"
        team_num = 1 if turn else 2

        if pool_size(maps_pool)[0] == 1:
            node = Node(maps_pool[1], "map_remains", True, 1, depth, parent)
            parent.children.append(node)
            return
        
        # Calculate the probability of the current map being picked/banned
        tdf = t1_data if turn == True else t2_data
        tdf['action'] = action
        for i in range(10):
            tdf[f'{i}_in_pool'] = maps_pool[i]
        model = pick_models if action else ban_models

        map_probabilities = {}
        total_prob = 0

        for i in range(10):
            if maps_pool[i] == 0:
                continue
            map_features = maps_id.copy()
            map_features.remove(i)
            features = [f"{m}_in_pool" for m in map_features] + [f"{i}_t1_pickrate", f"{i}_t1_banrate", f"{i}_t1_playrate", f"{i}_t1_winrate",
                f"{i}_t2_pickrate", f"{i}_t2_banrate", f"{i}_t2_playrate", f"{i}_t2_winrate"]
            action_prob = model[i].predict_proba(tdf.iloc[[0]][features])[0][1]
            map_probabilities[i] = action_prob
            total_prob += action_prob

        # for key in map_probabilities.keys():
        #     map_probabilities[key] = map_probabilities[key] / total_prob
        
        map_probabilities = {key: value / total_prob for key, value in map_probabilities.items()}
        for key in map_probabilities.keys():
            choice_node = Node(key, f"team{team_num}_{choice}", action, map_probabilities[key], depth, parent)
            parent.children.append(choice_node)
            new_map_pool = maps_pool.copy()
            new_map_pool[key] = 0
            simulate_tree((not turn), get_action(turn, action), new_map_pool, depth + 1, choice_node)


    map_pool_hdrs = [f"{map}_in_pool" for map in maps_id]
    map_pool = []
    for m in maps_id:
        map_pool.append(get_map_pool(date, m))
    map_pool_df = pd.DataFrame(data=[map_pool], columns=map_pool_hdrs)
    action_df = pd.DataFrame(data=[[True]], columns=['action'])
    mpa_df = pd.concat([map_pool_df, action_df], axis=1)
    t1_data, t2_data = mpa_df, mpa_df
    t1_data = pd.concat([mpa_df, get_team_data(team1, date, count, "df").rename(columns=rename_t1)], axis=1)
    t1_data = pd.concat([t1_data, get_team_data(team2, date, count, "df").rename(columns=rename_t2)], axis=1)
    t2_data = pd.concat([mpa_df, get_team_data(team2, date, count, "df").rename(columns=rename_t1)], axis=1)
    t2_data = pd.concat([t2_data, get_team_data(team1, date, count, "df").rename(columns=rename_t2)], axis=1)

    for sdf in [t1_data, t2_data]:
        sdf['action'] = np.where(sdf['action'] == "pick", 1, 0)
        for i in range(10):
            sdf[f'{i}_in_pool'] = np.where(sdf[f'{i}_in_pool'] == True, 1, 0)

    results = {}
    if binary:
        for i in range(10):
            if get_map_pool(date, i) == 0:
                continue

            root = BinaryTreeNode(None, 'start', None, 1, 0, False)
            simulate_binary_tree(i, 1, 0, map_pool, map_pool, 0, root)
            play_likelihoods = calculate_BT_play_likelihoods(root)
            for k in play_likelihoods.keys():
                if k in results.keys():
                    results[k] += play_likelihoods[k]
                else:
                    results[k] = play_likelihoods[k]
            print(play_likelihoods)
        return dict(sorted(results.items(), key=lambda item: item[1]))
    else:
        root = Node(None, 'start', None, 1, 0, False)
        simulate_tree(1, 0, map_pool, 0, root)
        print(calculate_T_play_likelihoods(root))

print("prdicting...")

def predict(t1,t2, date):
    v1 = predict_map_selection(t1 , t2, True, 0, date)
    v2 = predict_map_selection(t2, t1, True, 0, date)
    print(dict(sorted(v2.items(), key=lambda item: item[1])))
    res = {}
    for k in v1.keys():
        res[maps[k]] = v1[k] + v2[k]
    factor=1.0/sum(res.values())
    res = {k: v*factor for k, v in res.items() }
    return(dict(sorted(res.items(), key=lambda item: item[1])))

print(predict(1184,1001,"2024-05-04"))