import pandas as pd, ast, operator, numpy as np, os, math
from IPython.display import display
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split
from joblib import dump, load
from functools import partial
from series_data_parsing import get_team_data, get_map_pool

# Load data
maps_df = pd.read_csv("data/maps.csv")
series_df = pd.read_csv("data/series.csv", index_col=False)
# tier1_series_df = pd.read_csv("data/tier1_series.csv", index_col=False)
# teams = pd.read_csv("data/teams.csv", header=None)[0].to_list()

# Map, Agents data
maps = ["Ascent", "Bind", "Breeze", "Fracture", "Haven", "Icebox", "Lotus", "Pearl", "Split", "Sunset"]
maps_id = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
out_of_pool = {"2023-01-10": [6, 8, 9], "2023-04-25": [1, 2, 9], "2023-08-24": [2, 5, 9], "06-09-23": [3, 5, 7, 9], "2024-01-09": [3, 5, 7], "2024-11-12": [3, 4, 7]}
agents = ["Astra", "Breach", "Brimstone", "Chamber", "Clove", "Cypher", "Deadlock", "Fade", "Gekko", "Harbor", "Iso", 
          "Jett", "Kayo", "Killjoy", "Neon", "Omen", "Phoenix", "Raze", "Reyna", "Sage", "Skye", "Sova", "Viper", "Yoru"]
agents_id = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]

# Train model to predict if a team will pick or ban individual map
def train_map_selection_model(map, pb, count=0):
    # Load data
    maps_set = set(maps_id.copy())
    maps_set.remove(map)
    vdf = pd.read_csv(f"data/tier1/tier1_tvd.csv")
    vdf = vdf.loc[((vdf[f"{map}_in_pool"] == True))]
    vdf['map'] = np.where(vdf['map'] == map, 1, 0)
    vdf['action'] = np.where(vdf['action'] == "pick", 1, 0)
    for i in range(10):
        vdf[f'{i}_in_pool'] = np.where(vdf[f'{i}_in_pool'] == True, 1, 0)
    vdf = vdf.loc[vdf['action'] == pb]
    display(vdf.head(20))

    features = [f"{map}_t1_pickrate", f"{map}_t1_banrate", f"{map}_t1_playrate", f"{map}_t1_winrate",
                f"{map}_t2_pickrate", f"{map}_t2_banrate", f"{map}_t2_playrate", f"{map}_t2_winrate"]
    
    X = vdf[features]
    Y = vdf["map"]

    # Train model
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
    model = LogisticRegression()
    model.fit(X_train, Y_train)
    predictions = model.predict(X_test)

    directory_path = f"models/current/{count}"
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    file_path = os.path.join(directory_path, f"{maps[map].lower()}_map_{pb}.joblib")
    # dump(model, file_path)

    # Evaluate model
    accuracy = accuracy_score(Y_test, predictions)
    report = classification_report(Y_test, predictions, output_dict=False)
    # precision = report["weighted avg"]["precision"]
    # f1_score = report["weighted avg"]["f1-score"]
    # file_path = os.path.join(directory_path, f"model_metrics.txt")
    print(report)
    print(accuracy)
    # with open(file_path, 'a') as file:
    #     file.write(f"{maps[map]}_map_{pb} overview\n")
    #     file.write("Accuracy: {:.4f}\n".format(accuracy))
    #     file.write("Precision: {:.4f}\n".format(precision))
    #     file.write("F1 Score: {:.4f}\n".format(f1_score))
    #     file.write("\n")
    
    # print(f"Metrics saved to {file_path}")

def train_all_individual_map_selection_models(count):
    for map in maps_id:
        print(f"Training model")
        train_map_selection_model(map, "pick", count)
        train_map_selection_model(map, "ban", count)

train_map_selection_model(0, True, 0)