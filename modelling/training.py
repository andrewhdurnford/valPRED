import pandas as pd
from IPython.display import display
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split, GridSearchCV, RandomizedSearchCV
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from maps import get_maps

maps = get_maps('data/game_data/maps.txt')

def train_map_pick_model(vetos):
    df = vetos.copy(deep=True)
    # Original: 't1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%' 0.53
    # 't1_win%', 't1_pick%', 't1_ban%', 't2_win%', 't2_pick%', 't2_ban%', 'avg_play%' 0.55

    # features = ['t1_win%', 't1_pick%', 't1_ban%', 't1_play%', 't2_win%', 't2_pick%', 't2_ban%', 't2_play%']
    features = ['avg_play%', 'avg_pick%', 'avg_ban%', 'avg_win%']

    for col in features:
        df[col] = df[col] / df[col].abs().max() 

    X = df[features]
    Y = df['played']
    X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

    # GBM final bankroll: $4500-5000
    gbm_model = GradientBoostingClassifier(random_state=42)

    # Define the parameter grid
    param_grid = {
        'n_estimators': [100, 200, 300],
        'learning_rate': [0.01, 0.1, 0.2],
        'max_depth': [3, 4, 5],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4],
        'subsample': [0.8, 0.9, 1.0],
        'max_features': ['sqrt', 'log2']
    }

    # Set up RandomizedSearchCV
    random_search = RandomizedSearchCV(
        estimator=gbm_model,
        param_distributions=param_grid,
        n_iter=5000,  # Number of parameter settings that are sampled
        scoring='accuracy',
        cv=5,
        verbose=1,
        random_state=42,
        n_jobs=-1  # Use all available cores
    )

    grid_search = GridSearchCV(
        estimator=gbm_model,
        param_grid=param_grid,
        scoring='accuracy',
        cv=5,
        verbose=1,
        n_jobs=-1  # Use all available cores
    )

    # Fit the model
    grid_search.fit(X_train, y_train)

    # Evaluate the best model
    best_gbm_model = grid_search.best_estimator_
    y_pred = best_gbm_model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f'Accuracy of Map Pick model: {accuracy:.2f}')

    return best_gbm_model

def train_map_model(tmd, map):
    temp_tmd = tmd.copy()
    if map >= 0:
        temp_tmd = temp_tmd.loc[temp_tmd['map'] == map]
    if len(temp_tmd.index) == 0:
        return None

    # 'round_wr_diff', 'retake_wr_diff', 'postplant_wr_diff', 'fk_percent_diff', 
    #             'pistol_wr_diff', 'eco_wr_diff', 'antieco_wr_diff', 'fullbuy_wr_diff', 
    #             'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff', 'kadr_diff', 
    #             'kast_diff', 'rating_diff', 'mks_diff', 'clutch_diff', 'econ_diff'

    features = ['round_wr_diff', 'fk_percent_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff']

    X = temp_tmd[features]
    Y = temp_tmd['winner'].astype(int)
    X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

    # GBM final bankroll: $4500-5000
    gbm_model = GradientBoostingClassifier(random_state=42)

    # Define the parameter grid
    param_grid = {
        'n_estimators': [100, 200, 300],
        'learning_rate': [0.01, 0.1, 0.2],
        'max_depth': [3, 4, 5],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4],
        'subsample': [0.8, 0.9, 1.0],
        'max_features': ['sqrt', 'log2']
    }

    # Set up RandomizedSearchCV
    random_search = RandomizedSearchCV(
        estimator=gbm_model,
        param_distributions=param_grid,
        n_iter=5000,  # Number of parameter settings that are sampled
        scoring='accuracy',
        cv=5,
        verbose=1,
        random_state=42,
        n_jobs=-1  # Use all available cores
    )

    # Fit the model
    random_search.fit(X_train, y_train)

    # Evaluate the best model
    best_gbm_model = random_search.best_estimator_
    y_pred = best_gbm_model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)
    print(f'Accuracy of map model: {accuracy:.2f}')
    print(report)
    return best_gbm_model

def train_series_winner_model(sds):
    features = ['net_h2h', 'past_diff', 'winshare']
    X = sds[features]
    Y = sds['winner']

    X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
    
    gbm_model = GradientBoostingClassifier(random_state=42)

    # Define the parameter grid
    param_grid = {
        'n_estimators': [100, 200, 300],
        'learning_rate': [0.01, 0.1, 0.2],
        'max_depth': [3, 4, 5],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4],
        'subsample': [0.8, 0.9, 1.0],
        'max_features': ['sqrt', 'log2']
    }

    # Set up RandomizedSearchCV
    random_search = RandomizedSearchCV(
        estimator=gbm_model,
        param_distributions=param_grid,
        n_iter=1000,  # Number of parameter settings that are sampled
        scoring='accuracy',
        cv=5,
        verbose=1,
        random_state=42,
        n_jobs=-1  # Use all available cores
    )

    # Fit the model
    random_search.fit(X_train, y_train)

    # Evaluate the best model
    best_gbm_model = random_search.best_estimator_
    y_pred = best_gbm_model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f'Accuracy of the Series Winner model: {accuracy:.2f}')
    return best_gbm_model

