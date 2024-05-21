from joblib import dump, load
import pandas as pd, os
from IPython.display import display

from series_data_parsing import between_dates, explode_map_choices, transform_series_stats, get_tier1
from series_model_training import predict_series_outcomes, train_map_pick_model, train_series_winner_model
from map_data_parsing import format_map_data
from map_model_training import train_all_maps_model, train_map_model
from model_testing import simulate_bets, test_series_winner_model

# Load data
maps = pd.read_csv("data/tier1/maps.csv", index_col=False)
series = pd.read_csv("data/tier1/series.csv", index_col=False)

def init():
    exploded_vetos = explode_map_choices(series)
    exploded_vetos.to_csv('data/tier1/exploded_vetos.csv', index=False)
    map_training_data = format_map_data(maps)
    map_training_data.to_csv('data/tier1/map_training_data.csv', index=False)
    return exploded_vetos, map_training_data

def train(sd, ed):
    ev = pd.read_csv('data/tier1/exploded_vetos.csv', index_col=False)
    mpd = pd.read_csv('data/tier1/map_training_data.csv', index_col=False)
    map_pick_model = train_map_pick_model(between_dates(ev, "2023-01-01", ed))
    models = {}
    for i in range(10):
        models[i] = train_map_model(between_dates(mpd, sd, ed), i)

    transformed_series_data = transform_series_stats(format_map_data(maps, ev), models, map_pick_model)
    tsd_bd = between_dates(transformed_series_data, sd, ed)
    tsd_ad = transformed_series_data.loc[((transformed_series_data['date'] > ed))]

    series_winner_model = train_series_winner_model(tsd_bd)
    series_predictions = predict_series_outcomes(tsd_ad, series_winner_model)
    series_predictions.to_csv('data/tier1/preds.csv')
    results = simulate_bets(series_predictions, 1000)
    results.to_csv('data/tier1/results.csv', index=False)

# dump(map_pick_model, '')
# for i in range(10):
#     dump(models[i], f'models/current/maps/{i}.joblib')
# dump(series_winner_model, 'models/current/series_winner.joblib')

train("2020-01-01", "2024-02-18")