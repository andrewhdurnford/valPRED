from joblib import dump, load
import pandas as pd, os
from IPython.display import display

from maps import format_map_data, format_veto_data
from series import between_dates, explode_map_choices, transform_series_stats, get_tier1, get_map_in_pool, get_international, get_regional, remove_cn
from training import train_map_pick_model, train_series_winner_model, train_all_maps_model, train_map_model
from testing import simulate_bets, test_series_winner_model, predict_series_outcomes

# Load data
maps = remove_cn(pd.read_csv('data/raw/maps.csv', index_col=False))
series = remove_cn(get_tier1(pd.read_csv('data/tier1/series.csv', index_col=False)))
# maps.to_csv('data/tier1/maps.csv', index=False)
# series.to_csv('data/tier1/series.csv', index=False)
tier1 = pd.read_csv('data/tier1/teams.csv').iloc[:,0].tolist()
vct_2023_start = '2023-02-13'
vct_2024_start = '2024-02-16'

# Process data (irrespective of training timeframe)
def init():
    vetos = explode_map_choices(series)
    # vetos.to_csv('data/tier1/processed/vetos.csv', index=False)
    mapdata = format_map_data(maps)
    # mapdata.to_csv('data/tier1/processed/mapdata.csv', index=False)
    return vetos, mapdata

# Train based on timeframe
def train(tsd, ted, ed, vetos, mapdata):
    # vetos = pd.read_csv('data/tier1/processed/vetos.csv', index_col=False) 
    # mapdata = pd.read_csv('data/tier1/processed/mapdata.csv', index_col=False)

    models = {}
    map_pick_model = train_map_pick_model(between_dates(vetos, tsd, ted))
    for i in range(10):
        if get_map_in_pool(ted, i):
            models[i] = train_map_model(between_dates(mapdata, tsd, ted), i)
        else:
            models[i] = None

    # map_pick_model = load('models/map_pick.joblib')
    # for i in range(10):
    #     models[i] = load(f'models/maps/{i}.joblib')

    transformed_series_data = get_regional(transform_series_stats(format_veto_data(vetos, maps), models, map_pick_model))
    tsd_bd = between_dates(transformed_series_data, tsd, ted)
    tsd_ad = between_dates(transformed_series_data, ted, ed)

    series_winner_model = train_series_winner_model(tsd_bd)
    series_predictions = predict_series_outcomes(tsd_ad, series_winner_model)
    # dump(map_pick_model, 'models/map_pick.joblib')
    # for i in range(10):
    #     dump(models[i], f'models/maps/{i}.joblib')
    # dump(series_winner_model, 'models/series_winner.joblib')
    df, bets, bankroll, accuracy = simulate_bets(series_predictions, 1000)
    print(test_series_winner_model(series_predictions))
    df.to_csv('data/tier1/results/results.csv', index=False)
    return df, bets, bankroll, accuracy

vetos, mapdata = init()
train(vct_2023_start, vct_2024_start, '2024-06-01', vetos, mapdata)