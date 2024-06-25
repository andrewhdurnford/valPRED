from joblib import dump, load
import pandas as pd, os
from IPython.display import display

from maps import format_map_data, format_veto_data
from newmaps import format_map_data_nd, format_veto_data_nd, transform_series_stats_nd
from series import between_dates, explode_map_choices, transform_series_stats, get_tier1, get_map_in_pool, get_international, get_regional, remove_cn
from training import train_map_pick_model, train_series_winner_model, train_all_maps_model, train_map_model
from testing import simulate_bets, test_series_winner_model, predict_series_outcomes

# Load data
maps = pd.read_csv('data/raw/tier1_maps.csv', index_col=False)
series = pd.read_csv('data/raw/tier1_series.csv', index_col=False)
# maps.to_csv('data/tier1/maps.csv', index=False)
# series.to_csv('data/tier1/series.csv', index=False)
tier1 = pd.read_csv('data/tier1/teams.csv').iloc[:,0].tolist()
vct_2023_start = '2023-02-13'
vct_2024_start = '2024-02-16'

# Process data (irrespective of training timeframe)
def init():
    mapdata = format_map_data_nd(maps)
    vetos = explode_map_choices(series)
    vetos.to_csv('data/tier1/processed/vetos.csv', index=False)
    mapdata.to_csv('data/tier1/processed/mapdata_nd.csv', index=False)
    # return vetos, mapdata

# Train based on timeframe
def train(tsd, ted, ed, vetos, mapdata):
    vetos = between_dates(pd.read_csv('data/tier1/processed/vetos.csv', index_col=False), '2020-01-01', ed)
    mapdata = between_dates(pd.read_csv('data/tier1/processed/mapdata_nd.csv', index_col=False), '2020-01-01', ed)
    models = {}

    map_pick_model = train_map_pick_model(between_dates(vetos, tsd, ted))
    model = train_map_model(between_dates(mapdata, tsd, ted), -1)

    # map_pick_model = load('models/map_pick.joblib')
    # model = load('models/map_win.joblib')

    transformed_series_data = transform_series_stats_nd(format_veto_data_nd(vetos, maps), model, map_pick_model)

    tsd_bd = between_dates(transformed_series_data, tsd, ted)
    series_winner_model = train_series_winner_model(tsd_bd)
    
    tsd_ad = get_regional(remove_cn(between_dates(transformed_series_data, ted, ed)))
    series_predictions = predict_series_outcomes(tsd_ad, series_winner_model)
    df = simulate_bets(series_predictions, 1000)
    # test_series_winner_model(series_predictions)



    user_input = input("Press Enter to save 'model', or type anything else to skip: ")
    if user_input == "":
        dump(map_pick_model, 'models/map_pick_model.joblib')
        dump(model, 'models/map_win.joblib')
        dump(series_winner_model, 'models/series_winner.joblib')

# def test():
#     tsd_ad = get_regional(remove_cn(between_dates(tsd, ted, ed)))
#     series_predictions = predict_series_outcomes(tsd_ad, series_winner_model)
#     df = simulate_bets(series_predictions, 1000)
#     test_series_winner_model(series_predictions)
#     df.to_csv('data/tier1/results/results.csv', index=False)

train(vct_2023_start, vct_2024_start, '2024-05-23', None, None)