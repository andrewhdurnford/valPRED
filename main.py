from joblib import load
import pandas as pd
from IPython.display import display

maps = pd.read_csv("data/maps.csv")
series = pd.read_csv("data/series.csv", index_col=False)


# from modelling.series_data_parsing import explode_map_choices
# exploded_vetos = explode_map_choices(series)
vetos = pd.read_csv('data/exploded_vetos.csv')


from modelling.series_model_training import pred_sds, train_map_pick_model, train_pred_model, transform_data_stats
# train_map_pick_model(vetos)

from modelling.map_data_parsing import format_map_data
# tmd = format_map_data(maps)
# sds = format_map_data(maps, vetos, 5)

tmd = pd.read_csv('data/map_training_data.csv', index_col=False)
sds = pd.read_csv('data/series_data_stats.csv')

from modelling.map_model_training import train_map_model
# for i in range(10):
#     train_map_model(tmd, i)

# train_pred_model(sds)
# sds = pd.read_csv('data/transformed_pred_data.csv')
# pred_sds(sds).to_csv('data/predictions.csv')

preds = pd.read_csv('data/predictions.csv', index_col=False)
bankroll = 1000
preds.info()
for _, row in preds.iterrows():
    if row['pred_win%'] > row['worst_odds']: 
        if row['winner']: 
            bankroll += (10 * 1/row['worst_odds']) - 10
        else:
            bankroll -= 10
    elif row['pred_win%'] < row['best_odds']:
        if row['winner']:
            bankroll -= 10
        else:
            bankroll += (10 * 1/(1 - row['best_odds'])) - 10
    print(bankroll)