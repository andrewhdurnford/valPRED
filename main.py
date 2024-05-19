from joblib import load
import pandas as pd
from IPython.display import display

maps = pd.read_csv("data/maps.csv")
series = pd.read_csv("data/series.csv", index_col=False)


from modelling.series_data_parsing import explode_map_choices
from modelling.series_model_training import pred_sds, train_map_pick_model, train_pred_model, transform_data_stats
from modelling.map_model_training import train_map_model
from modelling.map_data_parsing import format_map_data

# exploded_vetos = explode_map_choices(series)
vetos = pd.read_csv('data/exploded_vetos.csv')



# train_map_pick_model(vetos)


# tmd = format_map_data(maps)
# sds = format_map_data(maps, vetos, 5)

tmd = pd.read_csv('data/map_training_data.csv', index_col=False)
sds = pd.read_csv('data/series_data_stats.csv')


# for i in range(10):
#     train_map_model(tmd, i)

# train_pred_model(sds)
# sds = pd.read_csv('data/transformed_pred_data.csv')
# pred_sds(sds).to_csv('data/predictions.csv')

tier1_teams = pd.read_csv('data/tier1/tier1_teams.csv').iloc[:,0].tolist()
preds = pd.read_csv('data/predictions.csv', index_col=False).sort_values(by='date')
since_lotus = preds.loc[(preds['date'] > '2024-01-09')]
t1_preds = preds.loc[(preds['t1'].isin(tier1_teams) | preds['t2'].isin(tier1_teams))]
t1_since_lotus = t1_preds.loc[(t1_preds['date'] > '2024-01-09')]

bankroll = 1000

for _, row in preds.iterrows():
    if row['pred_win%'] > row['worst_odds']: 
        if row['winner']: 
            bankroll += (50 * 1/row['worst_odds']) - 50
        else:
            bankroll -= 50
    elif row['pred_win%'] < row['best_odds']:
        if row['winner']:
            bankroll -= 50
        else:
            bankroll += (50 * 1/(1 - row['best_odds'])) - 50
    print(bankroll)