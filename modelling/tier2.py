from joblib import dump, load
import pandas as pd, operator
from IPython.display import display
from datetime import datetime
from maps import rename_team_cols, between_dates
from training import train_map_model
from predict import get_team_fullname

maps = pd.read_csv('data/raw/maps.csv', index_col=False)
series = pd.read_csv('data/raw/series.csv', index_col=False)
# model = load('models/map_win.joblib')

def get_team_map_stats(team, maps_df, date=datetime.today().strftime('%Y-%m-%d'), count=20):
    # Filter by team
    maps_df = maps_df.loc[((maps_df['t1'] == team) | (maps_df['t2'] == team))]

    maps_df = maps_df.loc[(maps_df['date'] < date)]
    if len(maps_df.index) == 0:
        return [0,] * 7

    # Check count flag and if team has played enough games
    maps_df = maps_df.sort_values(by='date', ascending=False)
    if count > 0 and len(maps_df.index) > count:
        maps_df = maps_df.head(count)
    
    maps_df = rename_team_cols(maps_df, team)
    maps_df = maps_df.fillna(0)
    if len(maps_df.index) == 0 or maps_df['t1_fks'].sum() + maps_df['t2_fks'].sum() == 0:
        return [0,] * 7
    
    round_wr = maps_df['t1_rds'].sum() / (maps_df['t2_rds'].sum() + maps_df['t1_rds'].sum())
    fk_percent = maps_df['t1_fks'].sum() / (maps_df['t1_fks'].sum() + maps_df['t2_fks'].sum()) 
    acs = maps_df['t1_acs'].sum() / len(maps_df.index)
    kills = maps_df['t1_kills'].sum() / (maps_df['t1_rds'].sum() + maps_df['t2_rds'].sum())
    assists = maps_df['t1_assists'].sum() / (maps_df['t1_rds'].sum() + maps_df['t2_rds'].sum())
    deaths = maps_df['t1_deaths'].sum() / (maps_df['t1_rds'].sum() + maps_df['t2_rds'].sum())
    kdr = maps_df['t1_kills'].sum() / maps_df['t1_deaths'].sum()
    
    return [round_wr, fk_percent, acs, kills, assists, deaths, kdr]

def get_series_map_stats_df(df, maps_df): 
    def get_map_stats_row(row, count=20):
        t1_stats = get_team_map_stats(row['t1'], maps_df, row['date'], count) 
        t2_stats = get_team_map_stats(row['t2'], maps_df, row['date'], count)
        stats_diff = row.tolist()[0:7]
        stats_diff.extend(map(operator.sub, t1_stats, t2_stats))
        stats_diff_list.append(stats_diff)

    columns = ['match_id', 't1', 't2', 'date', 'winner', 'past_diff', 'odds',
               'round_wr_diff', 'fk_percent_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff']

    stats_diff_list = []
    df = df.apply(get_map_stats_row, axis=1)
    return pd.DataFrame(data=stats_diff_list, columns=columns)

def process_series(series, maps):
    def get_mapwin_row(row):
        map_features = ['round_wr_diff', 'fk_percent_diff', 'acs_diff', 'kills_diff', 'assists_diff', 'deaths_diff', 'kdr_diff']
        df = pd.DataFrame(data=[[row['round_wr_diff'], row['fk_percent_diff'], row['acs_diff'], row['kills_diff'], row['assists_diff'], row['deaths_diff'], row['kdr_diff']]], columns=map_features)

        row['win%'] = model.predict_proba(df)[0][0] if model is not None else None
        return row
    
    series['past_diff'] = series['t1_past'] - series['t2_past']
    series = series[['match_id', 't1', 't2', 'date', 'winner', 'past_diff', 'odds']]
    series = get_series_map_stats_df(series, maps)

    model = train_map_model(between_dates(series, '2023-01-01', '2024-01-01'), -1)
    dump(model, filename='models/tier2.joblib')
    # model = load('models/tier2.joblib')

    series['win%'] = 0
    series = series.apply(get_mapwin_row, axis=1).sort_values(by='date', ascending=False)
    simulate_bets(between_dates(series, '2024-01-01', '2025-01-01'), 1000)
    series.to_csv('data/t2.csv', index=False)
    return series

def test_series_winner_model(sp):
    df = sp.copy(deep=True)
    correct = len(df.loc[(((df['win%'] > 0.5) & (df['winner'] == True)) | ((df['win%'] < 0.5) & (df['winner'] == False)))].index)
    return correct / len(df.index)

def simulate_bets(predictions, bankroll):
    bets = 0
    won = 0
    lost = 0
    betsize = 50
    start = bankroll
    dog = 0
    predictions = predictions.sort_values(by='date', ascending=True)
    cols = ['match_id', 't1', 't2', 'correct', 'date', 'bankroll', 'betsize', '$win', '$lose', 'best', 'worst']
    data = []
    for _, row in predictions.iterrows():
        # betsize = bankroll * 0.1
        if row['win%'] > row['odds'] and row['odds'] > 0.1: 
            bets += 1
            if row['odds'] < 0.5:
                dog += 1

            if row['winner']: 
                bankroll += (betsize * 1/row['odds']) - betsize
                won += 1
                data.append([row['match_id'], row['t1'], row['t2'], True, row['date'], bankroll, betsize, (betsize * 1/row['odds']) - betsize, betsize, row['odds'], row['odds']])

            else:
                bankroll -= betsize
                lost += 1
                data.append([row['match_id'], row['t1'], row['t2'], False, row['date'], bankroll, betsize, (betsize * 1/row['odds']) - betsize, betsize, row['odds'], row['odds']])

        elif row['win%'] < row['odds'] and row['odds'] < 0.9:
            bets += 1
            if row['odds'] > 0.5:
                dog += 1

            if row['winner']:
                bankroll -= betsize
                lost += 1
                data.append([row['match_id'], row['t1'], row['t2'], False, row['date'], bankroll, betsize, (betsize * 1/(1 - row['odds'])) - betsize, betsize, row['odds'], row['odds']])

            else:
                bankroll += (betsize * 1/(1 - row['odds'])) - betsize
                won += 1
                data.append([row['match_id'], row['t1'], row['t2'], True, row['date'], bankroll, betsize, (betsize * 1/(1 - row['odds'])) - betsize, betsize, row['odds'], row['odds']])

    accuracy = round(won / (won + lost) * 100, 2) if (won + lost) > 0 else 0
    expected_value = round((bankroll - start) / bets / betsize, 2)
    dog = round(dog / bets * 100, 2)
    print("Bets placed: " + str(bets) + " Ending bankroll: $" + str(round(bankroll, 2)) + " Accuracy: " + str(accuracy) + "%" + " EV: " + str(expected_value) + " Dog: " + str(dog) +"%")
    # df = pd.DataFrame(data=data, columns=cols)
    # return df

ser = process_series(series, maps)