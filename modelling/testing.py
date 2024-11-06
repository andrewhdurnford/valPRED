import pandas as pd

def predict_series_outcomes(sds, series_winner_model):
    return_df = sds.copy(deep=True)
    
    def series_win_row(row):
        cols = ['elo_diff', 'past_diff', 'winshare']
        df = pd.DataFrame(data=[[row['elo_diff'], row['past_diff'], row['winshare']]], columns=cols)
        row['pred_win%'] = series_winner_model.predict_proba(df)[0][0]
        return row
    
    return_df['pred_win%'] = 0
    return_df = return_df.apply(series_win_row, axis=1)
    return_df = return_df[['match_id', 't1', 't2', 'date', 'winner', 'pred_win%', 'odds', 'best_odds', 'worst_odds']]
    return return_df.copy(deep=True)

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
        if row['pred_win%'] > row['odds'] and row['odds'] > 0.1: 
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

        elif row['pred_win%'] < row['odds'] and row['odds'] < 0.9:
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

def simulate_bets_best(predictions, bankroll):
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
        if row['pred_win%'] > row['worst_odds'] and row['worst_odds'] > 0.1: 
            bets += 1
            if row['worst_odds'] < 0.5:
                dog += 1

            if row['winner']: 
                bankroll += (betsize * 1/row['worst_odds']) - betsize
                won += 1
                data.append([row['match_id'], row['t1'], row['t2'], True, row['date'], bankroll, betsize, (betsize * 1/row['worst_odds']) - betsize, betsize, row['best_odds'], row['worst_odds']])

            else:
                bankroll -= betsize
                lost += 1
                data.append([row['match_id'], row['t1'], row['t2'], False, row['date'], bankroll, betsize, (betsize * 1/row['worst_odds']) - betsize, betsize, row['best_odds'], row['worst_odds']])

        elif row['pred_win%'] < row['best_odds'] and row['best_odds'] < 0.9:
            bets += 1
            if row['best_odds'] > 0.5:
                dog += 1

            if row['winner']:
                bankroll -= betsize
                lost += 1
                data.append([row['match_id'], row['t1'], row['t2'], False, row['date'], bankroll, betsize, (betsize * 1/(1 - row['best_odds'])) - betsize, betsize, row['best_odds'], row['worst_odds']])

            else:
                bankroll += (betsize * 1/(1 - row['best_odds'])) - betsize
                won += 1
                data.append([row['match_id'], row['t1'], row['t2'], True, row['date'], bankroll, betsize, (betsize * 1/(1 - row['best_odds'])) - betsize, betsize, row['best_odds'], row['worst_odds']])

    accuracy = round(won / (won + lost) * 100, 2) if (won + lost) > 0 else 0
    expected_value = round((bankroll - start) / bets / betsize, 2)
    dog = round(dog / bets * 100, 2)
    print("Bets placed: " + str(bets) + " Ending bankroll: $" + str(round(bankroll, 2)) + " Accuracy: " + str(accuracy) + "%" + " EV: " + str(expected_value) + " Dog: " + str(dog) +"%")
    df = pd.DataFrame(data=data, columns=['match_id', 't1', 't2', 'correct', 'date', 'bankroll', 'betsize', 'earnings', 'bet_size', 'best_odds', 'worst_odds'])
    return df

def test_series_winner_model(sp):
    df = sp.copy(deep=True)
    correct = len(df.loc[(((df['pred_win%'] > 0.5) & (df['winner'] == True)) | ((df['pred_win%'] < 0.5) & (df['winner'] == False)))].index)
    return correct / len(df.index)