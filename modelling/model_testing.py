import pandas as pd

def simulate_bets(predictions, bankroll):
    bets = 0
    won = 0
    lost = 0
    betsize = 50
    predictions = predictions.sort_values(by='date', ascending=True)
    cols = ['match_id', 't1', 't2', 'correct', 'date', 'bankroll', '$win', '$lose']
    data = []
    for _, row in predictions.iterrows():
        # betsize = bankroll * 0.1
        if row['pred_win%'] > row['worst_odds']: 
            bets += 1

            if row['winner']: 
                bankroll += (betsize * 1/row['worst_odds']) - betsize
                won += 1
                data.append([row['match_id'], row['t1'], row['t2'], True, row['date'], bankroll, (betsize * 1/row['worst_odds']) - betsize, betsize])

            else:
                bankroll -= betsize
                lost += 1
                data.append([row['match_id'], row['t1'], row['t2'], False, row['date'], bankroll, (betsize * 1/row['worst_odds']) - betsize, betsize])

        elif row['pred_win%'] < row['best_odds']:
            bets += 1

            if row['winner']:
                bankroll -= betsize
                lost += 1
                data.append([row['match_id'], row['t1'], row['t2'], False, row['date'], bankroll, (betsize * 1/(1 - row['best_odds'])) - betsize, betsize])

            else:
                bankroll += (betsize * 1/(1 - row['best_odds'])) - betsize
                won += 1
                data.append([row['match_id'], row['t1'], row['t2'], True, row['date'], bankroll, (betsize * 1/(1 - row['best_odds'])) - betsize, betsize])

    accuracy = round(won / (won + lost) * 100, 2) if (won + lost) > 0 else 0
    print("Bets placed: " + str(bets) + " Ending bankroll: $" + str(round(bankroll, 2)) + " Accuracy: " + str(accuracy) + "%")
    df = pd.DataFrame(data=data, columns=cols)
    return df

def test_series_winner_model(sp):
    df = sp.copy(deep=True)
    correct = len(df.loc[(((df['pred_win%'] > 0.5) & (df['winner'] == True)) | ((df['pred_win%'] < 0.5) & (df['winner'] == False)))].index)
    return correct / len(df.index)