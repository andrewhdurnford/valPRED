import pandas as pd
from IPython.display import display

df = pd.read_csv('data/raw/tier1_series.csv')
df.sort_values(by='date', ascending=True, inplace=True)

# Initialize Elo ratings
elo_ratings = {}
default_elo = 1500
k_factor = 32

# Function to get current Elo rating or initialize it
def get_elo(team):
    if team not in elo_ratings:
        elo_ratings[team] = default_elo
    return elo_ratings[team]

# Function to calculate the expected probability
def expected_prob(rating1, rating2):
    return 1 / (1 + 10 ** ((rating2 - rating1) / 400))

# Function to update Elo ratings
def update_elo(winner_elo, loser_elo):
    winner_prob = expected_prob(winner_elo, loser_elo)
    loser_prob = expected_prob(loser_elo, winner_elo)
    
    new_winner_elo = winner_elo + k_factor * (1 - winner_prob)
    new_loser_elo = loser_elo + k_factor * (0 - loser_prob)
    
    return new_winner_elo, new_loser_elo

# Add Elo columns to the dataframe
df["t1_elo"] = 0
df["t2_elo"] = 0

# Calculate Elo ratings for each match
for index, row in df.iterrows():
    t1 = row["t1"]
    t2 = row["t2"]
    winner = row["winner"]
    
    t1_elo = get_elo(t1)
    t2_elo = get_elo(t2)
    
    df.at[index, "t1_elo"] = t1_elo
    df.at[index, "t2_elo"] = t2_elo
    
    if winner:
        new_t1_elo, new_t2_elo = update_elo(t1_elo, t2_elo)
    else:
        new_t2_elo, new_t1_elo = update_elo(t2_elo, t1_elo)
    
    elo_ratings[t1] = new_t1_elo
    elo_ratings[t2] = new_t2_elo

df['elo_diff'] = df['t1_elo'] - df['t2_elo']
display(df.tail(50))
df.to_csv('data/tier1/series.csv')