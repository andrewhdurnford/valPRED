import pandas as pd
from IPython.display import display
res = pd.read_csv('data/tier1/results/results.csv')
amer = pd.read_csv("data/tier1/teams/amer.csv").iloc[:,0].tolist()
emea = pd.read_csv("data/tier1/teams/emea.csv").iloc[:,0].tolist()
apac = pd.read_csv("data/tier1/teams/apac.csv").iloc[:,0].tolist()
cn = pd.read_csv("data/tier1/teams/cn.csv").iloc[:,0].tolist()
teams = pd.read_csv('data/tier1/teams.csv')

def calculate_net_earnings(data):
    def get_team_fullname(row):
        team = teams[teams['id'] == row['team']]
        row['teamname'] = team['fullname'].values[0]
        return row
    # Determine the net earnings for each game based on the 'correct' column
    data['t1_earnings'] = data.apply(lambda row: row['$win'] if row['correct'] else -row['$lose'], axis=1)
    data['t2_earnings'] = data.apply(lambda row: -row['$lose'] if row['correct'] else row['$win'], axis=1)
    
    # Group by team to calculate total net earnings
    t1_earnings = data.groupby('t1')['t1_earnings'].sum().reset_index()
    t2_earnings = data.groupby('t2')['t2_earnings'].sum().reset_index()
    
    # Rename columns for clarity
    t1_earnings.columns = ['team', 'net_earnings']
    t2_earnings.columns = ['team', 'net_earnings']
    
    # Combine the earnings from t1 and t2
    total_earnings = pd.concat([t1_earnings, t2_earnings]).groupby('team').sum().reset_index()
    total_earnings['teamname'] = ''
    total_earnings = total_earnings.apply(get_team_fullname, axis=1)
    total_earnings = total_earnings[['teamname', 'net_earnings']]

    return total_earnings

calculate_net_earnings(res).to_csv('data/earnings.csv')

amer = res.loc[res['t1'].isin(amer)]
amer_net = amer.loc[(amer['correct'] == True)]['$win'].sum() - amer['correct'].value_counts()[False] * 50
print(f"amer: {(amer['correct'].value_counts()[True] / amer['correct'].value_counts().sum()) * 100:.2f}%" + " net: " + str(amer_net))

emea = res.loc[res['t1'].isin(emea)]
emea_net = emea.loc[(emea['correct'] == True)]['$win'].sum() - emea['correct'].value_counts()[False] * 50
print(f"emea: {(emea['correct'].value_counts()[True] / emea['correct'].value_counts().sum()) * 100:.2f}%" + " net: " + str(emea_net))

apac = res.loc[res['t1'].isin(apac)]
apac_net = apac.loc[(apac['correct'] == True)]['$win'].sum() - apac['correct'].value_counts()[False] * 50
print(f"apac: {(apac['correct'].value_counts()[True] / apac['correct'].value_counts().sum()) * 100:.2f}%" + " net: " + str(apac_net))

# cn = res.loc[res['t1'].isin(cn)]
# cn_net = cn.loc[(cn['correct'] == True)]['$win'].sum() - cn['correct'].value_counts()[False] * 50
# print(f"cn: {(cn['correct'].value_counts()[True] / cn['correct'].value_counts().sum()) * 100:.2f}%" + " net: " + str(cn_net))
