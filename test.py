import pandas as pd
from IPython.display import display
res = pd.read_csv('data/tier1/results/results.csv')
amer = pd.read_csv("data/tier1/teams/amer.csv").iloc[:,0].tolist()
emea = pd.read_csv("data/tier1/teams/emea.csv").iloc[:,0].tolist()
apac = pd.read_csv("data/tier1/teams/apac.csv").iloc[:,0].tolist()
cn = pd.read_csv("data/tier1/teams/cn.csv").iloc[:,0].tolist()

amer = res.loc[res['t1'].isin(amer)]
amer_net = amer.loc[(amer['correct'] == True)]['$win'].sum() - amer['correct'].value_counts()[False] * 50
print(f"amer: {(amer['correct'].value_counts()[True] / amer['correct'].value_counts().sum()) * 100:.2f}%" + " net: " + str(amer_net))

emea = res.loc[res['t1'].isin(emea)]
emea_net = emea.loc[(emea['correct'] == True)]['$win'].sum() - emea['correct'].value_counts()[False] * 50
print(f"emea: {(emea['correct'].value_counts()[True] / emea['correct'].value_counts().sum()) * 100:.2f}%" + " net: " + str(emea_net))

apac = res.loc[res['t1'].isin(apac)]
apac_net = apac.loc[(apac['correct'] == True)]['$win'].sum() - apac['correct'].value_counts()[False] * 50
print(f"apac: {(apac['correct'].value_counts()[True] / apac['correct'].value_counts().sum()) * 100:.2f}%" + " net: " + str(apac_net))

cn = res.loc[res['t1'].isin(cn)]
cn_net = cn.loc[(cn['correct'] == True)]['$win'].sum() - cn['correct'].value_counts()[False] * 50
print(f"cn: {(cn['correct'].value_counts()[True] / cn['correct'].value_counts().sum()) * 100:.2f}%" + " net: " + str(cn_net))