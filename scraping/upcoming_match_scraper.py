import pandas as pd, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from link_scraper import fetch_data
from stats_scraper import parse_h2h, parse_history


site = 'https://www.vlr.gg'
teams = pd.read_csv('data/all/teams.csv', index_col=False)

def get_match_data(url):
    link = site + url
    soup = fetch_data(link)
    match_id = url.split('/')[1]

    # Check if teams are set
    if (soup.find('a', {'class': 'match-header-link wf-link-hover mod-1'}).text.strip() == 'TBD' 
        or soup.find('a', {'class': 'match-header-link wf-link-hover mod-2'}).text.strip() == 'TBD'):
        return None

    # Get match info
    date = soup.find('div', {'class': 'moment-tz-convert'}).get('data-utc-ts').split()[0]
    t1 = int(soup.find('a', {'class': 'match-header-link wf-link-hover mod-1'})['href'].split('/')[2])
    t2 = int(soup.find('a', {'class': 'match-header-link wf-link-hover mod-2'})['href'].split('/')[2])
    h2h = parse_h2h(soup.find("div", {"class": "match-h2h-matches"}))
    t1_past = parse_history(soup.find_all("div", {"class": "match-histories"})[0]) if len(soup.find_all("div", {"class": "match-histories"})) > 0 else 0
    t2_past = parse_history(soup.find_all("div", {"class": "match-histories"})[1]) if len(soup.find_all("div", {"class": "match-histories"})) > 0 else 0
    # Get betting odds
    t1_best = 1
    t1_odds = soup.find_all('span', {'class': 'match-bet-item-odds mod- mod-1'})
    if t1_odds is not None:
        for odd in t1_odds:
            if float(odd.text) > t1_best:
                t1_best = float(odd.text)
    
    t2_best = 1
    t2_odds = soup.find_all('span', {'class': 'match-bet-item-odds mod- mod-2'})
    if t2_odds is not None:
        for odd in t2_odds:
            if float(odd.text) > t2_best:
                t2_best = float(odd.text)
    
    return [match_id, t1, t2, date, h2h, t1_past, t2_past, t1_best, t2_best]

def get_matches(days):
    # Get list of match urls
    soup = fetch_data(f'{site}/matches')
    schedules = soup.find_all('div', {'class': re.compile(r'^wf-card')})
    matches = []
    for i in range(1, days + 1):
        match_list = schedules[i].find_all('a')
        for match in match_list:
            matches.append(match['href'])
    
    match_data = []
    # Get info for each match
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_data = {executor.submit(get_match_data, match): match for match in matches}
        for future in as_completed(future_to_data):
            match = future_to_data[future]
            try:
                data = future.result()
                if data is not None:
                    match_data.append(data)
            except Exception as exc:
                print(f'An error occurred for match {match}: {exc}')
    cols = ['match_id', 't1', 't2', 'date', 'net_h2h', 't1_past', 't2_past', 't1_odds', 't2_odds']
    df = pd.DataFrame(data=match_data, columns=cols)
    df.to_csv('data/raw/upcoming.csv', index=False)
    return df.copy(deep=True)

get_matches(7)