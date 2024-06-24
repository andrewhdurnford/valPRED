import requests, traceback, csv, pandas as pd
from bs4 import BeautifulSoup, Tag
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Site url
site = "https://www.vlr.gg"

# Tier 1 events
amer = "/event/2004/champions-tour-2024-americas-stage-1/regular-season"
emea = "/event/1998/champions-tour-2024-emea-stage-1/regular-season"
apac = "/event/2002/champions-tour-2024-pacific-stage-1/regular-season"
cn = "/event/2006/champions-tour-2024-china-stage-1/regular-season"

# Other events
na_s1_rs = "/event/1971/challengers-league-2024-north-america-stage-1/regular-season"
na_s1 = "/event/1971/challengers-league-2024-north-america-stage-1"
br_s1_rs = "/event/1949/gamers-club-challengers-league-2024-brazil-split-1/regular-season"
br_s1_oq1 = "/event/1949/gamers-club-challengers-league-2024-brazil-split-1/open-qualifier-1"
br_s1_oq2 = "/event/1949/gamers-club-challengers-league-2024-brazil-split-1/open-qualifier-2"
ne_pol_rs = "/event/1943/challengers-league-2024-northern-europe-polaris-split-1/regular-season"
ne_pol_oq = "/event/1943/challengers-league-2024-northern-europe-polaris-split-1/open-qualifier"
fr_rev = "/event/1942/challengers-league-2024-france-revolution-split-1/regular-season"
dach_ev_rs = "/event/1948/challengers-league-2024-dach-evolution-split-1/regular-season"
dach_ev_oq = "/event/1948/challengers-league-2024-dach-evolution-split-1/open-qualifier"
east_surge = "/event/1932/challengers-league-2024-east-surge-split-1/regular-season"
east_surge_oq = "/event/1932/challengers-league-2024-east-surge-split-1/open-qualifier"
pt_temp_rs = "/event/1945/challengers-league-2024-portugal-tempest-split-1/regular-season"
jp_s1 = "/event/1962/challengers-league-2024-japan-split-1"
kr_s1 = "/event/1958/wdg-challengers-league-2024-korea-split-1/main-stage"
kr_split2 = "/event/2055/wdg-challengers-league-2024-korea-split-2"
wcg_kr = "/event/1447/world-cyber-games-challengers-league-korea-split-1/regular-league"
vn_swiss = "/event/1974/challengers-league-2024-vietnam-split-1/swiss-stage"
tw_hk_main = "/event/1955/challengers-league-2024-taiwan-hong-kong-split-1/main-event"
sa_s1_rs = "/event/1966/omen-challengers-league-2024-south-asia-split-1/cup-2-regular-season"
sa_s1_oq = "/event/1966/omen-challengers-league-2024-south-asia-split-1/open-qualifier"
oce_s1_gs = "/event/1986/challengers-league-oceania-stage-1/group-stage"
oce_s1_oq = "/event/1986/challengers-league-oceania-stage-1/open-qualifier"
es_s1_rs = "/event/1939/challengers-league-2024-spain-rising-split-1/regular-season"
gc_champ = "/event/1750/game-changers-2023-championship-s-o-paulo"
tr_s1_rs = "/event/1893/challengers-league-2024-t-rkiye-birlik-split-1/regular-season"
mena_s1_lana = "/event/1944/challengers-league-2024-mena-resilience-split-1/levant-and-north-africa"
mena_s1_gai = "/event/1944/challengers-league-2024-mena-resilience-split-1/gcc-and-iraq"
it_s1_rs = "/event/1947/challengers-league-2024-italy-rinascimento-split-1/regular-season"
latamn_s1_rs = "/event/1898/challengers-league-2024-latam-north-ace-split-1/regular-phase"
latams_s1_rs = "/event/1950/challengers-league-2024-latam-south-ace-split-1/regular-phase"
ph_s1_gs = "/event/1964/challengers-league-2024-philippines-split-1/group-stage"
th_s1_gs = "/event/1960/afreecatv-challengers-league-2024-thailand-split-1/group-stage"
ms_s1_gs = "/event/1956/challengers-league-2024-malaysia-singapore-split-1/group-stage"
id_s1 = "/event/1952/challengers-league-2024-indonesia-split-1/main-event"

tier1_events = [amer, emea, apac, cn]
all_events = [
    amer, emea, apac, cn,
    na_s1_rs, na_s1, br_s1_rs, br_s1_oq1, br_s1_oq2,
    ne_pol_rs, ne_pol_oq, fr_rev, dach_ev_rs, dach_ev_oq,
    east_surge, east_surge_oq, pt_temp_rs, jp_s1, kr_s1, kr_split2, wcg_kr,
    vn_swiss, tw_hk_main, sa_s1_rs, sa_s1_oq, oce_s1_gs, oce_s1_oq,
    es_s1_rs, gc_champ, tr_s1_rs, mena_s1_lana, mena_s1_gai, it_s1_rs,
    latamn_s1_rs, latams_s1_rs, ph_s1_gs, th_s1_gs, ms_s1_gs, id_s1
]

# Dictionary of events
events_dict = {
    "amer": amer,
    "emea": emea,
    "apac": apac,
    "cn": cn,
    "na_s1_rs": na_s1_rs,
    "na_s1": na_s1,
    "br_s1_rs": br_s1_rs,
    "br_s1_oq1": br_s1_oq1,
    "br_s1_oq2": br_s1_oq2,
    "ne_pol_rs": ne_pol_rs,
    "ne_pol_oq": ne_pol_oq,
    "fr_rev": fr_rev,
    "dach_ev_rs": dach_ev_rs,
    "dach_ev_oq": dach_ev_oq,
    "east_surge": east_surge,
    "east_surge_oq": east_surge_oq,
    "pt_temp_rs": pt_temp_rs,
    "jp_s1": jp_s1,
    "kr_s1": kr_s1,
    "kr_split2": kr_split2,
    "wcg_kr": wcg_kr,
    "vn_swiss": vn_swiss,
    "tw_hk_main": tw_hk_main,
    "sa_s1_rs": sa_s1_rs,
    "sa_s1_oq": sa_s1_oq,
    "oce_s1_gs": oce_s1_gs,
    "oce_s1_oq": oce_s1_oq,
    "es_s1_rs": es_s1_rs,
    "gc_champ": gc_champ,
    "tr_s1_rs": tr_s1_rs,
    "mena_s1_lana": mena_s1_lana,
    "mena_s1_gai": mena_s1_gai,
    "it_s1_rs": it_s1_rs,
    "latamn_s1_rs": latamn_s1_rs,
    "latams_s1_rs": latams_s1_rs,
    "ph_s1_gs": ph_s1_gs,
    "th_s1_gs": th_s1_gs,
    "ms_s1_gs": ms_s1_gs,
    "id_s1": id_s1
}
team_cols = ['id', 'linkname', 'fullname', 'abbrev', 'secondary_id']

# Get soup from url
def fetch_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def get_team_links(event):
    soup = fetch_data(site + event)
    team_links = []
    if soup:
        teams = soup.find("div", {"class": "event-teams-container"})
        if teams:
            for team in teams.find_all("a", {"class": "event-team-name"}):
                href = team.get("href")
                team_links.append(href[5:]) 
    return team_links

def fetch_match_links(team_url_suffix, start_date):
    def fetch_links(team_url_suffix, start_date, page):
        match_links = []
        full_url = site + "/team/matches" + team_url_suffix + f"/?page={page}"
        soup = fetch_data(full_url)
        if soup:
            matchlist = soup.find("div", {"class": "col mod-1"}).find("div", {"class": "mod-dark"})
            if matchlist:
                for div in matchlist.children:
                    if isinstance(div, Tag):
                        a_tag = div.find("a")
                        if a_tag:
                            date_text = a_tag.find("div", {"class": "m-item-date"}).find("div").text.strip()
                            date = datetime.strptime(date_text, "%Y/%m/%d")
                            start = datetime.strptime(start_date, "%Y/%m/%d")
                            if date < start:
                                break
                            link = a_tag.get("href")
                            match_links.append(link)
        return match_links

    match_links = []
    full_url = site + "/team/matches" + team_url_suffix
    soup = fetch_data(full_url)
    pages = len(soup.find("div", {"class": "action-container-pages"}).contents)
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(fetch_links, team_url_suffix, start_date, i + 1) for i in range(pages)]
        for future in as_completed(futures):
            match_links.extend(future.result())
    return match_links

def scrape_all_games(start_date, events):
    global team_links, match_links
    team_links = []
    match_links = []
    # Fetch team links using multithreading
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_region = {executor.submit(get_team_links, url): url for url in events}
        for future in as_completed(future_to_region):
            region = future_to_region[future]
            try:
                team_links.extend(future.result())
            except Exception as exc:
                print(f'{region} generated an exception: {exc}')
                traceback.print_exc()

    # Fetch match links using multithreading
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_team = {executor.submit(fetch_match_links, url, start_date): url for url in team_links}
        for future in as_completed(future_to_team):
            team = future_to_team[future]
            try:
                match_links.extend(future.result())
            except Exception as exc:
                print(f'{team} generated an exception: {exc}')
                traceback.print_exc()

    return list(set(match_links))

def get_event_teams(event):
    # Function to retrieve teamnames with all varations
    def get_all_teamnames(link):
        parts = link.split('/')
        team = [int(parts[1]), parts[2]]
        soup = fetch_data(f"https://www.vlr.gg/team{link}")
        names = soup.select('[class*="wf-title"]') 
        for n in names:
            team.append(n.text)
        return team

    teamlinks = get_team_links(event)
    teams = []

    # Get all teamname variations
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_team = {executor.submit(get_all_teamnames, link): link for link in teamlinks}
        for future in as_completed(future_to_team):
            team = future_to_team[future]
            try:
                teams.append(future.result())
            except Exception as exc:
                print(f'{team} generated an exception: {exc}')
                traceback.print_exc()
    
    return teams

def get_all_teams():
    teams = []
    for event in all_events:
        teams.extend(get_event_teams(event))

    # Save to csv
    with open(f'data/all/teams.csv', 'w') as csv_file:  
        writer = csv.writer(csv_file)
        writer.writerow(team_cols)
        for t in teams:
            writer.writerow(t)

def get_tier1_teams():
    for event in tier1_events:
        teams = get_event_teams(event)

        # Find event name
        filename = list(events_dict.keys())[list(events_dict.values()).index(event)]

        # Save to csv
        with open(f'data/tier1/teams/{filename}.csv', 'w') as csv_file:  
            writer = csv.writer(csv_file)
            writer.writerow(team_cols)
            for t in teams:
                writer.writerow(t)

def get_all_tier1_teams():
    teams = []
    for event in tier1_events:
        teams.extend(get_event_teams(event))

        # Save to csv
        with open(f'data/tier1/teams.csv', 'w') as csv_file:  
            writer = csv.writer(csv_file)
            writer.writerow(team_cols)
            for t in teams:
                writer.writerow(t)

def get_all_matchlinks():
    links = scrape_all_games('2021/03/30', all_events)
    with open("scraping/match_links.csv", "w", newline="") as file:
        writer = csv.writer(file)
        for link in links:
            writer.writerow([link])

def update_all_matchlinks():
    series = pd.read_csv('data/series.csv', index_col=False)
    date = (datetime.strptime(series['date'].max(), "%Y-%m-%d") - timedelta(1)).strftime("%Y/%m/%d")
    links = scrape_all_games(date, all_events)

    with open("scraping/new_match_links.csv", "w", newline="") as file:
        writer = csv.writer(file)
        for link in links:
            writer.writerow([link])

def get_tier1_matchlinks():
    links = scrape_all_games('2021/03/30', tier1_events)
    with open("scraping/tier1_match_links.csv", "w", newline="") as file:
        writer = csv.writer(file)
        for link in links:
            writer.writerow([link])

def update_tier1_matchlinks():
    series = pd.read_csv('data/tier1/series.csv', index_col=False)
    date = (datetime.strptime(series['date'].max(), "%Y-%m-%d") - timedelta(1)).strftime("%Y/%m/%d")
    links = scrape_all_games(date, tier1_events)

    with open("scraping/new_tier1_match_links.csv", "w", newline="") as file:
        writer = csv.writer(file)
        for link in links:
            writer.writerow([link])
