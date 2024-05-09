import requests
from bs4 import BeautifulSoup, Tag
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import csv

region_urls = [
    "/event/2004/champions-tour-2024-americas-stage-1/regular-season",
    "/event/1998/champions-tour-2024-emea-stage-1/regular-season",
    "/event/2002/champions-tour-2024-pacific-stage-1/regular-season",
    "/event/1971/challengers-league-2024-north-america-stage-1/regular-season",
    "/event/1971/challengers-league-2024-north-america-stage-1",
    "/event/1949/gamers-club-challengers-league-2024-brazil-split-1/regular-season",
    "/event/1949/gamers-club-challengers-league-2024-brazil-split-1/open-qualifier-1",
    "/event/1949/gamers-club-challengers-league-2024-brazil-split-1/open-qualifier-2",
    "/event/1943/challengers-league-2024-northern-europe-polaris-split-1/regular-season",
    "/event/1943/challengers-league-2024-northern-europe-polaris-split-1/open-qualifier",
    "/event/1942/challengers-league-2024-france-revolution-split-1/regular-season",
    "/event/1948/challengers-league-2024-dach-evolution-split-1/regular-season",
    "/event/1948/challengers-league-2024-dach-evolution-split-1/open-qualifier",
    "/event/1932/challengers-league-2024-east-surge-split-1/regular-season",
    "/event/1932/challengers-league-2024-east-surge-split-1/open-qualifier",
    "/event/1945/challengers-league-2024-portugal-tempest-split-1/regular-season",
    "/event/1962/challengers-league-2024-japan-split-1",
    "/event/1958/wdg-challengers-league-2024-korea-split-1/main-stage",
    "/event/2055/wdg-challengers-league-2024-korea-split-2",
    "/event/1447/world-cyber-games-challengers-league-korea-split-1/regular-league",
    "/event/1974/challengers-league-2024-vietnam-split-1/swiss-stage",
    "/event/1955/challengers-league-2024-taiwan-hong-kong-split-1/main-event",
    "/event/1966/omen-challengers-league-2024-south-asia-split-1/cup-2-regular-season",
    "/event/1966/omen-challengers-league-2024-south-asia-split-1/open-qualifier",
    "/event/1986/challengers-league-oceania-stage-1/group-stage",
    "/event/1986/challengers-league-oceania-stage-1/open-qualifier",
    "/event/1939/challengers-league-2024-spain-rising-split-1/regular-season",
    "/event/1750/game-changers-2023-championship-s-o-paulo",
    "/event/1893/challengers-league-2024-t-rkiye-birlik-split-1/regular-season",
    "/event/1944/challengers-league-2024-mena-resilience-split-1/levant-and-north-africa",
    "/event/1944/challengers-league-2024-mena-resilience-split-1/gcc-and-iraq",
    "/event/1947/challengers-league-2024-italy-rinascimento-split-1/regular-season",
    "/event/1898/challengers-league-2024-latam-north-ace-split-1/regular-phase",
    "/event/1950/challengers-league-2024-latam-south-ace-split-1/regular-phase",
    "/event/1964/challengers-league-2024-philippines-split-1/group-stage",
    "/event/1960/afreecatv-challengers-league-2024-thailand-split-1/group-stage",
    "/event/1956/challengers-league-2024-malaysia-singapore-split-1/group-stage",
    "/event/1952/challengers-league-2024-indonesia-split-1/main-event"
]

tier1_urls = [
    "/event/2004/champions-tour-2024-americas-stage-1/regular-season",
    "/event/1998/champions-tour-2024-emea-stage-1/regular-season",
    "/event/2002/champions-tour-2024-pacific-stage-1/regular-season"
]

site = "https://www.vlr.gg"

def fetch_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def fetch_team_links(url):
    soup = fetch_data(site + url)
    team_links = []
    if soup:
        teams = soup.find("div", {"class": "event-teams-container"})
        if teams:
            for team in teams.find_all("a", {"class": "event-team-name"}):
                href = team.get("href")
                team_links.append(href[5:]) 
    return team_links

def fetch_match_links(team_url_suffix, start_date):
    match_links = []
    full_url = site + "/team/matches" + team_url_suffix
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

team_links = []
match_links = []

def scrape_all_games(start_date):
    # Fetch team links using multithreading
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_region = {executor.submit(fetch_team_links, url): url for url in region_urls}
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

    match_links = list(set(match_links))

    with open("data/match_links.csv", "w", newline="") as file:
        writer = csv.writer(file)
        for link in match_links:
            writer.writerow([link])

def fetch_all_teamnames(line):
    team = line.strip('\n').split(',')
    soup = fetch_data(f"https://www.vlr.gg/team/{team[0]}")
    names = soup.select('[class*="wf-title"]') 
    for n in names:
        team.append(n.text)
    return team


def get_all_teamname_variations(teamfile):
    teams = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        with open(teamfile, 'r') as f:
            future_to_team = {executor.submit(fetch_all_teamnames, line): line for line in f}
            for future in as_completed(future_to_team):
                team = future_to_team[future]
                try:
                    teams.append(future.result())
                except Exception as exc:
                    print(f'{team} generated an exception: {exc}')
                    traceback.print_exc()
    with open(teamfile, 'w') as csv_file:  
        writer = csv.writer(csv_file)
        for t in teams:
            writer.writerow(t)

get_all_teamname_variations('data/teams.csv')

# scrape_all_games("2023/02/12")

# Fetch tier 1 team links
# with open("data/tier1_teams.csv", "w", newline="") as file:
#     writer = csv.writer(file)
#     for link in team_links:
#         team_id = link.split('/')[1]
#         team = link.split('/')[2]
#         writer.writerow([team_id, team])
