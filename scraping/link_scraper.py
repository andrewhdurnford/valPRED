import csv
import sqlite3
import sys
import time
import tomllib
import traceback
from pathlib import Path
from bs4 import BeautifulSoup, Tag
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import CONFIG, DB, MATCH_LINKS, NEW_MATCH_LINKS


def load_config():
    with open(CONFIG, "rb") as config_file:
        return tomllib.load(config_file)


def normalize_scrape_date(date):
    return date.replace("-", "/")


def region_from_event(event):
    slug = event.strip("/").split("/")[2] if len(event.strip("/").split("/")) > 2 else event
    if "americas" in slug:
        return "amer"
    if "emea" in slug:
        return "emea"
    if "pacific" in slug:
        return "apac"
    if "china" in slug:
        return "cn"
    return "global"


cfg = load_config()
site = cfg["scraping"]["site"]
tier1_events = cfg["scraping"]["tier1_events"]
team_cols = ['id', 'linkname', 'fullname', 'abbrev', 'secondary_id']
request_attempts = cfg["scraping"].get("request_attempts", 2)
connect_timeout = cfg["scraping"].get("connect_timeout_seconds", 5)
read_timeout = cfg["scraping"].get("read_timeout_seconds", 30)
request_delay = cfg["scraping"].get("request_delay_seconds", 0.2)
request_backoff = cfg["scraping"].get("request_backoff_seconds", 2)
max_workers = cfg["scraping"].get("max_workers", 4)
event_workers = cfg["scraping"].get("event_workers", 2)
request_headers = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


class FetchError(RuntimeError):
    pass


# Get soup from url
def fetch_data(url, *, required=False):
    for attempt in range(1, request_attempts + 1):
        try:
            time.sleep(request_delay)
            response = requests.get(
                url,
                headers=request_headers,
                timeout=(connect_timeout, read_timeout),
            )
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            if attempt == request_attempts:
                log(f"Request failed for {url}: {e}")
                if required:
                    raise FetchError(f"Could not fetch required page: {url}") from e
                return None
            log(f"Request attempt {attempt}/{request_attempts} failed for {url}: {e}")
            time.sleep(request_backoff * (2 ** (attempt - 1)))


def write_team_rows(rows):
    log(f"Writing {len(rows)} team rows to {DB}")
    DB.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB) as con:
        con.executemany(
            """
            INSERT OR REPLACE INTO teams
            (id, linkname, fullname, abbrev, secondary_id, region)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def read_latest_series_date():
    with sqlite3.connect(DB) as con:
        try:
            max_date = con.execute("SELECT MAX(date) FROM series").fetchone()[0]
        except sqlite3.OperationalError:
            return None
    return max_date


def read_team_links_from_db(regions=None):
    where = ""
    params = []
    if regions:
        placeholders = ", ".join("?" for _ in regions)
        where = f"WHERE region IN ({placeholders})"
        params = list(regions)

    with sqlite3.connect(DB) as con:
        try:
            rows = con.execute(
                f"""
                SELECT id, linkname
                FROM teams
                {where}
                ORDER BY id
                """,
                params,
            ).fetchall()
        except sqlite3.OperationalError:
            return []

    links = [f"/{team_id}/{linkname}" for team_id, linkname in rows if team_id and linkname]
    log(f"Read {len(links)} team links from {DB}")
    return links


def write_links(path, links):
    # Plain LF lines, not csv.writer — csv.writer's default \r\n line terminator
    # used to mix with LF appends in append_match_links and broke set dedup.
    unique_links = sorted(set(links))
    log(f"Writing {len(unique_links)} match links to {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as file:
        file.write("\n".join(unique_links) + ("\n" if unique_links else ""))

def get_team_links(event):
    log(f"Fetching event teams: {event}")
    soup = fetch_data(site + event, required=True)
    team_links = []
    teams = soup.find("div", {"class": "event-teams-container"})
    if teams:
        for team in teams.find_all("a", {"class": "event-team-name"}):
            href = team.get("href")
            team_links.append(href[5:])
    log(f"Found {len(team_links)} teams for event: {event}")
    return team_links

def fetch_match_links(team_url_suffix, start_date):
    def parse_match_page(soup, start_date):
        match_links = []
        reached_start = False
        if soup:
            col = soup.find("div", {"class": "col mod-1"})
            matchlist = col.find("div", {"class": "mod-dark"}) if col else None
            if matchlist:
                for div in matchlist.children:
                    if isinstance(div, Tag):
                        a_tag = div.find("a")
                        if a_tag:
                            date_text = a_tag.find("div", {"class": "m-item-date"}).find("div").text.strip()
                            date = datetime.strptime(date_text, "%Y/%m/%d")
                            start = datetime.strptime(start_date, "%Y/%m/%d")
                            if date < start:
                                reached_start = True
                                break
                            link = a_tag.get("href")
                            match_links.append(link)
        return match_links, reached_start

    match_links = []
    full_url = site + "/team/matches" + team_url_suffix
    soup = fetch_data(full_url)
    if soup is None:
        log(f"No match page response for team: {team_url_suffix}")
        return match_links
    page_container = soup.find("div", {"class": "action-container-pages"})
    pages = len(page_container.contents) if page_container else 1
    log(f"Fetching {pages} match pages for team: {team_url_suffix}")

    page_links, reached_start = parse_match_page(soup, start_date)
    match_links.extend(page_links)
    if reached_start:
        log(f"Reached start date on first page for team: {team_url_suffix}")
        return match_links

    for page in range(2, pages + 1):
        page_url = site + "/team/matches" + team_url_suffix + f"/?page={page}"
        page_soup = fetch_data(page_url)
        page_links, reached_start = parse_match_page(page_soup, start_date)
        match_links.extend(page_links)
        if reached_start:
            break
    log(f"Found {len(match_links)} recent match links for team: {team_url_suffix}")
    return match_links

def scrape_team_match_links(start_date, team_links):
    global match_links
    match_links = []
    team_links = list(set(team_links))
    log(f"Scraping match links for {len(team_links)} teams since {start_date}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_team = {executor.submit(fetch_match_links, url, start_date): url for url in team_links}
        total_teams = len(future_to_team)
        completed = 0
        for future in as_completed(future_to_team):
            team = future_to_team[future]
            try:
                match_links.extend(future.result())
            except Exception as exc:
                log(f'{team} generated an exception: {exc}')
                traceback.print_exc()
            completed += 1
            if completed == 1 or completed % 10 == 0 or completed == total_teams:
                log(f"Team match-link fetch progress: {completed}/{total_teams}; links so far: {len(set(match_links))}")

    unique_match_links = list(set(match_links))
    log(f"Finished match-link scrape: {len(unique_match_links)} unique links")
    return unique_match_links


def scrape_all_games(start_date, events):
    team_links = []
    log(f"Scraping match links from {len(events)} events since {start_date}")
    # Fetch team links using multithreading
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_region = {executor.submit(get_team_links, url): url for url in events}
        total_events = len(future_to_region)
        completed = 0
        failed_events = []
        for future in as_completed(future_to_region):
            region = future_to_region[future]
            try:
                team_links.extend(future.result())
            except Exception as exc:
                log(f'{region} generated an exception: {exc}')
                traceback.print_exc()
                failed_events.append(region)
            completed += 1
            log(f"Event team fetch progress: {completed}/{total_events}")

    if failed_events:
        raise RuntimeError(f"Aborting match-link scrape after failed event fetches: {', '.join(failed_events)}")

    log(f"Collected {len(set(team_links))} unique team links")
    return scrape_team_match_links(start_date, team_links)

def get_event_teams(event):
    # Function to retrieve teamnames with all varations
    def get_all_teamnames(link):
        parts = link.split('/')
        team = [int(parts[1]), parts[2]]
        soup = fetch_data(f"https://www.vlr.gg/team{link}")
        if soup is None:
            return team
        names = soup.select('[class*="wf-title"]') 
        for n in names:
            team.append(n.text)
        return team

    log(f"Refreshing team metadata for event: {event}")
    teamlinks = get_team_links(event)
    teams = []

    # Get all teamname variations
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_team = {executor.submit(get_all_teamnames, link): link for link in teamlinks}
        total_teams = len(future_to_team)
        completed = 0
        for future in as_completed(future_to_team):
            team = future_to_team[future]
            try:
                teams.append(future.result())
            except Exception as exc:
                log(f'{team} generated an exception: {exc}')
                traceback.print_exc()
            completed += 1
            if completed == 1 or completed % 10 == 0 or completed == total_teams:
                log(f"Team metadata progress for {event}: {completed}/{total_teams}")
    
    log(f"Finished team metadata for event: {event}; teams: {len(teams)}")
    return teams

def get_all_tier1_teams():
    rows = []
    log(f"Refreshing tier 1 team metadata across {len(tier1_events)} events")
    with ThreadPoolExecutor(max_workers=event_workers) as executor:
        future_to_event = {executor.submit(get_event_teams, event): event for event in tier1_events}
        completed = 0
        failed_events = []
        for future in as_completed(future_to_event):
            event = future_to_event[future]
            region = region_from_event(event)
            try:
                teams = future.result()
            except Exception as exc:
                log(f'{event} generated an exception: {exc}')
                traceback.print_exc()
                failed_events.append(event)
                teams = []
            for team in teams:
                row = list(team[:len(team_cols)])
                row.extend([None] * (len(team_cols) - len(row)))
                rows.append(row + [region])
            completed += 1
            log(f"Tier 1 metadata event progress: {completed}/{len(tier1_events)}")
    if failed_events:
        raise RuntimeError(f"Aborting team metadata refresh after failed event fetches: {', '.join(failed_events)}")
    write_team_rows(rows)

def get_tier1_matchlinks():
    log("Building full tier 1 match-link file")
    links = scrape_all_games(normalize_scrape_date(cfg["scraping"]["start_date"]), tier1_events)
    write_links(MATCH_LINKS, links)

def update_tier1_matchlinks():
    latest_date = read_latest_series_date()
    if latest_date is None:
        date = normalize_scrape_date(cfg["scraping"]["start_date"])
    else:
        date = (datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(1)).strftime("%Y/%m/%d")
    log(f"Updating tier 1 match links using start date {date}")
    team_links = read_team_links_from_db(["amer", "emea", "apac", "cn", "global"])
    if team_links:
        links = scrape_team_match_links(date, team_links)
    else:
        log("No team links found in DB; falling back to event pages")
        links = scrape_all_games(date, tier1_events)
    write_links(NEW_MATCH_LINKS, links)
