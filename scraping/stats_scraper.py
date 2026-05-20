import sqlite3
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd, re, traceback, concurrent.futures, warnings
from bs4 import Tag
from link_scraper import fetch_data, max_workers

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import DB, MATCH_LINKS, NEW_MATCH_LINKS

team_dict = {}
maps = ['Ascent', 'Bind', 'Breeze', 'Fracture', 'Haven', 'Icebox', 'Lotus', 'Pearl', 'Split', 'Sunset', 'Abyss']
agents = ["Astra", "Breach", "Brimstone", "Chamber", "Clove", "Cypher", "Deadlock", "Fade", "Gekko", "Harbor", "Iso", 
          "Jett", "Kayo", "Killjoy", "Neon", "Omen", "Phoenix", "Raze", "Reyna", "Sage", "Skye", "Sova", "Viper", "Yoru"]
series_headers = ["match_id", "t1", "t2", "winner", "t1_ban1", "t1_ban2", "t2_ban1", "t2_ban2", "t1_pick", "t2_pick", "remaining", "t1_mapwins", "t2_mapwins", "net_h2h", "t1_past", "t2_past", "odds", "best_odds", "worst_odds", "date"]

site = "https://www.vlr.gg"
warnings.filterwarnings("ignore", category=FutureWarning)


def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def read_links(path):
    if not path.exists():
        log(f"Match-link file does not exist: {path}")
        return []
    with open(path, "r") as f:
        links = list({line.strip() for line in f if line.strip()})
    log(f"Read {len(links)} match links from {path}")
    return links


def write_scraped_table(df, table, key, replace=False):
    log(f"Writing {len(df)} rows to table '{table}' in {DB} (replace={replace})")
    DB.parent.mkdir(parents=True, exist_ok=True)
    df = df.drop_duplicates(subset=key, keep="first")
    # Coerce pd.NA / NaN to None so sqlite3 bindings accept them
    df = df.astype(object).where(pd.notnull(df), None)
    cols = list(df.columns)
    col_list = ", ".join(cols)
    placeholders = ", ".join("?" for _ in cols)
    # UPSERT so re-scraping a row doesn't clobber columns the scraper doesn't
    # own (e.g. t1_elo/t2_elo/elo_diff on series, populated later by elo.py).
    update_cols = [c for c in cols if c != key]
    set_clause = ", ".join(f"{c}=excluded.{c}" for c in update_cols)
    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT ({key}) DO UPDATE SET {set_clause}"
    )
    rows = list(df.itertuples(index=False, name=None))
    with sqlite3.connect(DB) as con:
        if replace:
            con.execute(f"DELETE FROM {table}")
        con.executemany(sql, rows)
        con.commit()

def isnum(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def get_team(href):
    id_match = int(re.search(r"/team/(\d+)", href).group(1))
    name_match = re.search(r"/team/\d+/([^/]+)$", href).group(1)
    team_dict[id_match] = name_match
    return id_match

def parse_vetos(t1, t2, vetos):
    teams = set(re.findall(r'(\w+) (?:ban|pick)', vetos))
    teams = list(teams)
    if len(teams) != 2:
        return "no vetos"
    
    team1, team2 = teams
    ban_pattern = r'(\w+) ban (\w+);'
    pick_pattern = r'(\w+) pick (\w+);'
    remaining_pattern = r'(\w+) remains'   
    bans = re.findall(ban_pattern, vetos)
    picks = re.findall(pick_pattern, vetos)
    remaining_map_search = re.search(remaining_pattern, vetos)

    t1_bans = []
    t2_bans = []
    t1_picks = []
    t2_picks = []
    remaining_map = None

    for ban in bans:
        if ban[0] == team1:
            t1_bans.append(ban[1])
        elif ban[0] == team2:
            t2_bans.append(ban[1])

    for pick in picks:
        if pick[0] == team1:
            t1_picks.append(pick[1])
        elif pick[0] == team2:
            t2_picks.append(pick[1])
    
    if remaining_map_search:
        remaining_map = remaining_map_search.group(1)
    else:
        return "no vetos"
    
    if len(t1_bans) != 2 or len(t2_bans) != 2 or len(t1_picks) != 1 or len(t2_picks) != 1 or remaining_map is None:
        return "no vetos"

    return [maps.index(t1_bans[0]), maps.index(t1_bans[1]), maps.index(t2_bans[0]), maps.index(t2_bans[1]), maps.index(t1_picks[0]), maps.index(t2_picks[0]), maps.index(remaining_map)]

def parse_econ(input_string):
    matches = input_string.strip("\n\t").split("(")
    matches[0] = matches[0].strip("\n\t")
    matches[1] = matches[1].strip(")\n\t")
    return int(matches[0]), int(matches[1])

def parse_econ_stats(econ, map_id):
    if 'Stats from this map are not available yet' in econ.find("div", {"class": "vm-stats-game mod-active"}).text:
        return [pd.NA, ] * 10

    econ_stats = econ.find("div", {"class": "vm-stats-container"}).find("div", {"data-game-id": map_id}).find("div", {"style": "overflow-x: auto;"}).find("table").find_all("tr")
    t1_pistols = int(econ_stats[1].find_all("td")[1].find("div", {"class": "stats-sq"}).text)
    t1_ecos_won, t1_ecos_lost = parse_econ(econ_stats[1].find_all("td")[2].find("div", {"class": "stats-sq"}).text)
    t1_fullbuys_won, t1_fullbuys_lost = parse_econ(econ_stats[1].find_all("td")[4].find("div", {"class": "stats-sq"}).text)
    t2_pistols = int(econ_stats[2].find_all("td")[1].find("div", {"class": "stats-sq"}).text) / 2
    t2_ecos_won, t2_ecos_lost = parse_econ(econ_stats[2].find_all("td")[2].find("div", {"class": "stats-sq"}).text)
    t2_fullbuys_won, t2_fullbuys_lost = parse_econ(econ_stats[2].find_all("td")[4].find("div", {"class": "stats-sq"}).text)
    return [t1_pistols, t2_pistols, t1_ecos_won, t1_ecos_lost, t2_ecos_won, t2_ecos_lost, t1_fullbuys_won, t1_fullbuys_lost, t2_fullbuys_won, t2_fullbuys_lost]

def parse_performance_stats(perf, map_id):
    if 'Stats from this map are not available yet' in perf.find("div", {"class": "vm-stats-game mod-active"}).text:
        return [pd.NA, ] * 10
    performance_stats = perf.find("div", {"class": "vm-stats-container"}).find("div", {"data-game-id": map_id}).find("div", {"style": "overflow-x: auto; padding-bottom: 500px; margin-bottom: -500px;"}).find("table").find_all("tr")
    t1_ps = performance_stats[1:6]
    t2_ps = performance_stats[6:11]
    
    # find t1 performance stats
    t1_mks = 0
    t1_clutches = 0
    t1_econ = 0
    t1_plants = 0
    t1_defuses = 0
    for tr in t1_ps:
        tds = tr.find_all("td")
        for i in range(2,14):
            if tds[i].text.strip().isdigit() and tds[i].text.strip() is not None:
                stat = int(tds[i].text.strip())
            else:
                stat = int(re.match(r"\d+", tds[i].text.strip()).group(0)) if re.match(r"\d+", tds[i].text.strip()) is not None else 0
            if 2 <= i <= 5:
                t1_mks += stat 
            elif 6 <= i <= 10:
                t1_clutches += stat
            elif i == 11:
                t1_econ += stat
            elif i == 12:
                t1_plants += stat
            elif i == 13:
                t1_defuses += stat

    t1_econ = t1_econ / 5
    
    # find t2 performance stats
    t2_mks = 0
    t2_clutches = 0
    t2_econ = 0
    t2_plants = 0
    t2_defuses = 0
    for tr in t2_ps:
        tds = tr.find_all("td")
        for i in range(2,14):
            if tds[i].text.strip().isdigit() and tds[i].text.strip() is not None:
                stat = int(tds[i].text.strip())
            else:
                stat = int(re.match(r"\d+", tds[i].text.strip()).group(0)) if re.match(r"\d+", tds[i].text.strip()) is not None else 0
            if 2 <= i <= 5:
                t2_mks += stat
            elif 6 <= i <= 10:
                t2_clutches += stat
            elif i == 11:
                t2_econ += stat
            elif i == 12:
                t2_plants += stat
            elif i == 13:
                t2_defuses += stat

    t2_econ = t2_econ / 5
    return [t1_mks, t1_clutches, t1_econ, t2_mks, t2_clutches, t2_econ, t1_plants, t1_defuses, t2_plants, t2_defuses]

def parse_player_stats(player_data):
    
    fks = 0
    acs = 0
    rating = 0
    kills = 0
    deaths = 0
    assists = 0
    kast = 0
    played_agents = []

    for player in player_data.find_all("tr"):
        played_agents.append(player.find("td", {"class": "mod-agents"}).find("img").get("title"))
        stats = player.find_all("td", {"class": "mod-stat"})
        rating += float(stats[0].find("span", {"class": "side mod-side mod-both"}).text) if isnum(stats[0].find("span", {"class": "side mod-side mod-both"}).text) else 0
        kills += int(stats[2].find("span", {"class": "side mod-side mod-both"}).text)
        deaths += int(stats[3].find("span", {"class": "side mod-both"}).text)
        assists += int(stats[4].find("span", {"class": "side mod-both"}).text)
        acs += int(stats[1].find("span", {"class": "side mod-side mod-both"}).text)
        fks += int(player.find("td", {"class": "mod-stat mod-fb"}).find("span", {"class": "side mod-both"}).text) if isnum(player.find("td", {"class": "mod-stat mod-fb"}).find("span", {"class": "side mod-both"}).text) else 0

    played_agents.sort()
    acs /= 5
    kast = kast / 5 if kast != 0 else None
    rating = rating / 5 if rating != 0 else None
    played_agents = [agents.index(agent) for agent in played_agents]
    return played_agents + [fks, rating, acs, kills, assists, deaths]

def parse_map(map, t1, t2):
    try:
        map_id = map.get("data-game-id")

        if map_id.isdigit():
            # find map name
            map_name = map.find("div", {"class": "vm-stats-game-header"}).find("div", {"class": "map"}).find("span", {"style": "position: relative;"}).text.strip().split("\t")[0]

            # find winner and overtime
            t1_overview = map.find("div", {"class": "vm-stats-game-header"}).find("div", {"class": "team"})
            t1_rds = int(t1_overview.find("div", {"class", "score"}).text.strip())
            t2_rds = int(map.find("div", {"class": "vm-stats-game-header"}).find("div", {"class": "team mod-right"}).find("div", {"class": "score"}).text.strip())
            winner = True if t1_overview.find("div", {"class": "score mod-win"}) else False

            player_stats = map.find_all("table", {"class": "wf-table-inset mod-overview"})
            gen_stats = pd.Series(data=[int(map_id), t1, t2, None, winner, maps.index(map_name), t1_rds, t2_rds], index=['map_id', 't1', 't2', 'date', 'winner', 'map', 't1_rds', 't2_rds'])
            t1_cols = ["t1_agent1", "t1_agent2", "t1_agent3", "t1_agent4", "t1_agent5", 't1_fks', 't1_rating', 't1_acs', 't1_kills', 't1_assists', 't1_deaths']
            t2_cols = ["t2_agent1", "t2_agent2", "t2_agent3", "t2_agent4", "t2_agent5", 't2_fks', 't2_rating', 't2_acs', 't2_kills', 't2_assists', 't2_deaths']
            t1_stats = pd.Series(data=(parse_player_stats(player_stats[0].find("tbody"))), index=t1_cols)
            t2_stats = pd.Series(data=parse_player_stats(player_stats[1].find("tbody")), index=t2_cols)
            stats = pd.concat([gen_stats, t1_stats, t2_stats])   
            return stats
    except Exception as e:
        log(f"Error parsing map: {str(e)}")
        traceback.print_exc()
        return None

def parse_h2h(h2h):
    if h2h is None:
        net_h2h = None
    else:
        h2h = h2h.find_all("span")
        try:
            t1_h2h = 0
            t2_h2h = 0
            for i in range(10):
                if i % 2 == 0:
                    t1_h2h += int(h2h[i].text)
                else:
                    t2_h2h += int(h2h[i].text)
        except:
            return t1_h2h - t2_h2h
        return t1_h2h - t2_h2h

def parse_history(history):
    if history is None:
        return None
    else:
        net = 0
        wins = history.find_all("span", {"class": "rf"})
        for win in wins:
            net += int(win.text)
        losses = history.find_all("span", {"class": "ra"})
        for loss in losses:
            net -= int(loss.text)
        return net

def process_match_link(index, link, total):
    log(f"Starting match {index}/{total}: {link}")
    match_id = re.search(r"/(\d+)/", link).group(1)
    match_link = site + link
    try:
        soup = fetch_data(match_link)
        if soup is None:
            log(f"No response for match {match_id}: {link}")
            return None
        date = soup.find("div", {"class": "moment-tz-convert"}).get("data-utc-ts").split()[0]
        t1 = get_team(soup.find("a", {"class": "match-header-link wf-link-hover mod-1"}).get("href"))
        t2 = get_team(soup.find("a", {"class": "match-header-link wf-link-hover mod-2"}).get("href"))
        score = soup.find("div", {"class": "match-header-vs-score"}).find("div", {"class": "js-spoiler"}).text.split(":")
        score[0] = int(score[0].strip()) if score[0].strip().isdigit() else None
        score[1] = int(score[1].strip()) if score[1].strip().isdigit() else None
        h2h = parse_h2h(soup.find("div", {"class": "match-h2h-matches"}))
        t1_past = parse_history(soup.find_all("div", {"class": "match-histories"})[0]) if len(soup.find_all("div", {"class": "match-histories"})) > 0 else None
        t2_past = parse_history(soup.find_all("div", {"class": "match-histories"})[1]) if len(soup.find_all("div", {"class": "match-histories"})) > 0 else None
        winner = score[0] > score[1]
        odds = soup.find_all("span", {"class": "match-bet-item-odds"})
        best_odds =  0
        worst_odds = 1
        if odds is not None:
            for odd in odds:
                val = float(odd.text[1:])
                val = 1/(val/100) if val > 0 else 0
                if not winner:
                    val = 1 - val
                if val > best_odds and val != 1:
                    best_odds = val
                if val < worst_odds and val != 0:
                    worst_odds = val
        
        if 0 < best_odds < 1 and 0 < worst_odds < 1:
            odds = (best_odds + worst_odds) / 2
        elif 0 < best_odds < 1:
            odds = best_odds
        elif 0 < worst_odds < 1:
            odds = worst_odds
        else:
            odds = None

        match_stats = None
        vetos = parse_vetos(t1, t2, soup.find("div", {"class": "match-header-note"}).text) if soup.find("div", {"class": "match-header-note"}) else 'no vetos'
        if vetos != 'no vetos':
            match_stats = pd.Series(data=([match_id, t1, t2, winner] + vetos + [score[0], score[1], h2h, t1_past, t2_past, odds, best_odds, worst_odds, date]), index=series_headers)

        map_stats = []
        for map in soup.find("div", {"class": "vm-stats-container"}).find_all("div", {"class": "vm-stats-game"}):
            if isinstance(map, Tag):  # Check if the child is a Tag
                stats = parse_map(map, t1, t2)
                if stats is not None:
                    stats.at['date'] = date
                    map_stats.append(stats)
        
        return match_stats, map_stats
    
    except Exception as e:
        log(f"Error processing match {match_id}: {str(e)}")
        traceback.print_exc()
        return None

def process_matches(links, replace=False):
    global series_df, maps_df

    match_stats_list = []
    map_stats_list = []
    total_links = len(links)
    log(f"Processing {total_links} match links (replace={replace})")

    if not links:
        log("No match links to process.")
        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_match_link, index, link, len(links))
            for index, link in enumerate(links, start=1)
        ]
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                log(f"Match worker generated an exception: {exc}")
                traceback.print_exc()
                result = None
            completed += 1
            if not result is None:
                match_stats, map_stats = result
                if match_stats is not None:
                    match_stats_list.append(match_stats)
                map_stats_list.extend(map_stats)
            if completed == 1 or completed % 10 == 0 or completed == total_links:
                log(
                    "Match processing progress: "
                    f"{completed}/{total_links}; "
                    f"series rows: {len(match_stats_list)}; "
                    f"map rows: {len(map_stats_list)}"
                )

    if not match_stats_list and not map_stats_list:
        log("No match data found.")
        return False

    if match_stats_list:
        series_df = pd.concat(match_stats_list, axis=1).T
        write_scraped_table(series_df, "series", "match_id", replace=replace)
    if map_stats_list:
        maps_df = pd.concat(map_stats_list, axis=1).T
        write_scraped_table(maps_df, "maps", "map_id", replace=replace)
    return True

def append_match_links(new_links_file, match_links_file):
    log(f"Appending new match links from {new_links_file} into {match_links_file}")
    # Open the new match links file and read its content
    with open(new_links_file, 'r') as new_links:
        new_content = new_links.readlines()
    
    # Open the tier1 match links file and append the new content
    with open(match_links_file, 'a') as match_links:
        match_links.writelines(new_content)
    
    # Clear the new match links file
    open(new_links_file, 'w').close()
    log(f"Appended {len(new_content)} match links and cleared {new_links_file}")


def process_all():
    log("Starting full match processing")
    process_matches(read_links(MATCH_LINKS), replace=True)

def process_tier1():
    log("Starting full tier 1 match processing")
    process_matches(read_links(MATCH_LINKS), replace=True)

def update_all():
    log("Starting incremental all-event match processing")
    process_matches(read_links(NEW_MATCH_LINKS), replace=False)

def update_tier1():
    log("Starting incremental tier 1 match processing")
    links = read_links(NEW_MATCH_LINKS)
    scraped = process_matches(links, replace=False)
    if scraped:
        append_match_links(NEW_MATCH_LINKS, MATCH_LINKS)
    else:
        log("Leaving new match links in place because no match data was scraped.")
