import sqlite3
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd, re, traceback, concurrent.futures, warnings
from link_scraper import fetch_data, max_workers

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import DB, MATCH_LINKS, NEW_MATCH_LINKS
from market import vig_opposite_probability

team_dict = {}
maps = ['Ascent', 'Bind', 'Breeze', 'Fracture', 'Haven', 'Icebox', 'Lotus', 'Pearl', 'Split', 'Sunset', 'Abyss']
series_headers = [
    "match_id", "t1", "t2", "winner",
    "t1_ban1", "t1_ban2", "t2_ban1", "t2_ban2",
    "t1_pick", "t2_pick", "remaining",
    "t1_mapwins", "t2_mapwins",
    "net_h2h", "t1_past", "t2_past",
    "odds", "best_odds", "worst_odds", "date",
    "t1_fks", "t1_fds", "t1_rating", "t1_acs", "t1_kills", "t1_deaths", "t1_assists",
    "t2_fks", "t2_fds", "t2_rating", "t2_acs", "t2_kills", "t2_deaths", "t2_assists",
]

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

    return [
        maps.index(t1_bans[0]), maps.index(t1_bans[1]),
        maps.index(t2_bans[0]), maps.index(t2_bans[1]),
        maps.index(t1_picks[0]), maps.index(t2_picks[0]),
        maps.index(remaining_map),
    ]


def parse_h2h(h2h):
    if h2h is None:
        return None
    h2h = h2h.find_all("span")
    try:
        t1_h2h = 0
        t2_h2h = 0
        for i in range(10):
            if i % 2 == 0:
                t1_h2h += int(h2h[i].text)
            else:
                t2_h2h += int(h2h[i].text)
    except Exception:
        return t1_h2h - t2_h2h
    return t1_h2h - t2_h2h


def parse_history(history):
    if history is None:
        return None
    net = 0
    wins = history.find_all("span", {"class": "rf"})
    for win in wins:
        net += int(win.text)
    losses = history.find_all("span", {"class": "ra"})
    for loss in losses:
        net -= int(loss.text)
    return net


def _stat_value(td_classes, player_row, span_class="side mod-side mod-both"):
    """Pull an integer/float from a player row's stat td.

    td_classes: list of class names that identify the td (e.g. ["mod-stat", "mod-fb"]).
    """
    td = player_row.find("td", {"class": " ".join(td_classes)})
    if td is None:
        return None
    span = td.find("span", {"class": span_class}) or td.find("span", {"class": "side mod-both"})
    if span is None or not isnum(span.text.strip()):
        return None
    return float(span.text.strip())


def parse_all_maps_team(table_body):
    """Aggregate one team's player rows from the All Maps view.

    Returns [fks, fds, rating, acs, kills, deaths, assists]:
      - rating/acs are averaged over 5 players (each player value is already
        averaged across maps in the All Maps view).
      - kills/deaths/assists/fks/fds are summed (each player value is already
        summed across maps in the All Maps view).
    """
    fks = 0
    fds = 0
    rating = 0.0
    acs = 0.0
    kills = 0
    deaths = 0
    assists = 0
    n_players = 0

    for player in table_body.find_all("tr"):
        stats = player.find_all("td", {"class": "mod-stat"})
        if len(stats) < 5:
            continue
        n_players += 1

        # rating, acs (rate stats — averaged later)
        for total_key, td in (("rating", stats[0]), ("acs", stats[1])):
            span = td.find("span", {"class": "side mod-side mod-both"}) or td.find("span", {"class": "side mod-both"})
            if span is None:
                continue
            txt = span.text.strip()
            if isnum(txt):
                if total_key == "rating":
                    rating += float(txt)
                else:
                    acs += float(txt)

        # K / D / A (counts — summed)
        for total_key, td in (("kills", stats[2]), ("deaths", stats[3]), ("assists", stats[4])):
            span = td.find("span", {"class": "side mod-side mod-both"}) or td.find("span", {"class": "side mod-both"})
            if span is None:
                continue
            txt = span.text.strip()
            if txt.isdigit():
                if total_key == "kills":
                    kills += int(txt)
                elif total_key == "deaths":
                    deaths += int(txt)
                else:
                    assists += int(txt)

        # First kills
        fk_val = _stat_value(["mod-stat", "mod-fb"], player, span_class="side mod-both")
        if fk_val is not None:
            fks += int(fk_val)

        # First deaths (best-effort: selector may be 'mod-fd' or absent)
        fd_val = _stat_value(["mod-stat", "mod-fd"], player, span_class="side mod-both")
        if fd_val is not None:
            fds += int(fd_val)

    if n_players == 0:
        return [None] * 7
    return [fks, fds, rating / n_players, acs / n_players, kills, deaths, assists]


def parse_all_maps(soup):
    """Find the data-game-id='all' div and pull both teams' aggregate stats.

    Returns (t1_stats, t2_stats); either may be [None]*7 if missing/malformed.
    """
    try:
        container = soup.find("div", {"class": "vm-stats-container"})
        if container is None:
            return [None] * 7, [None] * 7
        all_div = container.find("div", {"data-game-id": "all"})
        if all_div is None:
            return [None] * 7, [None] * 7
        player_tables = all_div.find_all("table", {"class": "wf-table-inset mod-overview"})
        if len(player_tables) < 2:
            return [None] * 7, [None] * 7
        t1_stats = parse_all_maps_team(player_tables[0].find("tbody"))
        t2_stats = parse_all_maps_team(player_tables[1].find("tbody"))
        return t1_stats, t2_stats
    except Exception as e:
        log(f"Error parsing All Maps view: {e}")
        return [None] * 7, [None] * 7


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
        if score[0] is None or score[1] is None:
            log(f"Skipping match {match_id}: no final score")
            return None
        h2h = parse_h2h(soup.find("div", {"class": "match-h2h-matches"}))
        histories = soup.find_all("div", {"class": "match-histories"})
        t1_past = parse_history(histories[0]) if len(histories) > 0 else None
        t2_past = parse_history(histories[1]) if len(histories) > 1 else None
        winner = score[0] > score[1]

        odds_spans = soup.find_all("span", {"class": "match-bet-item-odds"})
        best_odds = 0
        worst_odds = 1
        for odd in odds_spans:
            val = float(odd.text[1:])
            val = 1 / (val / 100) if val > 0 else 0
            if not winner:
                val = vig_opposite_probability(val)
                if val is None:
                    continue
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

        # Vetos — kept on the series row for future use, but no longer gate the row
        vetos_note = soup.find("div", {"class": "match-header-note"})
        vetos = parse_vetos(t1, t2, vetos_note.text) if vetos_note else "no vetos"
        veto_cols = [None] * 7 if vetos == "no vetos" else vetos

        # All Maps aggregate player stats
        t1_aggr, t2_aggr = parse_all_maps(soup)

        row = (
            [match_id, t1, t2, winner]
            + veto_cols
            + [score[0], score[1], h2h, t1_past, t2_past, odds, best_odds, worst_odds, date]
            + t1_aggr + t2_aggr
        )
        return pd.Series(data=row, index=series_headers)

    except Exception as e:
        log(f"Error processing match {match_id}: {str(e)}")
        traceback.print_exc()
        return None


def process_matches(links, replace=False):
    global series_df

    match_stats_list = []
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
            if result is not None:
                match_stats_list.append(result)
            if completed == 1 or completed % 10 == 0 or completed == total_links:
                log(
                    "Match processing progress: "
                    f"{completed}/{total_links}; "
                    f"series rows: {len(match_stats_list)}"
                )

    if not match_stats_list:
        log("No match data found.")
        return False

    series_df = pd.concat(match_stats_list, axis=1).T
    write_scraped_table(series_df, "series", "match_id", replace=replace)
    return True


def append_match_links(new_links_file, match_links_file):
    """Merge new_links_file into match_links_file as a deduped, sorted set.

    Prior versions blind-appended, which duplicated links every time an
    incremental scrape re-saw an already-known match.
    """
    log(f"Merging {new_links_file} into {match_links_file}")
    existing = set()
    if match_links_file.exists():
        with open(match_links_file, "r") as f:
            existing = {line.strip() for line in f if line.strip()}
    with open(new_links_file, "r") as f:
        new_links = {line.strip() for line in f if line.strip()}
    added = new_links - existing
    merged = sorted(existing | new_links)
    with open(match_links_file, "w") as f:
        f.write("\n".join(merged) + ("\n" if merged else ""))
    open(new_links_file, "w").close()
    log(f"Merged: {len(added)} new, {len(new_links) - len(added)} duplicates skipped; total {len(merged)}")


def process_tier1():
    log("Starting full tier 1 match processing")
    process_matches(read_links(MATCH_LINKS), replace=True)


def update_tier1():
    log("Starting incremental tier 1 match processing")
    links = read_links(NEW_MATCH_LINKS)
    scraped = process_matches(links, replace=False)
    if scraped:
        append_match_links(NEW_MATCH_LINKS, MATCH_LINKS)
    else:
        log("Leaving new match links in place because no match data was scraped.")
