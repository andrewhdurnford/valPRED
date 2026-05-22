import sqlite3
import sys
import threading
from pathlib import Path
from datetime import datetime

import pandas as pd, re, traceback, concurrent.futures, warnings
from bs4 import Tag
from link_scraper import fetch_data, max_workers

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import DB, MATCH_LINKS, NEW_MATCH_LINKS
from market import vig_opposite_probability

team_dict = {}
MAPS_FILE = ROOT / "data" / "game_data" / "maps.txt"
AGENTS_FILE = ROOT / "data" / "game_data" / "agents.txt"
DEFAULT_MAPS = ['Ascent', 'Bind', 'Breeze', 'Fracture', 'Haven', 'Icebox', 'Lotus', 'Pearl', 'Split', 'Sunset', 'Abyss']
DEFAULT_AGENTS = [
    "Astra", "Breach", "Brimstone", "Chamber", "Clove", "Cypher", "Deadlock",
    "Fade", "Gekko", "Harbor", "Iso", "Jett", "Kayo", "Killjoy", "Neon",
    "Omen", "Phoenix", "Raze", "Reyna", "Sage", "Skye", "Sova", "Viper", "Yoru",
]
maps_lock = threading.Lock()
agents_lock = threading.Lock()


def _load_namelist(path, default):
    if path.exists():
        with open(path, "r") as f:
            loaded = [line.strip() for line in f if line.strip()]
        if loaded:
            return loaded
    return default.copy()


def load_maps():
    return _load_namelist(MAPS_FILE, DEFAULT_MAPS)


def load_agents():
    return _load_namelist(AGENTS_FILE, DEFAULT_AGENTS)


maps = load_maps()
agents = load_agents()
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

map_headers = [
    "map_id", "t1", "t2", "date", "winner", "map",
    "t1_rds", "t2_rds", "t1_pistols", "t2_pistols",
    "t1_agent1", "t1_agent2", "t1_agent3", "t1_agent4", "t1_agent5",
    "t1_fks", "t1_rating", "t1_acs",
    "t1_kills", "t1_assists", "t1_deaths",
    "t2_agent1", "t2_agent2", "t2_agent3", "t2_agent4", "t2_agent5",
    "t2_fks", "t2_rating", "t2_acs",
    "t2_kills", "t2_assists", "t2_deaths",
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


def _append_namelist(path, name):
    """Persist a newly seen name so future runs assign it the same id."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            with open(path, "r") as f:
                if name in {line.strip() for line in f if line.strip()}:
                    return
        needs_newline = path.exists() and path.stat().st_size > 0
        if needs_newline:
            with open(path, "rb") as f:
                f.seek(-1, 2)
                needs_newline = f.read(1) != b"\n"
        with open(path, "a") as f:
            if needs_newline:
                f.write("\n")
            f.write(f"{name}\n")
    except OSError as e:
        log(f"Could not persist newly discovered name '{name}' to {path}: {e}")


def append_map_name(map_name):
    _append_namelist(MAPS_FILE, map_name)


def append_agent_name(agent_name):
    _append_namelist(AGENTS_FILE, agent_name)


def get_map_index(map_name):
    map_name = map_name.strip()
    with maps_lock:
        if map_name not in maps:
            maps.append(map_name)
            append_map_name(map_name)
            log(f"Discovered new map '{map_name}'; assigned map id {len(maps) - 1}")
        return maps.index(map_name)


def get_agent_index(agent_name):
    if agent_name is None:
        return None
    agent_name = agent_name.strip()
    if not agent_name:
        return None
    with agents_lock:
        if agent_name not in agents:
            agents.append(agent_name)
            append_agent_name(agent_name)
            log(f"Discovered new agent '{agent_name}'; assigned agent id {len(agents) - 1}")
        return agents.index(agent_name)


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
        get_map_index(t1_bans[0]), get_map_index(t1_bans[1]),
        get_map_index(t2_bans[0]), get_map_index(t2_bans[1]),
        get_map_index(t1_picks[0]), get_map_index(t2_picks[0]),
        get_map_index(remaining_map),
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


def _parse_int_text(node):
    if node is None:
        return None
    txt = node.text.strip()
    return int(txt) if txt.isdigit() else None


def parse_map_team_stats(table_body):
    """Per-map per-team aggregation from one player table tbody.

    Returns dict with: agents (sorted list of agent ids, padded to 5),
    fks, rating (avg), acs (avg), kills, deaths, assists.
    Counts are summed across players; rates are averaged across players found.
    """
    played_agents = []
    fks = 0
    rating = 0.0
    acs = 0.0
    kills = 0
    deaths = 0
    assists = 0
    n_players = 0

    for player in table_body.find_all("tr"):
        agent_cell = player.find("td", {"class": "mod-agents"})
        agent_img = agent_cell.find("img") if agent_cell is not None else None
        if agent_img is not None:
            played_agents.append(get_agent_index(agent_img.get("title")))

        stats = player.find_all("td", {"class": "mod-stat"})
        if len(stats) < 5:
            continue
        n_players += 1

        # rating, acs — prefer "both sides" aggregate span
        for total_key, td in (("rating", stats[0]), ("acs", stats[1])):
            span = td.find("span", {"class": "side mod-side mod-both"}) or td.find("span", {"class": "side mod-both"})
            if span is None or not isnum(span.text.strip()):
                continue
            val = float(span.text.strip())
            if total_key == "rating":
                rating += val
            else:
                acs += val

        # K / D / A
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

        # First kills (per map, both sides)
        fb_td = player.find("td", {"class": "mod-stat mod-fb"})
        if fb_td is not None:
            fb_span = fb_td.find("span", {"class": "side mod-both"}) or fb_td.find("span", {"class": "side mod-side mod-both"})
            if fb_span is not None and isnum(fb_span.text.strip()):
                fks += int(float(fb_span.text.strip()))

    # Pad agents list to 5 deterministically
    sorted_agents = sorted(a for a in played_agents if a is not None)
    while len(sorted_agents) < 5:
        sorted_agents.append(None)
    sorted_agents = sorted_agents[:5]

    if n_players == 0:
        return {
            "agents": sorted_agents,
            "fks": None, "rating": None, "acs": None,
            "kills": None, "deaths": None, "assists": None,
        }
    return {
        "agents": sorted_agents,
        "fks": fks,
        "rating": rating / n_players,
        "acs": acs / n_players,
        "kills": kills,
        "deaths": deaths,
        "assists": assists,
    }


def parse_map(map_div, t1, t2):
    """Parse a single vm-stats-game div for one map.

    Returns a dict matching map_headers (minus date/pistols which the caller
    fills in), or None if the div is not a real map (e.g. data-game-id='all').
    """
    try:
        map_id = map_div.get("data-game-id")
        if map_id is None or not map_id.isdigit():
            return None

        header = map_div.find("div", {"class": "vm-stats-game-header"})
        if header is None:
            return None

        # Map name
        map_span = header.find("div", {"class": "map"})
        if map_span is None:
            return None
        name_span = map_span.find("span", {"style": "position: relative;"}) or map_span.find("span")
        if name_span is None:
            return None
        map_name = name_span.text.strip().split("\t")[0].strip()
        # Some VLR rows append " PICK" / " DECIDER" — keep just the map word.
        map_name = map_name.split()[0] if map_name else map_name
        map_idx = get_map_index(map_name)

        # Rounds and winner
        t1_team = header.find("div", {"class": "team"})
        t2_team = header.find("div", {"class": "team mod-right"})
        if t1_team is None or t2_team is None:
            return None
        t1_rds = _parse_int_text(t1_team.find("div", {"class": "score"}))
        t2_rds = _parse_int_text(t2_team.find("div", {"class": "score"}))
        if t1_rds is None or t2_rds is None:
            return None
        winner = bool(t1_team.find("div", {"class": "score mod-win"}))

        # Player stats — first table is t1, second is t2.
        player_tables = map_div.find_all("table", {"class": "wf-table-inset mod-overview"})
        if len(player_tables) < 2:
            return None
        t1_stats = parse_map_team_stats(player_tables[0].find("tbody"))
        t2_stats = parse_map_team_stats(player_tables[1].find("tbody"))

        row = {
            "map_id": int(map_id),
            "t1": t1,
            "t2": t2,
            "date": None,  # filled in by caller
            "winner": winner,
            "map": map_idx,
            "t1_rds": t1_rds,
            "t2_rds": t2_rds,
            "t1_pistols": None,  # filled in by caller if econ available
            "t2_pistols": None,
            "t1_agent1": t1_stats["agents"][0],
            "t1_agent2": t1_stats["agents"][1],
            "t1_agent3": t1_stats["agents"][2],
            "t1_agent4": t1_stats["agents"][3],
            "t1_agent5": t1_stats["agents"][4],
            "t1_fks": t1_stats["fks"],
            "t1_rating": t1_stats["rating"],
            "t1_acs": t1_stats["acs"],
            "t1_kills": t1_stats["kills"],
            "t1_assists": t1_stats["assists"],
            "t1_deaths": t1_stats["deaths"],
            "t2_agent1": t2_stats["agents"][0],
            "t2_agent2": t2_stats["agents"][1],
            "t2_agent3": t2_stats["agents"][2],
            "t2_agent4": t2_stats["agents"][3],
            "t2_agent5": t2_stats["agents"][4],
            "t2_fks": t2_stats["fks"],
            "t2_rating": t2_stats["rating"],
            "t2_acs": t2_stats["acs"],
            "t2_kills": t2_stats["kills"],
            "t2_assists": t2_stats["assists"],
            "t2_deaths": t2_stats["deaths"],
        }
        return row
    except Exception as e:
        log(f"Error parsing map div: {e}")
        traceback.print_exc()
        return None


def parse_econ_pistols(econ_soup, map_id):
    """Pull (t1_pistols, t2_pistols) from the economy-tab soup for one map.

    VLR's econ table renders a stats-sq box for each team showing pistol
    rounds won. Returns (None, None) on any error.
    """
    if econ_soup is None:
        return None, None
    try:
        container = econ_soup.find("div", {"class": "vm-stats-container"})
        if container is None:
            return None, None
        game_div = container.find("div", {"data-game-id": str(map_id)})
        if game_div is None:
            return None, None
        # Filter out "stats not available" placeholders.
        active = container.find("div", {"class": "vm-stats-game mod-active"})
        if active is not None and "Stats from this map are not available yet" in active.get_text():
            return None, None
        table = game_div.find("table")
        if table is None:
            return None, None
        rows = table.find_all("tr")
        if len(rows) < 3:
            return None, None
        t1_tds = rows[1].find_all("td")
        t2_tds = rows[2].find_all("td")
        if len(t1_tds) < 2 or len(t2_tds) < 2:
            return None, None
        t1_box = t1_tds[1].find("div", {"class": "stats-sq"})
        t2_box = t2_tds[1].find("div", {"class": "stats-sq"})
        t1_pistols = _parse_int_text(t1_box)
        t2_pistols = _parse_int_text(t2_box)
        return t1_pistols, t2_pistols
    except Exception as e:
        log(f"Error parsing econ pistols for game {map_id}: {e}")
        return None, None


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

        best_odds = 0
        worst_odds = 1
        for book in soup.find_all("a", {"class": "match-bet-item"}):
            if "mod-noodds" in book.get("class", []):
                continue
            short = book.find("div", {"class": "match-bet-item-return-short"})
            if short is None:
                continue
            odds_span = short.find("span", {"class": "match-bet-item-odds"})
            if odds_span is None:
                continue
            try:
                decimal = float(odds_span.text.strip())
            except ValueError:
                continue
            if decimal <= 1.0:
                continue
            p = 1 / decimal
            if not winner:
                p = vig_opposite_probability(p)
                if p is None:
                    continue
            if 0 < p < 1:
                best_odds = max(best_odds, p)
                worst_odds = min(worst_odds, p)
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
        series_row = pd.Series(data=row, index=series_headers)

        # Per-map rows. Fetch econ tab for pistol counts; econ is best-effort.
        econ_soup = fetch_data(match_link + "/?game=all&tab=economy")
        map_rows = []
        container = soup.find("div", {"class": "vm-stats-container"})
        if container is not None:
            for map_div in container.find_all("div", {"class": "vm-stats-game"}):
                if not isinstance(map_div, Tag):
                    continue
                parsed = parse_map(map_div, t1, t2)
                if parsed is None:
                    continue
                parsed["date"] = date
                t1_pistols, t2_pistols = parse_econ_pistols(econ_soup, parsed["map_id"])
                parsed["t1_pistols"] = t1_pistols
                parsed["t2_pistols"] = t2_pistols
                map_rows.append(pd.Series(data=parsed, index=map_headers))

        return series_row, map_rows

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
            if result is not None:
                series_row, map_rows = result
                if series_row is not None:
                    match_stats_list.append(series_row)
                if map_rows:
                    map_stats_list.extend(map_rows)
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
