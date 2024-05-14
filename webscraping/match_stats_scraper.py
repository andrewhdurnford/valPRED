import numpy as np, pandas as pd, re, os, csv, traceback, concurrent.futures, pickle, requests, aiohttp, asyncio, warnings, random
from bs4 import BeautifulSoup, Tag
from datetime import datetime
from IPython.display import display

team_dict = {}
maps = ['Ascent', 'Bind', 'Breeze', 'Fracture', 'Haven', 'Icebox', 'Lotus', 'Pearl', 'Split', 'Sunset']
agents = ["Astra", "Breach", "Brimstone", "Chamber", "Clove", "Cypher", "Deadlock", "Fade", "Gekko", "Harbor", "Iso", 
          "Jett", "Kayo", "Killjoy", "Neon", "Omen", "Phoenix", "Raze", "Reyna", "Sage", "Skye", "Sova", "Viper", "Yoru"]
series_headers = ["match_id", "t1", "t2", "t1_ban1", "t1_ban2","t2_ban1", "t2_ban2", "t1_pick", "t2_pick", "remaining", "t1_mapwins", "t2_mapwins", "net_h2h", "t1_past", "t2_past", "date"]
map_headers = [
    "map_id", "t1", "t2", "winner", "map", 
    "t1_agent1", "t1_agent2", "t1_agent3", "t1_agent4", "t1_agent5",
    "t2_agent1", "t2_agent2", "t2_agent3", "t2_agent4", "t2_agent5", 
    "t1_rds", "t1_atk_rds", "t1_def_rds", "t2_rds","t2_atk_rds", "t2_def_rds", 
    "t1_retakes_won", "t1_retakes_lost", "t2_retakes_won", "t2_retakes_lost",
    "t1_postplants_won", "t1_postplants_lost", "t2_postplants_won", "t2_postplants_lost",
    "overtime", "t1_overtime_rds", "t2_overtime_rds",
    "t1_atk_fks", "t1_def_fks", "t2_atk_fks", "t2_def_fks", 
    "t1_pistols", "t2_pistols",  
    "t1_ecos_won", "t1_ecos_lost", "t2_ecos_won", "t2_ecos_lost", 
    "t1_fullbuys_won", "t1_fullbuys_lost", "t2_fullbuys_won", "t2_fullbuys_lost",
    "t1_atk_rating", "t1_def_rating", "t2_atk_rating", "t2_def_rating",
     "t1_avg_acs", "t2_avg_acs",
    "t1_kills", "t1_assists", "t1_deaths", "t1_kast", 
    "t2_kills", "t2_assists", "t2_deaths", "t2_kast",
    "t1_mks", "t1_clutches", "t1_econ", "t2_mks", "t2_clutches", "t2_econ",
    "date"
]
site = "https://www.vlr.gg"
series_df = pd.DataFrame(columns=series_headers)
maps_df = pd.DataFrame(columns=map_headers)
warnings.filterwarnings("ignore", category=FutureWarning)

# Load match links
with open("webscraping/match_links.csv", "r") as f:
    match_links = f.read().splitlines()

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

    return (t1, t2, maps.index(t1_bans[0]), maps.index(t1_bans[1]), maps.index(t2_bans[0]), maps.index(t2_bans[1]), maps.index(t1_picks[0]), maps.index(t2_picks[0]), maps.index(remaining_map))

def parse_econ(input_string):
    matches = input_string.strip("\n\t").split("(")
    matches[0] = matches[0].strip("\n\t")
    matches[1] = matches[1].strip(")\n\t")
    return int(matches[0]), int(matches[1])

def parse_econ_stats(econ, map_id):
    econ_stats = econ.find("div", {"class": "vm-stats-container"}).find("div", {"data-game-id": map_id}).find("div", {"style": "overflow-x: auto;"}).find("table").find_all("tr")
    t1_pistols = int(econ_stats[1].find_all("td")[1].find("div", {"class": "stats-sq"}).text)
    t1_ecos_won, t1_ecos_lost = parse_econ(econ_stats[1].find_all("td")[2].find("div", {"class": "stats-sq"}).text)
    t1_fullbuys_won, t1_fullbuys_lost = parse_econ(econ_stats[1].find_all("td")[4].find("div", {"class": "stats-sq"}).text)
    t2_pistols = int(econ_stats[2].find_all("td")[1].find("div", {"class": "stats-sq"}).text) / 2
    t2_ecos_won, t2_ecos_lost = parse_econ(econ_stats[2].find_all("td")[2].find("div", {"class": "stats-sq"}).text)
    t2_fullbuys_won, t2_fullbuys_lost = parse_econ(econ_stats[2].find_all("td")[4].find("div", {"class": "stats-sq"}).text)
    return [t1_pistols, t2_pistols, t1_ecos_won, t1_ecos_lost, t2_ecos_won, t2_ecos_lost, t1_fullbuys_won, t1_fullbuys_lost, t2_fullbuys_won, t2_fullbuys_lost]

def parse_performance_stats(perf, map_id):
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
    atk_fks = 0
    def_fks = 0
    max_acs = 0
    min_acs = 9999
    avg_acs = 0
    atk_rating = 0
    def_rating = 0
    kills = 0
    deaths = 0
    assists = 0
    kast = 0
    played_agents = []
    for player in player_data.find_all("tr"):
        played_agents.append(player.find("td", {"class": "mod-agents"}).find("img").get("title"))
        stats = player.find_all("td", {"class": "mod-stat"})
        atk_rating += float(stats[0].find("span", {"class": "side mod-side mod-t"}).text)
        def_rating += float(stats[0].find("span", {"class": "side mod-side mod-ct"}).text)
        acs = int(stats[1].find("span", {"class": "side mod-side mod-both"}).text)
        kills += int(stats[2].find("span", {"class": "side mod-side mod-both"}).text)
        deaths += int(stats[3].find("span", {"class": "side mod-both"}).text)
        assists += int(stats[4].find("span", {"class": "side mod-both"}).text)
        kast += int(stats[6].find("span", {"class": "side mod-both"}).text.strip("%"))
        avg_acs += acs
        if acs > max_acs:
            max_acs = acs
        if acs < min_acs:
            min_acs = acs
        fbs = player.find("td", {"class": "mod-stat mod-fb"})
        atk_fks += int(fbs.find("span", {"class": "mod-t"}).text)
        def_fks += int(fbs.find("span", {"class": "mod-ct"}).text)
    played_agents.sort()
    avg_acs = avg_acs / 5
    atk_rating = atk_rating / 5
    def_rating = def_rating / 5
    kast = kast / 500
    played_agents = [agents.index(agent) for agent in played_agents]
    return [played_agents, atk_fks, def_fks, atk_rating, def_rating, avg_acs, kills, assists, deaths, kast]

def parse_map(econ, perf, map, t1, t2):
    try:
        map_id = map.get("data-game-id")
        if map_id.isdigit():

            # find map name
            map_name = map.find("div", {"class": "vm-stats-game-header"}).find("div", {"class": "map"}).find("span", {"style": "position: relative;"}).text.strip().split("\t")[0]

            # find winner and overtime
            t1_overview = map.find("div", {"class": "vm-stats-game-header"}).find("div", {"class": "team"})
            t1_rds = int(t1_overview.find("div", {"class", "score"}).text.strip())
            t2_rds = int(map.find("div", {"class": "vm-stats-game-header"}).find("div", {"class": "team mod-right"}).find("div", {"class": "score"}).text.strip())
            if t1_overview.find("div", {"class": "score mod-win"}):
                winner = t1
                if t1_rds > 13:
                    overtime = True
                else:
                    overtime = False
            else:
                winner = t2
                if t2_rds > 13:
                    overtime = True
                else:
                    overtime = False

            # find round scores
            t1_overview = t1_overview.find_all("div")[1]
            t1_atk_rds = int(t1_overview.find("span", {"class": "mod-t"}).text)
            t1_def_rds = int(t1_overview.find("span", {"class": "mod-ct"}).text)
            t2_overview = map.find("div", {"class": "vm-stats-game-header"}).find("div", {"class": "team mod-right"}).find("div")
            t2_atk_rds = int(t2_overview.find("span", {"class": "mod-t"}).text)
            t2_def_rds = int(t2_overview.find("span", {"class": "mod-ct"}).text)

            # find overtime rounds
            if overtime:
                t1_overtime_rds = int(t1_overview.find("span", {"class": "mod-ot"}).text)
                t2_overtime_rds = int(t2_overview.find("span", {"class": "mod-ot"}).text)
            else:
                t1_overtime_rds = None
                t2_overtime_rds = None
            
            player_stats = map.find_all("table", {"class": "wf-table-inset mod-overview"})
            t1_stats = parse_player_stats(player_stats[0].find("tbody")) # agents, atk_fks, def_fks, atk_rating, def_rating, avg_acs, kills, assists, deaths, kast
            t2_stats = parse_player_stats(player_stats[1].find("tbody"))
            econ_stats = parse_econ_stats(econ, map_id) # t1_pistols, t2_pistols, t1_ecos_won, t1_ecos_lost, t2_ecos_won, t2_ecos_lost, t1_fullbuys_won, t1_fullbuys_lost, t2_fullbuys_won, t2_fullbuys_lost
            performance_stats = parse_performance_stats(perf, map_id) # t1_mks, t1_clutches, t1_econ, t2_mks, t2_clutches, t2_econ,6 t1_plants,7 t1_defuses,8 t2_plants,9 t2_defuses
            return  [
                        int(map_id), t1, t2, winner, maps.index(map_name), 
                        t1_stats[0][0], t1_stats[0][1], t1_stats[0][2], t1_stats[0][3], t1_stats[0][4],
                        t2_stats[0][0], t2_stats[0][1], t2_stats[0][2], t2_stats[0][3], t2_stats[0][4],
                        t1_rds, t1_atk_rds, t1_def_rds, t2_rds, t2_atk_rds, t2_def_rds, 
                        performance_stats[7], (performance_stats[8] - performance_stats[7]), performance_stats[9], (performance_stats[6] - performance_stats[9]), 
                        (performance_stats[6] - performance_stats[9]), performance_stats[9], (performance_stats[8]-performance_stats[7]), performance_stats[7], 
                        overtime, t1_overtime_rds, t2_overtime_rds,
                        t1_stats[1], t1_stats[2], t2_stats[1], t2_stats[2],
                        econ_stats[0], econ_stats[1], 
                        econ_stats[2], econ_stats[3], econ_stats[4], econ_stats[5], 
                        econ_stats[6], econ_stats[7], econ_stats[8], econ_stats[9],
                        t1_stats[3], t1_stats[4], t2_stats[3], t2_stats[4],
                        t1_stats[5], t2_stats[5],
                        t1_stats[6], t1_stats[7], t1_stats[8], t1_stats[9],
                        t2_stats[6], t2_stats[7], t2_stats[8], t2_stats[9],
                        performance_stats[0], performance_stats[1], performance_stats[2], 
                        performance_stats[3], performance_stats[4], performance_stats[5]
                    ]
    except Exception as e:
        print(f"Error parsing map: {str(e)}")
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

def process_match_link(link):
    print(f"Processing match {match_links.index(link)} out of {len(match_links)} ({round(match_links.index(link)/len(match_links)* 100, 2) }%)")
    match_id = re.search(r"/(\d+)/", link).group(1)
    match_link = site + link
    try:
        urls = [
            match_link,
            f"{match_link}/?game=all&tab=performance",
            f"{match_link}/?game=all&tab=economy"
        ]
        task_to_url = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            tasks = []
            for url in urls:
                task = executor.submit(fetch_data, url)
                tasks.append(task)
                task_to_url[task] = url

            results = []
            for future in concurrent.futures.as_completed(tasks):
                url = task_to_url[future]
                result = future.result()
                results.append((url, result))

            results.sort(key=lambda x: urls.index(x[0]))
            soup = results[0][1]
            perf = results[1][1]
            econ = results[2][1]
        
            date = soup.find("div", {"class": "moment-tz-convert"}).get("data-utc-ts").split()[0]
            t1 = get_team(soup.find("a", {"class": "match-header-link wf-link-hover mod-1"}).get("href"))
            t2 = get_team(soup.find("a", {"class": "match-header-link wf-link-hover mod-2"}).get("href"))
            score = soup.find("div", {"class": "match-header-vs-score"}).find("div", {"class": "js-spoiler"}).text.split(":")
            score[0] = int(score[0].strip()) if score[0].strip().isdigit() else None
            score[1] = int(score[1].strip()) if score[1].strip().isdigit() else None
            h2h = parse_h2h(soup.find("div", {"class": "match-h2h-matches"}))
            t1_past = parse_history(soup.find_all("div", {"class": "match-histories"})[0]) if len(soup.find_all("div", {"class": "match-histories"})) > 0 else None
            t2_past = parse_history(soup.find_all("div", {"class": "match-histories"})[1]) if len(soup.find_all("div", {"class": "match-histories"})) > 0 else None

            vetos = []
            match_df = pd.DataFrame()  # Define empty DataFrame
            if soup.find("div", {"class": "match-header-note"}):
                vetos = parse_vetos(t1, t2, soup.find("div", {"class": "match-header-note"}).text)
                if vetos != "no vetos":
                    vetos_data = [match_id] + list(vetos) + [score[0], score[1], h2h, t1_past, t2_past, date]
                    match_df = pd.DataFrame([vetos_data], columns=series_headers)

            map_stats_list = []
            for map in soup.find("div", {"class": "vm-stats-container"}).find_all("div", {"class": "vm-stats-game"}):
                if isinstance(map, Tag):  # Check if the child is a Tag
                    map_stats = parse_map(econ, perf, map, t1, t2)
                    if map_stats is not None:
                        map_stats.append(date)
                        map_stats_list.append(map_stats)
            
            map_stats_df = pd.DataFrame(map_stats_list, columns=map_headers)
            return match_df, map_stats_df
    except Exception as e:
        print(f"Error processing match {match_id}: {str(e)}")
        traceback.print_exc()
        return None, None

def fetch_data(url):
    response = requests.get(url)
    return BeautifulSoup(response.text, "html.parser")

def process_all_matches():
    global series_df, maps_df
    # async with aiohttp.ClientSession() as session:
    #     for link in match_links:
    #         tasks = []
    #         url = f"{site}{link}"
    #         tasks.append(fetch_data(session, url))
    #         tasks.append(fetch_data(session, f"{url}/?game=all&tab=performance"))
    #         tasks.append(fetch_data(session, f"{url}/?game=all&tab=economy"))
    #         results = await asyncio.gather(*tasks)  # Wait for all three tasks to complete

    #         match_soup = BeautifulSoup(results[0], "html.parser")
    #         performance_soup = BeautifulSoup(results[1], "html.parser")
    #         economy_soup = BeautifulSoup(results[2], "html.parser")

    #         if not match_soup:
    #             continue
    #         match_id = re.search(r"/(\d+)/", link).group(1)
    #         date = match_soup.find("div", {"class": "moment-tz-convert"}).get("data-utc-ts").split()[0]
    #         t1 = get_team(match_soup.find("a", {"class": "match-header-link wf-link-hover mod-1"}).get("href"))
    #         t2 = get_team(match_soup.find("a", {"class": "match-header-link wf-link-hover mod-2"}).get("href"))
    #         score = match_soup.find("div", {"class": "match-header-vs-score"}).find("div", {"class": "js-spoiler"}).text.split(":")
    #         score = [s.strip() for s in score]
    #         h2h = parse_h2h(match_soup.find("div", {"class": "match-h2h-matches"}))
    #         t1_past = parse_history(match_soup.find_all("div", {"class": "match-histories"})[0]) if len(match_soup.find_all("div", {"class": "match-histories"})) > 0 else None
    #         t2_past = parse_history(match_soup.find_all("div", {"class": "match-histories"})[1]) if len(match_soup.find_all("div", {"class": "match-histories"})) > 0 else None

    #         vetos = []
    #         match_df = pd.DataFrame()
    #         if match_soup.find("div", {"class": "match-header-note"}):
    #             vetos = parse_vetos(t1, t2, match_soup.find("div", {"class": "match-header-note"}).text)
    #             if vetos != "no vetos":
    #                 vetos_data = [match_id] + vetos + [score[0], score[1], h2h, t1_past, t2_past, date]
    #                 print(vetos_data)
    #                 match_df = pd.DataFrame([vetos_data], columns=series_headers)
    #             else:
    #                 print(match_id)
            
    #         map_stats_list = []
    #         for map in match_soup.find("div", {"class": "vm-stats-container"}).find_all("div", {"class": "vm-stats-game"}):
    #             if isinstance(map, Tag):  # Check if the child is a Tag
    #                 map_stats = parse_map(map, performance_soup, economy_soup, t1, t2)
    #                 if map_stats is not None:
    #                     map_stats.append(date)
    #                     map_stats_list.append(map_stats)
    #         map_stats_df = pd.DataFrame(map_stats_list, columns=map_headers)
    #         if match_df is not None and not match_df.empty:   
    #             if series_df.empty:
    #                 series_df = match_df
    #             elif not series_df.empty:
    #                 series_df = pd.concat([series_df, match_df], ignore_index=True, sort=False)
    #         if map_stats_df is not None and not map_stats_df.empty:
    #             if maps_df.empty:
    #                 maps_df = map_stats_df
    #             elif not maps_df.empty:
    #                 maps_df = pd.concat([maps_df, map_stats_df], ignore_index=True, sort=False)

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(process_match_link, link) for link in match_links]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                match_df, map_stats_df = result
                if match_df is not None and not match_df.empty:   
                    series_df = (match_df.copy() if series_df.empty else pd.concat([series_df, match_df], ignore_index=True, sort=False))
                if map_stats_df is not None and not map_stats_df.empty:
                    maps_df = (map_stats_df.copy() if maps_df.empty else pd.concat([maps_df, map_stats_df], ignore_index=True, sort=False))

    # Save processed data
    series_df.drop_duplicates(subset='match_id', keep='first').to_csv('data/series.csv', index=False)
    maps_df.drop_duplicates(subset='map_id', keep='first').to_csv('data/maps.csv', index=False)
    with open('data/teams.csv', 'w') as csv_file:  
        writer = csv.writer(csv_file)
        for key, value in team_dict.items():
            writer.writerow([key, value])

def main():
    process_all_matches()

main()
