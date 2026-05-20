import sqlite3
from paths import DB

def init_db():
    con = sqlite3.connect(DB)
    cur = con.cursor()

    cur.executescript("""
        DROP TABLE IF EXISTS mapdata;
        DROP TABLE IF EXISTS vetos;

        CREATE TABLE IF NOT EXISTS series (
            match_id TEXT PRIMARY KEY,
            t1 INTEGER, t2 INTEGER, winner INTEGER,
            t1_ban1 INTEGER, t1_ban2 INTEGER, t2_ban1 INTEGER, t2_ban2 INTEGER,
            t1_pick INTEGER, t2_pick INTEGER, remaining INTEGER,
            t1_mapwins INTEGER, t2_mapwins INTEGER,
            net_h2h REAL, t1_past REAL, t2_past REAL,
            odds REAL, best_odds REAL, worst_odds REAL, date TEXT,
            t1_elo REAL, t2_elo REAL, elo_diff REAL,
            t1_fks INTEGER, t1_fds INTEGER, t1_rating REAL, t1_acs REAL,
            t1_kills INTEGER, t1_deaths INTEGER, t1_assists INTEGER,
            t2_fks INTEGER, t2_fds INTEGER, t2_rating REAL, t2_acs REAL,
            t2_kills INTEGER, t2_deaths INTEGER, t2_assists INTEGER
        );
        CREATE TABLE IF NOT EXISTS maps (
            map_id INTEGER PRIMARY KEY,
            t1 INTEGER, t2 INTEGER, date TEXT, winner INTEGER, map INTEGER,
            t1_rds INTEGER, t2_rds INTEGER, t1_pistols INTEGER, t2_pistols INTEGER,
            t1_agent1 INTEGER, t1_agent2 INTEGER, t1_agent3 INTEGER, t1_agent4 INTEGER, t1_agent5 INTEGER,
            t1_fks INTEGER, t1_rating REAL, t1_acs REAL, t1_kills INTEGER,
            t1_assists INTEGER, t1_deaths INTEGER,
            t2_agent1 INTEGER, t2_agent2 INTEGER, t2_agent3 INTEGER, t2_agent4 INTEGER, t2_agent5 INTEGER,
            t2_fks INTEGER, t2_rating REAL, t2_acs REAL, t2_kills INTEGER,
            t2_assists INTEGER, t2_deaths INTEGER
        );
        CREATE TABLE IF NOT EXISTS upcoming (
            match_id TEXT PRIMARY KEY,
            t1 INTEGER, t2 INTEGER, date TEXT,
            net_h2h REAL, t1_past REAL, t2_past REAL,
            t1_odds REAL, t2_odds REAL
        );
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER, linkname TEXT, fullname TEXT, abbrev TEXT, secondary_id TEXT,
            region TEXT,
            PRIMARY KEY (id, region)
        );
        CREATE TABLE IF NOT EXISTS results (
            match_id TEXT, t1 TEXT, t2 TEXT, date TEXT,
            "pred_win%" REAL, t1_odds REAL, t2_odds REAL,
            ev_t1 REAL, ev_t2 REAL, bet TEXT
        );
    """)

    cur.execute("PRAGMA table_info(maps)")
    existing_map_cols = {row[1] for row in cur.fetchall()}
    for col in (
        "t1_agent1", "t1_agent2", "t1_agent3", "t1_agent4", "t1_agent5",
        "t2_agent1", "t2_agent2", "t2_agent3", "t2_agent4", "t2_agent5",
    ):
        if col not in existing_map_cols:
            cur.execute(f"ALTER TABLE maps ADD COLUMN {col} INTEGER")

    # Add team-aggregate columns to series if migrating an existing DB.
    cur.execute("PRAGMA table_info(series)")
    existing_series_cols = {row[1] for row in cur.fetchall()}
    new_series_cols = [
        ("t1_fks", "INTEGER"), ("t1_fds", "INTEGER"),
        ("t1_rating", "REAL"), ("t1_acs", "REAL"),
        ("t1_kills", "INTEGER"), ("t1_deaths", "INTEGER"), ("t1_assists", "INTEGER"),
        ("t2_fks", "INTEGER"), ("t2_fds", "INTEGER"),
        ("t2_rating", "REAL"), ("t2_acs", "REAL"),
        ("t2_kills", "INTEGER"), ("t2_deaths", "INTEGER"), ("t2_assists", "INTEGER"),
    ]
    for col, coltype in new_series_cols:
        if col not in existing_series_cols:
            cur.execute(f"ALTER TABLE series ADD COLUMN {col} {coltype}")

    con.commit()
    con.close()
    print(f"Database initialized at {DB}")

if __name__ == "__main__":
    init_db()
