import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import DB

_DEFAULT_ELO = 1500
_K = 32


def _expected(r1, r2):
    return 1 / (1 + 10 ** ((r2 - r1) / 400))


def _update(winner_elo, loser_elo):
    wp = _expected(winner_elo, loser_elo)
    lp = _expected(loser_elo, winner_elo)
    return winner_elo + _K * (1 - wp), loser_elo + _K * (0 - lp)


def _run_elo(df):
    ratings = {}
    t1_elos, t2_elos = [], []
    for _, row in df.iterrows():
        t1, t2 = row["t1"], row["t2"]
        r1 = ratings.get(t1, _DEFAULT_ELO)
        r2 = ratings.get(t2, _DEFAULT_ELO)
        t1_elos.append(r1)
        t2_elos.append(r2)
        if row["winner"]:
            ratings[t1], ratings[t2] = _update(r1, r2)
        else:
            ratings[t2], ratings[t1] = _update(r2, r1)
    return t1_elos, t2_elos, ratings


def compute_elo(con=None):
    """Compute Elo ratings for all series and update t1_elo, t2_elo, elo_diff in the series table."""
    close = con is None
    if con is None:
        con = sqlite3.connect(DB)

    df = pd.read_sql(
        "SELECT match_id, t1, t2, winner, date FROM series ORDER BY date ASC", con
    )

    t1_elos, t2_elos, _ = _run_elo(df)
    df["t1_elo"] = t1_elos
    df["t2_elo"] = t2_elos
    df["elo_diff"] = df["t1_elo"] - df["t2_elo"]

    cur = con.cursor()
    for _, row in df.iterrows():
        cur.execute(
            "UPDATE series SET t1_elo=?, t2_elo=?, elo_diff=? WHERE match_id=?",
            (row["t1_elo"], row["t2_elo"], row["elo_diff"], row["match_id"]),
        )
    con.commit()

    if close:
        con.close()

    return df


def get_current_ratings(con=None):
    """Return {team_id: elo} representing each team's rating after their most recent match."""
    close = con is None
    if con is None:
        con = sqlite3.connect(DB)

    df = pd.read_sql(
        "SELECT t1, t2, winner, date FROM series ORDER BY date ASC", con
    )
    _, _, ratings = _run_elo(df)

    if close:
        con.close()

    return ratings


if __name__ == "__main__":
    compute_elo()
    print("Elo ratings computed and saved to the series table.")
