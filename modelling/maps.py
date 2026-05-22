"""Map-level rolling feature engineering.

Mirrors the leakage-safe pattern in ``series.py::compute_rolling_features``
but at the per-map granularity. For each map row, computes diff features
that reflect each team's history *prior to* the map's date — both on the
specific map and across all maps as a fallback when specific-map sample is
thin.

Required columns on the input df (typically the ``maps`` table):

    map_id, t1, t2, date, winner, map, t1_rds, t2_rds,
    t1_pistols, t2_pistols,
    t1_fks, t2_fks, t1_rating, t2_rating, t1_acs, t2_acs,
    t1_kills, t2_kills, t1_assists, t2_assists, t1_deaths, t2_deaths

Output columns added (all distinct from series-level feature names so the
two can be merged side-by-side):

    map_wr_diff         — team-on-map rolling win rate diff
    round_share_diff    — team-on-map rolling round share diff
    pistol_diff         — team-on-map rolling pistol win rate diff
    map_rating_diff     — team-on-map rolling combat rating diff
    map_acs_diff        — team-on-map rolling ACS diff
    map_fk_net_diff     — team-on-map rolling (FK − FD) per-round diff
    map_kpm_diff        — team-on-map rolling kills per round diff
    map_dpm_diff        — team-on-map rolling deaths per round diff
    sample_size_diff    — diff in count of prior maps on this map between
                          the two teams (clipped at ``window``)

Fallback hierarchy (applied per feature, per side):

    1. Team-on-this-specific-map rolling mean (if >= ``min_periods`` priors).
    2. Team-on-all-maps rolling mean (if >= ``min_periods`` priors).
    3. NaN (caller can drop or impute).

Leakage rules:

- All rolling windows use ``closed='left'``: a map's own stats never feed
  its own feature.
- Maps are ordered chronologically within each rolling group.
- The function does not filter input — pass the full historical map set
  (Tier 1 + Tier 2) so a team's window is warm by the time they enter the
  modelling-eligible slice. Filter downstream.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


MAP_ROLLING_FEATURES = [
    "map_wr_diff",
    "round_share_diff",
    "pistol_diff",
    "map_rating_diff",
    "map_acs_diff",
    "map_fk_net_diff",
    "map_kpm_diff",
    "map_dpm_diff",
    "sample_size_diff",
]


def _team_long(df, prefix, total_rds):
    """Flatten one side of each map row into a per-team-per-map record."""
    win = (df["winner"].astype("Int64") == (0 if prefix == "t1" else 1)).astype("Int64")
    rds = df[f"{prefix}_rds"].astype(float)
    pistols = df[f"{prefix}_pistols"].astype(float)
    return pd.DataFrame({
        "map_id": df["map_id"].values,
        "date": df["date"].values,
        "team": df[prefix].values,
        "map": df["map"].values,
        "rating": df[f"{prefix}_rating"].astype(float).values,
        "acs": df[f"{prefix}_acs"].astype(float).values,
        # Per-round normalisation so 13-7 and 13-11 OT maps stay comparable.
        "kpr": (df[f"{prefix}_kills"].astype(float) / total_rds).values,
        "dpr": (df[f"{prefix}_deaths"].astype(float) / total_rds).values,
        "fkpr": (df[f"{prefix}_fks"].astype(float) / total_rds).values,
        # Opposing side first kills are this side's first deaths.
        "fdpr": (df[f"{'t2' if prefix == 't1' else 't1'}_fks"].astype(float) / total_rds).values,
        "round_share": (rds / total_rds).values,
        # Pistols are best-effort (None when econ tab couldn't be parsed).
        # Each map has 2 pistol rounds, so per-map pistol win rate is /2.
        "pistol_wr": (pistols / 2.0).values,
        "win": win.values,
    })


def _rolling_mean(long, group_cols, metric_cols, window, min_periods):
    """Apply ``rolling(window, closed='left').mean()`` within each group.

    Returns a frame aligned with ``long``'s index (after sorting) with one
    column per metric prefixed with ``r_`` plus a ``r_count`` column giving
    the rolling sample size (so the caller can drive fallback logic).
    """
    long = long.sort_values(group_cols + ["date"], kind="stable").reset_index(drop=True)
    rolled = (
        long.groupby(group_cols, group_keys=False)[metric_cols]
        .apply(lambda g: g.rolling(window=window, min_periods=min_periods, closed="left").mean())
    )
    rolled.columns = [f"r_{c}" for c in metric_cols]
    # Sample size (n of prior rows that actually entered the mean).
    # Use any metric that is never NaN in input — but rating/acs can be NaN
    # for old rows; count on "win" which is always 0/1.
    counts = (
        long.groupby(group_cols, group_keys=False)["win"]
        .apply(lambda g: g.rolling(window=window, min_periods=1, closed="left").count())
    )
    rolled["r_count"] = counts.values
    return pd.concat([long.reset_index(drop=True), rolled.reset_index(drop=True)], axis=1)


def compute_map_rolling_features(df, window=10, min_periods=3):
    """Append leakage-safe per-map rolling diff features.

    See module docstring for column contract and fallback semantics.
    """
    df = df.copy()
    df["map_id"] = df["map_id"].astype("int64")
    df["date"] = pd.to_datetime(df["date"])

    total_rds = (df["t1_rds"].astype(float) + df["t2_rds"].astype(float))
    # Guard divide-by-zero on malformed rows.
    total_rds = total_rds.where(total_rds > 0)

    long = pd.concat(
        [_team_long(df, "t1", total_rds), _team_long(df, "t2", total_rds)],
        ignore_index=True,
    )

    metric_cols = [
        "rating", "acs", "kpr", "dpr", "fkpr", "fdpr",
        "round_share", "pistol_wr", "win",
    ]

    # Team × map rolling (specific-map history).
    by_team_map = _rolling_mean(
        long.copy(), ["team", "map"], metric_cols, window, min_periods
    )[["map_id", "team", "map"] + [f"r_{c}" for c in metric_cols] + ["r_count"]]
    by_team_map = by_team_map.rename(columns={
        **{f"r_{c}": f"sm_{c}" for c in metric_cols},
        "r_count": "sm_count",
    })

    # Team-overall rolling (fallback when specific-map sample is too thin).
    # Wider window since it pools all maps and we want a stable fallback.
    by_team = _rolling_mean(
        long.copy(), ["team"], metric_cols, window=window * 3, min_periods=min_periods
    )[["map_id", "team"] + [f"r_{c}" for c in metric_cols] + ["r_count"]]
    by_team = by_team.rename(columns={
        **{f"r_{c}": f"all_{c}" for c in metric_cols},
        "r_count": "all_count",
    })

    # Merge both onto the long frame.
    long_feat = long.merge(by_team_map, on=["map_id", "team", "map"], how="left")
    long_feat = long_feat.merge(by_team, on=["map_id", "team"], how="left")

    # Per-feature fallback: use specific-map stat if its sample >= min_periods,
    # else the all-maps stat (which also requires >= min_periods to be present).
    use_specific = long_feat["sm_count"].fillna(0) >= min_periods
    for c in metric_cols:
        long_feat[f"f_{c}"] = np.where(use_specific, long_feat[f"sm_{c}"], long_feat[f"all_{c}"])

    keep_cols = ["map_id", "team"] + [f"f_{c}" for c in metric_cols] + ["sm_count"]
    long_feat = long_feat[keep_cols]

    # Split back into t1 / t2 sides and merge onto df.
    t1_feat = long_feat.rename(columns={
        "team": "t1",
        "sm_count": "t1_sm_count",
        **{f"f_{c}": f"t1_f_{c}" for c in metric_cols},
    })
    t2_feat = long_feat.rename(columns={
        "team": "t2",
        "sm_count": "t2_sm_count",
        **{f"f_{c}": f"t2_f_{c}" for c in metric_cols},
    })
    df = df.merge(t1_feat, on=["map_id", "t1"], how="left")
    df = df.merge(t2_feat, on=["map_id", "t2"], how="left")

    df["map_wr_diff"] = df["t1_f_win"] - df["t2_f_win"]
    df["round_share_diff"] = df["t1_f_round_share"] - df["t2_f_round_share"]
    df["pistol_diff"] = df["t1_f_pistol_wr"] - df["t2_f_pistol_wr"]
    df["map_rating_diff"] = df["t1_f_rating"] - df["t2_f_rating"]
    df["map_acs_diff"] = df["t1_f_acs"] - df["t2_f_acs"]
    df["map_fk_net_diff"] = (
        (df["t1_f_fkpr"] - df["t1_f_fdpr"]) - (df["t2_f_fkpr"] - df["t2_f_fdpr"])
    )
    df["map_kpm_diff"] = df["t1_f_kpr"] - df["t2_f_kpr"]
    df["map_dpm_diff"] = df["t1_f_dpr"] - df["t2_f_dpr"]
    # Clip per-side counts at the rolling window so the diff is bounded and
    # doesn't blow up for teams with very long specific-map histories.
    t1_n = df["t1_sm_count"].fillna(0).clip(upper=window)
    t2_n = df["t2_sm_count"].fillna(0).clip(upper=window)
    df["sample_size_diff"] = t1_n - t2_n

    return df


# ---------------------------------------------------------------------------
# Phase 3: candidate map rows per series
# ---------------------------------------------------------------------------

MAP_POOL_FILE = ROOT / "data" / "game_data" / "map_pool.txt"
MAX_MAP_ID = 11  # 12 maps known to maps.txt (Ascent .. Corrode)


CANDIDATE_FEATURES = [
    # Per-side pre-match rates from prior series with known vetos.
    "t1_pick_rate", "t2_pick_rate",
    "t1_ban_rate",  "t2_ban_rate",
    "t1_play_rate", "t2_play_rate",
    # H2H per-map history prior to this series.
    "h2h_map_count", "h2h_map_t1_winrate",
    # Per-team rolling map stats prior to this series (specific-map with
    # all-maps fallback, mirroring compute_map_rolling_features).
    "map_wr_diff", "round_share_diff", "pistol_diff",
    "map_rating_diff", "map_acs_diff", "map_fk_net_diff",
    "map_kpm_diff", "map_dpm_diff", "sample_size_diff",
]


def _load_map_pool_entries(path=MAP_POOL_FILE):
    """Parse map_pool.txt.

    File format (confirmed against git history): one line per pool-change
    event, ``YYYY-MM-DD;<comma-separated out-of-pool ids>``. The listed ids
    are the maps NOT in the active competitive pool at that date. Active
    pool at a given date = {0..MAX_MAP_ID} minus the listed ids from the
    most recent entry on or before the date.

    Returns a list of ``(date: pd.Timestamp, out_ids: set[int])`` sorted
    ascending by date. Returns an empty list if the file is missing.
    """
    if not Path(path).exists():
        return []
    entries = []
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        date_str, ids_str = line.split(";")
        date = pd.Timestamp(date_str)
        ids = {int(x) for x in ids_str.split(",") if x.strip() != ""}
        entries.append((date, ids))
    entries.sort(key=lambda x: x[0])
    return entries


def _active_pool_for_date(date, entries, max_map_id=MAX_MAP_ID):
    """Active map pool for a given date as a set of map ids.

    Uses the most recent pool entry with ``entry_date <= date``. If the date
    precedes all entries (e.g. 2021-2022 series before the first 2023 entry),
    falls back to the earliest entry's pool — better than empty.
    """
    if not entries:
        return set(range(max_map_id + 1))
    chosen = entries[0]
    for e in entries:
        if e[0] <= date:
            chosen = e
        else:
            break
    return set(range(max_map_id + 1)) - chosen[1]


def _veto_long(series_df):
    """Reshape veto columns into long format: one row per (series, team, map, action).

    Output columns: match_id, date, team, map, action ∈ {'pick','ban','remaining'}.
    Series without veto data are dropped — they cannot contribute to pick/ban rates.
    """
    s = series_df[series_df["t1_ban1"].notna()].copy()
    if s.empty:
        return pd.DataFrame(columns=["match_id", "date", "team", "map", "action"])
    s["date"] = pd.to_datetime(s["date"])
    rows = []
    for col, team_col, action in [
        ("t1_ban1", "t1", "ban"), ("t1_ban2", "t1", "ban"),
        ("t2_ban1", "t2", "ban"), ("t2_ban2", "t2", "ban"),
        ("t1_pick", "t1", "pick"),
        ("t2_pick", "t2", "pick"),
    ]:
        part = pd.DataFrame({
            "match_id": s["match_id"].values,
            "date": s["date"].values,
            "team": s[team_col].values,
            "map": s[col].values,
            "action": action,
        })
        rows.append(part)
    # 'remaining' (decider) — count it as a participation event for both teams.
    for team_col in ["t1", "t2"]:
        rows.append(pd.DataFrame({
            "match_id": s["match_id"].values,
            "date": s["date"].values,
            "team": s[team_col].values,
            "map": s["remaining"].values,
            "action": "remaining",
        }))
    long = pd.concat(rows, ignore_index=True)
    long = long.dropna(subset=["map"])
    long["map"] = long["map"].astype(int)
    return long


def _prematch_veto_rates(veto_long, queries):
    """Compute per-team pre-match pick / ban / play rates from prior vetos.

    queries: DataFrame with columns ``team``, ``map``, ``as_of_date`` (one
    row per (team, map) lookup at a given date).
    Returns the queries frame with ``pick_rate``, ``ban_rate``, ``play_rate``,
    and ``veto_history_n`` columns. Rates are over prior series in which the
    team had a known veto record (denominator = team's total prior known-veto
    series, NOT just appearances of the map). NaN when ``veto_history_n == 0``.

    A series counts toward "play" if the team picked the map, the opponent
    picked it, or it was the decider — i.e., any non-ban appearance. (Bans
    exclude the map from play in BO3/BO5.)
    """
    if veto_long.empty:
        out = queries.copy()
        for c in ["pick_rate", "ban_rate", "play_rate", "veto_history_n"]:
            out[c] = np.nan
        return out

    # Each (match_id, team) is one known-veto series. Per (team, map) we want
    # to count, over prior series of that team, how often the team picked /
    # banned / played that map.
    vl = veto_long.copy().sort_values("date", kind="stable").reset_index(drop=True)
    vl["picked"] = (vl["action"] == "pick").astype(int)
    vl["banned"] = (vl["action"] == "ban").astype(int)
    vl["played"] = vl["action"].isin(["pick", "remaining"]).astype(int)
    # Account for opponent-picked maps: a team "plays" a map when the OTHER
    # team picks it too. We need the opponent rows. The structure of
    # _veto_long already includes a row per team for the 'remaining' map but
    # not for opponent-picked maps from that team's perspective. Build it.
    opp = vl[vl["action"] == "pick"].copy()
    # For each opp pick, the other side also plays this map. Join via match_id
    # to get the other team in the series.
    pairs = veto_long[["match_id", "team"]].drop_duplicates()
    opp_pairs = opp.merge(pairs, on="match_id", suffixes=("", "_other"))
    opp_rows = opp_pairs[opp_pairs["team"] != opp_pairs["team_other"]].copy()
    opp_rows = opp_rows.rename(columns={"team": "_picker", "team_other": "team"})
    opp_rows["picked"] = 0
    opp_rows["banned"] = 0
    opp_rows["played"] = 1
    opp_rows["action"] = "opp_pick"
    opp_rows = opp_rows[["match_id", "date", "team", "map", "action",
                          "picked", "banned", "played"]]
    full = pd.concat([vl, opp_rows], ignore_index=True)
    full = full.sort_values("date", kind="stable").reset_index(drop=True)

    # Per (team, map) cumulative event counts up to BUT NOT INCLUDING the
    # query date. Use merge_asof.
    per_team_map = (
        full.groupby(["team", "map"], group_keys=False)
        .apply(lambda g: g.assign(
            cum_picked=g["picked"].cumsum().shift(1).fillna(0),
            cum_banned=g["banned"].cumsum().shift(1).fillna(0),
            cum_played=g["played"].cumsum().shift(1).fillna(0),
        ))
        [["date", "team", "map", "cum_picked", "cum_banned", "cum_played"]]
        .sort_values("date", kind="stable")
        .reset_index(drop=True)
    )

    # Per-team total prior series count (denominator). Each match contributes
    # exactly one row per team in the known-veto sample.
    per_team_series = (
        veto_long.drop_duplicates(["match_id", "team"])
        .sort_values("date", kind="stable")
        .reset_index(drop=True)
    )
    per_team_series["cum_series"] = (
        per_team_series.groupby("team").cumcount()  # 0..n-1, so prior count
    )
    per_team_series = per_team_series[["date", "team", "cum_series"]]

    q = queries.copy()
    q["as_of_date"] = pd.to_datetime(q["as_of_date"])
    # Preserve caller's row order — merge_asof requires sorting internally.
    q["_orig_idx"] = np.arange(len(q))
    q = q.sort_values("as_of_date", kind="stable").reset_index(drop=True)

    # merge_asof requires sorted-by-on. Match by (team, map) backward.
    per_team_map = per_team_map.sort_values("date", kind="stable")
    merged = pd.merge_asof(
        q, per_team_map,
        left_on="as_of_date", right_on="date",
        by=["team", "map"], direction="backward", allow_exact_matches=False,
    )
    merged = merged.drop(columns=["date"])
    per_team_series = per_team_series.sort_values("date", kind="stable")
    merged = pd.merge_asof(
        merged.sort_values("as_of_date", kind="stable"),
        per_team_series,
        left_on="as_of_date", right_on="date",
        by="team", direction="backward", allow_exact_matches=False,
    )

    merged["cum_picked"] = merged["cum_picked"].fillna(0)
    merged["cum_banned"] = merged["cum_banned"].fillna(0)
    merged["cum_played"] = merged["cum_played"].fillna(0)
    merged["veto_history_n"] = merged["cum_series"].fillna(0).astype(int)
    denom = merged["veto_history_n"].where(merged["veto_history_n"] > 0)
    merged["pick_rate"] = merged["cum_picked"] / denom
    merged["ban_rate"] = merged["cum_banned"] / denom
    merged["play_rate"] = merged["cum_played"] / denom

    # Restore caller's row order.
    merged = merged.sort_values("_orig_idx", kind="stable").reset_index(drop=True)
    return merged[[
        "team", "map", "as_of_date",
        "pick_rate", "ban_rate", "play_rate", "veto_history_n",
    ]]


def _h2h_map_history(maps_df, queries):
    """Pre-match head-to-head per-map record between two teams.

    queries: DataFrame with ``t1``, ``t2``, ``map``, ``as_of_date``.
    Returns queries with ``h2h_map_count`` (prior maps between the two teams
    on this specific map) and ``h2h_map_t1_winrate`` (t1's prior win rate on
    those maps; NaN when count is 0).
    Order-invariant: a prior map between B and A counts the same as A vs B.
    """
    if maps_df.empty:
        out = queries.copy()
        out["h2h_map_count"] = 0
        out["h2h_map_t1_winrate"] = np.nan
        return out

    m = maps_df[["map_id", "t1", "t2", "date", "winner", "map"]].copy()
    m["date"] = pd.to_datetime(m["date"])
    # Canonicalise pair so (A,B) and (B,A) align. Track t1_won_canonical:
    # 1 if the lower-id team won, 0 otherwise.
    lo = np.minimum(m["t1"], m["t2"])
    hi = np.maximum(m["t1"], m["t2"])
    lo_is_t1 = (m["t1"] == lo)
    # winner 0 = t1 won, 1 = t2 won
    lo_won = np.where(
        lo_is_t1,
        (m["winner"].astype("Int64") == 0).astype(float),
        (m["winner"].astype("Int64") == 1).astype(float),
    )
    pair = pd.DataFrame({
        "lo": lo.values, "hi": hi.values,
        "map": m["map"].values, "date": m["date"].values,
        "lo_won": lo_won,
    }).sort_values("date", kind="stable").reset_index(drop=True)

    pair["cum_n"] = pair.groupby(["lo", "hi", "map"]).cumcount()
    pair["cum_lo_wins_incl"] = pair.groupby(["lo", "hi", "map"])["lo_won"].cumsum()
    # Shift to exclude current row (prior-only).
    pair["cum_lo_wins"] = (
        pair.groupby(["lo", "hi", "map"])["cum_lo_wins_incl"].shift(1).fillna(0)
    )
    pair_lookup = pair[["lo", "hi", "map", "date", "cum_n", "cum_lo_wins"]]

    q = queries.copy()
    q["as_of_date"] = pd.to_datetime(q["as_of_date"])
    q["lo"] = np.minimum(q["t1"], q["t2"])
    q["hi"] = np.maximum(q["t1"], q["t2"])
    q["t1_is_lo"] = (q["t1"] == q["lo"])
    q["_orig_idx"] = np.arange(len(q))
    q = q.sort_values("as_of_date", kind="stable").reset_index(drop=True)
    pair_lookup = pair_lookup.sort_values("date", kind="stable")

    merged = pd.merge_asof(
        q, pair_lookup,
        left_on="as_of_date", right_on="date",
        by=["lo", "hi", "map"], direction="backward", allow_exact_matches=False,
    )
    merged["h2h_map_count"] = merged["cum_n"].fillna(0).astype(int)
    lo_wr = merged["cum_lo_wins"] / merged["h2h_map_count"].where(merged["h2h_map_count"] > 0)
    merged["h2h_map_t1_winrate"] = np.where(
        merged["t1_is_lo"], lo_wr, 1.0 - lo_wr
    )
    # Restore caller's row order.
    merged = merged.sort_values("_orig_idx", kind="stable").reset_index(drop=True)
    return merged[["t1", "t2", "map", "as_of_date",
                   "h2h_map_count", "h2h_map_t1_winrate"]]


def _team_map_rolling_at_date(maps_df, queries, window=10, min_periods=3):
    """Per-(team, map) rolling stats as of a query date, with all-maps fallback.

    queries: DataFrame with ``team``, ``map``, ``as_of_date``.
    Returns queries with the per-side rolling metrics (raw, not diffed):
    ``f_rating``, ``f_acs``, ``f_kpr``, ``f_dpr``, ``f_fkpr``, ``f_fdpr``,
    ``f_round_share``, ``f_pistol_wr``, ``f_win``, ``sm_count``.

    Mirrors ``compute_map_rolling_features``'s fallback semantics: specific-
    map stat if its sample >= ``min_periods``, else all-maps stat (wider
    window), else NaN.
    """
    metric_cols = ["rating", "acs", "kpr", "dpr", "fkpr", "fdpr",
                   "round_share", "pistol_wr", "win"]

    if maps_df.empty:
        out = queries.copy()
        for c in metric_cols:
            out[f"f_{c}"] = np.nan
        out["sm_count"] = 0
        return out

    df = maps_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    total_rds = df["t1_rds"].astype(float) + df["t2_rds"].astype(float)
    total_rds = total_rds.where(total_rds > 0)

    long = pd.concat(
        [_team_long(df, "t1", total_rds), _team_long(df, "t2", total_rds)],
        ignore_index=True,
    )
    long["date"] = pd.to_datetime(long["date"])

    # Specific-map rolling.
    sm = long.sort_values(["team", "map", "date"], kind="stable").reset_index(drop=True)
    sm_roll = (
        sm.groupby(["team", "map"], group_keys=False)[metric_cols]
        .apply(lambda g: g.rolling(window=window, min_periods=min_periods, closed="left").mean())
    )
    sm_roll.columns = [f"sm_{c}" for c in metric_cols]
    sm_count = (
        sm.groupby(["team", "map"], group_keys=False)["win"]
        .apply(lambda g: g.rolling(window=window, min_periods=1, closed="left").count())
    )
    sm = pd.concat([
        sm[["team", "map", "date"]].reset_index(drop=True),
        sm_roll.reset_index(drop=True),
    ], axis=1)
    sm["sm_count"] = sm_count.values

    # All-maps rolling.
    am = long.sort_values(["team", "date"], kind="stable").reset_index(drop=True)
    am_roll = (
        am.groupby("team", group_keys=False)[metric_cols]
        .apply(lambda g: g.rolling(window=window * 3, min_periods=min_periods, closed="left").mean())
    )
    am_roll.columns = [f"all_{c}" for c in metric_cols]
    am = pd.concat([
        am[["team", "date"]].reset_index(drop=True),
        am_roll.reset_index(drop=True),
    ], axis=1)

    q = queries.copy()
    q["as_of_date"] = pd.to_datetime(q["as_of_date"])
    q["_orig_idx"] = np.arange(len(q))
    q = q.sort_values("as_of_date", kind="stable").reset_index(drop=True)

    sm = sm.sort_values("date", kind="stable")
    merged = pd.merge_asof(
        q, sm,
        left_on="as_of_date", right_on="date",
        by=["team", "map"], direction="backward", allow_exact_matches=False,
    )
    merged = merged.drop(columns=["date"])
    am = am.sort_values("date", kind="stable")
    merged = pd.merge_asof(
        merged.sort_values("as_of_date", kind="stable"), am,
        left_on="as_of_date", right_on="date",
        by="team", direction="backward", allow_exact_matches=False,
    )
    merged = merged.drop(columns=["date"])

    use_specific = merged["sm_count"].fillna(0) >= min_periods
    for c in metric_cols:
        merged[f"f_{c}"] = np.where(use_specific, merged[f"sm_{c}"], merged[f"all_{c}"])
    merged["sm_count"] = merged["sm_count"].fillna(0).astype(int)

    # Restore caller's row order.
    merged = merged.sort_values("_orig_idx", kind="stable").reset_index(drop=True)
    return merged[["team", "map", "as_of_date", "sm_count"]
                  + [f"f_{c}" for c in metric_cols]]


def build_candidate_map_rows(series_df, maps_df, map_pool_path=MAP_POOL_FILE,
                              window=10, min_periods=3):
    """Build one row per (series, candidate_map) for downstream map-play /
    map-win modelling.

    Candidate maps for a series = union of (active map pool on series date),
    (maps actually played in that series), and (known-veto pick/decider maps).
    The union ensures we still emit rows for played maps even if the pool file
    is stale or the maps table has not been fully backfilled.

    Columns produced:
      match_id, date, t1, t2, map,
      played (1/0; from maps table when available, plus pick/decider veto
      labels for known-veto series),
      picked_by_t1, picked_by_t2, banned_by_t1, banned_by_t2,
      decider, veto_known,
      elo_diff, net_h2h, past_diff,
      t1_pick_rate, t2_pick_rate,
      t1_ban_rate,  t2_ban_rate,
      t1_play_rate, t2_play_rate,
      veto_history_n_t1, veto_history_n_t2,
      h2h_map_count, h2h_map_t1_winrate,
      map_wr_diff, round_share_diff, pistol_diff,
      map_rating_diff, map_acs_diff, map_fk_net_diff,
      map_kpm_diff, map_dpm_diff, sample_size_diff,
      t1_sm_count, t2_sm_count.

    Leakage rules:
    - All rolling / cumulative features use ``closed='left'`` (or
      ``allow_exact_matches=False`` in merge_asof), so a series' own data
      never enters its own features.
    - ``played`` / ``picked_by_*`` / ``banned_by_*`` are labels/state for the
      downstream map-play model. They must not be used as features when
      predicting upcoming matches.

    Returns
    -------
    pd.DataFrame
    """
    series_df = series_df.copy()
    series_df["match_id"] = series_df["match_id"].astype(str)
    series_df["date"] = pd.to_datetime(series_df["date"])

    maps_df = maps_df.copy()
    if not maps_df.empty:
        maps_df["date"] = pd.to_datetime(maps_df["date"])

    entries = _load_map_pool_entries(map_pool_path)

    # Build candidate (match_id, map) pairs.
    # Active pool per series.
    pool_per_series = {
        mid: _active_pool_for_date(d, entries)
        for mid, d in zip(series_df["match_id"], series_df["date"])
    }
    # Played maps per series (joined on t1, t2, date).
    if maps_df.empty:
        played_per_series = {mid: set() for mid in series_df["match_id"]}
    else:
        merged_play = series_df[["match_id", "t1", "t2", "date"]].merge(
            maps_df[["t1", "t2", "date", "map", "winner"]],
            on=["t1", "t2", "date"], how="left",
        )
        played_per_series = (
            merged_play.dropna(subset=["map"])
            .groupby("match_id")["map"]
            .apply(lambda s: set(int(x) for x in s))
            .to_dict()
        )

    # Veto-known series reveal the played maps via t1_pick, t2_pick, and
    # remaining/decider even when the maps table is not fully backfilled.
    veto_cols = ["t1_ban1", "t1_ban2", "t2_ban1", "t2_ban2",
                 "t1_pick", "t2_pick", "remaining"]
    veto_known_mask = series_df[veto_cols[0]].notna()
    veto_played_per_series = {}
    for _, s in series_df[veto_known_mask].iterrows():
        maps_played = {
            int(m)
            for m in [s.get("t1_pick"), s.get("t2_pick"), s.get("remaining")]
            if pd.notna(m)
        }
        veto_played_per_series[s["match_id"]] = maps_played

    candidate_rows = []
    for _, s in series_df.iterrows():
        mid = s["match_id"]
        pool = pool_per_series[mid]
        played = played_per_series.get(mid, set())
        veto_played = veto_played_per_series.get(mid, set())
        for map_id in sorted(pool | played | veto_played):
            candidate_rows.append({
                "match_id": mid,
                "date": s["date"],
                "t1": int(s["t1"]),
                "t2": int(s["t2"]),
                "map": int(map_id),
                "in_pool": int(map_id in pool),
                "played": int((map_id in played) or (map_id in veto_played)),
            })
    cand = pd.DataFrame(candidate_rows)
    if cand.empty:
        return cand

    # Veto-derived labels: picked_by_t1, picked_by_t2, banned_by_t1,
    # banned_by_t2, decider. veto_known indicates whether the series had veto
    # data at all. For series without veto data, these are NaN.
    veto_known_ids = set(series_df.loc[veto_known_mask, "match_id"])

    # Build a per-(match_id, map) veto status lookup.
    veto_status = {}
    for _, s in series_df[veto_known_mask].iterrows():
        mid = s["match_id"]
        for m, key in [
            (s.get("t1_ban1"), "banned_by_t1"),
            (s.get("t1_ban2"), "banned_by_t1"),
            (s.get("t2_ban1"), "banned_by_t2"),
            (s.get("t2_ban2"), "banned_by_t2"),
            (s.get("t1_pick"), "picked_by_t1"),
            (s.get("t2_pick"), "picked_by_t2"),
            (s.get("remaining"), "decider"),
        ]:
            if pd.notna(m):
                veto_status.setdefault((mid, int(m)), {})[key] = 1

    def _veto_field(row, field):
        if row["match_id"] not in veto_known_ids:
            return np.nan
        return veto_status.get((row["match_id"], row["map"]), {}).get(field, 0)

    for field in ["picked_by_t1", "picked_by_t2", "banned_by_t1",
                  "banned_by_t2", "decider"]:
        cand[field] = cand.apply(lambda r: _veto_field(r, field), axis=1)
    cand["veto_known"] = cand["match_id"].isin(veto_known_ids).astype(int)

    # Series-level context.
    series_df["past_diff"] = (
        series_df["t1_past"].fillna(0) - series_df["t2_past"].fillna(0)
    )
    ctx = series_df[["match_id", "elo_diff", "net_h2h", "past_diff"]]
    cand = cand.merge(ctx, on="match_id", how="left")

    # Veto rates (per team per map) prior to series date.
    veto_long = _veto_long(series_df)
    q_veto_t1 = cand[["t1", "map", "date"]].rename(columns={"t1": "team", "date": "as_of_date"})
    q_veto_t2 = cand[["t2", "map", "date"]].rename(columns={"t2": "team", "date": "as_of_date"})
    rates_t1 = _prematch_veto_rates(veto_long, q_veto_t1)
    rates_t2 = _prematch_veto_rates(veto_long, q_veto_t2)
    cand["t1_pick_rate"] = rates_t1["pick_rate"].values
    cand["t1_ban_rate"]  = rates_t1["ban_rate"].values
    cand["t1_play_rate"] = rates_t1["play_rate"].values
    cand["veto_history_n_t1"] = rates_t1["veto_history_n"].values
    cand["t2_pick_rate"] = rates_t2["pick_rate"].values
    cand["t2_ban_rate"]  = rates_t2["ban_rate"].values
    cand["t2_play_rate"] = rates_t2["play_rate"].values
    cand["veto_history_n_t2"] = rates_t2["veto_history_n"].values

    # H2H per-map history.
    q_h2h = cand[["t1", "t2", "map", "date"]].rename(columns={"date": "as_of_date"})
    h2h = _h2h_map_history(maps_df, q_h2h)
    cand["h2h_map_count"] = h2h["h2h_map_count"].values
    cand["h2h_map_t1_winrate"] = h2h["h2h_map_t1_winrate"].values

    # Per-team rolling map stats at the series date (specific-map + fallback).
    q_roll_t1 = cand[["t1", "map", "date"]].rename(columns={"t1": "team", "date": "as_of_date"})
    q_roll_t2 = cand[["t2", "map", "date"]].rename(columns={"t2": "team", "date": "as_of_date"})
    r1 = _team_map_rolling_at_date(maps_df, q_roll_t1, window=window, min_periods=min_periods)
    r2 = _team_map_rolling_at_date(maps_df, q_roll_t2, window=window, min_periods=min_periods)

    cand["t1_sm_count"] = r1["sm_count"].values
    cand["t2_sm_count"] = r2["sm_count"].values
    cand["map_wr_diff"]      = r1["f_win"].values         - r2["f_win"].values
    cand["round_share_diff"] = r1["f_round_share"].values - r2["f_round_share"].values
    cand["pistol_diff"]      = r1["f_pistol_wr"].values   - r2["f_pistol_wr"].values
    cand["map_rating_diff"]  = r1["f_rating"].values      - r2["f_rating"].values
    cand["map_acs_diff"]     = r1["f_acs"].values         - r2["f_acs"].values
    cand["map_fk_net_diff"]  = (
        (r1["f_fkpr"].values - r1["f_fdpr"].values)
        - (r2["f_fkpr"].values - r2["f_fdpr"].values)
    )
    cand["map_kpm_diff"]     = r1["f_kpr"].values - r2["f_kpr"].values
    cand["map_dpm_diff"]     = r1["f_dpr"].values - r2["f_dpr"].values
    t1_n = pd.Series(r1["sm_count"].values).fillna(0).clip(upper=window)
    t2_n = pd.Series(r2["sm_count"].values).fillna(0).clip(upper=window)
    cand["sample_size_diff"] = (t1_n - t2_n).values

    return cand


if __name__ == "__main__":
    # Smoke test against whatever is currently in the maps + series tables.
    import sqlite3
    from paths import DB

    with sqlite3.connect(DB) as con:
        maps_df = pd.read_sql("SELECT * FROM maps", con)
        series_df = pd.read_sql("SELECT * FROM series", con)
    print(f"loaded {len(series_df)} series rows, {len(maps_df)} map rows")

    out = compute_map_rolling_features(maps_df)
    print("Phase 2 rolling features head:")
    print(out[["map_id", "t1", "t2", "date", "map"] + MAP_ROLLING_FEATURES].to_string())

    # Phase 3 — build candidate rows for a small recent slice.
    sample = series_df.tail(50)
    cand = build_candidate_map_rows(sample, maps_df)
    print(f"\nPhase 3 candidate rows: {len(cand)} for {len(sample)} series")
    show_cols = [
        "match_id", "date", "t1", "t2", "map",
        "in_pool", "played", "veto_known",
        "picked_by_t1", "picked_by_t2", "banned_by_t1", "banned_by_t2", "decider",
        "veto_history_n_t1", "t1_pick_rate", "t1_ban_rate",
        "h2h_map_count", "h2h_map_t1_winrate",
        "elo_diff", "net_h2h", "past_diff",
    ]
    print(cand[show_cols].head(20).to_string())
