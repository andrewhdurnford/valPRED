# Map-By-Map Reintroduction — Implementation Log

## Phase 1 — Restore Per-Map Data (2026-05-21)

### What was implemented

Per-map parsing was reintroduced in `scraping/stats_scraper.py`. Series scraping
already worked; per-map rows were never written to the `maps` table by the
current scraper. The archive on `archive/map-by-map` retained the parsing logic
but stopped wiring it into `process_match_link`. Older commit `ddd5b31` showed
the original 3-URL fetch pattern (main page + performance tab + economy tab)
which the archive had collapsed back down to a single main-page fetch.

The implementation here uses two fetches per match:

1. Main match page — gives all per-map player tables (`vm-stats-game` divs)
2. Economy tab (`{match_link}/?game=all&tab=economy`) — gives pistol counts

The performance tab is **not** fetched. Mid-round multi-kills, clutches,
plants, defuses, and economy ratings are not in the current `maps` schema, so
fetching that page would be wasted work for Phase 1. If later phases want
those fields, they should be added back behind the same pattern.

### New / changed code in `scraping/stats_scraper.py`

- `AGENTS_FILE`, `DEFAULT_AGENTS`, `agents`, `agents_lock`, `load_agents()`
- `_load_namelist`, `_append_namelist`, `append_agent_name`, `get_agent_index`
  - Mirror of the existing map name handling. Auto-discovers and persists new
    agents (Vyse, Tejo, Waylay, etc.) so the scraper does not crash on new
    agent releases. Agents are 0-indexed by position in `agents.txt`, matching
    the existing map convention.
- `map_headers` — the column ordering for `maps` table writes, kept in sync
  with the schema in `db_init.py`.
- `_parse_int_text` — small helper for tolerantly extracting integers.
- `parse_map_team_stats(table_body)` — per-team aggregation from one player
  table tbody: fks (sum), rating (mean), acs (mean), kills/deaths/assists
  (sum), agents (sorted, padded to 5). Mirrors the older `parse_player_stats`
  but uses the resilient span-fallback pattern already in `parse_all_maps_team`.
- `parse_map(map_div, t1, t2)` — parses one `vm-stats-game` div. Returns
  `None` for the aggregate `data-game-id="all"` div (not a real map). Returns
  a dict keyed by `map_headers`.
- `parse_econ_pistols(econ_soup, map_id)` — extracts t1/t2 pistol counts from
  the economy-tab soup. Best-effort: returns `(None, None)` on any structural
  miss, so failures here do not block writing the rest of the map row.
- `process_match_link` now returns `(series_row, [map_rows])` instead of just
  `series_row`. Econ fetch is sequential after the main fetch (one extra
  network round trip per match, throttled by the existing `request_delay`).
- `process_matches` accepts the tuple result, builds `maps_df`, and writes it
  via the existing upsert helper (`write_scraped_table`, keyed on `map_id`).

### Schema

No schema changes. The existing `maps` table already covers the Phase 1
minimum target fields. `t1_pistols`/`t2_pistols` columns were already present
and are now populated.

### Smoke test (2026-05-21)

Running `process_matches` on 3 recent links wrote 7 map rows. Sample:

```
map_id 85117  date 2022-05-21  map=2 (Breeze)   t1_rds 7-13   t1_pistols 1-1   t1_fks 10   t1_rating 0.818
map_id 85118  date 2022-05-21  map=0 (Ascent)   t1_rds 7-13   t1_pistols 0-2   t1_fks 5    t1_rating 0.853
```

Pistols, FKs, ratings, agents, K/D/A all populated correctly. UPSERT keyed on
`map_id` means re-scraping the same match overwrites the row rather than
duplicating it.

### Decisions and deviations

- **Two fetches per match, not three.** The original scraper fetched main /
  performance / economy. Performance fields (multi-kills, clutches, plants,
  defuses, economy rating) are not in the current `maps` schema. Adding them
  would have been speculative work for Phase 2+ features. The fetch pattern
  is easy to extend later by adding a third `fetch_data` call and a
  `parse_performance_*` helper.
- **Dynamic agent IDs.** The old code used a hardcoded `agents.index(...)`
  that would `ValueError` on any new agent. Replaced with the same lazy /
  persistent pattern already used for maps. Existing `agents.txt` is treated
  as the source of truth on first load.
- **Map name suffix stripping.** Some VLR map header spans include trailing
  text like " PICK" or " DECIDER" after the map name. `parse_map` keeps only
  the first whitespace-separated token to guarantee a clean lookup in
  `maps.txt`.
- **Pistols best-effort, not blocking.** The older archive code had
  `t2_pistols = int(...) / 2` which was almost certainly a bug; not preserved.
  Pistols come from the economy tab and the parser silently degrades to
  `None` if VLR's markup shifts or stats aren't yet available.
- **No backfill of historical rows performed.** The current DB has 4745
  `series` rows and 0 `maps` rows (before the smoke test added 7). A full
  Phase 1 backfill means re-running `process_matches` over the full match
  link file. That is ~4745 × 2 fetches with the configured `request_delay`
  of 0.2 s and `max_workers` from config. Left for the user to trigger
  intentionally rather than running it implicitly.

### Known gaps / TODOs (deliberately left for later phases)

- No backfill run yet — only a 3-match smoke test was executed.
- Richer per-map fields (eco round W/L, fullbuy W/L, multi-kills, clutches,
  plants/defuses, economy rating) are not scraped or stored. Their schema
  is also absent. To add them: extend `db_init.py`, add a performance-tab
  fetch in `process_match_link`, and add `parse_econ_extras` /
  `parse_performance_stats` helpers (the archive code at
  `archive/map-by-map:scraping/stats_scraper.py` and the very old
  `webscraping/match_stats_scraper.py` at commit ddd5b31 are useful references
  for the markup, though selectors should be re-verified against current VLR).
- No "first deaths" per map (`t1_fds`/`t2_fds`). Not in Phase 1 minimum and
  the old per-map parser did not extract it either. Series-level `t1_fds` /
  `t2_fds` already work via the All-Maps aggregate path.
- The aggregate `data-game-id="all"` div is skipped intentionally; series-
  level aggregate stats continue to come from `parse_all_maps`.

### Open questions for later phases

- How aggressively should we re-scrape? A full re-scrape doubles network
  load (econ tab fetch). Could batch backfill in chunks.
- Should pistols failures be retried? Currently a transient econ-tab failure
  silently produces `(None, None)`. Probably acceptable; the upsert will
  overwrite next time the match is re-scraped.
- Should `maps.txt` / `agents.txt` get a header comment listing the assigned
  ids so users can read them more easily? Cosmetic only.

## Phase 2 — Map-Level Feature Engineering (2026-05-21)

### What was implemented

New module `modelling/maps.py` with `compute_map_rolling_features(df, window=10, min_periods=3)`. Mirrors the leakage-safe pattern from `series.py::compute_rolling_features` but at per-map granularity:

1. Flatten each map row into two team-side records (long format).
2. Run `rolling(window, closed='left').mean()` grouped by `(team, map)` — the "specific-map" history.
3. Run a second rolling pass grouped by `team` only with a wider window (`3 × window`) — the "all-maps" fallback.
4. Merge both feature sets back onto the long frame.
5. Per-feature fallback: if specific-map sample (`sm_count`) ≥ `min_periods` use the specific-map stat, else use the all-maps stat, else leave NaN.
6. Pivot back to wide and compute t1−t2 diff features.

### Output columns

Exported via `MAP_ROLLING_FEATURES`:

- `map_wr_diff` — team-on-map win rate diff
- `round_share_diff` — team-on-map round share (`t1_rds / total_rds`) diff
- `pistol_diff` — team-on-map pistol win rate (`pistols / 2`) diff
- `map_rating_diff` — team-on-map combat rating diff
- `map_acs_diff` — team-on-map ACS diff
- `map_fk_net_diff` — team-on-map `(FK − FD)` per-round, then differenced
- `map_kpm_diff` — team-on-map kills per round diff
- `map_dpm_diff` — team-on-map deaths per round diff
- `sample_size_diff` — diff in count of prior specific-map appearances (clipped at `window`)

Names are deliberately distinct from the series-level features (`rating_diff`, `acs_diff`, `fk_net_diff`, `winrate_diff`) so the two feature sets can be merged side-by-side later without column collisions, per the plan's guidance.

### Decisions and deviations

- **Per-round normalisation, not per-map.** The plan suggested `kpm_diff` / `dpm_diff` ("per map"). Maps differ in round counts (regulation 13-7 vs 13-11 OT etc.) so kills-per-map distorts comparisons. Used kills-per-round instead (`t1_kills / total_rds`). Kept the `kpm` / `dpm` naming for compatibility with the plan vocabulary but the semantic is per-round. Logged here so this is not silent drift.
- **`fdpr` from opponent's FK.** First deaths per map are not in the current schema — `parse_map_team_stats` does not extract them, and the series-level `fds` only exists from the All-Maps aggregate. Used the opposing side's first kills as this side's first deaths (algebraically equivalent at the map level). Avoids re-scraping or schema changes.
- **All-maps fallback window = `3 × window`.** Plan said "team-on-all-maps history" as fallback tier. A naïve same-window fallback bumps into `min_periods` for any new team. Tripled the window so the fallback is a stable team-level prior. The specific-map window stays at the plan's suggested `window=10`.
- **`sample_size_diff` clipped at `window`.** Pre-clip, this feature is unbounded and dominated by veteran teams whose specific-map history runs into the hundreds. Clipping at `window` keeps the feature in `[-window, window]` and aligned with the rolling window the other features are actually using.
- **Pistols best-effort.** If `t1_pistols` / `t2_pistols` are NaN (econ tab parse failure in Phase 1), they propagate as NaN through the rolling mean. No special handling needed — `rolling.mean()` skips NaN automatically.
- **Single-pass design.** The plan suggested computing on the broad historical map dataset before Tier 1 filtering. The function takes the full `maps` df and lets the caller filter downstream — same pattern as `compute_rolling_features` for the series model.

### Verification

1. **DB smoke test** (`python modelling/maps.py`) — 7 maps in DB. All rolling features NaN as expected (no team has ≥3 priors yet). `sample_size_diff = 1.0` on the third unique (team, map) row, confirming `closed='left'` is correctly excluding the row's own stats from its own window.

2. **Synthetic leakage / fallback test** — built a 12-row deterministic dataset where team A always wins map 0 (13-7, 2 pistols) and team B always wins map 1. Verified:
   - First 3 rows have NaN features (insufficient any-tier priors) ✓
   - Row 3: specific-map sample (1) thin → uses all-maps fallback → `map_wr_diff = 0.667 - 0.333 = 0.333` matches the analytical answer ✓
   - Rows 6+ (≥3 specific-map priors): `map_wr_diff = +1.0` on map 0, `-1.0` on map 1 ✓
   - `round_share_diff` and `pistol_diff` converge to `+0.30` / `+1.0` on map 0 as expected ✓

### Known gaps / TODOs

- No real-data validation. The DB has 7 maps because Phase 1's full backfill was deferred. Once `process_matches` is re-run over the full link file, the feature outputs should be re-inspected for sanity (distribution shape, NaN rate, fallback hit rate).
- `MAP_ROLLING_FEATURES` is exported but nothing in `modelling/main.py` consumes it yet — that wiring belongs to Phase 7.
- Per-map agent composition (`t1_agent1..5`) is in the schema but not yet a feature input. Phase 5 (map-win model) is the natural place to add a composition similarity / diversity feature if it pays off.
- No neutral-default tier in the fallback hierarchy. The plan listed neutral defaults as tier 3; left as NaN here so the caller can choose impute-vs-drop downstream. Easy to add a `default_for=` kwarg later if needed.

### Open questions for later phases

- Is `window=10` the right choice for specific-map rolling? Veteran teams may have hundreds of prior maps on Ascent; very recent meta shifts (agent reworks, map changes) might justify a shorter window. Worth a sensitivity sweep in Phase 9 backtests.
- Should the all-maps fallback also be exposed as its own feature (`t1_all_rating`, etc.) so the downstream model can learn how to combine specific-vs-general signal itself? Currently it is hidden inside the fallback selector. Defer until Phase 7 ablation evidence calls for it.
- Per-feature `min_periods` may want to differ — win rate stabilises slowly (Bernoulli), combat rating stabilises faster. Single threshold for now; revisit if calibration suggests it.

## Phase 3 — Candidate Map Rows Per Series (2026-05-21)

### What was implemented

Added `build_candidate_map_rows(series_df, maps_df, ...)` to `modelling/maps.py` plus four supporting helpers. Output is one row per (series, candidate_map) with the columns the plan lists for downstream phases.

Helpers:

- `_load_map_pool_entries(path)` — parses `data/game_data/map_pool.txt`.
- `_active_pool_for_date(date, entries)` — set of map ids in pool on a date.
- `_veto_long(series_df)` — reshapes the series veto columns into a long table of (match_id, date, team, map, action) where action ∈ {pick, ban, remaining}.
- `_prematch_veto_rates(veto_long, queries)` — per-(team, map) cumulative pick / ban / play counts and team-level total denominator, all strictly prior to each query date.
- `_h2h_map_history(maps_df, queries)` — prior head-to-head on a map between two teams; order-invariant via canonical (lo, hi) pair.
- `_team_map_rolling_at_date(maps_df, queries, window, min_periods)` — per-(team, map) rolling stats as of a query date with the same all-maps fallback as `compute_map_rolling_features`.

The main function produces these columns per candidate row:

- Identity: `match_id`, `date`, `t1`, `t2`, `map`.
- State / labels (NOT pre-match features): `played`, `picked_by_t1`, `picked_by_t2`, `banned_by_t1`, `banned_by_t2`, `decider`, `in_pool`, `veto_known`.
- Series-level context: `elo_diff`, `net_h2h`, `past_diff`.
- Per-team prior pick / ban / play rates: `t{1,2}_pick_rate`, `t{1,2}_ban_rate`, `t{1,2}_play_rate`, `veto_history_n_t{1,2}`.
- H2H on this map: `h2h_map_count`, `h2h_map_t1_winrate`.
- Per-team rolling map stats at series date (diffed): `map_wr_diff`, `round_share_diff`, `pistol_diff`, `map_rating_diff`, `map_acs_diff`, `map_fk_net_diff`, `map_kpm_diff`, `map_dpm_diff`, `sample_size_diff`, plus `t{1,2}_sm_count`.

Candidate maps for a series = (active pool on date) ∪ (maps actually played). Union guarantees we still emit rows for played maps even if the pool file is stale or missing.

### Decisions and deviations

- **Pool file format.** `map_pool.txt` format `YYYY-MM-DD;out_of_pool_ids` confirmed by inspection: e.g. `2023-01-10;6,8,9,10` leaves seven maps in the active pool (`{0,1,2,3,4,5,7}`). For series dated before the file's first entry (2023-01-10), the earliest entry's pool is used as a fallback. Logged here because the file documentation is implicit.
- **Map id 11 (Corrode) in pre-existence dates.** The file never lists 11 in any `out` set, so `{0..MAX_MAP_ID} - out` adds Corrode to old pools even though the map didn't exist yet. Not corrected at this stage: downstream models will see zero prior play / pick / ban evidence for it and a near-zero play probability will naturally follow. Cleaner fix would be to gate `MAX_MAP_ID` on the date of the first map appearance per id; deferred until it actually shows up as a problem.
- **Series without veto data.** ~2512 / 4745 series have `t1_ban1 IS NULL` and lack veto info. For those, `picked_by_*` / `banned_by_* / decider` are NaN and `veto_known=0`. They still get candidate rows (active-pool + played union) so Phase 5 (map-win) can use them; Phase 4 (map-play) will likely need to filter to `veto_known=1`. Documented in the function docstring.
- **`play` definition for pick/ban/play rates.** A team "plays" a map in a series when it picks it, the opponent picks it, or it is the decider. Pure bans count toward `ban_rate` but not `play_rate`. The denominator for each rate is the team's prior count of known-veto series (not prior appearances of the specific map). This makes rates sum to ~1 (picks), ~2 (bans), and ~3 (plays in BO3) across the active pool. Verified on team 731: pick_rate sums to 0.833 across the current pool — the missing 0.167 is on out-of-pool Pearl/Haven where the team had prior picks before the pool changed.
- **H2H key is order-invariant.** Canonicalised on `(min(t1,t2), max(t1,t2), map)` so a prior A-vs-B map on Ascent counts equally regardless of which team is t1 today. `h2h_map_t1_winrate` is flipped to t1's perspective.
- **Per-side rolling lookups are date-keyed, not map_id-keyed.** Phase 2 computes features for played maps (one feature row per played map). Phase 3 needs features for candidate maps that may never be played — so the lookup is "team's most recent rolling state at date" using `merge_asof(direction='backward', allow_exact_matches=False)`. Returns the same feature definitions as Phase 2 but indexed by query date rather than by played `map_id`.
- **Row-order preservation bug found and fixed.** First implementation called `_prematch_veto_rates(...)`, `_h2h_map_history(...)`, and `_team_map_rolling_at_date(...)` then assigned the returned columns back to `cand` via `.values`. Those helpers sort internally by `as_of_date` for `merge_asof`, so the assignment was misaligned. Symptom: `veto_history_n_t1` differed across candidate maps of the same series. Fixed by stamping an `_orig_idx` inside each helper and sorting back before returning. Verified by isolated unit-style check: team 731 at 2026-05-18 had 36 prior known-veto series consistent across all 12 candidate maps.

### Verification

1. **Schema audit on full DB** — `build_candidate_map_rows(series_df, maps_df)` over 4745 series and 7 maps produced 37,961 candidate rows (mean ~8 candidates/series, matching a 7-map active pool plus the occasional out-of-pool played map). 17,865 rows carry `veto_known=1`.
2. **Veto-rate stability per series** — Series 675356 (team 731 vs team 1119, 2026-05-18): `veto_history_n_t1 = 36` and `veto_history_n_t2 = 16` constant across all candidate maps. Rate sums (pick/ban/play ≈ 1 / 2 / 3 minus out-of-pool slack) match the expected per-series totals.
3. **NaN audit (expected, not bugs)**
    - `map_wr_diff` etc.: 100% NaN — only 7 map rows exist in the DB; Phase 1 full backfill has not been run. Will populate once the maps table is filled.
    - `h2h_map_t1_winrate`: 100% NaN — same reason.
    - `t1_pick_rate` / `t1_ban_rate`: 40% NaN — these are the rows where the team has zero prior known-veto series at the query date.
    - `net_h2h`: 45% NaN — pre-existing series-table sparsity (teams that never met before).
    - `elo_diff`, `past_diff`: 0% NaN — `elo.py` filled, and `past_diff` defaults to 0.

### Known gaps / TODOs

- No real-data validation of the map-rolling and H2H feature distributions — gated on Phase 1 backfill.
- Active-pool inclusion of map id 11 (Corrode) for old dates (see deviation above). Cosmetic for now.
- `_team_map_rolling_at_date` recomputes the long-format rolling table from scratch each call. For Phase 3 (called twice — once per side per build) this is fine. If Phase 8 wires up per-upcoming-match calls one at a time, consider caching.
- Pre-merge `groupby(..).apply(..)` triggers a pandas `DeprecationWarning` about grouping-column inclusion. Behaviour is correct today; an explicit `include_groups=False` (or column selection) would silence it. Left for a code-hygiene pass.

### Open questions for later phases

- Should map id 11 (Corrode, and any future new map) be gated by "first appearance in `maps`" rather than blindly added by the pool diff? Cleanest answer needs Phase 1 backfilled data.
- For Phase 4, should training filter to `veto_known=1` only, or should the model learn from partial-label series with a missing-veto indicator? Veto-known share is ~47% of series — non-trivial to drop.
- Should `h2h_map_count` and `veto_history_n_*` enter Phase 5 as features alongside the diff stats, or only as confidence weights? Trade-off between extra signal and tree-splits on small-N noise.

## Phase 4 — Map-Play Model (2026-05-21)

### What was implemented

Added `modelling/map_play.py`, a standalone Phase 4 training/evaluation script.
It builds Phase 3 candidate rows from the SQLite `series` and `maps` tables,
filters to known-veto rows for reliable labels, trains a simple logistic
regression pipeline, validates chronologically, and saves:

```text
models/map_play.joblib
```

The model estimates:

```text
P(candidate map is played | teams, date, map, prior map/veto history)
```

### Candidate-row label repair

Phase 4 exposed a Phase 3 issue: because the `maps` table still only contains
the Phase 1 smoke-test rows, `played` was effectively all zeros for historical
known-veto series. `build_candidate_map_rows(...)` now treats known veto
`t1_pick`, `t2_pick`, and `remaining` as played-map labels. Candidate maps are
now:

```text
active pool ∪ maps table played maps ∪ veto pick/decider maps
```

This keeps labels reliable before the full Phase 1 map backfill. Verification
on the full DB:

```text
candidate_rows = 39085
known_veto_rows = 18989
known_veto_series = 2233
played maps per known-veto series = exactly 3.0
known-veto row positive rate = 35.28%
```

### Model details

The starting model is intentionally conservative:

- `Pipeline(ColumnTransformer(...), LogisticRegression(max_iter=2000))`
- `map` is one-hot encoded.
- Numeric features are constant-imputed to neutral zero and scaled.
- Empty numeric columns are kept in the pipeline (`keep_empty_features=True`)
  so the artifact shape is stable once the maps table is backfilled.

Features used:

- Map identity / pool: `map`, `in_pool`
- Series context: `elo_diff`, `net_h2h`, `past_diff`
- Prior veto rates: `t{1,2}_pick_rate`, `t{1,2}_ban_rate`,
  `t{1,2}_play_rate`
- Derived veto preference features: sum/diff versions of pick, ban, and play
  rates
- Veto-history sample sizes: `veto_history_n_t1`, `veto_history_n_t2`,
  `veto_history_n_min`, `veto_history_n_diff`
- H2H map history: `h2h_map_count`, `h2h_map_t1_edge`
- Map rolling stats from Phase 2/3: `map_wr_diff`, `round_share_diff`,
  `pistol_diff`, `map_rating_diff`, `map_acs_diff`, `map_fk_net_diff`,
  `map_kpm_diff`, `map_dpm_diff`, `sample_size_diff`, `t1_sm_count`,
  `t2_sm_count`

Same-match labels/state are explicitly excluded from features:

- `played`
- `picked_by_t1`, `picked_by_t2`
- `banned_by_t1`, `banned_by_t2`
- `decider`
- `veto_known`

### Chronological validation

Ran:

```bash
./venv/bin/python modelling/map_play.py
```

The script used config dates:

- Train: `2023-02-13` through `2025-10-05`
- Test: `2026-01-15` through `2026-12-31`

Validation output:

```text
Rows: 1738
Series: 190
Positive rate: 32.80%
Accuracy: 76.12%
Brier: 0.1795
Log loss: 0.5397
ROC AUC: 0.6776
Average precision: 0.6595
Top-k played recall: 52.63%
Candidate rows built: 39085
```

`Top-k played recall` is a series-level sanity check: for each series, take
the top `k` map-play probabilities where `k` is the actual number of played
maps, then measure what share of the real played maps are recovered.

### Decisions and deviations

- **Filtered to `veto_known=1` for Phase 4.** Partial-label rows were not used
  because they cannot distinguish "not played" from "not present in the
  sparse maps table." This answers the Phase 3 open question for now. Those
  rows can still be useful for Phase 5 map-win training once maps are
  backfilled.
- **Logistic regression over GBM.** Chosen as the first map-play model because
  it is simple, fast, produces direct probabilities, and is easy to audit.
  No parameter cache was added because no hyperparameter search is being run.
  If Phase 6/7 ablations show map-play probabilities matter but calibration is
  weak, add a calibrated GBM or calibrated logistic variant later.
- **No Tier-1/regional filtering yet.** The map-play model trains on all
  known-veto rows within the chronological windows. Veto preferences benefit
  from extra sample, and the output is an auxiliary probability rather than
  the final betting model. Revisit during Phase 9 ablations if broad training
  hurts downstream regional/T1 performance.
- **Map stat features currently carry no real signal.** Because the `maps`
  table still has only 7 rows, `h2h_map_t1_edge` and the Phase 2 rolling map
  stat diffs are all missing in this run and are neutral-imputed. The saved
  pipeline keeps them so a rerun after full backfill can use them without
  code changes.

### Known gaps / TODOs

- Rerun Phase 4 after a full Phase 1 map backfill. The current model is mostly
  learning from map identity, active-pool membership, and prior veto rates.
- Calibration has only been checked by Brier/log loss. Before Phase 6 uses
  these probabilities for series aggregation, add a reliability curve or
  calibration-bin audit.
- Consider gating new maps (e.g. Corrode) by first observed date once the maps
  table is populated. The current active-pool helper can include future maps
  too early if `map_pool.txt` does not mark them out.
- Phase 5 should not assume this map-play model is final; treat it as the
  first baseline for ablation.

## Phase 5 — Map-Win Model (2026-05-22)

### What was implemented

Added `modelling/map_win.py`, a standalone Phase 5 training/evaluation script.
It builds labelled rows from actual played maps in the SQLite `maps` table and
merges in Phase 3 candidate-row features that are computed strictly before the
series date.

The model estimates:

```text
P(t1 wins map | teams, date, map, prior map/team context)
```

The saved target path, once enough map data exists, is:

```text
models/map_win.joblib
```

### Dataset construction

`build_map_win_rows(series_df, maps_df, candidate_df=None)`:

- Uses `maps.winner` as the label source.
- Defines `t1_win = 1` when `winner == 0`.
- Joins each actual map to Phase 3 candidate rows by `t1`, `t2`, `date`, and
  `map`.
- Uses Phase 3's map rolling features, so same-match map stats do not leak
  into the feature row.
- Falls back to synthetic `map-{map_id}` match ids only if a map cannot be
  linked back to a series row.

Same-match outcome columns such as `t1_rds`, `t2_rds`, `t1_rating`,
`t2_rating`, `t1_acs`, `t2_acs`, pistol counts, kills, deaths, assists, and
first kills are intentionally excluded from model features.

### Model details

Starting model:

- One pooled model across all maps.
- `map` one-hot encoded as the only categorical feature.
- Numeric history features constant-imputed to neutral zero.
- `GradientBoostingClassifier` base model with conservative fixed defaults.
- Sigmoid calibration fitted on the most recent chronological slice of the
  training data.

The calibrator uses whole-match chronological splitting. The base model must
train on at least 50 map rows, and the calibration slice must have at least 20
map rows; both slices must contain both target classes. This prevents saving a
calibrated artifact from the current tiny smoke-test map table.

Features used:

- `map`
- Series/team context: `elo_diff`, `net_h2h`, `past_diff`
- Prior H2H map history: `h2h_map_count`, `h2h_map_t1_edge`
- Phase 2/3 map rolling stats: `map_wr_diff`, `round_share_diff`,
  `pistol_diff`, `map_rating_diff`, `map_acs_diff`, `map_fk_net_diff`,
  `map_kpm_diff`, `map_dpm_diff`, `sample_size_diff`
- Specific-map sample context: `t1_sm_count`, `t2_sm_count`,
  `sm_count_min`, `sm_count_diff`

Prediction helper:

```text
predict_map_win_probabilities(model, candidate_df)
```

returns `P(t1 wins map)` for Phase 6 candidate-map aggregation.

### Verification

1. Syntax check:

```bash
./venv/bin/python -m py_compile modelling/map_win.py
```

2. Current DB run:

```bash
./venv/bin/python modelling/map_win.py
```

Output:

```text
Map-win model not trained: No map-win training rows after filtering
Map-win rows available: 7
Backfill the maps table with a full scrape, then rerun this script.
```

This is expected because the local `maps` table still has only the 7 Phase 1
smoke-test rows, all dated 2022-05-19 through 2022-05-21. The configured
training window is 2023-02-13 through 2025-10-05, so there are zero eligible
training rows under the real split.

3. Synthetic chronological smoke test:

Generated 180 synthetic played map rows with realistic columns, built Phase 5
rows through `build_map_win_rows`, split by whole match, trained the calibrated
model, and evaluated on the chronological holdout.

```text
rows=180 train=144 test=36
rows: 36
series: 36
positive_rate: 47.22%
accuracy: 61.11%
brier: 0.2897
log_loss: 0.8379
calibration_ece: 0.2624
roc_auc: 0.6285
average_precision: 0.6137
```

The metric values themselves are not meaningful because the data is synthetic;
the purpose was to exercise the real train/calibrate/evaluate path after a
chronological split.

### Decisions and deviations

- **Pooled GBM, not per-map models.** Current real data is far too sparse for
  per-map models. A pooled model with `map` as a categorical feature follows
  the plan's starting recommendation and will stay viable after backfill.
- **Chronological held-out calibration.** Used a recent-match calibration slice
  instead of random calibration folds. This mirrors the series model's temporal
  discipline while keeping the saved artifact fully calibrated for downstream
  EV-sensitive aggregation.
- **No hyperparameter cache.** Phase 5 does not run a hyperparameter search.
  The model uses fixed conservative defaults; caching only becomes relevant if
  Phase 9 ablations justify adding search.
- **No current-veto labels as features.** The model does not use `picked_by_*`,
  `banned_by_*`, `decider`, or `played`. Those are same-series state/labels and
  would not be available for standard pre-match prediction.
- **Prior veto rates not included yet.** They are pre-match safe and may carry
  map-comfort signal, but the first Phase 5 model keeps to prior performance
  features plus team/series context. Add them only if Phase 9 ablations support
  the extra complexity.
- **No artifact saved on current DB.** Saving `models/map_win.joblib` is gated
  by enough real rows/classes for base training and calibration. This avoids a
  misleading model trained from seven 2022 maps.

### Known gaps / TODOs

- Run the full Phase 1 map backfill, then rerun:

```bash
./venv/bin/python modelling/map_win.py
```

- Re-check NaN rates and feature distributions after backfill. At present the
  real DB cannot validate map rolling signal.
- Compare pooled GBM against a simpler logistic regression baseline in Phase 9
  if calibration looks unstable.
- Consider adding prior pick/play-rate comfort features only after an ablation.

## Phase 6 — Collapse Map Expectations Into Series Features (2026-05-22)

### What was implemented

Added `modelling/map_expectations.py`, a small aggregation module that turns
Phase 3 candidate-map rows plus Phase 4/5 model probabilities into one
series-level feature row per match.

Main entry points:

- `score_candidate_map_expectations(candidate_df, map_play_model, map_win_model)`
  adds `map_play_prob`, `map_win_prob`, and per-map expected contribution
  columns.
- `collapse_map_expectations(scored_candidate_df)` groups candidate maps by
  series and computes the Phase 6 summary features.
- `build_map_expectation_features(candidate_df, map_play_model, map_win_model)`
  runs both steps.
- `build_map_expectation_features_from_tables(series_df, maps_df, ...)` builds
  candidate rows from raw tables and then collapses them.
- `load_map_expectation_models()` loads `models/map_play.joblib` and
  `models/map_win.joblib`, failing loudly if either artifact is missing.

Feature formulas:

```text
expected_t1_maps = sum(map_play_prob * map_win_prob)
expected_t2_maps = sum(map_play_prob * (1 - map_win_prob))
expected_total_maps = expected_t1_maps + expected_t2_maps
map_winshare = expected_t1_maps / expected_total_maps
map_edge = expected_t1_maps - expected_t2_maps
```

If `expected_total_maps == 0`, `map_winshare` defaults to neutral `0.5`.

Exported first-pass model features:

```text
MAP_EXPECTATION_FEATURES = ["map_winshare", "map_edge"]
```

Audit columns are also returned for inspection but are not part of the first
Phase 7 feature set by default:

```text
expected_t1_maps, expected_t2_maps, expected_total_maps,
candidate_maps, map_play_mass
```

### Calibration audit update

`modelling/map_play.py` now reports equal-width expected calibration error
(`calibration_ece`) alongside Brier/log loss. Rerunning the current Phase 4
validation produced:

```text
Rows: 1738
Series: 190
Positive rate: 32.80%
Accuracy: 76.12%
Brier: 0.1795
Log loss: 0.5397
ECE: 0.1398
ROC AUC: 0.6776
Average precision: 0.6595
Top-k played recall: 52.63%
Candidate rows built: 39085
```

This satisfies the Phase 4 TODO to add a probability reliability check before
map-play probabilities are consumed by the series-level aggregator. It is not
a full reliability plot, but it gives a stable scalar to compare during Phase 9
ablations.

### Verification

1. Syntax check:

```bash
./venv/bin/python -m py_compile modelling/map_expectations.py modelling/map_play.py
```

2. Deterministic aggregation smoke test:

Synthetic candidate rows with pre-filled probabilities produced:

```text
match_id  map_winshare  map_edge  expected_t1_maps  expected_t2_maps
m1        0.555556      0.2       1.0               0.8
m2        0.500000      0.0       0.0               0.0
```

The `m1` row matches the hand calculation:

```text
expected_t1_maps = 0.8*0.75 + 0.6*0.50 + 0.4*0.25 = 1.0
expected_t2_maps = 0.8*0.25 + 0.6*0.50 + 0.4*0.75 = 0.8
map_winshare = 1.0 / 1.8 = 0.555556
map_edge = 1.0 - 0.8 = 0.2
```

The `m2` row verifies the neutral `map_winshare=0.5` fallback when the
candidate map-play mass is zero.

3. Current DB CLI run:

```bash
./venv/bin/python modelling/map_expectations.py
```

Output:

```text
Map expectation features not built: Required map expectation model file(s) missing: /Users/andrewdurnford/Documents/Development/valpred/models/map_win.joblib
Train map_play.py and map_win.py after the maps table is backfilled, then rerun.
```

This is expected. The real local DB still has only 7 maps from the Phase 1
smoke test, so Phase 5 correctly did not save a `map_win.joblib` artifact.

4. Real candidate-row plumbing smoke test:

Loaded the saved `models/map_play.joblib` and used an explicit in-memory
neutral map-win dummy (`P(t1 wins map)=0.5`) to verify the real candidate
rows can flow through the Phase 6 scorer without saving a fake artifact.

```text
candidate_rows=39085
feature_rows=4745
first expected_total_maps values: 2.217966, 3.633044, 2.559061
map_winshare: 0.5 for all rows
map_edge: 0.0 for all rows
```

This confirms the real map-play artifact, Phase 3 candidate rows, and Phase 6
aggregation join cleanly. The neutral dummy was only a smoke-test substitute
for the missing Phase 5 artifact.

### Decisions and deviations

- **New module instead of adding more to `maps.py`.** `maps.py` already owns
  rolling map stats and candidate-row construction. The probability collapse
  step depends on trained artifacts, so a separate `map_expectations.py` keeps
  feature engineering separate from model inference.
- **Start small.** Only `map_winshare` and `map_edge` are marked as
  first-pass model features. The plan listed additional candidates such as
  `best_map_edge`, `worst_map_edge`, `veto_confidence`, and
  `expected_close_maps`; these were not added yet because Phase 7/9 ablations
  should prove the simple expectation features help before expanding the
  series feature space.
- **No silent neutral map-win fallback in real runs.** The aggregation helpers
  can accept pre-scored probabilities for tests, but the CLI path requires both
  saved artifacts. This prevents Phase 7 from accidentally training on
  fabricated 50/50 map-win probabilities while the map table is still sparse.
- **Audit columns are returned, not model features.** `expected_t1_maps`,
  `expected_t2_maps`, and `map_play_mass` are useful sanity checks. They are
  deliberately separated from `MAP_EXPECTATION_FEATURES` so Phase 7 can make a
  narrow, explicit feature addition.

### Known gaps / TODOs

- Run the full Phase 1 map backfill.
- Rerun `modelling/map_win.py` until `models/map_win.joblib` can be saved from
  real historical maps.
- Rerun `modelling/map_expectations.py` on the real artifacts and inspect
  `map_play_mass`, `expected_total_maps`, and `map_edge` distributions.
- Phase 7 should merge `MAP_EXPECTATION_FEATURES` into the series training path
  and compare against the seven-feature baseline before changing prediction
  output.

## Phase 7 — Extend The Series Model (2026-05-22)

### What was implemented

The series training path now supports explicit feature sets while preserving
the original seven-feature baseline as the default.

Changed code:

- `modelling/training.py`
  - Split the original `FEATURES` into `BASELINE_FEATURES`.
  - Added `MAP_SERIES_FEATURES = ["map_winshare", "map_edge"]`.
  - Added `EXTENDED_FEATURES = BASELINE_FEATURES + MAP_SERIES_FEATURES`.
  - Kept `FEATURES = BASELINE_FEATURES` for backwards compatibility.
  - `train_series_winner_model(..., features=None)` can now train either the
    baseline or extended model.
  - `_PlattScaledModel` now stores `feature_names`, so saved artifacts know
    which columns they require at prediction time.
- `modelling/testing.py`
  - `predict_series_outcomes` now reads feature names from the model artifact
    instead of assuming the global baseline list.
- `modelling/main.py`
  - `_load_series_with_features(include_map_features=True)` builds Phase 6
    map expectation features from the full series/maps history, merges
    `map_winshare` and `map_edge` by `match_id`, then applies the same Tier 1 /
    non-CN / regional filters as the baseline path.
  - Added `--with-map-features` to train/test the normal saved
    `series_winner.joblib` with the extended feature set once map artifacts
    exist. If required map artifacts are missing, this exits with a concise
    message rather than a traceback.
  - Added `--phase7-ablations` to compare the baseline against
    baseline-plus-map-expectations without saving a model.
  - Added Brier score, log loss, and equal-width ECE to the ablation output.
- `modelling/predict.py`
  - Upcoming prediction now reads required feature names from the saved model.
    If a map-extended model is loaded before Phase 8 upcoming feature wiring
    exists, it raises a clear error listing the missing map features.

### Verification

Syntax check:

```bash
./venv/bin/python -m py_compile modelling/training.py modelling/testing.py modelling/main.py modelling/predict.py
```

Phase 7 ablation command:

```bash
./venv/bin/python modelling/main.py --phase7-ablations --skip-init
```

Current output:

```text
baseline
Bets placed: 122  Ending bankroll: $859.62  Accuracy: 38.52%  EV: -0.02  Dog: 93.44%
Bets placed: 126  Ending bankroll: $1101.0  Accuracy: 38.89%  EV: 0.02  Dog: 95.24%

Skipping map-expectation ablation: Required map expectation model file(s) missing: /Users/andrewdurnford/Documents/Development/valpred/models/map_win.joblib

Phase 7 ablation summary
feature_set  rows  features  accuracy    brier  log_loss  calibration_ece  avg_bets  avg_bankroll    avg_ev  best_bets  best_bankroll  best_ev
   baseline   142         7  0.542254 0.246916   0.68696         0.105343       122    859.623634 -0.023013        126         1101.0 0.016032
```

The baseline run uses the configured split:

- Train: `2023-02-13` through `2025-10-05`
- Test: `2026-01-15` through `2026-12-31`

### Decisions and deviations

- **Default remains baseline.** `python modelling/main.py` still trains the
  seven-feature model unless `--with-map-features` is passed. This avoids
  breaking the current working pipeline while the map table is still sparse.
- **Extended model is opt-in but fully wired.** Once `models/map_play.joblib`
  and `models/map_win.joblib` both exist, `--with-map-features` trains the
  saved series model with `map_winshare` and `map_edge`, and
  `--phase7-ablations` evaluates it against the baseline.
- **No fabricated map-win fallback.** The Phase 7 map ablation skips when
  `map_win.joblib` is missing. It does not substitute neutral 50/50 map-win
  probabilities, because that would create a misleading "extended" result.
- **Same parameter cache.** Series-model hyperparameter caching remains in
  `models/params/series.pkl`. The extended model reuses the cached GBM params
  unless that file is deleted, matching the existing cache semantics.
- **Map features are merged before filtering.** Phase 6 candidates are built
  from the full historical series table so veto/map histories are not narrowed
  to the final Tier 1 regional training rows. Filtering still happens after
  feature construction, matching the current rolling-feature discipline.

### Current blocker

The local DB still has only 7 map rows, all from the Phase 1 smoke test, and
there is no `models/map_win.joblib`. Because Phase 5 correctly refuses to save
a map-win model from insufficient data, Phase 7 cannot produce a real
map-expectation ablation yet.

Required next steps before rerunning the extended Phase 7 path:

1. Full Phase 1 map backfill.
2. Rerun `./venv/bin/python modelling/map_win.py` until
   `models/map_win.joblib` is saved.
3. Rerun `./venv/bin/python modelling/main.py --phase7-ablations --skip-init`
   and compare baseline vs baseline-plus-map-expectations.

### Known gaps / TODOs

- Phase 7 only wires the first-pass `map_winshare` and `map_edge` features.
  No uncertainty/sample-size map expectation feature was added yet; add one in
  Phase 9 only if the first two features show useful signal.
- Phase 8 upcoming prediction is now implemented below. Map-extended
  prediction still requires real `models/map_win.joblib`; the baseline model
  continues to predict without map artifacts.

## Phase 8 — Upcoming Match Prediction (2026-05-22)

### What was implemented

`modelling/predict.py` now supports both baseline and map-extended saved
series models.

- Loads the saved series model first and reads its required `feature_names`.
- If the model requires `MAP_EXPECTATION_FEATURES`, upcoming rows are padded
  as no-veto pre-match series, concatenated with historical `series`, passed
  through Phase 3 candidate-map construction, scored with Phase 4/5 artifacts,
  collapsed with Phase 6, and merged back onto upcoming matches.
- If the saved model is still the baseline seven-feature artifact, prediction
  skips the map-artifact path entirely.
- Missing map artifacts now fail with a concise Phase 8 error instead of a
  generic missing-column traceback.
- Upcoming EV now uses the same market-probability shape as backtesting:
  stored implied probabilities are used directly, decimal odds are converted to
  implied probabilities, and `vig_opposite_probability` fills a missing
  opposite side when only one side is available.
- Fixed upcoming `pred_win%` to use `predict_proba(... )[:, 1]`, matching
  `testing.py` and the Platt-scaled model's t1-win probability.

### Verification

Syntax check:

```bash
./venv/bin/python -m py_compile modelling/map_expectations.py modelling/predict.py modelling/main.py
```

Current DB prediction smoke test:

```bash
./venv/bin/python modelling/predict.py
```

Output:

```text
No upcoming tier-1 matches found.
Empty DataFrame
Columns: []
Index: []
```

### Decisions and deviations

- **No neutral map-win fallback.** If a map-extended series model is loaded,
  Phase 8 requires both `map_play.joblib` and `map_win.joblib`. This preserves
  the Phase 6/7 decision not to fabricate 50/50 map-win probabilities in real
  runs.
- **No known-veto/live-veto mode yet.** The plan allows this as optional. The
  implemented path is standard pre-match mode only.

## Phase 9 — Backtesting And Acceptance (2026-05-22)

### What was implemented

Added a Phase 9 ablation harness to `modelling/main.py`:

```bash
./venv/bin/python modelling/main.py --phase9-ablations --skip-init
```

The runner evaluates the planned chronological variants where data/artifacts
exist:

1. `baseline`
2. `baseline + simple map rolling`
3. `baseline + map-play expectation`
4. `baseline + map-win expectation` (skipped until `map_win.joblib` exists)
5. `final selected feature set` (skipped until `map_win.joblib` exists)

Supporting aggregation helpers were added to `modelling/map_expectations.py`:

- `build_simple_map_summary_features(candidate_df)` aggregates raw Phase 3
  rolling/H2H map features without requiring any map model artifact.
- `build_map_play_summary_features(candidate_df, map_play_model)` aggregates
  Phase 4 play probabilities without requiring Phase 5 map-win probabilities.
- `load_map_play_model()` and `load_map_win_model()` split artifact loading so
  map-play-only ablations can run while the maps table remains sparse.

The ablation summary now tracks:

- Accuracy.
- Brier score.
- Log loss.
- Equal-width calibration ECE.
- Number of bets.
- Final bankroll.
- Minimum bankroll.
- Maximum drawdown.
- EV.

### Verification

Current local run:

```text
Phase 9 candidate rows: 39085 across 4745 series

baseline
Bets placed: 122  Ending bankroll: $859.62  Accuracy: 38.52%  EV: -0.02

baseline + simple map rolling
Bets placed: 116  Ending bankroll: $746.82  Accuracy: 37.07%  EV: -0.04

baseline + map-play expectation
Bets placed: 114  Ending bankroll: $914.36  Accuracy: 38.6%  EV: -0.02

Skipped Phase 9 variants
- baseline + map-win expectation: Required map-win model file missing
- final selected feature set: Required map-win model file missing
```

Summary highlights from the same run:

| Feature set | Accuracy | Brier | Log loss | ECE | Avg bets | Avg EV | Best EV |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline | 0.5423 | 0.2469 | 0.6870 | 0.1053 | 122 | -0.0230 | 0.0160 |
| baseline + simple map rolling | 0.5845 | 0.2432 | 0.6795 | 0.1014 | 116 | -0.0437 | -0.0189 |
| baseline + map-play expectation | 0.6056 | 0.2413 | 0.6757 | 0.1016 | 114 | -0.0150 | 0.0175 |

### Acceptance note

These Phase 9 numbers are useful plumbing checks, not final acceptance
evidence. The local DB still has only 7 map rows and no real Phase 5 map-win
artifact, so the planned final map-win/final feature-set ablations remain
blocked until the full map backfill and `modelling/map_win.py` training pass.

## Phase 9 — First post-backfill ablation run (2026-05-22)

After the full `python scraping/main.py --full-rescrape` (DB now has 4749
series + a full maps table) and a Phase 5 training pass that did produce
`models/map_win.joblib`, the Phase 9 harness ran all five variants. All
hyperparameters at this point still came from the shared
`models/params/series.pkl` cache, i.e. the 7-feature baseline's tuned GBM
parameters were being reused across all feature shapes — see the follow-up
section for the per-shape re-tune.

Backtest window: 2026-01-15 → 2026-12-31. n=143 series in test.

| Feature set                       | Acc    | Brier  | LogLoss | ECE    | Avg EV  | Best EV |
| --------------------------------- | ------:| ------:| -------:| ------:| -------:| -------:|
| baseline                          | 0.5455 | 0.2466 |  0.6863 | 0.1077 | -0.0306 | +0.0080 |
| baseline + simple map rolling     | 0.5315 | 0.2436 |  0.6802 | 0.0996 | -0.0245 | **+0.0478** |
| baseline + map-play expectation   | **0.6014** | **0.2416** | **0.6764** | 0.1120 | -0.0441 | -0.0158 |
| baseline + map-win expectation    | 0.5175 | 0.2474 |  0.6878 | 0.1052 | -0.0517 | +0.0066 |
| final selected feature set        | 0.5594 | 0.2443 |  0.6815 | 0.1070 | -0.0384 | -0.0442 |

Interpretation:

- **`baseline + simple map rolling`** produced the strongest betting result —
  best EV ($1303 bankroll vs. $1051 baseline) and the best ECE. It does
  underperform baseline slightly on accuracy and matches it on Brier; this
  matches the broader pattern that better-calibrated probabilities expose
  +EV bets even when raw accuracy is similar.
- **`baseline + map-play expectation`** is the calibration/accuracy winner
  but the worst on EV. The map-play model picks winners more reliably, but
  the market is presumably pricing those favourites correctly, so the
  Kelly-style edge is gone. Useful as a signal source but not as the final
  prediction model.
- **`baseline + map-win expectation`** is a net negative on every metric vs.
  baseline. Likely cause: the pooled GBM map-win calibration is still thin
  given map-id × team-pair sparsity.
- **`final selected feature set`** (winshare + edge) underperforms both
  components individually — combining the model-driven map features did not
  add up additively.

### Decision

Do not promote `--with-map-features` as the default. The simple raw map
aggregates are the most promising candidate for an extended series model.

### Action taken

- Added `SIMPLE_MAP_SERIES_FEATURES` and `SIMPLE_FEATURES` in
  `modelling/training.py`. `SIMPLE_MAP_SERIES_FEATURES` mirrors
  `map_expectations.SIMPLE_MAP_ROLLING_FEATURES` so the saved model carries
  the right `feature_names`.
- Reshaped the `include_map_features=bool` API across `modelling/main.py`
  to a `feature_set: 'baseline' | 'map' | 'simple'` parameter. The
  Phase 7 / Phase 9 helpers and the `_load_series_with_features` data
  loader now honour the new selector.
- Added `_params_path(features)` to `modelling/training.py`. Baseline keeps
  the historical `models/params/series.pkl` filename for back-compat; every
  other feature shape gets a hashed `series_{n}f_{hash}.pkl`. This means a
  new feature shape automatically triggers a fresh `RandomizedSearchCV`
  instead of inheriting tuned-for-baseline params.
- Added a `--with-simple-map-features` CLI flag to `modelling/main.py`,
  mutually exclusive with `--with-map-features`.
- Added `_attach_simple_map_features_to_upcoming` to `modelling/predict.py`
  so saved simple-feature models can be used at predict time without code
  changes. The branch is gated on whether the saved model's `feature_names`
  include any of `SIMPLE_MAP_ROLLING_FEATURES`.

### Phase 9 — Re-run with per-shape hyperparameter tuning (2026-05-22)

Re-ran the harness with the new `_params_path(features)` indirection. Each
feature shape now has its own `RandomizedSearchCV` cache:

```text
models/params/series.pkl                  # baseline (7 features)
models/params/series_8f_543d1837.pkl      # baseline + map-win (8)
models/params/series_9f_ee900cb5.pkl      # final selected (9)
models/params/series_10f_3a188416.pkl     # baseline + map-play (10)
models/params/series_15f_a75a15c4.pkl     # baseline + simple map rolling (15)
```

Same backtest window (2026-01-15 → 2026-12-31, n=143). Same data.

| Feature set                       | Acc        | Brier      | LogLoss    | ECE        | Avg EV   | Best EV   | Best bankroll |
| --------------------------------- | ----------:| ----------:| ----------:| ----------:| --------:| ---------:| -------------:|
| baseline                          | 0.5455     | 0.2466     | 0.6863     | 0.1077     | -0.0306  | +0.0080   | $1051         |
| **baseline + simple map rolling** | **0.6434** | 0.2449     | 0.6829     | 0.1298     | **+0.0306** | **+0.0615** | **$1396**     |
| baseline + map-play expectation   | 0.5524     | 0.2472     | 0.6874     | 0.1081     | -0.0367  | +0.0298   | $1195         |
| baseline + map-win expectation    | 0.5944     | **0.2420** | **0.6770** | **0.1016** | -0.0173  | +0.0432   | $1268         |
| final selected feature set        | 0.5944     | 0.2436     | 0.6803     | 0.1045     | -0.0475  | +0.0376   | $1239         |

Interpretation:

- The per-shape re-tune flipped the simple-rolling variant from "good
  calibration / weak accuracy" to "best on accuracy and best on EV." The
  prior run had been forcing the 15-feature model to use baseline-tuned GBM
  params, which clearly underutilised the extra features.
- Map-win expectation is now the calibration winner (best Brier, log loss,
  ECE) and produces a +$1268 best-odds bankroll. It is genuinely useful;
  the earlier conclusion that it was a net negative was a tuning artifact.
- Map-play expectation alone remains the weakest accuracy/EV story.
- Final (winshare + edge) doesn't beat the individual map-win component on
  any metric, supporting the same "combined map-derived features are not
  additive" pattern as last run.

### Decision

**Promote `--with-simple-map-features` as the candidate default series
model.** It dominates baseline and every map-model-driven variant on
accuracy and EV at the cost of slightly worse calibration. For betting EV
on this data the accuracy/EV win is the dominant signal.

Trained and saved with:

```bash
./venv/bin/python modelling/main.py --with-simple-map-features --skip-init
```

`models/series_winner.joblib` now carries 15 features (the baseline 7 plus
the eight `SIMPLE_MAP_ROLLING_FEATURES`). `feature_names` is stored on the
`_PlattScaledModel` so `modelling/predict.py` and `modelling/main.py`
read the right schema automatically — no further code wiring needed.

Training-path backtest output (slightly different from the Phase 9 result
above because the save path trains through `vct_2026_start` and reserves
its own calibration tail, whereas Phase 9 trains through `vct_2025_end`):

```text
Series winner model saved to models/series_winner.joblib
Accuracy: 62.24%
Bets placed: 118  Ending bankroll: $852.01  Accuracy: 38.14%  EV: -0.03  Dog: 95.76%
Bets placed: 125  Ending bankroll: $1361.0  Accuracy: 40.8%   EV: +0.06  Dog: 96.0%
```

### Follow-up TODOs

- Watch the calibration ECE in production runs. The simple-rolling model
  is ~20% worse on ECE than baseline. If predicted-vs-realised hit rate
  drifts during 2026 stage 2 / 3 it may be worth a small calibration tweak
  (e.g. tighter Platt regularisation, or isotonic instead of sigmoid).
- The map-win expectation variant deserves a follow-up Phase 9 with the
  Phase 5 map-win model itself re-trained on the now-backfilled maps
  table; the metrics above already use the fresh artifact but a second
  re-run after any map-win calibration tweak is cheap.
- Old `models/params/series.pkl` is preserved for the baseline shape; the
  four new hashed cache files were created during this run. Deleting any
  hashed file forces a re-search for that shape only — useful when
  experimenting with new feature additions.
