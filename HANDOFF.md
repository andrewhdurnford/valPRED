# valpred — Handoff / Revival Plan

Valorant match predictor. Scrapes vlr.gg → processes features → trains GBMs → simulates betting strategy. Historically yielded ~1.2–1.4 EV on Tier 1 matches with 72% of bets on underdogs.

**Stack:** Python, BeautifulSoup, pandas, scikit-learn (GradientBoostingClassifier), joblib

---

## Current State

- `config.toml`, `paths.py`, `db_init.py`, and `data/valpred.db` now exist.
- Scraping storage migration is complete: scraper event URLs come from `config.toml`; scraper writes go to SQLite; imports no longer trigger scraping or file opens.
- `data/valpred.db` has the target tables: `maps`, `results`, `series`, `teams`, `upcoming`.
- `data/raw/` and `data/tier1/` CSV references still remain in `modelling/` and need the next migration chunk.
- Trained models exist in `models/` but training data is gone
- `scraping/tier1_match_links.csv` has 4032 links (2024), `match_links.csv` has 18298 — also stale

---

## Architectural Decisions

### Model simplification — map-by-map approach shelved

The old pipeline had three stages: a `map_pick` model (veto rates → which maps get played), a `map_win` model (per-map performance stats → per-map winner), and a `series_winner` model that took the aggregated `winshare` as a feature.

**Decision: collapse to a single `series_winner` model.** Per-map win rates are confounded by team comp (agent/comp matchups strongly influence map outcomes in ways stats can't capture), and Tier 1 sample sizes are too small for per-map splits to be reliable. The `map_win` and `map_pick` models are removed entirely.

The `maps` table (individual map results) is still scraped and stored. If the series model is found to be systematically wrong on specific maps during backtesting, the map-by-map approach is worth revisiting — but would need comp/agent data to be meaningful.

**If revisiting:** the old `map_win`/`map_pick` logic lives in `archive/maps.py`, `modelling/maps.py`, and `modelling/series.py` in git history on the `archive/map-by-map` branch.

---

## Storage Consolidation (partially implemented)

The current storage is fragmented — 8+ CSVs across 5 directories, 28+ hardcoded event URLs in Python source, and CWD-relative paths that break on import. Replace with:

- **`config.toml`** — all VCT event URLs + date config, edited each season instead of touching Python
- **`paths.py`** — absolute path constants, imported everywhere instead of string literals
- **`data/valpred.db`** — SQLite, replaces all tabular CSVs
- **`db_init.py`** — one-time schema setup

### New directory structure

```
valpred/
  config.toml               ← all VCT event URLs + modelling date config
  paths.py                  ← absolute path constants
  db_init.py                ← run once to create valpred.db schema
  data/
    valpred.db              ← SQLite (replaces all CSVs)
    game_data/              ← unchanged (maps.txt, map_pool.txt, agents.txt)
  scraping/
    tier1_match_links.csv   ← kept (flat URL list)
    new_tier1_match_links.csv
    link_scraper.py
    stats_scraper.py
    upcoming_match_scraper.py
    main.py
  modelling/
    elo.py
    series.py
    training.py
    testing.py
    predict.py
    main.py
```

Directories removed: `data/raw/`, `data/tier1/` and all subdirs. `modelling/maps.py` removed (map-by-map shelved).

### DB schema

| Table | PK | Description |
|---|---|---|
| `series` | `match_id` | Raw match series stats + `elo_diff` added by `elo.py` |
| `maps` | `map_id` | Raw per-map match stats (kept for post-hoc analysis) |
| `upcoming` | `match_id` | Upcoming match data, replaced each run |
| `teams` | `(id, region)` | Team metadata with `region` derived from event URL slug |
| `results` | — | Betting simulation results |

### `config.toml` format

```toml
[scraping]
site = "https://www.vlr.gg"
start_date = "2022-03-30"

tier1_events = [
    # 2024
    "/event/2004/champions-tour-2024-americas-stage-1/regular-season",
    # ... add each season
]

upcoming_events = [
    "/event/2860/vct-2026-americas-stage-1/group-stage",
    "/event/2863/vct-2026-emea-stage-1/group-stage",
    "/event/2775/vct-2026-pacific-stage-1/group-stage",
]

[modelling]
vct_2022_start = "2022-03-30"
vct_2023_start = "2023-02-13"
vct_2024_start = "2024-02-16"
vct_2024_end   = "2024-08-25"
```

### `paths.py` format

```python
from pathlib import Path
ROOT            = Path(__file__).resolve().parent
DB              = ROOT / "data" / "valpred.db"
GAME_DATA       = ROOT / "data" / "game_data"
MAPS_TXT        = GAME_DATA / "maps.txt"
MAP_POOL        = GAME_DATA / "map_pool.txt"
MATCH_LINKS     = ROOT / "scraping" / "tier1_match_links.csv"
NEW_MATCH_LINKS = ROOT / "scraping" / "new_tier1_match_links.csv"
MODELS          = ROOT / "models"
CONFIG          = ROOT / "config.toml"
```

### Key migration steps

**Scraping:**
- Done: `link_scraper.py` loads `tier1_events` from `config.toml`, writes match-link files via `paths.py`, and writes team metadata to `teams` with `region` derived from the event slug.
- Done: `stats_scraper.py` moved match-link reads inside functions, imports `fetch_data` from `link_scraper.py`, uses `enumerate` for progress, and writes scraped `series` / `maps` data to SQLite.
- Done: `upcoming_match_scraper.py` loads `upcoming_events` from `config.toml`, writes the `upcoming` table, and is guarded by `if __name__ == "__main__"`.
- Done: `scraping/main.py` aliases `update_tier1` to `scrape_tier1` and is guarded by `if __name__ == "__main__"`.
- Done: `link_scraper.py` has basic sleep + exponential backoff in `fetch_data`.

**Modelling:**
- `elo.py`: write to `series` table instead of `data/tier1/series.csv`
- `series.py`: replace `pd.read_csv("data/tier1/teams/amer.csv")` etc. with `pd.read_sql("SELECT id FROM teams WHERE region=?", con, params=["amer"])`
- `training.py`: replace `open('models/params/pick.pkl')` with `open(MODELS / "params" / "map.pkl")`
- `main.py`: replace all CSV reads/writes with SQL; pull date strings from `config.toml`

---

## Bugs Fixed (do not re-introduce)

### Scraping

| File | Issue |
|---|---|
| `link_scraper.py` | `get_tier1_matchlinks()` called at module level |
| `stats_scraper.py` | Duplicate `fetch_data` — import from `link_scraper` instead |
| `scraping/main.py` | `update_tier1()` shadowed import — alias to `scrape_tier1` |
| `stats_scraper.py` | `links.index(link)` O(n) — use `enumerate` |
| `upcoming_match_scraper.py` | `get_all_matches(7)` at module level — guard with `__main__` |
| `stats_scraper.py`, `link_scraper.py` | No rate limiting — add `time.sleep` + exponential backoff |

### Modelling

| File | Issue |
|---|---|
| `series.py` | Module-level CSV reads — wrap in `try/except FileNotFoundError` |
| `modelling/main.py` | `main()` at module level — guard with `__main__` |
| `series.py` | `get_map_in_pool`: `return` was inside for-loop |
| `testing.py` | `simulate_bets` returned `None` — must return `df` |
| `training.py` | Param caching was commented out — keep active to skip GridSearch |

---

## Work Plan

### Phase 1 — Storage consolidation

Scraping side is complete. Remaining Phase 1 work is the modelling CSV → SQLite migration described above. Verify the completed scraping chunk with:

```bash
python3 db_init.py
sqlite3 data/valpred.db ".tables"
# expected: maps  results  series  teams  upcoming

rg -n "data/raw|data/tier1|read_csv|to_csv" scraping/
# expected: no output

PYTHONPATH=/private/tmp/valpred_deps python3 - <<'PY'
import sys
sys.path.insert(0, "scraping")
import link_scraper, stats_scraper, upcoming_match_scraper, main
print("imports ok")
PY
```

Then complete the modelling side and verify the whole repo with:

```bash
python3 db_init.py
sqlite3 data/valpred.db ".tables"
# expected: maps  results  series  teams  upcoming

rg -n "data/raw|data/tier1|read_csv|to_csv" scraping/ modelling/
# expected: no output
```

### Phase 2 — Scrape fresh data

Event URLs in `config.toml` are already updated through 2026 Stage 1. Run:

```bash
python scraping/main.py
```

Takes a while (~4000+ match pages).

### Phase 3 — Retrain

```python
# modelling/main.py
init()
train_series_win_model('2023-02-13', '2024-02-16')
test_series_winner_model('2024-02-16', '2025-01-01')  # validate on 2024 data
```

### Phase 4 — Live predictions

Run `predict.py` once upcoming data is scraped.

---

## Data Flow (target state)

```
vlr.gg
  → link_scraper  → series table, maps table, teams table
  → elo.py        → series table (adds elo_diff)
  → training.py   → models/series_winner.joblib
  → predict.py    → predictions for upcoming matches
```
