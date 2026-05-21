# valpred

Valorant match predictor. Scrapes vlr.gg → engineers features → trains a GBM → simulates betting strategy.

## Structure

```
config.toml  — VCT event URLs + modelling date config (update each season)
paths.py     — absolute path constants, imported everywhere
db_init.py   — run once to create valpred.db schema
market.py    — house vig / implied-probability utilities
scraping/    — vlr.gg scrapers (match links, stats, upcoming matches)
modelling/   — feature engineering, model training, prediction, bet simulation
models/      — saved .joblib models + cached hyperparams
data/
  valpred.db     — SQLite (series, maps, teams, upcoming, results)
  game_data/     — static data (maps.txt, map_pool.txt, agents.txt)
```

## Data Flow

```
vlr.gg
  → link_scraper        →  tier1_match_links.csv
  → stats_scraper       →  series, maps, teams tables
  → elo.py              →  series.elo_diff (updated in place)
  → series.py           →  rolling form features
  → training.py         →  models/series_winner.joblib
  → predict.py          →  results table
```

## Running

```bash
python scraping/main.py    # scrape
python modelling/main.py   # train + backtest
python modelling/predict.py  # predict upcoming matches
```

Scripts use absolute paths via `paths.py` — no need to set a working directory.

## DB Schema

| Table | PK | Description |
|---|---|---|
| `series` | `match_id` | Raw series stats + `elo_diff` written by `elo.py` |
| `maps` | `map_id` | Per-map stats (scraped, kept for future use) |
| `upcoming` | `match_id` | Upcoming matches, replaced each scrape run |
| `teams` | `(id, region)` | Team metadata; `region` derived from event URL slug |
| `results` | — | Betting simulation output |

## Features

Seven features feed the model:

| Feature | How it's built |
|---|---|
| `elo_diff` | Standard Elo (K=32, start 1500) computed over all series chronologically |
| `net_h2h` | Head-to-head series win differential |
| `past_diff` | Maps played in prior seasons — proxy for experience |
| `rating_diff` | Rolling 10-series mean combat rating diff |
| `acs_diff` | Rolling 10-series mean ACS diff |
| `fk_net_diff` | Rolling `(FK−FD)` per team, then differenced — net first-contact advantage |
| `winrate_diff` | Rolling 10-series win rate diff |

Rolling features use `closed='left'` (no leakage). Computed on the full dataset including T2 matches before filtering to T1-only rows, so T2 games warm up a team's rolling window without entering training.

## Model

`training.py` → `_PlattScaledModel` (GBM + Platt scaling layer):

1. **Hyperparameter search** — `RandomizedSearchCV(n_iter=50)` with `TimeSeriesSplit(n_splits=5)`. Folds train on past, test on future. Best params cached in `models/params/series.pkl`; delete this file to re-run the search.
2. **Platt scaling** — GBMs compress probabilities towards 0.5. The most recent 20% of training data (chronologically) is held back to fit a `LogisticRegression` on raw GBM scores. This maps them to real win probabilities, which is necessary for the EV calculation to be meaningful.

The saved `.joblib` contains the full `_PlattScaledModel` object (base GBM + calibration layer).

## Betting Logic

`simulate_bets` in `testing.py` and EV calculation in `predict.py` both follow the same logic:

- Market implied probability for each side is inferred from the stored odds adjusted for house vig (`market.py`)
- A bet is placed when `model_probability > market_implied_probability`
- EV = `(model_prob × payout) − (1 − model_prob)`

## Architecture Notes

**Single model:** The old pipeline had three stages — `map_pick`, `map_win`, `series_winner`. This was collapsed to a single `series_winner` model. Per-map win rates are confounded by agent/comp matchups that raw stats can't capture, and Tier 1 sample sizes are too small for per-map splits to be reliable. The `maps` table is still scraped. Old logic is on the `archive/map-by-map` git branch.

**Temporal integrity:** All train/eval splits are chronological. `TimeSeriesSplit` for CV, calibration set is the most recent 20% of training data, backtesting window is set in `config.toml` and applied in `main.py`.

## Current Status

- `series_winner.joblib` trained on 2023–2025, backtested on 2026 kickoff (~57% accuracy, +0.03/+0.07 EV)
- Update `upcoming_events` in `config.toml` each stage, then re-scrape and run `predict.py`

## Do Not Reintroduce

| File | Issue |
|---|---|
| `link_scraper.py` | Module-level scraping calls — all scraping inside functions only |
| `scraping/main.py` | Name collision between `update_tier1` function and imported name |
| `upcoming_match_scraper.py` | `get_all_matches()` at module level — must be under `__main__` |
| `modelling/main.py` | `main()` at module level — must be under `__main__` |
| `testing.py` | `simulate_bets` must return `df`, not `None` |
| `training.py` | Param caching must stay active — deleting `series.pkl` triggers re-search |
| `training.py` | Do not use random `train_test_split` — data is temporal, leakage is real |
