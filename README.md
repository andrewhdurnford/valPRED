# Valorant Match Predictor

A machine learning predictor for Valorant esports matches. Scrapes historical match data from vlr.gg, engineers team-form features, trains a gradient boosted classifier, and simulates a betting strategy against historical market odds.

## How it works

### Data collection

Match stats are scraped from vlr.gg for all Tier 1 VCT events (Americas, EMEA, Pacific) going back to 2022, plus Tier 2 matches involving Tier 1 teams. Data is stored in SQLite (`data/valpred.db`). VCT event URLs are configured in `config.toml` and updated each season.

For each series the scraper records: teams, winner, map wins, and per-team aggregate stats (rating, ACS, kills, deaths, assists, first kills, first deaths).

### Feature engineering

Seven features are computed per series:

| Feature | Description |
|---|---|
| `elo_diff` | Difference in Elo ratings at time of match. Standard K=32 system, seeded at 1500. |
| `net_h2h` | Head-to-head series win differential between the two teams. |
| `past_diff` | Difference in maps played in prior VCT seasons (proxy for experience). |
| `rating_diff` | Rolling mean combat rating diff over each team's last 10 series. |
| `acs_diff` | Rolling mean average combat score diff. |
| `fk_net_diff` | Rolling mean net first-contact advantage diff: `(FK - FD)` per team, then differenced. Captures which team wins the opening duel more consistently. |
| `winrate_diff` | Rolling mean series win rate diff over last 10 series. |

Rolling features use a window of 10 series with `min_periods=3`, computed with `closed='left'` to prevent leakage. The full dataset (including T2 matches) is used to warm up rolling windows; only T1 regional matches are used for training and testing.

### Model

A single `GradientBoostingClassifier` predicts the series winner (t1 or t2). The pipeline:

1. **Hyperparameter search** — `RandomizedSearchCV` with `TimeSeriesSplit(n_splits=5)` ensures folds always train on the past and test on the future. Best params are cached in `models/params/series.pkl` to skip re-search on subsequent runs.
2. **Probability calibration** — GBMs compress predicted probabilities towards 0.5, making raw scores unreliable for EV calculations. The most recent 20% of training data (chronologically) is held back to fit a Platt scaling layer (logistic regression on the GBM's raw scores). This maps compressed scores to real win probabilities.

### Betting simulation

The backtest compares the model's calibrated win probability against the market's implied probability (derived from the house odds adjusted for the configured vig). A bet is placed when the model's probability exceeds the market's implied probability — i.e. when the model thinks the market is mispricing the match.

Two simulations are run: one using average market odds and one using best/worst odds across books.

### Results

Trained on 2023–2025 data, backtested on 2026 kickoff:

| Simulation | Bets | Bankroll | Accuracy | EV | Underdog % |
|---|---|---|---|---|---|
| Average odds | 113 | $1,176 | 40.7% | +0.03 | 93.8% |
| Best odds | 123 | $1,429 | 41.5% | +0.07 | 91.1% |

Starting bankroll: $1,000. Fixed $50 bet size. The model heavily favours underdogs — esports books strongly overprice favourites.

## Setup

```bash
pip install -r requirements.txt
python db_init.py          # create SQLite schema (run once)
```

## Running

```bash
# Scrape match data (takes a while — ~4000+ pages)
python scraping/main.py

# Train model and run backtest
python modelling/main.py

# Predict upcoming matches
python modelling/predict.py
```

Update `tier1_events` and `upcoming_events` in `config.toml` at the start of each season.

## Stack

Python, BeautifulSoup, pandas, scikit-learn (GradientBoostingClassifier), joblib, SQLite

## Data sources

- Match stats: https://www.vlr.gg/
- Game data (maps, agents): https://valorant.fandom.com/wiki/VALORANT_Wiki
