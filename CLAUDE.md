# valpred

Valorant match predictor. Scrapes vlr.gg → engineers features → trains GBMs → simulates betting strategy.

## Structure

```
scraping/    — vlr.gg scrapers (match links, stats, upcoming matches)
modelling/   — feature engineering, model training, prediction, bet simulation
models/      — saved .joblib models
data/raw/    — scraper output (series + map stats CSVs)
data/tier1/  — processed data and results
data/game_data/ — static game data (map pool history, agent/map lists)
```

## Data Flow

```
vlr.gg → link_scraper → stats_scraper → elo/maps/series processing → training → predict
```

## Running

All scripts are run from their own directory. Set working directory before running modelling scripts — they use relative paths for data and models.

Scraping entry point: `scraping/main.py`  
Modelling entry point: `modelling/main.py`

## Notes

- Only Tier 1 matches are modelled (AMER, EMEA, Pacific, CN leagues)
- CN matches are excluded from training due to limited data
- Event URLs in `scraping/link_scraper.py` and `scraping/upcoming_match_scraper.py` must be updated each season
- See `HANDOFF.md` for full known bugs and revival plan
