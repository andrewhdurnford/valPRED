# Map-By-Map Reintroduction Plan

## Goal

Reintroduce map-level information into the Valorant series prediction pipeline while preserving the current improvements:

- SQLite-backed data flow.
- Temporal train/test integrity.
- Rolling features computed without leakage.
- GBM-based series model with cached hyperparameters.
- Platt-scaled probabilities for betting EV.
- Vig-aware betting logic.

This plan is intentionally directional. The executor should make local implementation decisions based on code reality, sample size, scraper reliability, and backtest results. Any meaningful deviation or assumption must be logged.

## Required Executor Log

Create or update an implementation log as work proceeds, for example:

```text
docs/map_by_map_implementation_log.md
```

The log should include:

- Assumptions made.
- Deviations from this plan and why.
- Data availability issues.
- Scraper reliability issues.
- Feature definitions that changed during implementation.
- Backtest results and interpretation.
- Any TODOs deliberately left for later.

Do not silently change methodology. If a choice is ambiguous, choose a reasonable path and record the reasoning.

## Current Baseline To Preserve

The current model is a single series-winner model using:

- `elo_diff`
- `net_h2h`
- `past_diff`
- `rating_diff`
- `acs_diff`
- `fk_net_diff`
- `winrate_diff`

Important properties to preserve:

- Historical rows are sorted chronologically before model fitting.
- Cross-validation uses `TimeSeriesSplit`.
- Calibration uses the most recent chronological slice of training data.
- Rolling features use prior data only.
- Rolling features are computed on the broader dataset before filtering to Tier 1 training rows.
- Hyperparameter caching remains active.
- Betting decisions use market probabilities adjusted for vig.

Before extending the model, verify the current baseline still runs and record its accuracy, betting output, and any calibration metric the executor chooses to track.

## Known Current Gaps

At the time this plan was written:

- The current SQLite `maps` table exists but may not be populated.
- The current scraper stores series-level aggregate stats but does not appear to persist per-map rows.
- The current `maps` schema is slimmer than the older map-by-map pipeline expected.
- Old map logic relied on fields such as economy, retake, post-plant, KAST, clutches, and attack/defense ratings that may no longer be scraped.
- Upcoming matches do not have known vetoes unless the prediction is made after veto information is available.

The executor should verify these conditions locally before implementing.

## High-Level Method

Do not restore the old map-by-map pipeline wholesale.

Instead, use the old idea as inspiration:

1. Estimate which maps are likely to be played.
2. Estimate each team’s win probability on each candidate map.
3. Collapse those map expectations into series-level features.
4. Feed those features into the current series-winner model.

Map data should enter as pre-match expected map advantage, not as leaked knowledge of which maps were actually played.

## Phase 1: Restore Per-Map Data

Inspect the current scraper and database schema.

Minimum target fields:

- `map_id`
- `t1`
- `t2`
- `date`
- `winner`
- `map`
- `t1_rds`
- `t2_rds`
- `t1_pistols`
- `t2_pistols`
- `t1_agent1` through `t1_agent5`
- `t2_agent1` through `t2_agent5`
- `t1_fks`
- `t2_fks`
- `t1_rating`
- `t2_rating`
- `t1_acs`
- `t2_acs`
- `t1_kills`
- `t2_kills`
- `t1_assists`
- `t2_assists`
- `t1_deaths`
- `t2_deaths`

If richer fields are still reliably available from vlr.gg, the executor may restore them, but should not block the first implementation on them.

Expected work:

- Add or restore per-map parsing in `scraping/stats_scraper.py`.
- Write map rows to the `maps` table using the existing SQLite/upsert style.
- Keep scraping inside functions.
- Avoid module-level scraping calls.
- Make schema migrations additive where practical.
- Re-scrape or refresh enough data to populate `maps`.

Record scraper assumptions and missing fields in the implementation log.

## Phase 2: Create Map-Level Feature Engineering

Add a current-style map feature module, likely:

```text
modelling/maps.py
```

The executor may choose a different filename if it better fits the codebase, but should log the reason.

Suggested map rolling features:

- `map_wr_diff`
- `round_share_diff`
- `pistol_diff`
- `rating_diff`
- `acs_diff`
- `fk_net_diff`
- `kpm_diff`
- `dpm_diff`
- `sample_size_diff`

Feature rules:

- Use only maps before the target match date.
- Prefer rolling or expanding calculations that are equivalent to `closed='left'`.
- Compute features on the broad historical map dataset before Tier 1 filtering where useful.
- Add sensible fallbacks when a team has little history on a specific map.
- Keep feature names distinct from existing series feature names when merging.

Suggested fallback hierarchy:

1. Team-on-specific-map history.
2. Team-on-all-maps history.
3. Neutral defaults.

The executor should choose actual windows and minimum sample thresholds after checking available data.

## Phase 3: Build Candidate Map Rows For Each Series

For every historical series, create one row per candidate map.

Candidate rows should include:

- Match identity and date.
- Team ids.
- Map id.
- Whether the map was actually played, when known.
- Whether the map was banned or absent, when known.
- Team pick/ban/play rates before the match.
- Head-to-head map history where available.
- Map-level rolling stat diffs from Phase 2.
- Series-level context needed later, such as `elo_diff`, `net_h2h`, and `past_diff`.

Important:

- Do not use actual played status as a feature for predicting upcoming matches.
- Actual played status is only a label for historical map-play modelling.
- Veto columns may be missing for some historical rows; handle this explicitly.

## Phase 4: Train A Map-Play Model

Train a model that estimates:

```text
P(map is played | teams, date, map, prior map/veto history)
```

The executor should choose a simple starting model. Good candidates:

- Logistic regression.
- Gradient boosting classifier with chronological validation.

Preserve temporal discipline:

- Chronological split.
- No random `train_test_split`.
- No future veto or outcome leakage.

The old pipeline used a map-pick/play model. Reintroduce the concept, not necessarily the exact model.

## Phase 5: Train A Map-Win Model

Train a model that estimates:

```text
P(t1 wins map | teams, date, map, prior map stats)
```

Starting recommendation:

- One calibrated model across all maps with `map` included as a feature.

Alternative:

- Per-map models, only if sample size and backtests justify them.

Use actual played historical maps for labels. Do not include future or same-match stats in features.

Candidate features:

- Map rolling stat diffs.
- Team map win-rate diff.
- Round-share diff.
- Pistol diff.
- Rating and ACS diffs.
- FK/net first-contact diff.
- Sample size features.
- Map id.

Calibration matters because outputs feed betting EV indirectly. If the executor uses GBM-style models here, consider calibration for map-win probabilities too.

## Phase 6: Collapse Map Expectations Into Series Features

For each series, calculate map-derived summary features from candidate maps.

Suggested formulas:

```text
expected_t1_maps = sum(map_play_prob * map_win_prob)
expected_t2_maps = sum(map_play_prob * (1 - map_win_prob))
map_winshare = expected_t1_maps / (expected_t1_maps + expected_t2_maps)
map_edge = expected_t1_maps - expected_t2_maps
```

Additional possible features:

- `best_map_edge`
- `worst_map_edge`
- `map_pool_depth_diff`
- `veto_confidence`
- `expected_close_maps`

The executor should start small and expand only if backtests support it.

## Phase 7: Extend The Series Model

Extend the current series model features with the strongest map-derived features.

Likely first additions:

- `map_winshare`
- `map_edge`
- One uncertainty or sample-size feature, if useful.

Keep the current series model architecture unless results suggest otherwise:

- GBM.
- `TimeSeriesSplit`.
- Cached hyperparameters.
- Platt scaling.
- Chronological calibration split.

Do not remove the current seven baseline features unless an ablation clearly supports that.

## Phase 8: Upcoming Match Prediction

For upcoming matches:

- Compute Elo and existing series rolling features as today.
- Build candidate map rows from active map pool and pre-match history.
- Estimate map-play probabilities.
- Estimate map-win probabilities.
- Collapse to series map features.
- Run the extended series model.
- Compute EV with the current vig-aware logic.

If actual vetoes are available before prediction time, the executor may support an optional mode that uses known vetoes. This should be separate from the standard pre-match mode and clearly logged.

## Phase 9: Backtesting And Acceptance

Run ablations in chronological backtests:

1. Current baseline.
2. Baseline plus simple map rolling features.
3. Baseline plus map-play expectation.
4. Baseline plus map-win expectation.
5. Final selected feature set.

Track at least:

- Accuracy.
- Number of bets.
- EV.
- Bankroll path.
- Brier score or log loss.
- Calibration sanity check.

Acceptance should not be based on one lucky EV run. Prefer changes that improve calibration or maintain accuracy while improving betting selectivity.

## Guardrails

- No random train/test splits for model selection or reported backtests.
- No use of same-match map stats for pre-match predictions.
- No use of actual played maps as prediction features unless modelling a known-veto/live-veto mode.
- No module-level scraping calls.
- Keep parameter caching active.
- Keep `simulate_bets` returning a dataframe.
- Preserve current house-vig handling.
- Prefer small, testable changes over a large rewrite.

## Suggested Milestones

1. Populate `maps` reliably.
2. Produce leakage-safe map rolling features.
3. Produce candidate map rows for historical series.
4. Train and evaluate map-play model.
5. Train and evaluate map-win model.
6. Generate series-level map expectation features.
7. Extend series model and run ablations.
8. Wire upcoming predictions.
9. Document final methodology and results.

## Open Questions For Executor

- Is the current vlr.gg markup still sufficient for richer economy/performance stats?
- Are per-map models viable with current sample size, or is one pooled model better?
- What rolling windows produce stable map features?
- Should map pool history come from `map_pool.txt`, current active maps, or scraped veto evidence?
- How should missing veto rows be handled in map-play training?
- Should map-level probabilities be calibrated separately before series aggregation?

Answer these through implementation evidence, and record the answers in the implementation log.
