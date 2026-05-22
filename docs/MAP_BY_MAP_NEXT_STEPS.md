# Map-By-Map Reintroduction — Next Steps

## Completion status (as of 2026-05-21)

All nine phases of `MAP_BY_MAP_REINTRO_PLAN.md` are **implemented in code**.
None are **validated on real map data**. The local `maps` table still only
holds the 7-row Phase 1 smoke test, so every map-derived feature, model, and
ablation produced so far is either neutral-imputed or run against a
synthetic dataset.

| Phase | Code | Real-data evidence | Blocker |
|---|---|---|---|
| 1 — Per-map scrape (`scraping/stats_scraper.py`) | done | 7-row smoke test only | full scrape not yet executed |
| 2 — Map rolling features (`modelling/maps.py`) | done | none — all NaN today | needs Phase 1 backfill |
| 3 — Candidate map rows (`modelling/maps.py`) | done | 39,085 candidate rows build cleanly; map-history columns 100% NaN | needs Phase 1 backfill |
| 4 — Map-play model (`modelling/map_play.py`) | done; `models/map_play.joblib` saved | learns from map id + veto rates only; map stats neutral | OK as a first baseline; will improve after backfill |
| 5 — Map-win model (`modelling/map_win.py`) | done; artifact intentionally **not** saved | 0 eligible rows in configured window | needs Phase 1 backfill |
| 6 — Series collapse (`modelling/map_expectations.py`) | done | smoke test only; CLI exits because `map_win.joblib` is missing | needs Phase 5 artifact |
| 7 — Extended series model (`modelling/main.py --with-map-features` / `--phase7-ablations`) | done | baseline runs; map-extended path skipped — missing `map_win.joblib` | needs Phase 5 artifact |
| 8 — Upcoming prediction (`modelling/predict.py`) | done | no upcoming tier-1 matches in current `upcoming` table | needs upcoming scrape + Phase 5 artifact |
| 9 — Ablation harness (`modelling/main.py --phase9-ablations`) | done | three of five variants run; map-win variants skipped | needs Phase 5 artifact |

Net evaluation: the plumbing is complete and self-consistent. The
implementation log is thorough and notes every deviation. The pipeline is
ready to be exercised end-to-end as soon as a full historical re-scrape
populates the `maps` table.

---

## Sequencing — what to do after the next full scrape

The order below matters; later steps assume earlier ones finished cleanly.

### Step 1 — Refresh `config.toml`

- Confirm `upcoming_events` reflects current active VCT stage URLs.
- Confirm `modelling.vct_*_start` / `vct_*_end` windows are still the ones
  you want for train and test.

### Step 2 — Full scrape

```bash
python scraping/main.py
```

Notes:

- `process_match_link` now performs two fetches per match (main page +
  economy tab). With the configured `request_delay = 0.2 s` this roughly
  doubles wall time vs. the prior single-fetch scrape — budget accordingly.
- The scraper UPSERTs on `map_id`, so re-running is safe and idempotent.
- If new agents have shipped since the last scrape, they will be appended
  to `data/game_data/agents.txt` automatically.

Sanity check after it finishes:

```sql
SELECT COUNT(*) FROM maps;
SELECT COUNT(*) FROM maps WHERE t1_pistols IS NULL;
SELECT MIN(date), MAX(date) FROM maps;
```

Expected ballpark: roughly `2.5 × COUNT(series)` map rows (BO3 average).
A pistol-NULL rate of more than ~5–10% means the economy-tab parser
should be re-inspected before continuing.

### Step 3 — Re-run Elo over the refreshed series

```bash
python modelling/elo.py
```

Required because rolling features and Phase 3 context all read
`series.elo_diff`.

### Step 4 — Validate Phase 2 / Phase 3 on real data

```bash
./venv/bin/python modelling/maps.py
```

What to check before trusting the rest:

- `MAP_ROLLING_FEATURES` no longer all-NaN — confirm `map_wr_diff` and
  friends populate after `min_periods=3` priors.
- `sample_size_diff` distribution looks reasonable (bounded by `±window`).
- Phase 3 NaN audit: `t1_pick_rate` / `h2h_map_t1_winrate` NaN rates drop
  sharply once veto-known series and per-map history accumulate.
- Spot-check one series by hand: for a recent veteran-vs-veteran match,
  the diff signs should match intuition (e.g. a known Ascent main vs. a
  team that bans Ascent should produce a positive `map_wr_diff` for the
  Ascent main, and the opponent's Ascent `t2_ban_rate` should be high).

If anything in the audit looks wrong, **stop here** and fix before
retraining downstream models.

### Step 5 — Retrain Phase 4 map-play model on real data

```bash
./venv/bin/python modelling/map_play.py
```

Compare against the pre-backfill smoke-test numbers in the implementation
log (Brier 0.1795, log loss 0.5397, ECE 0.1398, top-k recall 52.6%).
Backfilled map features should push at least one of these in the right
direction. If not, the model is leaning on the same identity/veto signal
as before — note it in the log and proceed.

### Step 6 — Train Phase 5 map-win model

```bash
./venv/bin/python modelling/map_win.py
```

This is the gating artifact. Look for:

- `models/map_win.joblib` actually saved (the script refuses if there are
  <50 base rows or <20 calibration rows with two classes).
- Calibration ECE < 0.10 ideally; > 0.15 is a yellow flag for using the
  output in series aggregation.
- AUC well above 0.5 on the chronological test window. The plan called
  out that maps are pooled; if AUC is flat across maps, consider a Phase
  9 ablation with a logistic baseline before promoting the GBM.

### Step 7 — Phase 6 sanity check

```bash
./venv/bin/python modelling/map_expectations.py
```

Inspect `expected_total_maps`, `map_play_mass`, and the `map_edge`
distribution. Sane values:

- `map_play_mass` ≈ 3 per series for BO3 (sums of map-play probabilities
  across candidates).
- `expected_total_maps` clustered around the same mode.
- `map_winshare` centred near 0.5 with a sensible spread.

If `map_play_mass` is far from 3 (e.g. ≈ 7 because every map is rated
near 0.4), the Phase 4 calibration is the culprit, not Phase 6.

### Step 8 — Phase 7 series ablation

```bash
./venv/bin/python modelling/main.py --phase7-ablations --skip-init
./venv/bin/python modelling/main.py --phase9-ablations --skip-init
```

Compare baseline vs. baseline + map-expectation features. Decision rules:

- **Promote `--with-map-features`** only if the extended model improves at
  least one of {accuracy, Brier, log loss, ECE} without regressing the
  others by more than a noise-level amount **and** the betting EV is at
  least as good as baseline.
- Calibration is the priority metric per the plan — a small EV win on a
  worse-calibrated model is not acceptance.
- Record the comparison in `docs/map_by_map_implementation_log.md` as a
  Phase 9 result. Do not silently switch defaults.

If accepted, retrain and save the extended model:

```bash
./venv/bin/python modelling/main.py --with-map-features --skip-init
```

This overwrites `models/series_winner.joblib` with the extended-feature
artifact (Platt-scaled, with `feature_names` baked in).

### Step 9 — Upcoming prediction

```bash
./venv/bin/python modelling/predict.py
```

If Step 8 accepted the extended model:

- `predict.py` will route through Phase 3 candidate construction, Phase 4
  scoring, and Phase 6 collapse automatically, because it reads
  `feature_names` off the saved series model.
- Both `models/map_play.joblib` and `models/map_win.joblib` must exist.

If Step 8 rejected the extended model:

- The baseline seven-feature `series_winner.joblib` remains in place and
  `predict.py` skips the map-artifact path entirely. No further action.

### Step 10 — Memory and docs

- Update `MEMORY.md` / `project_status.md` so the next session does not
  re-discover that this work shipped. Note (a) whether map features were
  accepted, (b) the new metrics, and (c) the date of the validating
  scrape.
- Append a Phase 1 backfill / Phase 5 training entry to
  `docs/map_by_map_implementation_log.md` with real numbers replacing the
  placeholders.

---

## Known follow-ups deliberately deferred

These were called out in the implementation log and are intentionally
**not** part of the immediate post-scrape sequence. Pick them up only if
Step 8's results justify the extra surface area.

- **Richer per-map scraping fields** (eco rounds, fullbuy W/L, multi-kills,
  clutches, plants/defuses, economy rating). Requires schema migration in
  `db_init.py`, a third fetch (performance tab), and new parser helpers.
  Old references on `archive/map-by-map` and at commit `ddd5b31`.
- **Per-map first-deaths column** (`t{1,2}_fds`). Phase 2 currently
  derives FD from the opponent's FK; harmless but an explicit column
  would simplify future stats.
- **Map id 11 (Corrode) for pre-existence dates.** The active pool helper
  blindly includes maps not listed as out of pool. Gate `MAX_MAP_ID` by
  first-observed date in the maps table once data is populated.
- **Per-map agent composition features.** Schema has `t1_agent1..5` and
  `t2_agent1..5`; nothing consumes them yet. Natural candidate is a
  composition-similarity or comp-novelty feature in Phase 5.
- **`include_groups=False` cleanup** in the Phase 3 `groupby.apply` calls
  to silence the pandas deprecation warning.
- **Additional map expectation features** (`best_map_edge`,
  `worst_map_edge`, `veto_confidence`, `expected_close_maps`). Only add
  after `map_winshare` / `map_edge` show real signal — the plan warns
  explicitly against widening the feature space prematurely.
- **Known-veto / live-veto upcoming prediction mode.** Plan allows this
  as optional; Phase 8 currently implements pre-match mode only.
- **Re-search hyperparameters** (`models/params/series.pkl`). The cached
  GBM params were tuned on the seven-feature baseline. If the extended
  feature set is accepted, delete that file and let Phase 7 re-run the
  `RandomizedSearchCV` with the new feature shape.

---

## Quick-look summary for the next session

1. Update `config.toml`.
2. `python scraping/main.py` (full re-scrape with new map rows).
3. `python modelling/elo.py`.
4. `python modelling/maps.py` (verify Phase 2 / 3 outputs).
5. `python modelling/map_play.py` (retrain).
6. `python modelling/map_win.py` (must produce the artifact).
7. `python modelling/map_expectations.py` (Phase 6 sanity).
8. `python modelling/main.py --phase9-ablations --skip-init`, decide
   whether to promote map features, then optionally
   `python modelling/main.py --with-map-features --skip-init`.
9. `python modelling/predict.py`.
10. Update implementation log + memory.
