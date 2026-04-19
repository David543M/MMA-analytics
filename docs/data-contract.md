# Data Contract

This document defines the minimum shared contract between `MMA-analytics` and `mma-grid`.

It focuses on tables that are read directly by the app and populated by the canonical data pipeline.

See also:

- [Documentation Hub](./README.md)
- [War Room Operating Model](./OPERATING_MODEL.md)
- [MMA Analytics Operating Model](./MMA_ANALYTICS_OPERATING_MODEL.md)

Use this document as the operational reference when:

- changing ingestion schema
- debugging data issues
- verifying whether a UI bug is actually a data bug
- planning cross-repo features

## Contract Principles

- `MMA-analytics` owns canonical sports data.
- `mma-grid` consumes that data and may apply presentation-only fallbacks.
- The app should not silently repair ingestion bugs in a way that hides broken contracts.
- Nullable fields are allowed only when the app knows exactly how to degrade gracefully.

## Environment Rule

Before using this contract, verify both repos are targeting the same Supabase environment.

Most “the UI is wrong” issues in a two-repo setup are actually:

- fresh data written to one environment
- stale data read from another

## Table: `fighter_profiles`

### Owner

- `MMA-analytics`

### Purpose

Primary app-facing fighter profile table used across:

- `Fighters`
- `Fighter Detail`
- `Compare`
- `Rankings`
- `Overview`

### Required fields

- `id`
- `name`
- `record`
- `division`
- `last_fight_date`
- `violence_score`
- `momentum`
- `finish_rate`
- `slpm`
- `str_def`
- `td_avg`
- `td_def`
- `sub_avg`

### Optional but important fields

- `nickname`
- `country`
- `city`
- `camp`
- `stance`
- `reach_cm`
- `height_cm`
- `leg_reach_cm`
- `weight_kg`
- `portrait_*` fields
- `strike_zones`

### App expectations

- `name` must be stable and canonical.
- `division` should be normalized to the taxonomy used by rankings and compare filters.
- `record` should be app-readable as a compact display string.
- `violence_score` and `momentum` are expected to be non-null for primary ranking and compare surfaces.

### Fallback behavior in `mma-grid`

- missing portrait -> generated/default portrait fallback
- missing strike zones -> `Targeting breakdown unavailable`
- missing country/camp/location -> partial rendering allowed

### Freshness expectation

- updated whenever ingestion refreshes fighter-derived stats

## Table: `event_overview`

### Owner

- `MMA-analytics`

### Purpose

Primary event feed table used across:

- `Events`
- `Event Detail`
- `Overview`
- `War Room`

### Required fields

- `id`
- `slug`
- `name`
- `date`
- `status`
- `total_bouts`
- `completed_bouts`
- `scheduled_bouts`

### Optional but important fields

- `venue`
- `city`
- `country`
- `source_url`
- derived counts such as:
  - `main_card_bouts`
  - `prelims_bouts`
  - `early_prelims_bouts`
  - `linked_bouts`

### App expectations

- `slug` must be stable enough for routing.
- `status` should be normalized to app-readable event states.
- `total_bouts` should reflect the event card actually available to the app.

### Fallback behavior in `mma-grid`

- missing location fields -> `Location pending`
- missing linked counts -> derived from `event_bouts` where possible

### Freshness expectation

- at least daily around live/upcoming cards

## Table: `event_bouts`

### Owner

- `MMA-analytics`

### Purpose

Canonical per-bout data for:

- `Event Detail`
- compare-ready links
- `War Room`
- event prep workflows

### Required fields

- `id`
- `event_id`
- `bout_order`
- `card_segment`
- `fighter_a_name`
- `fighter_b_name`
- `status`

### Strongly expected fields

- `fighter_a_id`
- `fighter_b_id`
- `weight_class`
- `is_title_fight`
- `winner_corner`
- `method`
- `round`
- `time`

### App expectations

- `fighter_a_id` and `fighter_b_id` should be filled whenever a canonical fighter match is possible.
- compare links are only possible when both fighters are linked canonically.
- `card_segment` should be normalized for app grouping.

### Fallback behavior in `mma-grid`

- if one or both fighter IDs are missing:
  - event renders normally
  - compare CTA becomes unavailable
  - app may show `Compare unavailable` or `Mapping pending`

### Freshness expectation

- at least daily
- more frequently near live cards if supported

## Table: `ufc_rankings`

### Owner

- `MMA-analytics`

### Purpose

Current official UFC ranking boards used by:

- `Rankings`
- `Overview` ranking references
- `Custom vs Official` comparison reads

### Required fields

- `category_key`
- `category_label`
- `category_group`
- `sort_order`
- `fighter_name`
- `is_champion`
- `is_interim`
- `scraped_at`

### Strongly expected fields

- `rank_position`
- `fighter_id`
- `division_label`
- `source_label`
- `source_url`

### App expectations

- boards must be complete when official data exists
- divisions should usually contain:
  - champion
  - ranks `1` through `15`
- p4p boards should usually contain `15` rows
- category groups must normalize to:
  - `p4p`
  - `men`
  - `women`

### Fallback behavior in `mma-grid`

- if an official board is missing, app may show a `War Room read` fallback board
- if `fighter_id` is null, app can still render the row but cannot deep-link reliably

### Freshness expectation

- daily

## Table: `ufc_ranking_snapshots`

### Owner

- `MMA-analytics`

### Purpose

Historical ranking snapshots used for:

- movement arrows
- ranking deltas
- `up/down` indicators in `Rankings`

### Required fields

- `category_key`
- `category_label`
- `category_group`
- `sort_order`
- `fighter_name`
- `snapshot_at`

### Strongly expected fields

- `rank_position`
- `fighter_id`
- `is_champion`
- `is_interim`

### App expectations

- at least two snapshots are needed before the app can compute movement
- current and previous snapshots must be comparable by normalized category

### Fallback behavior in `mma-grid`

- no previous snapshot -> no movement arrows
- row still renders as a normal ranking entry

### Freshness expectation

- one retained snapshot per ranking refresh minimum

## Table: `fighter_ufc_profiles`

### Owner

- `MMA-analytics`

### Purpose

Detailed UFC profile layer used by `Fighter Detail`.

### Required fields

- `fighter_id`

### Optional but important fields

- `height_cm`
- `reach_cm`
- `leg_reach_cm`
- `stance`
- `weight_kg`
- `birth_date`
- `debut_date`

### App expectations

- table may be partially populated
- app must tolerate missing rows

### Fallback behavior in `mma-grid`

- `Physical profile` renders partial data
- missing values simply do not render as measurements

## Table: `fighter_ufc_stats`

### Owner

- `MMA-analytics`

### Purpose

Standard UFC stats layer used by:

- `Fighter Detail`
- compare detail surfaces

### Required fields

- `fighter_id`

### Strongly expected fields

- `slpm`
- `sapm`
- `str_acc`
- `str_def`
- `td_avg`
- `td_acc`
- `td_def`
- `sub_avg`

### App expectations

- app can tolerate missing rows
- when present, values should be numeric and normalized

## Table: `fighter_ufc_advanced_stats`

### Owner

- `MMA-analytics`

### Purpose

Advanced stat layer for richer scouting and compare reads.

### Required fields

- `fighter_id`

### App expectations

- this table is additive, not required for baseline page render
- missing data should never break a page

## App-Side Product Tables

These are not owned by `MMA-analytics`, but they matter when debugging shared behaviors.

### Owned by `mma-grid`

- `ranking_presets`
- `event_prep_sessions`
- `event_picks`
- `billing_customers`
- `billing_subscriptions`
- `entitlements`
- `legal_acceptances`
- `beta_access_invites`

If a bug only concerns these tables, start in `mma-grid`, not in the pipeline.

## Verification Checklist

When a table changes, validate:

1. row counts look plausible
2. required fields are present
3. nulls only appear where the app expects them
4. normalized categories and divisions still match app taxonomy
5. the target UI screen renders correctly against the updated data

## Fast Triage Guide

### If the app shows stale or incomplete rankings

Check:

- `ufc_rankings`
- `ufc_ranking_snapshots`
- environment alignment between both repos

### If compare links are missing on event bouts

Check:

- `event_bouts.fighter_a_id`
- `event_bouts.fighter_b_id`
- linking/backfill quality

### If a fighter page looks partially empty

Check:

- `fighter_profiles`
- `fighter_ufc_profiles`
- `fighter_ufc_stats`
- `fighter_ufc_advanced_stats`

### If account/billing/legal is wrong

Start in:

- `mma-grid`

not in the ingestion repo.

---

## Judging Intelligence Layer

The tables below power the Judging Intelligence feature. They are owned by `MMA-analytics` (ingestion) and consumed read-only by `mma-grid` (UI).

All five tables FK to `event_bouts.id` as the canonical bout identifier. The `fights` table is per-fighter (two rows per bout) and is not used as a FK target here.

## Table: `fight_scorecards`

### Owner

- `MMA-analytics`

### Purpose

Official judge scorecards per bout per round, sourced from MMA Decisions.

Used by:

- `Fight Scoring` (round-by-round explainer)
- `Judge Profiles` (aggregation source)
- `Expected Scoring` (divergence computation)

### Required fields

- `event_bout_id` (FK to `event_bouts`)
- `judge_name`
- `round_number`
- `fighter_a_score`
- `fighter_b_score`
- `source_url`
- `scraped_at`

### App expectations

- `fighter_a_score` / `fighter_b_score` correspond to corners A / B of `event_bouts`
- Scores are 7-10 (10-point-must system, 10-7 is extreme floor)
- One row per judge per round per bout
- Unique constraint on `(event_bout_id, judge_name_normalized, round_number)`

### Fallback behavior in `mma-grid`

- If no scorecards exist for a bout, the scoring page shows "Scorecards not yet available"
- Partial scorecards (fewer than 3 judges) render with a warning banner

### Freshness expectation

- Updated weekly via scheduled scraping job
- Backfill for historical bouts is best-effort

## Table: `fight_round_stats`

### Owner

- `MMA-analytics`

### Purpose

Per-round per-fighter stats breakdown. Two rows per round (one per fighter). No overlap with `fighter_ufc_advanced_stats` which only stores career-level aggregates.

Used by:

- `Fight Scoring` (round-by-round stat cards)
- `Expected Scoring` (input to ABC criteria model)

### Required fields

- `event_bout_id` (FK to `event_bouts`)
- `round_number`
- `fighter_id` (FK to `fighters`)

### Strongly expected fields

- `sig_str_head_landed`, `sig_str_head_attempted`
- `sig_str_body_landed`, `sig_str_body_attempted`
- `sig_str_leg_landed`, `sig_str_leg_attempted`
- `sig_str_distance_landed`, `sig_str_distance_attempted`
- `sig_str_clinch_landed`, `sig_str_clinch_attempted`
- `sig_str_ground_landed`, `sig_str_ground_attempted`
- `total_sig_str_landed`, `total_sig_str_attempted`
- `takedowns_landed`, `takedowns_attempted`
- `submission_attempts`
- `reversals`
- `control_time_seconds`
- `knockdowns`

### App expectations

- All stat columns default to 0 when data is unavailable
- The scoring engine skips rounds where all stats are zero rather than producing misleading scores
- `control_time_seconds` is stored as integer seconds (not interval) for simpler math

### Fallback behavior in `mma-grid`

- If no round stats exist for a bout, UI shows "Round stats unavailable"
- Partial rounds (e.g. only 2 of 3 rounds have stats) render available rounds with a note

### Freshness expectation

- Updated weekly alongside scorecards
- Historical backfill is best-effort

## Table: `judge_profiles`

### Owner

- `MMA-analytics`

### Purpose

Materialized judge tendency profiles computed from `fight_scorecards`. Rebuilt after each scorecard ingestion batch.

Used by:

- `Fight Scoring` (judge profile cards)
- Future: pre-event judge assignment previews

### Required fields

- `judge_name`
- `judge_name_key` (normalized for matching)
- `total_bouts_judged`
- `total_rounds_scored`

### Strongly expected fields

- `striker_bias` (0-1 scale)
- `grappler_bias` (0-1 scale)
- `ten_eight_rate` (% of rounds scored 10-8)
- `disagreement_rate` (% of bouts where judge disagreed with majority)
- `controversy_rate` (% of bouts on controversial cards)
- `avg_score_differential`
- `last_event_date`
- `computed_at`

### App expectations

- `judge_name_key` uses the same normalization logic as `normalize_canonical_fighter_key()`
- `striker_bias` and `grappler_bias` are independent (not forced to sum to 1.0)
- This is a computed table, not a raw ingestion table

### Fallback behavior in `mma-grid`

- If no profile exists for a judge, UI shows the judge name without tendency data
- Low sample size (< 20 bouts) triggers a "limited data" qualifier

### Freshness expectation

- Recomputed after each scorecard ingestion batch

## Table: `scoring_criteria_weights`

### Owner

- `MMA-analytics`

### Purpose

Configurable ABC criteria weights for the expected scoring model. Versioned with an `is_active` flag so alternative models can coexist without breaking production.

Used by:

- `Expected Scoring` computation job
- `Fight Scoring` UI (displays active model description)

### Required fields

- `model_name`
- `version`
- `is_active`
- `effective_striking_weight`
- `effective_grappling_weight`
- `aggression_weight`
- `cage_control_weight`

### App expectations

- Weights must sum to 1.0 (enforced by CHECK constraint with float tolerance)
- Only one active version per model name (enforced by partial unique index)
- Default seed: `abc_default` v1 with 40/30/15/15 split

### Fallback behavior in `mma-grid`

- If no active model exists, scoring page shows "Scoring model not configured"
- UI always reads the active model row, never selects by version

### Freshness expectation

- Rarely updated; changes are manual configuration events

## Table: `expected_round_scores`

### Owner

- `MMA-analytics`

### Purpose

Output of the deterministic ABC scoring engine. One row per round per model. Computes expected scores from `fight_round_stats` and compares against `fight_scorecards` for divergence detection.

Used by:

- `Fight Scoring` (expected vs actual comparison, controversy alerts)
- Future: controversy leaderboards, model calibration dashboard

### Required fields

- `event_bout_id` (FK to `event_bouts`)
- `round_number`
- `model_name`
- `fighter_a_expected`
- `fighter_b_expected`
- `expected_winner` (`A`, `B`, or `EVEN`)

### Strongly expected fields

- Component scores: `fighter_a_striking`, `fighter_b_striking`, `fighter_a_grappling`, `fighter_b_grappling`, `fighter_a_aggression`, `fighter_b_aggression`, `fighter_a_cage_control`, `fighter_b_cage_control`
- `divergence_score` (null if no scorecards available for this bout)
- `is_controversial` (boolean flag when divergence > threshold)
- `computed_at`

### App expectations

- Multiple models can coexist (keyed by `model_name`)
- `divergence_score` is null when scorecards have not been ingested yet — UI must handle this gracefully
- `is_controversial` is a denormalized flag for fast query of controversy feeds

### Fallback behavior in `mma-grid`

- If no expected scores exist, scoring page shows "Expected scoring not yet computed"
- If `divergence_score` is null, UI hides the divergence section rather than showing zeros

### Freshness expectation

- Recomputed after each round stats or scorecard ingestion batch
