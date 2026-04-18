# Judging Intelligence — Scoring Model

**Version:** 1 (model_name = `abc_default`)
**Owner repo:** MMA-analytics
**Output table:** `public.expected_round_scores`

This document specifies the deterministic scoring engine that converts
per-round statistics (`fight_round_stats`) into an expected score
under the ABC Unified Rules, and measures divergence against official
judge scorecards (`fight_scorecards`).

The engine is intentionally **not** machine-learned: every output is a
traceable function of inputs and the configurable weights stored in
`scoring_criteria_weights`.

---

## 1. Inputs

For each round, we take the two rows of `fight_round_stats` keyed by
`(event_bout_id, round_number, fighter_id)`.

| Variable | Source column | Meaning |
|---|---|---|
| `S_land` | `total_sig_str_landed` | significant strikes landed |
| `S_att` | `total_sig_str_attempted` | significant strikes attempted |
| `S_head` | `sig_str_head_landed` | head strikes landed |
| `S_body` | `sig_str_body_landed` | body strikes landed |
| `S_leg` | `sig_str_leg_landed` | leg strikes landed |
| `KD` | `knockdowns` | knockdowns |
| `TD_land` | `takedowns_landed` | takedowns landed |
| `SUB` | `submission_attempts` | submission attempts |
| `REV` | `reversals` | reversals |
| `CTRL` | `control_time_seconds` | control time (s) |

The active `scoring_criteria_weights` row (`is_active = true`)
provides: `w_str, w_grap, w_agg, w_cage` with `sum = 1.0`.

---

## 2. Component scores (per fighter, per round)

### 2.1 Effective striking

Weighted raw value:

```
raw_str = S_land + 2 * KD + 0.5 * S_head
```

Rationale: every landed strike counts; head strikes get a half bonus
(head damage = effective striking per ABC); knockdowns get a double
bonus (they are, by rule, the strongest signal of round dominance).

### 2.2 Effective grappling

```
raw_grap = 3 * TD_land + 2 * SUB + 1 * REV
```

Rationale: a completed takedown is the most persistent grappling
event; submission attempts signal threat; reversals reflect positional
control change.

### 2.3 Aggression

```
raw_agg = S_att + S_land
```

Rationale: attempts alone would reward pawing; landed+attempted
together reward forward pressure that produces output.

### 2.4 Cage control

```
raw_cage = CTRL
```

Plain control time in seconds. Floor-positional control, clinch
control on the cage, and top position all show up here.

---

## 3. Normalization (round-relative)

For each component, convert the raw pair `(raw_A, raw_B)` into a
0–10 score pair:

```
if raw_A + raw_B == 0:
    score_A = score_B = 5.0
else:
    ratio_A = raw_A / (raw_A + raw_B)
    score_A = 10 * ratio_A
    score_B = 10 * (1 - ratio_A)
```

This guarantees the two fighters' scores sum to 10 per component and
makes the output directly comparable to the 10-point-must system.

### Why not percentile normalization?

An earlier design normalized each component against the full dataset
percentile. With fewer than 200 bouts the percentile buckets are
noisy, and a fighter dominating a slow round (e.g. 12 strikes landed
vs 3) could rank lower than a fighter in a frantic round with worse
relative output. Round-relative normalization fixes that.

When the dataset exceeds 500 bouts, we will revisit this and may
introduce a hybrid: round-relative × pace-adjusted.

---

## 4. Composite score

```
expected_A = w_str  * score_A_str
           + w_grap * score_A_grap
           + w_agg  * score_A_agg
           + w_cage * score_A_cage
```

and symmetrically for B. With `w_* = (0.40, 0.30, 0.15, 0.15)`, both
`expected_A` and `expected_B` lie in `[0, 10]` and always sum to 10.

### Winner

```
if expected_A > expected_B + 0.25:  winner = 'A'
elif expected_B > expected_A + 0.25: winner = 'B'
else:                                winner = 'EVEN'
```

The 0.25 dead band prevents flipping winners on near-identical rounds.

---

## 5. Divergence from judges

When `fight_scorecards` rows exist for the round, we compute:

```
judge_winners = [
    'A' if sc.fighter_a_score > sc.fighter_b_score
    else 'B' if sc.fighter_b_score > sc.fighter_a_score
    else 'EVEN'
    for sc in scorecards_for_round
]
judge_majority = mode(judge_winners)  # or 'EVEN' on a tie

if winner == judge_majority:
    divergence_score = 0.0
else:
    divergence_score = abs(expected_A - expected_B) / 10.0
```

So divergence is 0 when the engine agrees with the judges, and scales
with the engine's confidence when it disagrees (max 1.0).

### Controversy flag

```
is_controversial = divergence_score > 0.30
                   AND at least 1 judge disagreed with the majority
```

This combines two signals: the deterministic engine disagrees
confidently, **and** at least one real judge disagreed with the other
two — i.e. humans also saw the round differently.

---

## 6. Judge profiles

`judge_profiles` is materialized from `fight_scorecards`. For each
distinct `judge_name`:

| Metric | Definition |
|---|---|
| `total_bouts_judged` | `count(distinct event_bout_id)` |
| `total_rounds_scored` | `count(*)` |
| `ten_eight_rate` | `sum(min(a,b) == 8) / total_rounds_scored` |
| `disagreement_rate` | share of bouts where this judge disagreed with the other 2 |
| `controversy_rate` | share of bouts where the engine flagged ≥1 controversial round |
| `striker_bias` | correlation between judge's score delta and striking component delta (rescaled to [0,1]) |
| `grappler_bias` | same for grappling component |
| `avg_score_differential` | mean of `|a_total − b_total|` across bouts |
| `last_event_date` | `max(events.date)` over bouts judged |

The table is replaced wholesale on each computation (no incremental
merge): cheap at our scale (< 200 distinct judges) and avoids stale
partial states.

---

## 7. Minimum-data guardrails

The engine skips a round and writes nothing to `expected_round_scores`
when:

- both rows in `fight_round_stats` are all-zero (missing data, not a
  10-8 shutout)
- the round has only one row (data corruption — both fighters must be
  present)

The engine still writes a row when the round has real stats but no
scorecards: `divergence_score = NULL` and `is_controversial = false`.

---

## 8. Re-computation cadence

- `compute_expected_scoring.py` runs **after** every successful
  scrape ingestion (weekly), replacing rows keyed by
  `(event_bout_id, round_number, model_name)`.
- `compute_judge_profiles.py` runs on the same cadence, after the
  scoring pass. It fully rewrites `judge_profiles`.
- Both scripts are idempotent: running them twice back-to-back is a
  no-op on output.
