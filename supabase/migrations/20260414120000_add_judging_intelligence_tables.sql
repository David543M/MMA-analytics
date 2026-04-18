-- Judging Intelligence tables
-- Adds five tables powering the Scorecards & Judging Intelligence feature:
--   1. fight_scorecards     — official judge scorecards per bout per round
--   2. fight_round_stats    — per-round per-fighter stat breakdowns
--   3. judge_profiles       — materialized judge tendency profiles
--   4. scoring_criteria_weights — configurable ABC criteria model weights
--   5. expected_round_scores — deterministic scoring engine output
--
-- All bout-level tables FK to event_bouts.id (canonical bout, not per-fighter fights).

-- =============================================================================
-- 1. fight_scorecards
-- =============================================================================

CREATE TABLE public.fight_scorecards (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_bout_id uuid NOT NULL REFERENCES public.event_bouts(id) ON DELETE CASCADE,
  judge_name text NOT NULL CHECK (btrim(judge_name) <> ''),
  round_number smallint NOT NULL CHECK (round_number BETWEEN 1 AND 5),
  fighter_a_score smallint NOT NULL CHECK (fighter_a_score BETWEEN 7 AND 10),
  fighter_b_score smallint NOT NULL CHECK (fighter_b_score BETWEEN 7 AND 10),
  source_url text NOT NULL DEFAULT '',
  scraped_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX fight_scorecards_bout_judge_round_idx
  ON public.fight_scorecards (event_bout_id, lower(btrim(judge_name)), round_number);

CREATE INDEX fight_scorecards_event_bout_id_idx
  ON public.fight_scorecards (event_bout_id, round_number);

ALTER TABLE public.fight_scorecards ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Fight scorecards are publicly readable"
  ON public.fight_scorecards FOR SELECT USING (true);

GRANT SELECT ON public.fight_scorecards TO anon, authenticated;

-- =============================================================================
-- 2. fight_round_stats
-- =============================================================================

CREATE TABLE public.fight_round_stats (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_bout_id uuid NOT NULL REFERENCES public.event_bouts(id) ON DELETE CASCADE,
  round_number smallint NOT NULL CHECK (round_number BETWEEN 1 AND 5),
  fighter_id uuid NOT NULL REFERENCES public.fighters(id) ON DELETE SET NULL,

  -- Significant strikes by target
  sig_str_head_landed smallint NOT NULL DEFAULT 0,
  sig_str_head_attempted smallint NOT NULL DEFAULT 0,
  sig_str_body_landed smallint NOT NULL DEFAULT 0,
  sig_str_body_attempted smallint NOT NULL DEFAULT 0,
  sig_str_leg_landed smallint NOT NULL DEFAULT 0,
  sig_str_leg_attempted smallint NOT NULL DEFAULT 0,

  -- Significant strikes by position
  sig_str_distance_landed smallint NOT NULL DEFAULT 0,
  sig_str_distance_attempted smallint NOT NULL DEFAULT 0,
  sig_str_clinch_landed smallint NOT NULL DEFAULT 0,
  sig_str_clinch_attempted smallint NOT NULL DEFAULT 0,
  sig_str_ground_landed smallint NOT NULL DEFAULT 0,
  sig_str_ground_attempted smallint NOT NULL DEFAULT 0,

  -- Totals
  total_sig_str_landed smallint NOT NULL DEFAULT 0,
  total_sig_str_attempted smallint NOT NULL DEFAULT 0,

  -- Grappling
  takedowns_landed smallint NOT NULL DEFAULT 0,
  takedowns_attempted smallint NOT NULL DEFAULT 0,
  submission_attempts smallint NOT NULL DEFAULT 0,
  reversals smallint NOT NULL DEFAULT 0,

  -- Control
  control_time_seconds smallint NOT NULL DEFAULT 0,
  knockdowns smallint NOT NULL DEFAULT 0,

  source_url text NOT NULL DEFAULT '',
  scraped_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX fight_round_stats_bout_round_fighter_idx
  ON public.fight_round_stats (event_bout_id, round_number, fighter_id);

CREATE INDEX fight_round_stats_event_bout_id_idx
  ON public.fight_round_stats (event_bout_id, round_number);

CREATE INDEX fight_round_stats_fighter_id_idx
  ON public.fight_round_stats (fighter_id, event_bout_id);

ALTER TABLE public.fight_round_stats ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Fight round stats are publicly readable"
  ON public.fight_round_stats FOR SELECT USING (true);

GRANT SELECT ON public.fight_round_stats TO anon, authenticated;

-- =============================================================================
-- 3. judge_profiles
-- =============================================================================

CREATE TABLE public.judge_profiles (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  judge_name text NOT NULL CHECK (btrim(judge_name) <> ''),
  judge_name_key text NOT NULL,

  total_bouts_judged integer NOT NULL DEFAULT 0,
  total_rounds_scored integer NOT NULL DEFAULT 0,

  -- Tendency metrics (0.0 to 1.0 scale)
  striker_bias numeric(4,3) NOT NULL DEFAULT 0.500,
  grappler_bias numeric(4,3) NOT NULL DEFAULT 0.500,
  ten_eight_rate numeric(5,4) NOT NULL DEFAULT 0.0000,
  disagreement_rate numeric(4,3) NOT NULL DEFAULT 0.000,
  controversy_rate numeric(4,3) NOT NULL DEFAULT 0.000,

  -- Scoring patterns
  avg_score_differential numeric(4,2) NOT NULL DEFAULT 0.00,

  last_event_date date,
  computed_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT judge_profiles_rates_valid CHECK (
    striker_bias BETWEEN 0 AND 1
    AND grappler_bias BETWEEN 0 AND 1
    AND disagreement_rate BETWEEN 0 AND 1
    AND controversy_rate BETWEEN 0 AND 1
  )
);

CREATE UNIQUE INDEX judge_profiles_name_key_idx
  ON public.judge_profiles (judge_name_key);

ALTER TABLE public.judge_profiles ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Judge profiles are publicly readable"
  ON public.judge_profiles FOR SELECT USING (true);

GRANT SELECT ON public.judge_profiles TO anon, authenticated;

-- =============================================================================
-- 4. scoring_criteria_weights
-- =============================================================================

CREATE TABLE public.scoring_criteria_weights (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  model_name text NOT NULL DEFAULT 'abc_default' CHECK (btrim(model_name) <> ''),
  version integer NOT NULL DEFAULT 1,
  is_active boolean NOT NULL DEFAULT true,

  effective_striking_weight numeric(4,3) NOT NULL DEFAULT 0.400,
  effective_grappling_weight numeric(4,3) NOT NULL DEFAULT 0.300,
  aggression_weight numeric(4,3) NOT NULL DEFAULT 0.150,
  cage_control_weight numeric(4,3) NOT NULL DEFAULT 0.150,

  description text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT weights_sum_to_one CHECK (
    abs(effective_striking_weight + effective_grappling_weight
        + aggression_weight + cage_control_weight - 1.0) < 0.001
  )
);

CREATE UNIQUE INDEX scoring_weights_active_model_idx
  ON public.scoring_criteria_weights (model_name, version) WHERE is_active = true;

ALTER TABLE public.scoring_criteria_weights ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Scoring criteria weights are publicly readable"
  ON public.scoring_criteria_weights FOR SELECT USING (true);

GRANT SELECT ON public.scoring_criteria_weights TO anon, authenticated;

-- Seed the default ABC model
INSERT INTO public.scoring_criteria_weights
  (model_name, version, is_active, description)
VALUES
  ('abc_default', 1, true,
   'ABC Unified Rules default: effective striking 40%, grappling 30%, aggression 15%, cage control 15%');

-- =============================================================================
-- 5. expected_round_scores
-- =============================================================================

CREATE TABLE public.expected_round_scores (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_bout_id uuid NOT NULL REFERENCES public.event_bouts(id) ON DELETE CASCADE,
  round_number smallint NOT NULL CHECK (round_number BETWEEN 1 AND 5),
  model_name text NOT NULL DEFAULT 'abc_default',

  -- Component scores (0-10 scale per fighter)
  fighter_a_striking numeric(4,2) NOT NULL DEFAULT 0,
  fighter_b_striking numeric(4,2) NOT NULL DEFAULT 0,
  fighter_a_grappling numeric(4,2) NOT NULL DEFAULT 0,
  fighter_b_grappling numeric(4,2) NOT NULL DEFAULT 0,
  fighter_a_aggression numeric(4,2) NOT NULL DEFAULT 0,
  fighter_b_aggression numeric(4,2) NOT NULL DEFAULT 0,
  fighter_a_cage_control numeric(4,2) NOT NULL DEFAULT 0,
  fighter_b_cage_control numeric(4,2) NOT NULL DEFAULT 0,

  -- Weighted composite
  fighter_a_expected numeric(4,2) NOT NULL,
  fighter_b_expected numeric(4,2) NOT NULL,
  expected_winner text NOT NULL DEFAULT 'EVEN' CHECK (expected_winner IN ('A', 'B', 'EVEN')),

  -- Divergence vs actual judge scores (null when no scorecards available)
  divergence_score numeric(5,3),
  is_controversial boolean NOT NULL DEFAULT false,

  computed_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX expected_scores_bout_round_model_idx
  ON public.expected_round_scores (event_bout_id, round_number, model_name);

CREATE INDEX expected_scores_controversial_idx
  ON public.expected_round_scores (is_controversial, event_bout_id)
  WHERE is_controversial = true;

ALTER TABLE public.expected_round_scores ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Expected round scores are publicly readable"
  ON public.expected_round_scores FOR SELECT USING (true);

GRANT SELECT ON public.expected_round_scores TO anon, authenticated;
