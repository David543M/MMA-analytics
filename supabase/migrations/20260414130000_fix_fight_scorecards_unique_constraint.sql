-- Replace the functional unique index on fight_scorecards with a plain
-- unique constraint so that PostgREST's on_conflict upsert spec matches.
--
-- Background: the original migration created a functional unique index on
-- (event_bout_id, lower(btrim(judge_name)), round_number). PostgREST's
-- upsert(on_conflict="event_bout_id,judge_name,round_number") requires a
-- matching unique/exclusion constraint on the exact columns, not an
-- expression index. mmadecisions.com returns judge names in a canonical
-- form, so a plain column constraint is sufficient.

DROP INDEX IF EXISTS public.fight_scorecards_bout_judge_round_idx;

ALTER TABLE public.fight_scorecards
  ADD CONSTRAINT fight_scorecards_bout_judge_round_key
  UNIQUE (event_bout_id, judge_name, round_number);
