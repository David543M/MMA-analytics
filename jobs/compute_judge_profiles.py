"""
Aggregate fight_scorecards into judge_profiles.

For each distinct judge_name, computes tendency metrics as defined in
docs/scoring-model.md §6.

Usage:
    python jobs/compute_judge_profiles.py

Environment variables:
    SUPABASE_URL — project URL (or VITE_SUPABASE_URL)
    SUPABASE_KEY — service-role key (write access)
"""

from __future__ import annotations

import logging
import os
import re
import sys
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from statistics import mean
from typing import Optional

from supabase import Client, create_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def normalize_name_key(name: str) -> str:
    nfkd = unicodedata.normalize("NFKD", name.replace("\xa0", " "))
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    lowered = ascii_only.lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", lowered).strip()
    return re.sub(r"\s+", " ", cleaned)


def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL") or os.environ.get("VITE_SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        log.error("SUPABASE_URL and SUPABASE_KEY must be set")
        sys.exit(1)
    return create_client(url, key)


def paginate(query_fn, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        resp = query_fn(offset, offset + page_size - 1)
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


@dataclass
class JudgeAgg:
    judge_name: str
    bout_ids: set = field(default_factory=set)
    rounds: int = 0
    ten_eight_rounds: int = 0
    bout_disagreements: int = 0
    bout_controversies: int = 0
    bout_totals_diffs: list[int] = field(default_factory=list)
    striking_deltas: list[float] = field(default_factory=list)
    grappling_deltas: list[float] = field(default_factory=list)
    judge_score_deltas: list[float] = field(default_factory=list)
    last_event_date: Optional[str] = None


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _correlation(xs: list[float], ys: list[float]) -> float:
    """Return Pearson r in [-1, 1] or 0 if not computable."""
    if len(xs) < 3 or len(xs) != len(ys):
        return 0.0
    mx = mean(xs)
    my = mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    denx = sum((x - mx) ** 2 for x in xs) ** 0.5
    deny = sum((y - my) ** 2 for y in ys) ** 0.5
    if denx == 0 or deny == 0:
        return 0.0
    return num / (denx * deny)


def main() -> None:
    supabase = get_supabase()

    scorecards = paginate(
        lambda off, end: supabase.table("fight_scorecards")
        .select("event_bout_id, judge_name, round_number, fighter_a_score, fighter_b_score")
        .range(off, end)
        .execute()
    )
    log.info("Loaded %d scorecards", len(scorecards))
    if not scorecards:
        log.warning("No scorecards — nothing to compute")
        return

    expected = paginate(
        lambda off, end: supabase.table("expected_round_scores")
        .select(
            "event_bout_id, round_number, fighter_a_striking, fighter_b_striking, "
            "fighter_a_grappling, fighter_b_grappling, is_controversial"
        )
        .range(off, end)
        .execute()
    )
    expected_by_key = {(e["event_bout_id"], e["round_number"]): e for e in expected}
    log.info("Loaded %d expected_round_scores", len(expected))

    bouts = paginate(
        lambda off, end: supabase.table("event_bouts")
        .select("id, events(date)")
        .range(off, end)
        .execute()
    )
    date_by_bout: dict[str, str] = {}
    for b in bouts:
        evt = b.get("events") or {}
        d = evt.get("date") if isinstance(evt, dict) else None
        if d:
            date_by_bout[b["id"]] = d

    # Group per bout to compute per-judge agreement
    cards_by_bout: dict[str, list[dict]] = defaultdict(list)
    for c in scorecards:
        cards_by_bout[c["event_bout_id"]].append(c)

    # For each bout, compute each judge's round winner and majority
    aggs: dict[str, JudgeAgg] = {}

    for bout_id, bout_cards in cards_by_bout.items():
        by_judge: dict[str, list[dict]] = defaultdict(list)
        for c in bout_cards:
            by_judge[c["judge_name"]].append(c)

        # Per-bout: sum each judge's totals
        judge_totals: dict[str, tuple[int, int]] = {}
        for judge, rows in by_judge.items():
            a = sum(r["fighter_a_score"] for r in rows)
            b = sum(r["fighter_b_score"] for r in rows)
            judge_totals[judge] = (a, b)

        # Bout winner per judge ('A' / 'B' / 'EVEN')
        judge_bout_winners: dict[str, str] = {}
        for judge, (a, b) in judge_totals.items():
            if a > b:
                judge_bout_winners[judge] = "A"
            elif b > a:
                judge_bout_winners[judge] = "B"
            else:
                judge_bout_winners[judge] = "EVEN"

        # Majority bout winner (tie -> EVEN)
        wcounts: dict[str, int] = defaultdict(int)
        for w in judge_bout_winners.values():
            wcounts[w] += 1
        maj, maj_n = max(wcounts.items(), key=lambda kv: kv[1])
        if sum(1 for n in wcounts.values() if n == maj_n) > 1:
            maj = "EVEN"

        # Controversial bout? Any round flagged?
        bout_is_controversial = any(
            expected_by_key.get((bout_id, r))
            and expected_by_key[(bout_id, r)].get("is_controversial")
            for r in range(1, 6)
        )

        bout_date = date_by_bout.get(bout_id)

        for judge, rows in by_judge.items():
            agg = aggs.setdefault(judge, JudgeAgg(judge_name=judge))
            agg.bout_ids.add(bout_id)
            agg.rounds += len(rows)

            # 10-8 rounds = min(a,b) == 8 (covers 10-8 cleanly)
            for r in rows:
                if min(r["fighter_a_score"], r["fighter_b_score"]) == 8:
                    agg.ten_eight_rounds += 1

            # Disagreement: this judge's bout winner differs from majority
            if judge_bout_winners[judge] != maj:
                agg.bout_disagreements += 1

            if bout_is_controversial:
                agg.bout_controversies += 1

            a_total, b_total = judge_totals[judge]
            agg.bout_totals_diffs.append(abs(a_total - b_total))

            # Striker/grappler bias: correlate judge delta with component delta
            for r in rows:
                exp = expected_by_key.get((bout_id, r["round_number"]))
                if not exp:
                    continue
                judge_delta = r["fighter_a_score"] - r["fighter_b_score"]
                str_delta = exp["fighter_a_striking"] - exp["fighter_b_striking"]
                grap_delta = exp["fighter_a_grappling"] - exp["fighter_b_grappling"]
                agg.judge_score_deltas.append(float(judge_delta))
                agg.striking_deltas.append(float(str_delta))
                agg.grappling_deltas.append(float(grap_delta))

            if bout_date and (not agg.last_event_date or bout_date > agg.last_event_date):
                agg.last_event_date = bout_date

    # Materialize rows
    rows_out: list[dict] = []
    for judge, agg in aggs.items():
        bouts_n = len(agg.bout_ids)
        if bouts_n == 0:
            continue

        ten_eight_rate = round(agg.ten_eight_rounds / agg.rounds, 4) if agg.rounds else 0.0
        disagreement_rate = round(agg.bout_disagreements / bouts_n, 3)
        controversy_rate = round(agg.bout_controversies / bouts_n, 3)
        avg_diff = round(mean(agg.bout_totals_diffs), 2) if agg.bout_totals_diffs else 0.0

        # Rescale correlation [-1,1] -> [0,1], then clamp
        striker_r = _correlation(agg.striking_deltas, agg.judge_score_deltas)
        grappler_r = _correlation(agg.grappling_deltas, agg.judge_score_deltas)
        striker_bias = round(_clamp((striker_r + 1.0) / 2.0, 0.0, 1.0), 3)
        grappler_bias = round(_clamp((grappler_r + 1.0) / 2.0, 0.0, 1.0), 3)

        rows_out.append({
            "judge_name": judge,
            "judge_name_key": normalize_name_key(judge),
            "total_bouts_judged": bouts_n,
            "total_rounds_scored": agg.rounds,
            "striker_bias": striker_bias,
            "grappler_bias": grappler_bias,
            "ten_eight_rate": ten_eight_rate,
            "disagreement_rate": disagreement_rate,
            "controversy_rate": controversy_rate,
            "avg_score_differential": avg_diff,
            "last_event_date": agg.last_event_date,
        })

    log.info("Computed %d judge profiles", len(rows_out))

    # Replace wholesale: truncate-then-insert avoids stale partial state
    try:
        supabase.table("judge_profiles").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
    except Exception as exc:
        log.warning("Failed to clear judge_profiles before recompute: %s", exc)

    batch_size = 100
    upserted = 0
    for i in range(0, len(rows_out), batch_size):
        batch = rows_out[i:i + batch_size]
        try:
            supabase.table("judge_profiles").upsert(
                batch,
                on_conflict="judge_name_key",
            ).execute()
            upserted += len(batch)
        except Exception as exc:
            log.warning("Batch upsert failed: %s", exc)

    log.info("Upserted %d judge_profiles rows", upserted)


if __name__ == "__main__":
    main()
