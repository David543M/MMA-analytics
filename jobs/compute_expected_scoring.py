"""
Compute per-round expected scores from fight_round_stats + scoring_criteria_weights
and upsert into expected_round_scores.

Formula and rationale: see docs/scoring-model.md.

Usage:
    python jobs/compute_expected_scoring.py

Environment variables:
    SUPABASE_URL — project URL (or VITE_SUPABASE_URL)
    SUPABASE_KEY — service-role key (write access)
"""

from __future__ import annotations

import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from supabase import Client, create_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------


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
# Scoring
# ---------------------------------------------------------------------------


@dataclass
class Weights:
    striking: float
    grappling: float
    aggression: float
    cage_control: float
    model_name: str


def load_active_weights(supabase: Client) -> Weights:
    resp = (
        supabase.table("scoring_criteria_weights")
        .select("*")
        .eq("is_active", True)
        .order("version", desc=True)
        .limit(1)
        .execute()
    )
    if not resp.data:
        log.error("No active row in scoring_criteria_weights")
        sys.exit(1)
    row = resp.data[0]
    return Weights(
        striking=float(row["effective_striking_weight"]),
        grappling=float(row["effective_grappling_weight"]),
        aggression=float(row["aggression_weight"]),
        cage_control=float(row["cage_control_weight"]),
        model_name=row["model_name"],
    )


def raw_components(stat: dict) -> tuple[float, float, float, float]:
    """Return raw (striking, grappling, aggression, cage_control) scalars for one row."""
    s_land = stat.get("total_sig_str_landed", 0) or 0
    s_att = stat.get("total_sig_str_attempted", 0) or 0
    s_head = stat.get("sig_str_head_landed", 0) or 0
    kd = stat.get("knockdowns", 0) or 0
    td_land = stat.get("takedowns_landed", 0) or 0
    sub = stat.get("submission_attempts", 0) or 0
    rev = stat.get("reversals", 0) or 0
    ctrl = stat.get("control_time_seconds", 0) or 0

    raw_str = s_land + 2 * kd + 0.5 * s_head
    raw_grap = 3 * td_land + 2 * sub + rev
    raw_agg = s_att + s_land
    raw_cage = ctrl
    return raw_str, raw_grap, raw_agg, raw_cage


def normalize_pair(raw_a: float, raw_b: float) -> tuple[float, float]:
    """Round-relative normalization: returns (score_a, score_b) summing to ~10."""
    total = raw_a + raw_b
    if total <= 0:
        return 5.0, 5.0
    ratio_a = raw_a / total
    return round(10.0 * ratio_a, 2), round(10.0 * (1 - ratio_a), 2)


def compute_expected_row(
    stat_a: dict, stat_b: dict, weights: Weights
) -> Optional[dict]:
    """Compute an expected_round_scores row for one (bout, round) pair.

    Returns None when data is insufficient (all-zero both sides).
    """
    raw_a = raw_components(stat_a)
    raw_b = raw_components(stat_b)
    if all(v == 0 for v in raw_a) and all(v == 0 for v in raw_b):
        return None

    str_a, str_b = normalize_pair(raw_a[0], raw_b[0])
    grap_a, grap_b = normalize_pair(raw_a[1], raw_b[1])
    agg_a, agg_b = normalize_pair(raw_a[2], raw_b[2])
    cage_a, cage_b = normalize_pair(raw_a[3], raw_b[3])

    expected_a = round(
        weights.striking * str_a
        + weights.grappling * grap_a
        + weights.aggression * agg_a
        + weights.cage_control * cage_a,
        2,
    )
    expected_b = round(
        weights.striking * str_b
        + weights.grappling * grap_b
        + weights.aggression * agg_b
        + weights.cage_control * cage_b,
        2,
    )

    if expected_a > expected_b + 0.25:
        winner = "A"
    elif expected_b > expected_a + 0.25:
        winner = "B"
    else:
        winner = "EVEN"

    return {
        "event_bout_id": stat_a["event_bout_id"],
        "round_number": stat_a["round_number"],
        "model_name": weights.model_name,
        "fighter_a_striking": str_a,
        "fighter_b_striking": str_b,
        "fighter_a_grappling": grap_a,
        "fighter_b_grappling": grap_b,
        "fighter_a_aggression": agg_a,
        "fighter_b_aggression": agg_b,
        "fighter_a_cage_control": cage_a,
        "fighter_b_cage_control": cage_b,
        "fighter_a_expected": expected_a,
        "fighter_b_expected": expected_b,
        "expected_winner": winner,
    }


# ---------------------------------------------------------------------------
# Divergence from judges
# ---------------------------------------------------------------------------


def attach_divergence(
    row: dict,
    scorecards_for_round: list[dict],
) -> dict:
    """Fill divergence_score + is_controversial based on judge scorecards."""
    if not scorecards_for_round:
        row["divergence_score"] = None
        row["is_controversial"] = False
        return row

    judge_winners: list[str] = []
    for sc in scorecards_for_round:
        a = sc["fighter_a_score"]
        b = sc["fighter_b_score"]
        if a > b:
            judge_winners.append("A")
        elif b > a:
            judge_winners.append("B")
        else:
            judge_winners.append("EVEN")

    # Majority vote; tie -> EVEN
    counts: dict[str, int] = defaultdict(int)
    for w in judge_winners:
        counts[w] += 1
    majority_winner, majority_count = max(counts.items(), key=lambda kv: kv[1])
    if sum(1 for c in counts.values() if c == majority_count) > 1:
        majority_winner = "EVEN"

    if row["expected_winner"] == majority_winner:
        divergence = 0.0
    else:
        divergence = round(
            abs(row["fighter_a_expected"] - row["fighter_b_expected"]) / 10.0, 3
        )

    judges_split = len(set(judge_winners)) > 1
    is_controversial = bool(divergence > 0.30 and judges_split)

    row["divergence_score"] = divergence
    row["is_controversial"] = is_controversial
    return row


# ---------------------------------------------------------------------------
# Mapping fighter corners to A/B
# ---------------------------------------------------------------------------


def resolve_corner_map(
    bout_rows: list[dict], bout_fighter_map: dict[str, dict[str, str]]
) -> dict[str, dict[str, str]]:
    """For each bout, determine which fighter_id is corner A vs B.

    event_bouts stores fighter_a_name and fighter_b_name. fight_round_stats
    stores fighter_id. We need the corner assignment per bout so the scoring
    engine can pair the two round rows correctly.

    Returns { event_bout_id: { fighter_id: 'A' | 'B' } }.

    Strategy: for each bout, look at the two distinct fighter_ids that show
    up in round_stats; cross-reference with fighters.name vs
    event_bouts.fighter_a_name/fighter_b_name.
    """
    return bout_fighter_map


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    supabase = get_supabase()
    weights = load_active_weights(supabase)
    log.info(
        "Loaded weights %s v (str=%.2f, grap=%.2f, agg=%.2f, cage=%.2f)",
        weights.model_name, weights.striking, weights.grappling,
        weights.aggression, weights.cage_control,
    )

    # Load all round stats
    stats = paginate(
        lambda off, end: supabase.table("fight_round_stats")
        .select("*")
        .range(off, end)
        .execute()
    )
    log.info("Loaded %d fight_round_stats rows", len(stats))

    if not stats:
        log.warning("No fight_round_stats data; nothing to compute")
        return

    # Load bout -> (fighter_a_name, fighter_b_name) and fighter id -> name
    bouts = paginate(
        lambda off, end: supabase.table("event_bouts")
        .select("id, fighter_a_name, fighter_b_name")
        .range(off, end)
        .execute()
    )
    bout_by_id = {b["id"]: b for b in bouts}

    fighters = paginate(
        lambda off, end: supabase.table("fighters")
        .select("id, name")
        .range(off, end)
        .execute()
    )
    fighter_name_by_id = {f["id"]: f.get("name") or "" for f in fighters}

    # Group round stats by (bout, round)
    grouped: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for s in stats:
        grouped[(s["event_bout_id"], s["round_number"])].append(s)

    # Load all scorecards and index by (bout, round)
    scorecards = paginate(
        lambda off, end: supabase.table("fight_scorecards")
        .select("event_bout_id, round_number, fighter_a_score, fighter_b_score")
        .range(off, end)
        .execute()
    )
    cards_by_key: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for c in scorecards:
        cards_by_key[(c["event_bout_id"], c["round_number"])].append(c)
    log.info("Loaded %d scorecards", len(scorecards))

    expected_rows: list[dict] = []
    skipped = 0
    for (bout_id, round_num), rows in grouped.items():
        if len(rows) != 2:
            skipped += 1
            continue
        bout = bout_by_id.get(bout_id)
        if not bout:
            skipped += 1
            continue

        # Assign corners A / B based on names
        name_a = (bout.get("fighter_a_name") or "").strip().lower()
        name_b = (bout.get("fighter_b_name") or "").strip().lower()
        f0_name = fighter_name_by_id.get(rows[0]["fighter_id"], "").strip().lower()
        f1_name = fighter_name_by_id.get(rows[1]["fighter_id"], "").strip().lower()

        if f0_name == name_a or f1_name == name_b:
            stat_a, stat_b = rows[0], rows[1]
        elif f0_name == name_b or f1_name == name_a:
            stat_a, stat_b = rows[1], rows[0]
        else:
            # Names don't align — skip rather than mis-assign
            skipped += 1
            continue

        computed = compute_expected_row(stat_a, stat_b, weights)
        if not computed:
            skipped += 1
            continue

        computed = attach_divergence(computed, cards_by_key.get((bout_id, round_num), []))
        expected_rows.append(computed)

    log.info("Computed %d expected_round_scores rows (%d rounds skipped)", len(expected_rows), skipped)

    # Upsert in batches
    batch_size = 100
    upserted = 0
    for i in range(0, len(expected_rows), batch_size):
        batch = expected_rows[i:i + batch_size]
        try:
            supabase.table("expected_round_scores").upsert(
                batch,
                on_conflict="event_bout_id,round_number,model_name",
            ).execute()
            upserted += len(batch)
        except Exception as exc:
            log.warning("Batch upsert failed: %s", exc)

    controversial = sum(1 for r in expected_rows if r["is_controversial"])
    log.info(
        "Upserted %d rows into expected_round_scores (%d controversial)",
        upserted, controversial,
    )


if __name__ == "__main__":
    main()
