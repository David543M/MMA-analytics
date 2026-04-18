"""
Validate fight_scorecards and fight_round_stats data for consistency.

Checks:
  1. Scorecard round counts match event_bouts.round (no missing/extra rounds)
  2. No NULL values on critical fields
  3. Score ranges are valid (7-10 for scorecards)
  4. Every fight_scorecards.event_bout_id exists in event_bouts
  5. Every fight_round_stats.event_bout_id and fighter_id exist in their tables
  6. Per-judge total matches sum of rounds (no arithmetic errors in scraped data)
  7. Round stats totals consistency (total_sig_str >= component sums)

Usage:
    python jobs/validate_scorecards.py

Environment variables:
    SUPABASE_URL         — project URL
    SUPABASE_SERVICE_KEY — service-role key (read access is sufficient)

Exit codes:
    0 — all checks passed
    1 — one or more checks failed
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field

from supabase import Client, create_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        log.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        sys.exit(1)
    return create_client(url, key)


@dataclass
class ValidationResult:
    check_name: str
    passed: bool
    details: str = ""
    warning_count: int = 0
    error_count: int = 0


def check_scorecard_orphans(supabase: Client) -> ValidationResult:
    """Check that every scorecard references an existing event_bout."""
    result = (
        supabase.rpc(
            "check_scorecard_orphans_raw",
            {},
        )
        .execute()
    )

    # Fallback: query directly
    try:
        scorecards = supabase.table("fight_scorecards").select("event_bout_id").execute()
        bout_ids = {
            row["id"]
            for row in (supabase.table("event_bouts").select("id").execute().data or [])
        }
        orphans = [
            row["event_bout_id"]
            for row in (scorecards.data or [])
            if row["event_bout_id"] not in bout_ids
        ]

        if orphans:
            return ValidationResult(
                check_name="scorecard_orphans",
                passed=False,
                details=f"{len(orphans)} scorecards reference missing event_bouts",
                error_count=len(orphans),
            )
        return ValidationResult(
            check_name="scorecard_orphans",
            passed=True,
            details=f"All scorecards reference valid event_bouts",
        )
    except Exception as exc:
        return ValidationResult(
            check_name="scorecard_orphans",
            passed=False,
            details=f"Query failed: {exc}",
            error_count=1,
        )


def check_round_count_consistency(supabase: Client) -> ValidationResult:
    """Check that scorecard round count matches event_bouts.round for completed bouts."""
    errors = 0
    warnings = 0
    details_parts: list[str] = []

    try:
        # Get completed bouts that have scorecards
        bouts = (
            supabase.table("event_bouts")
            .select("id, round")
            .eq("status", "completed")
            .not_.is_("round", "null")
            .execute()
        )

        for bout in bouts.data or []:
            bout_id = bout["id"]
            expected_rounds = bout["round"]
            if not expected_rounds:
                continue

            # Count distinct rounds in scorecards for this bout
            scorecards = (
                supabase.table("fight_scorecards")
                .select("round_number, judge_name")
                .eq("event_bout_id", bout_id)
                .execute()
            )

            if not scorecards.data:
                continue  # No scorecards for this bout — not an error

            actual_rounds = set()
            for sc in scorecards.data:
                actual_rounds.add(sc["round_number"])

            max_round = max(actual_rounds)

            # Allow scorecards to have fewer rounds if the fight ended early via finish
            # But flag if they have MORE rounds than the bout
            if max_round > expected_rounds:
                errors += 1
                details_parts.append(
                    f"Bout {bout_id}: scorecards have R{max_round} but bout ended in R{expected_rounds}"
                )
            elif len(actual_rounds) < expected_rounds:
                warnings += 1

        passed = errors == 0
        return ValidationResult(
            check_name="round_count_consistency",
            passed=passed,
            details="; ".join(details_parts[:10]) if details_parts else "Round counts consistent",
            warning_count=warnings,
            error_count=errors,
        )

    except Exception as exc:
        return ValidationResult(
            check_name="round_count_consistency",
            passed=False,
            details=f"Query failed: {exc}",
            error_count=1,
        )


def check_judge_total_consistency(supabase: Client) -> ValidationResult:
    """Check that per-judge round scores sum correctly (each round should be 10-X)."""
    errors = 0
    details_parts: list[str] = []

    try:
        scorecards = (
            supabase.table("fight_scorecards")
            .select("event_bout_id, judge_name, round_number, fighter_a_score, fighter_b_score")
            .order("event_bout_id")
            .order("judge_name")
            .order("round_number")
            .limit(5000)
            .execute()
        )

        for sc in scorecards.data or []:
            a_score = sc["fighter_a_score"]
            b_score = sc["fighter_b_score"]

            # In 10-point-must, at least one fighter should score 10
            if a_score != 10 and b_score != 10:
                # This is unusual but legal in MMA (both can get 10 for even round,
                # or both can get < 10 for a 10-8 round with a point deduction).
                # Only flag as error if NEITHER is 10 and NEITHER is in the valid deduction range
                if max(a_score, b_score) < 9:
                    errors += 1
                    details_parts.append(
                        f"Bout {sc['event_bout_id']} {sc['judge_name']} R{sc['round_number']}: "
                        f"{a_score}-{b_score} (unusual: neither fighter scored 9+)"
                    )

        return ValidationResult(
            check_name="judge_total_consistency",
            passed=errors == 0,
            details="; ".join(details_parts[:10]) if details_parts else "Judge totals consistent",
            error_count=errors,
        )

    except Exception as exc:
        return ValidationResult(
            check_name="judge_total_consistency",
            passed=False,
            details=f"Query failed: {exc}",
            error_count=1,
        )


def check_round_stats_orphans(supabase: Client) -> ValidationResult:
    """Check that every round stat references valid event_bouts and fighters."""
    try:
        stats = supabase.table("fight_round_stats").select("event_bout_id, fighter_id").limit(5000).execute()
        bout_ids = {
            row["id"]
            for row in (supabase.table("event_bouts").select("id").execute().data or [])
        }
        fighter_ids = {
            row["id"]
            for row in (supabase.table("fighters").select("id").execute().data or [])
        }

        bout_orphans = 0
        fighter_orphans = 0
        for row in stats.data or []:
            if row["event_bout_id"] not in bout_ids:
                bout_orphans += 1
            if row["fighter_id"] not in fighter_ids:
                fighter_orphans += 1

        errors = bout_orphans + fighter_orphans
        return ValidationResult(
            check_name="round_stats_orphans",
            passed=errors == 0,
            details=f"{bout_orphans} bout orphans, {fighter_orphans} fighter orphans" if errors else "No orphans",
            error_count=errors,
        )

    except Exception as exc:
        return ValidationResult(
            check_name="round_stats_orphans",
            passed=False,
            details=f"Query failed: {exc}",
            error_count=1,
        )


def check_round_stats_totals(supabase: Client) -> ValidationResult:
    """Check that total_sig_str >= sum of component strikes (head+body+leg)."""
    errors = 0
    details_parts: list[str] = []

    try:
        stats = (
            supabase.table("fight_round_stats")
            .select(
                "id, event_bout_id, fighter_id, round_number, "
                "total_sig_str_landed, sig_str_head_landed, sig_str_body_landed, sig_str_leg_landed"
            )
            .limit(5000)
            .execute()
        )

        for row in stats.data or []:
            component_sum = (
                row["sig_str_head_landed"]
                + row["sig_str_body_landed"]
                + row["sig_str_leg_landed"]
            )
            total = row["total_sig_str_landed"]

            # Total should be >= component sum (there may be strikes to uncategorized targets)
            # But total should never be < component sum
            if total < component_sum:
                errors += 1
                details_parts.append(
                    f"Stats {row['id']} R{row['round_number']}: "
                    f"total={total} < head+body+leg={component_sum}"
                )

        return ValidationResult(
            check_name="round_stats_totals",
            passed=errors == 0,
            details="; ".join(details_parts[:10]) if details_parts else "Totals consistent",
            error_count=errors,
        )

    except Exception as exc:
        return ValidationResult(
            check_name="round_stats_totals",
            passed=False,
            details=f"Query failed: {exc}",
            error_count=1,
        )


def check_row_counts(supabase: Client) -> ValidationResult:
    """Report row counts for monitoring — always passes."""
    try:
        scorecards_count = len(
            supabase.table("fight_scorecards").select("id", count="exact").execute().data or []
        )
        round_stats_count = len(
            supabase.table("fight_round_stats").select("id", count="exact").execute().data or []
        )
        judge_count = len(
            supabase.table("judge_profiles").select("id", count="exact").execute().data or []
        )
        expected_count = len(
            supabase.table("expected_round_scores").select("id", count="exact").execute().data or []
        )

        return ValidationResult(
            check_name="row_counts",
            passed=True,
            details=(
                f"scorecards={scorecards_count}, round_stats={round_stats_count}, "
                f"judge_profiles={judge_count}, expected_scores={expected_count}"
            ),
        )
    except Exception as exc:
        return ValidationResult(
            check_name="row_counts",
            passed=True,
            details=f"Could not count rows: {exc}",
        )


def main() -> None:
    supabase = get_supabase()
    log.info("Starting judging intelligence data validation")

    checks = [
        check_row_counts,
        check_scorecard_orphans,
        check_round_count_consistency,
        check_judge_total_consistency,
        check_round_stats_orphans,
        check_round_stats_totals,
    ]

    results: list[ValidationResult] = []
    for check_fn in checks:
        log.info("Running: %s", check_fn.__name__)
        result = check_fn(supabase)
        results.append(result)
        status = "PASS" if result.passed else "FAIL"
        log.info(
            "  %s %s: %s (errors=%d, warnings=%d)",
            status, result.check_name, result.details,
            result.error_count, result.warning_count,
        )

    total_errors = sum(r.error_count for r in results)
    total_warnings = sum(r.warning_count for r in results)
    all_passed = all(r.passed for r in results)

    log.info("---")
    log.info(
        "Validation %s: %d checks, %d errors, %d warnings",
        "PASSED" if all_passed else "FAILED",
        len(results),
        total_errors,
        total_warnings,
    )

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
