from __future__ import annotations

from ufc_rankings_v2 import (
    CATEGORY_ORDER,
    RankingRow,
    attach_fighter_ids,
    fetch_all_fighters,
    fetch_page_text,
    get_supabase,
    load_config,
    now_iso,
    parse_ranking_rows,
    parse_sections,
)


def sync_rankings_with_history(sb, rows: list[RankingRow], source_label: str, scraped_at: str) -> None:
    payload = [row.__dict__ for row in rows]
    if not payload:
        raise ValueError("No UFC ranking rows were parsed from the source page")

    sb.table("ufc_ranking_snapshots").insert(payload).execute()
    sb.table("ufc_rankings").upsert(payload, on_conflict="category_key,sort_order").execute()

    response = (
        sb.table("ufc_ranking_snapshots")
        .select("scraped_at")
        .eq("source_label", source_label)
        .order("scraped_at", desc=True)
        .limit(50)
        .execute()
    )
    snapshots = sorted({row["scraped_at"] for row in (response.data or []) if row.get("scraped_at")}, reverse=True)
    if len(snapshots) > 2:
        keep_after = snapshots[1]
        (
            sb.table("ufc_ranking_snapshots")
            .delete()
            .eq("source_label", source_label)
            .lt("scraped_at", keep_after)
            .execute()
        )


def main() -> None:
    config = load_config("config/ufc_rankings.yaml")
    if not config["job"].get("enabled", True):
        print("[INFO] job disabled")
        return

    scraped_at = now_iso()
    source_label = config["source"].get("source_label") or "UFC Rankings"
    source_url = config["source"]["rankings_url"]

    lines = fetch_page_text(config)
    sections = parse_sections(lines)
    parsed_rows: list[RankingRow] = []

    for category in CATEGORY_ORDER:
        matching_section = next((section_lines for section_category, section_lines in sections if section_category == category), None)
        if not matching_section:
            continue
        parsed_rows.extend(parse_ranking_rows(category, matching_section, source_label, source_url, scraped_at))

    sb = get_supabase(config)
    fighters = fetch_all_fighters(sb)
    resolved_rows = attach_fighter_ids(parsed_rows, fighters)
    sync_rankings_with_history(sb, resolved_rows, source_label, scraped_at)

    official_boards = len({row.category_key for row in resolved_rows})
    linked_rows = sum(1 for row in resolved_rows if row.fighter_id)
    print(
        f"[OK] synced {len(resolved_rows)} UFC ranking rows across {official_boards} boards with history | linked_fighters={linked_rows}"
    )


if __name__ == "__main__":
    main()
