from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
import yaml
from bs4 import BeautifulSoup
from supabase import Client, create_client

P4P_LABELS = [
    "Men's Pound-for-Pound",
    "Women's Pound-for-Pound",
]

MEN_DIVISIONS = [
    "Heavyweight",
    "Light Heavyweight",
    "Middleweight",
    "Welterweight",
    "Lightweight",
    "Featherweight",
    "Bantamweight",
    "Flyweight",
]

WOMEN_DIVISIONS = [
    "Women's Strawweight",
    "Women's Flyweight",
    "Women's Bantamweight",
    "Women's Featherweight",
]

CATEGORY_ORDER = [*P4P_LABELS, *MEN_DIVISIONS, *WOMEN_DIVISIONS]

CATEGORY_ALIASES: dict[str, str] = {
    "mens pound for pound": "Men's Pound-for-Pound",
    "men's pound for pound": "Men's Pound-for-Pound",
    "mens pound for pound top rank": "Men's Pound-for-Pound",
    "men's pound for pound top rank": "Men's Pound-for-Pound",
    "womens pound for pound": "Women's Pound-for-Pound",
    "women's pound for pound": "Women's Pound-for-Pound",
    "womens pound for pound top rank": "Women's Pound-for-Pound",
    "women's pound for pound top rank": "Women's Pound-for-Pound",
    "light heavyweight": "Light Heavyweight",
    "womens strawweight": "Women's Strawweight",
    "women's strawweight": "Women's Strawweight",
    "womens flyweight": "Women's Flyweight",
    "women's flyweight": "Women's Flyweight",
    "womens bantamweight": "Women's Bantamweight",
    "women's bantamweight": "Women's Bantamweight",
    "womens featherweight": "Women's Featherweight",
    "women's featherweight": "Women's Featherweight",
}

RANK_LINE_PATTERN = re.compile(r"^(?P<rank>\d{1,2})\s*\|\s*(?P<name>[^|]+?)(?:\s*\|\s*(?P<extra>.*))?$")


@dataclass
class RankingRow:
    category_key: str
    category_label: str
    category_group: str
    division_label: str | None
    sort_order: int
    rank_position: int | None
    fighter_name: str
    fighter_id: str | None
    is_champion: bool
    is_interim: bool
    source_label: str
    source_url: str
    scraped_at: str
    updated_at: str


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_spaces(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None


def normalize_name_key(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def normalize_category_label(value: str | None) -> str | None:
    cleaned = normalize_spaces(value)
    if not cleaned:
        return None

    normalized_key = re.sub(r"[^a-z0-9]+", " ", cleaned.lower()).strip()
    normalized_key = normalized_key.replace(" top rank", "")

    if cleaned in CATEGORY_ORDER:
        return cleaned
    if normalized_key in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[normalized_key]

    for label in CATEGORY_ORDER:
        if normalized_key == re.sub(r"[^a-z0-9]+", " ", label.lower()).strip():
            return label

    return None


def get_category_group(label: str) -> str:
    if label in P4P_LABELS:
        return "p4p"
    if label in WOMEN_DIVISIONS:
        return "women"
    return "men"


def get_category_key(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")


def load_config(path: str = "config/ufc_rankings.yaml") -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}

    if not isinstance(config, dict):
        raise ValueError(f"Invalid YAML config at {path}: expected a mapping root")

    required_keys = {"job", "source", "supabase"}
    missing = required_keys - set(config.keys())
    if missing:
        raise ValueError(f"Invalid config at {path}: missing top-level keys {sorted(missing)}")

    return config


def get_supabase(config: dict[str, Any]) -> Client:
    url = os.environ[config["supabase"]["url_env"]]
    key = os.environ[config["supabase"].get("key_env") or "SUPABASE_KEY"]
    return create_client(url, key)


def fetch_page_text(config: dict[str, Any]) -> list[str]:
    response = requests.get(
        config["source"]["rankings_url"],
        timeout=config["job"].get("timeout_seconds", 30),
        headers={"User-Agent": config["job"].get("user_agent")},
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    raw_lines = [normalize_spaces(line) for line in soup.get_text("\n").splitlines()]
    lines: list[str] = []

    for line in raw_lines:
        if not line:
            continue
        if lines and lines[-1] == line:
            continue
        lines.append(line)

    return lines


def parse_sections(lines: list[str]) -> list[tuple[str, list[str]]]:
    category_points: list[tuple[int, str]] = []

    for index, line in enumerate(lines):
        category = normalize_category_label(line)
        if not category:
            continue
        if category_points and category_points[-1][1] == category and index - category_points[-1][0] <= 2:
            continue
        category_points.append((index, category))

    sections: list[tuple[str, list[str]]] = []

    for idx, (start_index, category) in enumerate(category_points):
        end_index = category_points[idx + 1][0] if idx + 1 < len(category_points) else len(lines)
        section_lines = lines[start_index + 1 : end_index]
        if section_lines:
            sections.append((category, section_lines))

    return sections


def extract_champion_name(section_lines: list[str]) -> str | None:
    previous_value: str | None = None

    for line in section_lines[:12]:
        lowered = line.lower()
        if lowered in {"champion", "interim champion"}:
            return previous_value
        if RANK_LINE_PATTERN.match(line):
            continue
        if lowered.startswith("last updated"):
            break
        if "how are rankings determined" in lowered:
            break
        previous_value = line

    return None


def parse_ranking_rows(category: str, section_lines: list[str], source_label: str, source_url: str, scraped_at: str) -> list[RankingRow]:
    rows: list[RankingRow] = []
    category_group = get_category_group(category)
    category_key = get_category_key(category)
    division_label = None if category_group == "p4p" else category
    next_sort_order = 1

    champion_name = extract_champion_name(section_lines) if category_group != "p4p" else None
    if champion_name:
        rows.append(
            RankingRow(
                category_key=category_key,
                category_label=category,
                category_group=category_group,
                division_label=division_label,
                sort_order=next_sort_order,
                rank_position=None,
                fighter_name=champion_name,
                fighter_id=None,
                is_champion=True,
                is_interim=False,
                source_label=source_label,
                source_url=source_url,
                scraped_at=scraped_at,
                updated_at=scraped_at,
            )
        )
        next_sort_order += 1

    seen_names: set[str] = {normalize_name_key(champion_name)} if champion_name else set()

    for line in section_lines:
        match = RANK_LINE_PATTERN.match(line)
        if not match:
            continue

        fighter_name = normalize_spaces(match.group("name"))
        if not fighter_name:
            continue

        name_key = normalize_name_key(fighter_name)
        if name_key in seen_names:
            continue

        extra = normalize_spaces(match.group("extra")) or ""
        rows.append(
            RankingRow(
                category_key=category_key,
                category_label=category,
                category_group=category_group,
                division_label=division_label,
                sort_order=next_sort_order,
                rank_position=int(match.group("rank")),
                fighter_name=fighter_name,
                fighter_id=None,
                is_champion=False,
                is_interim="interim" in extra.lower(),
                source_label=source_label,
                source_url=source_url,
                scraped_at=scraped_at,
                updated_at=scraped_at,
            )
        )
        seen_names.add(name_key)
        next_sort_order += 1

    return rows


def fetch_all_fighters(sb: Client) -> list[dict[str, Any]]:
    fighters: list[dict[str, Any]] = []
    offset = 0
    page_size = 1000

    while True:
        response = (
            sb.table("fighters")
            .select("id,name,division")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = response.data or []
        if not batch:
            break
        fighters.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    return fighters


def attach_fighter_ids(rows: list[RankingRow], fighters: list[dict[str, Any]]) -> list[RankingRow]:
    by_name_and_division: dict[tuple[str, str], list[dict[str, Any]]] = {}
    by_name: dict[str, list[dict[str, Any]]] = {}

    for fighter in fighters:
        name_key = normalize_name_key(fighter.get("name"))
        division_label = normalize_category_label(fighter.get("division"))
        if not name_key:
            continue
        by_name.setdefault(name_key, []).append(fighter)
        if division_label:
            by_name_and_division.setdefault((name_key, division_label), []).append(fighter)

    resolved_rows: list[RankingRow] = []

    for row in rows:
        name_key = normalize_name_key(row.fighter_name)
        resolved_fighter_id: str | None = None

        if row.division_label:
            matches = by_name_and_division.get((name_key, row.division_label), [])
            if len(matches) == 1:
                resolved_fighter_id = matches[0]["id"]

        if not resolved_fighter_id:
            matches = by_name.get(name_key, [])
            if len(matches) == 1:
                resolved_fighter_id = matches[0]["id"]

        resolved_rows.append(
            RankingRow(
                category_key=row.category_key,
                category_label=row.category_label,
                category_group=row.category_group,
                division_label=row.division_label,
                sort_order=row.sort_order,
                rank_position=row.rank_position,
                fighter_name=row.fighter_name,
                fighter_id=resolved_fighter_id,
                is_champion=row.is_champion,
                is_interim=row.is_interim,
                source_label=row.source_label,
                source_url=row.source_url,
                scraped_at=row.scraped_at,
                updated_at=row.updated_at,
            )
        )

    return resolved_rows


def sync_rankings(sb: Client, rows: list[RankingRow], source_label: str, scraped_at: str) -> None:
    payload = [row.__dict__ for row in rows]
    if not payload:
        raise ValueError("No UFC ranking rows were parsed from the source page")

    sb.table("ufc_rankings").upsert(payload, on_conflict="category_key,sort_order").execute()
    (
        sb.table("ufc_rankings")
        .delete()
        .eq("source_label", source_label)
        .lt("scraped_at", scraped_at)
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
    sync_rankings(sb, resolved_rows, source_label, scraped_at)

    official_boards = len({row.category_key for row in resolved_rows})
    linked_rows = sum(1 for row in resolved_rows if row.fighter_id)
    print(
        f"[OK] synced {len(resolved_rows)} UFC ranking rows across {official_boards} boards | linked_fighters={linked_rows}"
    )


if __name__ == "__main__":
    main()
