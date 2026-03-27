from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import yaml
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright
from supabase import Client, create_client


@dataclass
class FighterSeed:
    fighter_id: str
    name: str
    slug: str | None = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_inline_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None


def normalize_multiline_text(value: str | None) -> str:
    if value is None:
        return ""
    normalized = value.replace("\r\n", "\n").replace("\r", "\n")
    lines = [normalize_inline_text(line) for line in normalized.split("\n")]
    return "\n".join(line for line in lines if line)


def slugify(name: str) -> str:
    value = name.strip().lower()
    value = re.sub(r"[^a-z0-9\s-]", "", value)
    value = re.sub(r"\s+", "-", value)
    value = re.sub(r"-+", "-", value)
    return value.strip("-")


def extract_slug_from_url(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"/athlete/([^/?#]+)", url)
    return match.group(1) if match else None


def normalize_key(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def load_config(path: str = "config/ufc_athlete_profiles.yaml") -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    if not isinstance(cfg, dict):
        raise ValueError(f"Invalid YAML config at {path}: expected a mapping at the root")

    required_keys = {"job", "source", "supabase"}
    missing = required_keys - set(cfg.keys())
    if missing:
        raise ValueError(
            f"Invalid config at {path}: missing top-level keys {sorted(missing)}. "
            f"Found: {sorted(cfg.keys())}"
        )

    return cfg


def get_supabase(cfg: dict[str, Any]) -> Client:
    url = os.environ[cfg["supabase"]["url_env"]]
    key_env = cfg["supabase"].get("key_env") or "SUPABASE_KEY"
    key = os.environ[key_env]
    return create_client(url, key)


def parse_int(value: str | int | float | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    match = re.search(r"(-?\d+)", str(value).replace(",", ""))
    return int(match.group(1)) if match else None


def parse_numeric(value: str | int | float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r"(-?\d+(?:\.\d+)?)", str(value).replace(",", ""))
    return float(match.group(1)) if match else None


def parse_birth_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return None


def calculate_age_from_birth_date(value: str | None) -> int | None:
    birth_date = parse_birth_date(value)
    if not birth_date:
        return None

    born = datetime.fromisoformat(f"{birth_date}T00:00:00+00:00").date()
    today = datetime.now(timezone.utc).date()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


def iter_nodes(payload: Any):
    if isinstance(payload, dict):
        yield payload
        for value in payload.values():
            yield from iter_nodes(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from iter_nodes(item)


def find_value_in_json(payloads: list[Any], aliases: list[str]) -> Any | None:
    alias_keys = {normalize_key(alias) for alias in aliases}

    for node in iter_nodes(payloads):
        if isinstance(node, dict):
            for key, value in node.items():
                if normalize_key(str(key)) in alias_keys and value not in (None, "", [], {}):
                    if isinstance(value, (dict, list)):
                        continue
                    return value
    return None


async def extract_embedded_json_payloads(page) -> list[Any]:
    payloads: list[Any] = []

    for raw_text in await page.locator("script[type='application/ld+json']").all_inner_texts():
        text = raw_text.strip()
        if not text:
            continue
        try:
            payloads.append(json.loads(text))
        except json.JSONDecodeError:
            continue

    next_data = page.locator("script#__NEXT_DATA__")
    if await next_data.count():
        text = (await next_data.first.inner_text()).strip()
        if text:
            try:
                payloads.append(json.loads(text))
            except json.JSONDecodeError:
                pass

    return payloads


async def extract_body_text(page) -> str:
    return normalize_multiline_text(await page.locator("body").inner_text())


async def extract_dom_profile_fields(page) -> dict[str, str]:
    fields: dict[str, str] = {}

    containers = await page.locator(".c-bio__field").all()
    for container in containers:
        label_locator = container.locator(".c-bio__label")
        text_locator = container.locator(".c-bio__text")

        if not await label_locator.count() or not await text_locator.count():
            continue

        label = normalize_inline_text(await label_locator.first.inner_text())
        value = normalize_inline_text(await text_locator.first.inner_text())

        if label and value:
            fields[label] = value

    return fields


def build_profile_url(fighter: FighterSeed, cfg: dict[str, Any]) -> str:
    slug = fighter.slug or slugify(fighter.name)
    return cfg["source"]["profile_url_template"].format(slug=slug)


def match_dom_field(fields: dict[str, str], aliases: list[str]) -> str | None:
    lowered = {key.lower(): value for key, value in fields.items()}
    for alias in aliases:
        value = lowered.get(alias.lower())
        if value:
            return value
    return None


def extract_profile_record(
    fighter: FighterSeed,
    source_url: str,
    dom_fields: dict[str, str],
    embedded_payloads: list[Any],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    info_map = cfg["mapping"]["info"]
    birth_date = find_value_in_json(embedded_payloads, ["birthDate", "birth_date"])

    def pick_text(key: str) -> str | None:
        return match_dom_field(dom_fields, info_map[key])

    return {
        "fighter_id": fighter.fighter_id,
        "source_url": source_url,
        "status": pick_text("status"),
        "place_of_birth": pick_text("place_of_birth"),
        "fighting_style": pick_text("fighting_style"),
        "trains_at": pick_text("trains_at"),
        "age": parse_int(pick_text("age")) or calculate_age_from_birth_date(birth_date),
        "height": pick_text("height"),
        "weight": pick_text("weight"),
        "octagon_debut": pick_text("octagon_debut"),
        "reach": pick_text("reach"),
        "leg_reach": pick_text("leg_reach"),
        "updated_at": now_iso(),
    }


def extract_basic_stats_record(
    fighter: FighterSeed,
    body_text: str,
) -> dict[str, Any]:
    flat_text = normalize_inline_text(body_text) or ""
    patterns = {
        "wins_by_ko": r"(\d+)\s+wins?\s+by\s+knockout",
        "wins_by_submission": r"(\d+)\s+wins?\s+by\s+submission",
        "first_round_finishes": r"(\d+)\s+first[- ]round finishes",
        "sig_strikes_landed": r"Sig\.\s*Strikes\s*Landed\s*(\d+)",
        "sig_strikes_attempted": r"Sig\.\s*Strikes\s*Attempted\s*(\d+)",
        "takedowns_landed": r"Takedowns\s*Landed\s*(\d+)",
        "takedowns_attempted": r"Takedowns\s*Attempted\s*(\d+)",
        "slpm": r"([\d\.]+)\s*Sig\.\s*Str\.\s*Landed\s*Per\s*Min",
        "sapm": r"([\d\.]+)\s*Sig\.\s*Str\.\s*Absorbed\s*Per\s*Min",
        "td_avg": r"([\d\.]+)\s*Takedown avg\s*Per 15 Min",
        "sub_avg": r"([\d\.]+)\s*Submission avg\s*Per 15 Min",
    }

    record = {
        "fighter_id": fighter.fighter_id,
        "wins_by_ko": None,
        "wins_by_submission": None,
        "first_round_finishes": None,
        "sig_strikes_landed": None,
        "sig_strikes_attempted": None,
        "takedowns_landed": None,
        "takedowns_attempted": None,
        "slpm": None,
        "sapm": None,
        "td_avg": None,
        "sub_avg": None,
        "updated_at": now_iso(),
    }

    numeric_keys = {"slpm", "sapm", "td_avg", "sub_avg"}

    for key, pattern in patterns.items():
        match = re.search(pattern, flat_text, flags=re.IGNORECASE)
        if match:
            parser = parse_numeric if key in numeric_keys else parse_int
            record[key] = parser(match.group(1))

    return record


def extract_label_value_pairs(container_text: str) -> list[tuple[str, str]]:
    lines = [normalize_inline_text(line) for line in container_text.splitlines()]
    lines = [line for line in lines if line]
    pairs: list[tuple[str, str]] = []

    i = 0
    while i < len(lines) - 1:
        label = lines[i]
        value = lines[i + 1]
        pairs.append((label, value))
        i += 2

    return pairs


async def extract_advanced_stats_record(page, fighter: FighterSeed) -> dict[str, Any]:
    record = {
        "fighter_id": fighter.fighter_id,
        "striking_accuracy_pct": None,
        "takedown_accuracy_pct": None,
        "sig_str_defense_pct": None,
        "takedown_defense_pct": None,
        "knockdown_avg": None,
        "average_fight_time": None,
        "sig_str_standing_landed": None,
        "sig_str_standing_pct": None,
        "sig_str_clinch_landed": None,
        "sig_str_clinch_pct": None,
        "sig_str_ground_landed": None,
        "sig_str_ground_pct": None,
        "sig_str_head_landed": None,
        "sig_str_head_pct": None,
        "sig_str_body_landed": None,
        "sig_str_body_pct": None,
        "sig_str_leg_landed": None,
        "sig_str_leg_pct": None,
        "win_by_ko_tko_count": None,
        "win_by_ko_tko_pct": None,
        "win_by_dec_count": None,
        "win_by_dec_pct": None,
        "win_by_sub_count": None,
        "win_by_sub_pct": None,
        "updated_at": now_iso(),
    }

    striking_title = page.locator("h2.e-t3", has_text="Striking accuracy")
    if await striking_title.count():
        wrapper = striking_title.first.locator("xpath=ancestor::div[contains(@class,'stats-records-inner')][1]")
        percent_text = normalize_inline_text(await wrapper.locator(".e-chart-circle__percent").first.inner_text()) if await wrapper.locator(".e-chart-circle__percent").count() else None
        record["striking_accuracy_pct"] = parse_numeric(percent_text)
        pairs = extract_label_value_pairs(await wrapper.inner_text())
        for label, value in pairs:
            normalized = normalize_key(label)
            if normalized == "sigstrikeslanded":
                record["sig_str_standing_landed"] = record["sig_str_standing_landed"]
            if normalized == "sigstrikeslanded":
                pass

    takedown_title = page.locator("h2.e-t3", has_text="Takedown Accuracy")
    if await takedown_title.count():
        wrapper = takedown_title.first.locator("xpath=ancestor::div[contains(@class,'stats-records-inner')][1]")
        percent_text = normalize_inline_text(await wrapper.locator(".e-chart-circle__percent").first.inner_text()) if await wrapper.locator(".e-chart-circle__percent").count() else None
        record["takedown_accuracy_pct"] = parse_numeric(percent_text)

    compare_groups = await page.locator(".c-stat-compare").all
