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
    key_env = cfg["supabase"].get("key_env") or cfg["supabase"].get("service_role_key_env") or "SUPABASE_KEY"
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


def extract_label_value_pairs_from_text(text: str) -> dict[str, str]:
    pairs: dict[str, str] = {}
    lines = [normalize_inline_text(line) for line in text.splitlines()]
    lines = [line for line in lines if line]

    i = 0
    while i < len(lines) - 1:
        current = lines[i]
        nxt = lines[i + 1]
        if len(current) <= 40 and len(nxt) <= 120 and current not in pairs:
            pairs[current] = nxt
        i += 1

    return pairs


def find_value_in_json(payloads: list[Any], aliases: list[str]) -> Any | None:
    alias_keys = {normalize_key(alias) for alias in aliases}

    for node in iter_nodes(payloads):
        if isinstance(node, dict):
            for key, value in node.items():
                if normalize_key(str(key)) in alias_keys and value not in (None, "", [], {}):
                    if isinstance(value, (dict, list)):
                        continue
                    return value

            label = node.get("label") or node.get("name") or node.get("title") or node.get("stat")
            if normalize_key(str(label)) in alias_keys:
                for candidate_key in ("value", "amount", "displayValue", "text"):
                    candidate = node.get(candidate_key)
                    if candidate not in (None, "", [], {}):
                        return candidate

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


async def extract_info_pairs(page) -> dict[str, str]:
    pairs: dict[str, str] = {}

    dt_nodes = await page.locator("dt").all()
    for dt in dt_nodes:
        key = normalize_inline_text(await dt.inner_text())
        if not key:
            continue

        dd = dt.locator("xpath=following-sibling::dd[1]")
        if await dd.count():
            value = normalize_inline_text(await dd.first.inner_text())
            if value:
                pairs[key] = value

    if pairs:
        return pairs

    body_text = await page.locator("body").inner_text()
    return extract_label_value_pairs_from_text(normalize_multiline_text(body_text))


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

        if not label or not value:
            continue

        fields[label] = value

    return fields


def build_profile_url(fighter: FighterSeed, cfg: dict[str, Any]) -> str:
    slug = fighter.slug or slugify(fighter.name)
    return cfg["source"]["profile_url_template"].format(slug=slug)


def match_alias(pairs: dict[str, str], aliases: list[str]) -> str | None:
    lowered = {key.lower(): value for key, value in pairs.items()}
    for alias in aliases:
        value = lowered.get(alias.lower())
        if value:
            return value
    return None


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
    pairs: dict[str, str],
    dom_fields: dict[str, str],
    embedded_payloads: list[Any],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    info_map = cfg["mapping"]["info"]
    json_map = cfg["mapping"].get("json", {})
    birth_date = find_value_in_json(embedded_payloads, ["birthDate", "birth_date"])

    def pick_text(key: str) -> str | None:
        return (
            match_dom_field(dom_fields, info_map[key])
            or match_alias(pairs, info_map[key])
            or normalize_inline_text(find_value_in_json(embedded_payloads, json_map.get(key, [])))
        )

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


def extract_stats_record(
    fighter: FighterSeed,
    body_text: str,
    embedded_payloads: list[Any],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    flat_text = normalize_inline_text(body_text) or ""
    json_map = cfg["mapping"].get("json", {})
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
            continue

        json_value = find_value_in_json(embedded_payloads, json_map.get(key, []))
        parser = parse_numeric if key in numeric_keys else parse_int
        record[key] = parser(json_value)

    return record


def names_look_related(expected: str, actual: str | None) -> bool:
    if not expected or not actual:
        return False

    expected_tokens = {token for token in re.findall(r"[a-z0-9]+", expected.lower()) if len(token) > 2}
    actual_tokens = {token for token in re.findall(r"[a-z0-9]+", actual.lower()) if len(token) > 2}

    if not expected_tokens or not actual_tokens:
        return False

    overlap = expected_tokens & actual_tokens
    return len(overlap) >= max(1, min(len(expected_tokens), 2))


def slug_matches_fighter(expected_name: str, final_url: str) -> bool:
    final_slug = extract_slug_from_url(final_url)
    if not final_slug:
        return False
    return final_slug == slugify(expected_name)


def count_meaningful_fields(payload: dict[str, Any], ignored_keys: set[str]) -> int:
    return sum(1 for key, value in payload.items() if key not in ignored_keys and value not in (None, "", []))


def validate_scrape_result(
    fighter: FighterSeed,
    page_title: str | None,
    body_text: str,
    result: dict[str, Any],
    cfg: dict[str, Any],
) -> tuple[bool, str]:
    validation_cfg = cfg.get("validation", {})
    require_name_match = validation_cfg.get("require_name_match", True)
    min_profile_fields = validation_cfg.get("min_profile_fields", 1)
    min_stats_fields = validation_cfg.get("min_stats_fields", 1)

    final_url = result["meta"]["final_url"]

    if require_name_match:
        body_window = normalize_inline_text(body_text[:5000])
        title_match = names_look_related(fighter.name, page_title)
        body_match = names_look_related(fighter.name, body_window)
        slug_match = fighter.slug == extract_slug_from_url(final_url) if fighter.slug else slug_matches_fighter(fighter.name, final_url)

        if not slug_match and not title_match and not body_match:
            return False, "page did not validate against fighter name"

        if fighter.slug is None and not slug_match:
            return False, "final UFC slug does not match expected fighter"

    profile_count = count_meaningful_fields(result["profile"], {"fighter_id", "source_url", "updated_at"})
    stats_count = count_meaningful_fields(result["stats"], {"fighter_id", "updated_at"})

    if profile_count < min_profile_fields and stats_count < min_stats_fields:
        return False, "no meaningful profile or stats extracted"

    return True, f"profile_fields={profile_count} stats_fields={stats_count}"


async def scrape_one(page, fighter: FighterSeed, cfg: dict[str, Any]) -> dict[str, Any]:
    url = build_profile_url(fighter, cfg)
    response = await page.goto(url, wait_until="networkidle", timeout=cfg["job"]["timeout_ms"])

    final_url = page.url
    page_title = normalize_inline_text(await page.title())
    pairs = await extract_info_pairs(page)
    body_text = await extract_body_text(page)
    dom_profile_fields = await extract_dom_profile_fields(page)
    embedded_payloads = await extract_embedded_json_payloads(page)

    result = {
        "profile": extract_profile_record(fighter, final_url, pairs, dom_profile_fields, embedded_payloads, cfg),
        "stats": extract_stats_record(fighter, body_text, embedded_payloads, cfg),
        "meta": {
            "requested_url": url,
            "final_url": final_url,
            "page_title": page_title,
            "status_code": response.status if response else None,
            "embedded_payloads": len(embedded_payloads),
        },
    }

    is_valid, reason = validate_scrape_result(fighter, page_title, body_text, result, cfg)
    result["meta"]["valid"] = is_valid
    result["meta"]["validation_reason"] = reason
    return result


def fetch_all_fighter_seeds(sb: Client, page_size: int) -> list[FighterSeed]:
    seeds: list[FighterSeed] = []
    offset = 0

    while True:
        try:
            response = (
                sb.table("fighters")
                .select("id,name,ufc_slug")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            rows = response.data or []
            batch = [
                FighterSeed(fighter_id=row["id"], name=row["name"], slug=row.get("ufc_slug"))
                for row in rows
            ]
        except Exception:
            response = (
                sb.table("fighters")
                .select("id,name")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            rows = response.data or []
            batch = [FighterSeed(fighter_id=row["id"], name=row["name"]) for row in rows]

        if not batch:
            break

        seeds.extend(batch)

        if len(batch) < page_size:
            break

        offset += page_size

    fighter_ids = [seed.fighter_id for seed in seeds]
    if not fighter_ids:
        return seeds

    slug_map: dict[str, str] = {}
    chunk_size = 500

    for i in range(0, len(fighter_ids), chunk_size):
        chunk = fighter_ids[i : i + chunk_size]
        try:
            response = (
                sb.table("fighter_ufc_profiles")
                .select("fighter_id,source_url")
                .in_("fighter_id", chunk)
                .execute()
            )
            for row in response.data or []:
                slug = extract_slug_from_url(row.get("source_url"))
                if slug:
                    slug_map[row["fighter_id"]] = slug
        except Exception:
            break

    for seed in seeds:
        if not seed.slug:
            seed.slug = slug_map.get(seed.fighter_id)

    return seeds


def upsert_profile(sb: Client, row: dict[str, Any]) -> None:
    sb.table("fighter_ufc_profiles").upsert(row).execute()


def upsert_stats(sb: Client, row: dict[str, Any]) -> None:
    sb.table("fighter_ufc_stats").upsert(row).execute()


async def main():
    cfg = load_config("config/ufc_athlete_profiles.yaml")
    if not cfg["job"].get("enabled", True):
        print("[INFO] job disabled")
        return

    sb = get_supabase(cfg)
    page_size = cfg["job"].get("page_size", 200)
    fighters = fetch_all_fighter_seeds(sb, page_size)

    test_limit = cfg["job"].get("test_limit")
    if isinstance(test_limit, int) and test_limit > 0:
        fighters = fighters[:test_limit]

    print(f"[INFO] fetched {len(fighters)} fighters")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=cfg["job"].get("headless", True))
        page = await browser.new_page()

        for index, fighter in enumerate(fighters, start=1):
            try:
                result = await scrape_one(page, fighter, cfg)

                if not result["meta"]["valid"]:
                    print(
                        f"[SKIP] {index}/{len(fighters)} {fighter.name} | "
                        f"title={result['meta']['page_title']} | "
                        f"url={result['meta']['final_url']} | "
                        f"reason={result['meta']['validation_reason']}"
                    )
                else:
                    upsert_profile(sb, result["profile"])
                    upsert_stats(sb, result["stats"])

                    print(
                        f"[OK] {index}/{len(fighters)} {fighter.name} | "
                        f"title={result['meta']['page_title']} | "
                        f"url={result['meta']['final_url']} | "
                        f"{result['meta']['validation_reason']} | "
                        f"embedded_json={result['meta']['embedded_payloads']}"
                    )

            except PlaywrightTimeoutError:
                print(f"[TIMEOUT] {index}/{len(fighters)} {fighter.name}")
            except Exception as exc:
                print(f"[FAIL] {index}/{len(fighters)} {fighter.name}: {exc}")

            await page.wait_for_timeout(cfg["job"]["delay_ms"])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
