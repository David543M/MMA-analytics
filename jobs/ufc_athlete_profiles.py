from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import yaml
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from supabase import Client, create_client


@dataclass
class FighterSeed:
    fighter_id: str
    name: str
    slug: str | None = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(name: str) -> str:
    value = name.strip().lower()
    value = re.sub(r"[^a-z0-9\s-]", "", value)
    value = re.sub(r"\s+", "-", value)
    value = re.sub(r"-+", "-", value)
    return value.strip("-")


def load_config(path: str = "config/ufc_athlete_profiles.yaml") -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_supabase(cfg: dict[str, Any]) -> Client:
    url = os.environ[cfg["supabase"]["url_env"]]
    key_env = cfg["supabase"].get("key_env") or cfg["supabase"].get("service_role_key_env") or "SUPABASE_KEY"
    key = os.environ[key_env]
    return create_client(url, key)


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None


def parse_int(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"(-?\d+)", value.replace(",", ""))
    return int(match.group(1)) if match else None


def parse_numeric(value: str | None) -> float | None:
    if not value:
        return None
    match = re.search(r"(-?\d+(?:\.\d+)?)", value.replace(",", ""))
    return float(match.group(1)) if match else None


def parse_date(value: str | None) -> str | None:
    if not value:
        return None

    value = normalize_text(value)
    if not value:
        return None

    for fmt in ("%b. %d, %Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue

    return None


def extract_label_value_pairs_from_text(text: str) -> dict[str, str]:
    pairs: dict[str, str] = {}
    lines = [normalize_text(line) for line in text.splitlines()]
    lines = [line for line in lines if line]

    i = 0
    while i < len(lines) - 1:
        current = lines[i]
        nxt = lines[i + 1]

        if len(current) <= 40 and len(nxt) <= 120:
            if current not in pairs:
                pairs[current] = nxt
        i += 1

    return pairs


async def extract_info_pairs(page) -> dict[str, str]:
    pairs: dict[str, str] = {}

    dt_nodes = await page.locator("dt").all()
    for dt in dt_nodes:
        key = normalize_text(await dt.inner_text())
        if not key:
            continue

        dd = dt.locator("xpath=following-sibling::dd[1]")
        if await dd.count():
            value = normalize_text(await dd.first.inner_text())
            if value:
                pairs[key] = value

    if pairs:
        return pairs

    body_text = await page.locator("body").inner_text()
    return extract_label_value_pairs_from_text(body_text)


async def extract_body_text(page) -> str:
    return normalize_text(await page.locator("body").inner_text()) or ""


def build_profile_url(fighter: FighterSeed, cfg: dict[str, Any]) -> str:
    slug = fighter.slug or slugify(fighter.name)
    return cfg["source"]["profile_url_template"].format(slug=slug)


def match_alias(pairs: dict[str, str], aliases: list[str]) -> str | None:
    lowered = {k.lower(): v for k, v in pairs.items()}
    for alias in aliases:
        value = lowered.get(alias.lower())
        if value:
            return value
    return None


def extract_profile_record(
    fighter: FighterSeed,
    source_url: str,
    pairs: dict[str, str],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    info_map = cfg["mapping"]["info"]

    return {
        "fighter_id": fighter.fighter_id,
        "source_url": source_url,
        "status": match_alias(pairs, info_map["status"]),
        "place_of_birth": match_alias(pairs, info_map["place_of_birth"]),
        "fighting_style": match_alias(pairs, info_map["fighting_style"]),
        "trains_at": match_alias(pairs, info_map["trains_at"]),
        "age": parse_int(match_alias(pairs, info_map["age"])),
        "height": match_alias(pairs, info_map["height"]),
        "weight": match_alias(pairs, info_map["weight"]),
        "octagon_debut": match_alias(pairs, info_map["octagon_debut"]),
        "reach": match_alias(pairs, info_map["reach"]),
        "leg_reach": match_alias(pairs, info_map["leg_reach"]),
        "updated_at": now_iso(),
    }


def extract_stats_record(fighter: FighterSeed, body_text: str) -> dict[str, Any]:
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

    for key, pattern in patterns.items():
        match = re.search(pattern, body_text, flags=re.IGNORECASE)
        if not match:
            continue
        if key in {"slpm", "sapm", "td_avg", "sub_avg"}:
            record[key] = parse_numeric(match.group(1))
        else:
            record[key] = parse_int(match.group(1))

    return record


def extract_qa_rows(fighter: FighterSeed, body_text: str, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    questions = cfg["selectors"]["qa_question_candidates"]
    rows: list[dict[str, Any]] = []

    escaped_questions = "|".join(re.escape(q) for q in questions)

    for index, question in enumerate(questions):
        pattern = rf"{re.escape(question)}\s*(.+?)(?=(?:{escaped_questions})|$)"
        match = re.search(pattern, body_text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue

        answer = normalize_text(match.group(1))
        if not answer:
            continue

        rows.append({
            "fighter_id": fighter.fighter_id,
            "question": question,
            "answer": answer[:4000],
            "sort_order": index,
            "updated_at": now_iso(),
        })

    return rows


def extract_fight_history_rows(fighter: FighterSeed, body_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    lines = [normalize_text(line) for line in body_text.splitlines()]
    lines = [line for line in lines if line]

    for idx, line in enumerate(lines):
        if not line.startswith("UFC "):
            continue

        chunk = " ".join(lines[idx:idx + 6])

        opponent = None
        result = None
        method = None
        round_value = None
        time_value = None
        weight_class = None

        result_match = re.search(r"\b(win|loss|draw|nc|no contest)\b", chunk, flags=re.IGNORECASE)
        if result_match:
            result = result_match.group(1).lower()

        round_match = re.search(r"\bR(?:ound)?\s*(\d+)\b", chunk, flags=re.IGNORECASE)
        if round_match:
            round_value = parse_int(round_match.group(1))

        time_match = re.search(r"\b(\d{1,2}:\d{2})\b", chunk)
        if time_match:
            time_value = time_match.group(1)

        method_match = re.search(r"\b(KO/TKO|TKO|KO|Submission|Decision)\b", chunk, flags=re.IGNORECASE)
        if method_match:
            method = method_match.group(1)

        vs_match = re.search(r"(?:vs\.?|def\.?|lost to)\s+([A-Z][A-Za-z\-\.' ]+)", chunk, flags=re.IGNORECASE)
        if vs_match:
            opponent = normalize_text(vs_match.group(1))

        weight_match = re.search(
            r"\b(Flyweight|Bantamweight|Featherweight|Lightweight|Welterweight|Middleweight|Light Heavyweight|Heavyweight|Women's Strawweight|Women's Flyweight|Women's Bantamweight|Women's Featherweight)\b",
            chunk,
            flags=re.IGNORECASE,
        )
        if weight_match:
            weight_class = weight_match.group(1)

        date_value = parse_date(line)

        rows.append({
            "fighter_id": fighter.fighter_id,
            "event_name": line[:255],
            "event_date": date_value,
            "opponent_name": opponent,
            "result": result,
            "method": method,
            "round": round_value,
            "time": time_value,
            "weight_class": weight_class,
            "notes": chunk[:4000],
            "updated_at": now_iso(),
        })

    return rows


def names_look_related(expected: str, actual: str | None) -> bool:
    if not expected or not actual:
        return False

    expected_tokens = {t for t in re.findall(r"[a-z0-9]+", expected.lower()) if len(t) > 2}
    actual_tokens = {t for t in re.findall(r"[a-z0-9]+", actual.lower()) if len(t) > 2}

    if not expected_tokens or not actual_tokens:
        return False

    overlap = expected_tokens & actual_tokens
    return len(overlap) >= max(1, min(len(expected_tokens), 2))


def count_meaningful_profile_fields(profile: dict[str, Any]) -> int:
    ignored_keys = {"fighter_id", "source_url", "updated_at"}
    return sum(1 for key, value in profile.items() if key not in ignored_keys and value not in (None, "", []))


def count_meaningful_stats(stats: dict[str, Any]) -> int:
    ignored_keys = {"fighter_id", "updated_at"}
    return sum(1 for key, value in stats.items() if key not in ignored_keys and value not in (None, "", []))


def validate_scrape_result(
    fighter: FighterSeed,
    page_title: str | None,
    body_text: str,
    result: dict[str, Any],
    cfg: dict[str, Any],
) -> tuple[bool, str]:
    validation_cfg = cfg.get("validation", {})
    require_name_match = validation_cfg.get("require_name_match", True)
    min_profile_fields = validation_cfg.get("min_profile_fields", 2)
    min_stats_fields = validation_cfg.get("min_stats_fields", 1)

    body_window = body_text[:5000]

    if require_name_match:
        title_match = names_look_related(fighter.name, page_title)
        body_match = names_look_related(fighter.name, body_window)
        if not title_match and not body_match:
            return False, "page did not validate against fighter name"

    profile_count = count_meaningful_profile_fields(result["profile"])
    stats_count = count_meaningful_stats(result["stats"])
    qa_count = len(result["qa_rows"])
    history_count = len(result["history_rows"])

    if profile_count < min_profile_fields and stats_count < min_stats_fields and qa_count == 0 and history_count == 0:
        return False, "no meaningful data extracted"

    return True, (
        f"profile_fields={profile_count} "
        f"stats_fields={stats_count} "
        f"qa_rows={qa_count} "
        f"history_rows={history_count}"
    )


async def scrape_one(page, fighter: FighterSeed, cfg: dict[str, Any]) -> dict[str, Any]:
    url = build_profile_url(fighter, cfg)
    response = await page.goto(url, wait_until="networkidle", timeout=cfg["job"]["timeout_ms"])

    final_url = page.url
    page_title = normalize_text(await page.title())
    pairs = await extract_info_pairs(page)
    body_text = await extract_body_text(page)

    result = {
        "profile": extract_profile_record(fighter, final_url, pairs, cfg),
        "stats": extract_stats_record(fighter, body_text),
        "qa_rows": extract_qa_rows(fighter, body_text, cfg),
        "history_rows": extract_fight_history_rows(fighter, body_text),
        "meta": {
            "requested_url": url,
            "final_url": final_url,
            "page_title": page_title,
            "status_code": response.status if response else None,
        },
    }

    is_valid, reason = validate_scrape_result(fighter, page_title, body_text, result, cfg)
    result["meta"]["valid"] = is_valid
    result["meta"]["validation_reason"] = reason
    return result


def fetch_fighter_seeds(sb: Client, limit: int) -> list[FighterSeed]:
    response = sb.table("fighters").select("id,name").limit(limit).execute()
    data = response.data or []
    return [FighterSeed(fighter_id=row["id"], name=row["name"]) for row in data]


def upsert_profile(sb: Client, row: dict[str, Any]) -> None:
    sb.table("fighter_ufc_profiles").upsert(row).execute()


def upsert_stats(sb: Client, row: dict[str, Any]) -> None:
    sb.table("fighter_ufc_stats").upsert(row).execute()


def replace_qa_rows(sb: Client, fighter_id: str, rows: list[dict[str, Any]]) -> None:
    sb.table("fighter_ufc_qa").delete().eq("fighter_id", fighter_id).execute()
    if rows:
        sb.table("fighter_ufc_qa").insert(rows).execute()


def replace_fight_history_rows(sb: Client, fighter_id: str, rows: list[dict[str, Any]]) -> None:
    sb.table("fighter_ufc_fight_history").delete().eq("fighter_id", fighter_id).execute()
    if rows:
        sb.table("fighter_ufc_fight_history").insert(rows).execute()


async def main():
    cfg = load_config("config/ufc_athlete_profiles.yaml")
    sb = get_supabase(cfg)
    fighters = fetch_fighter_seeds(sb, cfg["job"]["batch_size"])

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=cfg["job"].get("headless", True))
        page = await browser.new_page()

        for fighter in fighters:
            try:
                result = await scrape_one(page, fighter, cfg)

                if not result["meta"]["valid"]:
                    print(
                        f"[SKIP] {fighter.name} | "
                        f"title={result['meta']['page_title']} | "
                        f"url={result['meta']['final_url']} | "
                        f"reason={result['meta']['validation_reason']}"
                    )
                else:
                    upsert_profile(sb, result["profile"])
                    upsert_stats(sb, result["stats"])
                    replace_qa_rows(sb, fighter.fighter_id, result["qa_rows"])
                    replace_fight_history_rows(sb, fighter.fighter_id, result["history_rows"])

                    print(
                        f"[OK] {fighter.name} | "
                        f"title={result['meta']['page_title']} | "
                        f"url={result['meta']['final_url']} | "
                        f"{result['meta']['validation_reason']}"
                    )

            except PlaywrightTimeoutError:
                print(f"[TIMEOUT] {fighter.name}")
            except Exception as exc:
                print(f"[FAIL] {fighter.name}: {exc}")

            await page.wait_for_timeout(cfg["job"]["delay_ms"])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
