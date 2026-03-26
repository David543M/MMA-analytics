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


def parse_date(value: str | None) -> str | None:
    if not value:
        return None

    value = normalize_inline_text(value)
    if not value:
        return None

    for fmt in ("%b. %d, %Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue

    return None


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


def extract_profile_record(
    fighter: FighterSeed,
    source_url: str,
    pairs: dict[str, str],
    embedded_payloads: list[Any],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    info_map = cfg["mapping"]["info"]
    json_map = cfg["mapping"].get("json", {})

    def pick_text(key: str) -> str | None:
        return match_alias(pairs, info_map[key]) or normalize_inline_text(
            find_value_in_json(embedded_payloads, json_map.get(key, []))
        )

    return {
        "fighter_id": fighter.fighter_id,
        "source_url": source_url,
        "status": pick_text("status"),
        "place_of_birth": pick_text("place_of_birth"),
        "fighting_style": pick_text("fighting_style"),
        "trains_at": pick_text("trains_at"),
        "age": parse_int(pick_text("age")),
        "height": pick_text("height"),
        "weight": pick_text("weight"),
        "octagon_debut": pick_text("octagon_debut"),
        "reach": pick_text("reach"),
        "leg_reach": pick_text("leg_reach"),
        "updated_at": now_iso(),
    }


def extract_stats_record(fighter: FighterSeed, body_text: str, embedded_payloads: list[Any], cfg: dict[str, Any]) -> dict[str, Any]:
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


def extract_qa_rows(fighter: FighterSeed, body_text: str, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    questions = cfg["selectors"]["qa_question_candidates"]
    lowered_questions = {normalize_inline_text(question).lower(): question for question in questions}
    rows: list[dict[str, Any]] = []
    lines = [line for line in body_text.splitlines() if line]

    i = 0
    while i < len(lines):
        line = lines[i]
        canonical = lowered_questions.get(line.lower())
        if not canonical:
            i += 1
            continue

        answer_lines: list[str] = []
        j = i + 1
        while j < len(lines):
            if lines[j].lower() in lowered_questions:
                break
            answer_lines.append(lines[j])
            j += 1

        answer = normalize_inline_text(" ".join(answer_lines))
        if answer:
            rows.append(
                {
                    "fighter_id": fighter.fighter_id,
                    "question": canonical,
                    "answer": answer[:4000],
                    "sort_order": questions.index(canonical),
                    "updated_at": now_iso(),
                }
            )

        i = j

    return rows


def extract_fight_history_rows(fighter: FighterSeed, body_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    lines = [line for line in body_text.splitlines() if line]

    for idx, line in enumerate(lines):
        if not line.startswith("UFC "):
            continue

        chunk = " ".join(lines[idx : idx + 8])
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
            opponent = normalize_inline_text(vs_match.group(1))

        weight_match = re.search(
            r"\b(Flyweight|Bantamweight|Featherweight|Lightweight|Welterweight|Middleweight|Light Heavyweight|Heavyweight|Women's Strawweight|Women's Flyweight|Women's Bantamweight|Women's Featherweight)\b",
            chunk,
            flags=re.IGNORECASE,
        )
        if weight_match:
            weight_class = weight_match.group(1)

        rows.append(
            {
                "fighter_id": fighter.fighter_id,
                "event_name": line[:255],
                "event_date": parse_date(line),
                "opponent_name": opponent,
                "result": result,
                "method": method,
                "round": round_value,
                "time": time_value,
                "weight_class": weight_class,
                "notes": chunk[:4000],
                "updated_at": now_iso(),
            }
        )

    return rows


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
    min_profile_fields = validation_cfg.get("min_profile_fields", 2)
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
    page_title = normalize_inline_text(await page.title())
    pairs = await extract_info_pairs(page)
    body_text = await extract_body_text(page)
    embedded_payloads = await extract_embedded_json_payloads(page)

    result = {
        "profile": extract_profile_record(fighter, final_url, pairs, embedded_payloads, cfg),
        "stats": extract_stats_record(fighter, body_text, embedded_payloads, cfg),
        "qa_rows": extract_qa_rows(fighter, body_text, cfg),
        "history_rows": extract_fight_history_rows(fighter, body_text),
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


def fetch_fighter_seeds(sb: Client, limit: int) -> list[FighterSeed]:
    try:
        fighters_response = sb.table("fighters").select("id,name,ufc_slug").limit(limit).execute()
        fighters_data = fighters_response.data or []
        seeds = [
            FighterSeed(fighter_id=row["id"], name=row["name"], slug=row.get("ufc_slug"))
            for row in fighters_data
        ]
    except Exception:
        fighters_response = sb.table("fighters").select("id,name").limit(limit).execute()
        fighters_data = fighters_response.data or []
        seeds = [FighterSeed(fighter_id=row["id"], name=row["name"]) for row in fighters_data]

    fighter_ids = [seed.fighter_id for seed in seeds]
    if not fighter_ids:
        return seeds

    try:
        profiles_response = (
            sb.table("fighter_ufc_profiles")
            .select("fighter_id,source_url")
            .in_("fighter_id", fighter_ids)
            .execute()
        )
        slug_map = {
            row["fighter_id"]: extract_slug_from_url(row.get("source_url"))
            for row in (profiles_response.data or [])
            if extract_slug_from_url(row.get("source_url"))
        }
    except Exception:
        slug_map = {}

    for seed in seeds:
        if not seed.slug:
            seed.slug = slug_map.get(seed.fighter_id)

    return seeds


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

def maybe_write_debug_payload(cfg: dict[str, Any], fighter: FighterSeed, result: dict[str, Any], payloads: list[Any]) -> None:
    debug_cfg = cfg.get("debug", {})
    if not debug_cfg.get("enabled", False):
        return

    target_name = debug_cfg.get("fighter_name")
    if target_name and fighter.name.lower() != target_name.lower():
        return

    if not debug_cfg.get("write_payload_file", True):
        return

    output_path = debug_cfg.get("payload_file", "debug_ufc_payload.json")
    debug_blob = {
        "fighter": {
            "fighter_id": fighter.fighter_id,
            "name": fighter.name,
            "slug": fighter.slug,
        },
        "meta": result["meta"],
        "profile": result["profile"],
        "stats": result["stats"],
        "qa_rows_count": len(result["qa_rows"]),
        "history_rows_count": len(result["history_rows"]),
        "embedded_payloads": payloads,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(debug_blob, f, ensure_ascii=False, indent=2)



async def main():
    cfg = load_config("config/ufc_athlete_profiles.yaml")
    if not cfg["job"].get("enabled", True):
        print("[INFO] job disabled")
        return

    sb = get_supabase(cfg)
    fighters = fetch_fighter_seeds(sb, cfg["job"]["batch_size"])

    test_limit = cfg["job"].get("test_limit")
    if isinstance(test_limit, int) and test_limit > 0:
        fighters = fighters[:test_limit]

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
                        f"{result['meta']['validation_reason']} | "
                        f"embedded_json={result['meta']['embedded_payloads']}"
                    )

            except PlaywrightTimeoutError:
                print(f"[TIMEOUT] {fighter.name}")
            except Exception as exc:
                print(f"[FAIL] {fighter.name}: {exc}")

            await page.wait_for_timeout(cfg["job"]["delay_ms"])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
