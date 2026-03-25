from __future__ import annotations

import asyncio
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import yaml
from playwright.async_api import async_playwright
from supabase import create_client


@dataclass
class FighterSeed:
    fighter_id: str
    name: str
    slug: str | None = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(name: str) -> str:
    value = name.strip().lower()
    value = re.sub(r"[^a-z0-9\\s-]", "", value)
    value = re.sub(r"\\s+", "-", value)
    value = re.sub(r"-+", "-", value)
    return value.strip("-")


def load_config(path: str = "ufc_athlete_profiles.yaml") -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def parse_number(text: str | None) -> float | None:
    if not text:
        return None
    m = re.search(r"([0-9]+(?:\\.[0-9]+)?)", text)
    return float(m.group(1)) if m else None


def parse_int(text: str | None) -> int | None:
    val = parse_number(text)
    return int(val) if val is not None else None


def normalize_text(text: str | None) -> str | None:
    if text is None:
        return None
    text = re.sub(r"\\s+", " ", text).strip()
    return text or None


async def extract_info_pairs(page) -> dict[str, str]:
    pairs = {}
    dts = await page.locator("dt").all()
    for dt in dts:
        key = normalize_text(await dt.inner_text())
        if not key:
            continue
        dd = dt.locator("xpath=following-sibling::dd[1]")
        if await dd.count():
            value = normalize_text(await dd.first.inner_text())
            if value:
                pairs[key] = value
    return pairs


async def extract_body_text(page) -> str:
    body = page.locator("body")
    return normalize_text(await body.inner_text()) or ""


def find_first_matching_value(text: str, labels: list[str]) -> str | None:
    for label in labels:
        pattern = rf"{re.escape(label)}\\s*[:\\-]?\\s*(.+)"
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            return normalize_text(m.group(1))
    return None


def extract_fact_bullets(body_text: str, mapping: dict[str, list[str]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, aliases in mapping.items():
        raw = find_first_matching_value(body_text, aliases)
        if raw is None:
            result[key] = None
            continue
        if key in {"pro_since", "wins_by_ko", "wins_by_submission", "first_round_finishes"}:
            result[key] = parse_int(raw)
        else:
            result[key] = raw
    return result


def extract_stats_records(body_text: str) -> dict[str, Any]:
    patterns = {
        "sig_strikes_landed": r"Sig\\. Strikes Landed\\s*([0-9]+)",
        "sig_strikes_attempted": r"Sig\\. Strikes Attempted\\s*([0-9]+)",
        "takedowns_landed": r"Takedowns Landed\\s*([0-9]+)",
        "takedowns_attempted": r"Takedowns Attempted\\s*([0-9]+)",
        "slpm": r"([0-9]+(?:\\.[0-9]+)?)\\s*Sig\\. Str\\. Landed\\s*Per Min",
        "sapm": r"([0-9]+(?:\\.[0-9]+)?)\\s*Sig\\. Str\\. Absorbed\\s*Per Min",
        "td_avg": r"([0-9]+(?:\\.[0-9]+)?)\\s*Takedown avg\\s*Per 15 Min",
        "sub_avg": r"([0-9]+(?:\\.[0-9]+)?)\\s*Submission avg\\s*Per 15 Min",
    }
    out = {}
    for key, pattern in patterns.items():
        m = re.search(pattern, body_text, flags=re.IGNORECASE)
        out[key] = float(m.group(1)) if m else None
    return out


def extract_qa(body_text: str, questions: list[str]) -> list[dict[str, str]]:
    rows = []
    for q in questions:
        pattern = rf"{re.escape(q)}\\s*(.+?)(?=(?:[A-Z][^\\n]{{3,}}\\?)|$)"
        m = re.search(pattern, body_text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            answer = normalize_text(m.group(1))
            if answer:
                rows.append({"question": q, "answer": answer})
    return rows


def extract_fight_history(body_text: str) -> list[dict[str, Any]]:
    rows = []
    pattern = re.compile(
        r"(UFC[^\\n]*?)\\s*\\(([^\\)]+)\\)\\s*(.+?)(?=(?:UFC[^\\n]*?\\()|$)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for m in pattern.finditer(body_text):
        event_name = normalize_text(m.group(1))
        event_date = normalize_text(m.group(2))
        summary = normalize_text(m.group(3))
        if event_name and summary:
            rows.append({
                "event_name": event_name,
                "event_date": event_date,
                "summary": summary,
            })
    return rows


async def scrape_fighter(page, fighter: FighterSeed, cfg: dict[str, Any]) -> dict[str, Any]:
    slug = fighter.slug or slugify(fighter.name)
    url = cfg["source"]["profile_url_template"].format(slug=slug)

    await page.goto(url, wait_until="networkidle", timeout=cfg["job"]["timeout_ms"])

    info_pairs = await extract_info_pairs(page)
    body_text = await extract_body_text(page)

    profile = {
        "fighter_id": fighter.fighter_id,
        "source_url": url,
        "status": info_pairs.get("Status"),
        "place_of_birth": info_pairs.get("Place of Birth"),
        "trains_at": info_pairs.get("Trains at") or info_pairs.get("Training"),
        "fighting_style": info_pairs.get("Fighting style"),
        "age": parse_int(info_pairs.get("Age")),
        "height": parse_number(info_pairs.get("Height")),
        "weight": parse_number(info_pairs.get("Weight")),
        "octagon_debut": info_pairs.get("Octagon Debut"),
        "reach": parse_number(info_pairs.get("Reach")),
        "leg_reach": parse_number(info_pairs.get("Leg reach") or info_pairs.get("Leg Reach")),
        "updated_at": now_iso(),
    }

    facts = {
        "fighter_id": fighter.fighter_id,
        **extract_fact_bullets(body_text, cfg["mapping"]["derived_bullets"]),
        "updated_at": now_iso(),
    }

    stats = {
        "fighter_id": fighter.fighter_id,
        **extract_stats_records(body_text),
        "updated_at": now_iso(),
    }

    qa_rows = [
        {
            "fighter_id": fighter.fighter_id,
            "question": row["question"],
            "answer": row["answer"],
            "updated_at": now_iso(),
        }
        for row in extract_qa(body_text, cfg["selectors"]["qa_question_candidates"])
    ]

    fight_rows = [
        {
            "fighter_id": fighter.fighter_id,
            "event_name": row["event_name"],
            "event_date": row["event_date"],
            "summary": row["summary"],
            "updated_at": now_iso(),
        }
        for row in extract_fight_history(body_text)
    ]

    return {
        "profile": profile,
        "facts": facts,
        "stats": stats,
        "qa_rows": qa_rows,
        "fight_rows": fight_rows,
    }


def get_supabase(cfg: dict[str, Any]):
    url = os.environ[cfg["supabase"]["url_env"]]
    key = os.environ[cfg["supabase"]["service_role_key_env"]]
    return create_client(url, key)


def fetch_fighters_to_process(sb, limit: int) -> list[FighterSeed]:
    resp = sb.table("fighters").select("id,name").limit(limit).execute()
    return [FighterSeed(fighter_id=row["id"], name=row["name"]) for row in (resp.data or [])]


def upsert_all(sb, cfg: dict[str, Any], payload: dict[str, Any]) -> None:
    sb.table(cfg["targets"]["profile_table"]).upsert(payload["profile"]).execute()
    sb.table(cfg["targets"]["facts_table"]).upsert(payload["facts"]).execute()
    sb.table(cfg["targets"]["stats_table"]).upsert(payload["stats"]).execute()

    if payload["qa_rows"]:
        sb.table(cfg["targets"]["qa_table"]).upsert(payload["qa_rows"]).execute()

    if payload["fight_rows"]:
        sb.table(cfg["targets"]["fights_table"]).upsert(payload["fight_rows"]).execute()


async def main():
    ccfg = load_config("config/ufc_athlete_profiles.yaml")
    sb = get_supabase(cfg)
    fighters = fetch_fighters_to_process(sb, cfg["job"]["batch_size"])

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=cfg["job"]["headless"])
        page = await browser.new_page()

        for fighter in fighters:
            try:
                payload = await scrape_fighter(page, fighter, cfg)
                upsert_all(sb, cfg, payload)
                print(f"OK {fighter.name}")
            except Exception as exc:
                print(f"FAIL {fighter.name}: {exc}")
            await page.wait_for_timeout(cfg["job"]["delay_ms"])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

