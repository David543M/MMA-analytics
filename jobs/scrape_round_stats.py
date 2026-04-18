"""
Scrape per-round fight statistics from UFCStats.com and upsert into the
fight_round_stats Supabase table.

Usage:
    python jobs/scrape_round_stats.py              # scrape recent events
    python jobs/scrape_round_stats.py --backfill   # scrape all completed events

Respects robots.txt, rate-limits to 1 request every 2 seconds, and uses an
identifiable User-Agent.

Environment variables:
    SUPABASE_URL         — project URL
    SUPABASE_KEY — service-role key (write access)
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Optional
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup, Tag
from supabase import Client, create_client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "http://ufcstats.com"
USER_AGENT = "WarRoom-RoundStatsBot/1.0 (+https://github.com/David543M/mma-grid)"
REQUEST_DELAY_S = 2.0
MAX_EVENTS_DEFAULT = 10
MAX_EVENTS_BACKFILL = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def normalize_name_key(name: str) -> str:
    """Normalize a fighter name for matching. Mirrors normalize_canonical_fighter_key()."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    lowered = ascii_only.lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", lowered).strip()
    return re.sub(r"\s+", " ", cleaned)


def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        log.error("SUPABASE_URL and SUPABASE_KEY must be set")
        sys.exit(1)
    return create_client(url, key)


def check_robots(url: str) -> bool:
    rp = RobotFileParser()
    rp.set_url(f"{BASE_URL}/robots.txt")
    try:
        rp.read()
    except Exception:
        return True
    return rp.can_fetch(USER_AGENT, url)


def fetch_page(session: requests.Session, url: str) -> Optional[BeautifulSoup]:
    if not check_robots(url):
        log.warning("robots.txt disallows %s — skipping", url)
        return None
    time.sleep(REQUEST_DELAY_S)
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as exc:
        log.error("Failed to fetch %s: %s", url, exc)
        return None


def parse_control_time(text: str) -> int:
    """Parse control time string like '4:23' into seconds."""
    text = text.strip()
    if not text or text == "--":
        return 0
    match = re.match(r"(\d+):(\d{2})", text)
    if match:
        return int(match.group(1)) * 60 + int(match.group(2))
    return 0


def parse_strike_pair(text: str) -> tuple[int, int]:
    """Parse '12 of 25' into (landed, attempted)."""
    text = text.strip()
    match = re.match(r"(\d+)\s+of\s+(\d+)", text)
    if match:
        return int(match.group(1)), int(match.group(2))
    return 0, 0


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class RoundStatRow:
    fighter_name: str
    opponent_name: str
    event_date: str
    round_number: int

    sig_str_head_landed: int = 0
    sig_str_head_attempted: int = 0
    sig_str_body_landed: int = 0
    sig_str_body_attempted: int = 0
    sig_str_leg_landed: int = 0
    sig_str_leg_attempted: int = 0

    sig_str_distance_landed: int = 0
    sig_str_distance_attempted: int = 0
    sig_str_clinch_landed: int = 0
    sig_str_clinch_attempted: int = 0
    sig_str_ground_landed: int = 0
    sig_str_ground_attempted: int = 0

    total_sig_str_landed: int = 0
    total_sig_str_attempted: int = 0

    takedowns_landed: int = 0
    takedowns_attempted: int = 0
    submission_attempts: int = 0
    reversals: int = 0
    control_time_seconds: int = 0
    knockdowns: int = 0

    source_url: str = ""


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def get_completed_event_links(session: requests.Session, max_events: int) -> list[str]:
    """Get links to completed event pages from the UFCStats events listing."""
    url = f"{BASE_URL}/statistics/events/completed?page=all"
    soup = fetch_page(session, url)
    if not soup:
        return []

    links: list[str] = []
    for a_tag in soup.find_all("a", href=True, class_="b-link"):
        href = a_tag["href"]
        if isinstance(href, str) and "/event-details/" in href:
            if href not in links:
                links.append(href)
            if len(links) >= max_events:
                break

    log.info("Found %d completed event links", len(links))
    return links


def get_fight_links_from_event(session: requests.Session, event_url: str) -> list[str]:
    """Get individual fight detail links from an event page."""
    soup = fetch_page(session, event_url)
    if not soup:
        return []

    links: list[str] = []
    # UFCStats event pages expose fight links via <tr data-link="..."> and onclick,
    # not standard <a href>. Check both.
    for tr in soup.find_all("tr", attrs={"data-link": True}):
        href = tr.get("data-link", "")
        if isinstance(href, str) and "/fight-details/" in href and href not in links:
            links.append(href)
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if isinstance(href, str) and "/fight-details/" in href and href not in links:
            links.append(href)

    return links


def _extract_cell_pair(td: Tag) -> tuple[str, str]:
    """Each per-round <td> in UFCStats contains two <p> elements — one per fighter.

    Returns (fighter_a_text, fighter_b_text). Falls back to stripped full text if
    there aren't exactly two <p> children.
    """
    ps = td.find_all("p")
    if len(ps) >= 2:
        return ps[0].get_text(strip=True), ps[1].get_text(strip=True)
    text = td.get_text(strip=True)
    return text, text


def _parse_per_round_table(table: Tag) -> dict[int, list[tuple[str, str]]]:
    """Parse a UFCStats per-round table.

    Structure: <tbody> contains alternating <thead> (round header with colspan)
    and <tr> (single row per round where each <td> has two stacked <p>s).

    Returns { round_number: [(a_cell, b_cell), ...columns...] }.
    """
    out: dict[int, list[tuple[str, str]]] = {}
    tbody = table.find("tbody")
    if not tbody or not isinstance(tbody, Tag):
        return out

    current_round: Optional[int] = None
    for child in tbody.children:
        if not isinstance(child, Tag):
            continue
        # Round header markers are <thead class="...table-row_type_head">
        if child.name == "thead":
            header_text = child.get_text(" ", strip=True)
            m = re.search(r"Round\s+(\d+)", header_text, re.IGNORECASE)
            if m:
                current_round = int(m.group(1))
            continue
        if child.name == "tr" and current_round is not None:
            tds = child.find_all("td")
            if not tds:
                continue
            cells = [_extract_cell_pair(td) for td in tds]
            out[current_round] = cells
            # A round should appear only once per table
            current_round = None
    return out


def parse_fight_detail_page(
    soup: BeautifulSoup, page_url: str, event_date: str
) -> list[RoundStatRow]:
    """Parse a UFCStats fight detail page for per-round statistics.

    UFCStats renders two per-round tables (Totals, then Significant Strikes).
    Each round row has one <tr> with two <p>s per <td> (fighter A on top, B below).
    """
    rows: list[RoundStatRow] = []

    # Extract fighter names from the page header
    fighter_names: list[str] = []
    for name_el in soup.find_all("a", class_="b-fight-details__person-link"):
        name = name_el.get_text(strip=True)
        if name and name not in fighter_names:
            fighter_names.append(name)

    if len(fighter_names) < 2:
        log.debug("Could not find 2 fighter names on %s", page_url)
        return rows

    fighter_a, fighter_b = fighter_names[0], fighter_names[1]

    # Locate the per-round tables. They sit inside sections whose collapse link
    # says "Per round". Each section holds one table.
    per_round_tables: list[Tag] = []
    for section in soup.find_all("section", class_="b-fight-details__section"):
        if not isinstance(section, Tag):
            continue
        marker = section.find("i", class_="b-fight-details__collapse-left")
        if not marker or "per round" not in marker.get_text(strip=True).lower():
            continue
        table = section.find("table")
        if isinstance(table, Tag):
            per_round_tables.append(table)

    if len(per_round_tables) < 2:
        # Fallback: scan every table and classify by header content
        for table in soup.find_all("table"):
            if not isinstance(table, Tag):
                continue
            tbody = table.find("tbody")
            if not tbody:
                continue
            if tbody.find("thead", string=re.compile(r"Round\s+\d+", re.IGNORECASE)) or \
               any("Round" in (t.get_text(" ", strip=True) or "")
                   for t in tbody.find_all("thead")):
                if table not in per_round_tables:
                    per_round_tables.append(table)

    if len(per_round_tables) < 2:
        log.debug("Did not find 2 per-round tables on %s (found %d)", page_url, len(per_round_tables))
        return rows

    totals_table = _parse_per_round_table(per_round_tables[0])
    sig_table = _parse_per_round_table(per_round_tables[1])

    all_rounds = sorted(set(totals_table.keys()) | set(sig_table.keys()))

    for round_num in all_rounds:
        totals_cells = totals_table.get(round_num, [])
        sig_cells = sig_table.get(round_num, [])

        for fighter_idx, fighter_name, opponent_name in [
            (0, fighter_a, fighter_b),
            (1, fighter_b, fighter_a),
        ]:
            row = RoundStatRow(
                fighter_name=fighter_name,
                opponent_name=opponent_name,
                event_date=event_date,
                round_number=round_num,
                source_url=page_url,
            )

            # Totals columns: Fighter | KD | Sig. str. | Sig. str. % | Total str. | Td | Td % | Sub. att | Rev. | Ctrl
            if len(totals_cells) >= 10:
                def tv(col: int) -> str:
                    return totals_cells[col][fighter_idx]

                kd_raw = tv(1)
                try:
                    row.knockdowns = int(kd_raw) if kd_raw.isdigit() else 0
                except ValueError:
                    pass
                row.total_sig_str_landed, row.total_sig_str_attempted = parse_strike_pair(tv(2))
                row.takedowns_landed, row.takedowns_attempted = parse_strike_pair(tv(5))
                sub_raw = tv(7)
                try:
                    row.submission_attempts = int(sub_raw) if sub_raw.isdigit() else 0
                except ValueError:
                    pass
                rev_raw = tv(8)
                try:
                    row.reversals = int(rev_raw) if rev_raw.isdigit() else 0
                except ValueError:
                    pass
                row.control_time_seconds = parse_control_time(tv(9))

            # Sig strikes columns: Fighter | Sig. str | Sig. str. % | Head | Body | Leg | Distance | Clinch | Ground
            if len(sig_cells) >= 9:
                def sv(col: int) -> str:
                    return sig_cells[col][fighter_idx]

                row.sig_str_head_landed, row.sig_str_head_attempted = parse_strike_pair(sv(3))
                row.sig_str_body_landed, row.sig_str_body_attempted = parse_strike_pair(sv(4))
                row.sig_str_leg_landed, row.sig_str_leg_attempted = parse_strike_pair(sv(5))
                row.sig_str_distance_landed, row.sig_str_distance_attempted = parse_strike_pair(sv(6))
                row.sig_str_clinch_landed, row.sig_str_clinch_attempted = parse_strike_pair(sv(7))
                row.sig_str_ground_landed, row.sig_str_ground_attempted = parse_strike_pair(sv(8))

            rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Matching & Upsert
# ---------------------------------------------------------------------------


def _paginate(query_fn, page_size: int = 1000) -> list[dict]:
    """Paginate a PostgREST query using .range() until all rows fetched.

    query_fn: callable taking (offset, limit) returning an APIResponse.
    """
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


def build_matching_indexes(supabase: Client) -> tuple[dict[str, str], dict[tuple[str, str, str], str]]:
    """Load ALL fighters and event_bouts once, return lookup indexes.

    Returns:
      fighter_by_key: normalized_name -> fighter_id
      bout_by_pair_date: (sorted_name_a_key, sorted_name_b_key, event_date) -> event_bout_id
    """
    fighter_rows = _paginate(
        lambda off, end: supabase.table("fighters").select("id, name").range(off, end).execute()
    )
    fighter_by_key: dict[str, str] = {}
    for row in fighter_rows:
        key = normalize_name_key(row.get("name") or "")
        if key and key not in fighter_by_key:
            fighter_by_key[key] = row["id"]
    log.info("Loaded %d fighters into matching index", len(fighter_by_key))

    bout_rows = _paginate(
        lambda off, end: supabase.table("event_bouts")
        .select("id, fighter_a_name, fighter_b_name, events(date)")
        .range(off, end)
        .execute()
    )
    bout_by_pair_date: dict[tuple[str, str, str], str] = {}
    for row in bout_rows:
        event = row.get("events") or {}
        date = event.get("date", "") if isinstance(event, dict) else ""
        key_a = normalize_name_key(row.get("fighter_a_name") or "")
        key_b = normalize_name_key(row.get("fighter_b_name") or "")
        if not key_a or not key_b:
            continue
        pair = tuple(sorted([key_a, key_b]))
        bout_by_pair_date[(pair[0], pair[1], date)] = row["id"]
    log.info("Loaded %d event_bouts into matching index", len(bout_by_pair_date))

    return fighter_by_key, bout_by_pair_date


def upsert_round_stats(supabase: Client, stats: list[RoundStatRow]) -> int:
    """Upsert round stat rows into fight_round_stats."""
    upserted = 0
    unmatched_bouts = 0
    unmatched_fighters = 0

    fighter_by_key, bout_by_pair_date = build_matching_indexes(supabase)

    for row in stats:
        key_fighter = normalize_name_key(row.fighter_name)
        key_opponent = normalize_name_key(row.opponent_name)
        pair = tuple(sorted([key_fighter, key_opponent]))

        event_bout_id = bout_by_pair_date.get((pair[0], pair[1], row.event_date))
        fighter_id = fighter_by_key.get(key_fighter)

        if not event_bout_id:
            unmatched_bouts += 1
        if not fighter_id:
            unmatched_fighters += 1

        if not event_bout_id or not fighter_id:
            log.debug(
                "No match for %s vs %s (%s) — bout_id=%s, fighter_id=%s",
                row.fighter_name, row.opponent_name, row.event_date,
                event_bout_id, fighter_id,
            )
            continue

        payload = {
            "event_bout_id": event_bout_id,
            "round_number": row.round_number,
            "fighter_id": fighter_id,
            "sig_str_head_landed": row.sig_str_head_landed,
            "sig_str_head_attempted": row.sig_str_head_attempted,
            "sig_str_body_landed": row.sig_str_body_landed,
            "sig_str_body_attempted": row.sig_str_body_attempted,
            "sig_str_leg_landed": row.sig_str_leg_landed,
            "sig_str_leg_attempted": row.sig_str_leg_attempted,
            "sig_str_distance_landed": row.sig_str_distance_landed,
            "sig_str_distance_attempted": row.sig_str_distance_attempted,
            "sig_str_clinch_landed": row.sig_str_clinch_landed,
            "sig_str_clinch_attempted": row.sig_str_clinch_attempted,
            "sig_str_ground_landed": row.sig_str_ground_landed,
            "sig_str_ground_attempted": row.sig_str_ground_attempted,
            "total_sig_str_landed": row.total_sig_str_landed,
            "total_sig_str_attempted": row.total_sig_str_attempted,
            "takedowns_landed": row.takedowns_landed,
            "takedowns_attempted": row.takedowns_attempted,
            "submission_attempts": row.submission_attempts,
            "reversals": row.reversals,
            "control_time_seconds": row.control_time_seconds,
            "knockdowns": row.knockdowns,
            "source_url": row.source_url,
        }

        try:
            supabase.table("fight_round_stats").upsert(
                payload,
                on_conflict="event_bout_id,round_number,fighter_id",
            ).execute()
            upserted += 1
        except Exception as exc:
            log.warning("Upsert failed for %s R%d: %s", row.fighter_name, row.round_number, exc)

    log.info(
        "Matching: %d unmatched bouts, %d unmatched fighters out of %d total rows",
        unmatched_bouts, unmatched_fighters, len(stats),
    )
    return upserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape UFCStats per-round statistics")
    parser.add_argument("--backfill", action="store_true", help="Scrape all completed events")
    parser.add_argument("--dry-run", action="store_true", help="Parse but do not write to Supabase")
    args = parser.parse_args()

    max_events = MAX_EVENTS_BACKFILL if args.backfill else MAX_EVENTS_DEFAULT

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    log.info("Starting round stats scrape (max_events=%d, dry_run=%s)", max_events, args.dry_run)

    event_urls = get_completed_event_links(session, max_events)

    all_stats: list[RoundStatRow] = []
    event_failures = 0

    for i, event_url in enumerate(event_urls, 1):
        # Extract event date from event page
        event_soup = fetch_page(session, event_url)
        if not event_soup:
            event_failures += 1
            continue

        event_date = ""
        date_el = event_soup.find("li", class_="b-list__box-list-item")
        if date_el:
            date_text = date_el.get_text(strip=True)
            date_match = re.search(
                r"(\w+ \d{1,2}, \d{4})", date_text
            )
            if date_match:
                from datetime import datetime
                try:
                    dt = datetime.strptime(date_match.group(1), "%B %d, %Y")
                    event_date = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        fight_urls = get_fight_links_from_event(session, event_url)

        for fight_url in fight_urls:
            fight_soup = fetch_page(session, fight_url)
            if not fight_soup:
                continue
            rows = parse_fight_detail_page(fight_soup, fight_url, event_date)
            all_stats.extend(rows)

        if i % 5 == 0:
            log.info("Progress: %d/%d events, %d round stat rows", i, len(event_urls), len(all_stats))

    log.info(
        "Parsing complete: %d round stat rows from %d events (%d event failures)",
        len(all_stats), len(event_urls), event_failures,
    )

    if args.dry_run:
        log.info("Dry run — skipping Supabase upsert")
        for row in all_stats[:10]:
            log.info(
                "  %s R%d: %d/%d sig.str, %d/%d TD, %ds ctrl",
                row.fighter_name, row.round_number,
                row.total_sig_str_landed, row.total_sig_str_attempted,
                row.takedowns_landed, row.takedowns_attempted,
                row.control_time_seconds,
            )
        return

    supabase = get_supabase()
    upserted = upsert_round_stats(supabase, all_stats)
    log.info("Upserted %d round stat rows into fight_round_stats", upserted)


if __name__ == "__main__":
    main()
