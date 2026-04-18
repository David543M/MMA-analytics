"""
Scrape official fight scorecards from mmadecisions.com and upsert into
the fight_scorecards Supabase table.

Usage:
    python jobs/scrape_scorecards.py              # scrape recent decisions (homepage)
    python jobs/scrape_scorecards.py --backfill   # scrape all events from decisions-by-event
    python jobs/scrape_scorecards.py --dry-run    # parse without writing to Supabase

Respects robots.txt, rate-limits to 1 request every 2 seconds, and uses an
identifiable User-Agent.

Environment variables:
    SUPABASE_URL         — project URL (or VITE_SUPABASE_URL)
    SUPABASE_SERVICE_KEY — service-role key (write access)
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup, Tag

try:
    from supabase import Client, create_client
except ImportError:  # pragma: no cover
    Client = None  # type: ignore
    create_client = None  # type: ignore

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://mmadecisions.com"
USER_AGENT = "WarRoom-ScorecardsBot/1.0 (+https://github.com/David543M/mma-grid)"
REQUEST_DELAY_S = 2.0
MAX_DECISIONS_DEFAULT = 30
MAX_DECISIONS_BACKFILL = 10_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def normalize_name_key(name: str) -> str:
    """Normalize a fighter or judge name for matching.

    Mirrors normalize_canonical_fighter_key() in SQL migrations: strip accents,
    lowercase, collapse whitespace, keep alphanumerics only.
    """
    nfkd = unicodedata.normalize("NFKD", name.replace("\xa0", " "))
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    lowered = ascii_only.lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", lowered).strip()
    return re.sub(r"\s+", " ", cleaned)


def get_supabase() -> "Client":
    url = os.environ.get("SUPABASE_URL") or os.environ.get("VITE_SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        log.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        sys.exit(1)
    if create_client is None:
        log.error("supabase package not installed")
        sys.exit(1)
    return create_client(url, key)


_ROBOTS_CACHE: Optional[RobotFileParser] = None


def check_robots(url: str) -> bool:
    """Return True if robots.txt allows crawling the given URL."""
    global _ROBOTS_CACHE
    if _ROBOTS_CACHE is None:
        rp = RobotFileParser()
        rp.set_url(f"{BASE_URL}/robots.txt")
        try:
            rp.read()
        except Exception:
            log.warning("Could not fetch robots.txt, proceeding cautiously")
            return True
        _ROBOTS_CACHE = rp
    return _ROBOTS_CACHE.can_fetch(USER_AGENT, url)


def fetch_page(session: requests.Session, url: str) -> Optional[BeautifulSoup]:
    """Fetch a page with rate limiting."""
    if not check_robots(url):
        log.warning("robots.txt disallows %s — skipping", url)
        return None
    time.sleep(REQUEST_DELAY_S)
    try:
        resp = session.get(url, timeout=20)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as exc:
        log.error("Failed to fetch %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class ScorecardRow:
    event_name: str
    event_date: str  # YYYY-MM-DD
    fighter_a_full: str
    fighter_b_full: str
    judge_name: str
    round_number: int
    fighter_a_score: int
    fighter_b_score: int
    source_url: str


# ---------------------------------------------------------------------------
# Decision page parsing
# ---------------------------------------------------------------------------


_TITLE_RE = re.compile(r"^(.+?)\s+def\.\s+(.+?)\s*::\s*(.+?)\s*::\s*MMA Decisions", re.I)


def _clean_text(node: Tag | str) -> str:
    if isinstance(node, Tag):
        txt = node.get_text(" ", strip=True)
    else:
        txt = str(node)
    return txt.replace("\xa0", " ").strip()


def _parse_event_date(soup: BeautifulSoup) -> str:
    """Find the event date like 'April 16, 2026' and return YYYY-MM-DD."""
    # Date sits in a TD with class decision-top2 right after the event link
    for td in soup.find_all("td", class_="decision-top2"):
        text = _clean_text(td)
        match = re.search(r"([A-Z][a-z]+ \d{1,2},\s*\d{4})", text)
        if match:
            try:
                dt = datetime.strptime(match.group(1).replace(",", ""), "%B %d %Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    return ""


def parse_decision_page(soup: BeautifulSoup, page_url: str) -> list[ScorecardRow]:
    """Parse a single mmadecisions decision page."""
    rows: list[ScorecardRow] = []

    # Extract fighters + event from title
    title_el = soup.find("title")
    if not title_el:
        return rows

    title_text = _clean_text(title_el)
    title_match = _TITLE_RE.match(title_text)
    if not title_match:
        log.debug("Title not in expected format: %s", title_text)
        return rows

    fighter_a_full = title_match.group(1).strip()
    fighter_b_full = title_match.group(2).strip()
    event_name = title_match.group(3).strip()
    event_date = _parse_event_date(soup)

    # Each judge scorecard is a table containing a td.judge (judge name) +
    # tr.decision rows for each round + tr.bottom-row with totals.
    for judge_td in soup.find_all("td", class_="judge"):
        # Find judge name via the <a href="judge/..."> link
        judge_link = judge_td.find("a", href=re.compile(r"^judge/"))
        if not judge_link:
            continue
        judge_name = _clean_text(judge_link)
        if not judge_name:
            continue

        # Walk up to the containing table
        table = judge_td.find_parent("table")
        if not table:
            continue

        # Parse the decision rows (round_number | fighter_a_score | fighter_b_score)
        for tr in table.find_all("tr", class_="decision"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            try:
                round_num = int(_clean_text(tds[0]))
                a_score = int(_clean_text(tds[1]))
                b_score = int(_clean_text(tds[2]))
            except (ValueError, IndexError):
                continue

            if not (1 <= round_num <= 5):
                continue
            if not (7 <= a_score <= 10 and 7 <= b_score <= 10):
                continue

            rows.append(
                ScorecardRow(
                    event_name=event_name,
                    event_date=event_date,
                    fighter_a_full=fighter_a_full,
                    fighter_b_full=fighter_b_full,
                    judge_name=judge_name,
                    round_number=round_num,
                    fighter_a_score=a_score,
                    fighter_b_score=b_score,
                    source_url=page_url,
                )
            )

    return rows


# ---------------------------------------------------------------------------
# Listing discovery
# ---------------------------------------------------------------------------


def _absolute(href: str) -> str:
    if href.startswith("http"):
        return href
    return f"{BASE_URL}/{href.lstrip('/')}"


def discover_recent_decisions(session: requests.Session, limit: int) -> list[str]:
    """Scrape the homepage for recent decision links."""
    soup = fetch_page(session, f"{BASE_URL}/")
    if not soup:
        return []

    links: list[str] = []
    for a in soup.find_all("a", href=re.compile(r"^decision/\d+/")):
        href = a.get("href")
        if isinstance(href, str):
            url = _absolute(href)
            if url not in links:
                links.append(url)
        if len(links) >= limit:
            break

    log.info("Homepage yielded %d decision links", len(links))
    return links


def discover_backfill_decisions(session: requests.Session, limit: int) -> list[str]:
    """Walk decisions-by-event to discover all events, then each event's decisions."""
    events_soup = fetch_page(session, f"{BASE_URL}/decisions-by-event/")
    if not events_soup:
        return []

    event_urls: list[str] = []
    for a in events_soup.find_all("a", href=re.compile(r"^event/\d+/")):
        href = a.get("href")
        if isinstance(href, str):
            url = _absolute(href)
            if url not in event_urls:
                event_urls.append(url)

    log.info("Found %d events for backfill", len(event_urls))

    decision_urls: list[str] = []
    for i, event_url in enumerate(event_urls, 1):
        event_soup = fetch_page(session, event_url)
        if not event_soup:
            continue
        for a in event_soup.find_all("a", href=re.compile(r"^decision/\d+/")):
            href = a.get("href")
            if isinstance(href, str):
                url = _absolute(href)
                if url not in decision_urls:
                    decision_urls.append(url)
        if i % 10 == 0:
            log.info("Walked %d/%d events, %d decisions so far", i, len(event_urls), len(decision_urls))
        if len(decision_urls) >= limit:
            break

    return decision_urls[:limit]


# ---------------------------------------------------------------------------
# Matching & upsert
# ---------------------------------------------------------------------------


class _BoutIndex:
    """In-memory index of event_bouts for fast name+date matching."""

    def __init__(self, supabase: "Client") -> None:
        self.by_pair_date: dict[tuple[str, str, str], str] = {}
        # Pull all event_bouts joined to their event date
        rows: list[dict] = []
        page_size = 1000
        offset = 0
        while True:
            page = (
                supabase.table("event_bouts")
                .select("id, fighter_a_name, fighter_b_name, events(date)")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            batch = page.data or []
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        for row in rows:
            event = row.get("events") or {}
            date = event.get("date", "") if isinstance(event, dict) else ""
            key_a = normalize_name_key(row.get("fighter_a_name") or "")
            key_b = normalize_name_key(row.get("fighter_b_name") or "")
            if not key_a or not key_b:
                continue
            pair = tuple(sorted([key_a, key_b]))
            self.by_pair_date[(pair[0], pair[1], date)] = row["id"]

        log.info("Loaded %d event_bouts into matching index", len(self.by_pair_date))

    def resolve(self, fighter_a: str, fighter_b: str, event_date: str) -> Optional[str]:
        key_a = normalize_name_key(fighter_a)
        key_b = normalize_name_key(fighter_b)
        pair = tuple(sorted([key_a, key_b]))
        # Try exact date first
        if event_date:
            match = self.by_pair_date.get((pair[0], pair[1], event_date))
            if match:
                return match
        # Fallback: any date (less safe, last resort)
        for (a, b, _d), bout_id in self.by_pair_date.items():
            if a == pair[0] and b == pair[1]:
                return bout_id
        return None


def upsert_scorecards(
    supabase: "Client", scorecards: list[ScorecardRow]
) -> tuple[int, int]:
    """Upsert into fight_scorecards. Returns (upserted_count, unmatched_bouts)."""
    index = _BoutIndex(supabase)

    upserted = 0
    unmatched: set[tuple[str, str, str]] = set()

    for sc in scorecards:
        bout_id = index.resolve(sc.fighter_a_full, sc.fighter_b_full, sc.event_date)
        if not bout_id:
            unmatched.add((sc.fighter_a_full, sc.fighter_b_full, sc.event_date))
            continue

        payload = {
            "event_bout_id": bout_id,
            "judge_name": sc.judge_name.strip(),
            "round_number": sc.round_number,
            "fighter_a_score": sc.fighter_a_score,
            "fighter_b_score": sc.fighter_b_score,
            "source_url": sc.source_url,
        }
        try:
            supabase.table("fight_scorecards").upsert(
                payload,
                on_conflict="event_bout_id,judge_name,round_number",
            ).execute()
            upserted += 1
        except Exception as exc:
            log.warning(
                "Upsert failed for %s R%d (%s): %s",
                sc.judge_name, sc.round_number, bout_id, exc,
            )

    return upserted, len(unmatched)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape MMA Decisions scorecards")
    parser.add_argument("--backfill", action="store_true", help="Walk all events")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to Supabase")
    parser.add_argument("--limit", type=int, default=None, help="Max decisions to scrape")
    args = parser.parse_args()

    limit = args.limit or (MAX_DECISIONS_BACKFILL if args.backfill else MAX_DECISIONS_DEFAULT)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    log.info(
        "Starting scorecard scrape (mode=%s, limit=%d, dry_run=%s)",
        "backfill" if args.backfill else "recent",
        limit,
        args.dry_run,
    )

    if args.backfill:
        decision_urls = discover_backfill_decisions(session, limit)
    else:
        decision_urls = discover_recent_decisions(session, limit)

    log.info("Parsing %d decision pages", len(decision_urls))

    all_scorecards: list[ScorecardRow] = []
    parse_failures = 0

    for i, url in enumerate(decision_urls, 1):
        soup = fetch_page(session, url)
        if soup is None:
            parse_failures += 1
            continue
        rows = parse_decision_page(soup, url)
        if not rows:
            parse_failures += 1
        else:
            all_scorecards.extend(rows)
        if i % 10 == 0:
            log.info("Progress: %d/%d pages, %d scorecards", i, len(decision_urls), len(all_scorecards))

    failure_rate = parse_failures / max(len(decision_urls), 1)
    log.info(
        "Parsing done: %d scorecards from %d pages (%.1f%% parse failures)",
        len(all_scorecards), len(decision_urls), failure_rate * 100,
    )

    if failure_rate > 0.20:
        log.warning("Parse failure rate %.1f%% exceeds 20%% — review parser", failure_rate * 100)

    if args.dry_run:
        log.info("Dry run — sample output:")
        for sc in all_scorecards[:12]:
            log.info(
                "  %s vs %s (%s) | %s R%d: %d-%d",
                sc.fighter_a_full, sc.fighter_b_full, sc.event_date or "?",
                sc.judge_name, sc.round_number,
                sc.fighter_a_score, sc.fighter_b_score,
            )
        return

    supabase = get_supabase()
    upserted, unmatched = upsert_scorecards(supabase, all_scorecards)
    log.info("Upserted %d rows (%d bouts unmatched in event_bouts)", upserted, unmatched)


if __name__ == "__main__":
    main()
