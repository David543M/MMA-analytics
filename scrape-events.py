import os
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from supabase import create_client


SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("SUPABASE_KEY")
    or ""
)

EVENT_INDEX_SOURCES = [
    {"status": "completed", "url": "http://ufcstats.com/statistics/events/completed?page=all"},
    {"status": "scheduled", "url": "http://ufcstats.com/statistics/events/upcoming?page=all"},
]

WEIGHT_CLASS_HINTS = [
    "women's strawweight",
    "women's flyweight",
    "women's bantamweight",
    "women's featherweight",
    "light heavyweight",
    "heavyweight",
    "flyweight",
    "bantamweight",
    "featherweight",
    "lightweight",
    "welterweight",
    "middleweight",
    "strawweight",
]


def normalize_whitespace(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_key(value):
    return normalize_whitespace(value).lower()


def slugify(value):
    text = normalize_whitespace(value).lower()
    text = re.sub(r"[\"']", "", text)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def is_date_like(value):
    value = normalize_whitespace(value)
    return bool(
        re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{1,2},\s+\d{4}", value, re.I)
        or re.search(r"\d{4}-\d{2}-\d{2}", value)
    )


def parse_date(value):
    text = normalize_whitespace(value).replace(".", "")
    if not text:
        return None

    iso_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", text)
    if iso_match:
        return iso_match.group(1)

    month_map = {
        "jan": "01", "january": "01",
        "feb": "02", "february": "02",
        "mar": "03", "march": "03",
        "apr": "04", "april": "04",
        "may": "05",
        "jun": "06", "june": "06",
        "jul": "07", "july": "07",
        "aug": "08", "august": "08",
        "sep": "09", "sept": "09", "september": "09",
        "oct": "10", "october": "10",
        "nov": "11", "november": "11",
        "dec": "12", "december": "12",
    }

    match = re.search(r"\b([A-Za-z]{3,9})\s+(\d{1,2}),\s+(\d{4})\b", text)
    if not match:
        return None

    month = month_map.get(match.group(1).lower())
    if not month:
        return None

    day = match.group(2).zfill(2)
    year = match.group(3)
    return f"{year}-{month}-{day}"


def parse_location(value):
    text = normalize_whitespace(value)
    if not text:
        return {"city": "", "country": ""}

    parts = [normalize_whitespace(part) for part in text.split(",") if normalize_whitespace(part)]
    if len(parts) >= 2:
        return {"city": parts[0], "country": ", ".join(parts[1:])}

    return {"city": text, "country": ""}


def fetch_soup(url):
    response = requests.get(
        url,
        timeout=30,
        headers={
            "User-Agent": "Mozilla/5.0 MMA Grid scraper",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def extract_event_title(soup):
    for selector in [".b-content__title-highlight", "h1", "h2", "h3"]:
        element = soup.select_one(selector)
        text = normalize_whitespace(element.get_text(" ", strip=True)) if element else ""
        if text:
            return text
    return ""


def extract_event_metadata(soup):
    raw_text = soup.get_text("\n", strip=False)
    text = normalize_whitespace(raw_text)
    lines = [normalize_whitespace(line) for line in raw_text.splitlines() if normalize_whitespace(line)]

    date_value = parse_date(text)
    venue = ""
    location = ""

    venue_match = re.search(r"Venue:\s*(.+?)(?=Location:|Date:|$)", text, re.I)
    if venue_match:
        venue = normalize_whitespace(venue_match.group(1))

    location_match = re.search(r"Location:\s*(.+?)(?=Venue:|Date:|$)", text, re.I)
    if location_match:
        location = normalize_whitespace(location_match.group(1))

    if not location:
        candidate = next((line for line in lines if "," in line and not is_date_like(line) and len(line) < 80), "")
        location = candidate

    parsed_location = parse_location(location)

    return {
        "date": date_value,
        "venue": venue,
        "city": parsed_location["city"],
        "country": parsed_location["country"],
    }


def extract_event_links(soup):
    rows = []
    seen = set()

    for anchor in soup.select('a[href*="event-details"]'):
        href = anchor.get("href", "").strip()
        if not href or href in seen:
            continue
        seen.add(href)

        row = anchor.find_parent("tr")
        cells = row.find_all("td") if row else []
        row_texts = [normalize_whitespace(cell.get_text(" ", strip=True)) for cell in cells]
        row_texts = [text for text in row_texts if text]

        name = normalize_whitespace(anchor.get_text(" ", strip=True)) or (row_texts[0] if row_texts else href)
        date_text = next((value for value in row_texts if is_date_like(value)), "")
        location_text = next((value for value in row_texts if "," in value and not is_date_like(value) and value != name), "")

        rows.append({
            "href": href,
            "name": name,
            "date": parse_date(date_text),
            "location": location_text,
        })

    return rows


def extract_weight_class(text):
    lower = normalize_whitespace(text).lower()
    for hint in WEIGHT_CLASS_HINTS:
        if hint in lower:
            return " ".join(part.capitalize() for part in hint.split(" "))
    return ""


def find_section_label(text):
    lower = normalize_whitespace(text).lower()
    if "main card" in lower:
        return "main"
    if "early prelims" in lower:
        return "early_prelims"
    if "prelims" in lower:
        return "prelims"
    return None


def pick_fight_detail_link(row):
    anchor = row.select_one('a[href*="fight-details"]')
    return anchor.get("href", "").strip() if anchor else None


def extract_bout_fighters(row):
    names = []
    for anchor in row.select('a[href*="fighter-details"]'):
        name = normalize_whitespace(anchor.get_text(" ", strip=True))
        if name and name not in names:
            names.append(name)

    if len(names) >= 2:
        return names[:2]

    cells = row.find_all("td")
    fighter_like = []
    for cell in cells:
        text = normalize_whitespace(cell.get_text(" ", strip=True))
        if re.search(r"[A-Za-z]", text) and len(text) > 2:
            fighter_like.append(text)

    if len(fighter_like) >= 2:
        return fighter_like[:2]

    return []


def load_fighter_index(supabase):
    result = supabase.table("fighters").select("id, name").execute()
    rows = result.data or []

    exact = {}
    normalized = {}

    for row in rows:
        key = normalize_key(row["name"])
        exact.setdefault(key, row["id"])
        normalized.setdefault(key, row["id"])

    return {"exact": exact, "normalized": normalized}


def resolve_fighter_id(name, fighter_index):
    key = normalize_key(name)
    return fighter_index["exact"].get(key) or fighter_index["normalized"].get(key)


def scrape_fight_detail(fight_url):
    soup = fetch_soup(fight_url)
    rows = soup.find_all("tr", class_="b-fight-details__table-row")

    participants = []
    method = ""
    round_value = None
    time_value = ""
    winner_name = None

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 10:
            continue

        result = normalize_whitespace(cols[0].get_text(" ", strip=True)).lower()

        name_cell = cols[1]
        name_tags = name_cell.find_all("p")
        names = [normalize_whitespace(tag.get_text(" ", strip=True)) for tag in name_tags if normalize_whitespace(tag.get_text(" ", strip=True))]
        name = names[1] if len(names) > 1 else (names[0] if names else normalize_whitespace(name_cell.get_text(" ", strip=True)))

        row_method = normalize_whitespace(cols[7].get_text(" ", strip=True))
        row_round_raw = normalize_whitespace(cols[8].get_text(" ", strip=True))
        row_time = normalize_whitespace(cols[9].get_text(" ", strip=True)) or "0:00"

        try:
            parsed_round = int(row_round_raw)
        except Exception:
            parsed_round = None

        participants.append({"result": result, "name": name})

        if not method and row_method and row_method != "--":
            method = row_method
        if round_value is None and parsed_round is not None:
            round_value = parsed_round
        if not time_value and row_time and row_time != "--":
            time_value = row_time
        if not winner_name and "win" in result:
            winner_name = name

    return {
        "participants": participants,
        "winner_name": winner_name,
        "method": method or "N/A",
        "round": round_value,
        "time": time_value or "0:00",
    }


def upsert_event(supabase, payload):
    result = (
        supabase.table("events")
        .upsert(payload, on_conflict="slug")
        .execute()
    )
    if result.data:
        return result.data[0]

    existing = supabase.table("events").select("id, slug").eq("slug", payload["slug"]).execute()
    if existing.data:
        return existing.data[0]

    raise Exception(f"Unable to upsert event {payload['name']}")


def upsert_event_bouts(supabase, event_id, bouts):
    if not bouts:
        return

    payload = []
    for bout in bouts:
        row = dict(bout)
        row["event_id"] = event_id
        payload.append(row)

    supabase.table("event_bouts").upsert(payload, on_conflict="event_id,bout_order").execute()


def scrape_event_page(supabase, fighter_index, source):
    soup = fetch_soup(source["url"])
    event_links = extract_event_links(soup)

    print(f"\n[{source['status']}] {source['url']}")
    print(f"Found {len(event_links)} events")

    for event_link in event_links:
        try:
            detail_soup = fetch_soup(event_link["href"])
            event_name = extract_event_title(detail_soup) or event_link["name"]
            meta = extract_event_metadata(detail_soup)

            event_date = event_link["date"] or meta["date"]
            if not event_date:
                print(f"  skipping {event_name}: missing event date")
                continue

            slug = f"{slugify(event_name)}-{event_date}"

            event_payload = {
                "slug": slug,
                "name": event_name,
                "date": event_date,
                "status": source["status"],
                "venue": meta["venue"] or "",
                "city": meta["city"] or "",
                "country": meta["country"] or "",
                "source_url": event_link["href"],
            }

            saved_event = upsert_event(supabase, event_payload)

            bouts = []
            bout_order = 1
            current_segment = "main"

            for row in detail_soup.find_all("tr"):
                row_text = normalize_whitespace(row.get_text(" ", strip=True))
                if not row_text:
                    continue

                section = find_section_label(row_text)
                if section:
                    current_segment = section
                    continue

                fighters = extract_bout_fighters(row)
                if len(fighters) < 2:
                    continue

                fighter_a_name, fighter_b_name = fighters[0], fighters[1]
                fight_url = pick_fight_detail_link(row)
                fight_detail = None

                if fight_url:
                    try:
                        fight_detail = scrape_fight_detail(fight_url)
                    except Exception:
                        fight_detail = None

                winner_name = None
                if fight_detail:
                    winner_name = fight_detail["winner_name"]
                    if not winner_name:
                        win_row = next((p for p in fight_detail["participants"] if "win" in p["result"]), None)
                        winner_name = win_row["name"] if win_row else None

                fighter_a_id = resolve_fighter_id(fighter_a_name, fighter_index)
                fighter_b_id = resolve_fighter_id(fighter_b_name, fighter_index)

                winner_corner = None
                if winner_name:
                    if normalize_key(winner_name) == normalize_key(fighter_a_name):
                        winner_corner = "A"
                    elif normalize_key(winner_name) == normalize_key(fighter_b_name):
                        winner_corner = "B"

                bouts.append({
                    "bout_order": bout_order,
                    "card_segment": current_segment,
                    "fighter_a_id": fighter_a_id,
                    "fighter_b_id": fighter_b_id,
                    "fighter_a_name": fighter_a_name,
                    "fighter_b_name": fighter_b_name,
                    "weight_class": extract_weight_class(row_text),
                    "is_title_fight": bool(re.search(r"title", row_text, re.I)),
                    "status": "completed" if source["status"] == "completed" else "scheduled",
                    "winner_corner": winner_corner,
                    "method": fight_detail["method"] if fight_detail else "",
                    "round": fight_detail["round"] if fight_detail else None,
                    "time": fight_detail["time"] if fight_detail else "",
                })

                bout_order += 1

            upsert_event_bouts(supabase, saved_event["id"], bouts)
            print(f"  upserted {event_name} ({len(bouts)} bouts)")
            time.sleep(0.15)

        except Exception as error:
            print(f"  failed {event_link['href']}")
            print(error)


def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise Exception("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY/SUPABASE_KEY")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    fighter_index = load_fighter_index(supabase)

    print(f"Loaded fighter index: {len(fighter_index['exact'])} fighters")

    for source in EVENT_INDEX_SOURCES:
        scrape_event_page(supabase, fighter_index, source)


if __name__ == "__main__":
    main()
