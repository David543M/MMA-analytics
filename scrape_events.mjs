#!/usr/bin/env node

import { createClient } from "@supabase/supabase-js";
import { JSDOM } from "jsdom";

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL || "";
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_KEY ||
  process.env.VITE_SUPABASE_PUBLISHABLE_KEY ||
  "";

const EVENT_INDEX_SOURCES = [
  { status: "completed", url: "http://ufcstats.com/statistics/events/completed?page=all" },
  { status: "scheduled", url: "http://ufcstats.com/statistics/events/upcoming?page=all" },
];

const WEIGHT_CLASS_HINTS = [
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
];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function normalizeWhitespace(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function normalizeKey(value) {
  return normalizeWhitespace(value).toLowerCase();
}

function slugify(value) {
  return normalizeWhitespace(value)
    .toLowerCase()
    .normalize("NFKD")
    .replace(/['"]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function isDateLike(value) {
  return /\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{1,2},\s+\d{4}\b/i.test(value)
    || /\b\d{4}-\d{2}-\d{2}\b/.test(value);
}

function parseDate(value) {
  const text = normalizeWhitespace(value).replace(/\./g, "");
  if (!text) return null;

  const isoMatch = text.match(/\b(\d{4}-\d{2}-\d{2})\b/);
  if (isoMatch) return isoMatch[1];

  const match = text.match(/\b([A-Za-z]{3,9})\s+(\d{1,2}),\s+(\d{4})\b/);
  if (!match) return null;

  const monthMap = {
    jan: "01", january: "01", feb: "02", february: "02", mar: "03", march: "03",
    apr: "04", april: "04", may: "05", jun: "06", june: "06", jul: "07", july: "07",
    aug: "08", august: "08", sep: "09", sept: "09", september: "09", oct: "10", october: "10",
    nov: "11", november: "11", dec: "12", december: "12",
  };

  const month = monthMap[match[1].toLowerCase()];
  if (!month) return null;

  return `${match[3]}-${month}-${match[2].padStart(2, "0")}`;
}

function parseLocation(value) {
  const text = normalizeWhitespace(value);
  if (!text) return { city: "", country: "" };

  const parts = text.split(",").map((part) => normalizeWhitespace(part)).filter(Boolean);
  if (parts.length >= 2) {
    return { city: parts[0], country: parts.slice(1).join(", ") };
  }

  return { city: text, country: "" };
}

function extractEventTitle(document) {
  const selectors = [".b-content__title-highlight", "h1", "h2", "h3"];
  for (const selector of selectors) {
    const element = document.querySelector(selector);
    const text = normalizeWhitespace(element?.textContent);
    if (text) return text;
  }
  return "";
}

function extractEventMetadata(document) {
  const rawText = document.body?.textContent || "";
  const text = normalizeWhitespace(rawText);
  const lines = rawText.split(/\r?\n/).map(normalizeWhitespace).filter(Boolean);

  const date = parseDate(text) || null;
  let venue = "";
  let location = "";

  const venueMatch = text.match(/\bVenue:\s*([^]+?)(?=\b(?:Location|Date|Event|Bout|Main Card)\b|$)/i);
  if (venueMatch) venue = normalizeWhitespace(venueMatch[1]);

  const locationMatch = text.match(/\bLocation:\s*([^]+?)(?=\b(?:Venue|Date|Event|Bout|Main Card)\b|$)/i);
  if (locationMatch) location = normalizeWhitespace(locationMatch[1]);

  if (!location) {
    const candidate = lines.find((line) => line.includes(",") && !isDateLike(line) && line.length < 80);
    if (candidate) location = candidate;
  }

  const { city, country } = parseLocation(location);
  return { date, venue, city, country };
}

function extractEventLinks(document) {
  const anchors = Array.from(document.querySelectorAll('a[href*="event-details"]'));
  const seen = new Set();
  const rows = [];

  for (const anchor of anchors) {
    const href = anchor.href || anchor.getAttribute("href") || "";
    if (!href || seen.has(href)) continue;
    seen.add(href);

    const row = anchor.closest("tr");
    const cells = row ? Array.from(row.querySelectorAll("td")) : [];
    const rowTexts = cells.map((cell) => normalizeWhitespace(cell.textContent)).filter(Boolean);
    const name = normalizeWhitespace(anchor.textContent) || rowTexts[0] || href;
    const dateText = rowTexts.find((value) => isDateLike(value)) || "";
    const locationText = rowTexts.find((value) => value.includes(",") && !isDateLike(value) && value !== name) || "";

    rows.push({
      href,
      name,
      date: parseDate(dateText),
      location: locationText,
    });
  }

  return rows;
}

function extractWeightClass(text) {
  const lower = normalizeWhitespace(text).toLowerCase();
  for (const hint of WEIGHT_CLASS_HINTS) {
    if (lower.includes(hint)) {
      return hint
        .split(" ")
        .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
        .join(" ");
    }
  }
  return "";
}

function findSectionLabel(text) {
  const lower = normalizeWhitespace(text).toLowerCase();
  if (lower === "main card" || lower.includes("main card")) return "main";
  if (lower === "early prelims" || lower.includes("early prelims")) return "early_prelims";
  if (lower === "prelims" || lower.includes("prelims")) return "prelims";
  return null;
}

function resolveFighterId(name, fighterIndex) {
  return fighterIndex.exact.get(normalizeKey(name))
    ?? fighterIndex.normalized.get(normalizeKey(name))
    ?? null;
}

function pickFightDetailLink(row) {
  const anchor = row.querySelector('a[href*="fight-details"]');
  return anchor ? (anchor.href || anchor.getAttribute("href") || null) : null;
}

function extractBoutFighters(row) {
  const anchors = Array.from(row.querySelectorAll('a[href*="fighter-details"]'));
  const names = [];

  for (const anchor of anchors) {
    const name = normalizeWhitespace(anchor.textContent);
    if (name && !names.includes(name)) names.push(name);
  }

  if (names.length >= 2) return [names[0], names[1]];

  const cells = Array.from(row.querySelectorAll("td"));
  const fighterLike = cells
    .map((cell) => normalizeWhitespace(cell.textContent))
    .filter((value) => /[A-Za-z]/.test(value) && value.length > 2);

  if (fighterLike.length >= 2) return [fighterLike[0], fighterLike[1]];
  return [];
}

async function fetchDocument(url) {
  const response = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) MMA Grid scraper",
      accept: "text/html,application/xhtml+xml",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status} ${response.statusText}`);
  }

  const html = await response.text();
  return new JSDOM(html, { url }).window.document;
}

async function loadFighterIndex(supabase) {
  const { data, error } = await supabase.from("fighters").select("id, name");
  if (error) throw error;

  const exact = new Map();
  const normalized = new Map();

  for (const row of data ?? []) {
    const key = normalizeKey(row.name);
    if (!exact.has(key)) exact.set(key, row.id);
    if (!normalized.has(key)) normalized.set(key, row.id);
  }

  return { exact, normalized };
}

async function scrapeFightDetail(fightUrl) {
  const document = await fetchDocument(fightUrl);
  const rows = Array.from(document.querySelectorAll("tr.b-fight-details__table-row"));
  const participants = [];
  let method = "";
  let round = null;
  let time = "";
  let winnerName = null;

  for (const row of rows) {
    const cells = Array.from(row.querySelectorAll("td"));
    if (cells.length < 10) continue;

    const result = normalizeWhitespace(cells[0].textContent).toLowerCase();
    const nameCell = cells[1];
    const pTags = Array.from(nameCell.querySelectorAll("p"));
    const names = pTags.map((tag) => normalizeWhitespace(tag.textContent)).filter(Boolean);
    const name = names[1] || names[0] || normalizeWhitespace(nameCell.textContent);
    const rowMethod = normalizeWhitespace(cells[7].textContent);
    const rowRound = Number.parseInt(normalizeWhitespace(cells[8].textContent), 10);
    const rowTime = normalizeWhitespace(cells[9].textContent) || "0:00";

    participants.push({ result, name });

    if (!method && rowMethod && rowMethod !== "--") method = rowMethod;
    if (round === null && Number.isFinite(rowRound)) round = rowRound;
    if (!time && rowTime && rowTime !== "--") time = rowTime;
    if (!winnerName && result.includes("win")) winnerName = name;
  }

  return {
    participants,
    winnerName,
    method: method || "N/A",
    round: Number.isFinite(round) ? round : null,
    time: time || "0:00",
  };
}

async function upsertEvent(supabase, eventPayload) {
  const { data, error } = await supabase
    .from("events")
    .upsert(eventPayload, { onConflict: "slug" })
    .select("id, slug")
    .single();

  if (error) throw error;
  return data;
}

async function upsertEventBouts(supabase, eventId, bouts) {
  if (bouts.length === 0) return;

  const { error } = await supabase
    .from("event_bouts")
    .upsert(
      bouts.map((bout) => ({
        ...bout,
        event_id: eventId,
      })),
      { onConflict: "event_id,bout_order" },
    );

  if (error) throw error;
}

async function scrapeEventPage(supabase, fighterIndex, source) {
  const listDocument = await fetchDocument(source.url);
  const eventLinks = extractEventLinks(listDocument);

  console.log(`\n[${source.status}] ${source.url}`);
  console.log(`Found ${eventLinks.length} events`);

  for (const eventLink of eventLinks) {
    try {
      const detailDocument = await fetchDocument(eventLink.href);
      const eventName = extractEventTitle(detailDocument) || eventLink.name;
      const meta = extractEventMetadata(detailDocument);
      const eventDate = eventLink.date || meta.date;
      if (!eventDate) {
        console.warn(`  skipping ${eventName}: missing event date`);
        continue;
      }
      const slug = `${slugify(eventName)}-${eventDate}`;

      const eventPayload = {
        slug,
        name: eventName,
        date: eventDate,
        status: source.status,
        venue: meta.venue || "",
        city: meta.city || "",
        country: meta.country || "",
        source_url: eventLink.href,
      };

      const savedEvent = await upsertEvent(supabase, eventPayload);
      const bouts = [];
      let boutOrder = 1;
      let currentSegment = "main";
      const rows = Array.from(detailDocument.querySelectorAll("tr"));

      for (const row of rows) {
        const text = normalizeWhitespace(row.textContent);
        if (!text) continue;

        const section = findSectionLabel(text);
        if (section) {
          currentSegment = section;
          continue;
        }

        const fighters = extractBoutFighters(row);
        if (fighters.length < 2) continue;

        const [fighterAName, fighterBName] = fighters;
        const fightUrl = pickFightDetailLink(row);
        const fightDetail = fightUrl ? await scrapeFightDetail(fightUrl).catch(() => null) : null;
        const winnerName =
          fightDetail?.winnerName ||
          fightDetail?.participants.find((participant) => participant.result.includes("win"))?.name ||
          null;

        const fightText = normalizeWhitespace(row.textContent);
        const fighterAId = resolveFighterId(fighterAName, fighterIndex);
        const fighterBId = resolveFighterId(fighterBName, fighterIndex);
        const weightClass = extractWeightClass(fightText);
        const isTitleFight = /title/i.test(fightText);

        let winnerCorner = null;
        if (winnerName) {
          if (normalizeKey(winnerName) === normalizeKey(fighterAName)) winnerCorner = "A";
          else if (normalizeKey(winnerName) === normalizeKey(fighterBName)) winnerCorner = "B";
        }

        bouts.push({
          bout_order: boutOrder,
          card_segment: currentSegment,
          fighter_a_id: fighterAId,
          fighter_b_id: fighterBId,
          fighter_a_name: fighterAName,
          fighter_b_name: fighterBName,
          weight_class: weightClass,
          is_title_fight: isTitleFight,
          status: source.status === "completed" ? "completed" : "scheduled",
          winner_corner: winnerCorner,
          method: fightDetail?.method || "",
          round: fightDetail?.round ?? null,
          time: fightDetail?.time || "",
        });

        boutOrder += 1;
      }

      await upsertEventBouts(supabase, savedEvent.id, bouts);
      console.log(`  upserted ${eventName} (${bouts.length} bouts)`);
      await sleep(150);
    } catch (error) {
      console.error(`  failed ${eventLink.href}`);
      console.error(error instanceof Error ? error.message : error);
    }
  }
}

async function main() {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE key. Set SUPABASE_SERVICE_ROLE_KEY for writes.");
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  });

  if (!process.env.SUPABASE_SERVICE_ROLE_KEY) {
    console.warn("Warning: SUPABASE_SERVICE_ROLE_KEY is not set. Writes may fail once RLS is enabled.");
  }

  const fighterIndex = await loadFighterIndex(supabase);
  console.log(`Loaded fighter index: ${fighterIndex.exact.size} fighters`);

  for (const source of EVENT_INDEX_SOURCES) {
    await scrapeEventPage(supabase, fighterIndex, source);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
