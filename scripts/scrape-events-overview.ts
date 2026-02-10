import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import * as cheerio from "cheerio";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL) throw new Error("SUPABASE_URL is required");
if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY is required");

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ---------- CLI ----------
function arg(name: string): string | null {
  const i = process.argv.indexOf(name);
  if (i === -1) return null;
  return process.argv[i + 1] ?? null;
}

const fromYear = Number(arg("--from") ?? "2020");
const toYear = Number(arg("--to") ?? "2026");
const sleepMs = Number(arg("--sleep") ?? "250");
const limit = Number(arg("--limit") ?? "0"); // 0 = unlimited
const dry = process.argv.includes("--dry");
const debug = process.argv.includes("--debug");

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function clean(s: unknown) {
  return String(s ?? "").replace(/\s+/g, " ").trim();
}

function overviewUrl(ksiMatchId: string) {
  const u = new URL("https://www.ksi.is/leikir-og-urslit/felagslid/leikur");
  u.searchParams.set("id", ksiMatchId);
  u.searchParams.set("banner-tab", "overview");
  return u.toString();
}

async function fetchHtml(url: string): Promise<string> {
  const res = await fetch(url, {
    headers: { "user-agent": "Mozilla/5.0 (icelanddbv2; overview-events-scraper)" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.text();
}

function getParamId(href: string | null | undefined, key = "id"): string | null {
  if (!href) return null;
  const m = href.match(new RegExp(`[?&]${key}=(\\d+)`));
  return m ? m[1] : null;
}

// minute formats like: 22´ , 90+2´
function extractMinute(text: string): { minute: number | null; stoppage: number | null } {
  const t = clean(text);
  const m = t.match(/(\d{1,3})(?:\s*\+\s*(\d{1,2}))?\s*[´'’]/);
  if (!m) return { minute: null, stoppage: null };
  const minute = Number(m[1]);
  const stoppage = m[2] ? Number(m[2]) : null;
  return { minute: Number.isFinite(minute) ? minute : null, stoppage };
}

function removeMinuteToken(s: string) {
  return s
    .replace(/(\d{1,3})(?:\s*\+\s*\d{1,2})?\s*[´'’]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Icon-based classification (works when text has no keywords)
 */
function detectEventTypeFromIcons(
  $: cheerio.CheerioAPI,
  $el: cheerio.Cheerio<cheerio.AnyNode>,
  playerCount: number,
): string | null {
  // Yellow card: often a small yellow block
  const hasYellow =
    $el.find("div[style*='FAC83C']").length > 0 ||
    $el
      .find("div")
      .toArray()
      .some((d) => /bg-\[#FAC83C\]/i.test(clean($(d).attr("class"))) || /FAC83C/i.test(clean($(d).attr("style"))));

  if (hasYellow) return "yellow";

  // Substitution: if 2+ players it's substitution (most reliable on overview)
  if (playerCount >= 2) return "substitution";

  // Build icon HTML safely
  const iconHtml = clean(
    $el
      .find("svg, img")
      .toArray()
      .map((n) => clean($.html(n) ?? ""))
      .join(" "),
  );

  // Red (often #DD3636 appears in svg stroke)
  // NOTE: KSI also uses red for "sub out" arrows, but we already handled playerCount>=2 above.
  if (/#DD3636/i.test(iconHtml)) return "red";

  // Goal heuristic: on overview, single-player events with svg bubbles are very often goals.
  const hasSvg = $el.find("svg").length > 0;
  if (playerCount === 1 && hasSvg) return "goal";

  return null;
}

/**
 * Keyword-based classification (works when text/iconHints contain clues)
 */
function normaliseEventType(text: string, iconHints: string[], playerCount: number): string {
  const t = clean(text).toLowerCase();
  const h = clean(iconHints.join(" ")).toLowerCase();
  const all = `${t} ${h}`;

  if (playerCount >= 2) return "substitution";
  if (/(sjálfsmark|sjalfsmark|own goal)/i.test(all)) return "own_goal";
  if (/(víti|viti|penalty)/i.test(all)) return "penalty";
  if (/(mark|goal)/i.test(all)) return "goal";
  if (/(seinna gult|second yellow)/i.test(all)) return "second_yellow";
  if (/(gult|yellow)/i.test(all)) return "yellow";
  if (/(rautt|red)/i.test(all)) return "red";

  return "unknown";
}

type EventRow = {
  ksi_match_id: string;
  event_idx: number;
  minute: number;
  stoppage: number | null;
  event_type: string;
  ksi_team_id: string | null;
  ksi_player_id: string | null;
  related_player_ksi_id: string | null;
  notes: string | null;
  raw: any;
};

/**
 * Find the main Atburðir (events) two-column container on overview.
 */
function findEventsGrid($: cheerio.CheerioAPI): cheerio.Cheerio<any> | null {
  const grids = $("div.grid.grid-cols-2, div.grid.l\\:grid-cols-2, div.grid").toArray();

  const hasMinute = (root: cheerio.Cheerio<any>) =>
    root
      .find("*")
      .toArray()
      .some((n) => /\d{1,3}(?:\s*\+\s*\d{1,2})?\s*[´'’]/.test(clean($(n).text())));

  for (const g of grids) {
    const $g = $(g);
    const children = $g.children().toArray();
    if (children.length < 2) continue;

    const $c1 = $(children[0]);
    const $c2 = $(children[1]);

    const c1ok = $c1.find("a[href^='/leikmenn/leikmadur?id=']").length > 0 || hasMinute($c1);
    const c2ok = $c2.find("a[href^='/leikmenn/leikmadur?id=']").length > 0 || hasMinute($c2);
    if (!c1ok || !c2ok) continue;

    const aroundText = clean(
      $g
        .prevAll()
        .slice(0, 8)
        .toArray()
        .map((n) => clean($(n).text()))
        .join(" "),
    );

    const selfText = clean($g.text());
    if (/atburðir/i.test(aroundText + " " + selfText)) return $g;
  }

  // fallback: first 2-col grid with enough minute markers
  for (const g of grids) {
    const $g = $(g);
    const kids = $g.children().toArray();
    if (kids.length < 2) continue;
    const $c1 = $(kids[0]);
    const $c2 = $(kids[1]);
    const c1min = $c1.text().match(/\d{1,3}(?:\s*\+\s*\d{1,2})?\s*[´'’]/g)?.length ?? 0;
    const c2min = $c2.text().match(/\d{1,3}(?:\s*\+\s*\d{1,2})?\s*[´'’]/g)?.length ?? 0;
    if (c1min >= 2 && c2min >= 2) return $g;
  }

  return null;
}

function parseEventsFromColumn(
  $: cheerio.CheerioAPI,
  $col: cheerio.Cheerio<any>,
  sideTeamId: string | null,
): Omit<EventRow, "event_idx">[] {
  const out: Omit<EventRow, "event_idx">[] = [];

  const candidates = new Set<any>();

  // anchor parent blocks are usually best
  $col.find("a[href^='/leikmenn/leikmadur?id=']").each((_, a) => {
    const $a = $(a);
    const $row = $a.closest("div, li, tr");
    if ($row.length) candidates.add($row.get(0));
  });

  // include icon rows even if no player link
  $col.find("div, li, tr").each((_, el) => {
    const $el = $(el);
    const text = clean($el.text());
    if (!text) return;
    if (text.length > 220) return;
    if (!/\d{1,3}(?:\s*\+\s*\d{1,2})?\s*[´'’]/.test(text)) return;

    const hasIcon = $el.find("svg, img").length > 0;
    const hasPlayer = $el.find("a[href^='/leikmenn/leikmadur?id=']").length > 0;
    if (hasIcon || hasPlayer) candidates.add(el);
  });

  const seen = new Set<string>();

  for (const el of candidates) {
    const $el = $(el);
    const rawText = clean($el.text());
    if (!rawText) continue;
    if (rawText.length > 220) continue;
    if (/(byrjunarlið|varamenn|liðstjórn|dómarar|innbyrðis|staða|leikskýrsla)/i.test(rawText)) continue;

    const { minute, stoppage } = extractMinute(rawText);
    if (minute === null) continue;

    const playerLinks = $el.find("a[href^='/leikmenn/leikmadur?id=']").toArray();
    const playerCount = playerLinks.length;

    const p1 = playerLinks[0] ? getParamId($(playerLinks[0]).attr("href"), "id") : null;
    const p2 = playerLinks[1] ? getParamId($(playerLinks[1]).attr("href"), "id") : null;

    const iconHints: string[] = [];
    $el.find("[title], img[alt], img[title], svg[title]").each((_, node) => {
      const a = clean($(node).attr("alt"));
      const b = clean($(node).attr("title"));
      if (a) iconHints.push(a);
      if (b) iconHints.push(b);
    });

    let eventType = normaliseEventType(rawText, iconHints, playerCount);

    // ✅ If keyword logic can't classify, use icon-based logic
    if (eventType === "unknown") {
      const iconType = detectEventTypeFromIcons($, $el, playerCount);
      if (iconType) eventType = iconType;
    }

    // notes: remove minute tokens + player names
    let notes: string | null = removeMinuteToken(rawText);

    for (const pl of playerLinks) {
      const nm = clean($(pl).text());
      if (nm) notes = clean((notes ?? "").replace(nm, " "));
    }

    notes = clean((notes ?? "").replace(/[’'´]/g, " "));
    notes = clean(notes);
    if (!notes || /^[,.\-–—]+$/.test(notes)) notes = null;

    const key = [minute, stoppage ?? "", sideTeamId ?? "", p1 ?? "", p2 ?? "", eventType, notes ?? ""].join("|");
    if (seen.has(key)) continue;
    seen.add(key);

    out.push({
      ksi_match_id: "", // filled by caller
      minute,
      stoppage,
      event_type: eventType,
      ksi_team_id: sideTeamId,
      ksi_player_id: p1,
      related_player_ksi_id: p2,
      notes,
      raw: {
        text: rawText,
        iconHints,
        player_names: playerLinks.map((x) => clean($(x).text())),
      },
    });
  }

  return out;
}

function parseOverviewEvents(
  $: cheerio.CheerioAPI,
  ksiMatchId: string,
  homeTeamId: string | null,
  awayTeamId: string | null,
): EventRow[] {
  const $grid = findEventsGrid($);

  if (!$grid) {
    const fallback: EventRow[] = [];
    let idx = 0;

    $("div, li, tr").each((_, el) => {
      const $el = $(el);
      const text = clean($el.text());
      if (!text || text.length > 200) return;
      if (!/\d{1,3}(?:\s*\+\s*\d{1,2})?\s*[´'’]/.test(text)) return;

      const playerLinks = $el.find("a[href^='/leikmenn/leikmadur?id=']").toArray();
      const hasIcon = $el.find("svg, img").length > 0;
      if (!hasIcon && playerLinks.length === 0) return;

      const { minute, stoppage } = extractMinute(text);
      if (minute === null) return;

      const p1 = playerLinks[0] ? getParamId($(playerLinks[0]).attr("href"), "id") : null;
      const p2 = playerLinks[1] ? getParamId($(playerLinks[1]).attr("href"), "id") : null;

      let eventType = normaliseEventType(text, [], playerLinks.length);
      if (eventType === "unknown") {
        const iconType = detectEventTypeFromIcons($, $el, playerLinks.length);
        if (iconType) eventType = iconType;
      }

      let notes: string | null = removeMinuteToken(text);
      for (const pl of playerLinks) {
        const nm = clean($(pl).text());
        if (nm) notes = clean((notes ?? "").replace(nm, " "));
      }
      notes = clean(notes);
      if (!notes) notes = null;

      fallback.push({
        ksi_match_id: ksiMatchId,
        event_idx: idx++,
        minute,
        stoppage,
        event_type: eventType,
        ksi_team_id: null,
        ksi_player_id: p1,
        related_player_ksi_id: p2,
        notes,
        raw: { text },
      });
    });

    return fallback;
  }

  // assume first column = home, second = away
  const kids = $grid.children().toArray();
  const $homeCol = $(kids[0]);
  const $awayCol = $(kids[1]);

  const homeRaw = parseEventsFromColumn($, $homeCol, homeTeamId);
  const awayRaw = parseEventsFromColumn($, $awayCol, awayTeamId);

  const merged: EventRow[] = [];
  let eventIdx = 0;

  for (const e of [...homeRaw, ...awayRaw]) {
    merged.push({
      ...e,
      ksi_match_id: ksiMatchId,
      event_idx: eventIdx++,
    });
  }

  return merged;
}

// ---------- Main ----------
async function main() {
  console.log(`Scrape OVERVIEW events ${fromYear}..${toYear}`);
  console.log(
    `Dry: ${dry ? "YES" : "NO"} | sleep=${sleepMs}ms | limit=${limit || "none"} | debug=${debug ? "YES" : "NO"}`,
  );

  const { data: matches, error } = await supabase
    .from("matches")
    .select("ksi_match_id, season_year, home_team_ksi_id, away_team_ksi_id")
    .gte("season_year", fromYear)
    .lte("season_year", toYear);

  if (error) throw new Error(error.message);

  const all = (matches ?? []) as any[];
  const target = limit && limit > 0 ? all.slice(0, limit) : all;

  console.log(`Matches in range: ${all.length} | processing: ${target.length}`);

  let ok = 0;
  let fail = 0;

  for (let i = 0; i < target.length; i++) {
    const m = target[i];
    const mid = String(m.ksi_match_id);
    const url = overviewUrl(mid);

    const homeTeamId = m.home_team_ksi_id ? String(m.home_team_ksi_id) : null;
    const awayTeamId = m.away_team_ksi_id ? String(m.away_team_ksi_id) : null;

    console.log(`\n[#${i + 1}/${target.length}] match ${mid} -> ${url}`);

    try {
      const html = await fetchHtml(url);
      const $ = cheerio.load(html);

      const events = parseOverviewEvents($, mid, homeTeamId, awayTeamId);

      console.log(`  parsed events: ${events.length}`);
      if (debug) console.log(`  sample:`, events.slice(0, 10));

      if (dry) {
        console.log(`  DRY: would upsert events=${events.length}`);
      } else {
        if (events.length) {
          const { error: eErr } = await supabase.from("match_events").upsert(events, {
            onConflict: "ksi_match_id,event_idx",
          });

          if (eErr) throw new Error(`match_events upsert failed: ${eErr.message}`);
        }

        console.log(`  ✅ saved events=${events.length}`);
      }

      ok++;
    } catch (e: any) {
      fail++;
      console.error(`  ❌ ${e?.message ?? String(e)}`);
    }

    if (sleepMs > 0) await sleep(sleepMs);
  }

  console.log(`\nDone. OK=${ok} FAIL=${fail}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
