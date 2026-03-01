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

const fromYear = Number(arg("--from") ?? "2024");
const toYear = Number(arg("--to") ?? "2025");
const sleepMs = Number(arg("--sleep") ?? "150");
const limitMatches = Number(arg("--limitMatches") ?? "0"); // 0 = unlimited
const dry = process.argv.includes("--dry");
const debug = process.argv.includes("--debug");

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function clean(s: unknown) {
  return String(s ?? "").replace(/\s+/g, " ").trim();
}

function cleanText(s: any) {
  return String(s ?? "").replace(/\s+/g, " ").trim();
}

function matchReportUrl(ksiMatchId: string) {
  const u = new URL("https://www.ksi.is/leikir-og-urslit/felagslid/leikur");
  u.searchParams.set("id", ksiMatchId);
  u.searchParams.set("banner-tab", "report");
  return u.toString();
}

async function fetchHtml(url: string): Promise<string> {
  const res = await fetch(url, {
    headers: { "user-agent": "Mozilla/5.0 (icelanddbv2; fix-lineup-player-ids)" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.text();
}

function getParamId(href: string, key = "id"): string | null {
  const m = href.match(new RegExp(`[?&]${key}=(\\d+)`));
  return m ? m[1] : null;
}

function stripTrailingMinutes(name: string): string {
  return name.replace(/\s+\d{1,3}(?:\s*\+\s*\d{1,2})?\s*[´'’]\s*$/g, "").trim();
}

function stripGkMarker(name: string): { name: string; is_gk: boolean } {
  const is_gk = /\(M\)/i.test(name);
  return { name: name.replace(/\(M\)/gi, "").trim(), is_gk };
}

// ---------- LINEUP parsing (same structure as your report scraper) ----------
function findLineupGrid($: cheerio.CheerioAPI) {
  const grids = $("div.grid.grid-cols-2").toArray();
  for (const g of grids) {
    const $g = $(g);
    const hasHome = $g.find("img[alt='Heimamenn']").length > 0;
    const hasAway = $g.find("img[alt='Gestir']").length > 0;
    const hasByrjunar =
      $g.find("span").filter((_, s) => clean($(s).text()) === "Byrjunarlið").length > 0;

    if (hasHome && hasAway && hasByrjunar) return $g;
  }
  return null;
}

function findLineupListsForLabel(
  $: cheerio.CheerioAPI,
  $grid: cheerio.Cheerio<any>,
  label: "Byrjunarlið" | "Varamenn",
) {
  const kids = $grid.children().toArray();

  const isHeader = (el: any) => {
    const $el = $(el);
    const txt = clean($el.find("span").first().text());
    const hasAlt = $el.find("img[alt='Heimamenn'], img[alt='Gestir']").length > 0;
    return hasAlt && txt === label;
  };

  const looksLikeList = (x: cheerio.Cheerio<any>) =>
    x.is("div") && x.find("a[href*='leikmadur?id=']").length > 0;

  for (let i = 0; i < kids.length - 3; i++) {
    if (isHeader(kids[i]) && isHeader(kids[i + 1])) {
      const list1 = $(kids[i + 2]);
      const list2 = $(kids[i + 3]);
      if (looksLikeList(list1) && looksLikeList(list2)) {
        return { homeList: list1, awayList: list2 };
      }
    }
  }

  return null;
}

type LineupRow = {
  lineup_idx: number;
  ksi_player_id: string | null;
  player_name: string;
  shirt_number: number | null;
  side: "home" | "away";
  squad: "xi" | "bench";
};

function parseLineupList(
  $: cheerio.CheerioAPI,
  squad: "xi" | "bench",
  side: "home" | "away",
  $list: cheerio.Cheerio<any>,
  startIdx: number,
): LineupRow[] {
  const rows: LineupRow[] = [];

  // rows can be <a class="group..."> or <div class="group...">
  const items = $list
    .children()
    .filter((_, el) => {
      const $el = $(el);
      return ($el.is("a") || $el.is("div")) && $el.hasClass("group");
    })
    .toArray();

  for (const el of items) {
    const $el = $(el);

    // IMPORTANT: find player link even when wrapped; also allow relative/absolute
    const $a = $el.is("a") ? $el : $el.find("a[href*='leikmadur?id=']").first();
    const href = String($a.attr("href") ?? "");
    const ksi_player_id = href ? getParamId(href, "id") : null;

    // shirt number (may be missing)
    const shirtTxt = cleanText($el.find("span.w-\\[20rem\\], span.body-5").first().text());
    const shirt_number = /^\d+$/.test(shirtTxt) ? Number(shirtTxt) : null;

    // name
    const spanTexts = $el
      .find("span")
      .toArray()
      .map((s) => cleanText($(s).text()))
      .filter(Boolean);

    let nameRaw =
      spanTexts.find((t) => t.includes("(M)")) ??
      spanTexts.sort((a, b) => b.length - a.length)[0] ??
      cleanText($el.text());

    nameRaw = stripTrailingMinutes(nameRaw);
    const gk = stripGkMarker(nameRaw);
    const player_name = gk.name || nameRaw || "—";

    rows.push({
      lineup_idx: startIdx + rows.length,
      ksi_player_id,
      player_name,
      shirt_number,
      squad,
      side,
    });
  }

  return rows;
}

function parseLineupsFromReportHtml($: cheerio.CheerioAPI): LineupRow[] {
  const $grid = findLineupGrid($);
  if (!$grid) return [];

  const out: LineupRow[] = [];
  let idx = 0;

  const startingLists = findLineupListsForLabel($, $grid, "Byrjunarlið");
  if (startingLists) {
    out.push(...parseLineupList($, "xi", "home", startingLists.homeList, idx));
    idx = out.length;
    out.push(...parseLineupList($, "xi", "away", startingLists.awayList, idx));
    idx = out.length;
  }

  const benchLists = findLineupListsForLabel($, $grid, "Varamenn");
  if (benchLists) {
    out.push(...parseLineupList($, "bench", "home", benchLists.homeList, idx));
    idx = out.length;
    out.push(...parseLineupList($, "bench", "away", benchLists.awayList, idx));
    idx = out.length;
  }

  return out;
}

// ---------- U19 selection ----------
type CompetitionRow = { ksi_competition_id: string };

async function fetchU19CompetitionIds(fromYear: number, toYear: number): Promise<string[]> {
  const pageSize = 1000;
  let from = 0;
  const out: string[] = [];

  while (true) {
    const { data, error } = await supabase
      .from("competitions")
      .select("ksi_competition_id")
      .eq("gender", "Male")
      .eq("category", "U-19")
      .gte("season_year", fromYear)
      .lte("season_year", toYear)
      .range(from, from + pageSize - 1);

    if (error) throw new Error(error.message);

    const batch = (data ?? []) as CompetitionRow[];
    out.push(...batch.map((r) => String(r.ksi_competition_id)));

    if (batch.length < pageSize) break;
    from += pageSize;
  }

  return Array.from(new Set(out));
}

async function fetchU19Matches(fromYear: number, toYear: number, compIds: string[]) {
  const pageSize = 1000;
  const all: any[] = [];

  const idChunkSize = 200;
  for (let ci = 0; ci < compIds.length; ci += idChunkSize) {
    const chunkIds = compIds.slice(ci, ci + idChunkSize);

    let from = 0;
    while (true) {
      const { data, error } = await supabase
        .from("matches")
        .select("ksi_match_id, season_year, ksi_competition_id")
        .gte("season_year", fromYear)
        .lte("season_year", toYear)
        .in("ksi_competition_id", chunkIds)
        .order("ksi_match_id", { ascending: true })
        .range(from, from + pageSize - 1);

      if (error) throw new Error(error.message);

      const batch = data ?? [];
      all.push(...batch);

      if (batch.length < pageSize) break;
      from += pageSize;
    }
  }

  const byId = new Map<string, any>();
  for (const r of all) byId.set(String(r.ksi_match_id), r);
  return [...byId.values()].sort((a, b) => String(a.ksi_match_id).localeCompare(String(b.ksi_match_id)));
}

type MissingRow = {
  id: number;
  ksi_match_id: string;
  lineup_idx: number;
  player_name: string;
};

async function fetchMissingRowsForMatch(mid: string): Promise<MissingRow[]> {
  const { data, error } = await supabase
    .from("match_lineups")
    .select("id, ksi_match_id, lineup_idx, player_name")
    .eq("ksi_match_id", mid)
    .is("ksi_player_id", null);

  if (error) throw new Error(error.message);
  return (data ?? []) as MissingRow[];
}

async function fetchMatchIdsWithMissingLineups(matchIds: string[]): Promise<string[]> {
  const need = new Set<string>();
  const chunkSize = 200;

  for (let i = 0; i < matchIds.length; i += chunkSize) {
    const chunk = matchIds.slice(i, i + chunkSize);

    const { data, error } = await supabase
      .from("match_lineups")
      .select("ksi_match_id")
      .in("ksi_match_id", chunk)
      .is("ksi_player_id", null);

    if (error) throw new Error(error.message);
    for (const r of data ?? []) need.add(String((r as any).ksi_match_id));
  }

  return [...need].sort((a, b) => a.localeCompare(b));
}

// ---------- Main ----------
async function main() {
  console.log(`Fix missing match_lineups.ksi_player_id by lineup_idx (U-19) ${fromYear}..${toYear}`);
  console.log(
    `Dry: ${dry ? "YES" : "NO"} | sleep=${sleepMs}ms | limitMatches=${limitMatches || "none"} | debug=${
      debug ? "YES" : "NO"
    }`,
  );

  const compIds = await fetchU19CompetitionIds(fromYear, toYear);
  const matches = await fetchU19Matches(fromYear, toYear, compIds);
  const matchIds = matches.map((m) => String(m.ksi_match_id));

  const needing = await fetchMatchIdsWithMissingLineups(matchIds);
  const target = limitMatches && limitMatches > 0 ? needing.slice(0, limitMatches) : needing;

  console.log(`U19 competitions: ${compIds.length}`);
  console.log(`U19 matches: ${matches.length} | matches w/ missing ids: ${needing.length} | processing: ${target.length}`);

  let fixed = 0;
  let unresolved = 0;

  for (let i = 0; i < target.length; i++) {
    const mid = target[i];
    console.log(`\n[#${i + 1}/${target.length}] match ${mid}`);

    try {
      const missing = await fetchMissingRowsForMatch(mid);
      console.log(`  missing lineup rows: ${missing.length}`);
      if (!missing.length) continue;

      const html = await fetchHtml(matchReportUrl(mid));
      const $ = cheerio.load(html);

      const scraped = parseLineupsFromReportHtml($);
      if (!scraped.length) {
        console.log(`  ⚠️ could not parse any lineup rows from report page`);
        unresolved += missing.length;
        continue;
      }

      // map lineup_idx -> player_id
      const byIdx = new Map<number, string>();
      for (const r of scraped) {
        if (r.ksi_player_id) byIdx.set(r.lineup_idx, r.ksi_player_id);
      }

      let fixedThis = 0;
      let unresolvedThis = 0;

      for (const mr of missing) {
        const pid = byIdx.get(Number(mr.lineup_idx)) ?? null;
        if (!pid) {
          unresolvedThis++;
          if (debug) console.log(`  - unresolved idx=${mr.lineup_idx} name="${mr.player_name}"`);
          continue;
        }

        fixedThis++;
        if (debug) console.log(`  + fix idx=${mr.lineup_idx} "${mr.player_name}" -> ${pid}`);

        if (!dry) {
          const { error: upErr } = await supabase
            .from("match_lineups")
            .update({
              ksi_player_id: pid,
              raw: {
                fixed_player_id_from_report: true,
                fixed_at: new Date().toISOString(),
              },
            })
            .eq("id", mr.id);

          if (upErr) throw new Error(`update failed id=${mr.id}: ${upErr.message}`);
        }
      }

      fixed += fixedThis;
      unresolved += unresolvedThis;

      console.log(`  ✅ fixed=${fixedThis} unresolved=${unresolvedThis}`);
    } catch (e: any) {
      console.error(`  ❌ ${e?.message ?? String(e)}`);
    }

    if (sleepMs > 0) await sleep(sleepMs);
  }

  console.log(`\nDone.`);
  console.log(`Fixed: ${fixed}${dry ? " (dry run)" : ""}`);
  console.log(`Unresolved: ${unresolved}${dry ? " (dry run)" : ""}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});