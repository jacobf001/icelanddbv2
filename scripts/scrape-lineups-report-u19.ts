// scripts/scrape-lineups-report-u19.ts
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
const sleepMs = Number(arg("--sleep") ?? "250");
const limit = Number(arg("--limit") ?? "0"); // 0 = unlimited
const dry = process.argv.includes("--dry");
const debug = process.argv.includes("--debug");
const replace = process.argv.includes("--replace"); // if you want to wipe existing match_lineups for a match before upsert

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
    headers: { "user-agent": "Mozilla/5.0 (icelanddbv2; lineups-scraper)" },
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
  // removes "55´" or "90+2´" at end
  return name.replace(/\s+\d{1,3}(?:\s*\+\s*\d{1,2})?\s*[´'’]\s*$/g, "").trim();
}

function stripGkMarker(name: string): { name: string; is_gk: boolean } {
  const is_gk = /\(M\)/i.test(name);
  return { name: name.replace(/\(M\)/gi, "").trim(), is_gk };
}

async function fetchMatchIdsNeedingPlayerIds(fromYear: number, toYear: number, compIds: string[]) {
  // 1) Get all match ids (U19, in range)
  const matches = await fetchU19Matches(fromYear, toYear, compIds);
  const matchIds = matches.map((m) => String(m.ksi_match_id));

  // 2) Query match_lineups for rows where ksi_player_id is NULL (batch match ids)
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


// ---------- LINEUPS parsing ----------
function findLineupGrid($: cheerio.CheerioAPI) {
  const grids = $("div.grid.grid-cols-2").toArray();
  for (const g of grids) {
    const $g = $(g);
    const hasHome = $g.find("img[alt='Heimamenn']").length > 0;
    const hasAway = $g.find("img[alt='Gestir']").length > 0;
    const hasByrjunar =
      $g
        .find("span")
        .filter((_, s) => clean($(s).text()) === "Byrjunarlið").length > 0;

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
    x.is("div") && x.find("a[href^='/leikmenn/leikmadur?id=']").length > 0;

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
  ksi_match_id: string;
  lineup_idx: number;
  ksi_team_id: string | null;
  ksi_player_id: string | null;
  player_name: string;
  shirt_number: number | null;
  squad: "xi" | "bench";
  side: "home" | "away";
  is_gk: boolean | null;
  raw: any;
};

function parseLineupList(
  $: cheerio.CheerioAPI,
  ksiMatchId: string,
  ksiTeamId: string | null,
  squad: "xi" | "bench",
  side: "home" | "away",
  $list: cheerio.Cheerio<any>,
  startIdx: number,
): LineupRow[] {
  const rows: LineupRow[] = [];

  const items = $list
    .children()
    .filter((_, el) => {
      const $el = $(el);
      return ($el.is("a") || $el.is("div")) && $el.hasClass("group");
    })
    .toArray();

  for (const el of items) {
    const $el = $(el);

    // ✅ Works if the row is <a> OR a <div> containing an <a> to the player
    const $a = $el.is("a") ? $el : $el.find("a[href^='/leikmenn/leikmadur?id=']").first();

    const href = String($a.attr("href") ?? "");
    const ksi_player_id = href ? getParamId(href, "id") : null;

    const shirtTxt = cleanText($el.find("span.w-\\[20rem\\], span.body-5").first().text());
    const shirt_number = /^\d+$/.test(shirtTxt) ? Number(shirtTxt) : null;

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
      ksi_match_id: ksiMatchId,
      lineup_idx: startIdx + rows.length,
      ksi_team_id: ksiTeamId,
      side,
      squad,
      ksi_player_id,
      player_name,
      shirt_number,
      is_gk: gk.is_gk ? true : null,
      raw: {
        href: href || null,
        text: cleanText($el.text()),
      },
    });
  }

  return rows;
}

function parseLineupsFromReportHtml(
  $: cheerio.CheerioAPI,
  ksiMatchId: string,
  homeTeamId: string | null,
  awayTeamId: string | null,
): LineupRow[] {
  const $grid = findLineupGrid($);
  if (!$grid) return [];

  const out: LineupRow[] = [];
  let idx = 0;

  const startingLists = findLineupListsForLabel($, $grid, "Byrjunarlið");
  if (startingLists) {
    out.push(...parseLineupList($, ksiMatchId, homeTeamId, "xi", "home", startingLists.homeList, idx));
    idx = out.length;

    out.push(...parseLineupList($, ksiMatchId, awayTeamId, "xi", "away", startingLists.awayList, idx));
    idx = out.length;
  }

  const benchLists = findLineupListsForLabel($, $grid, "Varamenn");
  if (benchLists) {
    out.push(...parseLineupList($, ksiMatchId, homeTeamId, "bench", "home", benchLists.homeList, idx));
    idx = out.length;

    out.push(...parseLineupList($, ksiMatchId, awayTeamId, "bench", "away", benchLists.awayList, idx));
    idx = out.length;
  }

  return out;
}

// ---------- U19 match selection ----------
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
        .select("ksi_match_id, season_year, scraped_report_at, home_team_ksi_id, away_team_ksi_id, ksi_competition_id")
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

// ---------- Main ----------
async function main() {
  console.log(`Scrape U19 match lineups (report tab) ${fromYear}..${toYear}`);
  console.log(`Dry: ${dry ? "YES" : "NO"} | sleep=${sleepMs}ms | limit=${limit || "none"} | debug=${debug ? "YES" : "NO"} | replace=${replace ? "YES" : "NO"}`);

  const compIds = await fetchU19CompetitionIds(fromYear, toYear);
  console.log(`U19 competitions in range: ${compIds.length}`);

  const matches = await fetchU19Matches(fromYear, toYear, compIds);

  // only matches that currently have lineup rows with missing ksi_player_id
  const needsIds = await fetchMatchIdsNeedingPlayerIds(fromYear, toYear, compIds);
  const needsSet = new Set(needsIds);

  const todo = matches.filter((m) => needsSet.has(String(m.ksi_match_id)));
  const target = limit && limit > 0 ? todo.slice(0, limit) : todo;

  console.log(`Matches needing player_id fix: ${todo.length} | processing: ${target.length}`);

  let ok = 0;
  let fail = 0;

  for (let i = 0; i < target.length; i++) {
    const m = target[i];
    const mid = String(m.ksi_match_id);
    const url = matchReportUrl(mid);

    console.log(`\n[#${i + 1}/${target.length}] match ${mid} -> ${url}`);

    try {
      const html = await fetchHtml(url);
      const $ = cheerio.load(html);

      const homeTeamId = m.home_team_ksi_id ? String(m.home_team_ksi_id) : null;
      const awayTeamId = m.away_team_ksi_id ? String(m.away_team_ksi_id) : null;

      if (!homeTeamId || !awayTeamId) {
        // Don’t hard-fail; just warn (events script can infer too)
        console.log(`  ⚠️ missing home/away team ids on matches row (home=${homeTeamId ?? "-"} away=${awayTeamId ?? "-"})`);
      }

      const lineups = parseLineupsFromReportHtml($, mid, homeTeamId, awayTeamId);

      if (debug) {
        console.log(`  parsed lineups: ${lineups.length}`);
        console.log(`  sample lineup:`, lineups.slice(0, 8));
      }

      if (dry) {
        console.log(`  DRY: would upsert lineups=${lineups.length}`);
      } else {
        if (replace) {
          const { error: delErr } = await supabase.from("match_lineups").delete().eq("ksi_match_id", mid);
          if (delErr) throw new Error(`match_lineups delete failed: ${delErr.message}`);
        }

        if (lineups.length) {
          const { error: lErr } = await supabase
            .from("match_lineups")
            .upsert(lineups, { onConflict: "ksi_match_id,lineup_idx" });

          if (lErr) throw new Error(`match_lineups upsert failed: ${lErr.message}`);
        }

        const { error: mErr } = await supabase
          .from("matches")
          .update({ scraped_report_at: new Date().toISOString() })
          .eq("ksi_match_id", mid);

        if (mErr) throw new Error(`matches update failed: ${mErr.message}`);

        console.log(`  ✅ saved lineups=${lineups.length}`);
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