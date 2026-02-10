import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import * as cheerio from "cheerio";
import fs from "node:fs";

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
function args(name: string): string[] {
  const out: string[] = [];
  for (let i = 0; i < process.argv.length; i++) {
    if (process.argv[i] === name) out.push(process.argv[i + 1] ?? "");
  }
  return out.filter(Boolean);
}

const csvPath = arg("--csv"); // optional: csv with column ksi_match_id
const matchIdsFromFlags = args("--match"); // can repeat --match 123 --match 456
const dry = process.argv.includes("--dry");
const debug = process.argv.includes("--debug");
const sleepMs = Number(arg("--sleep") ?? "150");

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function clean(s: unknown) {
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
    headers: { "user-agent": "Mozilla/5.0 (icelanddbv2; lineups-fixer)" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.text();
}

function getParamId(href: string, key = "id"): string | null {
  const m = href.match(new RegExp(`[?&]${key}=(\\d+)`));
  return m ? m[1] : null;
}

// ------------ Lineups parsing (grid-based) ------------
function cleanText(s: string) {
  return s.replace(/\s+/g, " ").trim();
}

function stripTrailingMinutes(name: string): string {
  return name.replace(/\s+\d{1,3}(?:\s*\+\s*\d{1,2})?\s*[´'’]\s*$/g, "").trim();
}

function stripGkMarker(name: string): { name: string; is_gk: boolean } {
  const is_gk = /\(M\)/i.test(name);
  return { name: name.replace(/\(M\)/gi, "").trim(), is_gk };
}

/**
 * Finds the main lineups grid (contains both Heimamenn and Gestir icons).
 */
function findLineupGrid($: cheerio.CheerioAPI): cheerio.Cheerio<any> | null {
  const grids = $("div.grid.grid-cols-2").toArray();
  for (const g of grids) {
    const $g = $(g);
    const hasHome = $g.find("img[alt='Heimamenn']").length > 0;
    const hasAway = $g.find("img[alt='Gestir']").length > 0;
    const hasByrjunar = $g
      .find("span")
      .filter((_, s) => cleanText($(s).text()) === "Byrjunarlið").length >= 1;
    if (hasHome && hasAway && hasByrjunar) return $g;
  }
  return null;
}

/**
 * Within the lineup grid, KSI renders:
 *   header(home Byrjunarlið), header(away Byrjunarlið), list(home), list(away),
 *   header(home Varamenn), header(away Varamenn), list(home), list(away)
 */
function findLineupListsForLabel(
  $: cheerio.CheerioAPI,
  $grid: cheerio.Cheerio<any>,
  label: "Byrjunarlið" | "Varamenn",
): { homeList: cheerio.Cheerio<any>; awayList: cheerio.Cheerio<any> } | null {
  const kids = $grid.children().toArray();

  const isHeader = (el: any) => {
    const $el = $(el);
    const txt = cleanText($el.find("span").first().text());
    const hasAlt = $el.find("img[alt='Heimamenn'], img[alt='Gestir']").length > 0;
    return hasAlt && txt === label;
  };

  for (let i = 0; i < kids.length - 3; i++) {
    if (isHeader(kids[i]) && isHeader(kids[i + 1])) {
      const list1 = $(kids[i + 2]);
      const list2 = $(kids[i + 3]);

      const looksLikeList = (x: cheerio.Cheerio<any>) =>
        x.is("div") && x.find("a[href^='/leikmenn/leikmadur?id=']").length > 0;

      if (looksLikeList(list1) && looksLikeList(list2)) {
        return { homeList: list1, awayList: list2 };
      }
    }
  }
  return null;
}

type Squad = "xi" | "bench";
type Side = "home" | "away";

type LineupRow = {
  ksi_match_id: string;
  lineup_idx: number;
  ksi_team_id: string | null;
  side: Side;
  squad: Squad;
  ksi_player_id: string | null;
  player_name: string;
  shirt_number: number | null;
  is_gk: boolean | null;
  raw: any;
};

function parseLineupList(
  $: cheerio.CheerioAPI,
  ksiMatchId: string,
  ksiTeamId: string | null,
  squad: Squad,
  side: Side,
  $list: cheerio.Cheerio<any>,
  startIdx: number,
): LineupRow[] {
  const rows: LineupRow[] = [];
  const anchors = $list.find("a[href^='/leikmenn/leikmadur?id=']").toArray();

  for (const a of anchors) {
    const $a = $(a);
    const href = String($a.attr("href") ?? "");
    const ksi_player_id = getParamId(href, "id");

    // Shirt number: may be missing on some pages => we allow null.
    const shirtTxt = cleanText($a.find("span.w-\\[20rem\\], span.body-5").first().text());
    const shirt_number = shirtTxt && /^\d+$/.test(shirtTxt) ? Number(shirtTxt) : null;

    // Prefer “full name” span (usually longer / includes (M))
    const spanTexts = $a
      .find("span")
      .toArray()
      .map((s) => cleanText($(s).text()))
      .filter(Boolean);

    let nameRaw =
      spanTexts.find((t) => t.includes("(M)")) ??
      spanTexts.sort((a, b) => b.length - a.length)[0] ??
      "";

    nameRaw = stripTrailingMinutes(nameRaw);
    const gk = stripGkMarker(nameRaw);

    // Final fallback: anchor text
    const player_name = gk.name || stripTrailingMinutes(cleanText($a.text())) || "—";

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
      raw: { href, anchorText: cleanText($a.text()) },
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

// ---------- Input: match ids ----------
function readMatchIdsFromCsv(path: string): string[] {
  const txt = fs.readFileSync(path, "utf8");
  const lines = txt.split(/\r?\n/).filter(Boolean);
  if (!lines.length) return [];
  const header = lines[0].split(",").map((x) => x.trim());
  const idx = header.indexOf("ksi_match_id");
  if (idx === -1) throw new Error(`CSV must have a 'ksi_match_id' column`);
  const out: string[] = [];
  for (const line of lines.slice(1)) {
    const cols = line.split(",");
    const v = (cols[idx] ?? "").trim();
    if (v) out.push(v);
  }
  return [...new Set(out)];
}

async function main() {
  const matchIds = [
    ...matchIdsFromFlags,
    ...(csvPath ? readMatchIdsFromCsv(csvPath) : []),
  ].map(String);

  if (!matchIds.length) {
    console.log(`Usage:
  npx tsx scripts/fix-lineups.ts --match 719775 --match 761876 --dry --debug
  npx tsx scripts/fix-lineups.ts --csv path/to/incomplete.csv --dry --debug
CSV must contain a column named: ksi_match_id`);
    process.exit(1);
  }

  console.log(`Fix lineups for matches: ${matchIds.length} | dry=${dry ? "YES" : "NO"}`);

  // Pull home/away team ids for those matches
  const { data: matches, error } = await supabase
    .from("matches")
    .select("ksi_match_id, home_team_ksi_id, away_team_ksi_id")
    .in("ksi_match_id", matchIds);

  if (error) throw new Error(error.message);

  const byId = new Map<string, { home: string | null; away: string | null }>();
  for (const m of matches ?? []) {
    byId.set(String(m.ksi_match_id), {
      home: m.home_team_ksi_id ? String(m.home_team_ksi_id) : null,
      away: m.away_team_ksi_id ? String(m.away_team_ksi_id) : null,
    });
  }

  let ok = 0;
  let fail = 0;

  for (let i = 0; i < matchIds.length; i++) {
    const mid = matchIds[i];
    const url = matchReportUrl(mid);
    const teams = byId.get(mid);

    console.log(`\n[#${i + 1}/${matchIds.length}] ${mid} -> ${url}`);

    try {
      if (!teams) throw new Error(`match ${mid} not found in matches table`);

      const html = await fetchHtml(url);
      const $ = cheerio.load(html);

      const rows = parseLineupsFromReportHtml($, mid, teams.home, teams.away);

      if (debug) {
        console.log(`  parsed lineups: ${rows.length}`);
        console.log(`  sample:`, rows.slice(0, 5));
      }

      if (dry) {
        console.log(`  DRY: would upsert lineups=${rows.length}`);
      } else {
        const { error: lErr } = await supabase
          .from("match_lineups")
          .upsert(rows, { onConflict: "ksi_match_id,side,squad,lineup_idx" }); // ✅ your real unique constraint
        if (lErr) throw new Error(`match_lineups upsert failed: ${lErr.message}`);

        console.log(`  ✅ saved lineups=${rows.length}`);
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
