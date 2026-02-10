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

// ---------- LINEUPS parsing ----------
// We find the main grid that contains BOTH "Heimamenn" and "Gestir" icons and "Byrjunarlið" label.
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

/**
 * Inside the lineup grid, KSI renders (in order):
 *   header(home Byrjunarlið), header(away Byrjunarlið), list(home), list(away),
 *   header(home Varamenn),    header(away Varamenn),    list(home), list(away),
 * We detect the pair of headers, then grab the next 2 list divs.
 */
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
  squad: "starting" | "bench";
  is_gk: boolean | null;
  raw: any;
};

function parseLineupList(
  $: cheerio.CheerioAPI,
  ksiMatchId: string,
  ksiTeamId: string | null,
  squad: "starting" | "bench",
  $list: cheerio.Cheerio<any>,
  startIdx: number,
): LineupRow[] {
  const rows: LineupRow[] = [];
  const anchors = $list.find("a[href^='/leikmenn/leikmadur?id=']").toArray();

  for (const a of anchors) {
    const $a = $(a);
    const href = String($a.attr("href") ?? "");
    const ksi_player_id = getParamId(href, "id");

    // shirt number is usually the first small span with fixed width
    const shirtTxt = clean($a.find("span.w-\\[20rem\\], span.body-5").first().text());
    const shirt_number = shirtTxt && /^\d+$/.test(shirtTxt) ? Number(shirtTxt) : null;

    // choose the "best" name span:
    // - prefer the one that includes "(M)"
    // - otherwise pick the longest visible text span
    const spanTexts = $a
      .find("span")
      .toArray()
      .map((s) => clean($(s).text()))
      .filter(Boolean);

    let nameRaw =
      spanTexts.find((t) => t.includes("(M)")) ??
      spanTexts.sort((a, b) => b.length - a.length)[0] ??
      clean($a.text());

    nameRaw = stripTrailingMinutes(nameRaw);
    const gk = stripGkMarker(nameRaw);

    const player_name = gk.name || "—";

    rows.push({
      ksi_match_id: ksiMatchId,
      lineup_idx: startIdx + rows.length,
      ksi_team_id: ksiTeamId,
      ksi_player_id,
      player_name,
      shirt_number,
      squad,
      is_gk: gk.is_gk ? true : null,
      raw: {
        href,
        anchorText: clean($a.text()),
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

  const starting = findLineupListsForLabel($, $grid, "Byrjunarlið");
  if (starting) {
    out.push(...parseLineupList($, ksiMatchId, homeTeamId, "starting", starting.homeList, idx));
    idx = out.length;
    out.push(...parseLineupList($, ksiMatchId, awayTeamId, "starting", starting.awayList, idx));
    idx = out.length;
  }

  const bench = findLineupListsForLabel($, $grid, "Varamenn");
  if (bench) {
    out.push(...parseLineupList($, ksiMatchId, homeTeamId, "bench", bench.homeList, idx));
    idx = out.length;
    out.push(...parseLineupList($, ksiMatchId, awayTeamId, "bench", bench.awayList, idx));
    idx = out.length;
  }

  return out;
}

// ---------- Main ----------
async function main() {
  console.log(`Scrape match lineups ${fromYear}..${toYear}`);
  console.log(`Dry: ${dry ? "YES" : "NO"} | sleep=${sleepMs}ms | limit=${limit || "none"} | debug=${debug ? "YES" : "NO"}`);

  const { data: matches, error } = await supabase
    .from("matches")
    .select("ksi_match_id, season_year, scraped_report_at, home_team_ksi_id, away_team_ksi_id")
    .gte("season_year", fromYear)
    .lte("season_year", toYear);

  if (error) throw new Error(error.message);

  const all = (matches ?? []) as any[];
  const todo = all.filter((m) => !m.scraped_report_at);
  const target = limit && limit > 0 ? todo.slice(0, limit) : todo;

  console.log(`Matches needing lineup scrape: ${todo.length} | processing: ${target.length}`);

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

      const lineups = parseLineupsFromReportHtml($, mid, homeTeamId, awayTeamId);

      if (debug) {
        console.log(`  parsed lineups: ${lineups.length}`);
        console.log(`  sample lineup:`, lineups.slice(0, 5));
      }

      if (dry) {
        console.log(`  DRY: would upsert lineups=${lineups.length}`);
      } else {
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
