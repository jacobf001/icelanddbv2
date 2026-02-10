// scripts/fetch-league-tables.ts
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import * as cheerio from "cheerio";
import type { Element } from "domhandler";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL) throw new Error("SUPABASE_URL is required");
if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY is required");

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

type Competition = {
  ksi_competition_id: string;
  season_year: number;
  name: string;
  gender: string;
  category: string;
  tier: number | null;
};

function arg(name: string): string | null {
  const i = process.argv.indexOf(name);
  if (i === -1) return null;
  return process.argv[i + 1] ?? null;
}

const fromYear = Number(arg("--from") ?? "2020");
const toYear = Number(arg("--to") ?? "2026");
const sleepMs = Number(arg("--sleep") ?? "250");
const limit = Number(arg("--limit") ?? "0"); // 0 = no limit
const dry = process.argv.includes("--dry");
const debug = process.argv.includes("--debug");

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function cleanText(s: string) {
  return s.replace(/\s+/g, " ").trim();
}

/**
 * IMPORTANT: Icelandic letters (ð/þ/æ/ö) are NOT "diacritics".
 * If we don't fold them, headers like "Lið" won't match "lid".
 */
function foldIcelandic(s: string) {
  return s
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // strip accent marks
    .replace(/ð/g, "d")
    .replace(/Ð/g, "d")
    .replace(/þ/g, "th")
    .replace(/Þ/g, "th")
    .replace(/æ/g, "ae")
    .replace(/Æ/g, "ae")
    .replace(/ö/g, "o")
    .replace(/Ö/g, "o");
}

function normHeader(s: string) {
  return foldIcelandic(s).toLowerCase().replace(/\s+/g, " ").trim();
}

function toIntMaybe(s: string | undefined | null): number | null {
  if (!s) return null;
  const t = s.replace(/[^\d-]/g, "").trim();
  if (!t) return null;
  const n = Number(t);
  return Number.isFinite(n) ? n : null;
}

function parseGoalsPair(s: string): { gf: number | null; ga: number | null } {
  const m = s.match(/(\d+)\s*[-:]\s*(\d+)/);
  if (!m) return { gf: null, ga: null };
  return { gf: Number(m[1]), ga: Number(m[2]) };
}

function splitRankFromTeam(s: string) {
  const t = s.replace(/\s+/g, " ").trim();
  const m = t.match(/^(\d+)\s+(.*)$/);
  if (!m) return { pos: null as number | null, team: t };
  return { pos: Number(m[1]), team: m[2].trim() };
}

type ColMap = {
  variant: "ksi_new" | "classic";
  positionIdx: number | null;
  teamIdx: number;
  playedIdx: number | null;
  winsIdx: number | null;
  drawsIdx: number | null;
  lossesIdx: number | null;
  goalsIdx: number | null;
  gdIdx: number | null;
  pointsIdx: number | null;
};

function getStandingsColumnMap(headers: string[]): ColMap | null {
  const H = headers.map(normHeader);

  // TEAM header detection must accept lið/lid/team/felag etc
  const teamIdx = H.findIndex(
    (h) =>
      h === "lid" ||
      h.includes("lid") ||
      h === "felag" ||
      h.includes("felag") ||
      h === "team" ||
      h.includes("team")
  );
  if (teamIdx === -1) return null;

  // NEW KSI TABLE (your HTML):
  // Lið, S, U, J, T, M, +/-, S, SÍÐUSTU 5, Næsti
  const uIdx = H.findIndex((h) => h === "u");
  const jIdx = H.findIndex((h) => h === "j");
  const tIdx = H.findIndex((h) => h === "t");
  const mIdx = H.findIndex((h) => h === "m"); // goals pair like "47-27"
  const pmIdx = H.findIndex((h) => h === "+/-" || h.includes("+/-") || h.includes("diff") || h.includes("gd"));

  const sIdxs = H.map((h, i) => (h === "s" ? i : -1)).filter((i) => i >= 0);

  if (uIdx !== -1 && jIdx !== -1 && tIdx !== -1 && mIdx !== -1 && sIdxs.length >= 2) {
    const playedIdx = sIdxs.find((i) => i > teamIdx) ?? sIdxs[0];
    const pointsIdx = sIdxs[sIdxs.length - 1];

    return {
      variant: "ksi_new",
      positionIdx: null,
      teamIdx,
      playedIdx,
      winsIdx: uIdx,
      drawsIdx: jIdx,
      lossesIdx: tIdx,
      goalsIdx: mIdx,
      gdIdx: pmIdx !== -1 ? pmIdx : null,
      pointsIdx,
    };
  }

  // CLASSIC TABLES (older pages)
  const posIdx = H.findIndex((h) => h === "#" || h.includes("saeti") || h.includes("pos") || h.includes("nr"));
  const playedIdx = H.findIndex((h) => h === "l" || h.includes("leikir") || h.includes("played"));
  const winsIdx = H.findIndex((h) => h === "s" || h.includes("sigr") || h.includes("wins") || h === "w");
  const drawsIdx = H.findIndex((h) => h === "j" || h.includes("jafn") || h.includes("draw") || h === "d");
  const lossesIdx = H.findIndex((h) => h === "t" || h.includes("tap") || h.includes("loss"));
  const goalsIdx = H.findIndex((h) => h.includes("mork") || h.includes("goals") || h.includes("gf-ga") || h === "mk");
  const gdIdx = H.findIndex((h) => h.includes("markat") || h.includes("diff") || h.includes("gd") || h === "md");
  const pointsIdx = H.findIndex((h) => h === "st" || h.includes("stig") || h.includes("pts") || h.includes("points"));

  const hasSomeUseful =
    [playedIdx, winsIdx, drawsIdx, lossesIdx, pointsIdx].filter((x) => x >= 0).length >= 2;

  if (!hasSomeUseful) return null;

  return {
    variant: "classic",
    positionIdx: posIdx >= 0 ? posIdx : null,
    teamIdx,
    playedIdx: playedIdx >= 0 ? playedIdx : null,
    winsIdx: winsIdx >= 0 ? winsIdx : null,
    drawsIdx: drawsIdx >= 0 ? drawsIdx : null,
    lossesIdx: lossesIdx >= 0 ? lossesIdx : null,
    goalsIdx: goalsIdx >= 0 ? goalsIdx : null,
    gdIdx: gdIdx >= 0 ? gdIdx : null,
    pointsIdx: pointsIdx >= 0 ? pointsIdx : null,
  };
}

function extractKsiTeamIdFromRow($row: cheerio.Cheerio<Element>): string | null {
  const href = $row.find('a[href*="id="]').attr("href") || "";
  const m = href.match(/[?&]id=(\d+)/);
  return m ? m[1] : null;
}

function guessPhaseName($: cheerio.CheerioAPI, tableEl: Element): string | null {
  const $table = $(tableEl);

  const caption = cleanText($table.find("caption").first().text() || "");
  if (caption) return caption;

  let prev = $table.prev();
  for (let i = 0; i < 10 && prev.length; i++) {
    const tag = ((prev.get(0) as any)?.tagName || "").toLowerCase();
    if (["h1", "h2", "h3", "h4", "h5"].includes(tag)) {
      const t = cleanText(prev.text());
      if (t) return t;
    }
    prev = prev.prev();
  }
  return null;
}

function parseStandingsTable($: cheerio.CheerioAPI, tableEl: Element, headers: string[]) {
  const $table = $(tableEl);

  const map = getStandingsColumnMap(headers);
  if (!map) return [];

  const bodyRows = $table.find("tbody tr").toArray();

  return bodyRows
    .map((tr) => {
      const $tr = $(tr);
      const tds = $tr.find("td");
      if (!tds.length) return null;

      const cells = tds
        .toArray()
        .map((td) => cleanText($(td).text()));

      const teamCell = cells[map.teamIdx] ?? "";
      const { pos: posFromText, team: teamFromText } = splitRankFromTeam(teamCell);

      // New layout has <a> with two spans: rank + name
      const $teamTd = $tr.find("td").eq(map.teamIdx);
      const posFromSpan = toIntMaybe(cleanText($teamTd.find("a span").first().text()));
      const nameFromSpan = cleanText($teamTd.find("a span").last().text());

      const position =
        map.positionIdx !== null && cells[map.positionIdx]
          ? toIntMaybe(cells[map.positionIdx])
          : posFromSpan ?? posFromText ?? null;

      const teamName = nameFromSpan || teamFromText || teamCell;
      if (!teamName) return null;

      const ksiTeamId = extractKsiTeamIdFromRow($tr);

      let gf: number | null = null;
      let ga: number | null = null;
      if (map.goalsIdx !== null && cells[map.goalsIdx]) {
        const gpair = parseGoalsPair(cells[map.goalsIdx]);
        gf = gpair.gf;
        ga = gpair.ga;
      }

      return {
        position,
        team_name: teamName,
        ksi_team_id: ksiTeamId,
        played: map.playedIdx !== null ? toIntMaybe(cells[map.playedIdx]) : null,
        wins: map.winsIdx !== null ? toIntMaybe(cells[map.winsIdx]) : null,
        draws: map.drawsIdx !== null ? toIntMaybe(cells[map.drawsIdx]) : null,
        losses: map.lossesIdx !== null ? toIntMaybe(cells[map.lossesIdx]) : null,
        goals_for: gf,
        goals_against: ga,
        goal_diff: map.gdIdx !== null ? toIntMaybe(cells[map.gdIdx]) : null,
        points: map.pointsIdx !== null ? toIntMaybe(cells[map.pointsIdx]) : null,
        raw: { headers, cells, map },
      };
    })
    .filter(Boolean) as any[];
}

function competitionUrl(ksiCompetitionId: string) {
  return `https://www.ksi.is/oll-mot/mot?id=${encodeURIComponent(ksiCompetitionId)}`;
}

async function fetchHtml(url: string): Promise<string> {
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0 (icelanddbv2; league-tables-scraper)",
      "accept-language": "is-IS,is;q=0.9,en;q=0.7",
    },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.text();
}

async function main() {
  console.log(`Fetch league tables for tiers 1-6 (Male/Adults) ${fromYear}..${toYear}`);
  console.log(`Dry: ${dry ? "YES" : "NO"} | sleep=${sleepMs}ms | limit=${limit || "none"} | debug=${debug ? "YES" : "NO"}`);

  const { data: comps, error } = await supabase
    .from("competitions")
    .select("ksi_competition_id, season_year, name, gender, category, tier")
    .eq("gender", "Male")
    .eq("category", "Adults")
    .gte("season_year", fromYear)
    .lte("season_year", toYear)
    .in("tier", [1, 2, 3, 4, 5, 6])
    .order("season_year", { ascending: true })
    .order("tier", { ascending: true });

  if (error) throw new Error(error.message);

  const list = (comps ?? []) as Competition[];
  const target = limit && limit > 0 ? list.slice(0, limit) : list;

  console.log(`Competitions to process: ${target.length}`);

  let ok = 0;
  let fail = 0;

  for (const c of target) {
    const url = competitionUrl(c.ksi_competition_id);
    console.log(`\n[${c.season_year} T${c.tier}] ${c.name} -> ${url}`);

    try {
      const html = await fetchHtml(url);
      const $ = cheerio.load(html);

      const tables = $("table").toArray();
      if (debug) console.log(`  debug: tables found = ${tables.length}`);

      let foundAny = 0;
      let tableIndex = 0;

      for (const t of tables) {
        const headers = $(t)
          .find("thead tr th")
          .toArray()
          .map((th) => cleanText($(th).text()));

        const fallbackHeaders =
          headers.length === 0
            ? $(t)
                .find("tr")
                .first()
                .find("th,td")
                .toArray()
                .map((x) => cleanText($(x).text()))
            : headers;

        const map = getStandingsColumnMap(fallbackHeaders);
        if (!map) {
          if (debug && fallbackHeaders.length) {
            console.log(`  debug: skipping table headers=`, fallbackHeaders);
            console.log(`  debug: normalized=`, fallbackHeaders.map(normHeader));
          }
          continue;
        }

        const phaseName = guessPhaseName($, t);
        const rows = parseStandingsTable($, t, fallbackHeaders);
        if (rows.length === 0) continue;

        foundAny++;

        if (dry) {
          console.log(`  - standings table: phase="${phaseName ?? ""}" rows=${rows.length}`);
          console.log(`    sample:`, rows.slice(0, 2));
          tableIndex++;
          continue;
        }

        const phaseKey = phaseName ?? "";

        const { data: lt, error: ltErr } = await supabase
          .from("league_tables")
          .upsert(
            [
              {
                ksi_competition_id: c.ksi_competition_id,
                season_year: c.season_year,
                source_url: url,
                phase_name: phaseKey,
                table_index: tableIndex,
                fetched_at: new Date().toISOString(),
              },
            ],
            { onConflict: "ksi_competition_id,season_year,phase_name,table_index" }
          )
          .select("id")
          .single();

        if (ltErr) throw new Error(`league_tables upsert failed: ${ltErr.message}`);
        const leagueTableId = lt.id as number;

        const { error: delErr } = await supabase
          .from("league_table_rows")
          .delete()
          .eq("league_table_id", leagueTableId);

        if (delErr) throw new Error(`league_table_rows delete failed: ${delErr.message}`);

        const insertRows = rows.map((r: any) => ({
          league_table_id: leagueTableId,
          position: r.position ?? null,
          team_name: r.team_name,
          ksi_team_id: r.ksi_team_id ?? null,
          team_id: null,
          played: r.played ?? null,
          wins: r.wins ?? null,
          draws: r.draws ?? null,
          losses: r.losses ?? null,
          goals_for: r.goals_for ?? null,
          goals_against: r.goals_against ?? null,
          goal_diff: r.goal_diff ?? null,
          points: r.points ?? null,
          raw: r.raw ?? null,
        }));

        const { error: insErr } = await supabase.from("league_table_rows").insert(insertRows);
        if (insErr) throw new Error(`league_table_rows insert failed: ${insErr.message}`);

        console.log(`  ✅ saved table phase="${phaseKey}" rows=${rows.length}`);
        tableIndex++;
      }

      if (foundAny === 0) {
        console.log("  ⚠️ no standings tables detected on page");
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
