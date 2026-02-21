// scripts/ingest_u19.ts
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import * as cheerio from "cheerio";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) throw new Error("Missing supabase env");

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

const COMPETITIONS = ["196519", "196520"]; // only these two
const SEASONS = [2020, 2021, 2022, 2023, 2024, 2025];

const AGE_CATEGORY = "U-19";
const GENDER = "Karlar";

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchHtml(url: string) {
  const res = await fetch(url, {
    headers: {
      "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    },
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.text();
}

/**
 * The competition page contains links like:
 *   href="/leikir-og-urslit/felagslid/leikur?id=761846"
 * So we extract match ids ONLY from those links (NOT generic "id":123 matches in JSON).
 */
function extractMatchIdsFromCompetitionHtml(html: string): string[] {
  const $ = cheerio.load(html);
  const ids = new Set<string>();

  $('a[href*="/leikir-og-urslit/felagslid/leikur?id="]').each((_, el) => {
    const href = $(el).attr("href") ?? "";
    const m = href.match(/leikur\?id=(\d{5,10})/);
    if (m?.[1]) ids.add(m[1]);
  });

  // fallback regex in case markup changes but href still present in HTML
  const re = /\/leikir-og-urslit\/felagslid\/leikur\?id=(\d{5,10})/g;
  let mm: RegExpExecArray | null;
  while ((mm = re.exec(html))) ids.add(mm[1]);

  return Array.from(ids);
}

/**
 * Pagination links look like:
 *   ?id=196519&banner-tab=matches-and-results&page=2
 * We keep fetching pages until there's no "page=N" link forward.
 */
async function getAllMatchIdsForCompetitionAndSeason(competitionId: string, season: number): Promise<string[]> {
  const all = new Set<string>();

  // NOTE: KSI may or may not respect `season=` here.
  // If it doesn't, you'll still ingest the currently selected season.
  // If it does, great.
  let page = 1;

  while (true) {
    const url =
      `https://ksi.is/oll-mot/mot?id=${competitionId}` +
      `&banner-tab=matches-and-results` +
      `&page=${page}` +
      `&season=${season}`;

    const html = await fetchHtml(url);

    const ids = extractMatchIdsFromCompetitionHtml(html);
    for (const id of ids) all.add(id);

    // detect if there's a next page link
    const $ = cheerio.load(html);
    const nextHref = $(`a[href*="page=${page + 1}"]`).attr("href");

    // if we didn’t find any ids on this page OR no next link, stop
    if (ids.length === 0 || !nextHref) break;

    page += 1;
    await sleep(150);
  }

  return Array.from(all);
}

function inferIsGk(playerName: string | null | undefined): boolean {
  const n = (playerName ?? "").toLowerCase();
  return n.includes("(m)") || n.includes("(gk)") || n.includes("markv") || n.includes("keeper");
}

async function ensureCompetitionRow(args: {
  ksi_competition_id: string;
  season_year: number;
  name: string;
}) {
  // competitions schema (from your screenshot):
  // ksi_competition_id, season_year, name, gender, category, created_at, tier, is_phase, parent_competition_id
  const row: any = {
    ksi_competition_id: args.ksi_competition_id,
    season_year: args.season_year,
    name: args.name,
    gender: GENDER,
    category: AGE_CATEGORY,
    tier: null,
    is_phase: false,
    parent_competition_id: null,
  };

  const { error } = await sb.from("competitions").upsert(row, {
    onConflict: "ksi_competition_id,season_year",
  });

  if (error) throw new Error(`competitions upsert failed: ${error.message}`);
}

async function parseLineups(matchId: string) {
  // IMPORTANT: use ksi.is (not www.ksi.is) to match what the competition page links to
  const matchUrl = `https://ksi.is/leikir-og-urslit/felagslid/leikur?id=${matchId}`;

  const api =
    `http://localhost:3000/api/lineups-from-report?` +
    new URLSearchParams({ url: matchUrl }).toString();

  const res = await fetch(api, { cache: "no-store" });
  const json = (await res.json()) as any;

  if (!res.ok) throw new Error(json?.error ?? `lineups-from-report failed ${res.status}`);

  const payload = json && typeof json === "object" ? json : { data: json };
  return { matchUrl, ...payload };
}

async function upsertMatchAndLineups(args: {
  matchId: string;
  competitionId: string;
  seasonYear: number;
  lineups: any;
}) {
  const { matchId, competitionId, seasonYear, lineups } = args;

  const homeTeamId = lineups?.teams?.home?.ksi_team_id ? String(lineups.teams.home.ksi_team_id) : null;
  const awayTeamId = lineups?.teams?.away?.ksi_team_id ? String(lineups.teams.away.ksi_team_id) : null;

  // matches schema (from your CSV): includes these columns
  const matchRow: any = {
    ksi_match_id: String(matchId),
    ksi_competition_id: String(competitionId),
    season_year: seasonYear,
    age_category: AGE_CATEGORY,
    gender: GENDER,
    home_team_ksi_id: homeTeamId,
    away_team_ksi_id: awayTeamId,
    // leave kickoff_at/venue/score null (your other pipelines can fill later)
  };

  const { error: mErr } = await sb.from("matches").upsert(matchRow, { onConflict: "ksi_match_id" });
  if (mErr) throw new Error(`matches upsert failed: ${mErr.message}`);

  // match_lineups schema (from your screenshot)
  // UNIQUE (ksi_match_id, side, squad, lineup_idx)
  const toRows = (side: "home" | "away") => {
  const teamId = lineups?.teams?.[side]?.ksi_team_id ? String(lineups.teams[side].ksi_team_id) : null;

  const startersSrc: any[] = lineups?.[side]?.starters ?? [];
  const benchSrc: any[] = lineups?.[side]?.bench ?? [];

  const starters = (lineups?.[side]?.starters ?? []).map((p: any, i: number) => ({
    ksi_match_id: matchId,
    ksi_team_id: teamId,
    side,                 // 'home' | 'away'
    squad: "xi",
    lineup_idx: i + 1,    // 1..11
    shirt_number: p.shirt_no ?? null,
    player_name: p.name ?? null,
    ksi_player_id: String(p.ksi_player_id),
    minute_in: 0,
    minute_out: 90,
    raw: p,
    ksi_competition_id: competitionId,
    age_category: AGE_CATEGORY,
    gender: GENDER,
    }));

    const bench = (lineups?.[side]?.bench ?? []).map((p: any, i: number) => ({
    ksi_match_id: matchId,
    ksi_team_id: teamId,
    side,
    squad: "bench",
    lineup_idx: 12 + i,   // 12.. (NO collisions)
    shirt_number: p.shirt_no ?? null,
    player_name: p.name ?? null,
    ksi_player_id: String(p.ksi_player_id),
    minute_in: null,
    minute_out: null,
    raw: p,
    ksi_competition_id: competitionId,
    age_category: AGE_CATEGORY,
    gender: GENDER,
    }));

  return [...starters, ...bench];
};

  const rows = [...toRows("home"), ...toRows("away")].filter((r) => r.ksi_player_id);

  const { error: lErr } = await sb.from("match_lineups").upsert(rows, {
  onConflict: "ksi_match_id,side,lineup_idx",
});
if (lErr) throw new Error(`match_lineups upsert failed: ${lErr.message}`);
}

async function main() {
  for (const competitionId of COMPETITIONS) {
    for (const season of SEASONS) {
      console.log(`\n== Competition ${competitionId} Season ${season} ==`);

      // Make sure FK target exists first
      await ensureCompetitionRow({
        ksi_competition_id: competitionId,
        season_year: season,
        name: `U-19 ${GENDER} (${competitionId})`,
      });

      let matchIds: string[] = [];
      try {
        matchIds = await getAllMatchIdsForCompetitionAndSeason(competitionId, season);
      } catch (e: any) {
        console.error("Failed to list matches:", e.message);
        continue;
      }

      console.log(`Found ${matchIds.length} match ids`);
      if (matchIds.length === 0) continue;

      for (const matchId of matchIds) {
        try {
          const parsed = await parseLineups(matchId);

          await upsertMatchAndLineups({
            matchId,
            competitionId,
            seasonYear: season,
            lineups: parsed,
          });

          console.log(`✓ ${matchId}`);
          await sleep(250);
        } catch (e: any) {
          console.error(`✗ ${matchId}: ${e.message}`);
          await sleep(250);
        }
      }
    }
  }

  console.log("\nDone.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});