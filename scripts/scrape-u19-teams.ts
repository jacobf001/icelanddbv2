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
const sleepMs = Number(arg("--sleep") ?? "200");
const limit = Number(arg("--limit") ?? "0"); // 0 = unlimited
const dry = process.argv.includes("--dry");
const debug = process.argv.includes("--debug");
const backfillMatches = process.argv.includes("--backfillMatches");

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
    headers: { "user-agent": "Mozilla/5.0 (icelanddbv2; u19-teams-scraper)" },
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

/**
 * Extract home/away team ids + names from the match banner.
 * Banner DOM order is: left team (home), right team (away).
 */
function extractTeamsFromBanner(c: cheerio.CheerioAPI): {
  home_id: string | null;
  home_name: string | null;
  away_id: string | null;
  away_name: string | null;
} {
  const $links = c(".match-banner a[href*='/oll-mot/mot/lid?id=']");
  if ($links.length < 2) return { home_id: null, home_name: null, away_id: null, away_name: null };

  const $home = c($links.get(0));
  const $away = c($links.get(1));

  const homeHref = String($home.attr("href") ?? "");
  const awayHref = String($away.attr("href") ?? "");

  const home_id = getParamId(homeHref, "id");
  const away_id = getParamId(awayHref, "id");

  // name is typically in the last span in the link
  const home_name = clean($home.find("span").last().text()) || clean($home.text()) || null;
  const away_name = clean($away.find("span").last().text()) || clean($away.text()) || null;

  return { home_id, home_name, away_id, away_name };
}

// ---------- DB fetchers ----------
type Competition = { ksi_competition_id: string; season_year: number; name: string; category: string; gender: string };

async function fetchU19Competitions(fromYear: number, toYear: number): Promise<Competition[]> {
  const pageSize = 1000;
  let from = 0;
  const all: Competition[] = [];

  while (true) {
    const { data, error } = await supabase
      .from("competitions")
      .select("ksi_competition_id, season_year, name, category, gender")
      .eq("gender", "Male")
      .eq("category", "U-19")
      .gte("season_year", fromYear)
      .lte("season_year", toYear)
      .order("season_year", { ascending: true })
      .order("ksi_competition_id", { ascending: true })
      .range(from, from + pageSize - 1);

    if (error) throw new Error(error.message);

    const batch = (data ?? []) as Competition[];
    all.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }

  return all;
}

async function fetchMatchesForCompetitionIds(compIds: string[], fromYear: number, toYear: number) {
  const pageSize = 1000;
  const out: any[] = [];

  const idChunkSize = 200;
  for (let ci = 0; ci < compIds.length; ci += idChunkSize) {
    const chunkIds = compIds.slice(ci, ci + idChunkSize);

    let from = 0;
    while (true) {
      const { data, error } = await supabase
        .from("matches")
        .select("ksi_match_id, season_year, ksi_competition_id, home_team_ksi_id, away_team_ksi_id")
        .gte("season_year", fromYear)
        .lte("season_year", toYear)
        .in("ksi_competition_id", chunkIds)
        .order("ksi_match_id", { ascending: true })
        .range(from, from + pageSize - 1);

      if (error) throw new Error(error.message);

      const batch = data ?? [];
      out.push(...batch);

      if (batch.length < pageSize) break;
      from += pageSize;
    }
  }

  // de-dup
  const byId = new Map<string, any>();
  for (const r of out) byId.set(String(r.ksi_match_id), r);
  return [...byId.values()].sort((a, b) => String(a.ksi_match_id).localeCompare(String(b.ksi_match_id)));
}

type TeamUpsert = { ksi_team_id: string; name: string | null };

async function upsertTeams(rows: TeamUpsert[]) {
  if (!rows.length) return 0;

  // de-dup by id; keep first non-null name
  const byId = new Map<string, TeamUpsert>();
  for (const r of rows) {
    const id = String(r.ksi_team_id);
    const prev = byId.get(id);
    if (!prev) {
      byId.set(id, { ksi_team_id: id, name: r.name ?? null });
    } else if (!prev.name && r.name) {
      byId.set(id, { ksi_team_id: id, name: r.name });
    }
  }

  const payload = [...byId.values()];

  if (dry) {
    if (debug) console.log("DRY upsert teams sample:", payload.slice(0, 10));
    return payload.length;
  }

  const { error } = await supabase.from("teams").upsert(payload, { onConflict: "ksi_team_id" });
  if (error) throw new Error(`teams upsert failed: ${error.message}`);

  return payload.length;
}

async function backfillMatchTeams(matchId: string, homeId: string, awayId: string) {
  if (dry) return;

  const { error } = await supabase
    .from("matches")
    .update({ home_team_ksi_id: homeId, away_team_ksi_id: awayId })
    .eq("ksi_match_id", matchId);

  if (error) throw new Error(`matches backfill failed: ${error.message}`);
}

// ---------- Main ----------
async function main() {
  console.log(`U-19 TEAMS scrape ${fromYear}..${toYear}`);
  console.log(`Dry=${dry ? "YES" : "NO"} debug=${debug ? "YES" : "NO"} backfillMatches=${backfillMatches ? "YES" : "NO"}`);
  console.log(`sleep=${sleepMs}ms limit=${limit || "none"}`);

  const comps = await fetchU19Competitions(fromYear, toYear);
  const compIds = comps.map((c) => String(c.ksi_competition_id));

  console.log(`U-19 competitions: ${comps.length}`);
  if (debug) console.log("Sample comps:", comps.slice(0, 10).map((c) => `${c.season_year} ${c.ksi_competition_id} ${c.name}`));

  const matches = await fetchMatchesForCompetitionIds(compIds, fromYear, toYear);
  const target = limit && limit > 0 ? matches.slice(0, limit) : matches;

  console.log(`Matches loaded: ${matches.length} | processing: ${target.length}`);

  let ok = 0;
  let fail = 0;
  let totalTeamsUpserted = 0;
  let totalMatchesBackfilled = 0;

  // batch teams in memory so we don't hammer DB
  const teamBuffer: TeamUpsert[] = [];
  const flushEvery = 200;

  for (let i = 0; i < target.length; i++) {
    const m = target[i];
    const mid = String(m.ksi_match_id);
    const url = overviewUrl(mid);

    if (debug) console.log(`\n[#${i + 1}/${target.length}] ${mid} ${url}`);

    try {
      const html = await fetchHtml(url);
      const c = cheerio.load(html);

      const banner = extractTeamsFromBanner(c);

      if (!banner.home_id || !banner.away_id) {
        throw new Error(`Could not extract both team ids (home=${banner.home_id} away=${banner.away_id})`);
      }

      if (debug) {
        console.log(
          `  home=${banner.home_id} "${banner.home_name ?? ""}" | away=${banner.away_id} "${banner.away_name ?? ""}"`,
        );
      }

      teamBuffer.push({ ksi_team_id: banner.home_id, name: banner.home_name ?? null });
      teamBuffer.push({ ksi_team_id: banner.away_id, name: banner.away_name ?? null });

      // flush teams periodically
      if (teamBuffer.length >= flushEvery) {
        const n = await upsertTeams(teamBuffer.splice(0, teamBuffer.length));
        totalTeamsUpserted += n;
        if (debug) console.log(`  flushed teams: ${n}`);
      }

      // optional: after teams exist, backfill matches
      if (backfillMatches) {
        // ensure current buffer is flushed so FK passes
        if (teamBuffer.length) {
          const n = await upsertTeams(teamBuffer.splice(0, teamBuffer.length));
          totalTeamsUpserted += n;
        }

        await backfillMatchTeams(mid, banner.home_id, banner.away_id);
        totalMatchesBackfilled++;
      }

      ok++;
    } catch (e: any) {
      fail++;
      console.error(`  ❌ match ${mid}: ${e?.message ?? String(e)}`);
    }

    if (sleepMs > 0) await sleep(sleepMs);
  }

  // final flush
  if (teamBuffer.length) {
    const n = await upsertTeams(teamBuffer);
    totalTeamsUpserted += n;
  }

  console.log(`\nDone. OK=${ok} FAIL=${fail}`);
  console.log(`Teams upserted (counted by batch payload): ${totalTeamsUpserted}`);
  console.log(`Matches backfilled: ${totalMatchesBackfilled}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});