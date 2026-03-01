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

// -------------------- CLI ARGS --------------------
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

// -------------------- HELPERS --------------------
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
    headers: { "user-agent": "Mozilla/5.0 (icelanddbv2; u19-match-details)" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.text();
}

function getParamId(href: string, key = "id"): string | null {
  const m = href.match(new RegExp(`[?&]${key}=(\\d+)`));
  return m ? m[1] : null;
}

/**
 * Match header has exactly two team links inside `.match-banner`:
 * first = home (left), second = away (right)
 */
function extractTeamIdsFromOverview(c: cheerio.CheerioAPI): { home: string | null; away: string | null } {
  const $links = c(".match-banner a[href*='/oll-mot/mot/lid?id=']");
  if ($links.length >= 2) {
    const homeHref = String(c($links.get(0)).attr("href") ?? "");
    const awayHref = String(c($links.get(1)).attr("href") ?? "");
    return { home: getParamId(homeHref, "id"), away: getParamId(awayHref, "id") };
  }

  // fallback: first two anywhere
  const $fallback = c("a[href*='/oll-mot/mot/lid?id=']");
  if ($fallback.length >= 2) {
    const homeHref = String(c($fallback.get(0)).attr("href") ?? "");
    const awayHref = String(c($fallback.get(1)).attr("href") ?? "");
    return { home: getParamId(homeHref, "id"), away: getParamId(awayHref, "id") };
  }

  return { home: null, away: null };
}

function extractScoreFromOverview(c: cheerio.CheerioAPI): { home: number | null; away: number | null } {
  // e.g. <h1 class="headline-1 whitespace-nowrap">3 - 2</h1>
  const txt = clean(c(".match-banner h1.headline-1").first().text());
  const m = txt.match(/(\d+)\s*-\s*(\d+)/);
  if (!m) return { home: null, away: null };
  return { home: Number(m[1]), away: Number(m[2]) };
}

function extractKickoffAndVenue(c: cheerio.CheerioAPI): { kickoff_text: string | null; venue: string | null } {
  // In the match banner, the date/time line contains HH:MM
  // The venue line is usually the next body-5 line and does NOT contain HH:MM.
  const lines: string[] = [];

  c(".match-banner span.body-5").each((_, el) => {
    const t = clean(c(el).text());
    if (!t) return;
    lines.push(t);
  });

  // kickoff = first line in banner that contains a clock time
  const kickoffIdx = lines.findIndex((t) => /\b\d{1,2}:\d{2}\b/.test(t));
  const kickoff_text = kickoffIdx >= 0 ? lines[kickoffIdx] : null;

  // venue = first line AFTER kickoff that does NOT contain a clock time
  let venue: string | null = null;
  if (kickoffIdx >= 0) {
    for (let i = kickoffIdx + 1; i < lines.length; i++) {
      const t = lines[i];
      if (!/\b\d{1,2}:\d{2}\b/.test(t)) {
        venue = t;
        break;
      }
    }
  } else {
    // fallback: any short-ish line without time
    venue = lines.find((t) => !/\b\d{1,2}:\d{2}\b/.test(t) && t.length <= 50) ?? null;
  }

  return { kickoff_text, venue };
}

// -------------------- DATA FETCH --------------------
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
        .select("ksi_match_id, season_year, ksi_competition_id, home_team_ksi_id, away_team_ksi_id, home_score, away_score, kickoff_at, venue")
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

// -------------------- MAIN --------------------
async function main() {
  console.log(`Backfill U19 match details (teams + score) ${fromYear}..${toYear}`);
  console.log(`Dry: ${dry ? "YES" : "NO"} | sleep=${sleepMs}ms | limit=${limit || "none"} | debug=${debug ? "YES" : "NO"}`);

  const compIds = await fetchU19CompetitionIds(fromYear, toYear);
  console.log(`U19 competitions in range: ${compIds.length}`);

  const matches = await fetchU19Matches(fromYear, toYear, compIds);

  // Only process matches missing something we need for league tables
  const todo = matches.filter((m) => m.home_score == null || m.away_score == null || !m.home_team_ksi_id || !m.away_team_ksi_id);
  const target = limit && limit > 0 ? todo.slice(0, limit) : todo;

  console.log(`Matches needing backfill: ${todo.length} | processing: ${target.length}`);

  let ok = 0;
  let fail = 0;
  let updated = 0;
  let skipped = 0;

  for (let i = 0; i < target.length; i++) {
    const m = target[i];
    const mid = String(m.ksi_match_id);
    const url = overviewUrl(mid);

    console.log(`\n[#${i + 1}/${target.length}] match ${mid}`);

    try {
      const html = await fetchHtml(url);
      const c = cheerio.load(html);

      const teamIds = extractTeamIdsFromOverview(c);
      const score = extractScoreFromOverview(c);
      const kv = extractKickoffAndVenue(c);

      if (debug) {
        console.log(`  url=${url}`);
        console.log(`  teams: home=${teamIds.home ?? "-"} away=${teamIds.away ?? "-"}`);
        console.log(`  score: ${score.home ?? "-"} - ${score.away ?? "-"}`);
        console.log(`  kickoff_text=${kv.kickoff_text ?? "-"} venue=${kv.venue ?? "-"}`);
      }

      // If the match hasn’t been played yet, score might be null; that’s fine.
      const patch: any = {};

      // only set if we found values
      if (teamIds.home) patch.home_team_ksi_id = teamIds.home;
      if (teamIds.away) patch.away_team_ksi_id = teamIds.away;
      if (score.home != null) patch.home_score = score.home;
      if (score.away != null) patch.away_score = score.away;

      // optional fill-ins if your schema has these columns (you do)
      if (!m.venue && kv.venue) patch.venue = kv.venue;

      // DON’T try to parse kickoff_at into ISO unless you want to handle Icelandic months.
      // Leave kickoff_at for a dedicated parser later.
      // if (!m.kickoff_at && kv.kickoff_text) patch.kickoff_at = ...

      const hasAny = Object.keys(patch).length > 0;

      if (!hasAny) {
        console.log(`  ⚠️ nothing parsed to update`);
        skipped++;
      } else if (dry) {
        console.log(`  DRY: would update`, patch);
        updated++;
      } else {
        const { error: upErr } = await supabase.from("matches").update(patch).eq("ksi_match_id", mid);
        if (upErr) throw new Error(upErr.message);
        console.log(`  ✅ updated: ${Object.keys(patch).join(", ")}`);
        updated++;
      }

      ok++;
    } catch (e: any) {
      fail++;
      console.error(`  ❌ ${e?.message ?? String(e)}`);
    }

    if (sleepMs > 0) await sleep(sleepMs);
  }

  console.log(`\nDone. OK=${ok} FAIL=${fail}`);
  console.log(`Updated=${updated} Skipped=${skipped}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});