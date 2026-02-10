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

// ---------- Helpers ----------
function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
function clean(s: string) {
  return s.replace(/\s+/g, " ").trim();
}

function matchOverviewUrl(ksiMatchId: string) {
  const u = new URL("https://www.ksi.is/leikir-og-urslit/felagslid/leikur");
  u.searchParams.set("id", ksiMatchId);
  u.searchParams.set("banner-tab", "overview");
  return u.toString();
}

async function fetchHtml(url: string): Promise<string> {
  const res = await fetch(url, {
    headers: { "user-agent": "Mozilla/5.0 (icelanddbv2; match-overview-scraper)" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.text();
}

function parseScoreMaybe(text: string): { home: number | null; away: number | null } {
  const t = clean(text);
  const m = t.match(/(\d+)\s*[-:–]\s*(\d+)/);
  if (!m) return { home: null, away: null };
  return { home: Number(m[1]), away: Number(m[2]) };
}

function extractScore($: cheerio.CheerioAPI): { home_score: number | null; away_score: number | null } {
  const candidates: string[] = [];
  $("*").each((_, el) => {
    const txt = clean($(el).text());
    if (!txt) return;
    if (txt.length > 12) return;
    if (!/[0-9]\s*[-:–]\s*[0-9]/.test(txt)) return;
    candidates.push(txt);
  });

  for (const c of candidates) {
    const s = parseScoreMaybe(c);
    if (s.home !== null && s.away !== null) return { home_score: s.home, away_score: s.away };
  }
  return { home_score: null, away_score: null };
}

function parseKsiIdFromHref(href: string): string | null {
  const m = href.match(/[?&]id=(\d+)/);
  return m ? m[1] : null;
}

function extractHomeAwayTeam($: cheerio.CheerioAPI): {
  homeId: string | null;
  awayId: string | null;
  homeName: string | null;
  awayName: string | null;
} {
  // On overview page, team links are usually /oll-mot/mot/lid?id=XXXX&competitionId=YYYY
  const teamLinks: Array<{ id: string; name: string }> = [];

  $("a[href*='/oll-mot/mot/lid?id=']").each((_, a) => {
    const href = String($(a).attr("href") ?? "");
    const id = parseKsiIdFromHref(href);
    if (!id) return;

    const name = clean($(a).text());
    if (!name) return;

    teamLinks.push({ id, name });
  });

  // Fallback: sometimes team links are elsewhere, still with id=XXXX and not player
  if (teamLinks.length < 2) {
    $("a[href*='id=']").each((_, a) => {
      const href = String($(a).attr("href") ?? "");
      if (href.includes("/leikmenn/") || href.includes("leikmadur")) return;
      if (!href.includes("/oll-mot/mot/lid")) return;

      const id = parseKsiIdFromHref(href);
      if (!id) return;

      const name = clean($(a).text());
      if (!name) return;

      teamLinks.push({ id, name });
    });
  }

  // De-dupe by id while preserving order
  const uniq: Array<{ id: string; name: string }> = [];
  const seen = new Set<string>();
  for (const t of teamLinks) {
    if (seen.has(t.id)) continue;
    seen.add(t.id);
    uniq.push(t);
  }

  return {
    homeId: uniq[0]?.id ?? null,
    awayId: uniq[1]?.id ?? null,
    homeName: uniq[0]?.name ?? null,
    awayName: uniq[1]?.name ?? null,
  };
}

function extractVenueAndKickoff(html: string): { venue: string | null; kickoff_at: string | null } {
  const $ = cheerio.load(html);

  // Venue appears as “…völlurinn” etc; simplest: locate text near “Völlur”
  const pageText = clean($("body").text());

  // Example: “Fös 27. júní 2025 19:15 JÁVERK-völlurinn”
  // We’ll try to extract ISO from “27. júní 2025 19:15”
  const monthMap: Record<string, string> = {
    janúar: "01",
    februar: "02",
    febrúar: "02",
    mars: "03",
    april: "04",
    apríl: "04",
    mai: "05",
    maí: "05",
    juni: "06",
    júní: "06",
    juli: "07",
    júlí: "07",
    agust: "08",
    ágúst: "08",
    september: "09",
    oktober: "10",
    nóvember: "11",
    november: "11",
    desember: "12",
  };

  // Date format: "27. júní 2025 19:15"
  const m = pageText.match(/(\d{1,2})\.\s*([A-Za-záðéíóúýþæöÁÐÉÍÓÚÝÞÆÖ]+)\s+(\d{4})\s+(\d{1,2}:\d{2})/);
  let kickoff_at: string | null = null;
  if (m) {
    const dd = String(m[1]).padStart(2, "0");
    const monKey = m[2].toLowerCase();
    const mm = monthMap[monKey] ?? null;
    const yyyy = m[3];
    const hhmm = m[4].padStart(5, "0");
    if (mm) kickoff_at = `${yyyy}-${mm}-${dd}T${hhmm}:00Z`;
  }

  // Venue: pick something ending in “völlur/völlurinn”
  let venue: string | null = null;
  const v = pageText.match(/([A-Z0-9ÁÐÉÍÓÚÝÞÆÖa-záðéíóúýþæö\-\s]{3,80}völlurinn|[A-Z0-9ÁÐÉÍÓÚÝÞÆÖa-záðéíóúýþæö\-\s]{3,80}völlur)/i);
  if (v) venue = clean(v[0]);

  return { venue, kickoff_at };
}

async function ensureTeamsExist(teams: Array<{ id: string; name: string | null }>) {
  if (teams.length === 0) return;

  // only keep ids
  const payload = teams
    .filter((t) => t.id)
    .map((t) => ({
      ksi_team_id: t.id,
      name: t.name ?? null,
    }));

  // Upsert: if exists, keep existing name unless we have a real one
  // (If you want: update name always, switch to upsert with "name" included)
  const { error } = await supabase.from("teams").upsert(payload, { onConflict: "ksi_team_id" });
  if (error) throw new Error(`teams upsert failed: ${error.message}`);
}

// ---------- Main ----------
async function main() {
  console.log(`Scrape match overviews ${fromYear}..${toYear}`);
  console.log(`Dry: ${dry ? "YES" : "NO"} | sleep=${sleepMs}ms | limit=${limit || "none"} | debug=${debug ? "YES" : "NO"}`);

  const { data, error } = await supabase
    .from("matches")
    .select(
      "ksi_match_id, season_year, kickoff_at, home_team_ksi_id, away_team_ksi_id, home_score, away_score"
    )
    .gte("season_year", fromYear)
    .lte("season_year", toYear);

  if (error) throw new Error(error.message);

  const all = (data ?? []) as any[];
  const todo = all.filter((m) => {
    return (
      !m.kickoff_at ||
      !m.home_team_ksi_id ||
      !m.away_team_ksi_id ||
      m.home_score === null ||
      m.away_score === null
    );
  });

  const target = limit && limit > 0 ? todo.slice(0, limit) : todo;
    const total = target.length;
    console.log(`Matches needing overview scrape: ${todo.length} | processing: ${target.length}`);

  let ok = 0;
  let fail = 0;

  for (let i = 0; i < target.length; i++) {
    const m = target[i];
    const mid = String(m.ksi_match_id);
    const url = matchOverviewUrl(mid);
    console.log(`\nmatch ${mid} -> ${url}`);
    console.log(`\n[${i + 1}/${total}] match ${mid} -> ${url}`);

    try {
      const html = await fetchHtml(url);
      const $ = cheerio.load(html);

      const { homeId, awayId, homeName, awayName } = extractHomeAwayTeam($);
      const { home_score, away_score } = extractScore($);
      const { kickoff_at, venue } = extractVenueAndKickoff(html);

      if (debug) {
        console.log(`  parsed: home=${homeId ?? "—"} (${homeName ?? "—"}) away=${awayId ?? "—"} (${awayName ?? "—"})`);
        console.log(`  parsed: score=${home_score ?? "—"}-${away_score ?? "—"}`);
        console.log(`  parsed: kickoff_at=${kickoff_at ?? "—"} venue=${venue ?? "—"}`);
      }

      // ✅ FK-safe: ensure teams exist before writing match foreign keys
      const teamsToUpsert: Array<{ id: string; name: string | null }> = [];
      if (homeId) teamsToUpsert.push({ id: homeId, name: homeName });
      if (awayId) teamsToUpsert.push({ id: awayId, name: awayName });

      const patch = {
        home_team_ksi_id: homeId,
        away_team_ksi_id: awayId,
        home_score,
        away_score,
        kickoff_at,
        venue,
        scraped_overview_at: new Date().toISOString(),
      };

      if (dry) {
        console.log("  DRY teams upsert:", teamsToUpsert);
        console.log("  DRY match patch:", { ksi_match_id: mid, ...patch });
      } else {
        if (teamsToUpsert.length) await ensureTeamsExist(teamsToUpsert);

        const { error: upErr } = await supabase.from("matches").update(patch).eq("ksi_match_id", mid);
        if (upErr) throw new Error(upErr.message);

        console.log("  ✅ updated");
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
