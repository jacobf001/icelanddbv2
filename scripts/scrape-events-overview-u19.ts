// scripts/scrape-events-overview-u19.ts
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
const replace = process.argv.includes("--replace");
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
    headers: { "user-agent": "Mozilla/5.0 (icelanddbv2; overview-events-u19)" },
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

function debugPrint(mid: string, events: EventRow[]) {
  console.log(`  ---- parsed event rows for match ${mid} (${events.length}) ----`);
  for (const e of events) {
    const m = `${e.minute}${e.stoppage ? `+${e.stoppage}` : ""}`;
    const p1 = e.ksi_player_id ?? "-";
    const on = e.sub_on_ksi_player_id ?? "-";
    const off = e.sub_off_ksi_player_id ?? "-";
    console.log(
      `  idx=${String(e.event_idx).padStart(2)} min=${String(m).padStart(4)} type=${String(e.event_type).padEnd(
        12,
      )} team=${e.ksi_team_id ?? "-"} p=${p1} on=${on} off=${off} | ${e.raw?.text ?? ""}`,
    );
  }
}

/**
 * Match banner contains exactly two team links in DOM order:
 * left = home, right = away.
 */
function extractTeamIdsFromOverview(c: cheerio.CheerioAPI): { home: string | null; away: string | null } {
  const $links = c(".match-banner a[href*='/oll-mot/mot/lid?id=']");
  if ($links.length >= 2) {
    const homeHref = String(c($links.get(0)).attr("href") ?? "");
    const awayHref = String(c($links.get(1)).attr("href") ?? "");
    return { home: getParamId(homeHref, "id"), away: getParamId(awayHref, "id") };
  }

  const $fallback = c("a[href*='/oll-mot/mot/lid?id=']");
  if ($fallback.length >= 2) {
    const homeHref = String(c($fallback.get(0)).attr("href") ?? "");
    const awayHref = String(c($fallback.get(1)).attr("href") ?? "");
    return { home: getParamId(homeHref, "id"), away: getParamId(awayHref, "id") };
  }

  return { home: null, away: null };
}

function extractTeamNamesFromBanner(c: cheerio.CheerioAPI): { homeName: string | null; awayName: string | null } {
  const $links = c(".match-banner a[href*='/oll-mot/mot/lid?id=']");
  if ($links.length >= 2) {
    const homeName = clean(c($links.get(0)).text()) || null;
    const awayName = clean(c($links.get(1)).text()) || null;
    return { homeName, awayName };
  }
  return { homeName: null, awayName: null };
}

async function ensureTeamExists(teamId: string | null, name: string | null) {
  if (!teamId) return;
  if (dry) return;

  // if you have a richer teams schema, add more fields here
  const { error } = await supabase.from("teams").upsert(
    {
      ksi_team_id: String(teamId),
      name: name ?? null,
    },
    { onConflict: "ksi_team_id" },
  );

  if (error) {
    // don't hard-fail events scraping on a team upsert edge case
    console.warn(`  ⚠️ teams upsert failed for ${teamId}: ${error.message}`);
  }
}

async function backfillMatchTeams(matchId: string, homeId: string | null, awayId: string | null) {
  if (!backfillMatches || dry) return;

  // update one side at a time to avoid FK failing the whole update
  if (homeId) {
    const { error } = await supabase.from("matches").update({ home_team_ksi_id: homeId }).eq("ksi_match_id", matchId);
    if (error) console.warn(`  ⚠️ match home backfill failed (${matchId}): ${error.message}`);
  }
  if (awayId) {
    const { error } = await supabase.from("matches").update({ away_team_ksi_id: awayId }).eq("ksi_match_id", matchId);
    if (error) console.warn(`  ⚠️ match away backfill failed (${matchId}): ${error.message}`);
  }
}

// ---------- U19: fetch competitions then matches ----------
type Competition = {
  ksi_competition_id: string;
  season_year: number;
  name: string;
  gender: string;
  category: string;
  tier: number | null;
};

async function fetchU19Competitions(fromYear: number, toYear: number) {
  const pageSize = 1000;
  let from = 0;
  const all: Competition[] = [];

  while (true) {
    const { data, error } = await supabase
      .from("competitions")
      .select("ksi_competition_id, season_year, name, gender, category, tier")
      .eq("gender", "Male")
      .eq("category", "U-19")
      .gte("season_year", fromYear)
      .lte("season_year", toYear)
      .order("season_year", { ascending: true })
      .order("tier", { ascending: true, nullsFirst: true })
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

  const byId = new Map<string, any>();
  for (const r of out) byId.set(String(r.ksi_match_id), r);
  return [...byId.values()].sort((a, b) => String(a.ksi_match_id).localeCompare(String(b.ksi_match_id)));
}

// ---------- Types ----------
type EventRow = {
  ksi_match_id: string;
  event_idx: number;
  minute: number;
  stoppage: number | null;
  event_type: string;
  ksi_team_id: string | null;

  ksi_player_id: string | null;
  player_name: string | null;

  sub_on_ksi_player_id: string | null;
  sub_off_ksi_player_id: string | null;
  sub_on_name: string | null;
  sub_off_name: string | null;

  raw: any;
};

// ---------- Find Atburðir container ----------
function findAtburdirGrid(c: cheerio.CheerioAPI): cheerio.Cheerio<any> | null {
  const $label = c("span")
    .filter((_, el) => clean(c(el).text()).toLowerCase() === "atburðir")
    .first();

  if ($label.length) {
    const $grid = $label
      .closest("div")
      .nextAll("div")
      .filter((_, el) => {
        const cls = clean(c(el).attr("class"));
        return cls.includes("grid") && cls.includes("grid-cols-[1fr_auto_1fr]");
      })
      .first();

    if ($grid.length) return $grid;
  }

  const $fallback = c("div")
    .filter((_, el) => {
      const cls = clean(c(el).attr("class"));
      return cls.includes("grid") && cls.includes("grid-cols-[1fr_auto_1fr]");
    })
    .first();

  return $fallback.length ? $fallback : null;
}

// ---------- Minute + stoppage ----------
function parseMinuteFromRow(c: cheerio.CheerioAPI, $row: cheerio.Cheerio<any>) {
  const txt = clean($row.text());
  const m = txt.match(/(\d{1,3})(?:\s*’\s*|\s*’)?/);
  const minute = m ? Number(m[1]) : NaN;
  if (!Number.isFinite(minute)) return { minute: null as number | null, stoppage: null as number | null };

  const $stop = $row.find("span.text-\\[10rem\\]").first();
  const stoppage = $stop.length && /^\d{1,2}$/.test(clean($stop.text())) ? Number(clean($stop.text())) : null;

  return { minute, stoppage };
}

// ---------- Event type ----------
function detectEventType(c: cheerio.CheerioAPI, $event: cheerio.Cheerio<any>, playerCount: number): string {
  if ($event.find("div.bg-\\[\\#FAC83C\\], div[style*='FAC83C']").length) return "yellow";

  const svgHtml = clean(
    $event
      .find("svg")
      .toArray()
      .map((n) => clean(c.html(n) ?? ""))
      .join(" "),
  );
  if (/#1A7941/i.test(svgHtml) && /#DD3636/i.test(svgHtml)) return "substitution";

  if (playerCount === 1 && $event.find("svg").length) return "goal";

  return "unknown";
}

// ---------- Parse events ----------
function parseOverviewEventsFromHtml(
  c: cheerio.CheerioAPI,
  ksiMatchId: string,
  homeTeamId: string | null,
  awayTeamId: string | null,
  playerToTeam: Map<string, string>,
): EventRow[] {
  const $grid = findAtburdirGrid(c);
  if (!$grid) return [];

  const $events = $grid.find("div.match-event[data-event-id]").toArray();
  const out: EventRow[] = [];
  const seen = new Set<string>();

  for (const ev of $events) {
    const $ev = c(ev);
    const $row = $ev.closest("div.col-span-3");
    if (!$row.length) continue;

    // IMPORTANT: on these pages flex-row-reverse corresponds to the RIGHT column (away)
    const rowCls = clean($row.attr("class"));
    const isAwayColumn = rowCls.includes("flex-row-reverse");
    const columnTeamId = isAwayColumn ? awayTeamId : homeTeamId;

    const { minute, stoppage } = parseMinuteFromRow(c, $row);
    if (minute == null) continue;

    const playerLinks = $ev.find("a[href^='/leikmenn/leikmadur?id=']").toArray();
    const playerCount = playerLinks.length;

    const ids = playerLinks.map((a) => getParamId(c(a).attr("href"), "id"));
    const names = playerLinks.map((a) => clean(c(a).text()));

    const eventType = detectEventType(c, $ev, playerCount);

    const teamId =
      (ids[0] && playerToTeam.get(String(ids[0]))) ||
      (ids[1] && playerToTeam.get(String(ids[1]))) ||
      columnTeamId ||
      null;

    let subOnId: string | null = null;
    let subOffId: string | null = null;
    let subOnName: string | null = null;
    let subOffName: string | null = null;

    if (eventType === "substitution") {
      const $green = $ev.find("a.text-\\[\\#1A7941\\]").first();
      const $red = $ev.find("a.text-\\[\\#D80707\\]").first();

      if ($green.length) {
        subOnId = getParamId($green.attr("href"), "id");
        subOnName = clean($green.text()) || null;
      }
      if ($red.length) {
        subOffId = getParamId($red.attr("href"), "id");
        subOffName = clean($red.text()) || null;
      }

      if (!subOnId && ids[0]) {
        subOnId = ids[0];
        subOnName = names[0] || null;
      }
      if (!subOffId && ids[1]) {
        subOffId = ids[1];
        subOffName = names[1] || null;
      }
    }

    const singlePlayerId = eventType !== "substitution" ? (ids[0] ?? null) : null;
    const singlePlayerName = eventType !== "substitution" ? (names[0] ?? null) : null;

    const rawText = clean($row.text());

    const key = [minute, stoppage ?? "", eventType, teamId ?? "", singlePlayerId ?? "", subOnId ?? "", subOffId ?? ""].join("|");
    if (seen.has(key)) continue;
    seen.add(key);

    out.push({
      ksi_match_id: ksiMatchId,
      event_idx: 0,
      minute,
      stoppage,
      event_type: eventType,
      ksi_team_id: teamId,

      ksi_player_id: singlePlayerId,
      player_name: singlePlayerName,

      sub_on_ksi_player_id: eventType === "substitution" ? subOnId : null,
      sub_off_ksi_player_id: eventType === "substitution" ? subOffId : null,
      sub_on_name: eventType === "substitution" ? subOnName : null,
      sub_off_name: eventType === "substitution" ? subOffName : null,

      raw: {
        text: rawText,
        event_html: clean(c.html($ev) ?? ""),
        row_html: clean(c.html($row) ?? ""),
      },
    });
  }

  out.sort((a, b) => {
    if (a.minute !== b.minute) return a.minute - b.minute;
    if ((a.stoppage ?? 0) !== (b.stoppage ?? 0)) return (a.stoppage ?? 0) - (b.stoppage ?? 0);
    if ((a.ksi_team_id ?? "") !== (b.ksi_team_id ?? "")) return (a.ksi_team_id ?? "").localeCompare(b.ksi_team_id ?? "");
    return (a.event_type ?? "").localeCompare(b.event_type ?? "");
  });

  out.forEach((e, i) => (e.event_idx = i));
  return out;
}

// ---------- Only upsert columns that exist ----------
async function getMatchEventsColumns(): Promise<Set<string>> {
  const { data, error } = await supabase.from("match_events").select("*").limit(1);
  if (error) {
    return new Set([
      "ksi_match_id",
      "event_idx",
      "minute",
      "stoppage",
      "event_type",
      "ksi_team_id",
      "ksi_player_id",
      "player_name",
      "sub_on_ksi_player_id",
      "sub_off_ksi_player_id",
      "sub_on_name",
      "sub_off_name",
      "raw",
    ]);
  }
  const first = (data ?? [])[0] ?? {};
  return new Set(Object.keys(first));
}

function pickAllowed(row: any, allowed: Set<string>) {
  const out: any = {};
  for (const k of Object.keys(row)) if (allowed.has(k)) out[k] = row[k];
  return out;
}

// ---------- Main ----------
async function main() {
  console.log(`Scrape OVERVIEW events (U-19) ${fromYear}..${toYear}`);
  console.log(
    `Dry: ${dry ? "YES" : "NO"} | sleep=${sleepMs}ms | limit=${limit || "none"} | debug=${debug ? "YES" : "NO"} | replace=${
      replace ? "YES" : "NO"
    } | backfillMatches=${backfillMatches ? "YES" : "NO"}`,
  );

  const comps = await fetchU19Competitions(fromYear, toYear);
  const compIds = comps.map((c) => String(c.ksi_competition_id));
  console.log(`U-19 competitions in range: ${comps.length}`);

  const matches = await fetchMatchesForCompetitionIds(compIds, fromYear, toYear);
  const target = limit && limit > 0 ? matches.slice(0, limit) : matches;
  console.log(`Matches in range: ${matches.length} | processing: ${target.length}`);

  const allowedCols = await getMatchEventsColumns();

  let ok = 0;
  let fail = 0;

  for (let i = 0; i < target.length; i++) {
    const m = target[i];
    const mid = String(m.ksi_match_id);
    const url = overviewUrl(mid);

    console.log(`\n[#${i + 1}/${target.length}] match ${mid}`);

    try {
      // player->team mapping (optional, if you later have lineups)
      const { data: lineupRows, error: lErr } = await supabase
        .from("match_lineups")
        .select("ksi_player_id, ksi_team_id")
        .eq("ksi_match_id", mid);

      if (lErr) throw new Error(`match_lineups select failed: ${lErr.message}`);

      const playerToTeam = new Map<string, string>();
      for (const r of lineupRows ?? []) {
        if (r.ksi_player_id && r.ksi_team_id) playerToTeam.set(String(r.ksi_player_id), String(r.ksi_team_id));
      }

      let homeTeamId = m.home_team_ksi_id ? String(m.home_team_ksi_id) : null;
      let awayTeamId = m.away_team_ksi_id ? String(m.away_team_ksi_id) : null;

      const html = await fetchHtml(url);
      const c = cheerio.load(html);

      if (!homeTeamId || !awayTeamId) {
        const inferred = extractTeamIdsFromOverview(c);
        const names = extractTeamNamesFromBanner(c);

        homeTeamId = homeTeamId ?? inferred.home;
        awayTeamId = awayTeamId ?? inferred.away;

        if (debug) console.log(`  inferred teams: home=${homeTeamId ?? "-"} away=${awayTeamId ?? "-"}`);

        // ensure both teams exist before any backfill to avoid FK failure
        await ensureTeamExists(homeTeamId, names.homeName);
        await ensureTeamExists(awayTeamId, names.awayName);

        await backfillMatchTeams(mid, homeTeamId, awayTeamId);
      }

      const events = parseOverviewEventsFromHtml(c, mid, homeTeamId, awayTeamId, playerToTeam);
      console.log(`  parsed events: ${events.length}`);
      debugPrint(mid, events);

      if (dry) {
        ok++;
        continue;
      }

      if (replace) {
        const { error: delErr } = await supabase.from("match_events").delete().eq("ksi_match_id", mid);
        if (delErr) throw new Error(`match_events delete failed: ${delErr.message}`);
      }

      if (events.length) {
        const payload = events.map((e) => pickAllowed(e, allowedCols));
        const { error: eErr } = await supabase.from("match_events").upsert(payload, {
          onConflict: "ksi_match_id,event_idx",
        });
        if (eErr) throw new Error(`match_events upsert failed: ${eErr.message}`);
      }

      console.log(`  ✅ saved events=${events.length}`);
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