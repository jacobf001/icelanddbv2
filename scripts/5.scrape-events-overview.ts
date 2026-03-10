// scripts/scrape-events-overview.ts
import * as dotenv from "dotenv";
import path from "path";
import { createClient } from "@supabase/supabase-js";
import * as cheerio from "cheerio";
dotenv.config({ path: path.resolve(process.cwd(), "../.env"), override: true });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL) throw new Error("SUPABASE_URL is required");
if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY is required");

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

function arg(name: string): string | null {
  const i = process.argv.indexOf(name);
  if (i === -1) return null;
  return process.argv[i + 1] ?? null;
}

const fromYear = Number(arg("--from") ?? "2020");
const toYear = Number(arg("--to") ?? String(new Date().getFullYear()));
const sleepMs = Number(arg("--sleep") ?? "250");
const limit = Number(arg("--limit") ?? "0");
const dry = process.argv.includes("--dry");
const debug = process.argv.includes("--debug");
const replace = process.argv.includes("--replace");

function sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }
function clean(s: unknown) { return String(s ?? "").replace(/\s+/g, " ").trim(); }

function overviewUrl(ksiMatchId: string) {
  const u = new URL("https://www.ksi.is/leikir-og-urslit/felagslid/leikur");
  u.searchParams.set("id", ksiMatchId);
  u.searchParams.set("banner-tab", "overview");
  return u.toString();
}

async function fetchHtml(url: string): Promise<string> {
  const res = await fetch(url, {
    headers: { "user-agent": "Mozilla/5.0 (icelanddbv2; overview-events-scraper)" },
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

// Fetch all matches in range — all genders
async function fetchMatchesInRange(fromYear: number, toYear: number) {
  const pageSize = 1000;
  let from = 0;
  const all: any[] = [];

  while (true) {
    const { data, error } = await supabase
      .from("matches")
      .select("ksi_match_id, season_year, home_team_ksi_id, away_team_ksi_id")
      .gte("season_year", fromYear)
      .lte("season_year", toYear)
      .order("ksi_match_id", { ascending: true })
      .range(from, from + pageSize - 1);

    if (error) throw new Error(error.message);
    const batch = data ?? [];
    all.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }

  return all;
}

type EventRow = {
  ksi_match_id: string; event_idx: number; minute: number; stoppage: number | null;
  event_type: string; ksi_team_id: string | null; ksi_player_id: string | null;
  player_name: string | null; sub_on_ksi_player_id: string | null;
  sub_off_ksi_player_id: string | null; sub_on_name: string | null;
  sub_off_name: string | null; raw: any;
};

function findAtburdirGrid(c: cheerio.CheerioAPI): cheerio.Cheerio<any> | null {
  const $label = c("span")
    .filter((_, el) => clean(c(el).text()).toLowerCase() === "atburðir")
    .first();

  if ($label.length) {
    const $grid = $label.closest("div").nextAll("div")
      .filter((_, el) => {
        const cls = clean(c(el).attr("class"));
        return cls.includes("grid") && cls.includes("grid-cols-[1fr_auto_1fr]");
      }).first();
    if ($grid.length) return $grid;
  }

  const $fallback = c("div").filter((_, el) => {
    const cls = clean(c(el).attr("class"));
    return cls.includes("grid") && cls.includes("grid-cols-[1fr_auto_1fr]");
  }).first();

  return $fallback.length ? $fallback : null;
}

function parseMinuteFromRow(c: cheerio.CheerioAPI, $row: cheerio.Cheerio<any>) {
  const txt = clean($row.text());
  const m = txt.match(/(\d{1,3})(?:\s*'|\s*')?/);
  const minute = m ? Number(m[1]) : NaN;
  if (!Number.isFinite(minute)) return { minute: null as number | null, stoppage: null as number | null };
  const $stop = $row.find("span.text-\\[10rem\\]").first();
  const stoppage = $stop.length && /^\d{1,2}$/.test(clean($stop.text())) ? Number(clean($stop.text())) : null;
  return { minute, stoppage };
}

function detectEventType(c: cheerio.CheerioAPI, $event: cheerio.Cheerio<any>, playerCount: number): string {
  if ($event.find("div.bg-\\[\\#FAC83C\\], div[style*='FAC83C']").length) return "yellow";
  const svgHtml = clean($event.find("svg").toArray().map((n) => clean(c.html(n) ?? "")).join(" "));
  if (/#1A7941/i.test(svgHtml) && /#DD3636/i.test(svgHtml)) return "substitution";
  if (playerCount === 1 && $event.find("svg").length) return "goal";
  return "unknown";
}

function parseOverviewEventsFromHtml(c: cheerio.CheerioAPI, ksiMatchId: string,
  homeTeamId: string | null, awayTeamId: string | null, playerToTeam: Map<string, string>): EventRow[] {
  const $grid = findAtburdirGrid(c);
  if (!$grid) return [];

  const $events = $grid.find("div.match-event[data-event-id]").toArray();
  const out: EventRow[] = [];
  const seen = new Set<string>();

  for (const ev of $events) {
    const $ev = c(ev);
    const $row = $ev.closest("div.col-span-3");
    if (!$row.length) continue;

    const rowCls = clean($row.attr("class"));
    const isHomeSide = rowCls.includes("flex-row-reverse");
    const columnTeamId = isHomeSide ? homeTeamId : awayTeamId;

    const { minute, stoppage } = parseMinuteFromRow(c, $row);
    if (minute == null) continue;

    const playerLinks = $ev.find("a[href^='/leikmenn/leikmadur?id=']").toArray();
    const playerCount = playerLinks.length;
    const ids = playerLinks.map((a) => getParamId(c(a).attr("href"), "id"));
    const names = playerLinks.map((a) => clean(c(a).text()));
    const eventType = detectEventType(c, $ev, playerCount);

    const teamId = (ids[0] && playerToTeam.get(String(ids[0]))) ||
      (ids[1] && playerToTeam.get(String(ids[1]))) || columnTeamId || null;

    let subOnId: string | null = null, subOffId: string | null = null;
    let subOnName: string | null = null, subOffName: string | null = null;

    if (eventType === "substitution") {
      const $green = $ev.find("a.text-\\[\\#1A7941\\]").first();
      const $red = $ev.find("a.text-\\[\\#D80707\\]").first();
      if ($green.length) { subOnId = getParamId($green.attr("href"), "id"); subOnName = clean($green.text()) || null; }
      if ($red.length) { subOffId = getParamId($red.attr("href"), "id"); subOffName = clean($red.text()) || null; }
      if (!subOnId && ids[0]) { subOnId = ids[0]; subOnName = names[0] || null; }
      if (!subOffId && ids[1]) { subOffId = ids[1]; subOffName = names[1] || null; }
    }

    const singlePlayerId = eventType !== "substitution" ? (ids[0] ?? null) : null;
    const singlePlayerName = eventType !== "substitution" ? (names[0] ?? null) : null;
    const rawText = clean($row.text());

    const key = [minute, stoppage ?? "", eventType, teamId ?? "", singlePlayerId ?? "", subOnId ?? "", subOffId ?? ""].join("|");
    if (seen.has(key)) continue;
    seen.add(key);

    out.push({
      ksi_match_id: ksiMatchId, event_idx: 0, minute, stoppage, event_type: eventType,
      ksi_team_id: teamId, ksi_player_id: singlePlayerId, player_name: singlePlayerName,
      sub_on_ksi_player_id: eventType === "substitution" ? subOnId : null,
      sub_off_ksi_player_id: eventType === "substitution" ? subOffId : null,
      sub_on_name: eventType === "substitution" ? subOnName : null,
      sub_off_name: eventType === "substitution" ? subOffName : null,
      raw: { text: rawText, event_html: clean(c.html($ev) ?? ""), row_html: clean(c.html($row) ?? "") },
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

async function applySubMinutesToLineups(matchId: string, events: EventRow[]) {
  const { data: lineups, error } = await supabase.from("match_lineups")
    .select("id, ksi_player_id, minute_in, minute_out").eq("ksi_match_id", matchId);
  if (error) throw new Error(`fetch match_lineups failed: ${error.message}`);

  const byPlayer = new Map<string, { id: number; minute_in: number | null; minute_out: number | null }>();
  for (const r of lineups ?? []) {
    if (r.ksi_player_id) byPlayer.set(String(r.ksi_player_id), { id: r.id, minute_in: r.minute_in, minute_out: r.minute_out });
  }

  const updates: Array<{ id: number; minute_in?: number; minute_out?: number }> = [];
  for (const e of events) {
    if (e.event_type !== "substitution") continue;
    const onId = e.sub_on_ksi_player_id;
    const offId = e.sub_off_ksi_player_id;
    if (onId) { const rOn = byPlayer.get(onId); if (rOn && rOn.minute_in == null) updates.push({ id: rOn.id, minute_in: e.minute }); }
    if (offId) { const rOff = byPlayer.get(offId); if (rOff && rOff.minute_out == null) updates.push({ id: rOff.id, minute_out: e.minute }); }
  }

  if (!updates.length) return { updated: 0 };
  if (dry) { if (debug) console.log("  DRY: would update match_lineups minutes:", updates.slice(0, 50)); return { updated: updates.length }; }

  let ok = 0;
  for (const u of updates) {
    const { error: upErr } = await supabase.from("match_lineups").update({
      ...(u.minute_in !== undefined ? { minute_in: u.minute_in } : {}),
      ...(u.minute_out !== undefined ? { minute_out: u.minute_out } : {}),
    }).eq("id", u.id);
    if (upErr) throw new Error(`match_lineups update failed (id=${u.id}): ${upErr.message}`);
    ok++;
  }
  return { updated: ok };
}

async function getMatchEventsColumns(): Promise<Set<string>> {
  const { data, error } = await supabase.from("match_events").select("*").limit(1);
  if (error) return new Set(["ksi_match_id","event_idx","minute","stoppage","event_type","ksi_team_id","ksi_player_id","player_name","sub_on_ksi_player_id","sub_off_ksi_player_id","sub_on_name","sub_off_name","raw"]);
  const first = (data ?? [])[0] ?? {};
  return new Set(Object.keys(first));
}

function pickAllowed(row: any, allowed: Set<string>) {
  const out: any = {};
  for (const k of Object.keys(row)) { if (allowed.has(k)) out[k] = row[k]; }
  return out;
}

function debugPrint(mid: string, events: EventRow[]) {
  console.log(`  ---- parsed event rows for match ${mid} (${events.length}) ----`);
  for (const e of events) {
    const m = `${e.minute}${e.stoppage ? `+${e.stoppage}` : ""}`;
    console.log(`  idx=${String(e.event_idx).padStart(2)} min=${String(m).padStart(4)} type=${String(e.event_type).padEnd(12)} team=${e.ksi_team_id ?? "-"} p=${e.ksi_player_id ?? "-"} on=${e.sub_on_ksi_player_id ?? "-"} off=${e.sub_off_ksi_player_id ?? "-"} | ${e.raw?.text ?? ""}`);
  }
}

async function main() {
  console.log(`Scrape OVERVIEW events ${fromYear}..${toYear}`);
  console.log(`Dry: ${dry ? "YES" : "NO"} | sleep=${sleepMs}ms | limit=${limit || "none"} | debug=${debug ? "YES" : "NO"} | replace=${replace ? "YES" : "NO"}`);

  const matches = await fetchMatchesInRange(fromYear, toYear);
  const target = limit && limit > 0 ? matches.slice(0, limit) : matches;
  console.log(`Matches in range: ${matches.length} | processing: ${target.length}`);

  const allowedCols = await getMatchEventsColumns();
  let ok = 0, fail = 0;

  for (let i = 0; i < target.length; i++) {
    const m = target[i];
    const mid = String(m.ksi_match_id);
    const url = overviewUrl(mid);
    const homeTeamId = m.home_team_ksi_id ? String(m.home_team_ksi_id) : null;
    const awayTeamId = m.away_team_ksi_id ? String(m.away_team_ksi_id) : null;
    console.log(`\n[#${i + 1}/${target.length}] match ${mid} -> ${url}`);

    try {
      const { data: lineupRows, error: lErr } = await supabase.from("match_lineups")
        .select("ksi_player_id, ksi_team_id").eq("ksi_match_id", mid);
      if (lErr) throw new Error(`match_lineups select failed: ${lErr.message}`);

      const playerToTeam = new Map<string, string>();
      for (const r of lineupRows ?? []) {
        if (r.ksi_player_id && r.ksi_team_id) playerToTeam.set(String(r.ksi_player_id), String(r.ksi_team_id));
      }

      const html = await fetchHtml(url);
      const c = cheerio.load(html);
      const events = parseOverviewEventsFromHtml(c, mid, homeTeamId, awayTeamId, playerToTeam);

      console.log(`  parsed events: ${events.length}`);
      if (debug) debugPrint(mid, events);

      if (dry) {
        console.log(`  DRY: would save events=${events.length}`);
      } else {
        if (replace) {
          const { error: delErr } = await supabase.from("match_events").delete().eq("ksi_match_id", mid);
          if (delErr) throw new Error(`match_events delete failed: ${delErr.message}`);
        }
        if (events.length) {
          const payload = events.map((e) => pickAllowed(e, allowedCols));
          const { error: eErr } = await supabase.from("match_events").upsert(payload, { onConflict: "ksi_match_id,event_idx" });
          if (eErr) throw new Error(`match_events upsert failed: ${eErr.message}`);
        }
        console.log(`  ✅ saved events=${events.length}`);
      }

      const subRes = await applySubMinutesToLineups(mid, events);
      console.log(`  ${dry ? "DRY:" : "✅"} lineup minute updates from subs: ${subRes.updated}`);
      ok++;
    } catch (e: any) {
      fail++;
      console.error(`  ❌ ${e?.message ?? String(e)}`);
    }

    if (sleepMs > 0) await sleep(sleepMs);
  }

  console.log(`\nDone. OK=${ok} FAIL=${fail}`);
}

main().catch((e) => { console.error(e); process.exit(1); });