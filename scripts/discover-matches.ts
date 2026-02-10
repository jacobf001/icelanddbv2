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

// -------------------- CLI ARGS --------------------
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

// paging knobs (KSI may ignore them; harmless if ignored)
const maxPages = Number(arg("--maxPages") ?? "80");
const pageSize = Number(arg("--pageSize") ?? "200");

// -------------------- TYPES --------------------
type Competition = {
  ksi_competition_id: string;
  season_year: number;
  name: string;
  gender: string;
  category: string;
  tier: number | null;
};

type MatchUpsert = {
  ksi_match_id: string;
  ksi_competition_id: string;
  season_year: number;

  // these will be filled later by scraping each match overview/report
  kickoff_at: string | null;
  venue: string | null;
  home_team_ksi_id: string | null;
  away_team_ksi_id: string | null;
  home_score: number | null;
  away_score: number | null;
};

// -------------------- HELPERS --------------------
function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function extractMatchIdFromHref(href: string): string | null {
  const m = href.match(/[?&]id=(\d+)/);
  return m ? m[1] : null;
}

function competitionMatchesUrl(ksiCompetitionId: string, page?: number) {
  const u = new URL("https://www.ksi.is/oll-mot/mot");
  u.searchParams.set("id", ksiCompetitionId);
  u.searchParams.set("banner-tab", "matches-and-results");
  u.searchParams.set("pageSize", String(pageSize));
  if (page && page > 1) u.searchParams.set("page", String(page));
  return u.toString();
}

async function fetchHtml(url: string): Promise<string> {
  const res = await fetch(url, {
    headers: { "user-agent": "Mozilla/5.0 (icelanddbv2; discover-matches)" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.text();
}

function extractMatchIdsFromDoc($: cheerio.CheerioAPI) {
  const ids: string[] = [];

  $("a[href*='leikur?id=']").each((_, a) => {
    const href = ($(a as unknown as Element).attr("href") as any) ?? "";
    const mid = extractMatchIdFromHref(String(href));
    if (!mid) return;
    ids.push(mid);
  });

  // de-dup
  return Array.from(new Set(ids));
}

// -------------------- MAIN --------------------
async function main() {
  console.log(`Discover matches (store match IDs only) ${fromYear}..${toYear}`);
  console.log(
    `Dry: ${dry ? "YES" : "NO"} | sleep=${sleepMs}ms | limit=${limit || "none"} | debug=${debug ? "YES" : "NO"}`
  );

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
  let totalFound = 0;
  let totalUpserted = 0;

  for (const c of target) {
    console.log(`\n[${c.season_year} T${c.tier ?? "?"}] ${c.name}`);

    try {
      const seen = new Set<string>();
      const all: MatchUpsert[] = [];

      for (let page = 1; page <= maxPages; page++) {
        const url = competitionMatchesUrl(c.ksi_competition_id, page);
        if (debug) console.log(`  fetch: ${url}`);

        const html = await fetchHtml(url);
        const $ = cheerio.load(html);

        const ids = extractMatchIdsFromDoc($);
        if (debug) console.log(`  page ${page}: ids=${ids.length}`);

        let gained = 0;
        for (const id of ids) {
          if (seen.has(id)) continue;
          seen.add(id);
          gained++;

          all.push({
            ksi_match_id: id,
            ksi_competition_id: c.ksi_competition_id,
            season_year: c.season_year,

            kickoff_at: null,
            venue: null,
            home_team_ksi_id: null,
            away_team_ksi_id: null,
            home_score: null,
            away_score: null,
          });
        }

        if (debug) console.log(`  page ${page}: gained=${gained} total=${all.length}`);

        // stop when paging gives nothing new
        if (gained === 0) break;

        if (sleepMs > 0) await sleep(Math.min(150, sleepMs));
      }

      totalFound += all.length;

      if (dry) {
        console.log(`  - matches found: ${all.length}`);
        console.log(`  - sample:`, all.slice(0, 3));
      } else {
        if (all.length === 0) {
          console.log(`  ⚠️ no match ids detected`);
        } else {
          const chunkSize = 500;
          for (let i = 0; i < all.length; i += chunkSize) {
            const chunk = all.slice(i, i + chunkSize);
            const { error: upErr } = await supabase.from("matches").upsert(chunk, {
              onConflict: "ksi_match_id",
            });
            if (upErr) throw new Error(upErr.message);
            totalUpserted += chunk.length;
          }
          console.log(`  ✅ upserted ${all.length} match ids`);
        }
      }

      ok++;
    } catch (e: any) {
      fail++;
      console.error(`  ❌ ${e?.message ?? String(e)}`);
    }

    if (sleepMs > 0) await sleep(sleepMs);
  }

  console.log(`\nDone. OK=${ok} FAIL=${fail}`);
  console.log(`Total match ids found: ${totalFound}${dry ? "" : ` | upserted: ${totalUpserted}`}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
