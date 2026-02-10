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

type CompetitionRow = {
  ksi_competition_id: string; // text
  season_year: number;        // int
  name: string;               // text
  gender: string;             // text
  category: string;           // text
  tier: number;               // int
};

function arg(name: string): string | null {
  const i = process.argv.indexOf(name);
  if (i === -1) return null;
  return process.argv[i + 1] ?? null;
}

const fromYear = Number(arg("--from") ?? "2020");
const toYear = Number(arg("--to") ?? "2026");
const pageSize = Number(arg("--pageSize") ?? "200");
const dry = process.argv.includes("--dry");

function stripDiacritics(s: string) {
  return s.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

/** Remove "DRÖG" token but DO NOT drop the competition. */
function cleanName(name: string): string {
  // remove both "DRÖG" and "DROG" etc
  let s = name.replace(/\s*DRÖG\b/gi, "");
  // collapse spaces
  s = s.replace(/\s{2,}/g, " ").trim();
  // remove trailing hyphen if it became " -"
  s = s.replace(/\s*-\s*$/g, "").trim();
  return s;
}

function isMens(name: string): boolean {
  const n = stripDiacritics(name).toLowerCase();
  if (n.includes("kvenna") || n.includes("kvk")) return false;
  if (n.includes("karla")) return true;
  return false; // men-only pass
}

// Top 6 men’s domestic league tiers (names vary by sponsor/year, so regex).
const TIER_PATTERNS: Array<{ tier: number; re: RegExp }> = [
  // Tier 1
  { tier: 1, re: /(besta\s*deild|bestadeild|pepsi\s*max|urvalsdeild).*(karla)/i },
  // Tier 2
  { tier: 2, re: /(lengjudeild|inkasso|1\.\s*deild).*(karla)/i },
  // Tier 3–6
  { tier: 3, re: /2\.\s*deild.*karla/i },
  { tier: 4, re: /3\.\s*deild.*karla/i },
  { tier: 5, re: /4\.\s*deild.*karla/i },
  { tier: 6, re: /5\.\s*deild.*karla/i },
];

function inferTier(name: string): number | null {
  const n = stripDiacritics(name);
  for (const p of TIER_PATTERNS) {
    if (p.re.test(n)) return p.tier;
  }
  return null;
}

function extractCompetitionId(href: string): string | null {
  const m1 = href.match(/[?&]id=(\d+)/);
  if (m1) return m1[1];
  const m2 = href.match(/\/mot\/(\d+)/);
  if (m2) return m2[1];
  return null;
}

async function fetchCompetitionsForSeason(season: number): Promise<Map<string, string>> {
  const found = new Map<string, string>();

  for (let page = 1; page <= 50; page++) {
    const u = new URL("https://www.ksi.is/oll-mot/");
    u.searchParams.set("category", "Adults");
    u.searchParams.set("season", String(season));
    u.searchParams.set("pageSize", String(pageSize));
    u.searchParams.set("page", String(page));

    const res = await fetch(u.toString(), {
      headers: { "user-agent": "Mozilla/5.0 (icelanddbv2; competitions-scraper)" },
    });

    if (!res.ok) throw new Error(`Failed to fetch: ${u} (${res.status})`);

    const html = await res.text();
    const $ = cheerio.load(html);

    const before = found.size;

    $("a[href]").each((_, a) => {
      const href = $(a).attr("href") ?? "";
      if (!href.includes("mot")) return;

      const id = extractCompetitionId(href);
      if (!id) return;

      const name = $(a).text().trim().replace(/\s+/g, " ");
      if (!name) return;

      if (!found.has(id)) found.set(id, name);
    });

    const gained = found.size - before;
    if (gained <= 0) break;
    if (gained < Math.max(3, Math.floor(pageSize * 0.05))) break;
  }

  return found;
}

async function main() {
  console.log(`Discover competitions: Adults, seasons ${fromYear}..${toYear}, pageSize=${pageSize}`);
  console.log(`Dry run: ${dry ? "YES" : "NO"}`);

  const rows: CompetitionRow[] = [];

  for (let season = fromYear; season <= toYear; season++) {
    console.log(`\nSeason ${season}…`);
    const comps = await fetchCompetitionsForSeason(season);

    let keptThisSeason = 0;

    for (const [id, rawName] of comps.entries()) {
      const name = cleanName(rawName);

      if (!isMens(name)) continue;

      const tier = inferTier(name);
      if (!tier) continue;

      rows.push({
        ksi_competition_id: String(id),
        season_year: season,
        name,
        gender: "Male",
        category: "Adults",
        tier,
      });

      keptThisSeason++;
    }

    console.log(`Found total links: ${comps.size} | kept (mens top6): ${keptThisSeason}`);
  }

  // Uniqueness should be (ksi_competition_id, season_year)
  const byKey = new Map<string, CompetitionRow>();
  for (const r of rows) byKey.set(`${r.ksi_competition_id}:${r.season_year}`, r);
  const finalRows = [...byKey.values()];

  console.log(`\nTotal rows to upsert: ${finalRows.length}`);

  if (dry) {
    console.table(finalRows.filter(r => r.season_year === 2026).slice(0, 50));
    return;
  }

  const { error } = await supabase
    .from("competitions")
    .upsert(finalRows, { onConflict: "ksi_competition_id,season_year" });

  if (error) throw new Error(error.message);

  console.log("Upsert complete.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
