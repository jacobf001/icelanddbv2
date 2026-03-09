/**
 * discover-competitions.ts
 *
 * Scrapes ksi.is/oll-mot/ for women's competitions and upserts them
 * into the Supabase `competitions` table with gender='female' and
 * the correct tier derived from the competition name.
 *
 * Usage:
 *   npx ts-node discover-competitions.ts
 *
 * Env vars required:
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 */

import * as cheerio from "cheerio";
import { createClient } from "@supabase/supabase-js";
import path from "path";
import * as dotenv from "dotenv";

dotenv.config({ path: path.resolve(process.cwd(), "../.env"), override: true });
// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// Seasons and categories to scrape
const SEASONS = ["2020", "2021", "2022", "2023", "2024", "2025"];
const CATEGORIES = ["Fullorðnir", "U-20"]; // expand to ["Fullorðnir", "U-19", "U-21"] etc. if needed
const GENDER = "Konur";
const PAGE_SIZE = "100";

// ---------------------------------------------------------------------------
// Tier detection
// ---------------------------------------------------------------------------

interface TierPattern {
  tier: number;
  re: RegExp;
}

const WOMENS_TIER_PATTERNS: TierPattern[] = [
  { tier: 1, re: /besta\s*deild\s*kvenna/i },
  { tier: 2, re: /lengjudeild\s*kvenna/i },
  { tier: 3, re: /2\.\s*deild\s*kvenna/i },
  { tier: 4, re: /3\.\s*deild\s*kvenna/i },
  { tier: 5, re: /4\.\s*deild\s*kvenna/i },
  { tier: 6, re: /flokkur\s*kvenna/i },
];

function detectTier(name: string): number | null {
  for (const { tier, re } of WOMENS_TIER_PATTERNS) {
    if (re.test(name)) return tier;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Scraping
// ---------------------------------------------------------------------------

interface DiscoveredCompetition {
  ksi_id: number;
  name: string;
  season: number;
  category: string;
  gender: string;
  tier: number | null;
}

async function fetchCompetitionsPage(
  season: string,
  category: string
): Promise<DiscoveredCompetition[]> {
  const url = new URL("https://www.ksi.is/oll-mot/");
  url.searchParams.set("category", category);
  url.searchParams.set("season", season);
  url.searchParams.set("gender", GENDER);
  url.searchParams.set("pageSize", PAGE_SIZE);

  console.log(`Fetching: ${url.toString()}`);

  const res = await fetch(url.toString(), {
    headers: {
      // Mimic a real browser to avoid potential bot blocking
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      Accept: "text/html,application/xhtml+xml",
    },
  });

  if (!res.ok) {
    throw new Error(`HTTP ${res.status} fetching ${url}`);
  }

  const html = await res.text();
  const $ = cheerio.load(html);
  const results: DiscoveredCompetition[] = [];

  $("tr.clickable-table-row").each((_i, el) => {
    const row = $(el);
    const href = row.attr("data-href") ?? "";

    // Extract competition ID from e.g. "/oll-mot/mot?id=190372"
    const idMatch = href.match(/[?&]id=(\d+)/);
    if (!idMatch) return;
    const ksi_id = parseInt(idMatch[1], 10);

    const cells = row.find("td");
    const name = cells.eq(0).text().trim();
    const yearText = cells.eq(1).text().trim();
    const categoryText = cells.eq(2).text().trim();
    const genderText = cells.eq(3).text().trim();

    const seasonNum = parseInt(yearText, 10);
    if (!name || isNaN(seasonNum)) return;

    results.push({
      ksi_id,
      name,
      season: seasonNum,
      category: categoryText,
      gender: genderText,
      tier: detectTier(name),
    });
  });

  console.log(`  → Found ${results.length} competitions`);
  return results;
}

// ---------------------------------------------------------------------------
// Supabase upsert
// ---------------------------------------------------------------------------

async function upsertCompetitions(
  competitions: DiscoveredCompetition[],
  supabase: any
): Promise<void> {
  if (competitions.length === 0) return;

  const rows = competitions.map((c) => ({
    ksi_competition_id: c.ksi_id,
    season_year: c.season,
    name: c.name,
    gender: "Female",
    category: c.category,
    tier: c.tier,
    is_phase: false,
  }));

  const { error } = await supabase
    .from("competitions")
    .upsert(rows, { onConflict: "ksi_competition_id" });

  if (error) {
    console.error("Supabase upsert error:", error);
    throw error;
  }

  console.log(`  ✓ Upserted ${rows.length} rows into competitions`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const supabase = createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!
  );
  
  console.log("Supabase URL:", process.env.SUPABASE_URL); // debug line


  console.log("=== discover-competitions (women's) ===\n");

  const all: DiscoveredCompetition[] = [];

  for (const season of SEASONS) {
    for (const category of CATEGORIES) {
      const found = await fetchCompetitionsPage(season, category);
      all.push(...found);
      // Small polite delay between requests
      await new Promise((r) => setTimeout(r, 500));
    }
  }

  console.log(`\nTotal competitions discovered: ${all.length}`);

  // Print summary table
  console.log("\nID        | Tier | Name");
  console.log("----------|------|----------------------------------------");
  for (const c of all) {
    const tier = c.tier !== null ? String(c.tier) : "—";
    console.log(
      `${String(c.ksi_id).padEnd(9)} | ${tier.padEnd(4)} | ${c.name}`
    );
  }

  console.log("\nUpserting into Supabase...");
  const tieredOnly = all.filter((c) => c.tier !== null);
  console.log(`Competitions with tier: ${tieredOnly.length}`);

  await upsertCompetitions(tieredOnly, supabase);


  console.log("\nDone.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});