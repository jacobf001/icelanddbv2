/**
 * discover-competitions.ts
 *
 * Scrapes ksi.is/oll-mot/ for all competitions (men, men's U19, women, women's U20)
 * and upserts them into the Supabase `competitions` table.
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

const CURRENT_YEAR = new Date().getFullYear();
const SEASONS = ["2020", "2021", "2022", "2023", "2024", "2025", String(CURRENT_YEAR)].filter(
  (v, i, a) => a.indexOf(v) === i
);
const PAGE_SIZE = "100";

interface ScrapeTarget {
  gender: "Karlar" | "Konur";
  category: string;
  dbGender: "Male" | "Female";
  tierPatterns: Array<{ tier: number; re: RegExp }>;
}

const TARGETS: ScrapeTarget[] = [
  {
    gender: "Karlar",
    category: "Fullorðnir",
    dbGender: "Male",
    tierPatterns: [
      { tier: 1, re: /besta\s*deild\s*karla/i },
      { tier: 2, re: /1\.\s*deild/i },
      { tier: 3, re: /2\.\s*deild\s*(?!kvenna)/i },
      { tier: 4, re: /3\.\s*deild/i },
      { tier: 5, re: /4\.\s*deild/i },
      { tier: 6, re: /5\.\s*deild/i },
    ],
  },
  {
    gender: "Karlar",
    category: "U-19",
    dbGender: "Male",
    tierPatterns: [
      { tier: 6, re: /u.?19|flokkur\s*karla/i },
    ],
  },
  {
    gender: "Konur",
    category: "Fullorðnir",
    dbGender: "Female",
    tierPatterns: [
      { tier: 1, re: /besta\s*deild\s*kvenna/i },
      { tier: 2, re: /lengjudeild\s*kvenna/i },
      { tier: 3, re: /2\.\s*deild\s*kvenna/i },
      { tier: 4, re: /3\.\s*deild\s*kvenna/i },
      { tier: 5, re: /4\.\s*deild\s*kvenna/i },
      { tier: 6, re: /5\.\s*deild\s*kvenna/i },
    ],
  },
  {
    gender: "Konur",
    category: "U-20",
    dbGender: "Female",
    tierPatterns: [
      { tier: 6, re: /flokkur\s*kvenna|u.?20/i },
    ],
  },
];

interface DiscoveredCompetition {
  ksi_id: number;
  name: string;
  season: number;
  category: string;
  dbGender: "Male" | "Female";
  tier: number | null;
}

function detectTier(name: string, patterns: Array<{ tier: number; re: RegExp }>): number | null {
  for (const { tier, re } of patterns) {
    if (re.test(name)) return tier;
  }
  return null;
}

function cleanName(name: string): string {
  return name.replace(/\s*DR[OÖ]G[A-Z]?\s*/gi, "").trim();
}

async function fetchCompetitionsPage(season: string, target: ScrapeTarget): Promise<DiscoveredCompetition[]> {
  const url = new URL("https://www.ksi.is/oll-mot/");
  url.searchParams.set("category", target.category);
  url.searchParams.set("season", season);
  url.searchParams.set("gender", target.gender);
  url.searchParams.set("pageSize", PAGE_SIZE);

  console.log(`  Fetching: ${url.toString()}`);

  const res = await fetch(url.toString(), {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      Accept: "text/html,application/xhtml+xml",
    },
  });

  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);

  const html = await res.text();
  const $ = cheerio.load(html);
  const results: DiscoveredCompetition[] = [];

  $("tr.clickable-table-row").each((_i, el) => {
    const row = $(el);
    const href = row.attr("data-href") ?? "";
    const idMatch = href.match(/[?&]id=(\d+)/);
    if (!idMatch) return;
    const ksi_id = parseInt(idMatch[1], 10);

    const cells = row.find("td");
    const name = cleanName(cells.eq(0).text().trim());
    const yearText = cells.eq(1).text().trim();
    const seasonNum = parseInt(yearText, 10);
    if (!name || isNaN(seasonNum)) return;

    results.push({
      ksi_id,
      name,
      season: seasonNum,
      category: target.category,
      dbGender: target.dbGender,
      tier: detectTier(name, target.tierPatterns),
    });
  });

  console.log(`    → Found ${results.length} competitions`);
  return results;
}

async function upsertCompetitions(competitions: DiscoveredCompetition[], supabase: any): Promise<void> {
  if (competitions.length === 0) return;

  const rows = competitions.map((c) => ({
    ksi_competition_id: c.ksi_id,
    season_year: c.season,
    name: c.name,
    gender: c.dbGender,
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

  console.log(`  ✓ Upserted ${rows.length} rows`);
}

async function main() {
  const supabase = createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!
  );

  console.log("=== discover-competitions (all) ===\n");

  const all: DiscoveredCompetition[] = [];

  for (const target of TARGETS) {
    console.log(`\n[${target.dbGender} / ${target.category}]`);
    for (const season of SEASONS) {
      const found = await fetchCompetitionsPage(season, target);
      all.push(...found);
      await new Promise((r) => setTimeout(r, 400));
    }
  }

  console.log(`\nTotal discovered: ${all.length}`);
  const tieredOnly = all.filter((c) => c.tier !== null);
  console.log(`With tier: ${tieredOnly.length}`);

  const groups = new Map<string, DiscoveredCompetition[]>();
  for (const c of tieredOnly) {
    const k = `${c.dbGender}|${c.category}|${c.tier}`;
    const arr = groups.get(k) ?? [];
    arr.push(c);
    groups.set(k, arr);
  }

  console.log("\nGender     | Cat        | Tier | Count | Sample name");
  console.log("-----------|------------|------|-------|------------");
  for (const [k, arr] of Array.from(groups.entries()).sort()) {
    const [g, cat, tier] = k.split("|");
    console.log(
      `${g.padEnd(10)} | ${cat.padEnd(10)} | ${tier.padEnd(4)} | ${String(arr.length).padEnd(5)} | ${arr[0].name}`
    );
  }

  console.log("\nUpserting into Supabase...");
  await upsertCompetitions(tieredOnly, supabase);

  console.log("\nDone.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});