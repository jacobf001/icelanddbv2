// scripts/scrape-player-birth-years.ts
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

function arg(name: string): string | null {
  const i = process.argv.indexOf(name);
  if (i === -1) return null;
  return process.argv[i + 1] ?? null;
}

const limit = Number(arg("--limit") ?? "0"); // 0 = no limit
const sleepMs = Number(arg("--sleep") ?? "200");
const dry = process.argv.includes("--dry");

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function playerUrl(id: string) {
  const u = new URL("https://www.ksi.is/leikmenn/leikmadur");
  u.searchParams.set("id", id);
  return u.toString();
}

async function fetchHtml(url: string): Promise<string> {
  const res = await fetch(url, {
    headers: { "user-agent": "Mozilla/5.0 (icelanddbv2; player-birthyear)" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.text();
}

// ✅ Correct: birth year is shown as the first "eyebrow-2" span on the player page.
// Example snippet:
// <span class="eyebrow-2">2003</span>
function extractBirthYearFromHtml(html: string): number | null {
  const $ = cheerio.load(html);

  // 1) Primary: exact element shown in your snippet
  const primary = $("span.eyebrow-2").first().text().trim();
  const m1 = primary.match(/\b(19\d{2}|20\d{2})\b/);
  if (m1) {
    const y = Number(m1[1]);
    const now = new Date().getUTCFullYear();
    if (Number.isFinite(y) && y >= 1940 && y <= now) return y;
  }

  // 2) Secondary: scoped search inside the main profile card
  const scopedText = $(".col-span-12").first().text().replace(/\s+/g, " ").trim();
  const m2 = scopedText.match(/\b(19\d{2}|20\d{2})\b/);
  if (m2) {
    const y = Number(m2[1]);
    const now = new Date().getUTCFullYear();
    if (Number.isFinite(y) && y >= 1940 && y <= now) return y;
  }

  return null;
}

async function getPlayerIdsToScrape(): Promise<string[]> {
  // Take ids from match_lineups (covers basically everyone you’ve seen)
  const pageSize = 1000;
  let from = 0;
  const all: string[] = [];

  while (true) {
    const { data, error } = await supabase
      .from("match_lineups")
      .select("ksi_player_id")
      .not("ksi_player_id", "is", null)
      .range(from, from + pageSize - 1);

    if (error) throw new Error(error.message);

    for (const r of data ?? []) {
      const id = String((r as any).ksi_player_id ?? "");
      if (/^\d+$/.test(id)) all.push(id);
    }

    if ((data ?? []).length < pageSize) break;
    from += pageSize;
  }

  return Array.from(new Set(all)).sort((a, b) => a.localeCompare(b));
}

async function getKnownBirthYears(): Promise<Set<string>> {
  const known = new Set<string>();
  const pageSize = 1000;
  let from = 0;

  while (true) {
    const { data, error } = await supabase
      .from("players")
      .select("ksi_player_id,birth_year")
      .not("birth_year", "is", null)
      .range(from, from + pageSize - 1);

    if (error) {
      // table might not exist yet
      return known;
    }

    for (const r of data ?? []) {
      known.add(String((r as any).ksi_player_id));
    }

    if ((data ?? []).length < pageSize) break;
    from += pageSize;
  }

  return known;
}

async function main() {
  console.log(`Scrape player birth years`);
  console.log(`Dry: ${dry ? "YES" : "NO"} | sleep=${sleepMs}ms | limit=${limit || "none"}`);

  const ids = await getPlayerIdsToScrape();
  const known = await getKnownBirthYears();

  const todoAll = ids.filter((id) => !known.has(id));
  const todo = limit && limit > 0 ? todoAll.slice(0, limit) : todoAll;

  console.log(`Player ids total: ${ids.length} | already known: ${known.size} | to scrape: ${todo.length}`);

  let ok = 0;
  let fail = 0;
  let found = 0;

  for (let i = 0; i < todo.length; i++) {
    const id = todo[i];
    const url = playerUrl(id);

    try {
      const html = await fetchHtml(url);
      const birthYear = extractBirthYearFromHtml(html);

      if (!birthYear) {
        console.log(`[#${i + 1}/${todo.length}] ${id} -> no birth year found`);
        ok++;
        continue;
      }

      found++;

      if (dry) {
        console.log(`[#${i + 1}/${todo.length}] ${id} -> birth_year=${birthYear} (DRY)`);
      } else {
        const { error } = await supabase
          .from("players")
          .upsert([{ ksi_player_id: id, birth_year: birthYear }], { onConflict: "ksi_player_id" });

        if (error) throw new Error(error.message);
        console.log(`[#${i + 1}/${todo.length}] ${id} -> birth_year=${birthYear}`);
      }

      ok++;
    } catch (e: any) {
      fail++;
      console.error(`[#${i + 1}/${todo.length}] ${id} -> ❌ ${e?.message ?? String(e)}`);
    }

    if (sleepMs > 0) await sleep(sleepMs);
  }

  console.log(`Done. OK=${ok} FAIL=${fail} | found_birth_year=${found}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});