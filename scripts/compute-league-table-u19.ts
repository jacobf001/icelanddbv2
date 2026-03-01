import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

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
const limitComps = Number(arg("--limitComps") ?? "0"); // 0 = unlimited
const dry = process.argv.includes("--dry");
const debug = process.argv.includes("--debug");

// IMPORTANT: adjust this if your table uses a different unique constraint
// Common ones are: "ksi_competition_id,season_year,team_ksi_id"
// or if you store per-phase: add phase_name
const onConflict = arg("--onConflict") ?? "ksi_competition_id,season_year,team_ksi_id";

type Competition = {
  ksi_competition_id: string;
  season_year: number;
  name: string;
  gender: string;
  category: string;
  tier: number | null;
};

type MatchRow = {
  ksi_match_id: string;
  ksi_competition_id: string;
  season_year: number;
  home_team_ksi_id: string | null;
  away_team_ksi_id: string | null;
  home_score: number | null;
  away_score: number | null;
};

function pickAllowed(row: any, allowed: Set<string>) {
  const out: any = {};
  for (const k of Object.keys(row)) if (allowed.has(k)) out[k] = row[k];
  return out;
}

async function getComputedCols(): Promise<Set<string>> {
  const { data, error } = await supabase.from("computed_league_table").select("*").limit(1);
  if (error) {
    // fallback to a reasonable set; you can expand this once you confirm your schema
    return new Set([
      "ksi_competition_id",
      "season_year",
      "team_ksi_id",
      "team_name",
      "played",
      "wins",
      "draws",
      "losses",
      "goals_for",
      "goals_against",
      "goal_diff",
      "points",
      "position",
      "updated_at",
      "computed_at",
      "raw",
    ]);
  }
  const first = (data ?? [])[0] ?? {};
  return new Set(Object.keys(first));
}

function pointsFor(gf: number, ga: number) {
  if (gf > ga) return 3;
  if (gf === ga) return 1;
  return 0;
}

async function fetchAllMatchesForCompetition(c: Competition): Promise<MatchRow[]> {
  const pageSize = 1000;
  let from = 0;
  const out: MatchRow[] = [];

  while (true) {
    const { data, error } = await supabase
      .from("matches")
      .select(
        "ksi_match_id,ksi_competition_id,season_year,home_team_ksi_id,away_team_ksi_id,home_score,away_score",
      )
      .eq("ksi_competition_id", c.ksi_competition_id)
      .eq("season_year", c.season_year)
      .order("ksi_match_id", { ascending: true })
      .range(from, from + pageSize - 1);

    if (error) throw new Error(error.message);

    const batch = (data ?? []) as MatchRow[];
    out.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }

  return out;
}

async function main() {
  console.log(`Compute U-19 league tables from matches ${fromYear}..${toYear}`);
  console.log(`Dry: ${dry ? "YES" : "NO"} | debug=${debug ? "YES" : "NO"} | onConflict=${onConflict}`);

  // competitions in scope
  const { data: comps, error } = await supabase
    .from("competitions")
    .select("ksi_competition_id, season_year, name, gender, category, tier")
    .eq("gender", "Male")
    .eq("category", "U-19")
    .gte("season_year", fromYear)
    .lte("season_year", toYear)
    .order("season_year", { ascending: true })
    .order("tier", { ascending: true, nullsFirst: true });

  if (error) throw new Error(error.message);

  const list = (comps ?? []) as Competition[];
  const target = limitComps && limitComps > 0 ? list.slice(0, limitComps) : list;

  console.log(`Competitions to process: ${target.length}`);

  const allowedCols = await getComputedCols();

  let ok = 0;
  let fail = 0;

  for (const c of target) {
    console.log(`\n[${c.season_year} T${c.tier ?? "?"}] ${c.name} (${c.ksi_competition_id})`);

    try {
      const matches = await fetchAllMatchesForCompetition(c);

      const played = matches.filter(
        (m) =>
          m.home_team_ksi_id &&
          m.away_team_ksi_id &&
          m.home_score !== null &&
          m.away_score !== null,
      );

      if (played.length === 0) {
        console.log(`  ⚠️ no played matches with scores yet (home_score/away_score null)`);
        ok++;
        continue;
      }

      // aggregate by team
      type Agg = {
        team_ksi_id: string;
        played: number;
        wins: number;
        draws: number;
        losses: number;
        gf: number;
        ga: number;
        points: number;
      };

      const byTeam = new Map<string, Agg>();

      function ensure(teamId: string) {
        let a = byTeam.get(teamId);
        if (!a) {
          a = { team_ksi_id: teamId, played: 0, wins: 0, draws: 0, losses: 0, gf: 0, ga: 0, points: 0 };
          byTeam.set(teamId, a);
        }
        return a;
      }

      for (const m of played) {
        const home = String(m.home_team_ksi_id);
        const away = String(m.away_team_ksi_id);
        const hs = Number(m.home_score);
        const as = Number(m.away_score);

        const h = ensure(home);
        const a = ensure(away);

        h.played += 1;
        a.played += 1;

        h.gf += hs; h.ga += as;
        a.gf += as; a.ga += hs;

        const hp = pointsFor(hs, as);
        const ap = pointsFor(as, hs);

        h.points += hp;
        a.points += ap;

        if (hp === 3) { h.wins++; a.losses++; }
        else if (hp === 1) { h.draws++; a.draws++; }
        else { h.losses++; a.wins++; }
      }

      // rank
      const table = [...byTeam.values()]
        .map((t) => ({
          ...t,
          goal_diff: t.gf - t.ga,
        }))
        .sort((x, y) => {
          if (y.points !== x.points) return y.points - x.points;
          if (y.goal_diff !== x.goal_diff) return y.goal_diff - x.goal_diff;
          if (y.gf !== x.gf) return y.gf - x.gf;
          return x.team_ksi_id.localeCompare(y.team_ksi_id);
        })
        .map((t, i) => ({
          ksi_competition_id: c.ksi_competition_id,
          season_year: c.season_year,
          team_ksi_id: t.team_ksi_id,
          played: t.played,
          wins: t.wins,
          draws: t.draws,
          losses: t.losses,
          goals_for: t.gf,
          goals_against: t.ga,
          goal_diff: t.goal_diff,
          points: t.points,
          position: i + 1,
          // optional metadata (only written if those columns exist)
          computed_at: new Date().toISOString(),
          raw: {
            matches_counted: played.length,
            sort: ["points", "gd", "gf", "teamId"],
          },
        }));

      if (debug) {
        console.log(`  played matches counted: ${played.length}`);
        console.log(`  teams: ${table.length}`);
        console.log(`  top 5:`, table.slice(0, 5));
      }

      if (dry) {
        console.log(`  DRY: would upsert computed rows=${table.length}`);
        ok++;
        continue;
      }

      // upsert in chunks
      const chunkSize = 500;
      let upserted = 0;

      for (let i = 0; i < table.length; i += chunkSize) {
        const chunk = table.slice(i, i + chunkSize).map((r) => pickAllowed(r, allowedCols));

        const { error: upErr } = await supabase
          .from("computed_league_table")
          .upsert(chunk, { onConflict });

        if (upErr) throw new Error(`computed_league_table upsert failed: ${upErr.message}`);
        upserted += chunk.length;
      }

      console.log(`  ✅ saved computed table rows=${upserted}`);
      ok++;
    } catch (e: any) {
      fail++;
      console.error(`  ❌ ${e?.message ?? String(e)}`);
    }
  }

  console.log(`\nDone. OK=${ok} FAIL=${fail}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});