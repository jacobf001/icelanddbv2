// scripts/build-league-tables-u19-from-db.ts
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
const sleepMs = Number(arg("--sleep") ?? "150");
const limit = Number(arg("--limit") ?? "0"); // 0 = unlimited
const dry = process.argv.includes("--dry");
const debug = process.argv.includes("--debug");
const replace = process.argv.includes("--replace"); // delete+rebuild table rows

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

type Competition = {
  ksi_competition_id: string;
  season_year: number;
  name: string;
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

type TeamRow = { ksi_team_id: string; name: string };

type Standing = {
  ksi_team_id: string;
  team_name: string;
  played: number;
  wins: number;
  draws: number;
  losses: number;
  goals_for: number;
  goals_against: number;
  goal_diff: number;
  points: number;
};

// ---------- helpers: schema-safe upserts ----------
async function getColumns(table: string): Promise<Set<string>> {
  const { data, error } = await supabase.from(table).select("*").limit(1);
  if (error || !data) return new Set(); // if empty table, we can’t infer from a row
  const row = (data as any[])[0] ?? {};
  return new Set(Object.keys(row));
}

function pickAllowed<T extends Record<string, any>>(row: T, allowed: Set<string>) {
  if (!allowed.size) return row; // if we couldn't infer, just send as-is
  const out: any = {};
  for (const k of Object.keys(row)) if (allowed.has(k)) out[k] = row[k];
  return out;
}

// ---------- load competitions ----------
async function fetchU19Competitions(fromYear: number, toYear: number) {
  const pageSize = 1000;
  let from = 0;
  const out: Competition[] = [];

  while (true) {
    const { data, error } = await supabase
      .from("competitions")
      .select("ksi_competition_id, season_year, name, tier")
      .eq("gender", "Male")
      .eq("category", "U-19")
      .gte("season_year", fromYear)
      .lte("season_year", toYear)
      .order("season_year", { ascending: true })
      .order("tier", { ascending: true, nullsFirst: true })
      .range(from, from + pageSize - 1);

    if (error) throw new Error(error.message);

    const batch = (data ?? []) as Competition[];
    out.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }

  return out;
}

// ---------- load played matches for a competition ----------
async function fetchPlayedMatches(ksiCompetitionId: string, seasonYear: number) {
  const pageSize = 1000;
  let from = 0;
  const out: MatchRow[] = [];

  while (true) {
    const { data, error } = await supabase
      .from("matches")
      .select(
        "ksi_match_id, ksi_competition_id, season_year, home_team_ksi_id, away_team_ksi_id, home_score, away_score",
      )
      .eq("ksi_competition_id", ksiCompetitionId)
      .eq("season_year", seasonYear)
      .not("home_score", "is", null)
      .not("away_score", "is", null)
      .range(from, from + pageSize - 1);

    if (error) throw new Error(error.message);

    const batch = (data ?? []) as MatchRow[];
    out.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }

  // only those with team ids
  return out.filter((m) => m.home_team_ksi_id && m.away_team_ksi_id);
}

// ---------- fetch team names ----------
async function fetchTeamNames(teamIds: string[]): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  if (!teamIds.length) return map;

  const chunkSize = 200;
  for (let i = 0; i < teamIds.length; i += chunkSize) {
    const chunk = teamIds.slice(i, i + chunkSize);

    const { data, error } = await supabase
      .from("teams")
      .select("ksi_team_id, name")
      .in("ksi_team_id", chunk);

    if (error) {
      // If teams table doesn't exist / lacks columns, fall back quietly.
      if (debug) console.warn(`teams lookup failed: ${error.message}`);
      continue;
    }

    for (const r of (data ?? []) as TeamRow[]) {
      if (r.ksi_team_id) map.set(String(r.ksi_team_id), r.name);
    }
  }

  return map;
}

// ---------- build standings ----------
function buildStandings(matches: MatchRow[], teamNames: Map<string, string>): Standing[] {
  const byTeam = new Map<string, Standing>();

  function ensure(teamId: string) {
    if (!byTeam.has(teamId)) {
      byTeam.set(teamId, {
        ksi_team_id: teamId,
        team_name: teamNames.get(teamId) ?? `Team ${teamId}`,
        played: 0,
        wins: 0,
        draws: 0,
        losses: 0,
        goals_for: 0,
        goals_against: 0,
        goal_diff: 0,
        points: 0,
      });
    }
    return byTeam.get(teamId)!;
  }

  for (const m of matches) {
    const hId = String(m.home_team_ksi_id);
    const aId = String(m.away_team_ksi_id);
    const hs = Number(m.home_score);
    const as = Number(m.away_score);

    const home = ensure(hId);
    const away = ensure(aId);

    home.played += 1;
    away.played += 1;

    home.goals_for += hs;
    home.goals_against += as;

    away.goals_for += as;
    away.goals_against += hs;

    if (hs > as) {
      home.wins += 1;
      away.losses += 1;
      home.points += 3;
    } else if (hs < as) {
      away.wins += 1;
      home.losses += 1;
      away.points += 3;
    } else {
      home.draws += 1;
      away.draws += 1;
      home.points += 1;
      away.points += 1;
    }
  }

  // compute GD
  for (const s of byTeam.values()) {
    s.goal_diff = s.goals_for - s.goals_against;
  }

  // sort
  const rows = [...byTeam.values()];
  rows.sort((a, b) => {
    if (b.points !== a.points) return b.points - a.points;
    if (b.goal_diff !== a.goal_diff) return b.goal_diff - a.goal_diff;
    if (b.goals_for !== a.goals_for) return b.goals_for - a.goals_for;
    return a.team_name.localeCompare(b.team_name);
  });

  return rows;
}

async function main() {
  console.log(`Build U-19 league tables from DB matches ${fromYear}..${toYear}`);
  console.log(`Dry: ${dry ? "YES" : "NO"} | replace=${replace ? "YES" : "NO"} | limit=${limit || "none"} | debug=${debug ? "YES" : "NO"}`);

  const comps = await fetchU19Competitions(fromYear, toYear);
  const target = limit && limit > 0 ? comps.slice(0, limit) : comps;

  console.log(`Competitions to process: ${target.length}`);

  const ltCols = await getColumns("league_tables");
  const ltrCols = await getColumns("league_table_rows");

  let ok = 0;
  let fail = 0;

  for (const c of target) {
    console.log(`\n[${c.season_year} T${c.tier ?? "?"}] ${c.name} (${c.ksi_competition_id})`);

    try {
      const played = await fetchPlayedMatches(c.ksi_competition_id, c.season_year);

      if (!played.length) {
        console.log(`  ⚠️ no played matches with scores yet`);
        ok++;
        continue;
      }

      const teamIds = Array.from(
        new Set(
          played.flatMap((m) => [String(m.home_team_ksi_id), String(m.away_team_ksi_id)]).filter(Boolean),
        ),
      );

      const teamNames = await fetchTeamNames(teamIds);
      const standings = buildStandings(played, teamNames);

      if (dry) {
        console.log(`  - played matches: ${played.length}`);
        console.log(`  - teams: ${standings.length}`);
        console.log(`  - top 5:`);
        console.table(
          standings.slice(0, 5).map((s, i) => ({
            pos: i + 1,
            team: s.team_name,
            P: s.played,
            W: s.wins,
            D: s.draws,
            L: s.losses,
            GF: s.goals_for,
            GA: s.goals_against,
            GD: s.goal_diff,
            Pts: s.points,
          })),
        );
        ok++;
        if (sleepMs > 0) await sleep(sleepMs);
        continue;
      }

      // upsert league_tables record (single phase/table per competition)
      const tableIndex = 0;
      const phaseName = ""; // keep consistent with your men’s script if you used "" for single tables

      const ltPayload = pickAllowed(
        {
          ksi_competition_id: c.ksi_competition_id,
          season_year: c.season_year,
          source_url: "db://matches",
          phase_name: phaseName,
          table_index: tableIndex,
          fetched_at: new Date().toISOString(),
        },
        ltCols,
      );

      const { data: ltRow, error: ltErr } = await supabase
        .from("league_tables")
        .upsert([ltPayload], { onConflict: "ksi_competition_id,season_year,phase_name,table_index" })
        .select("id")
        .single();

      if (ltErr) throw new Error(`league_tables upsert failed: ${ltErr.message}`);

      const leagueTableId = (ltRow as any).id as number;

      if (replace) {
        const { error: delErr } = await supabase
          .from("league_table_rows")
          .delete()
          .eq("league_table_id", leagueTableId);

        if (delErr) throw new Error(`league_table_rows delete failed: ${delErr.message}`);
      }

      // insert rows
      const insertRows = standings.map((s, idx) =>
        pickAllowed(
          {
            league_table_id: leagueTableId,
            position: idx + 1,
            team_name: s.team_name,
            ksi_team_id: s.ksi_team_id,
            team_id: null,
            played: s.played,
            wins: s.wins,
            draws: s.draws,
            losses: s.losses,
            goals_for: s.goals_for,
            goals_against: s.goals_against,
            goal_diff: s.goal_diff,
            points: s.points,
            raw: {
              computed_from: "matches",
              sort: ["points", "goal_diff", "goals_for", "team_name"],
            },
          },
          ltrCols,
        ),
      );

      // If not replacing, you can upsert instead — but safest is replace mode.
      const { error: insErr } = await supabase.from("league_table_rows").insert(insertRows);
      if (insErr) throw new Error(`league_table_rows insert failed: ${insErr.message}`);

      console.log(`  ✅ saved standings rows=${insertRows.length} (played matches=${played.length})`);
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