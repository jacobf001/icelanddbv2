import { supabaseAdmin } from "./supabaseServer";
import type { TeamRow, TeamSeasonSummary, PlayerSeasonRow, LikelyXIPlayer } from "./types";

export async function listTeams(): Promise<TeamRow[]> {
  const { data, error } = await supabaseAdmin
    .from("teams")
    .select("ksi_team_id, name")
    .order("name", { ascending: true });

  if (error) throw new Error(error.message);

  return (data ?? []).map((r: any) => ({
    ksi_team_id: String(r.ksi_team_id),
    name: r.name ?? null,
    team_name: r.name ?? null, // <— mirror
  }));
}


// Helper: attach team name if missing on views
async function getTeamName(teamId: string): Promise<string | null> {
  const { data, error } = await supabaseAdmin
    .from("teams")
    .select("name")
    .eq("ksi_team_id", teamId)
    .maybeSingle();

  if (error) throw new Error(error.message);
  return (data?.name ?? null) as string | null;
}

export async function getTeamSeasonSummary(
  teamId: string,
  seasonYear: number,
): Promise<TeamSeasonSummary | null> {
  const { data, error } = await supabaseAdmin
    .from("team_season_to_date")
    .select("*")
    .eq("season_year", seasonYear)
    .eq("ksi_team_id", teamId)
    .maybeSingle();

  if (error) throw new Error(error.message);
  if (!data) return null;

  if (!data.team_name) {
    (data as any).team_name = await getTeamName(teamId);
  }

  return data as TeamSeasonSummary;
}

export async function getTeamPlayersSeason(
  teamId: string,
  seasonYear: number,
  limitRows = 250,
): Promise<PlayerSeasonRow[]> {
  const { data, error } = await supabaseAdmin
    .from("player_season_to_date")
    .select("*")
    .eq("season_year", seasonYear)
    .eq("ksi_team_id", teamId)
    .order("minutes", { ascending: false })
    .limit(limitRows);

  if (error) throw new Error(error.message);
  return (data ?? []) as PlayerSeasonRow[];
}

export async function getMatchMeta(matchId: string): Promise<{
  ksi_match_id: string;
  kickoff_at: string | null;
  home_team_ksi_id: string | null;
  away_team_ksi_id: string | null;
} | null> {
  const { data, error } = await supabaseAdmin
    .from("matches")
    .select("ksi_match_id,kickoff_at,home_team_ksi_id,away_team_ksi_id")
    .eq("ksi_match_id", matchId)
    .maybeSingle();

  if (error) throw new Error(error.message);
  if (!data) return null;

  return {
    ksi_match_id: String((data as any).ksi_match_id),
    kickoff_at: (data as any).kickoff_at ?? null,
    home_team_ksi_id: (data as any).home_team_ksi_id ? String((data as any).home_team_ksi_id) : null,
    away_team_ksi_id: (data as any).away_team_ksi_id ? String((data as any).away_team_ksi_id) : null,
  };
}


// Likely XI: prefer starts, then minutes
export async function getLikelyXI(teamId: string, seasonYear: number): Promise<LikelyXIPlayer[]> {
  const { data, error } = await supabaseAdmin
    .from("player_season_to_date")
    .select("ksi_player_id, player_name, minutes, starts, goals")
    .eq("season_year", seasonYear)
    .eq("ksi_team_id", teamId)
    .order("starts", { ascending: false })
    .order("minutes", { ascending: false })
    .limit(25);

  if (error) throw new Error(error.message);

  const rows = (data ?? []) as LikelyXIPlayer[];

  const starters = rows.filter((r) => (r.starts ?? 0) > 0).slice(0, 11);
  if (starters.length === 11) return starters;

  const used = new Set(starters.map((r) => r.ksi_player_id));
  const fill = rows.filter((r) => !used.has(r.ksi_player_id)).slice(0, 11 - starters.length);
  return [...starters, ...fill];
}

/**
 * NEW: season-to-date rows for a list of players (used in lineup preview)
 */
export async function getPlayersSeasonRows(
  playerIds: string[],
  seasonYear: number,
): Promise<PlayerSeasonRow[]> {
  const ids = playerIds.map(String).filter(Boolean);
  if (!ids.length) return [];

  const { data, error } = await supabaseAdmin
    .from("player_season_to_date")
    .select("*")
    .eq("season_year", seasonYear)
    .in("ksi_player_id", ids);

  if (error) throw new Error(error.message);
  return (data ?? []) as PlayerSeasonRow[];
}

export type RecentAppearanceRow = {
  ksi_player_id: string;
  ksi_match_id: string;
  ksi_team_id: string | null;
  side: string | null;
  squad: string | null;
  minute_in: number | null;
  minute_out: number | null;
  kickoff_at: string | null;
  home_team_ksi_id: string | null;
  away_team_ksi_id: string | null;
  home_score: number | null;
  away_score: number | null;
};

/**
 * NEW: last X appearances per player (season scoped)
 * We fetch a superset and slice per player in JS (Supabase can’t do “limit per group” easily).
 */
export async function getPlayersRecentAppearances(
  playerIds: string[],
  seasonYear: number,
  lastX: number,
): Promise<Record<string, RecentAppearanceRow[]>> {
  const ids = playerIds.map(String).filter(Boolean);
  if (!ids.length) return {};

  // grab plenty; we will group+slice per player
  const maxFetch = Math.min(ids.length * lastX * 3, 5000);

  const { data, error } = await supabaseAdmin
    .from("match_lineups")
    .select(
      `
      ksi_player_id,
      ksi_match_id,
      ksi_team_id,
      side,
      squad,
      minute_in,
      minute_out,
      matches!inner (
        kickoff_at,
        season_year,
        home_team_ksi_id,
        away_team_ksi_id,
        home_score,
        away_score
      )
    `,
    )
    .in("ksi_player_id", ids)
    .eq("matches.season_year", seasonYear)
    .order("kickoff_at", { foreignTable: "matches", ascending: false })
    .limit(maxFetch);

  if (error) throw new Error(error.message);

  const grouped: Record<string, RecentAppearanceRow[]> = {};
  for (const r of data ?? []) {
    const pid = String((r as any).ksi_player_id);
    const m = (r as any).matches ?? {};

    const row: RecentAppearanceRow = {
      ksi_player_id: pid,
      ksi_match_id: String((r as any).ksi_match_id),
      ksi_team_id: (r as any).ksi_team_id ? String((r as any).ksi_team_id) : null,
      side: (r as any).side ?? null,
      squad: (r as any).squad ?? null,
      minute_in: (r as any).minute_in ?? null,
      minute_out: (r as any).minute_out ?? null,
      kickoff_at: m.kickoff_at ?? null,
      home_team_ksi_id: m.home_team_ksi_id ? String(m.home_team_ksi_id) : null,
      away_team_ksi_id: m.away_team_ksi_id ? String(m.away_team_ksi_id) : null,
      home_score: m.home_score ?? null,
      away_score: m.away_score ?? null,
    };

    (grouped[pid] ||= []).push(row);
  }

  // slice to lastX per player
  for (const pid of Object.keys(grouped)) {
    grouped[pid] = grouped[pid].slice(0, lastX);
  }

  return grouped;
}
