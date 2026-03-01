import { NextResponse } from "next/server";
import type { MatchPreviewResponse } from "@/lib/types";
import { supabaseAdmin } from "@/lib/supabaseServer";
import { getLikelyXI, getTeamPlayersSeason, getTeamSeasonSummary } from "@/lib/queries";

function parseSeasonYear(input: string | null): number | null {
  if (!input) return null;
  const m = input.match(/(19|20)\d{2}/);
  if (!m) return null;
  const y = Number(m[0]);
  return Number.isFinite(y) ? y : null;
}

function parseIdText(input: string | null): string | null {
  const t = (input ?? "").trim();
  if (!t) return null;
  const m = t.match(/\d+/);
  return m ? m[0] : null;
}

async function attachBirthYears(rows: any[]) {
  const ids = Array.from(
    new Set((rows ?? []).map((r) => String(r?.ksi_player_id ?? "")).filter((x) => /^\d+$/.test(x))),
  );

  if (ids.length === 0) return rows ?? [];

  const { data, error } = await supabaseAdmin.from("players").select("ksi_player_id,birth_year").in("ksi_player_id", ids);
  if (error) throw new Error(error.message);

  const byMap = new Map<string, number>();
  for (const r of data ?? []) {
    if (r.birth_year != null) byMap.set(String(r.ksi_player_id), Number(r.birth_year));
  }

  return (rows ?? []).map((r) => ({
    ...r,
    birth_year: byMap.get(String(r.ksi_player_id)) ?? null,
  }));
}

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);

  const home_team_ksi_id =
    parseIdText(searchParams.get("homeTeam")) ??
    parseIdText(searchParams.get("homeTeamId")) ??
    parseIdText(searchParams.get("home")) ??
    null;

  const away_team_ksi_id =
    parseIdText(searchParams.get("awayTeam")) ??
    parseIdText(searchParams.get("awayTeamId")) ??
    parseIdText(searchParams.get("away")) ??
    null;

  const seasonStr =
    searchParams.get("season") ??
    searchParams.get("seasonYear") ??
    searchParams.get("season_year") ??
    searchParams.get("year") ??
    null;

  const season_year = parseSeasonYear(seasonStr);

  if (!home_team_ksi_id || !away_team_ksi_id || !season_year) {
    return NextResponse.json(
      {
        error: "Missing/invalid params",
        got: { home_team_ksi_id, away_team_ksi_id, season: seasonStr },
        parsed: { season_year },
        expected: "homeTeam, awayTeam, season",
      },
      { status: 400 },
    );
  }

  const [homeSummary, awaySummary, homePlayers0, awayPlayers0, homeXI0, awayXI0] = await Promise.all([
    getTeamSeasonSummary(home_team_ksi_id, season_year),
    getTeamSeasonSummary(away_team_ksi_id, season_year),
    getTeamPlayersSeason(home_team_ksi_id, season_year),
    getTeamPlayersSeason(away_team_ksi_id, season_year),
    getLikelyXI(home_team_ksi_id, season_year),
    getLikelyXI(away_team_ksi_id, season_year),
  ]);

  // ensure BOTH lists include birth_year
  const [homePlayers, awayPlayers, homeXI, awayXI] = await Promise.all([
    attachBirthYears(homePlayers0 as any[]),
    attachBirthYears(awayPlayers0 as any[]),
    attachBirthYears(homeXI0 as any[]),
    attachBirthYears(awayXI0 as any[]),
  ]);

  const payload: MatchPreviewResponse = {
    season_year,
    home_team_ksi_id,
    away_team_ksi_id,
    home: {
      summary: homeSummary,
      topPlayers: homePlayers as any,
      likelyXI: homeXI as any,
    },
    away: {
      summary: awaySummary,
      topPlayers: awayPlayers as any,
      likelyXI: awayXI as any,
    },
  };

  return NextResponse.json(payload);
}