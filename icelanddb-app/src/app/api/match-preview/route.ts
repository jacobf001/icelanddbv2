import { NextResponse } from "next/server";
import type { MatchPreviewResponse } from "@/lib/types";
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
        expected: "homeTeam, awayTeam, season (season can be '2025' or '2025/2026')",
      },
      { status: 400 },
    );
  }

  const [homeSummary, awaySummary, homePlayers, awayPlayers, homeXI, awayXI] = await Promise.all([
    getTeamSeasonSummary(home_team_ksi_id, season_year),
    getTeamSeasonSummary(away_team_ksi_id, season_year),
    getTeamPlayersSeason(home_team_ksi_id, season_year),
    getTeamPlayersSeason(away_team_ksi_id, season_year),
    getLikelyXI(home_team_ksi_id, season_year),
    getLikelyXI(away_team_ksi_id, season_year),
  ]);

  const payload: MatchPreviewResponse = {
    season_year,
    home_team_ksi_id,
    away_team_ksi_id,
    home: {
      summary: homeSummary,
      topPlayers: homePlayers,
      likelyXI: homeXI,
    },
    away: {
      summary: awaySummary,
      topPlayers: awayPlayers,
      likelyXI: awayXI,
    },
  };

  return NextResponse.json(payload);
}
