// src/app/api/lineup-stats/route.ts
import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabaseServer";
import { getLikelyXI } from "@/lib/queries";

export const runtime = "nodejs";

type LineupPlayer = { ksi_player_id: string; name: string; shirt_no: number | null };
type TeamLineup = { starters: LineupPlayer[]; bench: LineupPlayer[] };

type ParsedTeam = { ksi_team_id: string | null; team_name: string | null };
type TeamsBlock = { home: ParsedTeam; away: ParsedTeam };

type LineupsFromReportResponse = {
  inputUrl: string;
  fetchUrl: string;
  counts: { startersHome: number; startersAway: number; benchHome: number; benchAway: number };
  teams?: TeamsBlock;
  home: TeamLineup;
  away: TeamLineup;
};

type TeamSeasonContext = {
  season_year: number;
  team_ksi_id: string;
  competition_tier: number | null;
  competition_name: string | null;
  competition_category: string | null;  // 'Fullorðnir' | 'Adults' | 'U-19' | 'U-20' etc.
  position: number | null;
  played: number;
  points: number;
  league_size: number | null;
};

type TeamStrengthDebugRow = {
  team_ksi_id: string;
  competition_tier: number | null;
  competition_name: string | null;
  position: number | null;
  played: number;
  points: number;
  ppm: number;
  base: number;
  scale: number;
  strength: number;
  prev?: TeamSeasonContext | null;
};

function parseSeasonYear(input: string | null): number | null {
  if (!input) return null;
  const m = input.match(/(19|20)\d{2}/);
  if (!m) return null;
  const y = Number(m[0]);
  return Number.isFinite(y) ? y : null;
}

function uniqById(xs: LineupPlayer[]) {
  const seen = new Set<string>();
  const out: LineupPlayer[] = [];
  for (const x of xs) {
    const id = String(x.ksi_player_id);
    if (seen.has(id)) continue;
    seen.add(id);
    out.push({ ...x, ksi_player_id: id });
  }
  return out;
}

function minutesFromLineupRow(r: { minute_in: number | null; minute_out: number | null }) {
  const minIn = r.minute_in ?? 0;
  const minOut = r.minute_out ?? 90;
  return Math.max(0, Math.min(90, minOut) - Math.max(0, Math.min(90, minIn)));
}

function clamp01(x: number) {
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(1, x));
}

function clamp(x: number, lo: number, hi: number) {
  if (!Number.isFinite(x)) return lo;
  return Math.max(lo, Math.min(hi, x));
}

// Tier weight used only for team strength calculations, NOT player importance.
// Player importance is purely about minutes/starts/goals within their own team context.
// Whether that team is good or bad is handled by teamStrength separately.
function tierWeightForStrength(tier: number | null): number {
  const t = Number.isFinite(Number(tier)) ? Number(tier) : 6;
  if (t <= 1) return 1.0;
  if (t === 2) return 0.72;
  if (t === 3) return 0.50;
  if (t === 4) return 0.33;
  if (t === 5) return 0.18;
  return 0.08;
}

function tierScale(tier: number) {
  const t = Number.isFinite(tier) ? tier : 99;
  if (t <= 1) return 1.0;
  if (t === 2) return 0.78;
  if (t === 3) return 0.58;
  if (t === 4) return 0.43;
  return 0.32;
}

function tierQualityN(tier: number | null | undefined) {
  const t = Number.isFinite(Number(tier)) ? Number(tier) : 6;
  if (t <= 1) return 1.0;
  if (t === 2) return 0.78;
  if (t === 3) return 0.58;
  if (t === 4) return 0.43;
  if (t === 5) return 0.32;
  return 0.25;
}

function calcImportance(params: {
  minutes: number;   // already tier-weighted
  starts: number;
  goals: number;
  yellows: number;
  reds: number;
  maxGames: number;  // full season game count for the player's primary league
}) {
  const maxMins = params.maxGames * 90;
  const minutesN = clamp01(params.minutes / maxMins);
  const startsN = clamp01(params.starts / params.maxGames);

  // Minutes is the dominant signal (65%). Starts add texture (25%).
  // Goals give a small boost — toned down so a 4-goal squad player
  // doesn't outscore a full-season regular with 0 goals.
  const goalsBoost = clamp01(params.goals / 20) * 0.10;
  const cardPenalty = clamp01(params.yellows * 0.02 + params.reds * 0.08);

  // Starts weighted heavier than minutes — starting shows manager trust
  const base = minutesN * 0.35 + startsN * 0.55 + goalsBoost - cardPenalty;

  return Math.max(0, Math.round(base * 100));
}

function sideRating(side: { starters: any[]; bench: any[] }, sideStrength: number) {
  const starterSum = side.starters.reduce((s, p) => s + Number(p.importance ?? 0), 0);
  const benchSum = side.bench.reduce((s, p) => s + Number(p.importance ?? 0), 0);

  // Blend historical team strength with actual lineup quality.
  // The weaker the lineup relative to expectations, the more lineup quality dominates.
  // A full-strength side: history matters more. A youth/weakened side: lineup dominates.
  const avgStarterImp = side.starters.length > 0 ? (starterSum / side.starters.length) / 100 : 0.5;
  const histStrength = Number.isFinite(sideStrength) ? sideStrength : 0.5;
  // If avg importance is well below historical strength, lineup is clearly weakened — weight it more
  const lineupGap = Math.max(0, histStrength - avgStarterImp); // 0 = full strength, 1 = totally weak
  const lineupWeight = clamp01(0.40 + lineupGap * 0.60); // 0.40 normally → up to 1.0 for very weak lineups
  const histWeight = 1 - lineupWeight;
  const effectiveStrength = clamp01(histStrength * histWeight + avgStarterImp * lineupWeight);

  const raw = starterSum + benchSum * 0.35;
  const scaled = raw * (0.85 + 0.30 * effectiveStrength);

  const startersKnown = side.starters.filter((p) => p.season != null).length;
  const coverage = side.starters.length ? startersKnown / side.starters.length : 0;

  return {
    starters: Math.round(starterSum),
    bench: Math.round(benchSum),
    raw: Math.round(raw),
    total: Math.round(scaled),
    coverage,
    effectiveStrength,
  };
}

function sigmoid(x: number) {
  return 1 / (1 + Math.exp(-x));
}

function computeOverall(params: {
  teamStrength: number;
  tier: number | null;
  total: number;
  coverage: number;
  missingImpact: number;
}) {
  const strengthN = clamp01(params.teamStrength);
  const tierN = clamp01(tierQualityN(params.tier));
  const lineupN = clamp01(params.total / 800);
  const coverageN = clamp01(params.coverage);
  const missingN = clamp01(params.missingImpact / 650);

  // Rebalanced: tier weight reduced, lineup quality and strength boosted
  const overallN = 0.18 * tierN
                 + 0.30 * strengthN
                 + 0.42 * lineupN
                 + 0.10 * coverageN
                 - 0.05 * missingN;
  return Math.round(clamp01(overallN) * 100);
}

function computeOdds(params: { homeOverall: number; awayOverall: number; homeTier: number | null; awayTier: number | null }) {
  const diffOverall = (params.homeOverall - params.awayOverall) / 14;

  const homeTier = Number.isFinite(Number(params.homeTier)) ? Number(params.homeTier) : 6;
  const awayTier = Number.isFinite(Number(params.awayTier)) ? Number(params.awayTier) : 6;

  // Halved from 0.85 — tier already factored into overall, avoid double-counting
  const tierAdv = clamp((awayTier - homeTier) * 0.40, -2.5, 2.5);
  // Home advantage: worth ~0.45 on z-score in football modelling
  const homeAdv = 0.40;
  const z = diffOverall + tierAdv + homeAdv;

  const pHomeRaw = sigmoid(z);
  const pAwayRaw = 1 - pHomeRaw;

  const gap = Math.abs(z);
  const pDraw = clamp(0.26 - 0.07 * gap, 0.08, 0.28);

  const pHome = (1 - pDraw) * pHomeRaw;
  const pAway = (1 - pDraw) * pAwayRaw;

  return {
    probabilities: { home: pHome, draw: pDraw, away: pAway },
    odds: {
      home: pHome > 0 ? 1 / pHome : null,
      draw: pDraw > 0 ? 1 / pDraw : null,
      away: pAway > 0 ? 1 / pAway : null,
    },
  };
}

function pickBestRowPerTeam(rows: any[]) {
  const best = new Map<string, any>();
  for (const r of rows ?? []) {
    const id = String((r as any).team_ksi_id);
    const prev = best.get(id);

    const tier = Number((r as any).competition_tier ?? 99);
    const prevTier = prev ? Number(prev.competition_tier ?? 99) : 99;

    const played = Number((r as any).played ?? 0);
    const prevPlayed = prev ? Number(prev.played ?? 0) : 0;

    if (!prev || tier < prevTier || (tier === prevTier && played > prevPlayed)) {
      best.set(id, r);
    }
  }
  return best;
}

function posMultiplier(position: number | null, leagueSize: number | null) {
  if (!position || !leagueSize || leagueSize <= 1) return 1.0;
  const posN = clamp01(1 - (position - 1) / (leagueSize - 1));
  return 0.75 + 0.40 * posN;
}

function strengthFromRow(row: any, leagueSize: number | null) {
  const played = Number(row.played ?? 0);
  const points = Number(row.points ?? 0);
  const tier = Number(row.competition_tier ?? 99);
  const position = Number.isFinite(Number(row.position)) ? Number(row.position) : null;

  const ppm = played > 0 ? points / played : 0;
  const base = clamp01(ppm / 3);
  const scale = tierScale(tier);
  const posMul = posMultiplier(position, leagueSize);

  return {
    played,
    points,
    tier: Number.isFinite(tier) ? tier : null,
    position,
    ppm,
    base,
    scale,
    posMul,
    strength: clamp01(base * scale * posMul),
  };
}

function blendStrength(current: number, prev: number, played: number) {
  const w = clamp01(played / 8);
  return clamp01(w * current + (1 - w) * prev);
}

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);

  const inputUrl = searchParams.get("url");
  if (!inputUrl) {
    return NextResponse.json({ error: "Missing params", expected: "url" }, { status: 400 });
  }

  const seasonYear = parseSeasonYear(searchParams.get("season")) ?? new Date().getUTCFullYear();
  const prevSeasonYear = seasonYear - 1;

  // 1) Parse lineups
  const origin = new URL(req.url).origin;
  const lineupRes = await fetch(
    `${origin}/api/lineups-from-report?` + new URLSearchParams({ url: inputUrl }).toString(),
    { cache: "no-store" },
  );

  const lineupJson = (await lineupRes.json()) as LineupsFromReportResponse & { error?: string };
  if (!lineupRes.ok || lineupJson?.error) {
    return NextResponse.json({ error: lineupJson?.error ?? "Failed to parse lineups" }, { status: 400 });
  }

  const teams: TeamsBlock =
    lineupJson.teams ?? {
      home: { ksi_team_id: null, team_name: null },
      away: { ksi_team_id: null, team_name: null },
    };

  const homePlayers = uniqById([...lineupJson.home.starters, ...lineupJson.home.bench]);
  const awayPlayers = uniqById([...lineupJson.away.starters, ...lineupJson.away.bench]);
  const allIds = uniqById([...homePlayers, ...awayPlayers]).map((p) => p.ksi_player_id);

  if (allIds.length === 0) {
    return NextResponse.json({ error: "No players parsed from lineups" }, { status: 200 });
  }

  // Fetch birth years from players table
  const { data: playerBirthRows, error: birthErr } = await supabaseAdmin
    .from("players")
    .select("ksi_player_id, birth_year")
    .in("ksi_player_id", allIds);

  if (birthErr) return NextResponse.json({ error: birthErr.message }, { status: 500 });

  const birthYearById = new Map<string, number | null>();
  for (const r of playerBirthRows ?? []) {
    birthYearById.set(String(r.ksi_player_id), r.birth_year ?? null);
  }

  // 2) Player season rows: current + previous
  const { data: seasonRows, error: seasonErr } = await supabaseAdmin
    .from("player_season_to_date")
    .select("season_year, ksi_team_id, ksi_player_id, player_name, matches_played, starts, minutes, goals, yellows, reds")
    .eq("season_year", seasonYear)
    .in("ksi_player_id", allIds);

  if (seasonErr) return NextResponse.json({ error: seasonErr.message }, { status: 500 });

  const { data: prevSeasonRows, error: prevSeasonErr } = await supabaseAdmin
    .from("player_season_to_date")
    .select("season_year, ksi_team_id, ksi_player_id, player_name, matches_played, starts, minutes, goals, yellows, reds")
    .eq("season_year", prevSeasonYear)
    .in("ksi_player_id", allIds);

  if (prevSeasonErr) return NextResponse.json({ error: prevSeasonErr.message }, { status: 500 });

  // All current season rows per player (supports multiple clubs)
  const allRowsByPlayer = new Map<string, any[]>();
  for (const r of seasonRows ?? []) {
    const id = String((r as any).ksi_player_id);
    const arr = allRowsByPlayer.get(id) ?? [];
    arr.push(r);
    allRowsByPlayer.set(id, arr);
  }

  // Best current row per player (most minutes, for fallback)
  const bestRowByPlayer = new Map<string, any>();
  for (const [id, rows] of allRowsByPlayer.entries()) {
    bestRowByPlayer.set(id, rows.reduce((a, b) => Number(a.minutes ?? 0) >= Number(b.minutes ?? 0) ? a : b));
  }

  // All prev season rows per player
  const allPrevRowsByPlayer = new Map<string, any[]>();
  for (const r of prevSeasonRows ?? []) {
    const id = String((r as any).ksi_player_id);
    const arr = allPrevRowsByPlayer.get(id) ?? [];
    arr.push(r);
    allPrevRowsByPlayer.set(id, arr);
  }

  // Best prev row per player (for fallback)
  const bestPrevRowByPlayer = new Map<string, any>();
  for (const [id, rows] of allPrevRowsByPlayer.entries()) {
    bestPrevRowByPlayer.set(id, rows.reduce((a, b) => Number(a.minutes ?? 0) >= Number(b.minutes ?? 0) ? a : b));
  }

  // Team name lookup
  const seasonTeamIds = Array.from(
    new Set(
      [...(seasonRows ?? []), ...(prevSeasonRows ?? [])].map((r: any) => String(r.ksi_team_id)).filter(Boolean),
    ),
  );

  const teamNameById = new Map<string, string>();
  if (seasonTeamIds.length) {
    const { data: tnRows, error: tnErr } = await supabaseAdmin
      .from("teams")
      .select("ksi_team_id, name")
      .in("ksi_team_id", seasonTeamIds);

    if (tnErr) return NextResponse.json({ error: tnErr.message }, { status: 500 });

    for (const t of tnRows ?? []) {
      const id = String((t as any).ksi_team_id);
      const nm = (t as any).name ?? null;
      if (nm) teamNameById.set(id, String(nm));
    }
  }

  // 3) Team strength
  const homeTeamId = teams.home.ksi_team_id;
  const awayTeamId = teams.away.ksi_team_id;

  const statTeamIds = Array.from(
    new Set(
      [...(seasonRows ?? []), ...(prevSeasonRows ?? [])].map((r: any) => String(r.ksi_team_id)).filter(Boolean),
    ),
  );

  const teamIdsToLoad = Array.from(new Set([homeTeamId, awayTeamId, ...statTeamIds].filter(Boolean))) as string[];

  const strengthByTeam = new Map<string, number>();
  const tierByTeam = new Map<string, number>();
  const teamStrengthDebug = new Map<string, TeamStrengthDebugRow>();

  if (teamIdsToLoad.length) {
    const { data: curRows, error: curErr } = await supabaseAdmin
      .from("computed_league_table")
      .select("season_year, team_ksi_id, ksi_competition_id, played, points, competition_tier, competition_name, position")
      .eq("season_year", seasonYear)
      .in("team_ksi_id", teamIdsToLoad);

    if (curErr) return NextResponse.json({ error: curErr.message }, { status: 500 });

    const leagueSizeByComp = new Map<string, number>();
    for (const r of curRows ?? []) {
      const compId = String((r as any).ksi_competition_id ?? "");
      if (!compId) continue;
      leagueSizeByComp.set(compId, (leagueSizeByComp.get(compId) ?? 0) + 1);
    }

    const bestCur = pickBestRowPerTeam(curRows ?? []);

    const { data: prevRows, error: prevErr } = await supabaseAdmin
      .from("computed_league_table")
      .select("season_year, team_ksi_id, ksi_competition_id, played, points, competition_tier, competition_name, position")
      .eq("season_year", prevSeasonYear)
      .in("team_ksi_id", teamIdsToLoad);

    if (prevErr) return NextResponse.json({ error: prevErr.message }, { status: 500 });

    const prevLeagueSizeByComp = new Map<string, number>();
    for (const r of prevRows ?? []) {
      const compId = String((r as any).ksi_competition_id ?? "");
      if (!compId) continue;
      prevLeagueSizeByComp.set(compId, (prevLeagueSizeByComp.get(compId) ?? 0) + 1);
    }

    const bestPrev = pickBestRowPerTeam(prevRows ?? []);

    for (const id of teamIdsToLoad) {
      const cur = bestCur.get(id);
      const prev = bestPrev.get(id);

      const curCompId = cur ? String((cur as any).ksi_competition_id ?? "") : "";
      const prevCompId = prev ? String((prev as any).ksi_competition_id ?? "") : "";

      const curLeagueSize = curCompId ? (leagueSizeByComp.get(curCompId) ?? null) : null;
      const prevLeagueSize = prevCompId ? (prevLeagueSizeByComp.get(prevCompId) ?? null) : null;

      const curS = cur ? strengthFromRow(cur, curLeagueSize) : null;
      const prevS = prev ? strengthFromRow(prev, prevLeagueSize) : null;

      const curStrength = curS?.strength ?? 0.5;
      const prevStrength = prevS?.strength ?? 0.5;

      const played = curS?.played ?? 0;
      const finalStrength = blendStrength(curStrength, prevStrength, played);

      strengthByTeam.set(id, finalStrength);

      const tier = curS?.tier ?? prevS?.tier ?? null;
      if (tier != null) tierByTeam.set(id, tier);

      teamStrengthDebug.set(id, {
        team_ksi_id: id,
        competition_tier: curS?.tier ?? null,
        competition_name: cur?.competition_name ?? null,
        position: curS?.position ?? null,
        played: curS?.played ?? 0,
        points: curS?.points ?? 0,
        ppm: curS?.ppm ?? 0,
        base: curS?.base ?? 0,
        scale: curS?.scale ?? 0,
        strength: finalStrength,
        prev: prev
          ? {
              season_year: prevSeasonYear,
              team_ksi_id: id,
              competition_tier: prevS?.tier ?? null,
              competition_name: (prev as any).competition_name ?? null,
              competition_category: (prev as any).competition_category ?? null,
              position: prevS?.position ?? null,
              played: prevS?.played ?? 0,
              points: prevS?.points ?? 0,
              league_size: prevLeagueSize ?? null,
            }
          : null,
      });
    }
  }

  const homeStrength = homeTeamId ? (strengthByTeam.get(homeTeamId) ?? 0.5) : 0.5;
  const awayStrength = awayTeamId ? (strengthByTeam.get(awayTeamId) ?? 0.5) : 0.5;

  const homeTier = homeTeamId ? (tierByTeam.get(homeTeamId) ?? null) : null;
  const awayTier = awayTeamId ? (tierByTeam.get(awayTeamId) ?? null) : null;

  // Player club context
  const clubCtxBySeasonTeam = new Map<string, TeamSeasonContext>();

  if (statTeamIds.length) {
    const { data: clubRows, error: clubErr } = await supabaseAdmin
      .from("computed_league_table")
      .select("season_year, team_ksi_id, ksi_competition_id, played, points, competition_tier, competition_name, competition_category, position")
      .in("season_year", [seasonYear, prevSeasonYear])
      .in("team_ksi_id", statTeamIds);

    if (clubErr) return NextResponse.json({ error: clubErr.message }, { status: 500 });

    // Count how many teams are in each competition (= league size)
    const clubLeagueSizeByComp = new Map<string, number>();
    for (const r of clubRows ?? []) {
      const compId = String((r as any).ksi_competition_id ?? "");
      if (!compId) continue;
      clubLeagueSizeByComp.set(compId, (clubLeagueSizeByComp.get(compId) ?? 0) + 1);
    }

    const grouped = new Map<string, any[]>();
    for (const r of clubRows ?? []) {
      const k = `${r.season_year}-${r.team_ksi_id}`;
      const arr = grouped.get(k) ?? [];
      arr.push(r);
      grouped.set(k, arr);
    }

    for (const [k, rows] of grouped.entries()) {
      const best = Array.from(pickBestRowPerTeam(rows).values())[0];
      if (!best) continue;

      const season = Number(best.season_year);
      const teamId = String(best.team_ksi_id);
      const compId = String(best.ksi_competition_id ?? "");
      const leagueSize = compId ? (clubLeagueSizeByComp.get(compId) ?? null) : null;

      const ctxTier = Number.isFinite(Number(best.competition_tier)) ? Number(best.competition_tier) : null;
      // Use tier-based league size — partial match data only has teams present in this match
      function tierLeagueSize(tier: number | null): number | null {
        if (tier === null) return null;
        if (tier <= 3) return 12;
        if (tier === 4) return 10;
        if (tier === 5) return 8;
        return null;
      }
      clubCtxBySeasonTeam.set(`${season}-${teamId}`, {
        season_year: season,
        team_ksi_id: teamId,
        competition_tier: ctxTier,
        competition_name: best.competition_name ?? null,
        competition_category: best.competition_category ?? null,
        position: Number.isFinite(Number(best.position)) ? Number(best.position) : null,
        played: Number(best.played ?? 0),
        points: Number(best.points ?? 0),
        league_size: tierLeagueSize(ctxTier),
      });
    }
  }

  // 4) Recent form
  const { data: lineupRows, error: lineupErr } = await supabaseAdmin
    .from("match_lineups")
    .select("ksi_player_id, ksi_match_id, minute_in, minute_out")
    .in("ksi_player_id", allIds);

  if (lineupErr) return NextResponse.json({ error: lineupErr.message }, { status: 500 });

  const matchIds = Array.from(new Set((lineupRows ?? []).map((r: any) => String(r.ksi_match_id)).filter(Boolean)));

  const kickoffMap = new Map<string, number>();
  if (matchIds.length) {
    const { data: matchRows, error: matchErr } = await supabaseAdmin
      .from("matches")
      .select("ksi_match_id, kickoff_at")
      .in("ksi_match_id", matchIds);

    if (matchErr) return NextResponse.json({ error: matchErr.message }, { status: 500 });

    for (const m of matchRows ?? []) {
      const k = String((m as any).ksi_match_id);
      const t = (m as any).kickoff_at ? Date.parse((m as any).kickoff_at) : 0;
      kickoffMap.set(k, Number.isFinite(t) ? t : 0);
    }
  }

  const byPlayer = new Map<string, Array<any>>();
  for (const r of lineupRows ?? []) {
    const pid = String((r as any).ksi_player_id);
    const mid = String((r as any).ksi_match_id);
    const kickoff = kickoffMap.get(mid) ?? 0;

    const started = (r as any).minute_in === null || (r as any).minute_in === 0;
    const mins = minutesFromLineupRow({ minute_in: (r as any).minute_in, minute_out: (r as any).minute_out });

    const arr = byPlayer.get(pid) ?? [];
    arr.push({ ksi_match_id: mid, kickoff, started, minutes: mins });
    byPlayer.set(pid, arr);
  }

  function lastN(pid: string, n: number) {
    const arr = (byPlayer.get(pid) ?? []).slice().sort((a, b) => (b.kickoff ?? 0) - (a.kickoff ?? 0));
    const take = arr.slice(0, n);
    const lastNMinutes = take.reduce((s, x) => s + (x.minutes ?? 0), 0);
    const lastNStarts = take.reduce((s, x) => s + (x.started ? 1 : 0), 0);
    return { lastNApps: take.length, lastNMinutes, lastNStarts };
  }

  function calcWeightedImportance(
    playerRows: any[],
    seasonYearCtx: number,
    goalsOverride?: number,
    yellowsOverride?: number,
    redsOverride?: number,
  ) {
    // Importance uses RAW minutes — no tier discount.
    // A player who starts every game for Aldershot is just as "important to their team"
    // as one who starts every game for Man City. Tier quality is handled by teamStrength.
    // We do still pick the player's primary (highest) league to set the maxGames ceiling.
    let totalMins = 0;
    let totalStarts = 0;
    let totalGoals = goalsOverride ?? 0;
    let totalYellows = yellowsOverride ?? 0;
    let totalReds = redsOverride ?? 0;

    let primaryTier = 99;
    const weightedMinsByTier = new Map<string, number>();

    for (const row of playerRows) {
      const teamId = row.ksi_team_id ? String(row.ksi_team_id) : null;
      const ctx = teamId ? clubCtxBySeasonTeam.get(`${seasonYearCtx}-${teamId}`) ?? null : null;
      const tier = ctx?.competition_tier ?? null;
      const t = Number.isFinite(Number(tier)) ? Number(tier) : 99;
      const category = ctx?.competition_category ?? null;
      // Must check ctx explicitly — null tier coerces to 0 via Number(null)
      // which would incorrectly pass the t <= 5 senior check.
      const isYouth = ctx === null
        ? true  // no DB entry = untracked competition = treat as youth
        : category === "U-19" || category === "U-20" ||
          category === "U-21" || category === "U-17" || t > 5;
      const youthDiscount = isYouth ? 0.35 : 1.0;
      totalMins += Number(row.minutes ?? 0) * youthDiscount;
      totalStarts += Number(row.starts ?? 0) * youthDiscount;

      if (goalsOverride === undefined) totalGoals += Number(row.goals ?? 0) * youthDiscount;
      if (yellowsOverride === undefined) totalYellows += Number(row.yellows ?? 0) * youthDiscount;
      if (redsOverride === undefined) totalReds += Number(row.reds ?? 0) * youthDiscount;

      // Track weighted minutes per tier to determine primary tier
      // (tier with most weighted minutes = player's true primary competition)
      const key = `tier-${t}`;
      weightedMinsByTier.set(key, (weightedMinsByTier.get(key) ?? 0) + Number(row.minutes ?? 0) * youthDiscount);
      // Still track highest tier seen for fallback
      if (t < primaryTier) {
        primaryTier = t;
      }
    }

    // Derive primary tier = tier where player spent most weighted minutes
    // This avoids a player with 29 token senior minutes getting a Tier 1 ceiling
    if (weightedMinsByTier.size > 0) {
      let maxWMins = 0;
      for (const [key, wmins] of weightedMinsByTier.entries()) {
        if (wmins > maxWMins) {
          maxWMins = wmins;
          primaryTier = Number(key.replace("tier-", ""));
        }
      }
    }

    // Use tier-based maxGames — league size from partial match data is unreliable
    // since we only fetch teams present in this specific match, not the full competition.
    // Based on actual Icelandic league structures:
    // Tier 1 (Besta deild): 12 teams = 22 games
    // Tier 2 (Lengjudeild): 12 teams = 22 games
    // Tier 3 (2. deild): 12 teams = 22 games
    // Tier 4 (3. deild): 10 teams = 18 games
    // Tier 5 (4. deild): 8 teams = 14 games
    // U19 (Íslandsmót 2. flokkur karla): 3 rounds of 9 games = 27 max
    function maxGamesForTier(tier: number, isYouthComp: boolean): number {
      if (isYouthComp) return 27; // U19: 3 rounds × 9 games
      if (tier <= 3) return 22;   // Tiers 1-3: 12 teams
      if (tier === 4) return 18;  // 3. deild: 10 teams
      if (tier === 5) return 14;  // 4. deild: 8 teams
      return 22;                  // safe fallback
    }
    const isPrimaryYouth = primaryTier >= 6;
    const maxGames = primaryTier < 99 ? maxGamesForTier(primaryTier, isPrimaryYouth) : 22;

    const rawImportance = calcImportance({
      minutes: totalMins,
      starts: totalStarts,
      goals: totalGoals,
      yellows: totalYellows,
      reds: totalReds,
      maxGames,
    });

    // Apply tier+position ceiling.
    // Base ceilings per tier — big gaps to reflect quality difference.
    // Position within tier bridges up to 50% toward the tier above/below.
    function tierBaseCeiling(tier: number, isYouth: boolean): number {
      if (isYouth) return 25;
      if (tier <= 1) return 92;  // 93-100 reserved for exceptional (goals+full season)
      if (tier === 2) return 78;
      if (tier === 3) return 64;
      if (tier === 4) return 50;
      if (tier === 5) return 36;
      return 25;
    }

    let importanceCeiling = 100;
    if (primaryTier < 99) {
      const baseCeiling = tierBaseCeiling(primaryTier, isPrimaryYouth);
      const tierAboveCeiling = tierBaseCeiling(primaryTier - 1, false);
      const tierBelowCeiling = tierBaseCeiling(primaryTier + 1, false);

      // Find position context for primary club
      const primaryCtxEntry = [...playerRows]
        .map((r: any) => ({
          teamId: String(r.ksi_team_id ?? ""),
          ctx: clubCtxBySeasonTeam.get(`${seasonYearCtx}-${String(r.ksi_team_id ?? "")}`)
        }))
        .filter(x => x.ctx && (x.ctx.competition_tier ?? 99) === primaryTier)[0];

      // Use tier-based league size for position factor — partial match data is unreliable for league_size
      function leagueSizeForTier(tier: number): number {
        if (tier <= 3) return 12;
        if (tier === 4) return 10;
        if (tier === 5) return 8;
        return 10;
      }
      if (primaryCtxEntry?.ctx?.position != null) {
        const pos = primaryCtxEntry.ctx.position;
        const size = leagueSizeForTier(primaryTier);
        // positionFactor: 1.0 = top, 0.0 = bottom
        const positionFactor = (size - pos) / (size - 1);
        const adjustment = positionFactor >= 0.5
          ? (tierAboveCeiling - baseCeiling) * (positionFactor - 0.5)  // top half: bridge toward tier above
          : (tierBelowCeiling - baseCeiling) * (0.5 - positionFactor); // bottom half: bridge toward tier below
        importanceCeiling = Math.round(baseCeiling + adjustment);
      } else {
        importanceCeiling = baseCeiling;
      }
    }

    return { importance: Math.min(rawImportance, importanceCeiling), ceiling: importanceCeiling };
  }

  function enrich(p: LineupPlayer, side: "home" | "away") {
    const sideTeamId = side === "home" ? homeTeamId : awayTeamId;

    const playerRows = allRowsByPlayer.get(String(p.ksi_player_id)) ?? [];
    const prevPlayerRows = allPrevRowsByPlayer.get(String(p.ksi_player_id)) ?? [];

    // For display: prefer the row from today's team, fallback to most minutes
    const teamRow = sideTeamId ? playerRows.find((row: any) => String(row.ksi_team_id) === sideTeamId) ?? null : null;
    const r = teamRow ?? (playerRows.length > 0 ? playerRows.reduce((a: any, b: any) => Number(a.minutes ?? 0) >= Number(b.minutes ?? 0) ? a : b) : null);

    const statTeamId = r?.ksi_team_id ? String(r.ksi_team_id) : null;
    const seasonClubCtx = statTeamId ? clubCtxBySeasonTeam.get(`${seasonYear}-${statTeamId}`) ?? null : null;

    const sideStrength = side === "home" ? homeStrength : awayStrength;
    const strength = statTeamId ? (strengthByTeam.get(statTeamId) ?? 0.5) : sideStrength;

    // Importance: blend current and previous season using sliding scale.
    // The less current data exists, the more previous season counts (up to 30%).
    // Players with no current data fall back to prev season only.
    const currResult = playerRows.length > 0
      ? calcWeightedImportance(playerRows, seasonYear)
      : null;
    const prevResult = prevPlayerRows.length > 0
      ? calcWeightedImportance(prevPlayerRows, prevSeasonYear)
      : null;

    let importance = 0;
    let importanceCeiling = currResult?.ceiling ?? prevResult?.ceiling ?? 100;
    if (currResult !== null && prevResult !== null) {
      // Sliding scale: prevWeight = 0.30 at 0 curr mins → 0 at 500+ curr mins
      const currMins = playerRows.reduce((sum: number, r: any) => sum + Number(r.minutes ?? 0), 0);
      const prevWeight = Math.max(0, 1 - (currMins / 500)) * 0.30;
      importance = Math.round(currResult.importance * (1 - prevWeight) + prevResult.importance * prevWeight);
    } else if (currResult !== null) {
      importance = currResult.importance;
    } else if (prevResult !== null) {
      importance = prevResult.importance;
      importanceCeiling = prevResult.ceiling;
    }
    if (String(p.ksi_player_id) === "55994") {


      // Manually trace the importance calc
      for (const row of playerRows) {
        const tid = row.ksi_team_id ? String(row.ksi_team_id) : null;
        const ctx2 = tid ? clubCtxBySeasonTeam.get(`${seasonYear}-${tid}`) ?? null : null;
        const tier2 = ctx2?.competition_tier ?? null;
        const t2 = Number.isFinite(Number(tier2)) ? Number(tier2) : 99;
        const cat2 = ctx2?.competition_category ?? null;
        const isYouth2 = ctx2 === null ? true : cat2 === "U-19" || cat2 === "U-20" || cat2 === "U-21" || cat2 === "U-17" || t2 > 5;
        const discount2 = isYouth2 ? 0.35 : 1.0;
      }
    }

    // All prev season clubs for this player
    const prevSeasons = prevPlayerRows
      .sort((a: any, b: any) => Number(b.minutes ?? 0) - Number(a.minutes ?? 0))
      .map((pr: any) => {
        const prevTeamId = pr?.ksi_team_id ? String(pr.ksi_team_id) : null;
        const prevClubCtx = prevTeamId ? clubCtxBySeasonTeam.get(`${prevSeasonYear}-${prevTeamId}`) ?? null : null;
        return {
          season_year: prevSeasonYear,
          ksi_team_id: prevTeamId,
          team_name: prevTeamId ? (teamNameById.get(prevTeamId) ?? null) : null,
          player_name: pr.player_name ?? p.name,
          matches_played: Number(pr.matches_played ?? 0),
          starts: Number(pr.starts ?? 0),
          minutes: Number(pr.minutes ?? 0),
          goals: Number(pr.goals ?? 0),
          yellows: Number(pr.yellows ?? 0),
          reds: Number(pr.reds ?? 0),
          club_ctx: prevClubCtx,
        };
      });

    const seasons = playerRows
      .sort((a: any, b: any) => Number(b.minutes ?? 0) - Number(a.minutes ?? 0))
      .map((sr: any) => {
        const sTeamId = sr?.ksi_team_id ? String(sr.ksi_team_id) : null;
        const sClubCtx = sTeamId ? clubCtxBySeasonTeam.get(`${seasonYear}-${sTeamId}`) ?? null : null;
        return {
          season_year: seasonYear,
          ksi_team_id: sTeamId,
          team_name: sTeamId ? (teamNameById.get(sTeamId) ?? null) : null,
          player_name: sr.player_name ?? p.name,
          matches_played: Number(sr.matches_played ?? 0),
          starts: Number(sr.starts ?? 0),
          minutes: Number(sr.minutes ?? 0),
          goals: Number(sr.goals ?? 0),
          yellows: Number(sr.yellows ?? 0),
          reds: Number(sr.reds ?? 0),
          club_ctx: sClubCtx,
        };
      });

    const season = seasons[0] ?? null;

    return {
      ...p,
      birth_year: birthYearById.get(String(p.ksi_player_id)) ?? null,
      season,
      seasons,
      prevSeasons,
      recent5: lastN(String(p.ksi_player_id), 5),
      importance,
      importanceCeiling,
    };
  }

  const home = {
    starters: lineupJson.home.starters.map((p) => enrich(p, "home")),
    bench: lineupJson.home.bench.map((p) => enrich(p, "home")),
  };

  const away = {
    starters: lineupJson.away.starters.map((p) => enrich(p, "away")),
    bench: lineupJson.away.bench.map((p) => enrich(p, "away")),
  };

  // 5) Missing Likely XI
  async function buildMissingLikelyXI(side: "home" | "away") {
    const teamId = side === "home" ? homeTeamId : awayTeamId;
    if (!teamId) return { missing: [], missingImpact: 0 };

    const likely = await getLikelyXI(teamId, seasonYear);
    const starterIds = new Set((side === "home" ? home.starters : away.starters).map((p) => String(p.ksi_player_id)));

    const missingIds = likely.map((p) => String(p.ksi_player_id)).filter((id) => !starterIds.has(id));
    if (missingIds.length === 0) return { missing: [], missingImpact: 0 };

    const { data: missRows, error: missErr } = await supabaseAdmin
      .from("player_season_to_date")
      .select("ksi_player_id, player_name, starts, minutes, goals, yellows, reds, ksi_team_id")
      .eq("season_year", seasonYear)
      .eq("ksi_team_id", teamId)
      .in("ksi_player_id", missingIds);

    if (missErr) throw new Error(missErr.message);

    const missingPlayerIds = (missRows ?? []).map((r: any) => String(r.ksi_player_id));

    const { data: missingBirthRows } = await supabaseAdmin
      .from("players")
      .select("ksi_player_id, birth_year")
      .in("ksi_player_id", missingPlayerIds);

    const missingBirthById = new Map<string, number | null>();
    for (const r of missingBirthRows ?? []) {
      missingBirthById.set(String(r.ksi_player_id), r.birth_year ?? null);
    }

    // Group missing player rows by player id for weighted importance
    const missingRowsByPlayer = new Map<string, any[]>();
    for (const r of missRows ?? []) {
      const pid = String(r.ksi_player_id);
      const arr = missingRowsByPlayer.get(pid) ?? [];
      arr.push(r);
      missingRowsByPlayer.set(pid, arr);
    }

    const missing = Array.from(missingRowsByPlayer.entries()).map(([pid, rows]) => {
      const best = rows.reduce((a, b) => Number(a.minutes ?? 0) >= Number(b.minutes ?? 0) ? a : b);
      const { importance } = calcWeightedImportance(rows, seasonYear);

      return {
        ksi_player_id: pid,
        player_name: best.player_name ?? null,
        birth_year: missingBirthById.get(pid) ?? null,
        starts: rows.reduce((s: number, r: any) => s + Number(r.starts ?? 0), 0),
        minutes: rows.reduce((s: number, r: any) => s + Number(r.minutes ?? 0), 0),
        goals: rows.reduce((s: number, r: any) => s + Number(r.goals ?? 0), 0),
        importance,
      };
    });

    const missingImpact = missing.reduce((s, p) => s + Number(p.importance ?? 0), 0);
    missing.sort((a, b) => (b.importance ?? 0) - (a.importance ?? 0));

    return { missing, missingImpact };
  }

  const [homeMissing, awayMissing] = await Promise.all([buildMissingLikelyXI("home"), buildMissingLikelyXI("away")]);

  const homeRating = sideRating(home, homeStrength);
  const awayRating = sideRating(away, awayStrength);

  const homeOverall = computeOverall({
    teamStrength: homeRating.effectiveStrength,
    tier: homeTier,
    total: homeRating.total,
    coverage: homeRating.coverage,
    missingImpact: homeMissing.missingImpact,
  });

  const awayOverall = computeOverall({
    teamStrength: awayRating.effectiveStrength,
    tier: awayTier,
    total: awayRating.total,
    coverage: awayRating.coverage,
    missingImpact: awayMissing.missingImpact,
  });

  const pricing = computeOdds({ homeOverall, awayOverall, homeTier, awayTier });

  return NextResponse.json({
    inputUrl,
    season_year: seasonYear,
    teams,

    teamStrength: { home: homeRating.effectiveStrength, away: awayRating.effectiveStrength },
    teamStrengthDebug: {
      home: homeTeamId ? teamStrengthDebug.get(homeTeamId) ?? null : null,
      away: awayTeamId ? teamStrengthDebug.get(awayTeamId) ?? null : null,
    },

    overall: { home: homeOverall, away: awayOverall },
    ...pricing,

    home: {
      ...home,
      rating: homeRating,
      missingLikelyXI: homeMissing.missing,
      missingImpact: homeMissing.missingImpact,
    },
    away: {
      ...away,
      rating: awayRating,
      missingLikelyXI: awayMissing.missing,
      missingImpact: awayMissing.missingImpact,
    },
  });
}