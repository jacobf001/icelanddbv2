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

function isWomensCompetition(name: string | null | undefined): boolean {
  return !!name && /kvenna/i.test(name);
}

// Women's pyramid is much steeper — T1 near-professional, T2+ semi-amateur
function tierScale(tier: number, women = false) {
  const t = Number.isFinite(tier) ? tier : 99;
  if (t <= 1) return 1.0;
  if (women) {
    if (t === 2) return 0.25;
    if (t === 3) return 0.06;
    if (t === 4) return 0.03;
    return 0.02;
  }
  if (t === 2) return 0.78;
  if (t === 3) return 0.58;
  if (t === 4) return 0.43;
  return 0.32;
}

function tierQualityN(tier: number | null | undefined, women = false) {
  const t = Number.isFinite(Number(tier)) ? Number(tier) : 6;
  if (t <= 1) return 1.0;
  if (women) {
    if (t === 2) return 0.25;
    if (t === 3) return 0.06;
    if (t === 4) return 0.03;
    if (t === 5) return 0.02;
    return 0.01;
  }
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

  // Minutes + starts are the dominant signals, unchanged.
  // Goals added on top — not replacing other weight, so prolific scorers get
  // a genuine boost without penalising non-scoring regulars.
  // 8 goals → +0.10 boost, 12+ goals → full +0.15
  const goalsBoost = clamp01(params.goals / 12) * 0.15;
  const cardPenalty = clamp01(params.yellows * 0.02 + params.reds * 0.08);

  const base = minutesN * 0.35 + startsN * 0.55 + goalsBoost - cardPenalty;

  return Math.max(0, Math.round(base * 100));
}

function sideRating(side: { starters: any[]; bench: any[] }, sideStrength: number, missingImpact = 0) {
  const starterSum = side.starters.reduce((s, p) => s + Number(p.importance ?? 0), 0);
  const benchSum = side.bench.reduce((s, p) => s + Number(p.importance ?? 0), 0);

  // avgStarterImp: penalise for missing players relative to expected squad size.
  // missingRatio = what fraction of the full expected squad's quality is absent.
  // This preserves the difference between teams — a team with better actual starters
  // AND fewer missing players rates higher than one with weaker starters AND more missing.
  const presentAvg = side.starters.length > 0 ? starterSum / side.starters.length : 0;
  const expectedTotal = starterSum + missingImpact;
  const missingRatio = expectedTotal > 0 ? clamp01(missingImpact / expectedTotal) : 0;
  // Scale present avg down by missing ratio — more missing = bigger penalty
  const avgStarterImp = clamp01((presentAvg / 100) * (1 - missingRatio));
  const histStrength = Number.isFinite(sideStrength) ? sideStrength : 0.5;
  // If avg importance is well below historical strength, lineup is clearly weakened — weight it more
  const lineupGap = Math.max(0, histStrength - avgStarterImp); // 0 = full strength, 1 = totally weak
  const lineupWeight = clamp01(0.40 + lineupGap * 0.60); // 0.40 normally → up to 1.0 for very weak lineups
  const histWeight = 1 - lineupWeight;
  // Further cap history contribution based on missing ratio — if 40%+ of squad is missing,
  // history becomes increasingly unreliable as a predictor of today's performance.
  const historyCap = clamp01(1 - missingRatio * 1.5);
  const cappedHistStrength = histStrength * historyCap;
  const rawEffective = clamp01(cappedHistStrength * histWeight + avgStarterImp * lineupWeight);
  // Floor: scales with actual lineup quality — a team with zero-importance starters
  // gets no floor protection. Full floor only applies when starters are decent.
  // avgStarterImp near 0 → floor near 0; avgStarterImp near histStrength → floor = 40% hist.
  const tierFloor = histStrength * 0.40 * Math.min(1, avgStarterImp / Math.max(histStrength, 0.01));
  const effectiveStrength = Math.max(rawEffective, tierFloor);

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
  women?: boolean;
}) {
  const strengthN = clamp01(params.teamStrength);
  const tierN = clamp01(tierQualityN(params.tier, params.women));
  const lineupN = clamp01(params.total / 800);
  const coverageN = clamp01(params.coverage);
  // Normalise missing impact relative to tier ceiling — a missing 49-rated T4 player
  // is proportionally as damaging as a missing 78-rated T2 player.
  // Use ~4 missing regulars at tier ceiling as the "fully depleted" baseline.
  function tierCeilingForMissing(tier: number | null, women: boolean): number {
    const t = Number.isFinite(Number(tier)) ? Number(tier) : 3;
    if (t <= 1) return 92;
    if (women) return t <= 2 ? 78 : 25;
    if (t === 2) return 78;
    if (t === 3) return 64;
    if (t === 4) return 50;
    if (t === 5) return 36;
    return 25;
  }
  const tierCeiling = tierCeilingForMissing(params.tier, params.women ?? false);
  const missingN = clamp01(params.missingImpact / (tierCeiling * 4));

  // Rebalanced: tier weight reduced, lineup quality and strength boosted
  const overallN = 0.18 * tierN
                 + 0.30 * strengthN
                 + 0.42 * lineupN
                 + 0.10 * coverageN
                 - 0.12 * missingN;
  return Math.round(clamp01(overallN) * 100);
}

function computeOdds(params: {
  homeOverall: number; awayOverall: number;
  homeTier: number | null; awayTier: number | null;
  homeRawStrength: number; awayRawStrength: number;
  homeLineupTotal: number; awayLineupTotal: number;
  homeMissingImpact: number; awayMissingImpact: number;
}) {
  const homeTier = Number.isFinite(Number(params.homeTier)) ? Number(params.homeTier) : 6;
  const awayTier = Number.isFinite(Number(params.awayTier)) ? Number(params.awayTier) : 6;

  // Primary signal: effective team strength (blends historical strength with actual lineup quality)
  // effectiveStrength already accounts for missing players via sideRating's missingRatio penalty
  const rawStrengthDiff = clamp(params.homeRawStrength - params.awayRawStrength, -1, 1);
  const strengthZ = rawStrengthDiff * 6.5;

  // Secondary signal: lineup quality modifier — how does today's lineup compare to expected?
  // Baseline is tier-relative: a full-strength T1 side scores ~726, T2 ~580, T3 ~460 etc.
  // This prevents a T2 team's normal lineup from looking "weakened" against a T1 baseline.
  function lineupBaselineForTier(tier: number): number {
    if (tier <= 1) return 726;
    if (tier === 2) return 580;
    if (tier === 3) return 460;
    if (tier === 4) return 360;
    return 280;
  }
  const homeBaseline = lineupBaselineForTier(homeTier);
  const awayBaseline = lineupBaselineForTier(awayTier);
  // Wider clamp range: allow a truly devastated lineup (-1.0) to make a real difference
  // Subtract missing impact from effective lineup total — missing players hurt lineup quality
  // lineupZ removed — effectiveStrength already captures lineup quality and missing impact.
  // Adding a separate lineupZ double-penalises teams with missing players.
  const lineupZ = 0;

  // Tier cross-match adjustment (lower tier team away gets slight penalty)
  // But reduce this if the away team's lineup is significantly below their tier baseline —
  // a T1 team fielding a youth squad shouldn't get a tier bonus over a full-strength T2 side.
  // Tier advantage is structural — a T3 team playing T4 retains that quality gap
  // even when missing players. Don't dampen by lineup ratio.
  const tierAdv = clamp((awayTier - homeTier) * 1.50, -2.0, 2.0);

  // Home advantage scales strongly with tier — at T1 it's meaningful (crowd, travel),
  // at T4/T5 amateur level teams often share pitches and there's no real home advantage.
  const avgTier = ((homeTier ?? 3) + (awayTier ?? 3)) / 2;
  const homeAdv = clamp(0.40 - (avgTier - 1) * 0.10, 0.05, 0.40);

  const z = strengthZ + lineupZ + tierAdv + homeAdv;

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
  const women = isWomensCompetition(row.competition_name);

  const ppm = played > 0 ? points / played : 0;
  const base = clamp01(ppm / 3);
  const scale = tierScale(tier, women);
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

  // Batch 1: fetch birth years + current + prev season stats in parallel
  const [
    { data: playerBirthRows, error: birthErr },
    { data: seasonRows, error: seasonErr },
    { data: prevSeasonRows, error: prevSeasonErr },
  ] = await Promise.all([
    supabaseAdmin
      .from("players")
      .select("ksi_player_id, birth_year")
      .in("ksi_player_id", allIds),
    supabaseAdmin
      .from("player_season_to_date")
      .select("season_year, ksi_team_id, ksi_player_id, player_name, matches_played, starts, minutes, goals, yellows, reds")
      .eq("season_year", seasonYear)
      .in("ksi_player_id", allIds),
    supabaseAdmin
      .from("player_season_to_date")
      .select("season_year, ksi_team_id, ksi_player_id, player_name, matches_played, starts, minutes, goals, yellows, reds")
      .eq("season_year", prevSeasonYear)
      .in("ksi_player_id", allIds),
  ]);

  if (birthErr) return NextResponse.json({ error: birthErr.message }, { status: 500 });
  if (seasonErr) return NextResponse.json({ error: seasonErr.message }, { status: 500 });
  if (prevSeasonErr) return NextResponse.json({ error: prevSeasonErr.message }, { status: 500 });

  const birthYearById = new Map<string, number | null>();
  for (const r of playerBirthRows ?? []) {
    birthYearById.set(String(r.ksi_player_id), r.birth_year ?? null);
  }

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
  const compNameByTeam = new Map<string, string>();
  const tierByTeam = new Map<string, number>();
  const teamStrengthDebug = new Map<string, TeamStrengthDebugRow>();

  if (teamIdsToLoad.length) {
    // Batch 2: cur + prev team strength in parallel
    const [
      { data: curRows, error: curErr },
      { data: prevRows, error: prevErr },
    ] = await Promise.all([
      supabaseAdmin
        .from("computed_league_table")
        .select("season_year, team_ksi_id, ksi_competition_id, played, points, competition_tier, competition_name, position")
        .eq("season_year", seasonYear)
        .in("team_ksi_id", teamIdsToLoad),
      supabaseAdmin
        .from("computed_league_table")
        .select("season_year, team_ksi_id, ksi_competition_id, played, points, competition_tier, competition_name, position")
        .eq("season_year", prevSeasonYear)
        .in("team_ksi_id", teamIdsToLoad),
    ]);

    if (curErr) return NextResponse.json({ error: curErr.message }, { status: 500 });
    if (prevErr) return NextResponse.json({ error: prevErr.message }, { status: 500 });

    const leagueSizeByComp = new Map<string, number>();
    for (const r of curRows ?? []) {
      const compId = String((r as any).ksi_competition_id ?? "");
      if (!compId) continue;
      leagueSizeByComp.set(compId, (leagueSizeByComp.get(compId) ?? 0) + 1);
    }

    const bestCur = pickBestRowPerTeam(curRows ?? []);

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
      const compName = cur?.competition_name ?? null;
      if (compName) compNameByTeam.set(id, compName);

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
  const homeCompName = homeTeamId ? (compNameByTeam.get(homeTeamId) ?? null) : null;
  const awayCompName = awayTeamId ? (compNameByTeam.get(awayTeamId) ?? null) : null;
  const isWomen = isWomensCompetition(homeCompName) || isWomensCompetition(awayCompName);

  // Player club context
  const clubCtxBySeasonTeam = new Map<string, TeamSeasonContext>();

  // Batch 3: club context + recent lineups in parallel (both independent at this point)
  const [clubRowsResult, lineupRowsResult] = await Promise.all([
    statTeamIds.length
      ? supabaseAdmin
          .from("computed_league_table")
          .select("season_year, team_ksi_id, ksi_competition_id, played, points, competition_tier, competition_name, competition_category, position")
          .in("season_year", [seasonYear, prevSeasonYear])
          .in("team_ksi_id", statTeamIds)
      : Promise.resolve({ data: [], error: null }),
    supabaseAdmin
      .from("match_lineups")
      .select("ksi_player_id, ksi_match_id, minute_in, minute_out")
      .in("ksi_player_id", allIds),
  ]);

  const { data: clubRows, error: clubErr } = clubRowsResult;
  const { data: lineupRows, error: lineupErr } = lineupRowsResult;

  if (clubErr) return NextResponse.json({ error: clubErr.message }, { status: 500 });
  if (lineupErr) return NextResponse.json({ error: lineupErr.message }, { status: 500 });

  if (statTeamIds.length) {

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

  // 4) Recent form (lineupRows already fetched in batch 3 above)
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
          category === "U-21" || category === "U-17" ||
          (t > 5 && category !== "Fullorðnir" && category !== "Adults");
      const youthDiscount = isYouth ? 0.35 : 1.0;
      totalMins += Number(row.minutes ?? 0) * youthDiscount;
      totalStarts += Number(row.starts ?? 0) * youthDiscount;

      // Youth goals excluded entirely — they don't reflect senior threat level
      if (goalsOverride === undefined && !isYouth) totalGoals += Number(row.goals ?? 0);
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

    // Derive primary tier from weighted minutes, but senior starts override youth.
    // Track senior starts separately — if a player has 5+ senior starts they are
    // a senior player regardless of how many youth minutes are in the DB.
    let seniorStartsTotal = 0;
    let seniorMinsTotal = 0;
    let bestSeniorTierByMins = 99;
    let bestSeniorWMins = 0;
    let bestOverallTier = 99;
    let bestOverallWMins = 0;

    for (const row of playerRows) {
      const teamId = row.ksi_team_id ? String(row.ksi_team_id) : null;
      const ctx = teamId ? clubCtxBySeasonTeam.get(`${seasonYearCtx}-${teamId}`) ?? null : null;
      const tier = ctx?.competition_tier ?? null;
      const t = Number.isFinite(Number(tier)) ? Number(tier) : 99;
      const category = ctx?.competition_category ?? null;
      const isYouthRow = ctx === null
        ? true
        : category === "U-19" || category === "U-20" || category === "U-21" || category === "U-17" ||
          (t > 5 && category !== "Fullorðnir" && category !== "Adults");
      const wmins = Number(row.minutes ?? 0) * (isYouthRow ? 0.35 : 1.0);
      if (!isYouthRow) {
        seniorStartsTotal += Number(row.starts ?? 0);
        seniorMinsTotal += Number(row.minutes ?? 0);
        if (wmins > bestSeniorWMins) { bestSeniorWMins = wmins; bestSeniorTierByMins = t; }
      }
      if (wmins > bestOverallWMins) { bestOverallWMins = wmins; bestOverallTier = t; }
    }

    // Only treat as senior player if they have substantial senior minutes (540+, i.e. 6 full games)
    // For women, only T1/T2 counts as meaningfully senior — T3+ is amateur/youth equivalent
    const seniorTierThreshold = isWomen ? 3 : 99;
    if (seniorMinsTotal >= 540 && bestSeniorTierByMins < seniorTierThreshold) {
      primaryTier = bestSeniorTierByMins;
    } else if (weightedMinsByTier.size > 0) {
      primaryTier = bestOverallTier;
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
    function maxGamesForTier(tier: number, isYouthComp: boolean, women: boolean): number {
      if (isYouthComp) return 27; // U19: 3 rounds × 9 games
      if (women) {
        if (tier <= 2) return 18; // Besta/Lengjudeild kvenna: 10 teams = 18 games
        if (tier === 3) return 11; // 2. deild kvenna
        return 10;
      }
      if (tier <= 3) return 22;   // Tiers 1-3: 12 teams
      if (tier === 4) return 18;  // 3. deild: 10 teams
      if (tier === 5) return 14;  // 4. deild: 8 teams
      return 22;                  // safe fallback
    }
    // isPrimaryYouth: only true for actual youth competitions (category-based), not T6 adult
    // We check if the primary tier rows are youth competitions by category
    const primaryTierRows = playerRows.filter((r: any) => {
      const teamId = r.ksi_team_id ? String(r.ksi_team_id) : null;
      const ctx = teamId ? clubCtxBySeasonTeam.get(`${seasonYearCtx}-${teamId}`) ?? null : null;
      const t = Number.isFinite(Number(ctx?.competition_tier)) ? Number(ctx?.competition_tier) : 99;
      return t === primaryTier;
    });
    const primaryCategory = primaryTierRows[0]
      ? (() => {
          const teamId = primaryTierRows[0].ksi_team_id ? String(primaryTierRows[0].ksi_team_id) : null;
          const ctx = teamId ? clubCtxBySeasonTeam.get(`${seasonYearCtx}-${teamId}`) ?? null : null;
          return ctx?.competition_category ?? null;
        })()
      : null;
    const isPrimaryYouth = primaryCategory === "U-19" || primaryCategory === "U-20" ||
      primaryCategory === "U-21" || primaryCategory === "U-17" ||
      (primaryTier >= 6 && primaryCategory !== "Fullorðnir" && primaryCategory !== "Adults");
    const maxGames = primaryTier < 99 ? maxGamesForTier(primaryTier, isPrimaryYouth, isWomen) : (isWomen ? 18 : 22);

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
      if (isYouth) return 22;
      // Women's T3+ is amateur quality — treat same as youth ceiling
      if (isWomen && tier >= 3) return 22;
      if (tier <= 1) return 92;  // 93-100 reserved for exceptional (goals+full season)
      if (tier === 2) return 78;
      if (tier === 3) return 64;
      if (tier === 4) return 50;
      if (tier === 5) return 36;
      return 28;  // T6 adult — above youth but below T5
    }

    let importanceCeiling = 64; // sensible default if no tier data (assume mid-tier player)
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
          : 0; // bottom half: no downward bridging — tier ceiling is a floor, not a range
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

    // If a player is playing for a lower-tier team than their primary club,
    // cap their importance ceiling to the match team's tier ceiling.
    // e.g. a T1 fringe player starting for a T3 team should be rated as T3.
    const sideCtx = sideTeamId ? clubCtxBySeasonTeam.get(`${seasonYear}-${sideTeamId}`) ?? null : null;
    const sideTierRaw = Number(sideCtx?.competition_tier ?? 99);
    const sideTier = Number.isFinite(sideTierRaw) ? sideTierRaw : 99;
    if (sideTier < 99) {
      const sideCeiling = isWomen
        ? (sideTier <= 1 ? 92 : sideTier <= 2 ? 78 : 22)
        : (sideTier <= 1 ? 92 : sideTier === 2 ? 78 : sideTier === 3 ? 64 : sideTier === 4 ? 50 : sideTier === 5 ? 36 : 28);
      if (sideCeiling < importanceCeiling) {
        importanceCeiling = sideCeiling;
        // Player has no stats for the side team and their primary club is higher tier —
        // they're playing down (loan/dual reg). Use a default of 35% of side ceiling
        // rather than near-zero from sparse higher-tier minutes.
        const hasTeamStats = sideTeamId
          ? (playerRows ?? []).some((r: any) => String(r.ksi_team_id) === sideTeamId && Number(r.minutes ?? 0) > 0)
          : false;
        if (!hasTeamStats && importance < Math.round(sideCeiling * 0.35)) {
          importance = Math.round(sideCeiling * 0.35);
        } else {
          importance = Math.min(importance, sideCeiling);
        }
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

    // Fetch current season stats + birth years in parallel (both keyed on missingIds)
    const [
      { data: missRows, error: missErr },
      { data: missingBirthRows },
    ] = await Promise.all([
      supabaseAdmin
        .from("player_season_to_date")
        .select("ksi_player_id, player_name, starts, minutes, goals, yellows, reds, ksi_team_id")
        .eq("season_year", seasonYear)
        .in("ksi_player_id", missingIds),
      supabaseAdmin
        .from("players")
        .select("ksi_player_id, birth_year")
        .in("ksi_player_id", missingIds),
    ]);

    if (missErr) throw new Error(missErr.message);

    // Fetch prev season rows for players with no current data
    const hasCurrData = new Set((missRows ?? []).map((r: any) => String(r.ksi_player_id)));
    const needsPrevIds = missingIds.filter((id) => !hasCurrData.has(id));

    const { data: missPrevRows, error: missPrevErr } = needsPrevIds.length > 0
      ? await supabaseAdmin
          .from("player_season_to_date")
          .select("ksi_player_id, player_name, starts, minutes, goals, yellows, reds, ksi_team_id")
          .eq("season_year", prevSeasonYear)
          .eq("ksi_team_id", teamId)
          .in("ksi_player_id", needsPrevIds)
      : { data: [], error: null };

    if (missPrevErr) throw new Error(missPrevErr.message);

    const allMissRows = [...(missRows ?? []), ...(missPrevRows ?? [])];

    const missingBirthById = new Map<string, number | null>();
    for (const r of missingBirthRows ?? []) {
      missingBirthById.set(String(r.ksi_player_id), r.birth_year ?? null);
    }

    // Group by player — prefer current season rows, fall back to prev
    const missingRowsByPlayer = new Map<string, { rows: any[]; seasonCtx: number }>();
    for (const r of missRows ?? []) {
      const pid = String(r.ksi_player_id);
      const entry = missingRowsByPlayer.get(pid) ?? { rows: [], seasonCtx: seasonYear };
      entry.rows.push(r);
      missingRowsByPlayer.set(pid, entry);
    }
    for (const r of missPrevRows ?? []) {
      const pid = String(r.ksi_player_id);
      if (!missingRowsByPlayer.has(pid)) {
        missingRowsByPlayer.set(pid, { rows: [r], seasonCtx: prevSeasonYear });
      }
    }

    const missing = Array.from(missingRowsByPlayer.entries()).map(([pid, { rows, seasonCtx }]) => {
      const best = rows.reduce((a, b) => Number(a.minutes ?? 0) >= Number(b.minutes ?? 0) ? a : b);
      const { importance, ceiling: importanceCeiling } = calcWeightedImportance(rows, seasonCtx);

      return {
        ksi_player_id: pid,
        player_name: best.player_name ?? null,
        birth_year: missingBirthById.get(pid) ?? null,
        starts: rows.reduce((s: number, r: any) => s + Number(r.starts ?? 0), 0),
        minutes: rows.reduce((s: number, r: any) => s + Number(r.minutes ?? 0), 0),
        goals: rows.reduce((s: number, r: any) => s + Number(r.goals ?? 0), 0),
        importance,
        importanceCeiling,
        fromPrevSeason: seasonCtx === prevSeasonYear,
      };
    });

    const missingImpact = missing.reduce((s, p) => s + Number(p.importance ?? 0), 0);
    missing.sort((a, b) => (b.importance ?? 0) - (a.importance ?? 0));

    return { missing, missingImpact };
  }

  const [homeMissing, awayMissing] = await Promise.all([buildMissingLikelyXI("home"), buildMissingLikelyXI("away")]);

  const homeRating = sideRating(home, homeStrength, homeMissing.missingImpact);
  const awayRating = sideRating(away, awayStrength, awayMissing.missingImpact);

  const homeOverall = computeOverall({
    teamStrength: homeRating.effectiveStrength,
    tier: homeTier,
    total: homeRating.total,
    coverage: homeRating.coverage,
    missingImpact: homeMissing.missingImpact,
    women: isWomen,
  });

  const awayOverall = computeOverall({
    teamStrength: awayRating.effectiveStrength,
    tier: awayTier,
    total: awayRating.total,
    coverage: awayRating.coverage,
    missingImpact: awayMissing.missingImpact,
    women: isWomen,
  });

  const pricing = computeOdds({
    homeOverall,
    awayOverall,
    homeTier,
    awayTier,
    homeRawStrength: homeRating.effectiveStrength,
    awayRawStrength: awayRating.effectiveStrength,
    homeLineupTotal: homeRating.total,
    awayLineupTotal: awayRating.total,
    homeMissingImpact: homeMissing.missingImpact,
    awayMissingImpact: awayMissing.missingImpact,
  });

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