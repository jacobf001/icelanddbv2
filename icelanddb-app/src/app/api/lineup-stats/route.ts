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
  minutes: number;
  starts: number;
  goals: number;
  yellows: number;
  reds: number;
  maxGames: number;
  // FIX 2: pass ceiling so raw score is scaled tier-relatively from the start.
  // This prevents every regular starter from hitting ceiling/ceiling regardless of tier.
  // A full-season T5 starter scores ~35/36, a partial-season one scores ~20/36.
  importanceCeiling: number;
}) {
  const maxMins = params.maxGames * 90;
  const minutesN = clamp01(params.minutes / maxMins);
  const startsN = clamp01(params.starts / params.maxGames);

  const goalsBoost = clamp01(params.goals / 12) * 0.15;
  const cardPenalty = clamp01(params.yellows * 0.02 + params.reds * 0.08);

  const base = minutesN * 0.35 + startsN * 0.55 + goalsBoost - cardPenalty;

  // Scale by (ceiling / 92) so the natural range for each tier fills its ceiling.
  // 92 is the T1 ceiling — the "full" benchmark.
  // A T5 full-season regular: base≈0.90 → 90 * (36/92) ≈ 35, not 90 hard-capped to 36.
  // A T5 half-season player: base≈0.50 → 50 * (36/92) ≈ 20, properly differentiated.
  const tierScale = clamp01(params.importanceCeiling / 92);
  const raw = Math.max(0, Math.round(base * 100 * tierScale));

  return raw;
}

function sideRating(side: { starters: any[]; bench: any[] }, sideStrength: number, missingImpact = 0) {
  const starterSum = side.starters.reduce((s, p) => s + Number(p.importance ?? 0), 0);
  const benchSum = side.bench.reduce((s, p) => s + Number(p.importance ?? 0), 0);

  const presentAvg = side.starters.length > 0 ? starterSum / side.starters.length : 0;
  const expectedTotal = starterSum + missingImpact;
  const missingRatio = expectedTotal > 0 ? clamp01(missingImpact / expectedTotal) : 0;
  const avgStarterImp = clamp01((presentAvg / 100) * (1 - missingRatio));
  const histStrength = Number.isFinite(sideStrength) ? sideStrength : 0.5;
  const lineupGap = Math.max(0, histStrength - avgStarterImp);
  const lineupWeight = clamp01(0.40 + lineupGap * 0.60);
  const histWeight = 1 - lineupWeight;
  const historyCap = clamp01(1 - missingRatio * 1.5);
  const cappedHistStrength = histStrength * historyCap;
  const rawEffective = clamp01(cappedHistStrength * histWeight + avgStarterImp * lineupWeight);
  const tierFloor = histStrength * 0.40 * Math.min(1, avgStarterImp / Math.max(histStrength, 0.01));
  const rawEffectiveWithFloor = Math.max(rawEffective, tierFloor);

  // Untracked squad penalty: if starters have near-zero importance scores,
  // they are youth/unregistered players with no stats — treat as severely weakened.
  // avgImpRatio of 0 = fully untracked, 1 = full strength tracked squad.
  // At avgImpRatio=0 → 70% reduction. At avgImpRatio≥0.125 → no penalty.
  const avgImpRatio = side.starters.length > 0 ? (starterSum / side.starters.length) / 100 : 0;
  const untrackedPenalty = clamp01(1 - avgImpRatio * 8);
  const effectiveStrength = rawEffectiveWithFloor * (1 - untrackedPenalty * 0.70);

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

  const rawStrengthDiff = clamp(params.homeRawStrength - params.awayRawStrength, -1, 1);
  const strengthZ = rawStrengthDiff * 6.5;

  // Missing player impact — direct z-score adjustment.
  // Normalise against 4× tier ceiling (fully depleted squad = 1.0), scale to ±1.5 z.
  // This is the single biggest situational factor — a team missing Key players
  // should see a meaningful swing in win probability.
  const MISSING_CEILINGS: Record<number, number> = { 1: 92, 2: 78, 3: 64, 4: 50, 5: 36 };
  const homeMissingNorm = clamp(params.homeMissingImpact / ((MISSING_CEILINGS[homeTier] ?? 64) * 4), 0, 1);
  const awayMissingNorm = clamp(params.awayMissingImpact / ((MISSING_CEILINGS[awayTier] ?? 64) * 4), 0, 1);
  // Positive = away missing more (helps home); negative = home missing more (hurts home)
  const missingAdj = (awayMissingNorm - homeMissingNorm) * 1.5;

  function lineupBaseline(tier: number): number {
    if (tier <= 1) return 500; if (tier === 2) return 380; if (tier === 3) return 300;
    if (tier === 4) return 220; return 160;
  }
  const homeLineupRatio = clamp(params.homeLineupTotal / lineupBaseline(homeTier), 0, 1.5);
  const awayLineupRatio = clamp(params.awayLineupTotal / lineupBaseline(awayTier), 0, 1.5);
  const lineupZ = (homeLineupRatio - awayLineupRatio) * 1.5;

  const tierAdv = clamp((awayTier - homeTier) * 0.50, -2.0, 2.0);

  const avgTier = ((homeTier ?? 3) + (awayTier ?? 3)) / 2;
  const homeAdv = clamp(0.40 - (avgTier - 1) * 0.10, 0.05, 0.40);

  const z = strengthZ + lineupZ + missingAdj + tierAdv + homeAdv;
  
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

// Real Icelandic league goal averages from DB (home, away) by tier
// Tier: [avg_home, avg_away]
const TIER_GOAL_BASELINES: Record<number, [number, number]> = {
  1: [1.87, 1.50],
  2: [1.99, 1.56],
  3: [2.37, 1.85],
  4: [2.19, 1.80],
  5: [2.96, 2.16],
};

function poissonPmf(lambda: number, k: number): number {
  // P(X=k) for Poisson(lambda)
  if (lambda <= 0) return k === 0 ? 1 : 0;
  let logP = -lambda + k * Math.log(lambda);
  for (let i = 1; i <= k; i++) logP -= Math.log(i);
  return Math.exp(logP);
}

function computeGoals(params: {
  homeTier: number | null;
  awayTier: number | null;
  homeStrength: number;
  awayStrength: number;
  homeMissingGoals: number;  // goals/game lost from missing players
  awayMissingGoals: number;
  homeMissingImpact: number;
  awayMissingImpact: number;
}) {
  const homeTier = Number.isFinite(Number(params.homeTier)) ? Number(params.homeTier) : 3;
  const awayTier = Number.isFinite(Number(params.awayTier)) ? Number(params.awayTier) : 3;

  // Use the average tier for baseline — cross-tier matches sit between both
  const avgTier = Math.round((homeTier + awayTier) / 2);
  const baseline = TIER_GOAL_BASELINES[avgTier] ?? TIER_GOAL_BASELINES[3];
  let [baseHome, baseAway] = baseline;

  // Adjust for relative team strength using tier-relative average.
  // A global avgStrength of 0.35 breaks at lower tiers where all teams have str < 0.15 —
  // every team looks "well below average" and xG collapses toward the 0.5 floor.
  // Using tier-relative averages keeps equal-strength teams at the baseline.
  // Tier-relative average strength — keeps equal teams at the baseline.
  // A global avg of 0.35 breaks lower tiers where all teams have str < 0.15.
  const TIER_AVG: Record<number, number> = { 1: 0.42, 2: 0.28, 3: 0.18, 4: 0.12, 5: 0.07 };
  const avgStrength = TIER_AVG[Math.round((homeTier + awayTier) / 2)] ?? 0.18;

  // Attack modifier only — no separate defence modifier.
  // A defence modifier double-penalises the weaker team and collapses away xG.
  // Attack strength already captures the quality difference.
  const homeAttackMod = clamp(1 + (params.homeStrength - avgStrength) * 2.0, 0.6, 1.6);
  const awayAttackMod = clamp(1 + (params.awayStrength - avgStrength) * 2.0, 0.6, 1.6);

  let homeXG = baseHome * homeAttackMod;
  let awayXG = baseAway * awayAttackMod;

  // Apply missing goalscorer penalty as a capped percentage reduction.
  // Direct subtraction floors xG at 0.3 when many players are missing.
  // Cap at 20% reduction — losing key scorers hurts but the rest of the team still scores.
  const homeMissingReduction = clamp(params.homeMissingGoals / (baseHome * 2), 0, 0.20);
  const awayMissingReduction = clamp(params.awayMissingGoals / (baseAway * 2), 0, 0.20);
  homeXG = homeXG * (1 - homeMissingReduction);
  awayXG = awayXG * (1 - awayMissingReduction);

  // Scoreline probabilities up to 6 goals per side
  const MAX_GOALS = 6;
  let p_over15 = 0, p_over25 = 0, p_over35 = 0, p_btts = 0;
  let p_under15 = 0, p_under25 = 0;

  for (let h = 0; h <= MAX_GOALS; h++) {
    for (let a = 0; a <= MAX_GOALS; a++) {
      const p = poissonPmf(homeXG, h) * poissonPmf(awayXG, a);
      const total = h + a;
      if (total > 1.5) p_over15 += p;
      if (total > 2.5) p_over25 += p;
      if (total > 3.5) p_over35 += p;
      if (h > 0 && a > 0) p_btts += p;
      if (total < 1.5) p_under15 += p;
      if (total < 2.5) p_under25 += p;
    }
  }

  return {
    xG: { home: Math.round(homeXG * 100) / 100, away: Math.round(awayXG * 100) / 100 },
    expectedTotal: Math.round((homeXG + awayXG) * 10) / 10,
    markets: {
      over15: { prob: p_over15, odds: p_over15 > 0 ? 1 / p_over15 : null },
      under15: { prob: p_under15, odds: p_under15 > 0 ? 1 / p_under15 : null },
      over25: { prob: p_over25, odds: p_over25 > 0 ? 1 / p_over25 : null },
      under25: { prob: p_under25, odds: p_under25 > 0 ? 1 / p_under25 : null },
      over35: { prob: p_over35, odds: p_over35 > 0 ? 1 / p_over35 : null },
      btts_yes: { prob: p_btts, odds: p_btts > 0 ? 1 / p_btts : null },
      btts_no: { prob: 1 - p_btts, odds: (1 - p_btts) > 0 ? 1 / (1 - p_btts) : null },
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

function blendStrength(current: number, prev: number, played: number, tier: number | null = null) {
  const w = clamp01(played / 8);
  const blended = w * current + (1 - w) * prev;

  // Tier floor: minimum strength regardless of early-season results.
  // Diminishes to zero by game 8 — after that trust the data.
  const TIER_FLOORS: Record<number, number> = { 1: 0.55, 2: 0.35, 3: 0.20, 4: 0.12, 5: 0.06 };
  // Tier ceiling: a lower-tier team can never exceed the floor of the tier above.
  // This ensures T2 teams always rate above T3 teams, T3 above T4 etc.
  const TIER_CEILINGS: Record<number, number> = { 1: 1.00, 2: 0.54, 3: 0.34, 4: 0.19, 5: 0.11 };
  const t = Number.isFinite(Number(tier)) ? Number(tier) : null;
  const floor = t !== null ? (TIER_FLOORS[t] ?? 0.04) : 0;
  const ceiling = t !== null ? (TIER_CEILINGS[t] ?? 1.0) : 1.0;
  const effectiveFloor = floor * Math.max(0, 1 - played / 8);
  const withFloor = Math.max(blended, effectiveFloor);
  return clamp01(Math.min(withFloor, ceiling));
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

  const allRowsByPlayer = new Map<string, any[]>();
  for (const r of seasonRows ?? []) {
    const id = String((r as any).ksi_player_id);
    const arr = allRowsByPlayer.get(id) ?? [];
    arr.push(r);
    allRowsByPlayer.set(id, arr);
  }

  const bestRowByPlayer = new Map<string, any>();
  for (const [id, rows] of allRowsByPlayer.entries()) {
    bestRowByPlayer.set(id, rows.reduce((a, b) => Number(a.minutes ?? 0) >= Number(b.minutes ?? 0) ? a : b));
  }

  const allPrevRowsByPlayer = new Map<string, any[]>();
  for (const r of prevSeasonRows ?? []) {
    const id = String((r as any).ksi_player_id);
    const arr = allPrevRowsByPlayer.get(id) ?? [];
    arr.push(r);
    allPrevRowsByPlayer.set(id, arr);
  }

  const bestPrevRowByPlayer = new Map<string, any>();
  for (const [id, rows] of allPrevRowsByPlayer.entries()) {
    bestPrevRowByPlayer.set(id, rows.reduce((a, b) => Number(a.minutes ?? 0) >= Number(b.minutes ?? 0) ? a : b));
  }

  // seasonTeamIds and statTeamIds are the same set — compute once
  const seasonTeamIds = Array.from(
    new Set(
      [...(seasonRows ?? []), ...(prevSeasonRows ?? [])].map((r: any) => String(r.ksi_team_id)).filter(Boolean),
    ),
  );
  const statTeamIds = seasonTeamIds;

  // 3) Team strength + team names in one Promise.all — eliminates a sequential round-trip
  const homeTeamId = teams.home.ksi_team_id;
  const awayTeamId = teams.away.ksi_team_id;

  const teamIdsToLoad = Array.from(new Set([homeTeamId, awayTeamId, ...statTeamIds].filter(Boolean))) as string[];

  const strengthByTeam = new Map<string, number>();
  const compNameByTeam = new Map<string, string>();
  const tierByTeam = new Map<string, number>();
  const teamStrengthDebug = new Map<string, TeamStrengthDebugRow>();
  const teamNameById = new Map<string, string>();

  if (teamIdsToLoad.length) {
    const [
      { data: curRows, error: curErr },
      { data: prevRows, error: prevErr },
      { data: tnRows, error: tnErr },
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
      supabaseAdmin
        .from("teams")
        .select("ksi_team_id, name")
        .in("ksi_team_id", teamIdsToLoad),
    ]);

    if (curErr) return NextResponse.json({ error: curErr.message }, { status: 500 });
    if (prevErr) return NextResponse.json({ error: prevErr.message }, { status: 500 });
    if (tnErr) return NextResponse.json({ error: tnErr.message }, { status: 500 });

    for (const t of tnRows ?? []) {
      const id = String((t as any).ksi_team_id);
      const nm = (t as any).name ?? null;
      if (nm) teamNameById.set(id, String(nm));
    }

    const bestCur = pickBestRowPerTeam(curRows ?? []);
    const bestPrev = pickBestRowPerTeam(prevRows ?? []);

    // Use tier-based league size rather than counting rows in curRows/prevRows.
    // teamIdsToLoad only covers today's lineup players' clubs — so the competition
    // may appear to have only 2-3 teams, making posMultiplier collapse near zero.
    function leagueSizeForStrength(tier: number | null): number | null {
      const t = Number.isFinite(Number(tier)) ? Number(tier) : null;
      if (t === null) return null;
      if (t <= 3) return 12;
      if (t === 4) return 10;
      if (t === 5) return 8;
      return 12;
    }

    for (const id of teamIdsToLoad) {
      const cur = bestCur.get(id);
      const prev = bestPrev.get(id);

      const curLeagueSize = cur ? leagueSizeForStrength(Number(cur.competition_tier ?? null)) : null;
      const prevLeagueSize = prev ? leagueSizeForStrength(Number(prev.competition_tier ?? null)) : null;

      const curS = cur ? strengthFromRow(cur, curLeagueSize) : null;
      const prevS = prev ? strengthFromRow(prev, prevLeagueSize) : null;

      const curStrength = curS?.strength ?? 0.5;
      const prevStrength = prevS?.strength ?? 0.5;

      const played = curS?.played ?? 0;
      const blendTier = curS?.tier ?? prevS?.tier ?? null;
      const finalStrength = blendStrength(curStrength, prevStrength, played, blendTier);

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

  // Player club context — shared map, mutated by buildMissingLikelyXI for missing players' clubs
  const clubCtxBySeasonTeam = new Map<string, TeamSeasonContext>();

  // Batch 3: club ctx + match lineups in parallel
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

  // Helper: populate clubCtxBySeasonTeam from a batch of computed_league_table rows
  function ingestClubRows(rows: any[]) {
    const clubLeagueSizeByComp = new Map<string, number>();
    for (const r of rows ?? []) {
      const compId = String((r as any).ksi_competition_id ?? "");
      if (!compId) continue;
      clubLeagueSizeByComp.set(compId, (clubLeagueSizeByComp.get(compId) ?? 0) + 1);
    }

    const grouped = new Map<string, any[]>();
    for (const r of rows ?? []) {
      const k = `${r.season_year}-${r.team_ksi_id}`;
      const arr = grouped.get(k) ?? [];
      arr.push(r);
      grouped.set(k, arr);
    }

    function tierLeagueSize(tier: number | null): number | null {
      if (tier === null) return null;
      if (tier <= 3) return 12;
      if (tier === 4) return 10;
      if (tier === 5) return 8;
      return null;
    }

    for (const [, rows] of grouped.entries()) {
      const best = Array.from(pickBestRowPerTeam(rows).values())[0];
      if (!best) continue;

      const season = Number(best.season_year);
      const teamId = String(best.team_ksi_id);
      const key = `${season}-${teamId}`;

      // Don't overwrite an existing entry — lineup players' clubs were loaded first
      if (clubCtxBySeasonTeam.has(key)) continue;

      const ctxTier = Number.isFinite(Number(best.competition_tier)) ? Number(best.competition_tier) : null;
      clubCtxBySeasonTeam.set(key, {
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

  if (statTeamIds.length) {
    ingestClubRows(clubRows ?? []);
  }

  // 4) Recent form — kickoffs fetched here (must follow lineupRows to get match IDs)
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
      const isYouth = ctx === null
        ? true
        : category === "U-19" || category === "U-20" ||
          category === "U-21" || category === "U-17" ||
          (t > 5 && category !== "Fullorðnir" && category !== "Adults");
      const youthDiscount = isYouth ? 0.35 : 1.0;
      totalMins += Number(row.minutes ?? 0) * youthDiscount;
      totalStarts += Number(row.starts ?? 0) * youthDiscount;

      if (goalsOverride === undefined && !isYouth) totalGoals += Number(row.goals ?? 0);
      if (yellowsOverride === undefined) totalYellows += Number(row.yellows ?? 0) * youthDiscount;
      if (redsOverride === undefined) totalReds += Number(row.reds ?? 0) * youthDiscount;

      const key = `tier-${t}`;
      weightedMinsByTier.set(key, (weightedMinsByTier.get(key) ?? 0) + Number(row.minutes ?? 0) * youthDiscount);
      if (t < primaryTier) {
        primaryTier = t;
      }
    }

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

    const seniorTierThreshold = isWomen ? 3 : 99;
    if (seniorMinsTotal >= 540 && bestSeniorTierByMins < seniorTierThreshold) {
      primaryTier = bestSeniorTierByMins;
    } else if (weightedMinsByTier.size > 0) {
      primaryTier = bestOverallTier;
    }

    function maxGamesForTier(tier: number, isYouthComp: boolean, women: boolean): number {
      if (isYouthComp) return 27;
      if (women) {
        if (tier <= 2) return 18;
        if (tier === 3) return 11;
        return 10;
      }
      if (tier <= 3) return 22;
      if (tier === 4) return 18;
      if (tier === 5) return 14;
      return 22;
    }

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

    function tierBaseCeiling(tier: number, isYouth: boolean): number {
      if (isYouth) return 22;
      if (isWomen && tier >= 3) return 22;
      if (tier <= 1) return 92;
      if (tier === 2) return 78;
      if (tier === 3) return 64;
      if (tier === 4) return 50;
      if (tier === 5) return 36;
      return 28;
    }

    let importanceCeiling = 64;
    if (primaryTier < 99) {
      const baseCeiling = tierBaseCeiling(primaryTier, isPrimaryYouth);
      const tierAboveCeiling = tierBaseCeiling(primaryTier - 1, false);

      const primaryCtxEntry = [...playerRows]
        .map((r: any) => ({
          teamId: String(r.ksi_team_id ?? ""),
          ctx: clubCtxBySeasonTeam.get(`${seasonYearCtx}-${String(r.ksi_team_id ?? "")}`)
        }))
        .filter(x => x.ctx && (x.ctx.competition_tier ?? 99) === primaryTier)[0];

      function leagueSizeForTier(tier: number): number {
        if (tier <= 3) return 12;
        if (tier === 4) return 10;
        if (tier === 5) return 8;
        return 10;
      }
      if (primaryCtxEntry?.ctx?.position != null) {
        const pos = primaryCtxEntry.ctx.position;
        const size = leagueSizeForTier(primaryTier);
        const positionFactor = (size - pos) / (size - 1);
        const adjustment = positionFactor >= 0.5
          ? (tierAboveCeiling - baseCeiling) * (positionFactor - 0.5)
          : 0;
        importanceCeiling = Math.round(baseCeiling + adjustment);
      } else {
        importanceCeiling = baseCeiling;
      }
    }

    // FIX 2: pass ceiling into calcImportance so raw score is tier-scaled from the start.
    // This prevents every regular starter collapsing to ceiling/ceiling.
    const rawImportance = calcImportance({
      minutes: totalMins,
      starts: totalStarts,
      goals: totalGoals,
      yellows: totalYellows,
      reds: totalReds,
      maxGames,
      importanceCeiling,
    });

    return { importance: Math.min(rawImportance, importanceCeiling), ceiling: importanceCeiling };
  }

  function enrich(p: LineupPlayer, side: "home" | "away") {
    const sideTeamId = side === "home" ? homeTeamId : awayTeamId;

    const playerRows = allRowsByPlayer.get(String(p.ksi_player_id)) ?? [];
    const prevPlayerRows = allPrevRowsByPlayer.get(String(p.ksi_player_id)) ?? [];

    const teamRow = sideTeamId ? playerRows.find((row: any) => String(row.ksi_team_id) === sideTeamId) ?? null : null;
    const r = teamRow ?? (playerRows.length > 0 ? playerRows.reduce((a: any, b: any) => Number(a.minutes ?? 0) >= Number(b.minutes ?? 0) ? a : b) : null);

    const statTeamId = r?.ksi_team_id ? String(r.ksi_team_id) : null;
    const seasonClubCtx = statTeamId ? clubCtxBySeasonTeam.get(`${seasonYear}-${statTeamId}`) ?? null : null;

    const sideStrength = side === "home" ? homeStrength : awayStrength;
    const strength = statTeamId ? (strengthByTeam.get(statTeamId) ?? 0.5) : sideStrength;

    const currResult = playerRows.length > 0
      ? calcWeightedImportance(playerRows, seasonYear)
      : null;
    const prevResult = prevPlayerRows.length > 0
      ? calcWeightedImportance(prevPlayerRows, prevSeasonYear)
      : null;

    let importance = 0;
    let importanceCeiling = currResult?.ceiling ?? prevResult?.ceiling ?? 100;
    if (currResult !== null && prevResult !== null) {
      const currMins = playerRows.reduce((sum: number, r: any) => sum + Number(r.minutes ?? 0), 0);
      const prevWeight = Math.max(0, 1 - (currMins / 500)) * 0.30;
      importance = Math.round(currResult.importance * (1 - prevWeight) + prevResult.importance * prevWeight);
    } else if (currResult !== null) {
      importance = currResult.importance;
    } else if (prevResult !== null) {
      importance = prevResult.importance;
      importanceCeiling = prevResult.ceiling;
    }

    const sideCtx = sideTeamId ? clubCtxBySeasonTeam.get(`${seasonYear}-${sideTeamId}`) ?? null : null;
    const sideTierRaw = Number(sideCtx?.competition_tier ?? 99);
    const sideTier = Number.isFinite(sideTierRaw) ? sideTierRaw : 99;
    if (sideTier < 99) {
      const sideCeiling = isWomen
        ? (sideTier <= 1 ? 92 : sideTier <= 2 ? 78 : 22)
        : (sideTier <= 1 ? 92 : sideTier === 2 ? 78 : sideTier === 3 ? 64 : sideTier === 4 ? 50 : sideTier === 5 ? 36 : 28);
      if (sideCeiling < importanceCeiling) {
        importanceCeiling = sideCeiling;
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

    // Filter to players not already starting. Also exclude bench players — they're present.
    const benchIds = new Set((side === "home" ? home.bench : away.bench).map((p) => String(p.ksi_player_id)));
    const presentIds = new Set([...starterIds, ...benchIds]);
    const missingIds = likely.map((p) => String(p.ksi_player_id)).filter((id) => !presentIds.has(id));
    if (missingIds.length === 0) return { missing: [], missingImpact: 0 };

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

    // FIX 3: Load club context for missing players' clubs — these teams are not in
    // statTeamIds (which only covers players in today's lineup), so calcWeightedImportance
    // would treat all missing players as youth (ctx=null → isYouth=true → 0.35 discount),
    // severely underrating them and producing near-identical low scores.
    const missingTeamIds = Array.from(new Set(
      [...(missRows ?? []), ...(missPrevRows ?? [])]
        .map((r: any) => String(r.ksi_team_id ?? ""))
        .filter(Boolean)
        .filter((id) => !clubCtxBySeasonTeam.has(`${seasonYear}-${id}`) && !clubCtxBySeasonTeam.has(`${prevSeasonYear}-${id}`))
    ));

    if (missingTeamIds.length > 0) {
      const { data: missingClubRows } = await supabaseAdmin
        .from("computed_league_table")
        .select("season_year, team_ksi_id, ksi_competition_id, played, points, competition_tier, competition_name, competition_category, position")
        .in("season_year", [seasonYear, prevSeasonYear])
        .in("team_ksi_id", missingTeamIds);

      if (missingClubRows?.length) {
        ingestClubRows(missingClubRows);
      }
    }

    const missingBirthById = new Map<string, number | null>();
    for (const r of missingBirthRows ?? []) {
      missingBirthById.set(String(r.ksi_player_id), r.birth_year ?? null);
    }

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

    // Get the match team's tier ceiling for capping missing player importance
    const sideCtx = clubCtxBySeasonTeam.get(`${seasonYear}-${teamId}`) ?? null;
    const sideTierRaw = Number(sideCtx?.competition_tier ?? 99);
    const sideTier = Number.isFinite(sideTierRaw) ? sideTierRaw : 99;
    const sideCeiling = sideTier < 99
      ? (isWomen
        ? (sideTier <= 1 ? 92 : sideTier <= 2 ? 78 : 22)
        : (sideTier <= 1 ? 92 : sideTier === 2 ? 78 : sideTier === 3 ? 64 : sideTier === 4 ? 50 : sideTier === 5 ? 36 : 28))
      : 100;

    const missing = Array.from(missingRowsByPlayer.entries()).map(([pid, { rows, seasonCtx }]) => {
      const best = rows.reduce((a, b) => Number(a.minutes ?? 0) >= Number(b.minutes ?? 0) ? a : b);
      const { importance: rawImp, ceiling: rawCeiling } = calcWeightedImportance(rows, seasonCtx);

      // Apply the same side-tier cap as enrich() does for lineup players
      const importance = Math.min(rawImp, sideCeiling);
      const importanceCeiling = Math.min(rawCeiling, sideCeiling);

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

  // Goals per game lost from missing scorers — sum goals/maxGames for each missing player
  function missingGoalsPerGame(missing: any[], tier: number | null): number {
    const t = Number.isFinite(Number(tier)) ? Number(tier) : 3;
    const maxG = t <= 3 ? 22 : t === 4 ? 18 : 14;
    return missing.reduce((s, p) => s + (Number(p.goals ?? 0) / maxG), 0);
  }

  const goalsModel = computeGoals({
    homeTier,
    awayTier,
    homeStrength: homeStrength,  // raw historical strength, not lineup-blended
    awayStrength: awayStrength,  // same
    homeMissingGoals: missingGoalsPerGame(homeMissing.missing, homeTier),
    awayMissingGoals: missingGoalsPerGame(awayMissing.missing, awayTier),
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
    goals: goalsModel,

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