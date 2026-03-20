// src/lib/model/analyzeMatchById.ts
import { supabaseAdmin } from "@/lib/supabaseServer";
import { getLikelyXI } from "@/lib/queries";

type LineupPlayer = {
  ksi_player_id: string;
  name: string | null;
  shirt_no: number | null;
};

type TeamLineup = {
  starters: LineupPlayer[];
  bench: LineupPlayer[];
};

export type MatchModelOutput = {
  match_id: string;
  kickoff_at: string | null;
  season_year: number;
  home_team_ksi_id: string | null;
  away_team_ksi_id: string | null;
  home_team_name: string | null;
  away_team_name: string | null;

  home_competition_name: string | null;
  away_competition_name: string | null;
  home_tier: number | null;
  away_tier: number | null;

  teamStrength: { home: number; away: number };
  overall: { home: number; away: number };
  probabilities: { home: number; draw: number; away: number };
  fair_odds: { home: number | null; draw: number | null; away: number | null };

  home: {
    rating: {
      starters: number;
      bench: number;
      raw: number;
      total: number;
      coverage: number;
      effectiveStrength: number;
    };
    missingImpact: number;
  };

  away: {
    rating: {
      starters: number;
      bench: number;
      raw: number;
      total: number;
      coverage: number;
      effectiveStrength: number;
    };
    missingImpact: number;
  };

  women: boolean;
  model_version: string;
};

// Move these helpers from route.ts into a shared file later if you want.
function clamp01(x: number) {
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(1, x));
}

function clamp(x: number, lo: number, hi: number) {
  if (!Number.isFinite(x)) return lo;
  return Math.max(lo, Math.min(hi, x));
}

function sigmoid(x: number) {
  return 1 / (1 + Math.exp(-x));
}

function minutesFromLineupRow(r: { minute_in: number | null; minute_out: number | null }) {
  const minIn = r.minute_in ?? 0;
  const minOut = r.minute_out ?? 90;
  return Math.max(0, Math.min(90, minOut) - Math.max(0, Math.min(90, minIn)));
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

function calcImportance(params: {
  minutes: number;
  starts: number;
  goals: number;
  yellows: number;
  reds: number;
  maxGames: number;
}) {
  const maxMins = params.maxGames * 90;
  const minutesN = clamp01(params.minutes / maxMins);
  const startsN = clamp01(params.starts / params.maxGames);
  const goalsBoost = clamp01(params.goals / 12) * 0.15;
  const cardPenalty = clamp01(params.yellows * 0.02 + params.reds * 0.08);
  const base = minutesN * 0.35 + startsN * 0.55 + goalsBoost - cardPenalty;
  return Math.max(0, Math.round(base * 100));
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
  const effectiveStrength = clamp01(cappedHistStrength * histWeight + avgStarterImp * lineupWeight);

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

  const overallN =
    0.18 * tierN +
    0.30 * strengthN +
    0.42 * lineupN +
    0.10 * coverageN -
    0.12 * missingN;

  return Math.round(clamp01(overallN) * 100);
}

function computeOdds(params: {
  homeTier: number | null;
  awayTier: number | null;
  homeRawStrength: number;
  awayRawStrength: number;
  homeLineupTotal: number;
  awayLineupTotal: number;
  homeMissingImpact: number;
  awayMissingImpact: number;
}) {
  const homeTier = Number.isFinite(Number(params.homeTier)) ? Number(params.homeTier) : 6;
  const awayTier = Number.isFinite(Number(params.awayTier)) ? Number(params.awayTier) : 6;

  const rawStrengthDiff = clamp(params.homeRawStrength - params.awayRawStrength, -1, 1);
  const strengthZ = rawStrengthDiff * 6.5;

  const MISSING_CEILINGS: Record<number, number> = { 1: 92, 2: 78, 3: 64, 4: 50, 5: 36 };
  const homeMissingNorm = clamp(params.homeMissingImpact / ((MISSING_CEILINGS[homeTier] ?? 64) * 4), 0, 1);
  const awayMissingNorm = clamp(params.awayMissingImpact / ((MISSING_CEILINGS[awayTier] ?? 64) * 4), 0, 1);
  const missingAdj = (awayMissingNorm - homeMissingNorm) * 0.9;

  function lineupBaseline(tier: number): number {
    if (tier <= 1) return 500;
    if (tier === 2) return 380;
    if (tier === 3) return 300;
    if (tier === 4) return 220;
    return 160;
  }

  const homeLineupRatio = clamp(params.homeLineupTotal / lineupBaseline(homeTier), 0, 1.5);
  const awayLineupRatio = clamp(params.awayLineupTotal / lineupBaseline(awayTier), 0, 1.5);
  const lineupZ = (homeLineupRatio - awayLineupRatio) * 1.2;

  const tierAdv = clamp((awayTier - homeTier) * 1.0, -3.0, 3.0);

  const avgTier = ((homeTier ?? 3) + (awayTier ?? 3)) / 2;
  const homeAdv = clamp(0.40 - (avgTier - 1) * 0.10, 0.05, 0.40);

  const z = strengthZ + lineupZ + missingAdj + tierAdv + homeAdv;

  const pHomeRaw = sigmoid(z);
  const pAwayRaw = 1 - pHomeRaw;

  // ---- NEW DRAW CALIBRATION LAYER ----
  const tierAvg = Math.round((homeTier + awayTier) / 2);

  // Base draw by tier from your evaluation:
  // T1/T2 need more draw, T3/T4 about right, T5/T6 a bit less draw
  function baseDrawForTier(t: number): number {
    if (t <= 1) return 0.29;
    if (t === 2) return 0.27;
    if (t === 3) return 0.23;
    if (t === 4) return 0.21;
    if (t === 5) return 0.18;
    return 0.17;
  }

  // Stronger favourites should still reduce draw, but less brutally than before
  const gap = Math.abs(z);
  let pDraw = baseDrawForTier(tierAvg) - gap * 0.045;

  // Small tier-specific multiplier as a second calibration pass
  function drawMultiplierForTier(t: number): number {
    if (t <= 1) return 1.04; // more draw
    if (t === 2) return 1.09; // more draw
    if (t === 3) return 1.00; // leave about neutral
    if (t === 4) return 0.98; // tiny reduction
    if (t === 5) return 0.96; // less draw
    return 0.92; // less draw
  }

  pDraw = pDraw * drawMultiplierForTier(tierAvg);

  // Safer floors/ceilings by tier
  function drawBoundsForTier(t: number): [number, number] {
    if (t <= 1) return [0.14, 0.34];
    if (t === 2) return [0.12, 0.31];
    if (t === 3) return [0.10, 0.27];
    if (t === 4) return [0.09, 0.24];
    if (t === 5) return [0.07, 0.21];
    return [0.06, 0.20];
  }

  const [drawLo, drawHi] = drawBoundsForTier(tierAvg);
  pDraw = clamp(pDraw, drawLo, drawHi);

  // Then allocate the non-draw mass to home/away
  let pHome = (1 - pDraw) * pHomeRaw;
  let pAway = (1 - pDraw) * pAwayRaw;

  // Mild confidence cap on strong favourites, especially useful in T1/T2
  function maxSideProbForTier(t: number): number {
    if (t <= 1) return 0.78;
    if (t === 2) return 0.76;
    if (t === 3) return 0.80;
    if (t === 4) return 0.83;
    if (t === 5) return 0.86;
    return 0.85;
  }

  const maxSide = maxSideProbForTier(tierAvg);

  if (pHome > maxSide) {
    const excess = pHome - maxSide;
    pHome = maxSide;
    pDraw += excess * 0.60;
    pAway += excess * 0.40;
  }

  if (pAway > maxSide) {
    const excess = pAway - maxSide;
    pAway = maxSide;
    pDraw += excess * 0.60;
    pHome += excess * 0.40;
  }

  // Final renormalisation
  const total = pHome + pDraw + pAway;
  pHome = pHome / total;
  pDraw = pDraw / total;
  pAway = pAway / total;

  return {
    probabilities: { home: pHome, draw: pDraw, away: pAway },
    fair_odds: {
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

export async function analyzeMatchById(matchId: string): Promise<MatchModelOutput | null> {
  const { data: match, error: matchErr } = await supabaseAdmin
    .from("matches")
    .select("ksi_match_id, season_year, kickoff_at, home_team_ksi_id, away_team_ksi_id, home_score, away_score")
    .eq("ksi_match_id", matchId)
    .maybeSingle();

  if (matchErr) throw new Error(matchErr.message);
  if (!match) return null;

  const seasonYear = Number((match as any).season_year);
  const prevSeasonYear = seasonYear - 1;

  const homeTeamId = (match as any).home_team_ksi_id ? String((match as any).home_team_ksi_id) : null;
  const awayTeamId = (match as any).away_team_ksi_id ? String((match as any).away_team_ksi_id) : null;

  if (!homeTeamId || !awayTeamId) return null;

  const [{ data: lineupRows, error: lineupErr }, { data: teamRows, error: teamErr }] = await Promise.all([
    supabaseAdmin
      .from("match_lineups")
      .select("ksi_player_id, ksi_team_id, player_name, shirt_number, minute_in, minute_out, squad")
      .eq("ksi_match_id", matchId),
    supabaseAdmin
      .from("teams")
      .select("ksi_team_id, name")
      .in("ksi_team_id", [homeTeamId, awayTeamId]),
  ]);

  if (lineupErr) throw new Error(lineupErr.message);
  if (teamErr) throw new Error(teamErr.message);

  const teamNameById = new Map<string, string | null>();
  for (const t of teamRows ?? []) {
    teamNameById.set(String((t as any).ksi_team_id), (t as any).name ?? null);
  }

  const playerIds = Array.from(new Set((lineupRows ?? []).map((r: any) => String(r.ksi_player_id))));
  if (!playerIds.length) return null;

  const home: TeamLineup = { starters: [], bench: [] };
  const away: TeamLineup = { starters: [], bench: [] };

  for (const r of lineupRows ?? []) {
    const pid = String((r as any).ksi_player_id);
    const tid = String((r as any).ksi_team_id);

    const item: LineupPlayer = {
      ksi_player_id: pid,
      name: (r as any).player_name ?? null,
      shirt_no: (r as any).shirt_number ?? null,
    };

    const isStarter =
      ((r as any).minute_in === null || (r as any).minute_in === 0) &&
      String((r as any).squad ?? "").toLowerCase() !== "bench";

    if (tid === homeTeamId) {
      if (isStarter) home.starters.push(item);
      else home.bench.push(item);
    }
    if (tid === awayTeamId) {
      if (isStarter) away.starters.push(item);
      else away.bench.push(item);
    }
  }

  const allIds = [...home.starters, ...home.bench, ...away.starters, ...away.bench].map((p) => p.ksi_player_id);

  const [
  { data: seasonRows, error: seasonErr },
  { data: prevSeasonRows, error: prevSeasonErr },
] = await Promise.all([
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

  if (seasonErr) throw new Error(seasonErr.message);
  if (prevSeasonErr) throw new Error(prevSeasonErr.message);

  const allRowsByPlayer = new Map<string, any[]>();
  for (const r of seasonRows ?? []) {
    const id = String((r as any).ksi_player_id);
    const arr = allRowsByPlayer.get(id) ?? [];
    arr.push(r);
    allRowsByPlayer.set(id, arr);
  }

  const allPrevRowsByPlayer = new Map<string, any[]>();
  for (const r of prevSeasonRows ?? []) {
    const id = String((r as any).ksi_player_id);
    const arr = allPrevRowsByPlayer.get(id) ?? [];
    arr.push(r);
    allPrevRowsByPlayer.set(id, arr);
  }

  const statTeamIds = Array.from(
    new Set(
      [...(seasonRows ?? []), ...(prevSeasonRows ?? [])]
        .map((r: any) => String(r.ksi_team_id))
        .filter(Boolean),
    ),
  );

  const teamIdsToLoad = Array.from(new Set([homeTeamId, awayTeamId, ...statTeamIds].filter(Boolean))) as string[];

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

  if (curErr) throw new Error(curErr.message);
  if (prevErr) throw new Error(prevErr.message);

  const leagueSizeByComp = new Map<string, number>();
  for (const r of curRows ?? []) {
    const compId = String((r as any).ksi_competition_id ?? "");
    if (!compId) continue;
    leagueSizeByComp.set(compId, (leagueSizeByComp.get(compId) ?? 0) + 1);
  }

  const prevLeagueSizeByComp = new Map<string, number>();
  for (const r of prevRows ?? []) {
    const compId = String((r as any).ksi_competition_id ?? "");
    if (!compId) continue;
    prevLeagueSizeByComp.set(compId, (prevLeagueSizeByComp.get(compId) ?? 0) + 1);
  }

  const bestCur = pickBestRowPerTeam(curRows ?? []);
  const bestPrev = pickBestRowPerTeam(prevRows ?? []);

  const strengthByTeam = new Map<string, number>();
  const tierByTeam = new Map<string, number>();
  const compNameByTeam = new Map<string, string>();

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

    const compName = cur?.competition_name ?? prev?.competition_name ?? null;
    if (compName) compNameByTeam.set(id, compName);
  }

  const homeStrength = strengthByTeam.get(homeTeamId) ?? 0.5;
  const awayStrength = strengthByTeam.get(awayTeamId) ?? 0.5;

  const homeTier = tierByTeam.get(homeTeamId) ?? null;
  const awayTier = tierByTeam.get(awayTeamId) ?? null;

  const homeCompName = compNameByTeam.get(homeTeamId) ?? null;
  const awayCompName = compNameByTeam.get(awayTeamId) ?? null;
  const isWomen = isWomensCompetition(homeCompName) || isWomensCompetition(awayCompName);

  function maxGamesForTier(tier: number, women: boolean): number {
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

  function calcWeightedImportance(playerRows: any[], goalsOverride?: number, yellowsOverride?: number, redsOverride?: number) {
    let totalMins = 0;
    let totalStarts = 0;
    let totalGoals = goalsOverride ?? 0;
    let totalYellows = yellowsOverride ?? 0;
    let totalReds = redsOverride ?? 0;

    let primaryTier = 3;
    for (const row of playerRows) {
      totalMins += Number(row.minutes ?? 0);
      totalStarts += Number(row.starts ?? 0);
      if (goalsOverride === undefined) totalGoals += Number(row.goals ?? 0);
      if (yellowsOverride === undefined) totalYellows += Number(row.yellows ?? 0);
      if (redsOverride === undefined) totalReds += Number(row.reds ?? 0);
    }

    const teamIds = Array.from(new Set(playerRows.map((r: any) => String(r.ksi_team_id)).filter(Boolean)));
    for (const tid of teamIds) {
      const t = tierByTeam.get(tid);
      if (t != null) primaryTier = Math.min(primaryTier, t);
    }

    const maxGames = maxGamesForTier(primaryTier, isWomen);
    return calcImportance({
      minutes: totalMins,
      starts: totalStarts,
      goals: totalGoals,
      yellows: totalYellows,
      reds: totalReds,
      maxGames,
    });
  }

  function enrich(p: LineupPlayer, side: "home" | "away") {
    const playerRows = allRowsByPlayer.get(String(p.ksi_player_id)) ?? [];
    const prevPlayerRows = allPrevRowsByPlayer.get(String(p.ksi_player_id)) ?? [];

    let importance = 0;
    if (playerRows.length > 0 && prevPlayerRows.length > 0) {
      const curr = calcWeightedImportance(playerRows);
      const prev = calcWeightedImportance(prevPlayerRows);
      const currMins = playerRows.reduce((s: number, r: any) => s + Number(r.minutes ?? 0), 0);
      const prevWeight = Math.max(0, 1 - currMins / 500) * 0.30;
      importance = Math.round(curr * (1 - prevWeight) + prev * prevWeight);
    } else if (playerRows.length > 0) {
      importance = calcWeightedImportance(playerRows);
    } else if (prevPlayerRows.length > 0) {
      importance = calcWeightedImportance(prevPlayerRows);
    }

    return {
      ...p,
      season: playerRows[0] ?? null,
      importance,
    };
  }

  const homeEnriched = {
    starters: home.starters.map((p) => enrich(p, "home")),
    bench: home.bench.map((p) => enrich(p, "home")),
  };

  const awayEnriched = {
    starters: away.starters.map((p) => enrich(p, "away")),
    bench: away.bench.map((p) => enrich(p, "away")),
  };

  async function buildMissingLikelyXI(teamId: string, starters: any[]) {
    const likely = await getLikelyXI(teamId, seasonYear);
    const starterIds = new Set(starters.map((p) => String(p.ksi_player_id)));
    const missingIds = likely.map((p) => String(p.ksi_player_id)).filter((id) => !starterIds.has(id));
    if (!missingIds.length) return { missingImpact: 0 };

    const { data: missRows, error: missErr } = await supabaseAdmin
      .from("player_season_to_date")
      .select("ksi_player_id, starts, minutes, goals, yellows, reds, ksi_team_id")
      .eq("season_year", seasonYear)
      .eq("ksi_team_id", teamId)
      .in("ksi_player_id", missingIds);

    if (missErr) throw new Error(missErr.message);

    const grouped = new Map<string, any[]>();
    for (const r of missRows ?? []) {
      const pid = String((r as any).ksi_player_id);
      const arr = grouped.get(pid) ?? [];
      arr.push(r);
      grouped.set(pid, arr);
    }

    let missingImpact = 0;
    for (const rows of grouped.values()) {
      missingImpact += calcWeightedImportance(rows);
    }

    return { missingImpact };
  }

  const [homeMissing, awayMissing] = await Promise.all([
    buildMissingLikelyXI(homeTeamId, homeEnriched.starters),
    buildMissingLikelyXI(awayTeamId, awayEnriched.starters),
  ]);

  const homeRating = sideRating(homeEnriched, homeStrength, homeMissing.missingImpact);
  const awayRating = sideRating(awayEnriched, awayStrength, awayMissing.missingImpact);

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
    homeTier,
    awayTier,
    homeRawStrength: homeRating.effectiveStrength,
    awayRawStrength: awayRating.effectiveStrength,
    homeLineupTotal: homeRating.total,
    awayLineupTotal: awayRating.total,
    homeMissingImpact: homeMissing.missingImpact,
    awayMissingImpact: awayMissing.missingImpact,
  });

  return {
    match_id: String((match as any).ksi_match_id),
    kickoff_at: (match as any).kickoff_at ?? null,
    season_year: seasonYear,
    home_team_ksi_id: homeTeamId,
    away_team_ksi_id: awayTeamId,
    home_team_name: teamNameById.get(homeTeamId) ?? null,
    away_team_name: teamNameById.get(awayTeamId) ?? null,

    home_competition_name: homeCompName,
    away_competition_name: awayCompName,
    home_tier: homeTier,
    away_tier: awayTier,

    teamStrength: { home: homeRating.effectiveStrength, away: awayRating.effectiveStrength },
    overall: { home: homeOverall, away: awayOverall },
    probabilities: pricing.probabilities,
    fair_odds: pricing.fair_odds,
    home: { rating: homeRating, missingImpact: homeMissing.missingImpact },
    away: { rating: awayRating, missingImpact: awayMissing.missingImpact },
    women: isWomen,
    model_version: "v2_tier_draw",
  };
}