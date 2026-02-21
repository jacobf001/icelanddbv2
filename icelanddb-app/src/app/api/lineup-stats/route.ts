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
  position: number | null;
  played: number;
  points: number;
};

type TeamStrengthDebugRow = {
  team_ksi_id: string;
  competition_tier: number | null;
  competition_name: string | null;
  position: number | null;
  played: number;
  points: number;
  ppm: number;
  base: number; // clamp(ppm/3)
  scale: number; // tierScale
  strength: number; // clamp(base*scale)
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

/**
 * Tier penalty for points-per-match strength.
 * Lower tier number = higher division.
 */
function tierScale(tier: number) {
  const t = Number.isFinite(tier) ? tier : 99;
  if (t <= 1) return 1.0;
  if (t === 2) return 0.78;
  if (t === 3) return 0.58;
  if (t === 4) return 0.43;
  return 0.32;
}

/**
 * Direct “quality prior” by tier (mismatch games).
 */
function tierQualityN(tier: number | null | undefined) {
  const t = Number.isFinite(Number(tier)) ? Number(tier) : 6;
  if (t <= 1) return 1.0;
  if (t === 2) return 0.78;
  if (t === 3) return 0.58;
  if (t === 4) return 0.43;
  if (t === 5) return 0.32;
  return 0.25;
}

// rough “importance”
function calcImportance(params: {
  minutes: number;
  starts: number;
  goals: number;
  yellows: number;
  reds: number;
  teamStrength: number; // 0..1
}) {
  const minutesN = clamp01(params.minutes / (90 * 20));
  const startsN = clamp01(params.starts / 20);

  const goalsBoost = clamp01(params.goals / 10) * 0.25;
  const cardPenalty = clamp01(params.yellows * 0.02 + params.reds * 0.08);

  const base = minutesN * 0.55 + startsN * 0.35 + goalsBoost - cardPenalty;
  const scaled = base * (0.85 + 0.30 * params.teamStrength);

  return Math.max(0, Math.round(scaled * 100));
}

function sideRating(side: { starters: any[]; bench: any[] }, sideStrength: number) {
  const starterSum = side.starters.reduce((s, p) => s + Number(p.importance ?? 0), 0);
  const benchSum = side.bench.reduce((s, p) => s + Number(p.importance ?? 0), 0);

  const raw = starterSum + benchSum * 0.35;
  const scaled = raw * (0.85 + 0.30 * (Number.isFinite(sideStrength) ? sideStrength : 0.5));

  const startersKnown = side.starters.filter((p) => p.season != null).length;
  const coverage = side.starters.length ? startersKnown / side.starters.length : 0;

  return {
    starters: Math.round(starterSum),
    bench: Math.round(benchSum),
    raw: Math.round(raw),
    total: Math.round(scaled),
    coverage,
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

  const overallN = 0.30 * tierN + 0.25 * strengthN + 0.35 * lineupN + 0.15 * coverageN - 0.05 * missingN;
  return Math.round(clamp01(overallN) * 100);
}

function computeOdds(params: { homeOverall: number; awayOverall: number; homeTier: number | null; awayTier: number | null }) {
  const diffOverall = (params.homeOverall - params.awayOverall) / 12;

  const homeTier = Number.isFinite(Number(params.homeTier)) ? Number(params.homeTier) : 6;
  const awayTier = Number.isFinite(Number(params.awayTier)) ? Number(params.awayTier) : 6;

  // >0 means home is in better tier
  const tierAdv = clamp((awayTier - homeTier) * 0.85, -4, 4);

  const z = diffOverall + tierAdv;

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

  // choose per player the row with most minutes (handles transfers)
  const bestRowByPlayer = new Map<string, any>();
  for (const r of seasonRows ?? []) {
    const id = String((r as any).ksi_player_id);
    const prev = bestRowByPlayer.get(id);
    if (!prev || Number((r as any).minutes ?? 0) > Number((prev as any).minutes ?? 0)) bestRowByPlayer.set(id, r);
  }

  const bestPrevRowByPlayer = new Map<string, any>();
  for (const r of prevSeasonRows ?? []) {
    const id = String((r as any).ksi_player_id);
    const prev = bestPrevRowByPlayer.get(id);
    if (!prev || Number((r as any).minutes ?? 0) > Number((prev as any).minutes ?? 0)) bestPrevRowByPlayer.set(id, r);
  }

  // team name lookup (for any team appearing in either season rows)
  const seasonTeamIds = Array.from(
    new Set(
      [...(seasonRows ?? []), ...(prevSeasonRows ?? [])].map((r: any) => String(r.ksi_team_id)).filter(Boolean),
    ),
  );

  const teamNameById = new Map<string, string>();
  if (seasonTeamIds.length) {
    const { data: tnRows, error: tnErr } = await supabaseAdmin.from("teams").select("ksi_team_id, name").in("ksi_team_id", seasonTeamIds);
    if (tnErr) return NextResponse.json({ error: tnErr.message }, { status: 500 });

    for (const t of tnRows ?? []) {
      const id = String((t as any).ksi_team_id);
      const nm = (t as any).name ?? null;
      if (nm) teamNameById.set(id, String(nm));
    }
  }

  // 3) Team strength + context for HOME/AWAY (current + prev)
  const homeTeamId = teams.home.ksi_team_id;
  const awayTeamId = teams.away.ksi_team_id;
  const teamIdsToLoad = [homeTeamId, awayTeamId].filter(Boolean) as string[];

  const strengthByTeam = new Map<string, number>();
  const tierByTeam = new Map<string, number>();
  const teamStrengthDebug = new Map<string, TeamStrengthDebugRow>();

  if (teamIdsToLoad.length) {
    const { data: tableRows, error: tableErr } = await supabaseAdmin
      .from("computed_league_table")
      .select("team_ksi_id, played, points, competition_tier, competition_name, position")
      .eq("season_year", seasonYear)
      .in("team_ksi_id", teamIdsToLoad);

    if (tableErr) return NextResponse.json({ error: tableErr.message }, { status: 500 });

    const bestRowByTeam = pickBestRowPerTeam(tableRows ?? []);

    for (const [id, row] of bestRowByTeam.entries()) {
      const played = Number((row as any).played ?? 0);
      const points = Number((row as any).points ?? 0);
      const tier = Number((row as any).competition_tier ?? 99);

      const competition_name = (row as any).competition_name ?? null;
      const position = Number.isFinite(Number((row as any).position)) ? Number((row as any).position) : null;

      const ppm = played > 0 ? points / played : 0;
      const base = clamp01(ppm / 3);
      const scale = tierScale(tier);
      const strength = clamp01(base * scale);

      strengthByTeam.set(id, strength);
      tierByTeam.set(id, Number.isFinite(tier) ? tier : 99);

      teamStrengthDebug.set(id, {
        team_ksi_id: id,
        competition_tier: Number.isFinite(tier) ? tier : null,
        competition_name,
        position,
        played,
        points,
        ppm,
        base,
        scale,
        strength,
        prev: null,
      });
    }

    const { data: prevRows, error: prevErr } = await supabaseAdmin
      .from("computed_league_table")
      .select("team_ksi_id, played, points, competition_tier, competition_name, position")
      .eq("season_year", prevSeasonYear)
      .in("team_ksi_id", teamIdsToLoad);

    if (prevErr) return NextResponse.json({ error: prevErr.message }, { status: 500 });

    const prevBest = pickBestRowPerTeam(prevRows ?? []);
    for (const [id, row] of prevBest.entries()) {
      const dbg = teamStrengthDebug.get(id);
      if (!dbg) continue;

      dbg.prev = {
        season_year: prevSeasonYear,
        team_ksi_id: id,
        competition_tier: Number.isFinite(Number((row as any).competition_tier)) ? Number((row as any).competition_tier) : null,
        competition_name: (row as any).competition_name ?? null,
        position: Number.isFinite(Number((row as any).position)) ? Number((row as any).position) : null,
        played: Number((row as any).played ?? 0),
        points: Number((row as any).points ?? 0),
      };
    }
  }

  const homeStrength = homeTeamId ? (strengthByTeam.get(homeTeamId) ?? 0.5) : 0.5;
  const awayStrength = awayTeamId ? (strengthByTeam.get(awayTeamId) ?? 0.5) : 0.5;

  const homeTier = homeTeamId ? (tierByTeam.get(homeTeamId) ?? null) : null;
  const awayTier = awayTeamId ? (tierByTeam.get(awayTeamId) ?? null) : null;

  // --- NEW: club finishing context for STAT CLUBS (player season teams) ---
  const statTeamIds = Array.from(
    new Set(
      [...(seasonRows ?? []), ...(prevSeasonRows ?? [])].map((r: any) => String(r.ksi_team_id)).filter(Boolean),
    ),
  );

  const clubCtxBySeasonTeam = new Map<string, TeamSeasonContext>(); // key: `${season}-${teamId}`

  if (statTeamIds.length) {
    const { data: clubRows, error: clubErr } = await supabaseAdmin
      .from("computed_league_table")
      .select("season_year, team_ksi_id, played, points, competition_tier, competition_name, position")
      .in("season_year", [seasonYear, prevSeasonYear])
      .in("team_ksi_id", statTeamIds);

    if (clubErr) return NextResponse.json({ error: clubErr.message }, { status: 500 });

    // group by season-team
    const grouped = new Map<string, any[]>();
    for (const r of clubRows ?? []) {
      const k = `${r.season_year}-${r.team_ksi_id}`;
      const arr = grouped.get(k) ?? [];
      arr.push(r);
      grouped.set(k, arr);
    }

    for (const [k, rows] of grouped.entries()) {
      // pick best within that season-team group
      const best = Array.from(pickBestRowPerTeam(rows).values())[0];
      if (!best) continue;

      const season = Number(best.season_year);
      const teamId = String(best.team_ksi_id);

      clubCtxBySeasonTeam.set(`${season}-${teamId}`, {
        season_year: season,
        team_ksi_id: teamId,
        competition_tier: Number.isFinite(Number(best.competition_tier)) ? Number(best.competition_tier) : null,
        competition_name: best.competition_name ?? null,
        position: Number.isFinite(Number(best.position)) ? Number(best.position) : null,
        played: Number(best.played ?? 0),
        points: Number(best.points ?? 0),
      });
    }
  }
  // --- END NEW ---

  // 4) Recent form from match_lineups
  const { data: lineupRows, error: lineupErr } = await supabaseAdmin
    .from("match_lineups")
    .select("ksi_player_id, ksi_match_id, minute_in, minute_out")
    .in("ksi_player_id", allIds);

  if (lineupErr) return NextResponse.json({ error: lineupErr.message }, { status: 500 });

  const matchIds = Array.from(new Set((lineupRows ?? []).map((r: any) => String(r.ksi_match_id)).filter(Boolean)));

  const kickoffMap = new Map<string, number>();
  if (matchIds.length) {
    const { data: matchRows, error: matchErr } = await supabaseAdmin.from("matches").select("ksi_match_id, kickoff_at").in("ksi_match_id", matchIds);
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

  function enrich(p: LineupPlayer, side: "home" | "away") {
    const r = bestRowByPlayer.get(String(p.ksi_player_id)) ?? null;
    const pr = bestPrevRowByPlayer.get(String(p.ksi_player_id)) ?? null;

    const minutes = Number(r?.minutes ?? 0);
    const starts = Number(r?.starts ?? 0);
    const goals = Number(r?.goals ?? 0);
    const yellows = Number(r?.yellows ?? 0);
    const reds = Number(r?.reds ?? 0);

    const statTeamId = r?.ksi_team_id ? String(r.ksi_team_id) : null;
    const prevTeamId = pr?.ksi_team_id ? String(pr.ksi_team_id) : null;

    // player club context
    const seasonClubCtx = statTeamId ? clubCtxBySeasonTeam.get(`${seasonYear}-${statTeamId}`) ?? null : null;
    const prevClubCtx = prevTeamId ? clubCtxBySeasonTeam.get(`${prevSeasonYear}-${prevTeamId}`) ?? null : null;

    // prefer player's stat team strength; fallback to side strength
    const sideStrength = side === "home" ? homeStrength : awayStrength;
    const strength = statTeamId ? (strengthByTeam.get(statTeamId) ?? sideStrength) : sideStrength;

    return {
      ...p,
      season: r
        ? {
            season_year: seasonYear,
            ksi_team_id: statTeamId,
            team_name: statTeamId ? (teamNameById.get(statTeamId) ?? null) : null,
            player_name: r.player_name ?? p.name,
            matches_played: Number(r.matches_played ?? 0),
            starts,
            minutes,
            goals,
            yellows,
            reds,
            club_ctx: seasonClubCtx,
          }
        : null,

      prevSeason: pr
        ? {
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
          }
        : null,

      recent5: lastN(String(p.ksi_player_id), 5),
      importance: r ? calcImportance({ minutes, starts, goals, yellows, reds, teamStrength: strength }) : 0,
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

  // 5) Missing Likely XI (impact)
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

    const sideStrength = side === "home" ? homeStrength : awayStrength;

    const missing = (missRows ?? []).map((r: any) => {
      const minutes = Number(r.minutes ?? 0);
      const starts = Number(r.starts ?? 0);
      const goals = Number(r.goals ?? 0);
      const yellows = Number(r.yellows ?? 0);
      const reds = Number(r.reds ?? 0);

      return {
        ksi_player_id: String(r.ksi_player_id),
        player_name: r.player_name ?? null,
        starts,
        minutes,
        goals,
        importance: calcImportance({ minutes, starts, goals, yellows, reds, teamStrength: sideStrength }),
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
    teamStrength: homeStrength,
    tier: homeTier,
    total: homeRating.total,
    coverage: homeRating.coverage,
    missingImpact: homeMissing.missingImpact,
  });

  const awayOverall = computeOverall({
    teamStrength: awayStrength,
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

    teamStrength: { home: homeStrength, away: awayStrength },
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