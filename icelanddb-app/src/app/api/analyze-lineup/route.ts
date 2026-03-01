import { NextResponse } from "next/server";
import { getMatchMeta } from "@/lib/queries";
import { supabaseAdmin } from "@/lib/supabaseServer";

type LineupPlayer = { ksi_player_id: string; name: string; shirt_no: number | null };
type TeamLineup = { starters: LineupPlayer[]; bench: LineupPlayer[] };

type LineupsFromReportResponse = {
  inputUrl: string;
  fetchUrl: string;
  home: TeamLineup;
  away: TeamLineup;
};

function parseIdText(input: string | null): string | null {
  const t = (input ?? "").trim();
  if (!t) return null;
  const m = t.match(/\d+/);
  return m ? m[0] : null;
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

function clamp01(x: number) {
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(1, x));
}

// tune later
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
  const scaled = base * (0.85 + 0.3 * params.teamStrength);
  return Math.max(0, Math.round(scaled * 100));
}

function yearFromKickoff(kickoff_at: string | null) {
  if (!kickoff_at) return null;
  const t = Date.parse(kickoff_at);
  if (!Number.isFinite(t)) return null;
  return new Date(t).getUTCFullYear();
}

function minutesFromLineupRow(r: { minute_in: number | null; minute_out: number | null }) {
  const minIn = r.minute_in ?? 0;
  const minOut = r.minute_out ?? 90;
  return Math.max(0, Math.min(90, minOut) - Math.max(0, minIn));
}

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);

  const inputUrl = searchParams.get("url");
  if (!inputUrl) {
    return NextResponse.json({ error: "Missing params", expected: "url" }, { status: 400 });
  }

  // derive matchId from either ?matchId= or from the pasted KSI URL (?id=...)
  const matchId =
    parseIdText(searchParams.get("matchId")) ??
    parseIdText(searchParams.get("id")) ??
    (() => {
      try {
        return parseIdText(new URL(inputUrl).searchParams.get("id"));
      } catch {
        return null;
      }
    })();

  if (!matchId) {
    return NextResponse.json(
      { error: "Could not derive matchId", expected: "url containing ?id=#### or matchId=####" },
      { status: 400 },
    );
  }

  // get kickoff + team ids from matches (FIX: correct column names)
  const meta = await getMatchMeta(matchId);
  if (!meta) return NextResponse.json({ error: "Match not found", matchId }, { status: 404 });

  const seasonYear = yearFromKickoff(meta.kickoff_at);
  if (!seasonYear) {
    return NextResponse.json(
      { error: "Match kickoff_at missing/invalid, cannot determine season year", matchId },
      { status: 400 },
    );
  }

  // 1) Fetch parsed lineups from your existing parser route
  const base = new URL(req.url);
  const origin = `${base.protocol}//${base.host}`;

  const lineupRes = await fetch(
    `${origin}/api/lineups-from-report?` + new URLSearchParams({ url: inputUrl }).toString(),
    { cache: "no-store" },
  );

  const lineupJson = (await lineupRes.json()) as LineupsFromReportResponse & { error?: string };
  if (!lineupRes.ok || lineupJson.error) {
    return NextResponse.json(
      { error: lineupJson.error ?? "Failed to parse lineups" },
      { status: 400 },
    );
  }

  const homePlayers = uniqById([...lineupJson.home.starters, ...lineupJson.home.bench]);
  const awayPlayers = uniqById([...lineupJson.away.starters, ...lineupJson.away.bench]);
  const allIds = uniqById([...homePlayers, ...awayPlayers]).map((p) => p.ksi_player_id);

  if (allIds.length === 0) {
    return NextResponse.json({ error: "No players parsed from lineups" }, { status: 200 });
  }

  // 2) Season-to-date player rows
  const { data: seasonRows, error: seasonErr } = await supabaseAdmin
    .from("player_season_to_date")
    .select("season_year, ksi_team_id, ksi_player_id, player_name, matches_played, starts, minutes, goals, yellows, reds")
    .eq("season_year", seasonYear)
    .in("ksi_player_id", allIds);

  if (seasonErr) return NextResponse.json({ error: seasonErr.message }, { status: 500 });

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

  const bestRowByPlayer = new Map<string, any>();
  for (const r of seasonRows ?? []) {
    const id = String((r as any).ksi_player_id);
    const prev = bestRowByPlayer.get(id);
    if (!prev || Number((r as any).minutes ?? 0) > Number((prev as any).minutes ?? 0)) {
      bestRowByPlayer.set(id, r);
    }
  }

  const teamIds = Array.from(
    new Set((seasonRows ?? []).map((r: any) => String(r.ksi_team_id)).filter(Boolean)),
  );

  // 3) Team strength from team_season_to_date
  const { data: teamRows, error: teamErr } = await supabaseAdmin
    .from("team_season_to_date")
    .select("ksi_team_id, team_name, matches_played, points, goal_diff")
    .eq("season_year", seasonYear)
    .in("ksi_team_id", teamIds);

  if (teamErr) return NextResponse.json({ error: teamErr.message }, { status: 500 });

  const teamStrength = new Map<string, number>();
  for (const t of teamRows ?? []) {
    const mp = Number((t as any).matches_played ?? 0);
    const pts = Number((t as any).points ?? 0);
    const ppm = mp > 0 ? pts / mp : 0; // 0..3
    teamStrength.set(String((t as any).ksi_team_id), clamp01(ppm / 3));
  }

  // 4) Recent form (FIX: match_lineups has NO season_year)
  // Pull matches in this year, then fetch match_lineups for those match ids.
  const start = `${seasonYear}-01-01T00:00:00.000Z`;
  const end = `${seasonYear}-12-31T23:59:59.999Z`;

  const { data: seasonMatches, error: seasonMatchesErr } = await supabaseAdmin
    .from("matches")
    .select("ksi_match_id,kickoff_at")
    .gte("kickoff_at", start)
    .lte("kickoff_at", end);

  if (seasonMatchesErr) return NextResponse.json({ error: seasonMatchesErr.message }, { status: 500 });

  const seasonMatchIds = (seasonMatches ?? []).map((m: any) => String(m.ksi_match_id)).filter(Boolean);

  // safety: avoid giant IN()
  const seasonMatchIdsLimited = seasonMatchIds.slice(0, 5000);

  const { data: lineupRows, error: lineupErr } = await supabaseAdmin
    .from("match_lineups")
    .select("ksi_player_id, ksi_match_id, minute_in, minute_out")
    .in("ksi_player_id", allIds)
    .in("ksi_match_id", seasonMatchIdsLimited);

  if (lineupErr) return NextResponse.json({ error: lineupErr.message }, { status: 500 });

  const kickoffMap = new Map<string, number>();
  for (const m of seasonMatches ?? []) {
    const k = String((m as any).ksi_match_id);
    const t = (m as any).kickoff_at ? Date.parse((m as any).kickoff_at) : 0;
    kickoffMap.set(k, Number.isFinite(t) ? t : 0);
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
    return {
      lastNApps: take.length,
      lastNMinutes: take.reduce((s, x) => s + (x.minutes ?? 0), 0),
      lastNStarts: take.reduce((s, x) => s + (x.started ? 1 : 0), 0),
    };
  }

  function enrich(p: LineupPlayer) {
    const r = bestRowByPlayer.get(String(p.ksi_player_id)) ?? null;

    const minutes = Number(r?.minutes ?? 0);
    const starts = Number(r?.starts ?? 0);
    const goals = Number(r?.goals ?? 0);
    const yellows = Number(r?.yellows ?? 0);
    const reds = Number(r?.reds ?? 0);
    const teamId = r?.ksi_team_id ? String(r.ksi_team_id) : null;

    const strength = teamId ? (teamStrength.get(teamId) ?? 0.5) : 0.5;

    return {
      ...p,
      birth_year: birthYearById.get(String(p.ksi_player_id)) ?? null,  
      season: r
        ? {
            ksi_team_id: teamId,
            player_name: r.player_name ?? p.name,
            matches_played: Number(r.matches_played ?? 0),
            starts,
            minutes,
            goals,
            yellows,
            reds,
          }
        : null,
      recent5: lastN(String(p.ksi_player_id), 5),
      importance: calcImportance({ minutes, starts, goals, yellows, reds, teamStrength: strength }),
    };
  }

  return NextResponse.json({
    inputUrl,
    matchId,
    season_year: seasonYear,
    matchMeta: meta,
    home: { starters: lineupJson.home.starters.map(enrich), bench: lineupJson.home.bench.map(enrich) },
    away: { starters: lineupJson.away.starters.map(enrich), bench: lineupJson.away.bench.map(enrich) },
  });
}
