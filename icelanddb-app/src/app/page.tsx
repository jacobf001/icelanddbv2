"use client";

import React, { useEffect, useMemo, useState } from "react";
import type { MatchPreviewResponse, TeamRow } from "@/lib/types";

const SEASONS = [2020, 2021, 2022, 2023, 2024, 2025];

function clsx(...xs: Array<string | false | null | undefined>) {
  return xs.filter(Boolean).join(" ");
}

function fmt(n: number | null | undefined) {
  if (n === null || n === undefined) return "—";
  return n.toLocaleString();
}

function pct(x: number | null | undefined) {
  if (x === null || x === undefined) return "—";
  return `${Math.round(x * 100)}%`;
}

function strengthLabel(x: number | null | undefined) {
  if (x === null || x === undefined) return "—";
  return `${Math.round(x * 100)}`; // 0..100
}

function pickResult(p?: { home: number; draw: number; away: number }) {
  if (!p) return "—";
  const entries = [
    ["H", p.home],
    ["D", p.draw],
    ["A", p.away],
  ] as Array<[string, number]>;

  entries.sort((a, b) => b[1] - a[1]);
  return `${entries[0][0]} (${Math.round(entries[0][1] * 100)}%)`;
}

function teamLabel(t: TeamRow) {
  const nm = (t as any).team_name ?? (t as any).name; // supports both shapes
  return nm ? `${nm} (${t.ksi_team_id})` : `Team ${t.ksi_team_id}`;
}

type LineupPlayer = { ksi_player_id: string; name: string; shirt_no: number | null };
type TeamLineup = { starters: LineupPlayer[]; bench: LineupPlayer[] };
type LineupsFromReportResponse = {
  inputUrl: string;
  fetchUrl: string;
  home: TeamLineup;
  away: TeamLineup;
  counts: { startersHome: number; startersAway: number; benchHome: number; benchAway: number };
};

// ---------- NEW HEADER HELPERS (TOP-LEVEL, HOISTED) ----------
function formatLeagueLine(row: any) {
  if (!row) return "—";
  const name = row.competition_name ?? "—";
  const tier = row.competition_tier ?? "—";
  const pos = row.position ?? "—";
  return `${name} (Tier ${tier}) · Pos ${pos}`;
}

function SideHeaderCard({
  side,
  team,
  ctx,
}: {
  side: "Home" | "Away";
  team?: { ksi_team_id?: string | null; team_name?: string | null };
  ctx?: {
    competition_name?: string | null;
    competition_tier?: number | null;
    position?: number | null;
    prev?: {
      competition_name?: string | null;
      competition_tier?: number | null;
      position?: number | null;
    } | null;
  } | null;
}) {
  const nowLine = ctx ? formatLeagueLine(ctx) : "—";
  const prevLine = ctx?.prev ? formatLeagueLine(ctx.prev) : "—";

  return (
    <div className="rounded-xl border border-white/10 bg-black/30 p-4">
      <div className="text-xs text-white/60">{side}</div>

      <div className="mt-1 flex items-baseline justify-between gap-3">
        <div className="min-w-0">
          <div className="truncate text-xl font-semibold">{team?.team_name ?? side}</div>
        </div>
        <div className="shrink-0 text-sm text-white/60">({team?.ksi_team_id ?? "—"})</div>
      </div>

      <div className="mt-3 space-y-2">
        <div>
          <div className="text-xs text-white/60">2025</div>
          <div className="text-sm font-medium">{nowLine}</div>
        </div>

        <div>
          <div className="text-xs text-white/60">2024</div>
          <div className="text-sm font-medium text-white/80">{prevLine}</div>
        </div>
      </div>
    </div>
  );
}
// ------------------------------------------------------------

export default function HomePage() {
  const [teams, setTeams] = useState<TeamRow[]>([]);
  const [teamsLoading, setTeamsLoading] = useState(true);
  const [teamsError, setTeamsError] = useState<string | null>(null);

  const [seasonYear, setSeasonYear] = useState<number>(2025);

  const [homeTeamId, setHomeTeamId] = useState<string>("5980");
  const [awayTeamId, setAwayTeamId] = useState<string>("5737");

  const [matchLoading, setMatchLoading] = useState(false);
  const [matchError, setMatchError] = useState<string | null>(null);
  const [matchData, setMatchData] = useState<MatchPreviewResponse | null>(null);

  const [ksiUrl, setKsiUrl] = useState<string>(
    "https://www.ksi.is/leikir-og-urslit/felagslid/leikur?id=6966621",
  );

  const [lineupLoading, setLineupLoading] = useState(false);
  const [lineupError, setLineupError] = useState<string | null>(null);
  const [lineupData, setLineupData] = useState<LineupsFromReportResponse | null>(null);

  const [analysisLoading, setAnalysisLoading] = useState(false);
  const [analysisError, setAnalysisError] = useState<string | null>(null);
  const [analysis, setAnalysis] = useState<any | null>(null);

  async function runLineupAnalysis() {
    setAnalysisLoading(true);
    setAnalysisError(null);
    setAnalysis(null);

    try {
      const qs = new URLSearchParams({
        url: ksiUrl.trim(),
        season: String(seasonYear),
      });

      const res = await fetch(`/api/lineup-stats?${qs.toString()}`, { cache: "no-store" });
      const json = await res.json();

      if (!res.ok) throw new Error(json?.error ?? `Request failed (${res.status})`);
      setAnalysis(json);
    } catch (e: any) {
      setAnalysisError(e?.message ?? "Failed to analyze lineup");
    } finally {
      setAnalysisLoading(false);
    }
  }

  // Load teams for dropdowns
  useEffect(() => {
    let cancelled = false;

    async function loadTeams() {
      setTeamsLoading(true);
      setTeamsError(null);
      try {
        const res = await fetch("/api/teams", { cache: "no-store" });
        if (!res.ok) throw new Error(`Failed to load teams (${res.status})`);
        const json = (await res.json()) as { teams: TeamRow[] };
        if (!cancelled) setTeams(json.teams ?? []);
      } catch (e: any) {
        if (!cancelled) setTeamsError(e?.message ?? "Failed to load teams");
      } finally {
        if (!cancelled) setTeamsLoading(false);
      }
    }

    loadTeams();
    return () => {
      cancelled = true;
    };
  }, []);

  const teamMap = useMemo(() => {
    const m = new Map<string, TeamRow>();
    for (const t of teams) m.set(String(t.ksi_team_id), t);
    return m;
  }, [teams]);

  const homeName =
    (teamMap.get(homeTeamId) as any)?.team_name ?? (teamMap.get(homeTeamId) as any)?.name ?? null;
  const awayName =
    (teamMap.get(awayTeamId) as any)?.team_name ?? (teamMap.get(awayTeamId) as any)?.name ?? null;

  const canRunMatch = Boolean(homeTeamId && awayTeamId && seasonYear && homeTeamId !== awayTeamId);
  const canRunLineup = Boolean(ksiUrl.trim().length > 0);

  async function runMatchPreview() {
    if (!canRunMatch) return;

    setMatchLoading(true);
    setMatchError(null);
    setMatchData(null);

    try {
      const qs = new URLSearchParams({
        homeTeam: homeTeamId,
        awayTeam: awayTeamId,
        season: String(seasonYear),
      });

      const res = await fetch(`/api/match-preview?${qs.toString()}`, { cache: "no-store" });
      const json = await res.json();

      if (!res.ok) throw new Error(json?.error ?? `Request failed (${res.status})`);
      setMatchData(json as MatchPreviewResponse);
    } catch (e: any) {
      setMatchError(e?.message ?? "Failed to fetch match preview");
    } finally {
      setMatchLoading(false);
    }
  }

  async function runLineups() {
    if (!canRunLineup) return;

    setLineupLoading(true);
    setLineupError(null);
    setLineupData(null);

    try {
      const qs = new URLSearchParams({ url: ksiUrl.trim() });
      const res = await fetch(`/api/lineups-from-report?${qs.toString()}`, { cache: "no-store" });
      const json = await res.json();

      if (!res.ok) throw new Error(json?.error ?? `Request failed (${res.status})`);
      setLineupData(json as LineupsFromReportResponse);
    } catch (e: any) {
      setLineupError(e?.message ?? "Failed to parse lineups");
    } finally {
      setLineupLoading(false);
    }
  }

  function swapTeams() {
    setHomeTeamId(awayTeamId);
    setAwayTeamId(homeTeamId);
  }

  return (
    <main className="min-h-screen bg-black text-white">
      <div className="mx-auto max-w-6xl px-6 py-10">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-semibold">IcelandDB</h1>
          <p className="text-white/70">
            Paste a KSI match link to pull lineups, or pick teams + season for season-to-date stats.
          </p>
        </div>

        {/* LINEUPS FROM KSI */}
        <div className="mt-8 rounded-xl border border-white/10 bg-white/5 p-5">
          <h2 className="text-lg font-semibold">Lineups from KSI match link</h2>
          <p className="mt-1 text-sm text-white/60">
            Paste the KSI match URL. We fetch the report tab and parse starters + bench.
          </p>

          <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-12 md:items-end">
            <div className="md:col-span-2">
              <label className="mb-2 block text-sm text-white/70">Season</label>
              <select
                className="w-full rounded-lg border border-white/15 bg-black px-3 py-2"
                value={seasonYear}
                onChange={(e) => setSeasonYear(Number(e.target.value))}
              >
                {SEASONS.map((y) => (
                  <option key={y} value={y}>
                    {y}
                  </option>
                ))}
              </select>
            </div>

            <div className="md:col-span-8">
              <label className="mb-2 block text-sm text-white/70">KSI match URL</label>
              <input
                className="w-full rounded-lg border border-white/15 bg-black px-3 py-2 text-sm"
                value={ksiUrl}
                onChange={(e) => setKsiUrl(e.target.value)}
                placeholder="https://www.ksi.is/leikir-og-urslit/felagslid/leikur?id=..."
              />
            </div>

            <div className="md:col-span-2 flex gap-2 md:justify-end">
              <button
                type="button"
                onClick={runLineups}
                disabled={lineupLoading || !canRunLineup}
                className={clsx(
                  "rounded-lg px-4 py-2 text-sm font-medium",
                  lineupLoading || !canRunLineup
                    ? "bg-white/10 text-white/50"
                    : "bg-white text-black hover:bg-white/90",
                )}
              >
                {lineupLoading ? "Parsing..." : "Get lineups"}
              </button>

              <button
                type="button"
                onClick={runLineupAnalysis}
                disabled={analysisLoading || !canRunLineup}
                className={clsx(
                  "rounded-lg px-4 py-2 text-sm font-medium",
                  analysisLoading || !canRunLineup
                    ? "bg-white/10 text-white/50"
                    : "bg-white text-black hover:bg-white/90",
                )}
              >
                {analysisLoading ? "Analyzing..." : "Analyze"}
              </button>
            </div>
          </div>

          {lineupError && <div className="mt-3 text-sm text-red-400">{lineupError}</div>}

          {lineupData && (
            <div className="mt-5 grid grid-cols-1 gap-4 md:grid-cols-2">
              <LineupPanel title="Home" lineup={lineupData.home} />
              <LineupPanel title="Away" lineup={lineupData.away} />
            </div>
          )}
        </div>

        {analysisError && <div className="mt-3 text-sm text-red-400">{analysisError}</div>}

        {/* MODEL (rough) */}
        {analysis && (
          <div className="mt-4 rounded-xl border border-white/10 bg-white/5 p-5">
            <div className="text-sm text-white/60">Model (rough)</div>

            <div className="mt-3 grid grid-cols-1 gap-4 md:grid-cols-3">
              <MiniStat label="Home overall" value={String(analysis.overall?.home ?? "—")} />
              <MiniStat label="Away overall" value={String(analysis.overall?.away ?? "—")} />
              <MiniStat
                label="Predicted (H/D/A)"
                value={
                  analysis.probabilities
                    ? `${Math.round(analysis.probabilities.home * 100)}% / ${Math.round(
                        analysis.probabilities.draw * 100,
                      )}% / ${Math.round(analysis.probabilities.away * 100)}%`
                    : "—"
                }
              />
            </div>

            <div className="mt-3 text-sm text-white/70">
              Odds:{" "}
              {analysis.odds
                ? `H ${analysis.odds.home?.toFixed(2)} · D ${analysis.odds.draw?.toFixed(
                    2,
                  )} · A ${analysis.odds.away?.toFixed(2)}`
                : "—"}
            </div>
          </div>
        )}

        {/* LINEUP ANALYSIS */}
        {analysis && (
          <div className="mt-6 space-y-6">
            {/* Headline (split) */}
            <div className="rounded-xl border border-white/10 bg-white/5 p-5">
              <div className="text-sm text-white/60">Lineup analysis · Season {analysis.season_year}</div>

              <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-2">
                <SideHeaderCard side="Home" team={analysis.teams?.home} ctx={analysis.teamStrengthDebug?.home} />
                <SideHeaderCard side="Away" team={analysis.teams?.away} ctx={analysis.teamStrengthDebug?.away} />
              </div>
            </div>

            {/* Ratings */}
            <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
              <RatingPanel
                title={`${analysis.teams?.home?.team_name ?? "Home"} rating`}
                strength={analysis.teamStrength?.home}
                rating={analysis.home?.rating}
                missingImpact={analysis.home?.missingImpact}
                ctx={analysis.teamStrengthDebug?.home}
              />

              <RatingPanel
                title={`${analysis.teams?.away?.team_name ?? "Away"} rating`}
                strength={analysis.teamStrength?.away}
                rating={analysis.away?.rating}
                missingImpact={analysis.away?.missingImpact}
                ctx={analysis.teamStrengthDebug?.away}
              />
            </div>

            {/* Missing likely XI */}
            <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
              <MissingLikelyXI
                title={`${analysis.teams?.home?.team_name ?? "Home"} missing likely XI`}
                items={analysis.home?.missingLikelyXI ?? []}
              />
              <MissingLikelyXI
                title={`${analysis.teams?.away?.team_name ?? "Away"} missing likely XI`}
                items={analysis.away?.missingLikelyXI ?? []}
              />
            </div>

            {/* Player tables */}
            <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
              <PlayerAnalysisTable
                title={`${analysis.teams?.home?.team_name ?? "Home"} starters`}
                rows={analysis.home?.starters ?? []}
              />
              <PlayerAnalysisTable
                title={`${analysis.teams?.away?.team_name ?? "Away"} starters`}
                rows={analysis.away?.starters ?? []}
              />
            </div>

            <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
              <PlayerAnalysisTable
                title={`${analysis.teams?.home?.team_name ?? "Home"} bench`}
                rows={analysis.home?.bench ?? []}
              />
              <PlayerAnalysisTable
                title={`${analysis.teams?.away?.team_name ?? "Away"} bench`}
                rows={analysis.away?.bench ?? []}
              />
            </div>
          </div>
        )}

        {/* MATCH PREVIEW (season-to-date) */}
        <div className="mt-8 rounded-xl border border-white/10 bg-white/5 p-5">
          <h2 className="text-lg font-semibold">Season-to-date match preview</h2>
          <p className="mt-1 text-sm text-white/60">
            Choose two teams + a season to see team summary, top players, and likely XI (based on starts/minutes).
          </p>

          <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-12 md:items-end">
            <div className="md:col-span-3">
              <label className="mb-2 block text-sm text-white/70">Season</label>
              <select
                className="w-full rounded-lg border border-white/15 bg-black px-3 py-2"
                value={seasonYear}
                onChange={(e) => setSeasonYear(Number(e.target.value))}
              >
                {SEASONS.map((y) => (
                  <option key={y} value={y}>
                    {y}
                  </option>
                ))}
              </select>
            </div>

            <div className="md:col-span-4">
              <label className="mb-2 block text-sm text-white/70">Home team</label>
              <select
                className="w-full rounded-lg border border-white/15 bg-black px-3 py-2"
                value={homeTeamId}
                onChange={(e) => setHomeTeamId(e.target.value)}
                disabled={teamsLoading || Boolean(teamsError)}
              >
                {teams.map((t) => (
                  <option key={t.ksi_team_id} value={t.ksi_team_id}>
                    {teamLabel(t)}
                  </option>
                ))}
              </select>
            </div>

            <div className="md:col-span-1 flex md:justify-center">
              <button
                type="button"
                onClick={swapTeams}
                className="mt-2 rounded-lg border border-white/15 bg-black px-3 py-2 text-sm hover:bg-white/10 md:mt-0"
                title="Swap teams"
              >
                ⇄
              </button>
            </div>

            <div className="md:col-span-4">
              <label className="mb-2 block text-sm text-white/70">Away team</label>
              <select
                className="w-full rounded-lg border border-white/15 bg-black px-3 py-2"
                value={awayTeamId}
                onChange={(e) => setAwayTeamId(e.target.value)}
                disabled={teamsLoading || Boolean(teamsError)}
              >
                {teams.map((t) => (
                  <option key={t.ksi_team_id} value={t.ksi_team_id}>
                    {teamLabel(t)}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div className="mt-4 flex flex-wrap items-center gap-3">
            <button
              type="button"
              onClick={() => runMatchPreview()}
              disabled={!canRunMatch || matchLoading || teamsLoading}
              className={clsx(
                "rounded-lg px-4 py-2 text-sm font-medium",
                !canRunMatch || matchLoading || teamsLoading
                  ? "bg-white/10 text-white/50"
                  : "bg-white text-black hover:bg-white/90",
              )}
            >
              {matchLoading ? "Loading..." : "Preview"}
            </button>

            <div className="text-sm text-white/70">
              {teamsLoading && "Loading teams..."}
              {teamsError && <span className="text-red-400">{teamsError}</span>}
              {!teamsLoading && !teamsError && (
                <span>
                  {homeName ?? `Team ${homeTeamId}`} vs {awayName ?? `Team ${awayTeamId}`} — {seasonYear}
                </span>
              )}
            </div>

            {!canRunMatch && (
              <div className="text-sm text-red-400">Pick two different teams + a valid season.</div>
            )}
          </div>

          {matchError && <div className="mt-3 text-sm text-red-400">{matchError}</div>}

          {matchData && (
            <div className="mt-6 grid grid-cols-1 gap-6 md:grid-cols-2">
              <TeamPanel
                title="Home"
                teamId={matchData.home_team_ksi_id}
                season={matchData.season_year}
                block={matchData.home}
              />
              <TeamPanel
                title="Away"
                teamId={matchData.away_team_ksi_id}
                season={matchData.season_year}
                block={matchData.away}
              />
            </div>
          )}
        </div>
      </div>
    </main>
  );
}

function LineupPanel({ title, lineup }: { title: "Home" | "Away"; lineup: TeamLineup }) {
  return (
    <section className="rounded-xl border border-white/10 bg-black/30 p-4">
      <h3 className="text-base font-semibold">{title} lineup</h3>

      <div className="mt-3">
        <div className="text-sm text-white/70">Starters ({lineup.starters.length})</div>
        <div className="mt-2 overflow-hidden rounded-lg border border-white/10">
          <table className="w-full text-sm">
            <thead className="bg-white/5 text-white/70">
              <tr>
                <th className="px-3 py-2 text-left">#</th>
                <th className="px-3 py-2 text-left">Player</th>
                <th className="px-3 py-2 text-right">ID</th>
              </tr>
            </thead>
            <tbody>
              {lineup.starters.map((p) => (
                <tr key={p.ksi_player_id} className="border-t border-white/10">
                  <td className="px-3 py-2">{p.shirt_no ?? "—"}</td>
                  <td className="px-3 py-2">{p.name}</td>
                  <td className="px-3 py-2 text-right text-white/60">{(p as any).birth_year ?? p.ksi_player_id}</td>
                </tr>
              ))}
              {lineup.starters.length === 0 && (
                <tr>
                  <td colSpan={3} className="px-3 py-3 text-white/60">
                    No starters parsed yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="mt-5">
        <div className="text-sm text-white/70">Bench ({lineup.bench.length})</div>
        <div className="mt-2 overflow-hidden rounded-lg border border-white/10">
          <table className="w-full text-sm">
            <thead className="bg-white/5 text-white/70">
              <tr>
                <th className="px-3 py-2 text-left">#</th>
                <th className="px-3 py-2 text-left">Player</th>
                <th className="px-3 py-2 text-right">ID</th>
              </tr>
            </thead>
            <tbody>
              {lineup.bench.map((p) => (
                <tr key={p.ksi_player_id} className="border-t border-white/10">
                  <td className="px-3 py-2">{p.shirt_no ?? "—"}</td>
                  <td className="px-3 py-2">{p.name}</td>
                  <td className="px-3 py-2 text-right text-white/70">{p.ksi_player_id}</td>
                </tr>
              ))}
              {lineup.bench.length === 0 && (
                <tr>
                  <td colSpan={3} className="px-3 py-3 text-white/60">
                    No bench parsed.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

function TeamPanel({
  title,
  teamId,
  season,
  block,
}: {
  title: "Home" | "Away";
  teamId: string;
  season: number;
  block: MatchPreviewResponse["home"];
}) {
  const summary = block.summary;

  return (
    <section className="rounded-xl border border-white/10 bg-black/30 p-5">
      <h3 className="text-lg font-semibold">
        {title} — {summary?.team_name ?? `Team ${teamId}`}
      </h3>
      <div className="mt-1 text-sm text-white/60">
        Team ID: {teamId} · Season: {season}
      </div>

      <div className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
        <MiniStat label="MP" value={fmt(summary?.matches_played)} />
        <MiniStat label="W/D/L" value={summary ? `${summary.wins}/${summary.draws}/${summary.losses}` : "—"} />
        <MiniStat label="Pts" value={fmt(summary?.points)} />
        <MiniStat label="GF/GA" value={summary ? `${summary.goals_for}/${summary.goals_against}` : "—"} />
        <MiniStat label="GD" value={fmt(summary?.goal_diff)} />
        <MiniStat label="Yellows" value={fmt(summary?.yellows)} />
        <MiniStat label="Reds" value={fmt(summary?.reds)} />
      </div>

      <div className="mt-6">
        <h4 className="text-base font-semibold">Likely XI</h4>
        <div className="mt-2 overflow-hidden rounded-lg border border-white/10">
          <table className="w-full text-sm">
            <thead className="bg-white/5 text-white/70">
              <tr>
                <th className="px-3 py-2 text-left">Player</th>
                <th className="px-3 py-2 text-right">Starts</th>
                <th className="px-3 py-2 text-right">Minutes</th>
                <th className="px-3 py-2 text-right">Goals</th>
              </tr>
            </thead>
            <tbody>
              {block.likelyXI.length === 0 ? (
                <tr>
                  <td className="px-3 py-3 text-white/60" colSpan={4}>
                    No likely XI data.
                  </td>
                </tr>
              ) : (
                block.likelyXI.slice(0, 11).map((p) => (
                  <tr key={p.ksi_player_id} className="border-t border-white/10">
                    <td className="px-3 py-2">{p.player_name ?? `Player ${p.ksi_player_id}`}</td>
                    <td className="px-3 py-2 text-right">{fmt(p.starts)}</td>
                    <td className="px-3 py-2 text-right">{fmt(p.minutes)}</td>
                    <td className="px-3 py-2 text-right">{fmt(p.goals)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-white/10 bg-black/40 px-3 py-2">
      <div className="text-xs text-white/60">{label}</div>
      <div className="mt-1 text-base font-semibold">{value}</div>
    </div>
  );
}

function RatingPanel({
  title,
  strength,
  rating,
  missingImpact,
  ctx,
}: {
  title: string;
  strength?: number;
  rating?: any;
  missingImpact?: number;
  ctx?: any;
}) {
  return (
    <div className="rounded-xl border border-white/10 bg-black/30 p-4">
      <h3 className="text-base font-semibold">{title}</h3>

      <div className="mt-3 grid grid-cols-2 gap-3 sm:grid-cols-4">
        <MiniStat label="Team strength" value={strengthLabel(strength)} />
        <MiniStat label="Lineup total" value={rating ? String(rating.total ?? "—") : "—"} />
        <MiniStat label="Coverage" value={rating ? pct(rating.coverage ?? 0) : "—"} />
        <MiniStat label="Missing impact" value={missingImpact !== undefined ? String(missingImpact) : "—"} />
      </div>

      {ctx && (
        <div className="mt-3 text-xs text-white/60 space-y-1">
          <div>
            2025: {ctx.competition_name ? ctx.competition_name : "—"}
            {ctx.competition_tier ? ` (Tier ${ctx.competition_tier})` : ""}
            {ctx.position ? ` · Pos ${ctx.position}` : ""}
          </div>
          <div>
            2024: {ctx.prev?.competition_name ? ctx.prev.competition_name : "—"}
            {ctx.prev?.competition_tier ? ` (Tier ${ctx.prev.competition_tier})` : ""}
            {ctx.prev?.position ? ` · Pos ${ctx.prev.position}` : ""}
          </div>
        </div>
      )}

      {rating && (
        <div className="mt-3 text-xs text-white/60">
          Starters: {rating.starters} · Bench: {rating.bench} · Raw: {rating.raw}
        </div>
      )}
    </div>
  );
}

function MissingLikelyXI({
  title,
  items,
}: {
  title: string;
  items: Array<{ ksi_player_id: string; player_name: string | null; importance: number; birth_year?: number | null }>;
}) {
  if (!items || items.length === 0) return null;

  return (
    <div className="rounded-xl border border-white/10 bg-black/30 p-4">
      <h3 className="text-base font-semibold">{title}</h3>
      <p className="mt-1 text-sm text-white/60">
        Players who are often in the likely XI but are missing from the starters today.
      </p>

      <div className="mt-3 overflow-hidden rounded-lg border border-white/10">
        <table className="w-full text-sm">
          <thead className="bg-white/5 text-white/70">
            <tr>
              <th className="px-3 py-2 text-left">Player</th>
              <th className="px-3 py-2 text-right">Impact</th>
              <th className="px-3 py-2 text-right">Born</th>
            </tr>
          </thead>
          <tbody>
            {items.slice(0, 10).map((p) => (
              <tr key={p.ksi_player_id} className="border-t border-white/10">
                <td className="px-3 py-2">{p.player_name ?? `Player ${p.ksi_player_id}`}</td>
                <td className="px-3 py-2 text-right font-semibold text-red-300">{p.importance}</td>
               <td className="px-3 py-2 text-right text-white/60">{p.birth_year ?? p.ksi_player_id}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PlayerAnalysisTable({ title, rows }: { title: string; rows: any[] }) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  function toggle(id: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  return (
    <div className="rounded-xl border border-white/10 bg-black/30 p-4">
      <h3 className="text-base font-semibold">{title}</h3>

      <div className="mt-3 overflow-hidden rounded-lg border border-white/10">
        <table className="w-full text-sm">
          <thead className="bg-white/5 text-white/70">
            <tr>
              <th className="px-3 py-2 text-left">#</th>
              <th className="px-3 py-2 text-left">Player</th>
              <th className="px-3 py-2 text-right">Min</th>
              <th className="px-3 py-2 text-right">Starts</th>
              <th className="px-3 py-2 text-right">G</th>
              <th className="px-3 py-2 text-right">Last5</th>
              <th className="px-3 py-2 text-right">Imp</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={7} className="px-3 py-3 text-white/60">No rows.</td>
              </tr>
            ) : (
              rows.map((p) => {
                const isOpen = expanded.has(p.ksi_player_id);
                const imp = p.importance ?? 0;
                const impColor = imp >= 80 ? "text-emerald-400" : imp >= 50 ? "text-yellow-400" : imp >= 25 ? "text-white/80" : "text-white/40";
                const rowAccent = imp >= 80 
                  ? "border-l-2 border-l-emerald-500/60 bg-emerald-950/30" 
                  : imp >= 50 
                  ? "border-l-2 border-l-yellow-500/40 bg-yellow-950/20" 
                  : "";
                return (
                  <React.Fragment key={p.ksi_player_id}>
                    <tr
                      className={`border-t border-white/10 cursor-pointer hover:brightness-125 transition-colors ${rowAccent}`}
                      onClick={() => toggle(p.ksi_player_id)}
                    >
                      <td className="px-3 py-2 text-white/50">{p.shirt_no ?? "—"}</td>
                      <td className="px-3 py-2">
                        <div className={`font-medium ${imp >= 80 ? "text-white" : "text-white/80"}`}>
                          {p.season?.player_name ?? p.name ?? `Player ${p.ksi_player_id}`}
                        </div>
                        <div className="text-xs text-white/40">{p.birth_year ?? "—"}</div>
                      </td>
                      <td className="px-3 py-2 text-right text-white/60">{p.season ? p.season.minutes : "—"}</td>
                      <td className="px-3 py-2 text-right text-white/60">{p.season ? p.season.starts : "—"}</td>
                      <td className="px-3 py-2 text-right text-white/60">{p.season ? p.season.goals : "—"}</td>
                      <td className="px-3 py-2 text-right text-white/60">{p.recent5 ? p.recent5.lastNMinutes : "—"}</td>
                      <td className={`px-3 py-2 text-right font-bold ${impColor}`}>{p.importance ?? "—"}</td>
                    </tr>
                    {isOpen && (
                      <tr key={`${p.ksi_player_id}-detail`} className="bg-white/3 border-t border-white/5">
                        <td />
                        <td colSpan={6} className="px-3 py-2 text-xs text-white/60 space-y-1">
                          {p.seasons?.length > 0
                            ? p.seasons.map((s: any, i: number) => (
                                <div key={i}>
                                  2025: {s.team_name ?? "—"}
                                  {s.club_ctx?.competition_tier ? ` · Tier ${s.club_ctx.competition_tier}` : ""}
                                  {s.club_ctx?.position ? ` · Pos ${s.club_ctx.position}` : ""}
                                  {s.minutes ? ` (${s.minutes}m)` : ""}
                                </div>
                              ))
                            : <div>2025: —</div>
                          }
                          {p.prevSeasons?.length > 0
                            ? p.prevSeasons.map((ps: any, i: number) => (
                                <div key={i}>
                                  2024: {ps.team_name ?? "—"}
                                  {ps.club_ctx?.competition_tier ? ` · Tier ${ps.club_ctx.competition_tier}` : ""}
                                  {ps.club_ctx?.position ? ` · Pos ${ps.club_ctx.position}` : ""}
                                  {ps.minutes ? ` (${ps.minutes}m)` : ""}
                                </div>
                              ))
                            : <div>2024: —</div>
                          }
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      <p className="mt-2 text-xs text-white/40">Click a row to expand club details.</p>
    </div>
  );
}
