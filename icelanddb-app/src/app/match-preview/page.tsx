"use client";

import { useEffect, useMemo, useState } from "react";
import type { MatchPreviewResponse, TeamRow } from "@/lib/types";

const SEASONS = [2020, 2021, 2022, 2023, 2024, 2025];

function clsx(...xs: Array<string | false | null | undefined>) {
  return xs.filter(Boolean).join(" ");
}

function fmt(n: number | null | undefined) {
  if (n === null || n === undefined) return "—";
  return n.toLocaleString();
}

function teamLabel(t: TeamRow) {
  const nm = t.team_name ?? t.name; // prefer team_name, fallback to name
  return nm ? `${nm} (${t.ksi_team_id})` : `Team ${t.ksi_team_id}`;
}



export default function MatchPreviewPage() {
  const [teams, setTeams] = useState<TeamRow[]>([]);
  const [teamsLoading, setTeamsLoading] = useState(true);
  const [teamsError, setTeamsError] = useState<string | null>(null);

  const [seasonYear, setSeasonYear] = useState<number>(2025);

  const [homeTeamId, setHomeTeamId] = useState<string>("5980");
  const [awayTeamId, setAwayTeamId] = useState<string>("5737");

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [data, setData] = useState<MatchPreviewResponse | null>(null);

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

  const canRun = useMemo(() => {
    return Boolean(homeTeamId && awayTeamId && seasonYear && homeTeamId !== awayTeamId);
  }, [homeTeamId, awayTeamId, seasonYear]);

  async function runPreview() {
    if (!canRun) return;

    setLoading(true);
    setError(null);
    setData(null);

    try {
      const qs = new URLSearchParams({
        homeTeam: homeTeamId,
        awayTeam: awayTeamId,
        season: String(seasonYear),
      });

      const res = await fetch(`/api/match-preview?${qs.toString()}`, { cache: "no-store" });
      const json = await res.json();

      if (!res.ok) {
        throw new Error(json?.error ?? `Request failed (${res.status})`);
      }

      setData(json as MatchPreviewResponse);
    } catch (e: any) {
      setError(e?.message ?? "Failed to fetch match preview");
    } finally {
      setLoading(false);
    }
  }

  function swapTeams() {
    setHomeTeamId(awayTeamId);
    setAwayTeamId(homeTeamId);
  }

  // Optional: auto-run once teams are loaded
  useEffect(() => {
    if (!teamsLoading) runPreview();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [teamsLoading]);

  const homeName = teamMap.get(homeTeamId)?.name ?? null;
    const awayName = teamMap.get(awayTeamId)?.name ?? null;


  return (
    <main className="min-h-screen bg-black text-white">
      <div className="mx-auto max-w-6xl px-6 py-10">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-semibold">Match Preview</h1>
          <p className="text-white/70">
            Select two teams + season, then preview season-to-date team + player stats and a likely XI.
          </p>
        </div>

        {/* Controls */}
        <div className="mt-8 rounded-xl border border-white/10 bg-white/5 p-5">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-12 md:items-end">
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
              onClick={runPreview}
              disabled={!canRun || loading || teamsLoading}
              className={clsx(
                "rounded-lg px-4 py-2 text-sm font-medium",
                !canRun || loading || teamsLoading
                  ? "bg-white/10 text-white/50"
                  : "bg-white text-black hover:bg-white/90",
              )}
            >
              {loading ? "Loading..." : "Preview"}
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

            {!canRun && (
              <div className="text-sm text-red-400">Pick two different teams + a valid season.</div>
            )}
          </div>

          {error && <div className="mt-3 text-sm text-red-400">{error}</div>}
        </div>

        {/* Results */}
        {data && (
          <div className="mt-8 grid grid-cols-1 gap-6 md:grid-cols-2">
            <TeamPanel
            title="Home"
            teamId={data.home_team_ksi_id}
            season={data.season_year}
            block={data.home}
            teamDisplayName={teamMap.get(String(data.home_team_ksi_id))?.name ?? data.home.summary?.team_name ?? null}
            />

            <TeamPanel
            title="Away"
            teamId={data.away_team_ksi_id}
            season={data.season_year}
            block={data.away}
            teamDisplayName={teamMap.get(String(data.away_team_ksi_id))?.name ?? data.away.summary?.team_name ?? null}
            />

          </div>
        )}

        {!data && !loading && !error && (
          <div className="mt-10 text-white/60">No data loaded yet. Click “Preview”.</div>
        )}
      </div>
    </main>
  );
}

function TeamPanel({
  title,
  teamId,
  season,
  block,
  teamDisplayName,
}: {
  title: "Home" | "Away";
  teamId: string;
  season: number;
  block: MatchPreviewResponse["home"];
    teamDisplayName: string | null;

}) {
  const summary = block.summary;

  return (
    <section className="rounded-xl border border-white/10 bg-white/5 p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h2 className="text-xl font-semibold">
            {title} — {teamDisplayName ?? `Team ${teamId}`}
          </h2>
          <div className="mt-1 text-sm text-white/60">
            Team ID: {teamId} · Season: {season}
          </div>
        </div>
      </div>

      {/* Summary cards */}
      <div className="mt-5 grid grid-cols-2 gap-3 sm:grid-cols-4">
        <MiniStat label="MP" value={fmt(summary?.matches_played)} />
        <MiniStat label="W/D/L" value={summary ? `${summary.wins}/${summary.draws}/${summary.losses}` : "—"} />
        <MiniStat label="Pts" value={fmt(summary?.points)} />
        <MiniStat
          label="GF/GA"
          value={summary ? `${summary.goals_for}/${summary.goals_against}` : "—"}
        />
        <MiniStat label="GD" value={fmt(summary?.goal_diff)} />
        <MiniStat label="Yellows" value={fmt(summary?.yellows)} />
        <MiniStat label="Reds" value={fmt(summary?.reds)} />
      </div>

      {/* Likely XI */}
      <div className="mt-6">
        <h3 className="text-base font-semibold">Likely XI</h3>
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

      {/* Top players */}
      <div className="mt-6">
        <h3 className="text-base font-semibold">Top players (minutes)</h3>
        <div className="mt-2 overflow-hidden rounded-lg border border-white/10">
          <table className="w-full text-sm">
            <thead className="bg-white/5 text-white/70">
              <tr>
                <th className="px-3 py-2 text-left">Player</th>
                <th className="px-3 py-2 text-right">MP</th>
                <th className="px-3 py-2 text-right">Starts</th>
                <th className="px-3 py-2 text-right">Minutes</th>
                <th className="px-3 py-2 text-right">Goals</th>
                <th className="px-3 py-2 text-right">Y</th>
                <th className="px-3 py-2 text-right">R</th>
              </tr>
            </thead>
            <tbody>
              {block.topPlayers.length === 0 ? (
                <tr>
                  <td className="px-3 py-3 text-white/60" colSpan={7}>
                    No player rows found for this team/season.
                  </td>
                </tr>
              ) : (
                block.topPlayers.slice(0, 15).map((p) => (
                  <tr key={p.ksi_player_id} className="border-t border-white/10">
                    <td className="px-3 py-2">{p.player_name ?? `Player ${p.ksi_player_id}`}</td>
                    <td className="px-3 py-2 text-right">{fmt(p.matches_played)}</td>
                    <td className="px-3 py-2 text-right">{fmt(p.starts)}</td>
                    <td className="px-3 py-2 text-right">{fmt(p.minutes)}</td>
                    <td className="px-3 py-2 text-right">{fmt(p.goals)}</td>
                    <td className="px-3 py-2 text-right">{fmt(p.yellows)}</td>
                    <td className="px-3 py-2 text-right">{fmt(p.reds)}</td>
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
