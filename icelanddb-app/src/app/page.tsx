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

function teamLabel(t: TeamRow) {
  const nm = (t as any).team_name ?? (t as any).name;
  return nm ? `${nm} (${t.ksi_team_id})` : `Team ${t.ksi_team_id}`;
}

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
  const isHome = side === "Home";

  return (
    <div className={clsx(
      "rounded-xl border p-5 relative overflow-hidden",
      isHome ? "border-blue-500/20 bg-blue-950/20" : "border-orange-500/20 bg-orange-950/20"
    )}>
      <div className={clsx(
        "absolute top-0 left-0 w-1 h-full",
        isHome ? "bg-blue-500" : "bg-orange-500"
      )} />
      <div className="pl-3">
        <div className={clsx("text-xs font-mono uppercase tracking-widest", isHome ? "text-blue-400" : "text-orange-400")}>
          {side}
        </div>
        <div className="mt-1 flex items-baseline justify-between gap-3">
          <div className="text-2xl font-bold">{team?.team_name ?? side}</div>
          <div className="text-sm text-white/40 font-mono">#{team?.ksi_team_id ?? "—"}</div>
        </div>
        <div className="mt-3 space-y-1.5">
          <div className="flex items-center gap-2">
            <span className="text-xs font-mono text-white/40 w-8">2025</span>
            <span className="text-sm text-white/80">{nowLine}</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-xs font-mono text-white/30 w-8">2024</span>
            <span className="text-sm text-white/50">{prevLine}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

type LineupPlayer = { ksi_player_id: string; name: string; shirt_no: number | null };
type TeamLineup = { starters: LineupPlayer[]; bench: LineupPlayer[] };

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

  const [ksiUrl, setKsiUrl] = useState<string>("");

  const [analysisLoading, setAnalysisLoading] = useState(false);
  const [analysisError, setAnalysisError] = useState<string | null>(null);
  const [analysis, setAnalysis] = useState<any | null>(null);

  // Market odds state — persists across re-analyses until user clears


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
    return () => { cancelled = true; };
  }, []);

  const teamMap = useMemo(() => {
    const m = new Map<string, TeamRow>();
    for (const t of teams) m.set(String(t.ksi_team_id), t);
    return m;
  }, [teams]);

  const homeName = (teamMap.get(homeTeamId) as any)?.team_name ?? (teamMap.get(homeTeamId) as any)?.name ?? null;
  const awayName = (teamMap.get(awayTeamId) as any)?.team_name ?? (teamMap.get(awayTeamId) as any)?.name ?? null;

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

  function swapTeams() {
    setHomeTeamId(awayTeamId);
    setAwayTeamId(homeTeamId);
  }

  return (
    <main className="min-h-screen bg-[#0a0a0c] text-white">
      {/* Header */}
      <div className="border-b border-white/5 bg-black/40">
        <div className="mx-auto max-w-6xl px-6 py-5 flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold tracking-tight">IcelandDB</h1>
            <p className="text-xs text-white/40 mt-0.5 font-mono">KSÍ match analysis</p>
          </div>
        </div>
      </div>

      <div className="mx-auto max-w-6xl px-6 py-8 space-y-8">

        {/* MATCH ANALYSIS FROM URL */}
        <section className="rounded-2xl border border-white/8 bg-white/3 p-6">
          <h2 className="text-base font-semibold text-white/90">Match Analysis</h2>
          <p className="mt-1 text-sm text-white/40">Paste a KSI match URL to analyse lineups and generate a match prediction.</p>

          <div className="mt-5 flex flex-col gap-3 md:flex-row md:items-end">
            <div className="w-full md:w-32">
              <label className="mb-1.5 block text-xs text-white/50 font-mono uppercase tracking-wider">Season</label>
              <select
                className="w-full rounded-lg border border-white/10 bg-black/60 px-3 py-2.5 text-sm focus:border-white/30 focus:outline-none"
                value={seasonYear}
                onChange={(e) => setSeasonYear(Number(e.target.value))}
              >
                {SEASONS.map((y) => (
                  <option key={y} value={y}>{y}</option>
                ))}
              </select>
            </div>

            <div className="flex-1">
              <label className="mb-1.5 block text-xs text-white/50 font-mono uppercase tracking-wider">KSI Match URL</label>
              <input
                className="w-full rounded-lg border border-white/10 bg-black/60 px-3 py-2.5 text-sm focus:border-white/30 focus:outline-none"
                value={ksiUrl}
                onChange={(e) => setKsiUrl(e.target.value)}
                placeholder="https://www.ksi.is/leikir-og-urslit/felagslid/leikur?id=..."
              />
            </div>

            <button
              type="button"
              onClick={runLineupAnalysis}
              disabled={analysisLoading || !canRunLineup}
              className={clsx(
                "rounded-lg px-6 py-2.5 text-sm font-semibold transition-all",
                analysisLoading || !canRunLineup
                  ? "bg-white/8 text-white/30 cursor-not-allowed"
                  : "bg-white text-black hover:bg-white/90 active:scale-95",
              )}
            >
              {analysisLoading ? "Analysing…" : "Analyse"}
            </button>
          </div>

          {analysisError && (
            <div className="mt-3 rounded-lg bg-red-950/40 border border-red-500/20 px-4 py-3 text-sm text-red-400">
              {analysisError}
            </div>
          )}
        </section>

        {/* ANALYSIS RESULTS */}
        {analysis && (
          <div className="space-y-6">

            {/* Team headers */}
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
              <SideHeaderCard side="Home" team={analysis.teams?.home} ctx={analysis.teamStrengthDebug?.home} />
              <SideHeaderCard side="Away" team={analysis.teams?.away} ctx={analysis.teamStrengthDebug?.away} />
            </div>

            {/* Model / Prediction */}
            <ModelCard
              analysis={analysis}

            />

            {/* Missing XI */}
            {((analysis.home?.missingLikelyXI?.length ?? 0) > 0 || (analysis.away?.missingLikelyXI?.length ?? 0) > 0) && (
              <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                <MissingLikelyXI
                  title={`${analysis.teams?.home?.team_name ?? "Home"} missing likely XI`}
                  items={analysis.home?.missingLikelyXI ?? []}
                  accent="blue"
                />
                <MissingLikelyXI
                  title={`${analysis.teams?.away?.team_name ?? "Away"} missing likely XI`}
                  items={analysis.away?.missingLikelyXI ?? []}
                  accent="orange"
                />
              </div>
            )}

            {/* Starters */}
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
              <PlayerAnalysisTable
                title={`${analysis.teams?.home?.team_name ?? "Home"} starters`}
                rows={analysis.home?.starters ?? []}
                accent="blue"
              />
              <PlayerAnalysisTable
                title={`${analysis.teams?.away?.team_name ?? "Away"} starters`}
                rows={analysis.away?.starters ?? []}
                accent="orange"
              />
            </div>

            {/* Bench */}
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
              <PlayerAnalysisTable
                title={`${analysis.teams?.home?.team_name ?? "Home"} bench`}
                rows={analysis.home?.bench ?? []}
                accent="blue"
              />
              <PlayerAnalysisTable
                title={`${analysis.teams?.away?.team_name ?? "Away"} bench`}
                rows={analysis.away?.bench ?? []}
                accent="orange"
              />
            </div>
          </div>
        )}

        {/* SEASON-TO-DATE PREVIEW */}
        <section className="rounded-2xl border border-white/8 bg-white/3 p-6">
          <h2 className="text-base font-semibold text-white/90">Season-to-date Preview</h2>
          <p className="mt-1 text-sm text-white/40">Pick two teams and a season to compare stats, likely XI, and form.</p>

          <div className="mt-5 grid grid-cols-1 gap-3 md:grid-cols-12 md:items-end">
            <div className="md:col-span-2">
              <label className="mb-1.5 block text-xs text-white/50 font-mono uppercase tracking-wider">Season</label>
              <select
                className="w-full rounded-lg border border-white/10 bg-black/60 px-3 py-2.5 text-sm focus:border-white/30 focus:outline-none"
                value={seasonYear}
                onChange={(e) => setSeasonYear(Number(e.target.value))}
              >
                {SEASONS.map((y) => <option key={y} value={y}>{y}</option>)}
              </select>
            </div>

            <div className="md:col-span-4">
              <label className="mb-1.5 block text-xs text-white/50 font-mono uppercase tracking-wider">Home team</label>
              <select
                className="w-full rounded-lg border border-white/10 bg-black/60 px-3 py-2.5 text-sm focus:border-white/30 focus:outline-none"
                value={homeTeamId}
                onChange={(e) => setHomeTeamId(e.target.value)}
                disabled={teamsLoading || Boolean(teamsError)}
              >
                {teams.map((t) => <option key={t.ksi_team_id} value={t.ksi_team_id}>{teamLabel(t)}</option>)}
              </select>
            </div>

            <div className="md:col-span-1 flex md:justify-center pt-5">
              <button
                type="button"
                onClick={swapTeams}
                className="rounded-lg border border-white/10 bg-black/60 px-3 py-2.5 text-sm hover:bg-white/10 transition-colors"
                title="Swap teams"
              >⇄</button>
            </div>

            <div className="md:col-span-4">
              <label className="mb-1.5 block text-xs text-white/50 font-mono uppercase tracking-wider">Away team</label>
              <select
                className="w-full rounded-lg border border-white/10 bg-black/60 px-3 py-2.5 text-sm focus:border-white/30 focus:outline-none"
                value={awayTeamId}
                onChange={(e) => setAwayTeamId(e.target.value)}
                disabled={teamsLoading || Boolean(teamsError)}
              >
                {teams.map((t) => <option key={t.ksi_team_id} value={t.ksi_team_id}>{teamLabel(t)}</option>)}
              </select>
            </div>

            <div className="md:col-span-1 flex md:justify-end pt-5">
              <button
                type="button"
                onClick={runMatchPreview}
                disabled={!canRunMatch || matchLoading || teamsLoading}
                className={clsx(
                  "w-full rounded-lg px-4 py-2.5 text-sm font-semibold transition-all",
                  !canRunMatch || matchLoading || teamsLoading
                    ? "bg-white/8 text-white/30 cursor-not-allowed"
                    : "bg-white text-black hover:bg-white/90 active:scale-95",
                )}
              >
                {matchLoading ? "…" : "Go"}
              </button>
            </div>
          </div>

          {!teamsLoading && !teamsError && (
            <div className="mt-3 text-sm text-white/40 font-mono">
              {homeName ?? `Team ${homeTeamId}`} vs {awayName ?? `Team ${awayTeamId}`} · {seasonYear}
            </div>
          )}
          {teamsError && <div className="mt-3 text-sm text-red-400">{teamsError}</div>}
          {matchError && <div className="mt-3 text-sm text-red-400">{matchError}</div>}

          {matchData && (
            <div className="mt-6 grid grid-cols-1 gap-6 md:grid-cols-2">
              <TeamPanel title="Home" teamId={matchData.home_team_ksi_id} season={matchData.season_year} block={matchData.home} />
              <TeamPanel title="Away" teamId={matchData.away_team_ksi_id} season={matchData.season_year} block={matchData.away} />
            </div>
          )}
        </section>
      </div>
    </main>
  );
}

// ---------- VALUE HELPERS ----------

// Convert decimal odds to implied probability (no margin removal)
// ---------- MODEL CARD ----------
function ModelCard({ analysis }: { analysis: any }) {
  const p = analysis.probabilities;
  const odds = analysis.odds;

  const homeStrength = analysis.teamStrength?.home ?? 0;
  const awayStrength = analysis.teamStrength?.away ?? 0;
  const homeName = analysis.teams?.home?.team_name ?? "Home";
  const awayName = analysis.teams?.away?.team_name ?? "Away";

  return (
    <div className="rounded-2xl border border-white/8 bg-white/3 p-6">
      <div className="flex items-center justify-between mb-5">
        <h3 className="text-base font-semibold">Prediction</h3>
        <div className="text-xs font-mono text-white/30 uppercase tracking-wider">Model v1</div>
      </div>

      {/* Split probability bar */}
      {p && (
        <div className="mb-6">
          <div className="flex justify-between text-sm mb-2">
            <span className="font-medium text-blue-300">{homeName} <span className="font-mono text-blue-400">{Math.round(p.home * 100)}%</span></span>
            <span className="font-mono text-white/30 text-xs">Draw {Math.round(p.draw * 100)}%</span>
            <span className="font-medium text-orange-300"><span className="font-mono text-orange-400">{Math.round(p.away * 100)}%</span> {awayName}</span>
          </div>
          <div className="h-3 rounded-full bg-white/5 overflow-hidden flex">
            <div className="h-full bg-blue-500 transition-all duration-500" style={{ width: `${Math.round(p.home * 100)}%` }} />
            <div className="h-full bg-white/15 transition-all duration-500" style={{ width: `${Math.round(p.draw * 100)}%` }} />
            <div className="h-full bg-orange-500 transition-all duration-500" style={{ width: `${Math.round(p.away * 100)}%` }} />
          </div>
        </div>
      )}

      {/* Fair odds */}
      {odds && (
        <div className="flex items-center gap-4 pb-4 border-b border-white/5">
          <span className="text-xs text-white/30 font-mono uppercase tracking-wider">Fair odds</span>
          <div className="flex gap-4 text-sm font-mono">
            <span><span className="text-white/40">H</span> <span className="text-blue-300">{odds.home?.toFixed(2)}</span></span>
            <span><span className="text-white/40">D</span> <span className="text-white/60">{odds.draw?.toFixed(2)}</span></span>
            <span><span className="text-white/40">A</span> <span className="text-orange-300">{odds.away?.toFixed(2)}</span></span>
          </div>
        </div>
      )}

      {/* Team strength comparison */}
      <div className="mt-4 grid grid-cols-2 gap-3">
        {([
          { name: homeName, strength: homeStrength, tier: analysis.teamStrengthDebug?.home?.competition_tier, color: "blue" as const },
          { name: awayName, strength: awayStrength, tier: analysis.teamStrengthDebug?.away?.competition_tier, color: "orange" as const },
        ]).map(({ name, strength, tier, color }) => {
          const str = Math.round(strength * 100);
          const isBlue = color === "blue";

          const tierLabel = tier != null ? `T${tier}` : "—";
          const tierColor =
            tier == null  ? "text-white/20 bg-white/5 border-white/5" :
            tier <= 1     ? "text-emerald-300 bg-emerald-950/60 border-emerald-500/20" :
            tier === 2    ? "text-green-300 bg-green-950/60 border-green-500/20" :
            tier === 3    ? "text-yellow-300 bg-yellow-950/60 border-yellow-500/20" :
            tier === 4    ? "text-orange-300 bg-orange-950/60 border-orange-500/20" :
                            "text-red-300 bg-red-950/60 border-red-500/20";

          const barColor = isBlue ? "bg-blue-500" : "bg-orange-500";
          const barW = Math.max(2, str);

          return (
            <div key={name} className="rounded-lg bg-white/3 border border-white/5 px-3 py-2.5">
              <div className="flex items-center justify-between mb-2">
                <span className={`text-xs font-mono font-semibold px-1.5 py-0.5 rounded border ${tierColor}`}>
                  {tierLabel}
                </span>
                <span className="text-xs font-mono text-white/50">
                  {str}<span className="text-white/20">/100</span>
                </span>
              </div>
              <div className="h-1.5 rounded-full bg-white/8 overflow-hidden mb-2">
                <div
                  className={`h-full rounded-full ${barColor} transition-all duration-500`}
                  style={{ width: `${barW}%` }}
                />
              </div>
              <div className={`text-xs font-medium truncate ${isBlue ? "text-blue-300/80" : "text-orange-300/80"}`}>
                {name}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ---------- MISSING LIKELY XI ----------
function MissingLikelyXI({
  title,
  items,
  accent,
}: {
  title: string;
  items: Array<{ ksi_player_id: string; player_name: string | null; importance: number; importanceCeiling?: number; birth_year?: number | null; goals?: number }>;
  accent: "blue" | "orange";
}) {
  if (!items || items.length === 0) return null;

  // Colour based on ratio of importance to ceiling — reflects quality within their tier
  function impactColor(imp: number, ceiling: number): string {
    const ratio = ceiling > 0 ? imp / ceiling : 0;
    if (ratio >= 0.90) return "text-emerald-400";  // key player — near their ceiling
    if (ratio >= 0.75) return "text-green-400";    // regular starter
    if (ratio >= 0.55) return "text-yellow-400";   // rotation
    return "text-white/35";                        // fringe
  }

  function impactLabel(imp: number, ceiling: number): string {
    const ratio = ceiling > 0 ? imp / ceiling : 0;
    if (ratio >= 0.90) return "Key";
    if (ratio >= 0.75) return "Regular";
    if (ratio >= 0.55) return "Squad";
    return "";
  }

  const accentBorder = accent === "blue" ? "border-blue-500/30" : "border-orange-500/30";
  const accentHeader = accent === "blue" ? "text-blue-400" : "text-orange-400";

  return (
    <div className={clsx("rounded-2xl border bg-white/3 p-5", accentBorder)}>
      <h3 className={clsx("text-sm font-semibold", accentHeader)}>{title}</h3>
      <p className="mt-1 text-xs text-white/40">Expected starters not in today's lineup.</p>

      <div className="mt-3 space-y-1">
        {items.slice(0, 8).map((p) => {
          const ceiling = p.importanceCeiling ?? 100;
          const color = impactColor(p.importance, ceiling);
          const label = impactLabel(p.importance, ceiling);
          return (
            <div key={p.ksi_player_id} className="flex items-center justify-between py-1.5 px-2 rounded-lg hover:bg-white/3 transition-colors">
              <div className="flex items-center gap-2 min-w-0">
                <span className="text-sm text-white/85 truncate">{p.player_name ?? `Player ${p.ksi_player_id}`}</span>
                {p.birth_year && <span className="text-xs text-white/30 font-mono shrink-0">{p.birth_year}</span>}
                {p.goals != null && p.goals > 0 && (
                  <span className="text-xs font-mono text-emerald-400/80 shrink-0">⚽ {p.goals}</span>
                )}
              </div>
              <div className="flex items-center gap-2 shrink-0 ml-3">
                {label && (
                  <span className={clsx("text-xs font-mono opacity-60", color)}>
                    {label}
                  </span>
                )}
                <span className={clsx("text-sm font-bold font-mono text-right", color)}>
                  {p.importance}
                  <span className="text-white/25 font-normal">/{ceiling}</span>
                </span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ---------- PLAYER ANALYSIS TABLE ----------
function avgAge(rows: any[]): string {
  const currentYear = new Date().getFullYear();
  const years = rows
    .filter((p) => p.birth_year && p.birth_year > 1940)
    .map((p) => currentYear - p.birth_year);
  if (years.length === 0) return "—";
  return (years.reduce((a, b) => a + b, 0) / years.length).toFixed(1);
}

function PlayerAnalysisTable({ title, rows, accent }: { title: string; rows: any[]; accent: "blue" | "orange" }) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  function toggle(id: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  const accentBorder = accent === "blue" ? "border-l-blue-500" : "border-l-orange-500";
  const starters = rows.filter((p) => !p.squad || p.squad === "xi");
  const age = avgAge(starters.length > 0 ? starters : rows);

  return (
    <div className="rounded-2xl border border-white/8 bg-white/3 p-5">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-semibold">{title}</h3>
        {age !== "—" && (
          <div className="text-xs font-mono text-white/40">
            Avg age <span className="text-white/70 font-semibold">{age}</span>
          </div>
        )}
      </div>

      <div className="overflow-hidden rounded-xl border border-white/8">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-white/3 border-b border-white/8">
              <th className="px-3 py-2.5 text-left w-8 text-xs text-white/30 font-mono">#</th>
              <th className="px-3 py-2.5 text-left text-xs text-white/30 font-mono uppercase tracking-wider">Player</th>
              <th className="px-3 py-2.5 text-right text-xs text-white/30 font-mono hidden sm:table-cell">Min</th>
              <th className="px-3 py-2.5 text-right text-xs text-white/30 font-mono hidden sm:table-cell">GS</th>
              <th className="px-3 py-2.5 text-right text-xs text-white/30 font-mono hidden sm:table-cell">G</th>
              <th className="px-3 py-2.5 text-right text-xs text-white/30 font-mono hidden sm:table-cell">L5</th>
              <th className="px-3 py-2.5 text-right text-xs text-white/30 font-mono">Imp</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={7} className="px-3 py-4 text-white/30 text-xs text-center">No data</td>
              </tr>
            ) : (
              rows.map((p) => {
                const isOpen = expanded.has(p.ksi_player_id);
                const imp = p.importance ?? 0;
                const ceiling = p.importanceCeiling ?? 100;
                const impRatio = ceiling > 0 ? imp / ceiling : 0;
                const impColor = impRatio >= 0.75 ? "text-emerald-400" : impRatio >= 0.55 ? "text-yellow-400" : imp >= 15 ? "text-white/70" : "text-white/30";
                const rowHighlight = impRatio >= 0.75
                  ? `border-l-2 border-l-emerald-500 bg-emerald-950/20`
                  : impRatio >= 0.55
                  ? `border-l-2 border-l-yellow-500/50 bg-yellow-950/10`
                  : "";

                return (
                  <React.Fragment key={p.ksi_player_id}>
                    <tr
                      className={clsx(
                        "border-t border-white/5 cursor-pointer hover:bg-white/3 transition-colors",
                        rowHighlight
                      )}
                      onClick={() => toggle(p.ksi_player_id)}
                    >
                      <td className="px-3 py-2.5 text-white/25 text-xs font-mono">{p.shirt_no ?? "—"}</td>
                      <td className="px-3 py-2.5">
                        <div className="font-medium text-white/90 text-sm leading-snug">
                          {p.season?.player_name ?? p.name ?? `Player ${p.ksi_player_id}`}
                        </div>
                        {p.birth_year && (
                          <div className="text-xs text-white/30 font-mono">{p.birth_year}</div>
                        )}
                      </td>
                      <td className="px-3 py-2.5 text-right text-white/50 text-xs font-mono hidden sm:table-cell">
                        {p.season?.minutes ?? "—"}
                      </td>
                      <td className="px-3 py-2.5 text-right text-white/50 text-xs font-mono hidden sm:table-cell">
                        {p.season?.starts ?? "—"}
                      </td>
                      <td className="px-3 py-2.5 text-right text-white/50 text-xs font-mono hidden sm:table-cell">
                        {p.season?.goals ?? "—"}
                      </td>
                      <td className="px-3 py-2.5 text-right hidden sm:table-cell">
                        <FormDots recent5={p.recent5} />
                      </td>
                      <td className={clsx("px-3 py-2.5 text-right font-mono", impColor)}>
                        {imp > 0 ? (
                          <div className="flex flex-col items-end leading-none">
                            <span className="font-bold text-sm">{imp}</span>
                            <span className="text-xs opacity-40 mt-0.5">/{ceiling}</span>
                          </div>
                        ) : <span className="text-white/20">—</span>}
                      </td>
                    </tr>
                    {isOpen && (
                      <tr className="border-t border-white/5 bg-black/20">
                        <td />
                        <td colSpan={6} className="px-3 py-3 text-xs text-white/50 space-y-1">
                          <div className="flex gap-4 sm:hidden mb-2 text-white/60">
                            <span>Min: {p.season?.minutes ?? "—"}</span>
                            <span>GS: {p.season?.starts ?? "—"}</span>
                            <span>G: {p.season?.goals ?? "—"}</span>
                          </div>
                          {p.recent5 && (
                            <div className="mb-1 flex items-center gap-2">
                              <span className="text-white/30">Last 5:</span>
                              <span>{p.recent5.lastNMinutes}m in {p.recent5.lastNApps} apps ({p.recent5.lastNStarts} starts)</span>
                            </div>
                          )}
                          {p.seasons?.length > 0
                            ? p.seasons.map((s: any, i: number) => (
                                <div key={i} className="flex gap-1">
                                  <span className="text-white/30 w-8 shrink-0">2025</span>
                                  <span>{s.team_name ?? "—"}{s.club_ctx?.competition_tier ? ` · Tier ${s.club_ctx.competition_tier}` : ""}{s.club_ctx?.position ? ` · Pos ${s.club_ctx.position}` : ""}{s.minutes ? ` (${s.minutes}m)` : ""}</span>
                                </div>
                              ))
                            : <div className="flex gap-1"><span className="text-white/30 w-8">2025</span><span>—</span></div>
                          }
                          {p.prevSeasons?.length > 0
                            ? p.prevSeasons.map((ps: any, i: number) => (
                                <div key={i} className="flex gap-1">
                                  <span className="text-white/30 w-8 shrink-0">2024</span>
                                  <span>{ps.team_name ?? "—"}{ps.club_ctx?.competition_tier ? ` · Tier ${ps.club_ctx.competition_tier}` : ""}{ps.club_ctx?.position ? ` · Pos ${ps.club_ctx.position}` : ""}{ps.minutes ? ` (${ps.minutes}m)` : ""}</span>
                                </div>
                              ))
                            : <div className="flex gap-1"><span className="text-white/30 w-8">2024</span><span>—</span></div>
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
      <p className="mt-2 text-xs text-white/20 font-mono">Tap row to expand · L5 = minutes in last 5 apps</p>
    </div>
  );
}

// ---------- FORM DOTS ----------
function FormDots({ recent5 }: { recent5?: { lastNApps: number; lastNMinutes: number; lastNStarts: number } | null }) {
  if (!recent5 || recent5.lastNApps === 0) {
    return <span className="text-white/20 text-xs font-mono">—</span>;
  }

  const mins = recent5.lastNMinutes;
  const pct = Math.min(100, Math.round((mins / 450) * 100));
  const color = pct >= 70 ? "bg-emerald-500" : pct >= 40 ? "bg-yellow-500" : "bg-white/20";

  return (
    <div className="flex items-center gap-1.5 justify-end">
      <div className="w-12 h-1.5 rounded-full bg-white/8 overflow-hidden">
        <div className={clsx("h-full rounded-full", color)} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs font-mono text-white/40 w-8 text-right">{mins}</span>
    </div>
  );
}

// ---------- TEAM PANEL (season preview) ----------
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
  const isHome = title === "Home";

  return (
    <section className={clsx(
      "rounded-2xl border p-5",
      isHome ? "border-blue-500/15 bg-blue-950/10" : "border-orange-500/15 bg-orange-950/10"
    )}>
      <h3 className="text-base font-semibold">{summary?.team_name ?? `Team ${teamId}`}</h3>
      <div className="text-xs text-white/30 font-mono mt-0.5">Season {season}</div>

      <div className="mt-4 grid grid-cols-4 gap-2">
        {[
          { label: "MP", value: fmt(summary?.matches_played) },
          { label: "W/D/L", value: summary ? `${summary.wins}/${summary.draws}/${summary.losses}` : "—" },
          { label: "Pts", value: fmt(summary?.points) },
          { label: "GD", value: fmt(summary?.goal_diff) },
        ].map(({ label, value }) => (
          <div key={label} className="rounded-lg bg-black/30 px-2 py-2 text-center border border-white/5">
            <div className="text-xs text-white/30 font-mono">{label}</div>
            <div className="text-sm font-bold mt-0.5">{value}</div>
          </div>
        ))}
      </div>

      <div className="mt-5">
        <h4 className="text-xs font-mono text-white/40 uppercase tracking-wider mb-2">Likely XI</h4>
        <div className="space-y-0.5">
          {block.likelyXI.length === 0 ? (
            <div className="text-sm text-white/30 py-2">No data</div>
          ) : (
            block.likelyXI.slice(0, 11).map((p) => (
              <div key={p.ksi_player_id} className="flex items-center justify-between py-1.5 px-2 rounded-lg hover:bg-white/3 transition-colors">
                <span className="text-sm text-white/80">{p.player_name ?? `Player ${p.ksi_player_id}`}</span>
                <div className="flex items-center gap-3 text-xs font-mono text-white/30">
                  <span>{fmt(p.starts)} gs</span>
                  <span>{fmt(p.minutes)} m</span>
                  {Number(p.goals) > 0 && <span className="text-yellow-400">{p.goals}g</span>}
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </section>
  );
}