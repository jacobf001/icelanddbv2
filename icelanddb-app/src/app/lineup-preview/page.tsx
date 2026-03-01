"use client";

import { useMemo, useState } from "react";

type LineupPlayer = { ksi_player_id: string; name: string; shirt_no: number | null };
type TeamLineup = { starters: LineupPlayer[]; bench: LineupPlayer[] };

type ParsedLineupsResponse = {
  inputUrl: string;
  fetchUrl: string;
  counts: { startersHome: number; startersAway: number; benchHome: number; benchAway: number };
  home: TeamLineup;
  away: TeamLineup;
};

type PlayerSeasonRow = {
  season_year: number;
  ksi_team_id: string;
  ksi_player_id: string;
  player_name: string | null;
  matches_played: number;
  starts: number;
  minutes: number;
  goals: number;
  yellows: number;
  reds: number;
};

type RecentAppearanceRow = {
  ksi_player_id: string;
  ksi_match_id: string;
  kickoff_at: string | null;
  home_team_ksi_id: string | null;
  away_team_ksi_id: string | null;
  home_score: number | null;
  away_score: number | null;
  minute_in: number | null;
  minute_out: number | null;
  squad: string | null;
};

type LineupStatsResponse = {
  seasonYear: number;
  lastX: number;
  players: PlayerSeasonRow[];
  recentAppearances: Record<string, RecentAppearanceRow[]>;
};

const SEASONS = [2020, 2021, 2022, 2023, 2024, 2025];

function cleanUrl(u: string) {
  return u.trim();
}

function fmt(n: number | null | undefined) {
  if (n === null || n === undefined) return "—";
  return n.toLocaleString();
}

function playerSubline(p: LineupPlayer & { birth_year?: number | null }) {
  if (p.birth_year != null) return String(p.birth_year);
  return p.ksi_player_id ? String(p.ksi_player_id) : "—";
}


export default function LineupPreviewPage() {
  const [url, setUrl] = useState<string>("https://www.ksi.is/leikir-og-urslit/felagslid/leikur?id=6966621");
  const [seasonYear, setSeasonYear] = useState<number>(2025);
  const [lastX, setLastX] = useState<number>(5);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [parsed, setParsed] = useState<ParsedLineupsResponse | null>(null);
  const [stats, setStats] = useState<any>(null);

  const seasonMap = useMemo(() => {
    const m = new Map<string, PlayerSeasonRow>();
    for (const r of stats?.players ?? []) m.set(String(r.ksi_player_id), r);
    return m;
  }, [stats]);

  async function run() {
    setLoading(true);
    setError(null);
    setParsed(null);
    setStats(null);

    try {
      const u = cleanUrl(url);
      if (!u) throw new Error("Paste a KSI match URL");

      // 1) parse lineups
      const pRes = await fetch(`/api/lineups-from-report?url=${encodeURIComponent(u)}`, { cache: "no-store" });
      const pText = await pRes.text();
      let pJson: any = null;
      try {
        pJson = pText ? JSON.parse(pText) : null;
      } catch {
        // ignore
      }
      if (!pRes.ok) throw new Error(pJson?.error ?? `Parse failed (${pRes.status}): ${pText.slice(0, 300)}`);
      if (!pJson) throw new Error(`Parse failed (${pRes.status}): empty response body`);

      setParsed(pJson as ParsedLineupsResponse);

      // 2) fetch stats for all players found
      const playerIds = [
        ...(pJson?.home?.starters ?? []),
        ...(pJson?.home?.bench ?? []),
        ...(pJson?.away?.starters ?? []),
        ...(pJson?.away?.bench ?? []),
      ]
        .map((x: any) => String(x.ksi_player_id))
        .filter(Boolean);

      const sRes = await fetch(
        `/api/lineup-stats?url=${encodeURIComponent(u)}&season=${seasonYear}`,
        { cache: "no-store" }
      );


      const sText = await sRes.text();

      let sJson: any = null;
      try {
        sJson = sText ? JSON.parse(sText) : null;
      } catch {
        // ignore
      }

      if (!sRes.ok) {
        throw new Error(sJson?.error ?? `Stats failed (${sRes.status}). Response:\n${sText.slice(0, 500)}`);
      }
      if (!sJson) throw new Error(`Stats failed (${sRes.status}). Empty response body.`);

      setStats(sJson as LineupStatsResponse);
      console.log("stats home starters:", sJson?.home?.starters?.slice(0, 2));
      console.log("stats keys:", Object.keys(sJson ?? {}));
    } catch (e: any) {
      setError(e?.message ?? "Failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="min-h-screen bg-black text-white">
      <div className="mx-auto max-w-6xl px-6 py-10">
        <h1 className="text-3xl font-semibold">Lineup Preview</h1>
        <p className="mt-2 text-white/70">
          Paste a KSI match link → parse starting XIs and bench → show season-to-date + last {lastX} appearances.
        </p>

        <div className="mt-6 rounded-xl border border-white/10 bg-white/5 p-5">
          <div className="grid grid-cols-1 gap-3 md:grid-cols-12 md:items-end">
            <div className="md:col-span-7">
              <label className="mb-2 block text-sm text-white/70">KSI match URL</label>
              <input
                className="w-full rounded-lg border border-white/15 bg-black px-3 py-2 text-sm"
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                placeholder="https://www.ksi.is/leikir-og-urslit/felagslid/leikur?id=..."
              />
            </div>

            <div className="md:col-span-2">
              <label className="mb-2 block text-sm text-white/70">Season</label>
              <select
                className="w-full rounded-lg border border-white/15 bg-black px-3 py-2 text-sm"
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

            <div className="md:col-span-1">
              <label className="mb-2 block text-sm text-white/70">Last X</label>
              <input
                className="w-full rounded-lg border border-white/15 bg-black px-3 py-2 text-sm"
                type="number"
                min={1}
                max={20}
                value={lastX}
                onChange={(e) => setLastX(Number(e.target.value))}
              />
            </div>

            <div className="md:col-span-2">
              <button
                type="button"
                onClick={run}
                disabled={loading}
                className="w-full rounded-lg bg-white px-4 py-2 text-sm font-medium text-black hover:bg-white/90 disabled:opacity-50"
              >
                {loading ? "Running..." : "Run"}
              </button>
            </div>
          </div>

          {error && <div className="mt-3 whitespace-pre-wrap text-sm text-red-400">{error}</div>}
        
        </div>

      {parsed && (
        <div className="mt-8 grid grid-cols-1 gap-6 md:grid-cols-2">
          <LineupBlock title="Home" lineup={(stats as any)?.home ?? parsed.home} seasonMap={seasonMap} recent={stats?.recentAppearances ?? {}} />
          <LineupBlock title="Away" lineup={(stats as any)?.away ?? parsed.away} seasonMap={seasonMap} recent={stats?.recentAppearances ?? {}} />
        </div>
      )}
      </div>
    </main>
  );
}

function LineupBlock({
  title,
  lineup,
  seasonMap,
  recent,
}: {
  title: "Home" | "Away";
  lineup: { starters: LineupPlayer[]; bench: LineupPlayer[] };
  seasonMap: Map<string, PlayerSeasonRow>;
  recent: Record<string, RecentAppearanceRow[]>;
}) {
  return (
    <section className="rounded-xl border border-white/10 bg-white/5 p-5">
      <h2 className="text-xl font-semibold">{title} lineup</h2>

      <h3 className="mt-5 text-base font-semibold">Starters</h3>
      <div className="mt-2 overflow-hidden rounded-lg border border-white/10">
        <table className="w-full text-sm">
          <thead className="bg-white/5 text-white/70">
            <tr>
              <th className="px-3 py-2 text-left">#</th>
              <th className="px-3 py-2 text-left">Player</th>
              <th className="px-3 py-2 text-right">Min</th>
              <th className="px-3 py-2 text-right">Starts</th>
              <th className="px-3 py-2 text-right">Goals</th>
              <th className="px-3 py-2 text-right">Last</th>
            </tr>
          </thead>
          <tbody>
            {lineup.starters.map((p) => {
              const row = seasonMap.get(p.ksi_player_id);
              const last = recent[p.ksi_player_id]?.length ?? 0;

              return (
                <tr key={p.ksi_player_id} className="border-t border-white/10">
                  <td className="px-3 py-2">{p.shirt_no ?? "—"}</td>
                  <td className="px-3 py-2">
                    <div className="leading-tight">
                      <div>{row?.player_name ?? p.name ?? `Player ${p.ksi_player_id}`}</div>
                      <div className="text-xs text-white/50">{playerSubline(p as any)}</div>
                    </div>
                  </td>
                  <td className="px-3 py-2 text-right">{fmt(row?.minutes)}</td>
                  <td className="px-3 py-2 text-right">{fmt(row?.starts)}</td>
                  <td className="px-3 py-2 text-right">{fmt(row?.goals)}</td>
                  <td className="px-3 py-2 text-right">{last ? `${last}` : "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <h3 className="mt-6 text-base font-semibold">Bench</h3>
      <div className="mt-2 overflow-hidden rounded-lg border border-white/10">
        <table className="w-full text-sm">
          <thead className="bg-white/5 text-white/70">
            <tr>
              <th className="px-3 py-2 text-left">#</th>
              <th className="px-3 py-2 text-left">Player</th>
              <th className="px-3 py-2 text-right">Min</th>
              <th className="px-3 py-2 text-right">Starts</th>
              <th className="px-3 py-2 text-right">Goals</th>
            </tr>
          </thead>
          <tbody>
            {lineup.bench.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-3 py-3 text-white/60">
                  No bench parsed.
                </td>
              </tr>
            ) : (
              lineup.bench.map((p) => {
                const row = seasonMap.get(p.ksi_player_id);

                return (
                  <tr key={p.ksi_player_id} className="border-t border-white/10">
                    <td className="px-3 py-2">{p.shirt_no ?? "—"}</td>
                    <td className="px-3 py-2">
                      <div className="leading-tight">
                        <div>{row?.player_name ?? p.name ?? `Player ${p.ksi_player_id}`}</div>
                       <div className="text-xs text-white/50">{playerSubline(p as any)}</div>
                      </div>
                    </td>
                    <td className="px-3 py-2 text-right">{fmt(row?.minutes)}</td>
                    <td className="px-3 py-2 text-right">{fmt(row?.starts)}</td>
                    <td className="px-3 py-2 text-right">{fmt(row?.goals)}</td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}