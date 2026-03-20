// scripts/evaluate-lineup-model.ts
import dotenv from "dotenv";
dotenv.config({ path: ".env.local" });

import fs from "node:fs";
import path from "node:path";

type ResultCode = "H" | "D" | "A";

function getResultCode(homeScore: number | null, awayScore: number | null): ResultCode | null {
  if (homeScore == null || awayScore == null) return null;
  if (homeScore > awayScore) return "H";
  if (homeScore < awayScore) return "A";
  return "D";
}

function toCsv(rows: Record<string, unknown>[]) {
  if (!rows.length) return "";

  const headers = Array.from(
    rows.reduce((set, row) => {
      Object.keys(row).forEach((k) => set.add(k));
      return set;
    }, new Set<string>()),
  );

  const escape = (v: unknown) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    if (s.includes(",") || s.includes('"') || s.includes("\n")) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  };

  return [
    headers.join(","),
    ...rows.map((row) => headers.map((h) => escape(row[h])).join(",")),
  ].join("\n");
}

async function main() {
  const { supabaseAdmin } = await import("../src/lib/supabaseServer");
  const { analyzeMatchById } = await import("../src/lib/model/analyzeMatchById");

  const seasonArg = process.argv[2] ?? "all";
  const seasonYear = seasonArg === "all" ? null : Number(seasonArg);

  const allMatches: any[] = [];
  const pageSize = 1000;
  let from = 0;

  while (true) {
    let query = supabaseAdmin
      .from("matches")
      .select(`
        ksi_match_id,
        season_year,
        kickoff_at,
        home_team_ksi_id,
        away_team_ksi_id,
        home_score,
        away_score
      `)
      .not("home_score", "is", null)
      .not("away_score", "is", null)
      .order("kickoff_at", { ascending: true })
      .range(from, from + pageSize - 1);

    if (seasonYear !== null) {
      query = query.eq("season_year", seasonYear);
    }

    const { data, error } = await query;
    if (error) throw new Error(error.message);

    const batch = data ?? [];
    allMatches.push(...batch);

    if (batch.length < pageSize) break;
    from += pageSize;
  }

  const matches = allMatches;

  const seasons = [...new Set(matches.map((m: any) => Number(m.season_year)))].sort((a, b) => a - b);
  console.log("Seasons in export:", seasons);
  console.log("Match count:", matches.length);

  const out: Record<string, unknown>[] = [];
  let done = 0;
  let skipped = 0;

  for (const m of matches ?? []) {
    const matchId = String((m as any).ksi_match_id);

    try {
      const pred = await analyzeMatchById(matchId);
      if (!pred) {
        skipped += 1;
        continue;
      }

      const homeScore = Number((m as any).home_score);
      const awayScore = Number((m as any).away_score);
      const actual = getResultCode(homeScore, awayScore);

      console.log(pred.home_competition_name, pred.home_tier);

      out.push({
        match_id: pred.match_id,
        kickoff_at: pred.kickoff_at,
        season_year: pred.season_year,
        home_team: pred.home_team_name,
        away_team: pred.away_team_name,

        home_competition_name: pred.home_competition_name,
        away_competition_name: pred.away_competition_name,
        home_tier: pred.home_tier,
        away_tier: pred.away_tier,

        home_score: homeScore,
        away_score: awayScore,
        actual_result: actual,

        pred_home: pred.probabilities.home,
        pred_draw: pred.probabilities.draw,
        pred_away: pred.probabilities.away,

        fair_home: pred.fair_odds.home,
        fair_draw: pred.fair_odds.draw,
        fair_away: pred.fair_odds.away,

        home_strength: pred.teamStrength.home,
        away_strength: pred.teamStrength.away,

        home_rating_total: pred.home.rating.total,
        away_rating_total: pred.away.rating.total,
        home_coverage: pred.home.rating.coverage,
        away_coverage: pred.away.rating.coverage,
        home_missing_impact: pred.home.missingImpact,
        away_missing_impact: pred.away.missingImpact,

        women: pred.women,
        model_version: pred.model_version,

        picked_result:
          pred.probabilities.home >= pred.probabilities.draw && pred.probabilities.home >= pred.probabilities.away
            ? "H"
            : pred.probabilities.draw >= pred.probabilities.home &&
                pred.probabilities.draw >= pred.probabilities.away
              ? "D"
              : "A",

        picked_correct:
          actual === null
            ? null
            : (
                (pred.probabilities.home >= pred.probabilities.draw &&
                  pred.probabilities.home >= pred.probabilities.away &&
                  actual === "H") ||
                (pred.probabilities.draw >= pred.probabilities.home &&
                  pred.probabilities.draw >= pred.probabilities.away &&
                  actual === "D") ||
                (pred.probabilities.away >= pred.probabilities.home &&
                  pred.probabilities.away >= pred.probabilities.draw &&
                  actual === "A")
              ),
      });

      done += 1;
      if (done % 25 === 0) {
        console.log(`Processed ${done} matches...`);
      }
    } catch (err) {
      skipped += 1;
      console.error(`Failed on match ${matchId}:`, err);
    }
  }

  const csv = toCsv(out);
  const outDir = path.join(process.cwd(), "outputs");
  fs.mkdirSync(outDir, { recursive: true });

  const seasonLabel = seasonYear === null ? "all" : String(seasonYear);
  const filePath = path.join(outDir, `eval_${seasonLabel}_${Date.now()}.csv`);
  fs.writeFileSync(filePath, csv, "utf8");

  console.log(`Done. Wrote ${out.length} rows to ${filePath}. Skipped ${skipped}.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});