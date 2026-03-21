/**
 * run-weekly-update.ts
 *
 * Runs the full KSI scrape pipeline for the current season.
 * Safe to run at any time — all scripts use upserts and skip already-processed data.
 *
 * Usage:
 *   npx ts-node run-weekly-update.ts
 *   npx ts-node run-weekly-update.ts --dry        # dry run (no writes)
 *   npx ts-node run-weekly-update.ts --year 2026  # specific year
 *
 * Recommended: run weekly via Windows Task Scheduler or cron.
 */

import { execSync } from "child_process";
import path from "path";

const year = (() => {
  const i = process.argv.indexOf("--year");
  return i !== -1 ? process.argv[i + 1] : String(new Date().getFullYear());
})();

const dry = process.argv.includes("--dry");
const dryFlag = dry ? " --dry" : "";

const SCRIPTS_DIR = path.resolve(process.cwd());

function run(script: string, extraArgs = "") {
  const cmd = `npx ts-node "${path.join(SCRIPTS_DIR, script)}"${extraArgs}`;
  console.log(`\n${"=".repeat(60)}`);
  console.log(`▶ ${script}${extraArgs}`);
  console.log("=".repeat(60));
  try {
    execSync(cmd, { stdio: "inherit", cwd: SCRIPTS_DIR });
    console.log(`✅ ${script} complete`);
  } catch (e: any) {
    console.error(`❌ ${script} failed — continuing pipeline`);
    // Don't exit — continue with remaining scripts
  }
}

async function main() {
  const start = Date.now();
  console.log(`\nKSI Weekly Update — ${year}${dry ? " (DRY RUN)" : ""}`);
  console.log(`Started: ${new Date().toISOString()}\n`);

  // Step 1: Discover competitions for current season
  run("1.discover-competitions.ts", dryFlag);

  // Step 2: Discover completed matches (only those with scores)
  run("2.discover-matches.ts", ` --from ${year} --to ${year} --sleep 300${dryFlag}`);

  // Step 3: Scrape match overviews (scores, teams, kickoff times)
  // Processes matches missing home_team_ksi_id or home_score
  run("3.scrape-match-overview.ts", ` --from ${year} --to ${year} --sleep 300${dryFlag}`);

  // Step 4: Scrape lineups for matches not yet scraped
  run("4.scrape-match-lineups.ts", ` --from ${year} --to ${year} --sleep 300${dryFlag}`);

  // Step 5: Scrape events (goals, cards, subs) for matches not yet scraped
  run("5.scrape-events-overview.ts", ` --from ${year} --to ${year} --sleep 300${dryFlag}`);

  // Step 6: Scrape birth years for any new players seen in lineups
  run("6.scrape-player-birth-years.ts", `${dryFlag}`);

  const elapsed = Math.round((Date.now() - start) / 1000);
  console.log(`\n${"=".repeat(60)}`);
  console.log(`✅ Weekly update complete in ${elapsed}s`);
  console.log(`Finished: ${new Date().toISOString()}`);
  console.log("=".repeat(60));
}

main().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});