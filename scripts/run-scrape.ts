import { execSync } from "child_process";

const scripts = [
  "1.discover-competitions.ts",
  "2.discover-matches.ts", 
  "3.scrape-match-overview.ts",
  "4.scrape-match-lineups.ts",
  "5.scrape-events-overview.ts",
  "6.scrape-player-birth-years.ts",
  "cleanup-future-matches.ts",
];

for (const script of scripts) {
  console.log(`Running ${script}...`);
  execSync(`npx ts-node ${script}`, { stdio: "inherit" });
  console.log(`Done: ${script}`);
}