@echo off
echo === Iceland Update %date% %time% ===
cd /d C:\Users\jacob\OneDrive\Desktop\icelanddb\scripts

echo Running match overview scrape...
npx tsx 3.scrape-match-overview.ts --from 2026 --to 2026

echo Running lineup scrape...
npx tsx 4.scrape-match-lineups.ts --from 2026 --to 2026

echo === Done ===
pause