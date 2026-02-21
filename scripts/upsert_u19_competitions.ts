// scripts/upsert_u19_competitions.ts
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) throw new Error("Missing supabase env");

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// IMPORTANT: one row per competition ID (because competitions_pkey is ksi_competition_id)
const rows = [
  {
    ksi_competition_id: "196519",
    season_year: 2025,
    name: "2. flokkur karla B-lið B",
    gender: "Male",
    category: "U-19",
    tier: 2,
    is_phase: false,
    parent_competition_id: null,
  },
  {
    ksi_competition_id: "196520",
    season_year: 2025,
    name: "2. flokkur karla B-lið A",
    gender: "Male",
    category: "U-19",
    tier: 1,
    is_phase: false,
    parent_competition_id: null,
  },
];

async function main() {
  const { error } = await supabase.from("competitions").upsert(rows, {
    onConflict: "ksi_competition_id", // <- matches competitions_pkey
  });

  if (error) throw new Error(error.message);
  console.log("Upserted U19 competitions OK");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});