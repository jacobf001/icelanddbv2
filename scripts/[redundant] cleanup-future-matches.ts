import * as dotenv from "dotenv";
import path from "path";
import { createClient } from "@supabase/supabase-js";
dotenv.config({ path: path.resolve(process.cwd(), "../.env"), override: true });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL) throw new Error("SUPABASE_URL is required");
if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY is required");

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const dry = process.argv.includes("--dry");

async function main() {
  console.log(`Cleanup future matches | Dry: ${dry ? "YES" : "NO"}`);

  const now = new Date().toISOString();

  // Count first
  const { count, error: countErr } = await supabase
    .from("matches")
    .select("*", { count: "exact", head: true })
    .is("home_score", null)
    .gt("kickoff_at", now);

  if (countErr) throw new Error(countErr.message);
  console.log(`Future matches with no score: ${count}`);

  if (!count || count === 0) {
    console.log("Nothing to delete.");
    return;
  }

  if (dry) {
    console.log("DRY: would delete these matches.");
    return;
  }

  const { error } = await supabase
    .from("matches")
    .delete()
    .is("home_score", null)
    .gt("kickoff_at", now);

  if (error) throw new Error(error.message);
  console.log(`✅ Deleted ${count} future matches.`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});