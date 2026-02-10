import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

async function main() {
  // sanity check: list tables via a tiny query that should always work:
  const { data, error } = await supabase.from("competitions").select("*").limit(1);

  if (error) {
    console.error("Supabase error:", error.message);
    process.exit(1);
  }

  console.log("Connected OK. competitions sample:", data);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
