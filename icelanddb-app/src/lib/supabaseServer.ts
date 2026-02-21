import { createClient } from "@supabase/supabase-js";

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!url) throw new Error("SUPABASE_URL missing");
if (!key) throw new Error("SUPABASE_SERVICE_ROLE_KEY missing");

export const supabaseAdmin = createClient(url, key, {
  auth: { persistSession: false },
});
