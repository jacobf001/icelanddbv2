import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

async function fetchHtml(url: string) {
  const res = await fetch(url, { headers: { "user-agent": "iceland-db-scraper/1.0" } });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} for ${url}`);
  return await res.text();
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function run(limit = 1000, sleepMs = 200) {
  const { data: matches, error } = await supabase
    .from("matches")
    .select("ksi_match_id")
    .order("ksi_match_id", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!matches?.length) {
    console.log("No matches found.");
    return;
  }

  for (const m of matches) {
    const id = m.ksi_match_id;

    const overviewUrl = `https://www.ksi.is/leikir-og-urslit/felagslid/leikur?id=${id}&banner-tab=overview`;
    const reportUrl = `https://www.ksi.is/leikir-og-urslit/felagslid/leikur?id=${id}&banner-tab=report`;

    const overview = await fetchHtml(overviewUrl);
    const report = await fetchHtml(reportUrl);

    const { error: upErr } = await supabase.from("match_pages").upsert(
      [
        { ksi_match_id: id, page: "overview", html: overview },
        { ksi_match_id: id, page: "report", html: report },
      ],
      { onConflict: "ksi_match_id,page" }
    );
    if (upErr) throw upErr;

    await supabase
      .from("matches")
      .update({ scraped_overview_at: new Date().toISOString(), scraped_report_at: new Date().toISOString() })
      .eq("ksi_match_id", id);

    console.log(`Saved pages for match ${id}`);
    await sleep(sleepMs);
  }
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
