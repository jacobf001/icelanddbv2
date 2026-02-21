import { NextResponse } from "next/server";
import * as cheerio from "cheerio";

export const runtime = "nodejs";

type LineupPlayer = {
  ksi_player_id: string;
  name: string;
  shirt_no: number | null;
};

type TeamLineup = {
  starters: LineupPlayer[];
  bench: LineupPlayer[];
};

type TeamMeta = {
  ksi_team_id: string | null;
  team_name: string | null;
};

type ParsedTeam = {
  ksi_team_id: string | null;
  team_name: string | null;
};

type TeamsBlock = {
  home: ParsedTeam;
  away: ParsedTeam;
};


function withReportTab(inputUrl: string) {
  const u = new URL(inputUrl);
  u.searchParams.set("banner-tab", "report");
  return u.toString();
}

function uniqById(xs: LineupPlayer[]) {
  const seen = new Set<string>();
  const out: LineupPlayer[] = [];
  for (const x of xs) {
    if (seen.has(x.ksi_player_id)) continue;
    seen.add(x.ksi_player_id);
    out.push(x);
  }
  return out;
}

function parseTeamsFromHtml($: cheerio.CheerioAPI): TeamsBlock {
  // These are the two club badges + names in the middle header area:
  // /oll-mot/mot/lid?id=5170&competitionId=...
  const teamAnchors = $('a[href^="/oll-mot/mot/lid?id="]');

  // Fallback default
  const empty: TeamsBlock = {
    home: { ksi_team_id: null, team_name: null },
    away: { ksi_team_id: null, team_name: null },
  };

  if (!teamAnchors || teamAnchors.length < 2) return empty;

  function readTeam(el: any): ParsedTeam {
    const href = $(el).attr("href") ?? "";
    const m = href.match(/[?&]id=(\d+)/);
    const id = m?.[1] ? String(m[1]) : null;

    // In your snippet the name is: <span class="body-4 ...">Kría</span>
    // But safer: just take the visible text inside the anchor, trimmed.
    const name = $(el).find("span").first().text().replace(/\s+/g, " ").trim() || null;

    return { ksi_team_id: id, team_name: name };
  }

  // Order on page is left-to-right: home then away
  const home = readTeam(teamAnchors.get(0));
  const away = readTeam(teamAnchors.get(1));

  return { home, away };
}

function parseAnchor($: cheerio.CheerioAPI, el: any): LineupPlayer | null {
  const $a = $(el) as any;

  const href = $a.attr("href") ?? "";
  const idMatch = href.match(/[?&]id=(\d+)/);
  if (!idMatch) return null;

  // number is in the first span (w-[20rem]) in your HTML
  const numText =
    $a.find('span[class*="w-[20rem]"]').first().text().trim() ||
    $a.find("span").first().text().trim();

  const shirt_no = /^\d{1,2}$/.test(numText) ? Number(numText) : null;

  // Full name is in span with class containing "l:inline" (colon must be escaped)
  const fullName =
    $a.find("span.l\\:inline").first().text().trim() ||
    // fallback: second span after number
    $a.find("span").eq(1).text().trim() ||
    "";

  if (!fullName) return null;

  // strip trailing role whitespace
  const name = fullName.replace(/\s+/g, " ").trim();

  return {
    ksi_player_id: idMatch[1],
    name,
    shirt_no,
  };
}

function parseList($: cheerio.CheerioAPI, container: any): LineupPlayer[] {
  const out: LineupPlayer[] = [];
  $(container)
    .find('a[href^="/leikmenn/leikmadur?id="]')
    .each((_, el) => {
      const p = parseAnchor($, el);
      if (p) out.push(p);
    });
  return uniqById(out);
}

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const inputUrl = searchParams.get("url");

  if (!inputUrl) return NextResponse.json({ error: "Missing url param" }, { status: 400 });

  let safe: URL;
  try {
    safe = new URL(inputUrl);
    if (safe.hostname !== "www.ksi.is" && safe.hostname !== "ksi.is") {
      return NextResponse.json({ error: "Only ksi.is URLs allowed" }, { status: 400 });
    }
  } catch {
    return NextResponse.json({ error: "Invalid url" }, { status: 400 });
  }

  const fetchUrl = withReportTab(safe.toString());
  const res = await fetch(fetchUrl, {
    cache: "no-store",
    headers: {
      "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "accept-language": "en-GB,en;q=0.9",
    },
  });

  if (!res.ok) {
    return NextResponse.json({ error: `Fetch failed (${res.status})`, fetchUrl }, { status: 400 });
  }

  const html = await res.text();
  const $ = cheerio.load(html);
  const teams = parseTeamsFromHtml($);

  // This is the exact grid you pasted
  const grid = $("div.grid.grid-cols-2").first();
  if (!grid.length) {
    return NextResponse.json(
      { error: "Could not find the 2-col lineup grid", fetchUrl },
      { status: 200 },
    );
  }

  // Children order (based on your pasted HTML):
  // 0 home "Byrjunarlið" header
  // 1 away "Byrjunarlið" header
  // 2 home starters list (flex flex-col...)
  // 3 away starters list
  // 4 home "Varamenn" header
  // 5 away "Varamenn" header
  // 6 home bench list
  // 7 away bench list
  const children = grid.children().toArray();

  const homeStartersEl = children[2];
  const awayStartersEl = children[3];
  const homeBenchEl = children[6];
  const awayBenchEl = children[7];

  if (!homeStartersEl || !awayStartersEl) {
    return NextResponse.json(
      {
        error: "Grid did not contain expected starter blocks",
        fetchUrl,
        childCount: children.length,
      },
      { status: 200 },
    );
  }

  const homeStarters = parseList($, homeStartersEl);
  const awayStarters = parseList($, awayStartersEl);
  const homeBench = homeBenchEl ? parseList($, homeBenchEl) : [];
  const awayBench = awayBenchEl ? parseList($, awayBenchEl) : [];

  return NextResponse.json({
    inputUrl,
    fetchUrl,
    counts: {
      startersHome: homeStarters.length,
      startersAway: awayStarters.length,
      benchHome: homeBench.length,
      benchAway: awayBench.length,
    },
    teams,
    home: { starters: homeStarters, bench: homeBench } satisfies TeamLineup,
    away: { starters: awayStarters, bench: awayBench } satisfies TeamLineup,
  });
}
