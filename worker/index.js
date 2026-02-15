/**
 * BlackSignals Stream Worker (MVP)
 * - Fetches Kagi RSS feeds + curated Black US RSS feeds
 * - Applies impact term scoring to Kagi items
 * - Keeps rolling last 3 hours only
 * - Dedupes + returns a single, sorted list (headline-only UI)
 *
 * No storage, no KV. Just short caching at the edge.
 */

const FEEDS_BASE =
  "https://raw.githubusercontent.com/princenyc/blacksignals/main/feeds";

const DEFAULT_CACHE_SECONDS = 900; // 15 minutes
const WINDOW_HOURS = 3;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // CORS + basic routing
    if (request.method === "OPTIONS") return handleOptions();

    if (url.pathname === "/" || url.pathname === "/api/health") {
      return json(
        {
          ok: true,
          service: "blacksignals-stream-worker",
          now: new Date().toISOString(),
        },
        200
      );
    }

    if (url.pathname === "/api/stream") {
      // edge cache
      const cacheKey = new Request(url.toString(), request);
      const cache = caches.default;
      const cached = await cache.match(cacheKey);
      if (cached) return withCors(cached);

      const result = await buildStream();

      const response = json(result, 200, {
        "Cache-Control": `public, max-age=${DEFAULT_CACHE_SECONDS}`,
      });
      ctx.waitUntil(cache.put(cacheKey, response.clone()));
      return withCors(response);
    }

    return withCors(
      json(
        {
          ok: false,
          error: "Not found",
          routes: ["/api/health", "/api/stream"],
        },
        404
      )
    );
  },
};

async function buildStream() {
  const now = Date.now();
  const cutoff = now - WINDOW_HOURS * 60 * 60 * 1000;

  // Load configs from GitHub raw
  const [impactTerms, curated, kagiInputs] = await Promise.all([
    fetchJson(`${FEEDS_BASE}/impact_terms.json`),
    fetchJson(`${FEEDS_BASE}/curated_black_us.json`),
    fetchJson(`${FEEDS_BASE}/kagi_inputs.json`),
  ]);

  const threshold = impactTerms.threshold_score ?? 3;

  // 1) Fetch and parse RSS for Kagi feeds
  const kagiFeedUrls = (kagiInputs.feeds ?? []).map((f) => f.rss_url);
  const kagiItemsRaw = await fetchAndParseManyRss(kagiFeedUrls, "kagi");

  // Filter window first (cheap)
  const kagiItemsWindowed = kagiItemsRaw.filter((it) => it.published_ts >= cutoff);

  // Apply impact scoring/filter
  const kagiFiltered = kagiItemsWindowed
    .map((it) => {
      const scored = scoreImpact(it, impactTerms);
      return { ...it, ...scored };
    })
    .filter((it) => {
      // Exclusions (headline/desc contains any exclusion phrase)
      const text = `${it.title} ${it.description ?? ""}`.toLowerCase();
      const exclusions = (impactTerms.exclusions ?? []).map((x) => x.toLowerCase());
      if (exclusions.some((ex) => text.includes(ex))) return false;

      return it.impact_score >= threshold;
    });

  // 2) Fetch and parse curated feeds (auto-include)
  const curatedUrls = (curated.feeds ?? []).map((f) => f.rss_url);
  const curatedItemsRaw = await fetchAndParseManyRss(curatedUrls, "curated");

  const curatedItemsWindowed = curatedItemsRaw
    .filter((it) => it.published_ts >= cutoff)
    .map((it) => ({
      ...it,
      impact_score: 999,
      match_reasons: ["curated_source"],
    }));

  // 3) Merge + dedupe + sort desc
  const merged = [...curatedItemsWindowed, ...kagiFiltered];

  const deduped = dedupeItems(merged);

  deduped.sort((a, b) => b.published_ts - a.published_ts);

  // limit to keep it light
  const limited = deduped.slice(0, 60);

  return {
    ok: true,
    generated_at: new Date(now).toISOString(),
    window_hours: WINDOW_HOURS,
    count: limited.length,
    items: limited.map((it) => ({
      title: it.title,
      url: it.url,
      source: it.source,
      published_at: new Date(it.published_ts).toISOString(),
      origin: it.origin,
      impact_score: it.impact_score,
      match_reasons: it.match_reasons,
    })),
  };
}

function scoreImpact(item, impactTerms) {
  const text = `${item.title} ${item.description ?? ""}`.toLowerCase();

  const reasons = [];
  let score = 0;

  const tiers = impactTerms.tiers ?? {};
  const boosters = impactTerms.context_boosters ?? { weight: 1, terms: [] };

  // tiers
  for (const [tierName, tierObj] of Object.entries(tiers)) {
    const weight = tierObj.weight ?? 1;
    const terms = tierObj.terms ?? [];
    for (const t of terms) {
      const term = String(t).toLowerCase();
      if (term && text.includes(term)) {
        score += weight;
        reasons.push(`${tierName}:${t}`);
      }
    }
  }

  // context boosters
  const bWeight = boosters.weight ?? 1;
  const bTerms = boosters.terms ?? [];
  for (const t of bTerms) {
    const term = String(t).toLowerCase();
    if (term && text.includes(term)) {
      score += bWeight;
      reasons.push(`context:${t}`);
    }
  }

  return {
    impact_score: score,
    match_reasons: reasons.length ? reasons : ["no_match"],
  };
}

function dedupeItems(items) {
  const seen = new Set();
  const out = [];

  for (const it of items) {
    // primary key: normalized URL
    const key = normalizeUrl(it.url) || `${it.source}|${it.title}`.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(it);
  }

  return out;
}

function normalizeUrl(u) {
  try {
    const url = new URL(u);
    // strip common tracking params
    ["utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid"].forEach((p) =>
      url.searchParams.delete(p)
    );
    url.hash = "";
    return url.toString();
  } catch {
    return "";
  }
}

async function fetchAndParseManyRss(urls, origin) {
  const results = await Promise.allSettled(urls.map((u) => fetchAndParseRss(u, origin)));
  const items = [];
  for (const r of results) {
    if (r.status === "fulfilled") items.push(...r.value);
  }
  return items;
}

async function fetchAndParseRss(feedUrl, origin) {
  const res = await fetch(feedUrl, {
    headers: {
      "User-Agent": "BlackSignalsStream/1.0",
      "Accept": "application/rss+xml, application/atom+xml, text/xml, */*",
    },
  });
  if (!res.ok) return [];

  const xml = await res.text();
  return parseRssXml(xml, feedUrl, origin);
}

/**
 * Minimal RSS/Atom parser using regex + basic XML extraction.
 * (Enough for MVP; later we can harden it.)
 */
function parseRssXml(xml, feedUrl, origin) {
  const items = [];

  const source = sourceNameFromFeedUrl(feedUrl);

  // RSS <item>...</item>
  const itemMatches = xml.match(/<item[\s\S]*?<\/item>/gi) || [];

  for (const block of itemMatches) {
    const title = getTagText(block, "title");
    const link = getTagText(block, "link");
    const pubDate = getTagText(block, "pubDate") || getTagText(block, "dc:date");
    const description = stripHtml(getTagText(block, "description"));

    const ts = dateToTs(pubDate);
    if (!title || !link || !ts) continue;

    items.push({
      title: decodeXml(title).trim(),
      url: decodeXml(link).trim(),
      source,
      origin,
      description,
      published_ts: ts,
    });
  }

  // Atom <entry>...</entry>
  if (items.length === 0) {
    const entryMatches = xml.match(/<entry[\s\S]*?<\/entry>/gi) || [];
    for (const block of entryMatches) {
      const title = getTagText(block, "title");
      const link = getAtomLink(block);
      const updated = getTagText(block, "updated") || getTagText(block, "published");
      const summary = stripHtml(getTagText(block, "summary") || getTagText(block, "content"));

      const ts = dateToTs(updated);
      if (!title || !link || !ts) continue;

      items.push({
        title: decodeXml(title).trim(),
        url: decodeXml(link).trim(),
        source,
        origin,
        description: summary,
        published_ts: ts,
      });
    }
  }

  return items;
}

function getTagText(xml, tag) {
  const re = new RegExp(`<${escapeRegExp(tag)}[^>]*>([\\s\\S]*?)<\\/${escapeRegExp(tag)}>`, "i");
  const m = xml.match(re);
  return m ? m[1] : "";
}

function getAtomLink(entryXml) {
  // <link href="..."/>
  const m = entryXml.match(/<link[^>]*href=["']([^"']+)["'][^>]*\/?>/i);
  if (m) return m[1];
  // fallback <link>text</link>
  return getTagText(entryXml, "link");
}

function stripHtml(s) {
  return (s || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

function dateToTs(dateStr) {
  if (!dateStr) return 0;
  const ts = Date.parse(dateStr);
  return Number.isFinite(ts) ? ts : 0;
}

function decodeXml(s) {
  return (s || "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

function sourceNameFromFeedUrl(feedUrl) {
  try {
    const u = new URL(feedUrl);
    return u.hostname.replace(/^www\./, "");
  } catch {
    return "unknown";
  }
}

async function fetchJson(url) {
  const res = await fetch(url, { headers: { "Accept": "application/json" } });
  if (!res.ok) throw new Error(`Failed to fetch JSON: ${url}`);
  return res.json();
}

function json(obj, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      ...extraHeaders,
    },
  });
}

function withCors(res) {
  const headers = new Headers(res.headers);
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("Access-Control-Allow-Methods", "GET,OPTIONS");
  headers.set("Access-Control-Allow-Headers", "Content-Type");
  return new Response(res.body, { status: res.status, headers });
}

function handleOptions() {
  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Max-Age": "86400",
    },
  });
}

function escapeRegExp(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
