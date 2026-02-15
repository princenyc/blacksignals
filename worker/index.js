/**
 * BlackSignals Stream Worker (MVP)
 * - Fetches Kagi RSS feeds + curated Black US RSS feeds
 * - Scores items via impact terms + influence (named people boosts) + recency + source weight
 * - Keeps rolling last N hours only
 * - Dedupes & returns a single sorted list (headline + source + score)
 * - No storage. Uses short caching at the edge.
 */

const FEEDS_BASE =
  "https://raw.githubusercontent.com/princenyc/blacksignals/main/feeds";

const KAGI_INPUTS_PATH = "kagi_inputs.json";
const CURATED_FEEDS_PATH = "curated_black_us.json";
const IMPACT_TERMS_PATH = "impact_terms.json";
const INFLUENCE_PATH = "person_influence.json"; // <-- change if your filename differs

const DEFAULT_CACHE_SECONDS = 900; // 15 min
const DEFAULT_WINDOW_HOURS = 3;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === "OPTIONS") return handleOptions();

    // Routes
    if (url.pathname === "/" || url.pathname === "/api/health") {
      return json(withCors({ ok: true, service: "blacksignals-stream-worker", now: new Date().toISOString() }));
    }

    if (url.pathname === "/api/stream") {
      // Allow overriding window/cache for testing:
      // /api/stream?window=6&cache=60
      const windowHours = clampInt(url.searchParams.get("window"), 1, 48, DEFAULT_WINDOW_HOURS);
      const cacheSeconds = clampInt(url.searchParams.get("cache"), 0, 86400, DEFAULT_CACHE_SECONDS);

      // Cache key includes parameters that affect output
      const cacheKey = new Request(
        `${url.origin}${url.pathname}?window=${windowHours}&cache=${cacheSeconds}`,
        request
      );

      // Edge cache
      const cached = await caches.default.match(cacheKey);
      if (cached) return withCors(cached);

      const started = Date.now();
      const result = await buildStream({ windowHours });

      const res = json(result, 200, {
        "Cache-Control": `public, max-age=${cacheSeconds}`,
        "X-Worker-Time-MS": String(Date.now() - started),
      });

      ctx.waitUntil(caches.default.put(cacheKey, res.clone()));
      return withCors(res);
    }

    return withCors(json({ ok: false, error: "Not found" }, 404));
  },
};

/* ---------------------------
   Core pipeline
---------------------------- */

async function buildStream({ windowHours }) {
  // Load configs
  const [kagiInputs, curatedFeeds, impactTerms, influence] = await Promise.all([
    fetchJson(`${FEEDS_BASE}/${KAGI_INPUTS_PATH}`, { fallback: { feeds: [] } }),
    fetchJson(`${FEEDS_BASE}/${CURATED_FEEDS_PATH}`, { fallback: { feeds: [] } }),
    fetchJson(`${FEEDS_BASE}/${IMPACT_TERMS_PATH}`, { fallback: defaultImpactTerms() }),
    fetchJson(`${FEEDS_BASE}/${INFLUENCE_PATH}`, { fallback: { people: [] } }),
  ]);

  // Normalize feed lists
  const feeds = [];
  // Kagi list might be { feeds: [{url,label,weight,origin}] } or an array; we support both
  const kagiFeedList = Array.isArray(kagiInputs) ? kagiInputs : (kagiInputs.feeds || []);
  const curatedFeedList = Array.isArray(curatedFeeds) ? curatedFeeds : (curatedFeeds.feeds || []);

  for (const f of kagiFeedList) {
    if (!f?.url) continue;
    feeds.push({
      url: f.url,
      label: f.label || f.source || guessHostname(f.url),
      weight: numOr(f.weight, 1),
      origin: "kagi",
    });
  }
  for (const f of curatedFeedList) {
    if (!f?.url) continue;
    feeds.push({
      url: f.url,
      label: f.label || f.source || guessHostname(f.url),
      weight: numOr(f.weight, 1),
      origin: "curated",
    });
  }

  // Influence structure expected:
  // { people: [ { name, role?, boost?, aliases?:[] } ] }
  const influenceIndex = buildInfluenceIndex(influence);

  const now = Date.now();
  const windowMs = windowHours * 60 * 60 * 1000;
  const cutoff = now - windowMs;

  // Fetch & parse feeds in parallel (bounded)
  const items = await mapWithConcurrency(feeds, 8, async (feed) => {
    const parsed = await fetchAndParseFeed(feed.url);
    return parsed.map((it) => ({
      ...it,
      feed_label: feed.label,
      feed_origin: feed.origin,
      feed_weight: feed.weight,
    }));
  }).then((arrays) => arrays.flat());

  // Filter to window; if published date missing, keep but treat as "weak recency"
  const recent = items.filter((it) => {
    if (!it.published_at) return true;
    const t = Date.parse(it.published_at);
    if (Number.isNaN(t)) return true;
    return t >= cutoff;
  });

  // Dedup
  const deduped = dedupeItems(recent);

  // Score
  const scored = deduped.map((it) => scoreItem(it, { impactTerms, influenceIndex, now }));

  // Sort: score desc then newest first
  scored.sort((a, b) => {
    if (b.impact_score !== a.impact_score) return b.impact_score - a.impact_score;
    const ta = safeTime(a.published_at);
    const tb = safeTime(b.published_at);
    return tb - ta;
  });

  return {
    ok: true,
    generated_at: new Date().toISOString(),
    window_hours: windowHours,
    count: scored.length,
    items: scored,
  };
}

function scoreItem(item, { impactTerms, influenceIndex, now }) {
  const title = item.title || "";
  const source = item.source || item.feed_label || "";
  const publishedAt = item.published_at || null;

  const haystack = normalizeText(`${title} ${source}`);

  // --- Base scoring components ---
  let score = 0;
  const match_reasons = [];

  // 1) Source weight (authority proxy)
  // feed_weight defaults to 1; allow stronger sources to float
  const sourceWeight = numOr(item.feed_weight, 1);
  score += Math.round(40 * sourceWeight);
  match_reasons.push(`source_weight:${sourceWeight}`);

  // 2) Recency (stronger within the window)
  // If published is missing, give minimal recency points
  const recencyPoints = recencyScore(publishedAt, now);
  score += recencyPoints.points;
  match_reasons.push(recencyPoints.reason);

  // 3) Impact terms (keywords / phrases)
  const impact = impactScore(haystack, impactTerms);
  score += impact.points;
  if (impact.reasons.length) match_reasons.push(...impact.reasons);

  // 4) Influence boost (named people + aliases)
  const infl = influenceBoost(haystack, influenceIndex);
  score += infl.points;
  if (infl.reasons.length) match_reasons.push(...infl.reasons);

  // Clamp and round
  score = clampInt(score, 0, 999, 0);

  return {
    title,
    url: item.url,
    source: source || item.feed_label || "",
    published_at: publishedAt,
    origin: item.feed_origin || "unknown",
    impact_score: score,
    match_reasons,
  };
}

/* ---------------------------
   Influence logic
---------------------------- */

function buildInfluenceIndex(influenceJson) {
  const people = Array.isArray(influenceJson?.people) ? influenceJson.people : [];

  // We build:
  // - list of { name, boost, patterns[] }
  // - combined regex scanning is tricky with aliases; we keep per-person patterns
  const normalized = people
    .filter((p) => p && p.name)
    .map((p) => {
      const boost = clampInt(p.boost, 0, 300, 60); // default boost if not provided
      const aliases = Array.isArray(p.aliases) ? p.aliases : [];
      const needles = [p.name, ...aliases].filter(Boolean).map(String);

      // We match whole words loosely; for multi-word names we match phrase boundaries
      const patterns = needles.map((n) => new RegExp(boundaryRegex(n), "i"));

      return {
        name: String(p.name),
        role: p.role ? String(p.role) : "",
        boost,
        patterns,
      };
    });

  return { people: normalized };
}

function influenceBoost(haystack, influenceIndex) {
  let points = 0;
  const reasons = [];

  for (const p of influenceIndex.people) {
    let hit = false;
    for (const re of p.patterns) {
      if (re.test(haystack)) {
        hit = true;
        break;
      }
    }
    if (hit) {
      points += p.boost;
      reasons.push(`influence:${p.name}:${p.boost}`);
    }
  }

  // Keep influence from dominating everything
  points = clampInt(points, 0, 350, points);
  return { points, reasons };
}

/* ---------------------------
   Impact terms logic
---------------------------- */

function defaultImpactTerms() {
  // Fallback if file missing; keep minimal
  return {
    phrases: [
      { term: "voting rights", weight: 70 },
      { term: "police shooting", weight: 80 },
      { term: "naacp", weight: 40 },
      { term: "lawsuit", weight: 30 },
    ],
    words: [
      { term: "election", weight: 20 },
      { term: "strike", weight: 15 },
      { term: "protest", weight: 15 },
      { term: "union", weight: 10 },
    ],
  };
}

function impactScore(haystack, impactTerms) {
  const reasons = [];
  let points = 0;

  const phrases = Array.isArray(impactTerms?.phrases) ? impactTerms.phrases : [];
  const words = Array.isArray(impactTerms?.words) ? impactTerms.words : [];

  // phrases: substring match (case-insensitive via normalized haystack)
  for (const p of phrases) {
    if (!p?.term) continue;
    const term = normalizeText(String(p.term));
    if (!term) continue;
    if (haystack.includes(term)) {
      const w = clampInt(p.weight, 0, 300, 20);
      points += w;
      reasons.push(`impact_phrase:${term}:${w}`);
    }
  }

  // words: whole word regex
  for (const wobj of words) {
    if (!wobj?.term) continue;
    const term = String(wobj.term).trim();
    if (!term) continue;
    const re = new RegExp(`\\b${escapeRegExp(term)}\\b`, "i");
    if (re.test(haystack)) {
      const w = clampInt(wobj.weight, 0, 200, 10);
      points += w;
      reasons.push(`impact_word:${term.toLowerCase()}:${w}`);
    }
  }

  // Cap impact points so it doesn’t explode
  points = clampInt(points, 0, 450, points);
  return { points, reasons };
}

/* ---------------------------
   Recency
---------------------------- */

function recencyScore(publishedAt, nowMs) {
  if (!publishedAt) return { points: 5, reason: "recency:unknown:5" };

  const t = Date.parse(publishedAt);
  if (Number.isNaN(t)) return { points: 5, reason: "recency:unparseable:5" };

  const ageMin = Math.max(0, Math.floor((nowMs - t) / 60000));

  // Points curve: freshest gets more; fades quickly
  // 0–30 min: 120
  // 30–90 min: 80
  // 90–180 min: 40
  // older: 10
  let points = 10;
  if (ageMin <= 30) points = 120;
  else if (ageMin <= 90) points = 80;
  else if (ageMin <= 180) points = 40;

  return { points, reason: `recency:${ageMin}m:${points}` };
}

/* ---------------------------
   Feed fetching + parsing
---------------------------- */

async function fetchAndParseFeed(feedUrl) {
  const res = await fetch(feedUrl, {
    headers: {
      "User-Agent": "BlackSignalsWorker/1.0",
      "Accept": "application/rss+xml, application/xml, text/xml, */*",
    },
  });

  if (!res.ok) return [];

  const text = await res.text();
  // Quick sniff: RSS/Atom XML
  return parseXmlFeed(text);
}

function parseXmlFeed(xmlText) {
  // We don’t have DOMParser in Workers reliably in every mode; use simple regex parsing.
  // This is intentionally conservative: title/link/pubDate for RSS; title/link/updated for Atom.
  const items = [];

  const isAtom = /<feed[\s>]/i.test(xmlText) && /<entry[\s>]/i.test(xmlText);

  if (isAtom) {
    const entries = xmlText.match(/<entry[\s\S]*?<\/entry>/gi) || [];
    for (const e of entries) {
      const title = decodeHtml(extractTag(e, "title")) || "";
      const link =
        extractAttrFromTag(e, "link", "href") ||
        decodeHtml(extractTag(e, "link")) ||
        "";
      const updated = extractTag(e, "updated") || extractTag(e, "published") || "";
      const source = ""; // not always present
      if (title && link) {
        items.push({
          title: title.trim(),
          url: link.trim(),
          source,
          published_at: updated ? new Date(updated).toISOString() : null,
        });
      }
    }
    return items;
  }

  // RSS
  const rssItems = xmlText.match(/<item[\s\S]*?<\/item>/gi) || [];
  for (const it of rssItems) {
    const title = decodeHtml(extractTag(it, "title")) || "";
    const link = decodeHtml(extractTag(it, "link")) || "";
    const pubDate = extractTag(it, "pubDate") || extractTag(it, "dc:date") || "";
    const source = decodeHtml(extractTag(it, "source")) || "";

    if (title && link) {
      items.push({
        title: title.trim(),
        url: link.trim(),
        source: source ? stripTags(source).trim() : "",
        published_at: pubDate ? safeIso(pubDate) : null,
      });
    }
  }
  return items;
}

/* ---------------------------
   Dedupe + helpers
---------------------------- */

function dedupeItems(items) {
  const seen = new Set();
  const out = [];

  for (const it of items) {
    const key = (it.url || "").trim().toLowerCase() || normalizeText(it.title || "");
    if (!key) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(it);
  }
  return out;
}

function safeIso(dateStr) {
  const t = Date.parse(dateStr);
  if (Number.isNaN(t)) return null;
  return new Date(t).toISOString();
}

function safeTime(dateStr) {
  if (!dateStr) return 0;
  const t = Date.parse(dateStr);
  return Number.isNaN(t) ? 0 : t;
}

function normalizeText(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[\u201C\u201D]/g, '"')
    .replace(/\s+/g, " ")
    .trim();
}

function stripTags(s) {
  return String(s || "").replace(/<[^>]*>/g, "");
}

function guessHostname(u) {
  try {
    return new URL(u).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function boundaryRegex(nameOrAlias) {
  // Creates a regex string that tries to match a name as a phrase boundary.
  // Example: "Wes Moore" => \bwes\s+moore\b
  const n = normalizeText(nameOrAlias);
  const parts = n.split(" ").filter(Boolean).map(escapeRegExp);
  if (parts.length === 1) return `\\b${parts[0]}\\b`;
  return `\\b${parts.join("\\s+")}\\b`;
}

function numOr(v, fallback) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function clampInt(v, min, max, fallback) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(n)));
}

/* ---------------------------
   Fetch JSON helper
---------------------------- */

async function fetchJson(url, { fallback }) {
  try {
    const res = await fetch(url, {
      headers: { "Accept": "application/json" },
    });
    if (!res.ok) return fallback;
    return await res.json();
  } catch {
    return fallback;
  }
}

/* ---------------------------
   Concurrency helper
---------------------------- */

async function mapWithConcurrency(list, limit, fn) {
  const results = new Array(list.length);
  let i = 0;

  const workers = new Array(Math.min(limit, list.length)).fill(0).map(async () => {
    while (true) {
      const idx = i++;
      if (idx >= list.length) break;
      results[idx] = await fn(list[idx]);
    }
  });

  await Promise.all(workers);
  return results;
}

/* ---------------------------
   Response helpers + CORS
---------------------------- */

function json(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data, null, 2), {
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
  headers.set("Access-Control-Max-Age", "86400");
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

/* ---------------------------
   Minimal HTML entity decode
---------------------------- */

function decodeHtml(s) {
  if (!s) return "";
  // Decode the few that show up most in RSS
  return String(s)
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#8217;/g, "'")
    .replace(/&#8220;/g, '"')
    .replace(/&#8221;/g, '"')
    .replace(/&#8211;/g, "–")
    .replace(/&#8212;/g, "—");
}

function extractTag(block, tagName) {
  const re = new RegExp(`<${tagName}[^>]*>([\\s\\S]*?)<\\/${tagName}>`, "i");
  const m = block.match(re);
  if (!m) return "";
  return stripCdata(m[1]).trim();
}

function extractAttrFromTag(block, tagName, attrName) {
  const re = new RegExp(`<${tagName}[^>]*\\s${attrName}="([^"]+)"[^>]*\\/?>`, "i");
  const m = block.match(re);
  return m ? m[1] : "";
}

function stripCdata(s) {
  return String(s || "").replace(/^<!\[CDATA\[/i, "").replace(/\]\]>$/i, "");
}
