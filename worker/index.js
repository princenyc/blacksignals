/**
 * BlackSignals Stream Worker (MVP)
 * - Fetches Kagi RSS feeds + curated Black US RSS feeds
 * - Scores items via impact terms + influence (named people boosts) + recency + source weight
 * - Keeps rolling last N hours only
 * - Dedupes & returns a single sorted list (headline + source + score)
 * - No storage. Uses short caching at the edge.
 *
 * NEW in this version:
 * - Robust feed JSON loading (supports multiple shapes, not just {feeds:[]})
 * - Proxy fallback for blocked feeds (default Jina relay, optional env.PROXY_BASE)
 * - Debug endpoints:
 *    /api/debug/config
 *    /api/debug/fetch?url=...
 */

const FEEDS_BASE =
  "https://raw.githubusercontent.com/princenyc/blacksignals/main/feeds";

const KAGI_INPUTS_PATH = "kagi_inputs.json";
const CURATED_FEEDS_PATH = "curated_black_us.json";
const IMPACT_TERMS_PATH = "impact_terms.json";
const INFLUENCE_PATH = "person_influence.json";

const DEFAULT_CACHE_SECONDS = 900; // 15 min
const DEFAULT_WINDOW_HOURS = 3;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === "OPTIONS") return handleOptions();

    // Health
    if (url.pathname === "/" || url.pathname === "/api/health") {
      return json(
        withCors({
          ok: true,
          service: "blacksignals-stream-worker",
          now: new Date().toISOString(),
        })
      );
    }

    // Debug: show loaded config + derived feeds
    if (url.pathname === "/api/debug/config") {
      const [kagiInputs, curatedFeeds, impactTerms, influence] =
        await Promise.all([
          fetchJson(`${FEEDS_BASE}/${KAGI_INPUTS_PATH}`, { fallback: null }),
          fetchJson(`${FEEDS_BASE}/${CURATED_FEEDS_PATH}`, { fallback: null }),
          fetchJson(`${FEEDS_BASE}/${IMPACT_TERMS_PATH}`, { fallback: null }),
          fetchJson(`${FEEDS_BASE}/${INFLUENCE_PATH}`, { fallback: null }),
        ]);

      const kagiList = extractFeedList(kagiInputs);
      const curatedList = extractFeedList(curatedFeeds);

      const feeds = normalizeFeeds([...kagiList], "kagi").concat(
        normalizeFeeds([...curatedList], "curated")
      );

      return json(
        withCors({
          ok: true,
          paths: {
            kagi: `${FEEDS_BASE}/${KAGI_INPUTS_PATH}`,
            curated: `${FEEDS_BASE}/${CURATED_FEEDS_PATH}`,
            impact: `${FEEDS_BASE}/${IMPACT_TERMS_PATH}`,
            influence: `${FEEDS_BASE}/${INFLUENCE_PATH}`,
          },
          counts: {
            kagi_raw: Array.isArray(kagiList) ? kagiList.length : 0,
            curated_raw: Array.isArray(curatedList) ? curatedList.length : 0,
            feeds_total: feeds.length,
            impact_phrases: Array.isArray(impactTerms?.phrases)
              ? impactTerms.phrases.length
              : 0,
            impact_words: Array.isArray(impactTerms?.words)
              ? impactTerms.words.length
              : 0,
            influence_people: Array.isArray(influence?.people)
              ? influence.people.length
              : 0,
          },
          sample_feeds: feeds.slice(0, 25),
          notes:
            "If feeds_total is 0, your JSON shape doesn’t match what the worker expects. If feeds_total > 0 but stream is empty, most feeds may be blocked and need proxy fallback (this worker includes it).",
        })
      );
    }

    // Debug: test one feed fetch + parse
    if (url.pathname === "/api/debug/fetch") {
      const testUrl = url.searchParams.get("url");
      if (!testUrl) {
        return json(withCors({ ok: false, error: "Missing ?url=" }), 400);
      }
      const out = await debugFetchOne(testUrl, env);
      return json(withCors(out), 200);
    }

    // Stream
    if (url.pathname === "/api/stream") {
      // /api/stream?window=6&cache=60
      const windowHours = clampInt(
        url.searchParams.get("window"),
        1,
        48,
        DEFAULT_WINDOW_HOURS
      );
      const cacheSeconds = clampInt(
        url.searchParams.get("cache"),
        0,
        86400,
        DEFAULT_CACHE_SECONDS
      );

      // Cache key includes params that affect output
      const cacheKey = new Request(
        `${url.origin}${url.pathname}?window=${windowHours}&cache=${cacheSeconds}`,
        request
      );

      const cached = await caches.default.match(cacheKey);
      if (cached) return withCorsResponse(cached);

      const started = Date.now();
      const result = await buildStream({ windowHours }, env);

      const res = json(
        withCors(result),
        200,
        {
          "Cache-Control": `public, max-age=${cacheSeconds}`,
          "X-Worker-Time-MS": String(Date.now() - started),
        }
      );

      ctx.waitUntil(caches.default.put(cacheKey, res.clone()));
      return withCorsResponse(res);
    }

    return withCorsResponse(json(withCors({ ok: false, error: "Not found" }), 404));
  },
};

/* ---------------------------
   Core pipeline
---------------------------- */

async function buildStream({ windowHours }, env) {
  const [kagiInputs, curatedFeeds, impactTermsRaw, influenceRaw] =
    await Promise.all([
      fetchJson(`${FEEDS_BASE}/${KAGI_INPUTS_PATH}`, { fallback: null }),
      fetchJson(`${FEEDS_BASE}/${CURATED_FEEDS_PATH}`, { fallback: null }),
      fetchJson(`${FEEDS_BASE}/${IMPACT_TERMS_PATH}`, { fallback: defaultImpactTerms() }),
      fetchJson(`${FEEDS_BASE}/${INFLUENCE_PATH}`, { fallback: { people: [] } }),
    ]);

  // Extract feeds from whatever shape the JSON is in
  const kagiList = extractFeedList(kagiInputs);
  const curatedList = extractFeedList(curatedFeeds);

  const feeds = normalizeFeeds(kagiList, "kagi").concat(
    normalizeFeeds(curatedList, "curated")
  );

  // If you get 0 feeds, return early with a clear reason
  if (!feeds.length) {
    return {
      ok: true,
      generated_at: new Date().toISOString(),
      window_hours: windowHours,
      count: 0,
      items: [],
      debug: {
        reason: "No feeds loaded. Check /api/debug/config to confirm JSON shape.",
      },
    };
  }

  const impactTerms = normalizeImpactTerms(impactTermsRaw);
  const influenceIndex = buildInfluenceIndex(influenceRaw);

  const now = Date.now();
  const windowMs = windowHours * 60 * 60 * 1000;
  const cutoff = now - windowMs;

  // Fetch & parse feeds in parallel (bounded)
  const items = await mapWithConcurrency(feeds, 8, async (feed) => {
    const parsed = await fetchAndParseFeed(feed.url, env, feed);
    return parsed.map((it) => ({
      ...it,
      feed_label: feed.label,
      feed_origin: feed.origin,
      feed_weight: feed.weight,
    }));
  }).then((arrays) => arrays.flat());

  // Filter to window; if published missing, keep but weak recency
  const recent = items.filter((it) => {
    if (!it.published_at) return true;
    const t = Date.parse(it.published_at);
    if (Number.isNaN(t)) return true;
    return t >= cutoff;
  });

  // Dedup
  const deduped = dedupeItems(recent);

  // Score
  const scored = deduped.map((it) =>
    scoreItem(it, { impactTerms, influenceIndex, now })
  );

  // Sort: score desc then newest first
  scored.sort((a, b) => {
    if (b.impact_score !== a.impact_score) return b.impact_score - a.impact_score;
    return safeTime(b.published_at) - safeTime(a.published_at);
  });

  return {
    ok: true,
    generated_at: new Date().toISOString(),
    window_hours: windowHours,
    count: scored.length,
    items: scored,
  };
}

function normalizeFeeds(list, origin) {
  const arr = Array.isArray(list) ? list : [];
  const out = [];

  for (const f of arr) {
    // Support both {url,label,weight} and {feedUrl,source,major,blackOwned,...}
    const url = f?.url || f?.feedUrl;
    if (!url) continue;

    out.push({
      url,
      label: f.label || f.source || guessHostname(url),
      weight: numOr(f.weight, 1),
      origin,
    });
  }

  return out;
}

function normalizeImpactTerms(impactTerms) {
  return {
    phrases: Array.isArray(impactTerms?.phrases) ? impactTerms.phrases : [],
    words: Array.isArray(impactTerms?.words) ? impactTerms.words : [],
  };
}

function scoreItem(item, { impactTerms, influenceIndex, now }) {
  const title = item.title || "";
  const source = item.source || item.feed_label || "";
  const publishedAt = item.published_at || null;

  const haystack = normalizeText(`${title} ${source}`);

  let score = 0;
  const match_reasons = [];

  // 1) Source weight
  const sourceWeight = numOr(item.feed_weight, 1);
  score += Math.round(40 * sourceWeight);
  match_reasons.push(`source_weight:${sourceWeight}`);

  // 2) Recency
  const recencyPoints = recencyScore(publishedAt, now);
  score += recencyPoints.points;
  match_reasons.push(recencyPoints.reason);

  // 3) Impact terms
  const impact = impactScore(haystack, impactTerms);
  score += impact.points;
  if (impact.reasons.length) match_reasons.push(...impact.reasons);

  // 4) Influence boost (highest single boost per article)
  const infl = influenceBoost(haystack, influenceIndex);
  score += infl.points;
  if (infl.reasons.length) match_reasons.push(...infl.reasons);

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
   - Uses your feeds/person_influence.json
   - Highest single boost per article (no stacking)
---------------------------- */

function buildInfluenceIndex(influenceJson) {
  const people = Array.isArray(influenceJson?.people) ? influenceJson.people : [];

  const normalized = people
    .filter((p) => p && p.name)
    .map((p) => {
      const boost = clampInt(p.boost, 0, 300, 60);
      const aliases = Array.isArray(p.aliases) ? p.aliases : [];
      const needles = [p.name, ...aliases].filter(Boolean).map(String);
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
  let best = { points: 0, reason: "" };

  for (const p of influenceIndex.people) {
    let hit = false;
    for (const re of p.patterns) {
      if (re.test(haystack)) {
        hit = true;
        break;
      }
    }
    if (hit && p.boost > best.points) {
      best = { points: p.boost, reason: `influence:${p.name}:${p.boost}` };
    }
  }

  return best.points
    ? { points: clampInt(best.points, 0, 350, best.points), reasons: [best.reason] }
    : { points: 0, reasons: [] };
}

/* ---------------------------
   Impact terms logic
---------------------------- */

function defaultImpactTerms() {
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

  let points = 10;
  if (ageMin <= 30) points = 120;
  else if (ageMin <= 90) points = 80;
  else if (ageMin <= 180) points = 40;

  return { points, reason: `recency:${ageMin}m:${points}` };
}

/* ---------------------------
   Feed fetching + parsing (with proxy fallback)
---------------------------- */

async function fetchAndParseFeed(feedUrl, env, feedMeta) {
  const ua = "BlackSignalsWorker/2.0 (+https://blacksignals.org)";
  const headers = {
    "User-Agent": ua,
    Accept: "application/rss+xml, application/xml, text/xml, */*",
  };

  // 1) Direct
  const direct = await fetchWithMeta(feedUrl, { headers });
  if (looksLikeXmlFeed(direct)) {
    return parseXmlFeed(cleanToXML(direct.text));
  }

  // 2) Proxy fallback (default ON)
  const allowProxy = feedMeta?.allowProxy !== false;
  if (!allowProxy) return [];

  const proxyUrl = buildProxyUrl(feedUrl, env);
  const proxied = await fetchWithMeta(proxyUrl, {
    headers: { ...headers, Referer: "https://blacksignals.org" },
  });

  if (looksLikeXmlFeed(proxied, true)) {
    return parseXmlFeed(cleanToXML(proxied.text));
  }

  return [];
}

async function debugFetchOne(feedUrl, env) {
  const ua = "BlackSignalsWorker/2.0 (+https://blacksignals.org)";
  const headers = {
    "User-Agent": ua,
    Accept: "application/rss+xml, application/xml, text/xml, */*",
  };

  const direct = await fetchWithMeta(feedUrl, { headers });
  const proxyUrl = buildProxyUrl(feedUrl, env);
  const proxied = await fetchWithMeta(proxyUrl, { headers });

  const directLooks = looksLikeXmlFeed(direct);
  const proxyLooks = looksLikeXmlFeed(proxied, true);

  const chosen = directLooks ? direct : (proxyLooks ? proxied : direct);
  const parsed = directLooks || proxyLooks ? parseXmlFeed(cleanToXML(chosen.text)) : [];

  return {
    ok: true,
    feedUrl,
    proxyUrlUsed: proxyUrl,
    direct: {
      status: direct.status,
      ok: direct.ok,
      contentType: direct.contentType,
      first200: direct.first200,
      looksLikeFeed: directLooks,
    },
    proxy: {
      status: proxied.status,
      ok: proxied.ok,
      contentType: proxied.contentType,
      first200: proxied.first200,
      looksLikeFeed: proxyLooks,
    },
    parsedCount: parsed.length,
    sampleTitles: parsed.slice(0, 10).map((x) => x.title),
  };
}

async function fetchWithMeta(url, options) {
  try {
    const res = await fetch(url, options);
    const contentType = res.headers.get("content-type") || "";
    const text = await res.text();
    return {
      status: res.status,
      ok: res.ok,
      contentType,
      text,
      first200: text.slice(0, 200),
    };
  } catch (e) {
    return {
      status: 0,
      ok: false,
      contentType: "",
      text: "",
      first200: `fetch_error:${String(e && e.message ? e.message : e)}`.slice(0, 200),
    };
  }
}

function looksLikeXmlFeed(meta) {
  if (!meta || !meta.ok) return false;

  const ct = (meta.contentType || "").toLowerCase();
  const t = (meta.text || "").toLowerCase();

  // obvious blocks/html
  if (ct.includes("text/html")) return false;
  if (t.includes("captcha") || t.includes("sgcaptcha")) return false;

  // xml/feeds
  if (ct.includes("xml") || ct.includes("rss") || ct.includes("atom")) return true;
  return t.includes("<rss") || t.includes("<feed") || t.includes("<channel") || t.includes("<item");
}

function cleanToXML(s) {
  const idx = (s || "").indexOf("<");
  return idx >= 0 ? s.slice(idx) : s;
}

function buildProxyUrl(feedUrl, env) {
  if (env && env.PROXY_BASE) {
    return `${env.PROXY_BASE}${encodeURIComponent(feedUrl)}`;
  }
  // Default Jina relay:
  return `https://r.jina.ai/${feedUrl}`;
}

function parseXmlFeed(xmlText) {
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
      if (title && link) {
        items.push({
          title: title.trim(),
          url: link.trim(),
          source: "",
          published_at: updated ? safeIso(updated) : null,
        });
      }
    }
    return items;
  }

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
   Feed list extraction (robust)
---------------------------- */

function extractFeedList(jsonObj) {
  if (!jsonObj) return [];

  // Already an array? assume array of feeds
  if (Array.isArray(jsonObj)) return jsonObj;

  // Common keys
  const candidateKeys = ["feeds", "sources", "items", "list", "input", "inputs"];
  for (const k of candidateKeys) {
    if (Array.isArray(jsonObj[k])) return jsonObj[k];
  }

  // If it's an object of objects: { "Capital B": {url:...}, ... }
  // return values that look like feeds
  const vals = Object.values(jsonObj);
  const looks = vals.filter((v) => v && (v.url || v.feedUrl));
  if (looks.length) return looks;

  return [];
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
    const res = await fetch(url, { headers: { Accept: "application/json" } });
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

function withCors(obj) {
  return obj;
}

function withCorsResponse(res) {
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
  const re = new RegExp(
    `<${tagName}[^>]*\\s${attrName}="([^"]+)"[^>]*\\/?>`,
    "i"
  );
  const m = block.match(re);
  return m ? m[1] : "";
}

function stripCdata(s) {
  return String(s || "").replace(/^<!\[CDATA\[/i, "").replace(/\]\]>$/i, "");
}
