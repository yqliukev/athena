# Project Research

**Project:** Personal information pipeline — collect **current structured facts** (stock open price, today's weather, esports match times, etc.) from a wide variety of sites/APIs on a schedule or on demand, with a path to auto-onboard new sources and display everything in an information dashboard.
**Researched:** 2026-07-01 (updated 2026-06-29 — de-emphasized RSS-first patterns)
**Questions:**
- How can stocks, weather, esports schedules, and other disparate sources share one framework?
- What architecture fits manual + scheduled collection from heterogeneous websites?
- Build custom vs adopt/extend an existing self-hosted tool?
- How should new websites be added to the pipeline with minimal friction?
- What extraction stack handles APIs, static HTML, iCal, and JS-heavy pages?
- What storage and scheduling are appropriate at personal scale?
- What dashboard patterns fit a personal reference tool (not a news reader clone)?

## Executive summary

Athena is best scoped as a **layered ingestion pipeline**, not a pure scraper: prefer **APIs and direct connectors** (`api_json`, `html_selector`, `ical`) for the structured facts this project targets; use static HTML + heuristic extraction (`trafilatura`) when no API exists; escalate to headless browser or LLM extraction only for exceptions. **RSS is optional** — use it when a feed already exists or can be created with a low-code tool (e.g. RSS-Bridge), but most target sites will not expose feeds for the values you care about.

For v1, two viable paths emerge:

1. **Adopt + customize** — Start from a self-hosted aggregator like [jaypetez/glean](https://github.com/jaypetez/glean) (YAML sources, daily schedule, built-in dashboard) or [changedetection.io](https://github.com/dgtlmoon/changedetection.io) (change monitoring + history). Fastest time-to-value; less control over dashboard UX.
2. **Build lean custom** — Python ingestion worker + SQLite + a small Next.js dashboard. Use declarative YAML site configs (Scrapit-style) and a selector-based onboarding form for new sources. Best if the dashboard is the product and you want full control over onboarding UX.

Recommended direction: **build a thin custom core** (source registry, scheduler, normalized storage) but **reuse battle-tested extraction libraries** rather than writing per-site scrapers. Defer Celery/Temporal until you exceed ~50 sources or need per-source retry isolation.

**Heterogeneous data (stocks, weather, esports):** Do **not** unify extraction logic — unify the **output contract**. Each source declares a *kind* (scalar, forecast, schedule, article) and a *connector* (API, CSS selector, iCal, etc.). The pipeline always produces the same envelope (`DataPoint`: value + unit + timestamp + metadata). Prefer official/free **APIs** for stocks and weather; scrape only when no stable API exists.

## Findings

### Unified framework via adapter pattern + DataPoint envelope

**Summary:** Sites differ at fetch/extract time but converge on a small set of **value shapes**. Athena should treat each source as a config-driven **adapter** that implements one interface (`fetch → extract → normalize → DataPoint[]`) while allowing completely different internals per source.

**The three layers:**

```
┌─────────────────────────────────────────────────────────────┐
│  SOURCE CONFIG (per site)                                   │
│  kind: stock_quote | weather | schedule | text | custom     │
│  connector: api_json | html_selector | ical | rss | browser │
│  extract: { jsonpath / css / ical parser / schema }         │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  CONNECTOR RUNTIME (small set of reusable handlers)         │
│  ~6 handlers cover most sites; no per-site Python code      │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  NORMALIZED DataPoint (same for ALL sources)                │
│  { source_id, kind, label, value, unit, as_of, meta }       │
└─────────────────────────────────────────────────────────────┘
```

**Value kinds (not extraction methods):**

| Kind | Example output | Dashboard widget |
|------|----------------|------------------|
| `scalar` | AAPL open: $212.40 | Single stat card |
| `timeseries` | hourly temps, intraday prices | Sparkline / chart |
| `forecast` | high 82°F, rain 30% | Weather card |
| `schedule` | [{team_a, team_b, start_at, tournament}] | Calendar / list |
| `text` | patch notes, headline | Text block |
| `table` | standings rows | Sortable table |

**Connector types (how to get raw data):**

| Connector | When to use | Config example |
|-----------|-------------|----------------|
| `api_json` | Site has JSON API (or you use Alpha Vantage, Open-Meteo) | `url`, `jsonpath`, `headers` |
| `html_selector` | Static page with known DOM | `url`, `selector`, `attr`, `transform` |
| `html_table` | Standings, price tables | `url`, `table_selector`, `column_map` |
| `ical` | Esports/calendar `.ics` feeds | `url` → parse VEVENT |
| `rss` | **When a feed exists** — schedules/news as items | `url` → map item fields |
| `browser` | JS-rendered SPA (last resort) | Playwright + same selectors |

**Example configs (same framework, different sources):**

```yaml
# Stock — prefer API, not scraping Yahoo HTML
id: aapl-open
kind: scalar
connector: api_json
fetch:
  url: "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey=${AV_KEY}"
extract:
  path: "$.Global Quote.05. open"
transform: [{ op: float }]
normalize:
  label: "AAPL open"
  unit: USD
schedule: "0 9 * * 1-5"   # 9am weekdays

# Weather — Open-Meteo (free, no key)
id: nyc-today
kind: forecast
connector: api_json
fetch:
  url: "https://api.open-meteo.com/v1/forecast?latitude=40.71&longitude=-74.01&daily=temperature_2m_max,precipitation_probability_max&timezone=America/New_York"
extract:
  path: "$.daily"
  map: { high: "$.temperature_2m_max[0]", rain_pct: "$.precipitation_probability_max[0]" }
normalize:
  label: "NYC today"
  unit: imperial
schedule: "0 6 * * *"

# Esports — iCal if available, else HTML table
id: lol-schedule
kind: schedule
connector: ical
fetch:
  url: "https://example.com/schedule.ics"
extract:
  fields: [summary, dtstart, location, description]
schedule: "0 */4 * * *"
```

**Advantages:**
- Adding a site = adding config, not code (until you need a new connector type)
- Dashboard renders by `kind`, not by source implementation
- Stocks/weather via API are more stable than CSS scraping finance portals
- Manual "run now" and daily cron use identical adapter path

**Disadvantages:**
- Initial connector library must be designed deliberately (~6 types)
- Some sites need bespoke handling → falls back to `custom` connector or LLM schema extract
- API keys and rate limits become operational concerns per category

**Links:**
- [Scrapit YAML directive model](http://scrapit.space/docs.html)
- [Open-Meteo API](https://open-meteo.com/)
- [Alpha Vantage](https://www.alphavantage.co/)

**Relevance:** This is the core answer to "how can drastically different sites share one framework." **Unify the envelope and connector interface; specialize only in small, reusable connector handlers and category templates.**

---

### Prefer APIs over scraping for structured facts (stocks, weather)

**Summary:** For scalar/time-series facts (prices, temperature, indices), public APIs are more reliable than HTML scraping. Reserve scraping for sources with no API (niche esports sites, proprietary dashboards).

**Advantages:**
- Stable JSON schemas vs brittle CSS selectors on finance/weather UIs
- Built-in units and timestamps; less parsing logic
- Legal/ToS often clearer for API use than scraping

**Disadvantages:**
- API keys, rate limits, and free-tier caps (e.g. Alpha Vantage 25 req/day on free tier)
- Not every niche site has an API (obscure esports leagues)
- Vendor dependency unless you cache locally

**Links:**
- [Alpha Vantage](https://www.alphavantage.co/)
- [Open-Meteo](https://open-meteo.com/)
- [esport.is API](https://esport.is/api-docs)
- [FMP — why APIs beat scraping for stock history](https://site.financialmodelingprep.com/how-to/how-to-pull-clean-historical-stock-price-data-using-a-free-api)

**Relevance:** Athena source registry should record `origin: api | scrape` per source. UI "add stock" wizard should default to ticker → API lookup, not URL paste. "Add weather" → lat/lon → Open-Meteo. "Add esports" → try iCal/API first, HTML table scrape second; RSS only if a feed is already available.

---

### Category templates for onboarding (future automation)

**Summary:** When auto-adding websites, don't infer everything from HTML — classify the user's intent first ("track a stock", "track weather", "track match schedule"), then apply a **template** that pre-fills connector + extract + normalize + widget type.

**Templates:**

| User intent | Template fills | User provides |
|-------------|----------------|---------------|
| Stock price | `api_json` + Alpha Vantage paths | Ticker symbol, which field (open/close/price) |
| Weather | `api_json` + Open-Meteo | Location or lat/lon |
| Esports schedule | `ical` → `api` → `html_table` fallback chain | League URL or team name |
| Generic page value | `html_selector` | URL + element picker (manual) or LLM suggestion |
| Page change | changedetection-style diff | URL + optional selector |

**Advantages:**
- "Wide variety" becomes ~5 onboarding flows, not infinite one-offs
- Automation (phase 4) only needs to solve selector/URL discovery within a template
- Dashboard knows which widget to render without per-source UI code

**Disadvantages:**
- Templates don't cover every edge case; need escape hatch (`custom` + LLM schema)
- Maintaining API template mappings when providers change response shapes

**Relevance:** Directly answers future "automate adding new websites" — automate **within a category**, not across all possible web pages at once.

---

### Per-source fetch strategy: API and direct extraction first

**Summary:** For structured facts (prices, weather, schedules), each source declares how to reach the data directly — API, iCal, or a known HTML selector — not a discovery layer that assumes RSS or sitemaps. Normalize all outputs into `DataPoint` regardless of fetch method. RSS and sitemaps are valid **only when already available**; they are not the default path for most target sites.

**Advantages:**
- Matches how stocks, weather, and esports data actually appear on the web
- Fewer moving parts than RSS-first pipelines that fail when no feed exists
- Clean separation: connector runtime vs normalize/store vs dashboard

**Disadvantages:**
- Per-source config required when no API exists
- HTML selectors break on redesigns; APIs have rate limits
- JS-heavy pages still need browser escalation

**Links:**
- [Olostep — fallback scraping architecture](https://www.olostep.com/blog/how-to-scrape-news-articles)

**Relevance:** Core architectural pattern for Athena. Each source in the registry should declare `connector: api_json | html_selector | ical | rss | browser` based on what the site actually offers. Manual runs use the same pipeline as scheduled runs.

---

### changedetection.io — change monitoring with history

**Summary:** Mature (~32k GitHub stars) self-hosted tool that watches pages for changes, supports CSS/XPath/JSONPath selectors, Playwright for JS pages, and 85+ notification channels.

**Advantages:**
- Excellent for "tell me when this page changes" use cases (pricing, docs, announcements)
- Built-in visual selector UI lowers friction for adding sites manually
- REST API, import/export, Chrome extension for quick URL adds
- Historical diffs — useful for audit-style dashboards

**Disadvantages:**
- Optimized for *change detection*, not rich structured extraction or cross-site analytics
- Dashboard is monitoring-centric, not a customizable personal knowledge dashboard
- JS-heavy pages need Playwright setup; docs can be uneven
- Harder to plug into a custom "information dashboard" without treating it as a data source via API/webhook

**Links:**
- [changedetection.io GitHub](https://github.com/dgtlmoon/changedetection.io)

**Relevance:** Strong fit as a **phase-1 shortcut** if your primary need is "alert me when X changes" across a handful of URLs. Weaker fit as the long-term dashboard foundation unless you only need diff views. Could run alongside a custom Athena core and push webhooks into your store.

---

### jaypetez/glean — pluggable personal signal aggregator

**Summary:** Self-hosted Python daemon: YAML-configured sources (scrape, APIs, search, and RSS where available) → optional LLM processing → scheduled delivery to sinks including a built-in SQLite-backed dashboard. Plugin architecture for sources, sinks, LLM providers, and search backends.

**Advantages:**
- Closest OSS match to Athena's stated vision (multi-source ingest + schedule + dashboard)
- Declarative YAML feeds; web UI with feed editor, run-now, SSE live status
- Plugin system aligns with future "add new website" automation
- Ships sinks for Discord, Slack, webhooks, file export — easy to fork data out

**Disadvantages:**
- Small community (~4 stars); less battle-tested than changedetection.io or FreshRSS
- Dashboard UX is digest-oriented, not a free-form information dashboard
- Scraping source is less documented; may still need per-site tuning
- Name collision with [LeslieLeung/glean](https://github.com/LeslieLeung/glean/) (different project — PKM tool)

**Links:**
- [jaypetez/glean GitHub](https://github.com/jaypetez/glean)

**Relevance:** Worth **spiking locally in Docker** before building from scratch. If the plugin model and dashboard are "good enough," fork/extend rather than greenfield. If not, borrow its YAML source schema and plugin boundaries for Athena's design.

---

### Optional: low-code feed generation (RSS-Bridge)

**Summary:** For the minority of sources where an RSS-style item stream is useful and no native feed exists, [RSS-Bridge](https://github.com/RSS-Bridge/rss-bridge) can generate Atom/RSS from CSS selectors via a web form (no code). Athena's primary onboarding path should still be **direct connectors** (`html_selector`, `api_json`); treat generated feeds as a convenience layer, not the core architecture.

**When it helps:** News-style pages where you want a list of new items and a feed is easier to poll than re-scraping a listing page.

**When to skip:** Structured facts (stock quote, weather, match time) — configure the connector directly; wrapping in RSS adds indirection without benefit.

**Links:**
- [RSS-Bridge GitHub](https://github.com/RSS-Bridge/rss-bridge)
- [CssSelectorBridge walkthrough](https://chrislongros.com/2026/02/09/9861/)

**Relevance:** Optional manual tool in v2+. Borrow the **visual selector picker** idea (also seen in changedetection.io) for Athena's own "add source" form rather than requiring a separate PHP service.

---

### Extraction stack: trafilatura + Playwright + optional LLM

**Summary:** Use heuristic main-content extraction for ~90% of article pages; escalate to Playwright only when static fetch returns empty content; use LLM structured extraction only for enrichment (summary, tags) or irregular layouts.

**Advantages:**
- `trafilatura` / Readability avoid brittle per-site CSS for article bodies
- Playwright fallback handles SPAs without browser-first cost on every run
- LLM on *new items only* keeps daily costs negligible
- Crawl4AI or Firecrawl available if you want LLM-ready markdown or schema extraction later

**Disadvantages:**
- Heuristic extractors fail on non-article pages (dashboards, tables, pricing grids)
- Playwright adds ~300MB+ dependencies and operational overhead
- LLM extraction introduces non-determinism; needs schema validation

**Links:**
- [Crawl4AI GitHub](https://github.com/unclecode/crawl4ai)
- [Firecrawl self-host docs](https://github.com/firecrawl/firecrawl/blob/main/SELF_HOST.md)
- [Roundproxies — async scraper + Mistral pipeline pattern](https://roundproxies.com/blog/web-scraping-mistral-ai/)

**Relevance:** Default extraction chain for Athena custom build. Per-source config should specify `fetch: httpx | playwright` and `extract: trafilatura | css | llm_schema`. Start with httpx + trafilatura only.

---

### Declarative YAML site configs (Scrapit pattern)

**Summary:** Define each scrape target as a YAML "directive": URL(s), backend (BeautifulSoup/Playwright), CSS selectors with fallbacks, transforms, validation, pagination, and change-detection webhooks.

**Advantages:**
- Adding a site = adding a file, not changing application code
- Fallback selectors and `all: true` reduce breakage
- Built-in change detection and SQLite output in Scrapit
- Natural stepping stone to UI-generated configs later

**Disadvantages:**
- CSS/XPath configs still break on redesigns
- YAML can get complex for multi-step flows (login, pagination, API)
- No built-in dashboard — output is files/DB/webhooks

**Links:**
- [Scrapit docs](http://scrapit.space/docs.html)
- [Scrapit GitHub](https://github.com/joaobenedetmachado/scrapit)

**Relevance:** Model for Athena's **source registry schema**. Even with a DB-backed registry, store extraction rules as structured JSON/YAML per source. Future "add website" UI writes to this schema; version configs so selector changes are auditable.

---

### Storage: SQLite first, Postgres when shared

**Summary:** For a personal single-machine pipeline, SQLite is the sweet spot — one file, SQL queries, zero ops. Store raw fetch metadata + normalized items + content snapshots (or hashes). Graduate to Postgres when multiple workers write concurrently or the dashboard is multi-user.

**Advantages:**
- Zero infrastructure for v1; portable backup (copy one file)
- Sufficient for hundreds of sources × daily runs × years of headlines
- JSON columns (or JSONL raw archive) for unstable page structure
- Same SQL skills transfer to Postgres later

**Disadvantages:**
- Concurrent writes from parallel workers are limited (single writer)
- No built-in full-text search at scale (FTS5 helps; Meilisearch optional later)
- Large HTML snapshots bloat the DB — store extracted text + optional raw HTML path

**Links:**
- [Store scraped data — SQLite vs Postgres](https://webscraper.live/how-to-store-scraped-data)
- [$12/month scraping warehouse pattern (SQLite → DuckDB)](https://dev.to/vhub_systems_ed5641f65d59/the-web-scraping-data-warehouse-pipeline-i-run-for-12month-4b3e)

**Relevance:** Start with SQLite tables: `sources`, `fetch_runs`, `items` (url, title, content, content_hash, fetched_at, source_id), `snapshots` (optional full-page for diff). Add DuckDB or Postgres only if you need analytics across millions of rows.

---

### Scheduling: cron / APScheduler for v1

**Summary:** Daily personal collection does not need Celery or Temporal on day one. Use system cron, Docker cron, or APScheduler inside the worker process. Add a task queue when per-source retries, concurrency limits, or failure isolation become painful (~50+ sources).

**Advantages:**
- Trivial to debug: one schedule triggers one job
- Manual "run now" reuses the same entrypoint
- APScheduler supports per-source cron expressions in code without Redis/RabbitMQ

**Disadvantages:**
- No automatic per-task retry isolation (one failing source can block a batch if not handled)
- Harder to scale horizontally without redesign
- Long-running Playwright jobs may need timeout/watchdog logic in-process

**Links:**
- [Cron vs queues for scheduled scraping](https://webscraper.app/scheduled-web-scraping-cron-jobs-queues-and-when-to-use-each)
- [Scraping Central — scheduling guide](https://scrapingcentral.com/learn/production-scale/scheduling)

**Relevance:** Athena v1: single Python worker, `0 6 * * *` daily run + CLI/API for manual trigger. Design fetch loop so each source is isolated (try/except per source, structured logs). Revisit Celery Beat when source count or reliability requirements grow.

---

### Dashboard: custom information board, not a feed reader

**Summary:** Athena's dashboard should render **DataPoints by kind** (scalar cards, forecast, schedule lists, charts) — not mimic a news/RSS reader. Existing widget dashboards (e.g. Hati) are useful references for layout patterns, but the product is a personal reference board for structured facts.

**Advantages (custom build):**
- Tailored widgets: live stats, schedules, "failed fetches," category filters
- Unified view across API results, scrape extractions, and change-detection diffs
- "Add source" wizard as a first-class UI flow

**Advantages (borrow patterns from):**
- Hati ([prathamdupare/hati](https://github.com/prathamdupare/hati)) — YAML-configured Next.js widget dashboard (weather, HN, etc.)
- Metabase + DuckDB — zero-code charts if analytics matter more than live widgets

**Disadvantages:**
- Custom dashboard is the largest build surface area
- Feed-reader UIs are a poor fit for scalar/time-series data without heavy adaptation

**Links:**
- [Hati GitHub](https://github.com/prathamdupare/hati)

**Relevance:** Phase 1 dashboard MVP: source health table + widgets keyed by `DataPoint.kind` + search. Phase 2: configurable layout. Borrow Hati's YAML page layout idea without adopting a feed-reader model.

---

### Compliance: robots.txt, rate limits, personal use

**Summary:** For a personal reference tool scraping public pages you would visit manually, risk is low if (superseded) good practice still applies: honor robots.txt, rate-limit (1–5 s between requests), identify your bot via User-Agent, avoid authenticated/login walls, minimize PII collection, and snapshot ToS/robots.txt when adding a source.

**Advantages:**
- Reduces risk of IP blocks and cease-and-desist
- Per-source rate limits in config prevent accidental hammering
- Documented compliance helps if you later share the tool

**Disadvantages:**
- Some useful pages may be disallowed in robots.txt
- Legal landscape varies by jurisdiction; not legal advice
- GDPR applies if you store personal data about EU residents

**Links:**
- [Web scraping legal guide (2026)](https://use-apify.com/blog/web-scraping-legal-guide)
- [robots.txt guide for scrapers](https://www.lection.app/blogs/complete-guide-to-robots-txt-for-web-scrapers)

**Relevance:** Bake into Athena's fetch layer: robots.txt check before first fetch, configurable delay per domain, store `robots_checked_at` on each source. Flag sources that require login as out-of-scope for v1.

---

### Suggested phased scope for Athena

**Summary:** A pragmatic build sequence that matches "manual or daily now, automate onboarding later, dashboard eventually."

| Phase | Scope | Outcome |
|-------|--------|---------|
| **0 — Spike** | Run changedetection.io or jaypetez/glean locally with 3–5 real URLs | Validate whether adopt > build |
| **1 — Core pipeline** | Source registry (YAML/DB), `api_json` + `html_selector` + `ical` connectors, SQLite, daily cron + manual CLI | Data accumulating daily |
| **2 — Manual onboarding** | "Paste URL + pick element" form or YAML; store connector config | Add sites without code changes |
| **3 — Dashboard MVP** | Web UI: sources list, run history, widgets by `DataPoint.kind`, search | Your "information dashboard" v1 |
| **4 — Smart onboarding** | LLM selector suggestion → human confirm; optional RSS-Bridge for feed-style sources | Faster pipeline expansion |
| **5 — Scale** | Per-source Playwright fallback, Celery queue, Postgres, notifications | 50+ sources, production reliability |

**Advantages:**
- Each phase delivers usable value
- Avoids over-building orchestration before source count justifies it
- Manual path (phase 2) unblocks you before automation (phase 4)

**Disadvantages:**
- Phase 0 may tempt "good enough" adoption and stall custom dashboard
- Rework possible if spike choice differs from long-term vision

**Links:**
- (Synthesis of findings above)

**Relevance:** Use this as the backbone for a follow-up `spec.md` / project plan when you are ready to implement.

## Recommendations

1. **Spike before committing to a greenfield build.** Docker-run [changedetection.io](https://github.com/dgtlmoon/changedetection.io) (change-focused) and [jaypetez/glean](https://github.com/jaypetez/glean) (digest + dashboard) with 5 URLs you actually care about. If one meets 80% of needs, extend it; otherwise proceed with custom Athena.

2. **If building custom, start with phase 1 only:** Python worker + SQLite + declarative source configs supporting `api_json`, `html_selector`, and `ical`, daily schedule + `athena fetch --source X` manual command. Add `rss` connector only when a feed URL is already known.

3. **Design the source registry for future automation now.** Fields: `url`, `connector`, `schedule`, `extract`, `normalize`, `tags`, `enabled`, `last_status`. The "add website" UI in phase 4 should only write rows — not require pipeline rewrites.

## Open questions

- **Which categories first?** Stocks + weather are API-heavy and fast to ship; esports may need more scraping/ical work — pick 2 categories for v1.
- **Historical depth?** "Current value only" (overwrite) vs time-series history (chart AAPL over 30 days) — affects storage schema.
- **Change detection vs snapshot?** Is "price changed since last run" an alert, or always show latest value?
- **Manual workflow preference?** Browser extension, web form, YAML files, or CLI when adding a site?
- **Hosting target?** Local Mac only, always-on home server, or cloud VPS — affects scheduling and Playwright deployment.
- **LLM enrichment?** Optional summaries/tags add value but require API keys and cost budgeting.
- **Multi-user?** Personal tool only, or might others use the dashboard later ( affects auth + Postgres choice)?
- **Adopt vs build threshold?** After the spike, what would "good enough" mean — digest email, diff alerts, or a specific dashboard layout?