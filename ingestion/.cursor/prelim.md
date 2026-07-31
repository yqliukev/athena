# Ingestion components

Build order and responsibilities for the Athena ingestion layer. Aligns with the ingestion contract: connectors decode transport; source mappings parse provider responses; consumers only see normalized results.

**Start with the result contract and storage, not the connector or scheduler.** Those pieces are the integration boundary. Everything else exists to produce and persist normalized records.

## Runtime path

```
connector → source mapping → data contract → result store
```

| Step | Component | Role |
|------|-----------|------|
| 1 | Connector | Fetch and decode a transport class (HTTP JSON, iCal, RSS, HTML). Produces raw records; knows no Athena semantics. |
| 2 | Source mapping | Select record root, extract fields, apply transforms, compute identity. **This is where a new API response is parsed.** |
| 3 | Data contract | Validate normalized fields against a versioned semantic schema (`scalar`, `schedule_event`, `task_item`, …). |
| 4 | Result store | Upsert the stable envelope, retain provenance, advance a result revision for consumers. |

Parsing ownership: connector decodes JSON syntax; mapping interprets the provider shape; contract validation guarantees the downstream shape.

## Build order

### 1. Result contract and storage (start here)

The shared boundary for agent and dashboard. Without this, fetches have nowhere stable to land.

**Deliverables**

- `ResultRecord` envelope
- SQLite schema and upsert helpers
- At least one semantic schema (e.g. `scalar` or `schedule_event`) with validation

**`ResultRecord` sections**

| Section | Contents | Why |
|---------|----------|-----|
| Identity | `source_id`, `record_id`, `schema_id`, `schema_version` | Dedup and schema lookup |
| Data | Normalized domain fields | Only source-specific values consumers should see |
| Time | `observed_at`, `effective_at`, `expires_at` | Freshness and planning windows |
| Facets | Common fields such as `title`, `status`, `priority`, `start_at`, `end_at`, `url` | Cross-source query without knowing every payload |
| Provenance | `fetch_run_id`, `source_url`, `content_hash` | Audit and origin links |

**SQLite tables (v1)**

| Table | Purpose |
|-------|---------|
| `sources` | Source metadata: id, connector, schema ref, schedule, enabled, last_status, last_run_at |
| `items` | Latest result records; upsert by `(source_id, record_id)`; `data` as JSON |
| `fetch_runs` | Per-run audit: status, duration, error, item_count |
| `sync_state` | Optional incremental cursors / page tokens |

**Done when:** a normalized record can be written and queried from SQLite.

### 2. Source mapping engine

Where provider-specific shapes become contract fields. Develop against fixture JSON — no HTTP required.

**Deliverables**

- Mapping compiler/runtime driven by a source definition
- Safe transform ops
- Identity / `record_id` computation
- Schema validation after map
- Unit tests with saved API responses

**Mapping inputs (from a source definition)**

| Field | Example | Role |
|-------|---------|------|
| `records_path` | `$.daily` or `$.data.matches[*]` | Record root in the raw response |
| `field_map` | `$.temperature_2m_max[0]` → `high` | Provider path → contract field |
| `transforms` | `float`, `int`, `date`, `coalesce`, `join`, `lookup` | Per-field conversion |
| `identity` | fields used for `record_id` | Stable dedup key |
| `schema_ref` | `schedule_event@2` | Target contract |

**Done when:** fixture responses for at least one real API map to valid `ResultRecord`s without network calls.

### 3. `http_json` connector

Intentionally thin. No provider semantics.

**Deliverables**

- Build request from source definition (`url`, method, params, headers)
- Resolve auth from credential refs / `${ENV_VAR}` — never store secrets in config
- Decode JSON body
- Hand raw JSON to the mapping engine

**Interface (conceptual)**

```
run(source_definition) → raw response (status, headers, body)
```

Later connectors (`ical`, `rss`, `html_selector`) implement the same transport boundary; they do not replace mapping.

**Done when:** a live Open-Meteo (or similar) call returns JSON that the mapping engine already handles via fixtures.

### 4. Worker and CLI

Wire extract → map → store into one code path for manual and scheduled runs.

**Deliverables**

- Fetch loop with per-source error isolation
- `athena fetch --source <id>` and `athena fetch --all`
- Upsert results, log `fetch_runs`, update `sources.last_status`
- Advance result revision when content changes

**Flow**

```
load source definition
  → connector.fetch()
  → mapping.apply()
  → validate against schema
  → store.upsert()
  → log fetch_run
  → advance revision if content_hash changed
```

**Done when:** `athena fetch --source nyc-weather` upserts normalized rows you can `SELECT` from `items`.

### 5. Scheduler (later)

Per-source cron after the manual path works. Same worker entrypoint as the CLI. Do not build scheduling before fetch → store is trustworthy.

### 6. Onboarding APIs and UI (later)

Frontend form writes the same `SourceDefinition` the worker already understands.

| Step | Action |
|------|--------|
| Connect | URL, method, params, credential ref, schedule, connector |
| Sample | Test fetch; return redacted response tree and candidate arrays |
| Map | User picks record root and maps fields; suggestions optional |
| Preview | Normalized rows, IDs, agent/dashboard projections |
| Save | Versioned definition; YAML is import/export only |
| Activate | Only a passing definition is scheduled; edits draft a new version |

## Consumer connection

Ingestion writes; agent and dashboard read. They never call external APIs and never parse provider JSON.

| Consumer | Behavior |
|----------|----------|
| Agent context service | Query by schema, facets, freshness; structured entities + schema descriptions; record which result revision a plan used |
| Dashboard API | Same result query service; known schemas → widgets; unknown schemas → generic table/card; source health from `fetch_runs` |

When a fetch changes normalized data: advance the result revision and mark any cached plan **stale**. Do not invoke the planning agent on every poll. Show the cached plan with a stale indicator and offer reprioritization.

## When code is required

| Change | Code? | Action |
|--------|-------|--------|
| New JSON API response shape | No | Create/update a source mapping |
| New source using an existing schema | No | Reuse schema; map provider fields into it |
| New domain shape | Usually no | New data schema + generic dashboard rendering |
| New transport / document format | Yes, once | New connector (e.g. iCal, HTML) |
| Transform not expressible in the DSL | Yes, once | Add a safe reusable transform op |
| Highly interactive / stateful site | Yes | Specialized connector — do not hide code in config |

## Suggested layout

```
ingestion/
  athena/
    models/          # ResultRecord, SourceDefinition, DataSchema
    store/           # SQLite schema + upsert
    mapping/         # field_map, transforms, identity, validate
    connectors/
      http_json.py
    worker.py
    cli.py
  schemas/           # scalar.yaml, schedule_event.yaml, …
  sources/           # nyc-weather.yaml (dev/import); DB is runtime registry later
  tests/
    fixtures/        # sample API responses
    test_mapping.py
```

## Smallest meaningful milestone

One end-to-end source (prefer Open-Meteo: no auth, stable JSON):

1. Source definition (file is fine for v1)
2. Mapping turns the response into `ResultRecord`s
3. `athena fetch --source nyc-weather` upserts into SQLite
4. `SELECT * FROM items` shows normalized data

## Defer

| Component | Why wait |
|-----------|----------|
| Scheduler | Timing is irrelevant until fetch → store works |
| Onboarding UI | Needs mapping preview + validation APIs |
| Extra connectors | Prove the path with `http_json` first |
| Agent / dashboard | Read from `items` once records exist |

## If you only build one thing first

Prefer **mapping + fixture tests**. It is the hardest, most source-specific piece and can be developed entirely offline. Connectors and storage are straightforward once mapping is correct. Pair it with the result envelope and SQLite upsert so mapped records have a place to land.

## References

- [ingestion-contract.html](.cursor/ingestion-contract.html) — contract diagrams and consumer rules
- [ingestion-research.md](.cursor/ingestion-research.md) — earlier research and connector taxonomy
- [architecture-overview.html](../.cursor/architecture-overview.html) — system context (ingestion vs agent vs dashboard)
