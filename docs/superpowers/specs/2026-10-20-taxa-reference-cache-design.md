# Cache saved-taxa reference lookups in the iNaturalist runner

Status: proposed design; this PR contains documentation only.

## Intent

Opening or editing a configuration should not repeatedly fetch the same saved taxon IDs from iNaturalist. Cache public reference data in the action runner so it survives portal sessions and can be reused across runner instances. Do not cache observation extraction or change saved configuration values.

## Current behavior

- `app/actions/handlers.py::action_list_taxa` gives `ids` precedence over `q`. Saved IDs are resolved through `asyncio.to_thread(get_taxa_by_ids, ids)`, indexed by ID, and returned in requested order as `ReferenceDataResponse` options.
- `app/datasource/inaturalist.py::get_taxa_by_ids` filters nonnumeric IDs and requests batches of up to 500 with `is_active="any"`. Missing IDs are omitted; the portal retains their saved values and can flag them as unresolved.
- Search uses `search_taxa` and the autocomplete endpoint. An empty query returns an empty, truncated response; this is not a request for the whole taxonomy.
- The runner already uses asynchronous Redis for state. `IntegrationStateManager` deliberately skips writes for ephemeral runs, so it is not suitable for this reusable reference cache.

## Scope and proposed defaults

The defaults below are design proposals, not previously agreed product requirements:

1. Cache the **saved-ID resolve path** (`list_taxa(ids=...)`). Autocomplete query caching is a separate follow-up; this change must not download or prewarm the full taxonomy.
2. Cache by individual canonical numeric taxon ID, not by an entire configured list. Reopening a configuration is all hits; adding one ID fetches only that missing ID. Reordered or overlapping lists reuse entries.
3. Store the minimal option data needed by `_taxon_option`/the response, not full upstream records. Use a versioned connector-specific namespace, e.g. `inat:reference:taxa:v1:<id>`, in the configured Redis state database with a dedicated cache adapter. Do not use integration-state key names or synthetic ephemeral integration IDs.
4. Successful found entries expire after **24 hours**; confirmed missing IDs expire after **5 minutes**. Use explicit tagged found/missing records so a missing taxon is distinct from a cache miss. Hits do not renew expiry. No stale-on-error behavior in this first version.
5. Both TTLs are deployment-configurable positive durations, with the proposed defaults. The precise environment-variable names and adapter module name are left to implementation. Include defaults and operational guidance in documentation.

## Lookup algorithm and compatibility

- Cache orchestration belongs in connector-owned action code, for example `app/actions/taxa_cache.py`, called by `action_list_taxa`. Keep the datasource client independently usable with plain arguments and leave template `app/services/` code unchanged.
- For cache lookup and upstream fetch only, deduplicate numeric IDs and canonicalize their numeric spelling. Preserve the handler's existing emitted option values, input order and duplicate behavior; do not turn previously unresolved alternate spellings or arbitrary free text into newly resolved values. Never rewrite stored config or reorder the caller's list. Add compatibility tests against the current handler before implementing reconstruction.
- Multi-read the cache for requested IDs; validate cached schema/version. Corrupt or unknown-format entries are misses, not action failures.
- Fetch only uncached/expired numeric IDs through the existing batched helper, preserving `is_active="any"`, rate-limit behavior and thread offloading. If every requested numeric ID is cached, make **zero** iNaturalist calls. Nonnumeric-only inputs also make zero upstream calls.
- Extend the datasource boundary with a plain-argument validated lookup helper that returns records plus the IDs whose absence is conclusively established. It must inspect each batch response before flattening, validate the results shape and pagination/count metadata, and reject malformed or incomplete batches rather than treating them as empty. Keep the existing `get_taxa_by_ids` callable contract for other callers (it may delegate). The exact metadata shape is left to the builder. Do not infer completeness from the existing flattened list alone; that helper currently discards batch metadata.
- Publish cache entries only after the entire miss fetch succeeds and its response is validated as complete for the requested batch semantics. A malformed/truncated response or transport/rate-limit error must not create negative entries. If completeness cannot be established, do not negative-cache omissions. Do not cache exception payloads.
- Reassemble the response from fresh cached entries plus newly fetched entries using the same selection/order rules as the current handler. Omit confirmed unknown IDs and retain `truncated=False` for successful resolve responses. Preserve the existing response envelope and option formatting.
- If some misses cannot be fetched, preserve the current failure behavior rather than returning a partial list that would falsely mark unavailable IDs as nonexistent. Successfully cached IDs remain available for later calls.
- Keep the `q` search and empty-query paths unchanged, including `ids` precedence. No schema or portal changes are required.

## Shared storage, failures and concurrency

The cache is public reference data, not integration state. It may be shared across organizations only because the current lookup is unauthenticated and global. No credentials, configuration lists, integration IDs or user identifiers belong in keys or values. If lookup results later vary by API host, locale, filters or credentials, version or extend the namespace before sharing those results.

Use the runner's configured Redis connection settings and bounded connection/read/write timeouts; cache access must not introduce long retries. Reuse/close connections according to the runner's application lifecycle. This must work for both persisted integrations and draft/ephemeral execution without changing ephemeral state-write protections.

Redis read failure: bypass the cache and use the existing upstream path. Redis write failure: return the successful upstream result. Record a bounded warning/metric; never fail a valid reference response solely because caching is unavailable.

Coalesce overlapping in-flight misses within a runner process using per-ID futures/tasks; waiters share outcomes without one caller's cancellation cancelling work needed by others. Clean up entries after success, failure or cancellation. Cross-instance distributed locking is explicitly out of scope: simultaneous cold misses on separate instances may duplicate a request, but completed entries are shared. This avoids introducing an unbounded lock wait or making Redis availability mandatory.

TTL bounds storage lifetime but not instantaneous cardinality: do not cache the full taxonomy, avoid per-query/per-list entries, and measure namespace memory and miss volume during rollout. Use Redis eviction/memory controls rather than silently changing the shared Redis deployment in this implementation.

## Acceptance criteria and tests

1. First resolve of a list fetches its uncached numeric IDs. Repeating it before expiry makes zero upstream calls, including from a second cache-adapter instance using the same Redis backend and from an ephemeral invocation.
2. A reordered/overlapping list reuses entries; adding an ID fetches only that ID. Preserve option order, duplicates, labels/descriptions, inactive taxa, and existing handling of blank, nonnumeric and alternate numeric spellings.
3. Expiry causes refresh. A valid missing ID is negatively cached for the shorter TTL and retried after expiry; errors and incomplete responses are never cached as missing.
4. More than 500 misses retain existing batching. Failure in a later batch fails the action rather than emitting partial resolved options or poisoning the cache.
5. Redis unavailability, malformed cached entries and failed writes leave the upstream fallback usable with bounded cache latency.
6. Concurrent same-process requests for the same IDs share one upstream miss fetch; overlap, cancellation and failed-fetch cleanup are tested. Different processes can share completed cache entries without claiming distributed single-flight.
7. Search, empty-query behavior, `ids` precedence, response format and saved configuration remain unchanged.
8. Tests use mocked iNaturalist/Redis and a controllable clock; no production services. Add focused handler/cache tests and run the existing reference-action and datasource suites. A stage smoke test repeats a saved-ID request through two fresh portal sessions and confirms reduced upstream calls without label/config changes.

## Rollout and observability

Record aggregate cache hit/miss/negative-hit, bypass/error, coalesced-wait and upstream-call counts plus resolve latency. Do not log credentials, full request ID lists or cached response bodies. Compare cold and warm lookups and Redis memory on stage before production.

Use a deployment switch to bypass this cache for rollback, restoring the existing direct lookup behavior without deleting integration state. Namespace/version changes provide safe invalidation; any manual purge must target only this connector's reference-cache prefix. There is no data migration.

## Non-goals

No observation-data caching, full taxonomy synchronization, portal redesign, cross-instance locks, new Redis infrastructure, background refresh, stale serving or changes to generic template services. Runtime code and dependencies are not changed by this spec PR.
