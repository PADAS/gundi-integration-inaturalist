# Taxa Reference Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve saved iNaturalist taxon IDs from a shared Redis cache so reopening or editing a configuration makes zero iNat calls for IDs any runner has already seen.

**Architecture:** A new connector-owned module `app/actions/taxa_cache.py` sits between `action_list_taxa` and the datasource. It reads per-taxon entries from a dedicated Redis database through a small backend interface, fetches only the misses through a new validating datasource helper, writes found and confirmed-missing entries back with separate TTLs, and falls back to the existing direct lookup whenever Redis misbehaves or the cache is switched off. Template code under `app/services/` is not touched.

**Tech Stack:** Python 3.10, FastAPI action runner, `redis.asyncio` (redis-py 5.0.8), `pyinaturalist` 0.19, pydantic v1, pytest + pytest-asyncio + pytest-mock.

**Spec:** `docs/superpowers/specs/2026-10-20-taxa-reference-cache-design.md` (PR #51). The four 🟡 review findings on that PR are resolved by decisions in Global Constraints below; fold them into the spec when it merges.

## Global Constraints

- Cache key: `inat:reference:taxa:v1:<canonical int id>`. No credentials, config lists, integration IDs or user identifiers in keys or values.
- Cached value is the minimal option data only: `id`, `name`, `preferred_common_name`, `rank`. Entries are tagged `{"kind": "found", ...}` or `{"kind": "missing"}`.
- Found TTL default **86400** s; missing TTL default **300** s. Hits do not renew expiry. No stale-on-error.
- Dedicated Redis database `REDIS_REFERENCE_CACHE_DB` default **3** (state 0, configs 1, token cache 2). Never the state DB. (Resolves review 🟡 line 48.)
- Dedicated `redis.asyncio.Redis` client: `socket_connect_timeout` = `socket_timeout` = `INAT_TAXA_CACHE_TIMEOUT_SECONDS` default **0.5**, `retry_on_timeout=False`, `retry=Retry(NoBackoff(), 0)`, no stamina wrapper, lazily created module-level singleton, no close hook. (Resolves review 🟡 line 42.)
- A batch is complete iff `total_results == len(results)` and `total_results <= TAXA_BY_ID_BATCH_SIZE`; only then may absent IDs be negatively cached. Anything else raises and caches nothing. (Resolves review 🟡 line 32.)
- Tests use an in-memory backend with an injected clock; no `fakeredis`, no change to `requirements-dev.in`, no new runtime dependency. (Resolves review 🟡 line 59.)
- `ListTaxaQuery.ids` is capped at **2000** items per request.
- Bypass switch `INAT_TAXA_CACHE_ENABLED` (default true). When false, behaviour is byte-for-byte the current `get_taxa_by_ids` path.
- Emitted option values, order and the "exact canonical spelling only" rule of the current handler are preserved: `"007"` is still not labelled, `"7"` is.
- Redis read failure → bypass to upstream. Redis write failure → return the upstream result. Never fail a valid response because the cache is unavailable.
- Existing `get_taxa_by_ids(ids: List[str]) -> List[Dict]` keeps its contract and its tests pass unchanged.
- One structured INFO log line per resolve with aggregate counts; never log ID lists or cached bodies.

## Review Focus

1. **iNat pages a 500-ID batch** (`total_results` greater than `len(results)`): must raise `INatIncompleteBatchError`, never treat the tail as missing. Pinned in Task 1.
2. **Redis slow, not down** (read exceeds the 0.5 s timeout): resolve must still return the upstream result within the timeout plus one iNat call. Pinned in Task 2 (`asyncio.TimeoutError` from the backend).
3. **Entry written by a different schema** (unparseable JSON, unknown `kind`, non-dict `taxon`): treated as uncached and refetched, never a 500. Pinned in Task 2.
4. **Taxon without a `name` or `rank`**: cached record keeps the `None` fields and `_taxon_option` still labels it by id. Pinned in Task 2.
5. **Same taxon spelled two ways in one request** (`"7"` and `"007"`): one upstream ID, `"7"` labelled, `"007"` left raw, exactly as today. Pinned in Task 4.

---

### Task 1: Validating taxa lookup in the datasource

**Files:**
- Modify: `app/datasource/inaturalist.py:144-166` (`get_taxa_by_ids`) and the error classes around line 169
- Test: `app/datasource/tests/test_inaturalist.py` (append after the `get_taxa_by_ids` block, line 640)

**Interfaces:**
- Consumes: existing `get_taxa`, `_typeahead_session`, `TAXA_BY_ID_BATCH_SIZE = 500`, `INatRequestError`, `IntegrationConnectionError`.
- Produces:
  ```python
  class INatIncompleteBatchError(INatRequestError): ...
  class TaxaLookup(NamedTuple):
      found: Dict[int, Dict]   # taxon id -> full upstream record
      missing: List[int]       # requested ids conclusively absent, in request order
  def lookup_taxa_by_ids(numeric_ids: List[int]) -> TaxaLookup
  ```
  `numeric_ids` are already deduplicated canonical ints (the caller does that). `get_taxa_by_ids` is unchanged in behaviour; both share a private batch iterator.

- [ ] **Step 1: Write the failing tests**

```python
def _complete_taxa(id, **params):
    results = [{"id": i, "name": f"t{i}"} for i in id if i != 999]
    return {"results": results, "total_results": len(results), "page": 1, "per_page": params["per_page"]}


def test_lookup_taxa_by_ids_separates_found_from_confirmed_missing(mocker):
    from app.datasource import inaturalist
    mocker.patch.object(inaturalist, "get_taxa", side_effect=_complete_taxa)
    lookup = inaturalist.lookup_taxa_by_ids([41955, 999, 41963])
    assert set(lookup.found) == {41955, 41963}
    assert lookup.found[41955]["name"] == "t41955"
    assert lookup.missing == [999]


def test_lookup_taxa_by_ids_batches_like_get_taxa_by_ids(mocker):
    from app.datasource import inaturalist
    get_taxa = mocker.patch.object(inaturalist, "get_taxa", side_effect=_complete_taxa)
    lookup = inaturalist.lookup_taxa_by_ids(list(range(1, inaturalist.TAXA_BY_ID_BATCH_SIZE + 3)))
    assert len(lookup.found) == inaturalist.TAXA_BY_ID_BATCH_SIZE + 2
    assert [len(c.kwargs["id"]) for c in get_taxa.call_args_list] == [inaturalist.TAXA_BY_ID_BATCH_SIZE, 2]
    for call in get_taxa.call_args_list:
        assert call.kwargs["is_active"] == "any"
        assert call.kwargs["session"] is inaturalist._typeahead_session()


@pytest.mark.parametrize("response", [
    {"results": [{"id": 1}], "total_results": 2, "page": 1, "per_page": 500},   # paged: Review Focus 1
    {"results": [{"id": 1}], "total_results": 501, "page": 1, "per_page": 500},
    {"results": [{"id": 1}]},                                                   # no count metadata
    {"results": "nope", "total_results": 1},
    {"total_results": 0},
    {"results": [{"name": "no id"}], "total_results": 1},
])
def test_lookup_taxa_by_ids_rejects_incomplete_or_malformed_batches(mocker, response):
    from app.datasource import inaturalist
    mocker.patch.object(inaturalist, "get_taxa", return_value=response)
    with pytest.raises(inaturalist.INatIncompleteBatchError):
        inaturalist.lookup_taxa_by_ids([1, 2])


def test_lookup_taxa_by_ids_second_batch_failure_raises_without_partial_result(mocker):
    from app.datasource import inaturalist
    mocker.patch.object(inaturalist, "get_taxa", side_effect=[_complete_taxa(id=list(range(1, 501)), per_page=500), requests.ReadTimeout("t")])
    with pytest.raises(IntegrationConnectionError):
        inaturalist.lookup_taxa_by_ids(list(range(1, 503)))


def test_lookup_taxa_by_ids_empty_input_makes_no_request(mocker):
    from app.datasource import inaturalist
    get_taxa = mocker.patch.object(inaturalist, "get_taxa")
    assert inaturalist.lookup_taxa_by_ids([]) == inaturalist.TaxaLookup({}, [])
    get_taxa.assert_not_called()
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `pytest app/datasource/tests/test_inaturalist.py -k lookup_taxa_by_ids -v`
Expected: FAIL with `AttributeError: ... has no attribute 'lookup_taxa_by_ids'`

- [ ] **Step 3: Implement in `app/datasource/inaturalist.py`**

Add `INatIncompleteBatchError(INatRequestError)` with a docstring saying it is raised for a by-ID batch whose completeness cannot be established, so absent IDs must not be treated as missing. Add `TaxaLookup`. Extract the loop body of `get_taxa_by_ids` into `_taxa_batches(numeric: List[int]) -> Iterator[Tuple[List[int], Dict]]` that yields `(batch, response)` and keeps the existing `IntegrationConnectionError` wrapping; `get_taxa_by_ids` becomes a flatten over it. `lookup_taxa_by_ids` iterates the same generator and, per batch, checks the completeness rule from Global Constraints plus "every result is a dict with an int `id`", raising `INatIncompleteBatchError(f"...")` otherwise; found is keyed by `id`, missing is `[i for i in batch if i not in found]`, accumulated across batches.

- [ ] **Step 4: Run the whole datasource file to verify new and old tests pass**

Run: `pytest app/datasource/tests/test_inaturalist.py -v`
Expected: PASS, including the four pre-existing `get_taxa_by_ids` tests unchanged.

- [ ] **Step 5: Commit**

```bash
git add app/datasource/inaturalist.py app/datasource/tests/test_inaturalist.py
git commit -m "feat(inat): validating taxa lookup that distinguishes missing from incomplete"
```

---

### Task 2: Cache entries, TTLs and the in-memory test backend

**Files:**
- Create: `app/actions/taxa_cache.py`
- Create: `app/actions/tests/memory_cache_backend.py`
- Test: `app/actions/tests/test_taxa_cache.py`

**Interfaces:**
- Consumes: nothing from Task 1 yet (pure cache logic).
- Produces:
  ```python
  TAXA_CACHE_KEY_PREFIX = "inat:reference:taxa:v1:"
  CACHED_TAXON_FIELDS = ("id", "name", "preferred_common_name", "rank")
  BACKEND_ERRORS = (RedisError, OSError, asyncio.TimeoutError)   # from redis.exceptions import RedisError

  class CacheBackend(Protocol):
      async def get_many(self, keys: List[str]) -> List[Optional[bytes]]: ...
      async def set_many(self, entries: List[Tuple[str, bytes, int]]) -> None: ...  # (key, value, ttl_seconds)

  def taxa_cache_key(taxon_id: int) -> str

  class CachedTaxa(NamedTuple):
      found: Dict[int, Dict]   # minimal records
      missing: Set[int]
      corrupt: int             # entries present but undecodable (treated as uncached)
      failed: bool             # backend raised; everything is uncached

  class TaxaCache:
      def __init__(self, backend: CacheBackend, found_ttl_seconds: int, missing_ttl_seconds: int): ...
      async def get_many(self, ids: List[int]) -> CachedTaxa: ...
      async def put(self, found: Dict[int, Dict], missing: Iterable[int]) -> bool: ...  # False when the backend raised
  ```
  Test helper (not shipped):
  ```python
  class MemoryBackend:  # app/actions/tests/memory_cache_backend.py
      def __init__(self, clock: Callable[[], float]): ...
      store: Dict[str, Tuple[bytes, float]]   # value, expires_at
      async def get_many(self, keys) -> List[Optional[bytes]]   # drops expired entries
      async def set_many(self, entries) -> None
  ```

- [ ] **Step 1: Write the failing tests**

```python
import asyncio, json, pytest
from redis.exceptions import ConnectionError as RedisConnectionError

LEOPARD = {"id": 41955, "name": "Panthera pardus", "preferred_common_name": "Leopard", "rank": "species",
           "ancestor_ids": [1, 2, 3], "wikipedia_url": "https://..."}


@pytest.fixture
def clock():
    now = [1_000_000.0]
    class Clock:
        def __call__(self): return now[0]
        def advance(self, s): now[0] += s
    return Clock()


@pytest.fixture
def backend(clock):
    from app.actions.tests.memory_cache_backend import MemoryBackend
    return MemoryBackend(clock)


@pytest.fixture
def cache(backend):
    from app.actions.taxa_cache import TaxaCache
    return TaxaCache(backend, found_ttl_seconds=86400, missing_ttl_seconds=300)


def test_taxa_cache_key_is_versioned_and_namespaced():
    from app.actions.taxa_cache import taxa_cache_key
    assert taxa_cache_key(41955) == "inat:reference:taxa:v1:41955"


@pytest.mark.asyncio
async def test_put_stores_minimal_tagged_records_and_get_returns_them(cache, backend):
    await cache.put({41955: LEOPARD}, missing=[999])
    raw = json.loads(backend.store["inat:reference:taxa:v1:41955"][0])
    assert raw == {"kind": "found", "taxon": {"id": 41955, "name": "Panthera pardus",
                                              "preferred_common_name": "Leopard", "rank": "species"}}
    assert json.loads(backend.store["inat:reference:taxa:v1:999"][0]) == {"kind": "missing"}
    got = await cache.get_many([41955, 999, 7])
    assert got.found == {41955: {"id": 41955, "name": "Panthera pardus", "preferred_common_name": "Leopard", "rank": "species"}}
    assert got.missing == {999}
    assert got.corrupt == 0 and got.failed is False


@pytest.mark.asyncio
async def test_found_and_missing_expire_on_their_own_ttls(cache, clock):
    await cache.put({41955: LEOPARD}, missing=[999])
    clock.advance(301)
    got = await cache.get_many([41955, 999])
    assert 41955 in got.found and got.missing == set()
    clock.advance(86400)
    got = await cache.get_many([41955, 999])
    assert got.found == {} and got.missing == set()


@pytest.mark.asyncio
async def test_hits_do_not_renew_expiry(cache, clock):
    await cache.put({41955: LEOPARD}, missing=[])
    clock.advance(86000)
    await cache.get_many([41955])
    clock.advance(500)
    assert (await cache.get_many([41955])).found == {}


@pytest.mark.asyncio
async def test_record_without_name_or_rank_round_trips(cache):   # Review Focus 4
    from app.actions.handlers import _taxon_option
    await cache.put({5: {"id": 5}}, missing=[])
    got = await cache.get_many([5])
    assert got.found[5] == {"id": 5, "name": None, "preferred_common_name": None, "rank": None}
    assert _taxon_option(got.found[5]).label == "5"


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [b"not json", b"[]", b'{"kind": "v2thing"}', b'{"kind": "found"}',
                                 b'{"kind": "found", "taxon": "x"}', b'{"kind": "found", "taxon": {"name": "no id"}}'])
async def test_undecodable_entries_count_as_uncached(cache, backend, raw):   # Review Focus 3
    backend.store["inat:reference:taxa:v1:5"] = (raw, float("inf"))
    got = await cache.get_many([5])
    assert got.found == {} and got.missing == set() and got.corrupt == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [RedisConnectionError("down"), asyncio.TimeoutError(), OSError("refused")])
async def test_backend_read_failure_reports_failed_not_raises(cache, backend, exc, caplog):   # Review Focus 2
    async def boom(keys): raise exc
    backend.get_many = boom
    got = await cache.get_many([41955])
    assert got == ({}, set(), 0, True)
    assert "taxa cache" in caplog.text.lower()


@pytest.mark.asyncio
async def test_backend_write_failure_returns_false_not_raises(cache, backend):
    async def boom(entries): raise RedisConnectionError("down")
    backend.set_many = boom
    assert await cache.put({41955: LEOPARD}, missing=[]) is False


@pytest.mark.asyncio
async def test_get_many_with_no_ids_does_not_touch_backend(cache, backend):
    async def boom(keys): raise AssertionError("should not be called")
    backend.get_many = boom
    assert await cache.get_many([]) == ({}, set(), 0, False)
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest app/actions/tests/test_taxa_cache.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.actions.taxa_cache'`

- [ ] **Step 3: Create `app/actions/tests/memory_cache_backend.py`**

`MemoryBackend(clock)` as in Interfaces. `get_many` returns `None` for absent keys and for entries whose `expires_at <= clock()`; `set_many` stores `(value, clock() + ttl)`.

- [ ] **Step 4: Create `app/actions/taxa_cache.py` with the Task 2 surface**

Module docstring: why this is connector-owned, why not `IntegrationStateManager` (ephemeral write-skip, no TTL, state DB), and that the data is public. `TaxaCache.get_many` builds keys, calls `backend.get_many` inside `try/except BACKEND_ERRORS` → logs `logger.warning("taxa cache read failed (%s); resolving upstream", type(e).__name__)` and returns `CachedTaxa({}, set(), 0, True)`. Decoding: JSON object with `kind == "found"` and `taxon` a dict carrying an int `id` → found (restrict to `CACHED_TAXON_FIELDS`, filling absent fields with `None`); `kind == "missing"` → missing; anything else → `corrupt += 1`. `put` encodes `{"kind": "found", "taxon": {f: taxon.get(f) for f in CACHED_TAXON_FIELDS}}` with the found TTL and `{"kind": "missing"}` with the missing TTL, one `set_many` call, `except BACKEND_ERRORS` → warning, `return False`. Empty inputs short-circuit without a backend call.

- [ ] **Step 5: Run to verify they pass**

Run: `pytest app/actions/tests/test_taxa_cache.py -v`
Expected: PASS (all)

- [ ] **Step 6: Commit**

```bash
git add app/actions/taxa_cache.py app/actions/tests/memory_cache_backend.py app/actions/tests/test_taxa_cache.py
git commit -m "feat(inat): per-taxon reference cache entries with found/missing TTLs"
```

---

### Task 3: Redis backend, settings and the bypass switch

**Files:**
- Modify: `app/settings/integration.py` (currently a one-line comment)
- Modify: `app/actions/taxa_cache.py` (append)
- Modify: `.env.example` (after line 12), `CLAUDE.md` "Key env vars" section
- Test: `app/actions/tests/test_taxa_cache.py` (append)

**Interfaces:**
- Consumes: `TaxaCache`, `CacheBackend` from Task 2; `settings.REDIS_HOST`, `settings.REDIS_PORT`.
- Produces:
  ```python
  # app/settings/integration.py  (from .base import env)
  REDIS_REFERENCE_CACHE_DB = env.int("REDIS_REFERENCE_CACHE_DB", 3)
  INAT_TAXA_CACHE_ENABLED = env.bool("INAT_TAXA_CACHE_ENABLED", True)
  INAT_TAXA_CACHE_TTL_SECONDS = env.int("INAT_TAXA_CACHE_TTL_SECONDS", 86400, validate=lambda n: n > 0)
  INAT_TAXA_CACHE_MISSING_TTL_SECONDS = env.int("INAT_TAXA_CACHE_MISSING_TTL_SECONDS", 300, validate=lambda n: n > 0)
  INAT_TAXA_CACHE_TIMEOUT_SECONDS = env.float("INAT_TAXA_CACHE_TIMEOUT_SECONDS", 0.5, validate=lambda s: s > 0)

  # app/actions/taxa_cache.py
  class RedisBackend:
      def __init__(self, client: redis.asyncio.Redis): ...
      async def get_many(self, keys) -> List[Optional[bytes]]      # client.mget
      async def set_many(self, entries) -> None                     # pipeline(transaction=False); set(key, value, ex=ttl); execute
  def build_redis_client() -> redis.asyncio.Redis                   # kwargs from Global Constraints
  def default_cache() -> Optional[TaxaCache]                        # None when disabled; else lazily built module singleton `_default`
  ```

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.asyncio
async def test_redis_backend_uses_mget_and_a_pipeline_of_set_ex(mocker):
    from unittest.mock import AsyncMock, MagicMock
    from app.actions.taxa_cache import RedisBackend
    client = MagicMock()
    client.mget = AsyncMock(return_value=[b"a", None])
    pipe = MagicMock(); pipe.execute = AsyncMock(return_value=[True])
    client.pipeline.return_value.__aenter__ = AsyncMock(return_value=pipe)
    client.pipeline.return_value.__aexit__ = AsyncMock(return_value=False)
    backend = RedisBackend(client)
    assert await backend.get_many(["k1", "k2"]) == [b"a", None]
    client.mget.assert_awaited_once_with(["k1", "k2"])
    await backend.set_many([("k1", b"v", 86400), ("k2", b"m", 300)])
    client.pipeline.assert_called_once_with(transaction=False)
    assert [c.kwargs["ex"] for c in pipe.set.call_args_list] == [86400, 300]
    pipe.execute.assert_awaited_once()


def test_build_redis_client_is_bounded_and_on_the_reference_db(mocker):
    from app.actions import taxa_cache
    from redis.backoff import NoBackoff
    Redis = mocker.patch.object(taxa_cache.redis, "Redis")
    mocker.patch.object(taxa_cache.settings, "REDIS_REFERENCE_CACHE_DB", 3)
    mocker.patch.object(taxa_cache.settings, "INAT_TAXA_CACHE_TIMEOUT_SECONDS", 0.5)
    taxa_cache.build_redis_client()
    kw = Redis.call_args.kwargs
    assert kw["db"] == 3 and kw["socket_timeout"] == 0.5 and kw["socket_connect_timeout"] == 0.5
    assert kw["retry_on_timeout"] is False
    assert kw["retry"]._retries == 0 and isinstance(kw["retry"]._backoff, NoBackoff)


def test_default_cache_is_none_when_disabled(mocker):
    from app.actions import taxa_cache
    mocker.patch.object(taxa_cache, "_default", None)
    mocker.patch.object(taxa_cache.settings, "INAT_TAXA_CACHE_ENABLED", False)
    assert taxa_cache.default_cache() is None


def test_default_cache_is_a_lazily_built_singleton_with_configured_ttls(mocker):
    from app.actions import taxa_cache
    mocker.patch.object(taxa_cache, "_default", None)
    mocker.patch.object(taxa_cache.settings, "INAT_TAXA_CACHE_ENABLED", True)
    mocker.patch.object(taxa_cache.settings, "INAT_TAXA_CACHE_TTL_SECONDS", 11)
    mocker.patch.object(taxa_cache.settings, "INAT_TAXA_CACHE_MISSING_TTL_SECONDS", 2)
    build = mocker.patch.object(taxa_cache, "build_redis_client")
    first, second = taxa_cache.default_cache(), taxa_cache.default_cache()
    assert first is second and build.call_count == 1
    assert (first.found_ttl_seconds, first.missing_ttl_seconds) == (11, 2)


def test_settings_defaults():
    from app import settings
    assert settings.REDIS_REFERENCE_CACHE_DB == 3
    assert settings.INAT_TAXA_CACHE_ENABLED is True
    assert (settings.INAT_TAXA_CACHE_TTL_SECONDS, settings.INAT_TAXA_CACHE_MISSING_TTL_SECONDS) == (86400, 300)
    assert settings.INAT_TAXA_CACHE_TIMEOUT_SECONDS == 0.5
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest app/actions/tests/test_taxa_cache.py -k "redis_backend or build_redis or default_cache or settings_defaults" -v`
Expected: FAIL with `AttributeError` on `RedisBackend` / settings names.

- [ ] **Step 3: Add the settings to `app/settings/integration.py`**

Exactly the five lines in Interfaces, with a comment block: the cache is public reference data shared across integrations, lives in its own DB so prefix purges and eviction policy never touch state, and `INAT_TAXA_CACHE_ENABLED=false` is the rollback switch.

- [ ] **Step 4: Add `RedisBackend`, `build_redis_client`, `default_cache` to `app/actions/taxa_cache.py`**

`import redis.asyncio as redis`, `from redis.asyncio.retry import Retry`, `from redis.backoff import NoBackoff`, `from app import settings`. `TaxaCache` must expose `found_ttl_seconds` / `missing_ttl_seconds` attributes (the Task 2 constructor stores them). `_default: Optional[TaxaCache] = None` at module level.

- [ ] **Step 5: Run to verify they pass**

Run: `pytest app/actions/tests/test_taxa_cache.py -v`
Expected: PASS

- [ ] **Step 6: Document the env vars**

Append to `.env.example` after `REDIS_TOKEN_CACHE_DB=2`:
```
REDIS_REFERENCE_CACHE_DB=3
# iNat taxa reference cache (public data, shared across integrations). Set ENABLED=false to roll back.
INAT_TAXA_CACHE_ENABLED=true
INAT_TAXA_CACHE_TTL_SECONDS=86400
INAT_TAXA_CACHE_MISSING_TTL_SECONDS=300
INAT_TAXA_CACHE_TIMEOUT_SECONDS=0.5
```
In `CLAUDE.md` "Key env vars", replace the sentence saying the integration has no iNat-specific env vars with one paragraph naming these five and that the cache lives in `app/actions/taxa_cache.py`.

- [ ] **Step 7: Commit**

```bash
git add app/settings/integration.py app/actions/taxa_cache.py app/actions/tests/test_taxa_cache.py .env.example CLAUDE.md
git commit -m "feat(inat): Redis backend and settings for the taxa reference cache"
```

---

### Task 4: Resolve through the cache and wire the handler

**Files:**
- Modify: `app/actions/taxa_cache.py` (append)
- Modify: `app/actions/handlers.py:603-608` (`action_list_taxa` ids branch) and its `get_taxa_by_ids` import
- Modify: `app/actions/configurations.py:406-410` (`ListTaxaQuery.ids`)
- Modify: `app/actions/tests/conftest.py` (autouse fixture)
- Modify: `app/actions/tests/test_reference_actions.py:430-452` (`test_list_taxa_resolves_saved_ids_in_order`)
- Test: `app/actions/tests/test_taxa_cache.py`, `app/actions/tests/test_reference_actions.py`

**Interfaces:**
- Consumes: `lookup_taxa_by_ids`, `TaxaLookup`, `get_taxa_by_ids` (Task 1); `TaxaCache`, `default_cache` (Tasks 2–3).
- Produces:
  ```python
  # app/actions/taxa_cache.py
  async def resolve_taxa(ids: List[str], cache: Optional[TaxaCache]) -> Dict[str, Dict]
      # keys are canonical str(int); cache=None is the bypass path (legacy get_taxa_by_ids, unchanged)
  async def _fetch_uncached(numeric_ids: List[int]) -> TaxaLookup
      # Task 4: `await asyncio.to_thread(lookup_taxa_by_ids, numeric_ids)`; Task 5 adds coalescing here

  # app/actions/configurations.py
  MAX_TAXA_IDS_PER_REQUEST = 2000
  ```
  Handler: `from app.actions import taxa_cache` (module import, so tests can patch `taxa_cache.default_cache`), and the ids branch becomes `by_id = await taxa_cache.resolve_taxa(action_config.ids, taxa_cache.default_cache())` followed by the existing `options = [...]` line.

- [ ] **Step 1: Add the autouse bypass fixture to `app/actions/tests/conftest.py`**

```python
@pytest.fixture(autouse=True)
def taxa_cache_off(mocker):
    """Reference-action tests exercise the legacy path unless they build a cache themselves."""
    from app.actions import taxa_cache
    mocker.patch.object(taxa_cache, "default_cache", return_value=None)
```

- [ ] **Step 2: Write the failing tests in `test_taxa_cache.py`**

```python
@pytest.fixture
def upstream(mocker):
    """lookup_taxa_by_ids stand-in: knows 41955 and 41963, nothing else; counts calls."""
    from app.actions import taxa_cache
    from app.datasource.inaturalist import TaxaLookup
    known = {41955: {"id": 41955, "name": "Panthera pardus", "preferred_common_name": "Leopard", "rank": "species"},
             41963: {"id": 41963, "name": "Panthera", "rank": "genus"}}
    def lookup(ids):
        return TaxaLookup({i: known[i] for i in ids if i in known}, [i for i in ids if i not in known])
    return mocker.patch.object(taxa_cache, "lookup_taxa_by_ids", side_effect=lookup)


@pytest.mark.asyncio
async def test_repeat_resolve_makes_zero_upstream_calls_across_cache_instances(cache, backend, upstream):   # AC1
    from app.actions.taxa_cache import TaxaCache, resolve_taxa
    first = await resolve_taxa(["41955", "999", "41963"], cache)
    assert list(first) == ["41955", "41963"] and upstream.call_count == 1
    other = TaxaCache(backend, 86400, 300)
    assert await resolve_taxa(["41963", "41955", "999"], other) == first
    assert upstream.call_count == 1


@pytest.mark.asyncio
async def test_ephemeral_run_still_reads_and_writes_the_cache(cache, upstream):   # AC1
    from app.services.activity_logger import ephemeral_run
    from app.actions.taxa_cache import resolve_taxa
    token = ephemeral_run.set(True)
    try:
        await resolve_taxa(["41955"], cache)
        await resolve_taxa(["41955"], cache)
    finally:
        ephemeral_run.reset(token)
    assert upstream.call_count == 1


@pytest.mark.asyncio
async def test_adding_an_id_fetches_only_that_id(cache, upstream):   # AC2
    from app.actions.taxa_cache import resolve_taxa
    await resolve_taxa(["41955"], cache)
    await resolve_taxa(["41963", "41955"], cache)
    assert [list(c.args[0]) for c in upstream.call_args_list] == [[41955], [41963]]


@pytest.mark.asyncio
async def test_alternate_spellings_resolve_like_today(cache, upstream):   # Review Focus 5
    from app.actions.taxa_cache import resolve_taxa
    by_id = await resolve_taxa(["007", "7", "41955", "041955", "leopard", ""], cache)
    assert list(by_id) == ["41955"]
    assert upstream.call_args.args[0] == [7, 41955]


@pytest.mark.asyncio
async def test_nonnumeric_only_input_makes_no_upstream_call(cache, upstream):
    from app.actions.taxa_cache import resolve_taxa
    assert await resolve_taxa(["leopard"], cache) == {}
    upstream.assert_not_called()


@pytest.mark.asyncio
async def test_expiry_and_negative_cache_retry(cache, clock, upstream):   # AC3
    from app.actions.taxa_cache import resolve_taxa
    await resolve_taxa(["41955", "999"], cache)
    clock.advance(301)
    await resolve_taxa(["41955", "999"], cache)
    assert upstream.call_args.args[0] == [999]
    clock.advance(86400)
    await resolve_taxa(["41955", "999"], cache)
    assert upstream.call_args.args[0] == [41955, 999]


@pytest.mark.asyncio
@pytest.mark.parametrize("exc_type", ["INatIncompleteBatchError", "IntegrationConnectionError"])
async def test_upstream_failure_propagates_and_caches_nothing(cache, backend, mocker, exc_type):   # AC3, AC4
    from app.actions import taxa_cache
    from app.datasource import inaturalist
    from app.services import errors
    exc = getattr(inaturalist, exc_type, None) or getattr(errors, exc_type)
    mocker.patch.object(taxa_cache, "lookup_taxa_by_ids", side_effect=exc("boom"))
    with pytest.raises(exc):
        await taxa_cache.resolve_taxa(["41955", "999"], cache)
    assert backend.store == {}


@pytest.mark.asyncio
async def test_redis_read_failure_falls_back_to_upstream(cache, backend, upstream):   # AC5
    from app.actions.taxa_cache import resolve_taxa
    async def boom(keys): raise RedisConnectionError("down")
    backend.get_many = boom
    assert list(await resolve_taxa(["41955"], cache)) == ["41955"]
    assert upstream.call_count == 1


@pytest.mark.asyncio
async def test_redis_write_failure_still_returns_the_result(cache, backend, upstream):   # AC5
    from app.actions.taxa_cache import resolve_taxa
    async def boom(entries): raise RedisConnectionError("down")
    backend.set_many = boom
    assert list(await resolve_taxa(["41955"], cache)) == ["41955"]


@pytest.mark.asyncio
async def test_bypass_uses_the_legacy_lookup_verbatim(mocker, upstream):
    from app.actions import taxa_cache
    legacy = mocker.patch.object(taxa_cache, "get_taxa_by_ids", return_value=[{"id": 41955, "name": "Panthera pardus"}])
    by_id = await taxa_cache.resolve_taxa(["41955", "007", "x"], None)
    legacy.assert_called_once_with(["41955", "007", "x"])
    upstream.assert_not_called()
    assert by_id == {"41955": {"id": 41955, "name": "Panthera pardus"}}


@pytest.mark.asyncio
async def test_resolve_logs_aggregate_counts_not_ids(cache, upstream, caplog):
    import logging
    from app.actions.taxa_cache import resolve_taxa
    with caplog.at_level(logging.INFO, logger="app.actions.taxa_cache"):
        await resolve_taxa(["41955", "999"], cache)
        await resolve_taxa(["41955", "999"], cache)
    lines = [r.getMessage() for r in caplog.records if "taxa resolve" in r.getMessage()]
    assert "requested=2" in lines[0] and "misses=2" in lines[0] and "upstream_batches=1" in lines[0]
    assert "hits=1" in lines[1] and "negative_hits=1" in lines[1] and "upstream_batches=0" in lines[1]
    assert "41955" not in lines[0] and "41955" not in lines[1]
```

And in `test_reference_actions.py`, change `test_list_taxa_resolves_saved_ids_in_order` to patch `app.actions.taxa_cache.get_taxa_by_ids` instead of `handlers.get_taxa_by_ids` (the autouse fixture keeps it on the legacy path); assertions unchanged. Add to `test_configurations.py`:

```python
def test_list_taxa_query_caps_ids_per_request():
    from app.actions.configurations import ListTaxaQuery, MAX_TAXA_IDS_PER_REQUEST
    assert len(ListTaxaQuery(ids=[str(i) for i in range(MAX_TAXA_IDS_PER_REQUEST)]).ids) == MAX_TAXA_IDS_PER_REQUEST
    with pytest.raises(pydantic.ValidationError):
        ListTaxaQuery(ids=[str(i) for i in range(MAX_TAXA_IDS_PER_REQUEST + 1)])
```

- [ ] **Step 3: Run to verify they fail**

Run: `pytest app/actions/tests/test_taxa_cache.py app/actions/tests/test_reference_actions.py app/actions/tests/test_configurations.py -v`
Expected: new tests FAIL with `AttributeError: ... 'resolve_taxa'` / `MAX_TAXA_IDS_PER_REQUEST`; the renamed patch target in the handler test FAILS until the handler is rewired.

- [ ] **Step 4: Implement `resolve_taxa` and `_fetch_uncached`**

`resolve_taxa`: if `cache is None`, `taxa = await asyncio.to_thread(get_taxa_by_ids, ids)` and return `{str(t["id"]): t for t in taxa if t.get("id") is not None}` (the handler's current dict, moved). Otherwise: `numeric = list(dict.fromkeys(int(i) for i in ids if i.isdigit()))` (order-preserving dedup); `cached = await cache.get_many(numeric)`; `uncached = [i for i in numeric if i not in cached.found and i not in cached.missing]`; if `uncached`, `lookup = await _fetch_uncached(uncached)` then `await cache.put(lookup.found, lookup.missing)`; merge `cached.found` and `lookup.found` into `{str(i): t}` by iterating `numeric`, so key order follows the request (the AC1 and overlap tests assert it). Emit the INFO line `"taxa resolve: requested=%d hits=%d negative_hits=%d misses=%d corrupt=%d cache_failed=%s upstream_batches=%d elapsed_ms=%d"` with `upstream_batches = ceil(len(uncached) / TAXA_BY_ID_BATCH_SIZE)`.

- [ ] **Step 5: Rewire the handler and cap the query**

In `handlers.py` drop `get_taxa_by_ids` from the datasource import if nothing else uses it, add `from app.actions import taxa_cache`, and replace lines 605–606 per Interfaces. In `configurations.py` add `MAX_TAXA_IDS_PER_REQUEST = 2000` with a one-line comment (bounds cache cardinality per request; four 500-ID iNat calls, inside the typeahead bucket) and `max_items=MAX_TAXA_IDS_PER_REQUEST` on `ids`.

- [ ] **Step 6: Run the full suite**

Run: `pytest -q`
Expected: PASS. The `ui_schema` drift test in `test_configurations.py` must still pass (the `max_items` constraint adds `maxItems` to the query schema, which is not part of the `PullEventsConfig` ui schema).

- [ ] **Step 7: Commit**

```bash
git add app/actions/taxa_cache.py app/actions/handlers.py app/actions/configurations.py app/actions/tests/
git commit -m "feat(inat): resolve saved taxa through the shared reference cache"
```

---

### Task 5: Coalesce overlapping in-flight misses within a process

**Files:**
- Modify: `app/actions/taxa_cache.py` (`_fetch_uncached` only, plus a module-level `_inflight`)
- Test: `app/actions/tests/test_taxa_cache.py` (append)

**Interfaces:**
- Consumes: `_fetch_uncached(numeric_ids) -> TaxaLookup` signature from Task 4 (unchanged for callers).
- Produces:
  ```python
  _inflight: Dict[int, "asyncio.Future[Optional[Dict]]"]   # taxon id -> taxon record, or None when confirmed missing
  ```

- [ ] **Step 1: Write the failing tests**

```python
@pytest.fixture
def gated_upstream(mocker):
    """lookup_taxa_by_ids that blocks until released, so two resolves overlap."""
    import threading
    from app.actions import taxa_cache
    from app.datasource.inaturalist import TaxaLookup
    gate, calls = threading.Event(), []
    def lookup(ids):
        calls.append(list(ids)); gate.wait(5)
        return TaxaLookup({i: {"id": i} for i in ids if i != 999}, [i for i in ids if i == 999])
    mocker.patch.object(taxa_cache, "lookup_taxa_by_ids", side_effect=lookup)
    return gate, calls


@pytest.mark.asyncio
async def test_overlapping_requests_share_one_upstream_fetch(cache, gated_upstream):   # AC6
    from app.actions import taxa_cache
    gate, calls = gated_upstream
    a = asyncio.create_task(taxa_cache.resolve_taxa(["1", "2", "999"], cache))
    await asyncio.sleep(0.05)
    b = asyncio.create_task(taxa_cache.resolve_taxa(["2", "3", "999"], cache))
    await asyncio.sleep(0.05)
    gate.set()
    ra, rb = await asyncio.gather(a, b)
    assert sorted(sum(calls, [])) == [1, 2, 3, 999]          # each id fetched once
    assert list(ra) == ["1", "2"] and list(rb) == ["2", "3"]
    assert taxa_cache._inflight == {}


@pytest.mark.asyncio
async def test_cancelling_one_waiter_does_not_cancel_the_shared_fetch(cache, gated_upstream):   # AC6
    from app.actions import taxa_cache
    gate, calls = gated_upstream
    a = asyncio.create_task(taxa_cache.resolve_taxa(["1", "2"], cache))
    await asyncio.sleep(0.05)
    b = asyncio.create_task(taxa_cache.resolve_taxa(["2"], cache))
    await asyncio.sleep(0.05)
    a.cancel()
    gate.set()
    assert list(await b) == ["2"]
    with pytest.raises(asyncio.CancelledError):
        await a
    assert taxa_cache._inflight == {}


@pytest.mark.asyncio
async def test_failed_shared_fetch_fails_all_waiters_and_clears_inflight(cache, backend, mocker):   # AC6
    import threading
    from app.actions import taxa_cache
    from app.datasource.inaturalist import INatIncompleteBatchError
    gate = threading.Event()
    def lookup(ids): gate.wait(5); raise INatIncompleteBatchError("truncated")
    mocker.patch.object(taxa_cache, "lookup_taxa_by_ids", side_effect=lookup)
    a = asyncio.create_task(taxa_cache.resolve_taxa(["1"], cache))
    await asyncio.sleep(0.05)
    b = asyncio.create_task(taxa_cache.resolve_taxa(["1"], cache))
    await asyncio.sleep(0.05)
    gate.set()
    results = await asyncio.gather(a, b, return_exceptions=True)
    assert all(isinstance(r, INatIncompleteBatchError) for r in results)
    assert taxa_cache._inflight == {} and backend.store == {}
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest app/actions/tests/test_taxa_cache.py -k "overlapping or cancelling or failed_shared" -v`
Expected: the first FAILS on `sorted(sum(calls, []))` containing duplicates; the others FAIL on `_inflight` missing.

- [ ] **Step 3: Rewrite `_fetch_uncached` with coalescing**

Split `numeric_ids` into `waiting` (ids already in `_inflight`) and `owned`. For `owned`, create one `loop.create_future()` per id, register them, and start `task = asyncio.create_task(asyncio.to_thread(lookup_taxa_by_ids, owned))` with a done-callback that sets each future's result to the record or `None`, or `set_exception` on failure, and removes those ids from `_inflight` in all cases. Await `asyncio.shield(task)` and `asyncio.shield(f)` for each waiting future (so a cancelled caller never cancels work others need), then assemble a `TaxaLookup` from the owned lookup plus waited results (`None` → missing). Call `fut.exception()` on failed futures nobody else awaited to avoid "exception never retrieved" noise. A waiter's later `cache.put` of the same entries is a harmless duplicate write. Log at DEBUG `"taxa resolve coalesced: waited=%d owned=%d"` when `waiting` is non-empty (the spec asks for a coalesced-wait count).

- [ ] **Step 4: Run the full suite**

Run: `pytest -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/actions/taxa_cache.py app/actions/tests/test_taxa_cache.py
git commit -m "feat(inat): share one upstream fetch across overlapping taxa resolves"
```

---

## Verification before merge (manual, AC 8)

1. `pytest -q` green; `pip-compile` not needed (no dependency change) but CI re-runs it, so confirm `requirements.txt` is untouched.
2. Stage: deploy with defaults. Open a saved configuration with a few hundred taxa in two fresh portal sessions. In the runner logs, the first `taxa resolve` line shows `upstream_batches>=1`, the second `upstream_batches=0`; labels in the portal are identical both times; the saved config is unchanged.
3. Stage: `redis-cli -n 3 --scan --pattern 'inat:reference:taxa:v1:*' | wc -l` roughly equals the distinct taxa opened; `INFO memory` on db 3 is in the KB range.
4. Stage: confirm the 500-ID cap is honoured by iNat before relying on negative caching: resolve a config with more than 500 taxa and check no valid taxon is reported as missing (`redis-cli -n 3 GET inat:reference:taxa:v1:<known id>` is a `found` record).
5. Rollback drill: set `INAT_TAXA_CACHE_ENABLED=false`, redeploy, confirm labels still resolve and the log line disappears.
