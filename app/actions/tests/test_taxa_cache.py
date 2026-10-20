from app.actions.taxa_cache import default_cache as real_default_cache  # the autouse fixture patches the module attribute
import asyncio, json, pytest
from redis.exceptions import ConnectionError as RedisConnectionError

LEOPARD = {"id": 41955, "name": "Panthera pardus", "preferred_common_name": "Leopard", "rank": "species",
           "ancestor_ids": [1, 2, 3], "wikipedia_url": "https://..."}


@pytest.fixture(autouse=True)
def no_leaked_inflight():
    """A fetch left in flight by one test must not be shared into the next."""
    from app.actions import taxa_cache
    yield
    leaked = dict(taxa_cache._inflight)
    taxa_cache._inflight.clear()
    taxa_cache._fetch_tasks.clear()
    assert leaked == {}


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
@pytest.mark.parametrize("taxon", [
    {"id": 5, "rank": []},
    {"id": 5, "name": 5},
    {"id": 5, "preferred_common_name": {}},
    {"id": 6, "name": "Other", "rank": "species"},   # key/ID mismatch
])
async def test_malformed_or_mismatched_found_entries_count_as_uncached(cache, backend, taxon):
    backend.store["inat:reference:taxa:v1:5"] = (json.dumps({"kind": "found", "taxon": taxon}).encode(), float("inf"))
    got = await cache.get_many([5])
    assert got.found == {} and got.missing == set() and got.corrupt == 1


@pytest.mark.asyncio
async def test_found_entry_with_null_string_fields_still_decodes(cache, backend):
    taxon = {"id": 5, "name": None, "preferred_common_name": None, "rank": None}
    backend.store["inat:reference:taxa:v1:5"] = (json.dumps({"kind": "found", "taxon": taxon}).encode(), float("inf"))
    got = await cache.get_many([5])
    assert got.found[5]["id"] == 5 and got.corrupt == 0


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
    assert real_default_cache() is None


def test_default_cache_is_a_lazily_built_singleton_with_configured_ttls(mocker):
    from app.actions import taxa_cache
    mocker.patch.object(taxa_cache, "_default", None)
    mocker.patch.object(taxa_cache.settings, "INAT_TAXA_CACHE_ENABLED", True)
    mocker.patch.object(taxa_cache.settings, "INAT_TAXA_CACHE_TTL_SECONDS", 11)
    mocker.patch.object(taxa_cache.settings, "INAT_TAXA_CACHE_MISSING_TTL_SECONDS", 2)
    build = mocker.patch.object(taxa_cache, "build_redis_client")
    first, second = real_default_cache(), real_default_cache()
    assert first is second and build.call_count == 1
    assert (first.found_ttl_seconds, first.missing_ttl_seconds) == (11, 2)


def test_settings_defaults():
    from app import settings
    assert settings.REDIS_REFERENCE_CACHE_DB == 3
    assert settings.INAT_TAXA_CACHE_ENABLED is True
    assert (settings.INAT_TAXA_CACHE_TTL_SECONDS, settings.INAT_TAXA_CACHE_MISSING_TTL_SECONDS) == (86400, 300)
    assert settings.INAT_TAXA_CACHE_TIMEOUT_SECONDS == 0.5


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
async def test_failed_read_skips_the_cache_write(cache, backend, upstream):   # bypass during an outage
    from app.actions.taxa_cache import resolve_taxa
    async def boom(keys): raise RedisConnectionError("down")
    writes = []
    async def spy(entries): writes.append(entries)
    backend.get_many, backend.set_many = boom, spy
    assert list(await resolve_taxa(["41955", "999"], cache)) == ["41955"]
    assert upstream.call_count == 1 and writes == []


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
