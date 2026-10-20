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
