"""Shared cache for saved-taxon lookups used by the config form.

The portal re-requests the same saved taxon IDs every time the form opens, and each
miss costs an iNaturalist call against a rate-limited budget. This cache is owned by
the connector (not the portal) so every portal instance shares it.

It deliberately does not use ``IntegrationStateManager``: that store is a state DB
with no TTL support and an ephemeral write-skip, neither of which fits a cache.
The data cached is public iNaturalist taxon metadata, so nothing sensitive is
stored in keys or values. Key cardinality is bounded by the TTLs, by writing only
validated numeric IDs, and by the dedicated Redis DB, not by a per-request ID cap.

Semantics: hits do not renew expiry, and there is no stale-on-error. Backend
failures never raise; reads report ``failed`` and writes return False.
"""
import asyncio
import json
import logging
import math
import time
from typing import Dict, Iterable, List, NamedTuple, Optional, Protocol, Set, Tuple

import redis.asyncio as redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError

from app import settings
from app.datasource.inaturalist import TAXA_BY_ID_BATCH_SIZE, TaxaLookup, get_taxa_by_ids, lookup_taxa_by_ids

logger = logging.getLogger(__name__)

TAXA_CACHE_KEY_PREFIX = "inat:reference:taxa:v1:"
CACHED_TAXON_FIELDS = ("id", "name", "preferred_common_name", "rank")
BACKEND_ERRORS = (RedisError, OSError, asyncio.TimeoutError)


class CacheBackend(Protocol):
    async def get_many(self, keys: List[str]) -> List[Optional[bytes]]: ...

    async def set_many(self, entries: List[Tuple[str, bytes, int]]) -> None: ...  # (key, value, ttl_seconds)


def taxa_cache_key(taxon_id: int) -> str:
    return f"{TAXA_CACHE_KEY_PREFIX}{int(taxon_id)}"


class CachedTaxa(NamedTuple):
    found: Dict[int, Dict]  # minimal records
    missing: Set[int]
    corrupt: int  # entries present but undecodable (treated as uncached)
    failed: bool  # backend raised; everything is uncached


def _decode(raw: bytes):
    """Return ("found", record), ("missing", None) or None when undecodable."""
    try:
        entry = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(entry, dict):
        return None
    kind = entry.get("kind")
    if kind == "missing":
        return "missing", None
    if kind == "found":
        taxon = entry.get("taxon")
        if isinstance(taxon, dict) and isinstance(taxon.get("id"), int) and not isinstance(taxon.get("id"), bool):
            return "found", {f: taxon.get(f) for f in CACHED_TAXON_FIELDS}
    return None


class TaxaCache:
    def __init__(self, backend: CacheBackend, found_ttl_seconds: int, missing_ttl_seconds: int):
        self.backend = backend
        self.found_ttl_seconds = found_ttl_seconds
        self.missing_ttl_seconds = missing_ttl_seconds

    async def get_many(self, ids: List[int]) -> CachedTaxa:
        if not ids:
            return CachedTaxa({}, set(), 0, False)
        try:
            raws = await self.backend.get_many([taxa_cache_key(i) for i in ids])
        except BACKEND_ERRORS as e:
            logger.warning("taxa cache read failed (%s); resolving upstream", type(e).__name__)
            return CachedTaxa({}, set(), 0, True)
        found: Dict[int, Dict] = {}
        missing: Set[int] = set()
        corrupt = 0
        for taxon_id, raw in zip(ids, raws):
            if raw is None:
                continue
            decoded = _decode(raw)
            if decoded is None:
                corrupt += 1
            elif decoded[0] == "found":
                found[taxon_id] = decoded[1]
            else:
                missing.add(taxon_id)
        return CachedTaxa(found, missing, corrupt, False)

    async def put(self, found: Dict[int, Dict], missing: Iterable[int]) -> bool:
        entries: List[Tuple[str, bytes, int]] = []
        for taxon_id, taxon in found.items():
            body = {"kind": "found", "taxon": {f: taxon.get(f) for f in CACHED_TAXON_FIELDS}}
            entries.append((taxa_cache_key(taxon_id), json.dumps(body).encode(), self.found_ttl_seconds))
        for taxon_id in missing:
            entries.append((taxa_cache_key(taxon_id), json.dumps({"kind": "missing"}).encode(),
                            self.missing_ttl_seconds))
        if not entries:
            return True
        try:
            await self.backend.set_many(entries)
        except BACKEND_ERRORS as e:
            logger.warning("taxa cache write failed (%s)", type(e).__name__)
            return False
        return True


class RedisBackend:
    def __init__(self, client: redis.Redis):
        self.client = client

    async def get_many(self, keys: List[str]) -> List[Optional[bytes]]:
        return await self.client.mget(keys)

    async def set_many(self, entries: List[Tuple[str, bytes, int]]) -> None:
        async with self.client.pipeline(transaction=False) as pipe:
            for key, value, ttl in entries:
                pipe.set(key, value, ex=ttl)
            await pipe.execute()


def build_redis_client() -> redis.Redis:
    """Dedicated, tightly bounded client: a slow Redis must never stall the config form."""
    timeout = settings.INAT_TAXA_CACHE_TIMEOUT_SECONDS
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_REFERENCE_CACHE_DB,
        socket_connect_timeout=timeout,
        socket_timeout=timeout,
        retry_on_timeout=False,
        retry=Retry(NoBackoff(), 0),
    )


_default: Optional[TaxaCache] = None


def default_cache() -> Optional[TaxaCache]:
    """The shared cache, or None when INAT_TAXA_CACHE_ENABLED is false."""
    global _default
    if not settings.INAT_TAXA_CACHE_ENABLED:
        return None
    if _default is None:
        _default = TaxaCache(
            RedisBackend(build_redis_client()),
            found_ttl_seconds=settings.INAT_TAXA_CACHE_TTL_SECONDS,
            missing_ttl_seconds=settings.INAT_TAXA_CACHE_MISSING_TTL_SECONDS,
        )
    return _default


def _minimal(taxon: Optional[Dict]) -> Optional[Dict]:
    """Same shape as a cache hit, so a response does not depend on hit or miss."""
    return None if taxon is None else {f: taxon.get(f) for f in CACHED_TAXON_FIELDS}


# Upstream fetches in flight in this process: taxon id -> taxon record, or None when
# confirmed missing. Overlapping resolves (two portal tabs, a retry) wait on these
# instead of re-fetching. Entries are removed as soon as their fetch settles.
_inflight: Dict[int, "asyncio.Future[Optional[Dict]]"] = {}
# Strong references to running shared fetches: the event loop keeps only weak ones,
# so an unreferenced task could be garbage-collected mid-flight.
_fetch_tasks: Set["asyncio.Task[TaxaLookup]"] = set()


def _settle(task: "asyncio.Task[TaxaLookup]", futures: Dict[int, "asyncio.Future[Optional[Dict]]"]) -> None:
    """Done-callback of a shared fetch: resolve every owned future and unregister it."""
    _fetch_tasks.discard(task)
    for taxon_id, fut in futures.items():
        if _inflight.get(taxon_id) is fut:
            del _inflight[taxon_id]
        if task.cancelled():
            fut.cancel()
        elif task.exception() is not None:
            fut.set_exception(task.exception())
            fut.exception()  # mark retrieved: no "exception was never retrieved" if nobody waited
        else:
            fut.set_result(task.result().found.get(taxon_id))


async def _fetch_uncached(numeric_ids: List[int]) -> TaxaLookup:
    """Fetch ids upstream, sharing any fetch already in flight for the same ids.

    Callers await shielded work, so cancelling one caller never cancels a fetch
    other callers are waiting on.
    """
    waiting = {i: _inflight[i] for i in numeric_ids if i in _inflight}
    owned = [i for i in numeric_ids if i not in waiting]
    found: Dict[int, Dict] = {}
    missing: List[int] = []
    task = None
    if owned:
        loop = asyncio.get_running_loop()
        futures = {i: loop.create_future() for i in owned}
        _inflight.update(futures)
        task = asyncio.create_task(asyncio.to_thread(lookup_taxa_by_ids, owned))
        _fetch_tasks.add(task)
        task.add_done_callback(lambda t: _settle(t, futures))
    if waiting:
        logger.debug("taxa resolve coalesced: waited=%d owned=%d", len(waiting), len(owned))
    if task is not None:
        lookup = await asyncio.shield(task)
        found.update(lookup.found)
        missing.extend(lookup.missing)
    for taxon_id, fut in waiting.items():
        taxon = await asyncio.shield(fut)
        if taxon is None:
            missing.append(taxon_id)
        else:
            found[taxon_id] = taxon
    return TaxaLookup(found, missing)


async def resolve_taxa(ids: List[str], cache: Optional[TaxaCache]) -> Dict[str, Dict]:
    """Resolve saved taxon IDs to taxa, keyed by canonical ``str(int)`` in request order.

    ``cache=None`` is the bypass path: the legacy lookup, unchanged. Otherwise only
    IDs the cache cannot answer go upstream, and the cache is written only after the
    whole upstream fetch succeeded and only when the cache read did not fail (a failed
    read means the cache is bypassed for this request). Upstream errors propagate;
    cache errors never do.
    """
    if cache is None:
        taxa = await asyncio.to_thread(get_taxa_by_ids, ids)
        return {str(t["id"]): t for t in taxa if t.get("id") is not None}
    started = time.monotonic()
    numeric = list(dict.fromkeys(int(i) for i in ids if i.isdigit()))
    cached = await cache.get_many(numeric)
    uncached = [i for i in numeric if i not in cached.found and i not in cached.missing]
    fetched: Dict[int, Dict] = {}
    if uncached:
        lookup = await _fetch_uncached(uncached)
        fetched = lookup.found
        if not cached.failed:
            await cache.put(lookup.found, lookup.missing)
    result: Dict[str, Dict] = {}
    for i in numeric:
        taxon = cached.found.get(i) or _minimal(fetched.get(i))
        if taxon is not None:
            result[str(i)] = taxon
    logger.info(
        "taxa resolve: requested=%d hits=%d negative_hits=%d misses=%d corrupt=%d cache_failed=%s "
        "upstream_batches=%d elapsed_ms=%d",
        len(numeric), len(cached.found), len(cached.missing), len(uncached), cached.corrupt, cached.failed,
        math.ceil(len(uncached) / TAXA_BY_ID_BATCH_SIZE), int((time.monotonic() - started) * 1000),
    )
    return result
