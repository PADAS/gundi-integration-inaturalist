"""Shared cache for saved-taxon lookups used by the config form.

The portal re-requests the same saved taxon IDs every time the form opens, and each
miss costs an iNaturalist call against a rate-limited budget. This cache is owned by
the connector (not the portal) so every portal instance shares it.

It deliberately does not use ``IntegrationStateManager``: that store is a state DB
with no TTL support and an ephemeral write-skip, neither of which fits a cache.
The data cached is public iNaturalist taxon metadata, so nothing sensitive is
stored in keys or values.

Semantics: hits do not renew expiry, and there is no stale-on-error. Backend
failures never raise; reads report ``failed`` and writes return False.
"""
import asyncio
import json
import logging
from typing import Dict, Iterable, List, NamedTuple, Optional, Protocol, Set, Tuple

from redis.exceptions import RedisError

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
