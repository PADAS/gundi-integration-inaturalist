"""In-memory CacheBackend with an injected clock, for tests only (not shipped)."""
from typing import Callable, Dict, List, Optional, Tuple


class MemoryBackend:
    def __init__(self, clock: Callable[[], float]):
        self._clock = clock
        self.store: Dict[str, Tuple[bytes, float]] = {}  # key -> (value, expires_at)

    async def get_many(self, keys: List[str]) -> List[Optional[bytes]]:
        now = self._clock()
        result: List[Optional[bytes]] = []
        for key in keys:
            entry = self.store.get(key)
            if entry is not None and entry[1] <= now:
                del self.store[key]
                entry = None
            result.append(entry[0] if entry is not None else None)
        return result

    async def set_many(self, entries: List[Tuple[str, bytes, int]]) -> None:
        now = self._clock()
        for key, value, ttl_seconds in entries:
            self.store[key] = (value, now + ttl_seconds)
