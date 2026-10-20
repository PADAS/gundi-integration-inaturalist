from .base import env

# iNaturalist taxa reference cache. The cached data is public reference data shared
# across integrations. It lives in its own Redis DB so prefix purges and FLUSHDB never
# touch the state DB. Eviction (maxmemory, maxmemory-policy) is instance-wide, not per DB,
# so every cache key carries a TTL and the rollout checks the instance's maxmemory-policy.
# INAT_TAXA_CACHE_ENABLED=false is the rollback switch.
REDIS_REFERENCE_CACHE_DB = env.int("REDIS_REFERENCE_CACHE_DB", 3)
INAT_TAXA_CACHE_ENABLED = env.bool("INAT_TAXA_CACHE_ENABLED", True)
INAT_TAXA_CACHE_TTL_SECONDS = env.int("INAT_TAXA_CACHE_TTL_SECONDS", 86400, validate=lambda n: n > 0)
INAT_TAXA_CACHE_MISSING_TTL_SECONDS = env.int("INAT_TAXA_CACHE_MISSING_TTL_SECONDS", 300, validate=lambda n: n > 0)
INAT_TAXA_CACHE_TIMEOUT_SECONDS = env.float("INAT_TAXA_CACHE_TIMEOUT_SECONDS", 0.5, validate=lambda s: s > 0)
