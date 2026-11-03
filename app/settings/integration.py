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

# Most observations one pull_events run reads from iNaturalist (matching the
# annotation filter or not), so a large backlog drains over several scheduled
# runs instead of one run timing out.
INAT_MAX_OBSERVATIONS_PER_PULL = env.int("INAT_MAX_OBSERVATIONS_PER_PULL", 150, validate=lambda n: n > 0)
# A pull stops taking new work this many seconds before the tighter of
# MAX_ACTION_EXECUTION_TIME and INAT_PULL_TIME_LIMIT_SECONDS (Cloud Run's request
# timeout), so it ends with its progress saved instead of being cut off.
INAT_PULL_TIME_LIMIT_SECONDS = env.int("INAT_PULL_TIME_LIMIT_SECONDS", 300, validate=lambda n: n > 0)
INAT_PULL_TIME_MARGIN_SECONDS = env.int("INAT_PULL_TIME_MARGIN_SECONDS", 60, validate=lambda n: n >= 0)
