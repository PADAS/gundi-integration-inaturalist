from .base import env

from .base import INTEGRATION_TYPE_SLUG


def _type_name(slug, name):
    """Display name of the integration type in Gundi. The template derives one
    from the slug when none is set, which gives "Inaturalist" for this runner's
    slug; the INTEGRATION_TYPE_NAME env var still wins."""
    if name:
        return name
    if (slug or "").strip().lower() == "inaturalist":
        return "iNaturalist"
    return None


INTEGRATION_TYPE_NAME = _type_name(INTEGRATION_TYPE_SLUG, env.str("INTEGRATION_TYPE_NAME", None))

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

# Default of the connection's max_new_events_per_run: the most new events one
# pull_events run sends to Gundi. The run reads from iNaturalist only until it
# has found that many, so a large backlog drains over several scheduled runs.
# Updates found on the way are sent too and don't count.
# Kept within the form's range (1-1000): pydantic doesn't check a field's default.
INAT_MAX_NEW_EVENTS_PER_PULL = env.int("INAT_MAX_NEW_EVENTS_PER_PULL", 150, validate=lambda n: 1 <= n <= 1000)
# A pull stops taking new work this many seconds before the tighter of
# MAX_ACTION_EXECUTION_TIME and INAT_PULL_TIME_LIMIT_SECONDS (Cloud Run's request
# timeout), so it ends with its progress saved instead of being cut off.
INAT_PULL_TIME_LIMIT_SECONDS = env.int("INAT_PULL_TIME_LIMIT_SECONDS", 300, validate=lambda n: n > 0)
INAT_PULL_TIME_MARGIN_SECONDS = env.int("INAT_PULL_TIME_MARGIN_SECONDS", 60, validate=lambda n: n >= 0)

# pull_events starts add_photos through the runner's actions topic. The infra
# module names it "<name>-actions-topic" and doesn't pass the name to the
# service, so fall back to it here; an env var still overrides it.
INTEGRATION_COMMANDS_TOPIC = env.str("INTEGRATION_COMMANDS_TOPIC", "inaturalist-actions-topic")
