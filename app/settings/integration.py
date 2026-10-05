# Add your integration-specific settings here
from environs import Env

env = Env()
env.read_env()

# Phase 0 of the reference-data design (gundi-integration-cmore docs/superpowers/
# specs/2026-07-31-reference-data-config-ui-design.md): reference actions are only
# registered in Gundi once the platform accepts the "reference" action type.
# Until then this stays off so self-registration never sends a type the API
# would reject.
REGISTER_REFERENCE_ACTIONS = env.bool("REGISTER_REFERENCE_ACTIONS", False)

# Requests the portal's map widget for the bounding box. Portals without the
# "bbox" widget (gundi-portal PR 380) fail to render the whole config form on an
# unknown widget name, so this stays off until every portal has it.
PORTAL_BBOX_WIDGET = env.bool("PORTAL_BBOX_WIDGET", False)
