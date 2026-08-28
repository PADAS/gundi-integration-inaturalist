# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A **Gundi v2 integration** that pulls observations from the [iNaturalist](https://www.inaturalist.org/) API on a schedule and forwards them to the [Gundi](https://gundiservice.org) platform as events (with photos as attachments). It is a FastAPI service built from the standard `gundi-integration-action-runner` template, but the webhook surface is unused — the real work lives in the `pull_events` action.

Note: a parent `../CLAUDE.md` describes a generic webhook integration. That description does not apply here; this integration is pull-only.

## Commands

```bash
# Install (Python 3.10 — see .python-version)
pip-compile --output-file=requirements.txt requirements-base.in requirements-dev.in requirements.in
pip install -r requirements.txt

# Tests
pytest
pytest app/actions/tests/test_pull_events.py -v
pytest app/datasource/tests/test_inaturalist.py::test_get_observations_batches_taxa -v

# Run server locally
uvicorn app.main:app --reload --port 8080

# Run the pull_events action against stage data (requires env from local/.env.local)
python handler_test_configuration.py

# Local docker stack (FastAPI + Redis) — uses local/.env.local
cd local && docker compose up --build
```

## Architecture

### Action flow (the only flow that matters here)

PubSub message → `POST /` (`app/main.py`) → `app/services/action_runner.py::execute_action` → `app/actions/handlers.py::action_pull_events`.

`action_pull_events` is the single entry point and does:

1. Loads cursor from `IntegrationStateManager` under key `STATE_LAST_RUN_KEY="last_run"` (falls back to legacy `"updated_to"`). If absent, uses `now - days_to_load`.
2. Calls `app/datasource/inaturalist.py::get_observations` (synchronous, uses `pyinaturalist.get_observations_v2`).
3. Splits observations into **new** (create in Gundi) vs **existing** (patch only if `observation.updated_at > last_synced_at`), by reading per-observation state keyed by `source_id=inat_id`.
4. Transforms via `_transform_inat_to_gundi_event` and submits in chunks of `GUNDI_SUBMISSION_CHUNK_SIZE = 100` via `send_events_to_gundi`.
5. If `include_photos`, downloads each photo URL with `httpx.AsyncClient(verify=False)` and uploads as Gundi attachments.
6. Patches changed observations via `update_event_in_gundi`.
7. Writes the new cursor (max `updated_at` across all observations seen) and per-observation `STATE_INAT_UPDATED_AT_KEY` so the next run doesn't re-patch unchanged observations.

If the iNat query returns nothing, the cursor is still advanced to `now()` so the next run does not re-query the same heavy window.

### iNaturalist client (`app/datasource/inaturalist.py`)

- Pagination: counts results with `per_page=0`, then walks pages of `INAT_PAGE_SIZE = 200`, ordered by `updated_at asc`. The iNat API rejects pagination past 10,000 results (`page * per_page > 10,000` returns a 500), so fetching is windowed: at most `MAX_PAGES_PER_WINDOW = 50` pages per query, then the query is re-issued with `updated_since` advanced to the last fetched observation's `updated_at` (results are id-deduped across windows).
- **Taxa batching**: `taxa` is a comma-separated string. The iNat API limits how many taxon IDs fit in a single request, so IDs are split into batches of `TAXA_BATCH_SIZE = 100` and each batch is queried independently; results are merged in a `dict[int, Observation]` (id-deduped). If `taxa` is empty/whitespace, a single batch of `None` is used (no taxon filter).
- Annotation filtering happens **client-side** after fetch in `_match_annotations_to_config` — `annotations` is `{term: [allowed_values...]}`; all terms in the dict must be present on the observation (AND across terms), and within a term all listed values must be present (AND across values).

### Configuration (`app/actions/configurations.py`)

`PullEventsConfig` is the user-facing schema rendered in the Gundi portal:

- `taxa` is a **list of ID strings** in the config model; a pydantic pre-validator coerces
  legacy comma-separated strings (and scalars). The datasource boundary stays a string:
  downstream code uses `action_config.taxa_str` (comma-joined), and `get_observations`
  is unchanged.
- `bounding_box` is accepted as a JSON-encoded string `"[ne_lat, ne_lng, sw_lat, sw_lng]"` and validated/parsed into a list of floats with range and ordering checks.
- `annotations` is accepted as structured rows; legacy JSON-encoded strings/dicts are coerced.
- `event_type` / `event_prefix` get coerced to `None` when the literal string `"any"` is submitted (workaround for a Gundi Portal limitation — do not remove without coordinating with portal).
- `ui_global_options` controls field ordering in the Gundi portal (react-jsonschema-form ui schema). Add new fields to this list when introducing them.

### Reference actions (selectable config values)

`action_list_projects`, `action_list_annotation_terms`, and `action_list_annotation_values`
in `app/actions/handlers.py` are **reference actions** (marker base
`ReferenceActionConfiguration` in `app/actions/core.py`): stateless queries the Gundi
portal calls through `/v1/actions/execute` with `config_overrides` to populate config-form
dropdowns. They wrap public, unauthenticated iNat endpoints. Design spec:
`gundi-integration-cmore/docs/superpowers/specs/2026-07-31-reference-data-config-ui-design.md`.

- They are hidden from registration unless `REGISTER_REFERENCE_ACTIONS=true`
  (`app/settings/integration.py`) — flip it only once the Gundi API accepts
  `"type": "reference"`.
- `PullEventsConfig.ui_schema()` carries `gundi:reference` annotations (helper
  `_reference` in `configurations.py`). Never set `ui:widget` on those nodes — old
  portals must keep plain text inputs. A drift test in
  `app/actions/tests/test_configurations.py` ties annotations to real actions.
- `annotations` config is now a list of `AnnotationFilter` rows (`term`, `values`);
  legacy JSON-string/dict configs are coerced by a pre-validator. Downstream code uses
  `action_config.annotations_dict` (the legacy `{term: [values]}` shape).
- `quality_grade` is a `List[Literal[...]]` — the enum must track
  `pyinaturalist.constants.QUALITY_GRADES` (drift-tested).
- `taxa` is a typeahead: `action_list_taxa` (wrapping `/v1/taxa/autocomplete`) plus a
  `search: {param: "q", min_chars: 2}` annotation per the typeahead contract extension
  (`cdip:docs/superpowers/specs/2026-08-27-typeahead-reference-data-design.md`). `q` is
  optional; empty query returns no options with `truncated: true`.

### State (`app/services/state.py`)

Two distinct shapes stored under `(integration_id, "pull_events")`:

| Key shape | What it is | Where written |
|-----------|-----------|---------------|
| no `source_id` | `{last_run: "%Y-%m-%d %H:%M:%S%z"}` cursor | end of `action_pull_events` |
| `source_id=<inat_id>` | Gundi response (incl. `object_id`) plus `inat_updated_at` | `save_events_state`, `save_patched_events_state` |

When changing state, preserve backward-compat reads of the legacy `"updated_to"` key (`_get_load_since`).

### Webhooks

`app/webhooks/handlers.py` and `app/webhooks/configurations.py` are empty. The webhook router is mounted but unused. Don't add a webhook handler unless iNaturalist gains push support — they don't.

## Conventions to follow

- **`taxa` is a string**, comma-separated. If you receive a list, coerce; do not branch downstream.
- New per-observation state must continue to be addressed by `source_id=str(inat_id)` so existing rows are found.
- Photo downloads use `verify=False` deliberately — iNat's photo CDN has had cert issues in the past. Do not "fix" without testing against production photo URLs.
- The pull action returns `{"result": {"events_extracted": int, "events_updated": int, "photos_attached": int}}` — these numbers are used by Gundi monitoring; preserve them.
- `gundi_core`, `gundi_client_v2`, and `pyinaturalist` are pinned via `requirements.in` / `requirements-base.in`. Run `pip-compile` after editing.

## Testing

- All external services (iNat, Gundi API, PubSub, Redis) are mocked. Shared fixtures live in `app/conftest.py` (very large — search before adding new ones). Action-specific fixtures including the `inaturalist_integration_v2` fixture are in `app/actions/tests/conftest.py`.
- CI: `.github/workflows/_tests.yml` re-runs `pip-compile` before `pytest`, so any change to `requirements.in` that doesn't compile cleanly will fail the PR.

## Key env vars (in addition to the standard Gundi template ones)

The integration itself has no iNat-specific env vars — the iNat API key is per-integration config, not per-service env. Standard Gundi template variables (`GUNDI_API_BASE_URL`, `KEYCLOAK_*`, `REDIS_*`, `INTEGRATION_TYPE_SLUG`, `REGISTER_ON_START`) apply; see `.env.example` and `app/settings/base.py`.
