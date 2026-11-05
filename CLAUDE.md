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

1. Loads cursor from `IntegrationStateManager` under key `STATE_LAST_RUN_KEY="last_run"` (falls back to legacy `"updated_to"`), but never further back than `now - days_to_load`; if absent, uses `now - days_to_load`.
2. Reads pages with `app/datasource/inaturalist.py::iter_observation_pages` (synchronous, `pyinaturalist.get_observations_v2`, run in a thread), oldest change first, until `max_new_events_per_run` new observations, the end of what changed, or the time budget.
3. Sorts each observation (`_sort_observation`), reading per-observation state keyed by `source_id=inat_id`: **too old** (uploaded more than `max_age_days` ago by `created_at`; skipped, nothing stored), **new** (create in Gundi), **update** (in state and the payload hash differs from the stored `sent_hash`, or none is stored) or **unchanged** (skipped). `updated_at` never decides anything and is not in the payload.
4. Transforms via `_transform_inat_to_gundi_event` and submits in chunks of `GUNDI_SUBMISSION_CHUNK_SIZE = 100` via `send_events_to_gundi`.
5. If `include_photos`, records the first `max_photos` photos of each new event as pending and starts the separate `add_photos` action, which downloads each photo URL with `httpx.AsyncClient(verify=False)` and uploads it as a Gundi attachment.
6. Patches changed observations via `update_event_in_gundi`.
7. After each chunk, writes the cursor (the chunk's last `updated_at`) and per-observation `STATE_INAT_UPDATED_AT_KEY` and `STATE_SENT_HASH_KEY` so the next run doesn't re-patch observations whose payload didn't change.

If the iNat query returns nothing, the cursor is still advanced to `now()` so the next run does not re-query the same heavy window.

### Manual backfill (`backfill_events`)

A generic, executable action (never scheduled) that a user runs from the portal: it creates events for sightings uploaded in the last `days_back` days (default 30), oldest first, using the connection's `pull_events` settings, until `max_events` (default 1000). Creations only: sightings already in state are skipped, nothing is updated, no photos are sent, and state is written like the pull's (with `sent_hash`). It reads with `iter_upload_pages` (upload window, id ascending, resumable by taxa batch and id), holds the same run lock as the pull (a pull that finds it taken leaves a `pull_waiting` note and the backfill hands over), and runs as an asynchronous chain: each run stops before the time budget and starts the internal `backfill_events_continue` action with its position, also saved under `backfill.<window end>`. Running it again is safe. Each run carries a `seq`, raised only when a run hands over, so duplicate messages do nothing and a cut-off run's redelivery or a same-settings restart picks the chain up; a change to the pull filters stops it, and a different start replaces it (checked against `backfill_current` before each chunk and before handing over). With `TRIGGER_ACTIONS_ALWAYS_SYNC` (local) continuations run nested inside the first run. Design: spec §5.

### iNaturalist client (`app/datasource/inaturalist.py`)

- Pagination: counts results with `per_page=0`, then walks pages of `INAT_PAGE_SIZE = 200`, ordered by `updated_at asc`. The iNat API rejects pagination past 10,000 results (`page * per_page > 10,000` returns a 500), so fetching is windowed: at most `MAX_PAGES_PER_WINDOW = 50` pages per query, then the query is re-issued with `updated_since` advanced to the last fetched observation's `updated_at` (results are id-deduped across windows).
- `iter_upload_pages` (backfill only): one upload window (`created_d1`/`created_d2`), `order_by=id asc`, paged with `id_above`, so it has no 10,000-result limit and resumes exactly from a taxa batch and an id.
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

- They are registered with Gundi as `"type": "reference"` (template behavior).
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
| no `source_id` | `{last_run: "%Y-%m-%d %H:%M:%S%z"}` cursor | after each chunk of `action_pull_events`, and at the end when nothing matched |
| `source_id=run_lock` / `pull_waiting` | the run lock a pull or backfill holds (a backfill's value names it: `{"backfill": key, "token": …}`); the note a skipped pull leaves |
| `source_id=backfill_wait.<key>.<seq>.<n>` | one-shot claim, so only one copy of a waiting backfill message hands over | `action_pull_events`, `_backfill` |
| `source_id=backfill.<window end>` | a backfill's position (`seq`, batch, last id, count, last upload date), then a `done` marker once it ends (rows it creates carry `backfill_job` = this key and `backfill_batch`; patches keep them) | `_backfill_locked` |
| `source_id=backfill_current` | the last started backfill's settings, filter fingerprint and window, so a repeated start continues it | `_backfill_job_for` |
| `source_id=backfill_starting` | short lock so backfill starts choose their job one at a time | `_backfill_job_for` |
| `source_id=photos.<inat_id>` / `photos_lock.<inat_id>` | a new event's pending photos (`object_id`, `pending`); the lock held by the photo run adding them | `save_events_state`, `action_add_photos` |
| `source_id=inat_down` | how many pulls in a row found iNaturalist down | `_handle_inat_down` |
| `source_id=<inat_id>` | Gundi response (incl. `object_id`) plus `inat_updated_at` and `sent_hash` (SHA-256 of the payload last sent); while a new event is being sent, only `sending_since` (a row left with only that is never sent again) | `save_events_state`, `save_patched_events_state` |

When changing state, preserve backward-compat reads of the legacy `"updated_to"` key (`_get_load_since`).

### Webhooks

`app/webhooks/handlers.py` and `app/webhooks/configurations.py` are empty. The webhook router is mounted but unused. Don't add a webhook handler unless iNaturalist gains push support — they don't.

## Conventions to follow

- **`taxa` is a list in the config model and a comma-separated string at the datasource.** Downstream code uses `action_config.taxa_str`; coerce at the config boundary, do not branch on shape downstream.
- New per-observation state must continue to be addressed by `source_id=str(inat_id)` so existing rows are found.
- Photo downloads use `verify=False` deliberately — iNat's photo CDN has had cert issues in the past. Do not "fix" without testing against production photo URLs.
- The pull action returns `{"result": {"events_extracted": int, "events_updated": int, "photos_attached": int, ...}}` — these three numbers are used by Gundi monitoring; preserve them. It also returns `events_skipped_too_old`, `events_skipped_unchanged`, `photos_left_out` and `cursor_lag_days`.
- Pull limits live on the connection (`PullEventsConfig`): `max_age_days` (90), `max_photos` (1, first photos in iNaturalist order; 0 = none) and `max_new_events_per_run` (defaults to `INAT_MAX_NEW_EVENTS_PER_PULL`). Design: `docs/superpowers/specs/2026-11-05-pull-limits-design.md`.
- `gundi_core`, `gundi_client_v2`, and `pyinaturalist` are pinned via `requirements.in` / `requirements-base.in`. Run `pip-compile` after editing.

## Testing

- All external services (iNat, Gundi API, PubSub, Redis) are mocked. Shared fixtures live in `app/conftest.py` (very large — search before adding new ones). Action-specific fixtures including the `inaturalist_integration_v2` fixture are in `app/actions/tests/conftest.py`.
- CI: `.github/workflows/_tests.yml` re-runs `pip-compile` before `pytest`, so any change to `requirements.in` that doesn't compile cleanly will fail the PR.

## Key env vars (in addition to the standard Gundi template ones)

The iNat API key is per-integration config, not per-service env. `pull_events` has three: `INAT_MAX_NEW_EVENTS_PER_PULL` (150; the default of each connection's `max_new_events_per_run`: reading stops once that many new events are found, updates don't count), `INAT_PULL_TIME_LIMIT_SECONDS` (300, Cloud Run's request timeout) and `INAT_PULL_TIME_MARGIN_SECONDS` (60; a run stops taking new chunks this long before the tighter of that limit and `MAX_ACTION_EXECUTION_TIME`). `INTEGRATION_COMMANDS_TOPIC` defaults to `inaturalist-actions-topic` (the topic the infra module creates; deployed runners don't set it), which `pull_events` uses to start `add_photos`. The rest belong to the saved-taxa reference cache in `app/actions/taxa_cache.py`: `REDIS_REFERENCE_CACHE_DB` (default 3, never the state DB), `INAT_TAXA_CACHE_ENABLED` (default true; `false` is the rollback switch), `INAT_TAXA_CACHE_TTL_SECONDS` (86400), `INAT_TAXA_CACHE_MISSING_TTL_SECONDS` (300) and `INAT_TAXA_CACHE_TIMEOUT_SECONDS` (0.5, Redis connect/socket timeout). Standard Gundi template variables (`GUNDI_API_BASE_URL`, `KEYCLOAK_*`, `REDIS_*`, `INTEGRATION_TYPE_SLUG`, `REGISTER_ON_START`) apply; see `.env.example` and `app/settings/base.py`.
