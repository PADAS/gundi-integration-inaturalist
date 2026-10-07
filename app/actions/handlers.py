import asyncio
from datetime import date, datetime, timedelta, timezone
import logging
from typing import Dict, List
from uuid import uuid4

import httpx
import requests
from pyrate_limiter import BucketFullException
from gundi_core.schemas.v2 import Integration, LogLevel
from pyinaturalist import Observation

from gundi_client_v2.errors import GundiAPIError

from app import settings
from app.actions.configurations import (
    PullEventsConfig,
    ListProjectsQuery,
    ListAnnotationTermsQuery,
    ListAnnotationValuesQuery,
    ListTaxaQuery,
)
from app.actions.core import ReferenceDataResponse, ReferenceOption, action_title
from app.services.activity_logger import activity_logger, ephemeral_run, log_action_activity
from app.services.gundi import (
    send_event_attachments_to_gundi,
    send_events_to_gundi,
    update_event_in_gundi,
)
from app.datasource.inaturalist import (
    get_observations,
    bbox_to_search_circle,
    find_existing_projects,
    list_controlled_terms,
    search_projects_near,
    search_taxa,
)
from app.services.state import IntegrationStateManager

GUNDI_SUBMISSION_CHUNK_SIZE = 100

# Pull-events state: single key for the cursor (legacy "updated_to" still read for backward compatibility)
STATE_LAST_RUN_KEY = "last_run"
STATE_DATETIME_FMT = "%Y-%m-%d %H:%M:%S%z"
# Per-observation state: when we last synced this observation to Gundi (so we only patch when it changes)
STATE_INAT_UPDATED_AT_KEY = "inat_updated_at"
# Per-integration run lock: held while a pull runs, expires if a run dies without releasing it.
# Each run writes its own token, so a run that outlives its lock can't release the next run's.
STATE_RUN_LOCK_SOURCE_ID = "run_lock"
RUN_LOCK_TTL_SECONDS = settings.MAX_ACTION_EXECUTION_TIME + 60

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


def _get_load_since(state: dict, fallback_days: int) -> datetime:
    """Return the datetime to use for updated_since. Uses stored cursor or now - fallback_days."""
    raw = state.get(STATE_LAST_RUN_KEY) or state.get("updated_to")
    if raw:
        return datetime.strptime(raw, STATE_DATETIME_FMT)
    return datetime.now(tz=timezone.utc) - timedelta(days=fallback_days)


def _build_pull_events_state(last_updated: datetime) -> dict:
    """Build the state dict to persist after a pull_events run."""
    return {STATE_LAST_RUN_KEY: last_updated.strftime(STATE_DATETIME_FMT)}

async def _drop_unknown_projects(integration: Integration, projects: List[str]) -> List[str]:
    """The saved projects that exist on iNaturalist, warning about the rest.

    iNat rejects an observations query whose projects are all unknown (422), so
    unknown projects are left out of the query. If the lookup itself fails, the
    projects are used as saved.
    """
    try:
        existing = find_existing_projects(projects)
    except (requests.RequestException, BucketFullException) as e:
        # BucketFullException is pyinaturalist's rate-limit error, not a RequestException.
        logger.warning(f"Could not check the saved iNaturalist projects for integration ID: {integration.id}: {e}")
        return projects
    unknown = [p for p in projects if p not in existing]
    if unknown:
        if existing:
            msg = (f"These saved projects don't exist on iNaturalist and were left out of the pull: "
                   f"{', '.join(unknown)}.")
        else:
            msg = (f"None of the saved projects exist on iNaturalist ({', '.join(unknown)}), "
                   f"so the pull was skipped. Update the projects in the configuration.")
        logger.warning(f"{msg} Integration ID: {integration.id}.")
        try:
            await log_action_activity(
                integration_id=integration.id,
                action_id="pull_events",
                level=LogLevel.WARNING,
                title=msg,
                data={"unknown_projects": unknown},
            )
        except Exception as log_error:
            # Best-effort: the warning is already in the logs.
            logger.warning(f"Could not publish the unknown-projects warning for integration ID: {integration.id}: {log_error}")
    return existing


def chunk_list(list_a, chunk_size):
  for i in range(0, len(list_a), chunk_size):
    yield list_a[i:i + chunk_size]

@action_title("Pull iNaturalist Observations")
@activity_logger()
async def action_pull_events(integration: Integration, action_config: PullEventsConfig):
    # A redelivered run (transient failures are nacked) can overlap the next scheduled
    # run for the same integration, so only one pull runs per integration at a time.
    # Ephemeral runs persist no state, so they skip the lock.
    integration_id = str(integration.id)
    if ephemeral_run.get():
        return await _pull_events(integration, action_config)
    lock_token = uuid4().hex
    acquired = await state_manager.set_if_absent(
        integration_id, "pull_events", ttl_seconds=RUN_LOCK_TTL_SECONDS, source_id=STATE_RUN_LOCK_SOURCE_ID,
        value=lock_token,
    )
    if not acquired:
        msg = f"Skipping iNaturalist pull for integration ID: {integration_id}: another run is in progress."
        logger.info(msg)
        try:
            await log_action_activity(
                integration_id=integration.id,
                action_id="pull_events",
                level=LogLevel.INFO,
                title=msg,
                data={"message": msg}
            )
        except Exception as log_error:
            # Best-effort, like the runner's own skip: a publisher failure must not
            # turn the skip into an error that PubSub redelivers.
            logger.warning(f"Could not publish the skip notice for integration ID: {integration_id}: {log_error}")
        return {'result': {'events_extracted': 0,
                           'events_updated': 0,
                           'photos_attached': 0}}
    try:
        return await _pull_events(integration, action_config)
    finally:
        try:
            released = await state_manager.delete_state_if_value(
                integration_id, "pull_events", lock_token, source_id=STATE_RUN_LOCK_SOURCE_ID
            )
            if not released:
                logger.info(
                    f"Pull lock for integration ID: {integration_id} had already expired or been "
                    f"taken by another run; left it in place."
                )
        except Exception:
            # The lock expires on its own; don't mask the run's own outcome.
            logger.exception(f"Error releasing the pull lock for integration ID: {integration_id}.")


async def _pull_events(integration: Integration, action_config: PullEventsConfig):

    # Log the id only: the Integration carries every action config, including
    # the auth row's api_key.
    logger.info(f"Executing 'pull_events' action with integration {integration.id} and action_config {action_config}...")

    state = await state_manager.get_state(integration.id, "pull_events")
    load_since = _get_load_since(state, action_config.days_to_load)

    projects = action_config.projects
    if projects:
        projects = await _drop_unknown_projects(integration, projects)
        if not projects:
            # Pulling without the projects would widen the query to the whole
            # bounding box (or the world), so skip until the config is fixed.
            return {'result': {'events_extracted': 0,
                               'events_updated': 0,
                               'photos_attached': 0}}

    # Todo: write an async version of get_observations that uses httpx.AsyncClient to fetch the observations.
    observations = get_observations(
        load_since,
        bounding_box=action_config.bounding_box,
        taxa=action_config.taxa_str,
        projects=projects,
        quality_grade=action_config.quality_grade,
        annotations=action_config.annotations_dict,
    )

    if not observations:
        msg = f"No new iNaturalist observations to process for integration ID: {str(integration.id)}."
        logger.info(msg)
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_events",
            level=LogLevel.WARNING,
            title=msg,
            data={"message": msg}
        )
        # Advance cursor so next run doesn't re-query the same window (avoids repeated heavy requests)
        now = datetime.now(tz=timezone.utc)
        await state_manager.set_state(
            str(integration.id), "pull_events", _build_pull_events_state(now)
        )
        return {'result': {'events_extracted': 0,
                           'events_updated': 0,
                           'photos_attached': 0}}

    logger.info(f"Processing {len(observations)} observations from iNaturalist.")

    async def get_inaturalist_events_to_patch():
        # Split observations into: new (create in Gundi) vs existing (patch only if observation changed).
        patch_these_events = []
        process_these_events = []
        for event_id, observation in observations.items():
            saved_event = await state_manager.get_state(str(integration.id), "pull_events", str(event_id))
            if not saved_event:
                process_these_events.append(observation)
                continue
            # Only patch when the observation has changed since we last synced it (avoids updating every run)
            last_synced_at = saved_event.get(STATE_INAT_UPDATED_AT_KEY)
            if last_synced_at:
                try:
                    if isinstance(last_synced_at, str):
                        last_synced_at = datetime.strptime(last_synced_at, STATE_DATETIME_FMT)
                    ob_updated = observation.updated_at
                    if ob_updated.tzinfo is None:
                        ob_updated = ob_updated.replace(tzinfo=timezone.utc)
                    if last_synced_at.tzinfo is None:
                        last_synced_at = last_synced_at.replace(tzinfo=timezone.utc)
                    if ob_updated <= last_synced_at:
                        continue  # Already in sync, skip patch
                except (ValueError, TypeError):
                    pass  # Bad or legacy value, patch to be safe
            patch_these_events.append((saved_event.get("object_id"), observation))
        return process_these_events, patch_these_events

    filtered_observations, events_to_patch = await get_inaturalist_events_to_patch()

    events_to_process = []

    updated_count = 0
    added_count = 0
    attachment_count = 0

    if filtered_observations:
        all_event_photos = {}
        inat_updated_at_map = {}  # inat_id -> updated_at for state we persist after create
        newest = None
        for ob in filtered_observations:

            if(not newest or (newest < ob.created_at)):
                newest = ob.created_at

            e = _transform_inat_to_gundi_event(ob, action_config)
            events_to_process.append(e)

            inat_id = e['event_details']['inat_id']
            inat_updated_at_map[inat_id] = ob.updated_at
            all_event_photos[inat_id] = []
            for photo in ob.photos:
                all_event_photos[inat_id].append((photo.id, photo.large_url if photo.large_url else photo.url))

        logger.info(f"Submitting {len(events_to_process)} iNaturalist observations to Gundi")

        for i, to_add_chunk in enumerate(chunk_list(events_to_process, GUNDI_SUBMISSION_CHUNK_SIZE)):

            logger.info(f"Processing chunk #{i+1}")

            response = await send_events_to_gundi(events=to_add_chunk, integration_id=str(integration.id))
            added_count += len(response)

            if response:
                # Send images as attachments (if available)
                if action_config.include_photos:
                    attachments_response = await process_attachments(to_add_chunk, response, all_event_photos, integration)
                    attachment_count += attachments_response
                # Process events to patch
                await save_events_state(response, to_add_chunk, integration, inat_updated_at_map)

    else:
        logger.info(f"No new iNaturalist observations to process for integration ID: {str(integration.id)}.")

    if events_to_patch:
        # Process events to patch
        logger.info(f"Updating {len(events_to_patch)} events from iNaturalist observations to Gundi for integration ID: {str(integration.id)}.")
        response = await patch_events(events_to_patch, action_config, integration)
        updated_count += len(response)
        await save_patched_events_state(events_to_patch, integration)

    last_updated = max(ob.updated_at for ob in observations.values())
    logger.info("Updating state through %s", last_updated)
    await state_manager.set_state(
        str(integration.id), "pull_events", _build_pull_events_state(last_updated)
    )
        
    return {'result': {'events_extracted': added_count,
                       'events_updated': updated_count,
                       'photos_attached': attachment_count}}


async def process_attachments(events, response, all_event_photos, integration):
    attachments_processed = 0
    for event, event_id in zip(events, response):
        inat_id = event['event_details']['inat_id']
        gundi_id = event_id['object_id']
        available_photos = all_event_photos.get(inat_id, [])
        if not available_photos:
            continue
        attachments = []
        try:
            for photo_id, photo_url in available_photos:
                logger.info(f"Adding {photo_url} from iNat event {inat_id} to Gundi event {gundi_id}")

                filename = str(photo_id) + "." + photo_url.split(".")[-1]

                async with httpx.AsyncClient(timeout=120, verify=False) as session:
                    image_response = await session.get(photo_url)
                    image_response.raise_for_status()

                img = await image_response.aread()

                attachments.append((filename, img))

            attachments_response = await send_event_attachments_to_gundi(
                event_id=gundi_id,
                attachments=attachments,
                integration_id=str(integration.id)
            )
            if attachments_response:
                attachments_processed += len(attachments)
        except Exception as e:
            request = {
                "event_id": gundi_id,
                # Filenames only: the tuples also hold the raw photo bytes.
                "attachments": [filename for filename, _ in attachments],
                "integration_id": str(integration.id)
            }
            message = f"Error while processing event attachments for event ID '{event_id['object_id']}'. Exception: {e}. Request: {request}"
            logger.exception(message, extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            })
            log_data = {"message": message}
            if isinstance(e, GundiAPIError):
                log_data["server_response_body"] = e.detail
            elif server_response := getattr(e, "response", None):
                log_data["server_response_body"] = server_response.text
            await log_action_activity(
                integration_id=integration.id,
                action_id="pull_events",
                level=LogLevel.WARNING,
                title=message,
                data=log_data
            )
            continue
    return attachments_processed


async def patch_events(events, updated_config_data, integration):
    responses = []
    for event in events:
        gundi_object_id = event[0]
        new_event = event[1]
        transformed_data = _transform_inat_to_gundi_event(new_event, updated_config_data)
        if transformed_data:
            response = await update_event_in_gundi(
                event_id=gundi_object_id,
                event=transformed_data,
                integration_id=str(integration.id)
            )
            responses.append(response)
    return responses


async def save_events_state(response, events, integration, inat_updated_at_map=None):
    """Persist Gundi event state per observation so we know what we created and when we last synced."""
    inat_updated_at_map = inat_updated_at_map or {}
    for saved_event, event in zip(response, events):
        try:
            event_id = event["event_details"]["inat_id"]
            state = dict(saved_event)
            updated_at = inat_updated_at_map.get(event_id)
            if updated_at is not None:
                state[STATE_INAT_UPDATED_AT_KEY] = (
                    updated_at.strftime(STATE_DATETIME_FMT)
                    if hasattr(updated_at, "strftime") else str(updated_at)
                )
            await state_manager.set_state(
                integration_id=str(integration.id),
                action_id="pull_events",
                state=state,
                source_id=event_id
            )
        except Exception as e:
            inat_id = event.get("event_details", {}).get("inat_id", "unknown")
            message = f"Error while saving event ID '{inat_id}'. Exception: {e}."
            logger.exception(message, extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            })
            raise e


async def save_patched_events_state(events_to_patch, integration):
    """After patching, update per-observation state so we don't patch again until the observation changes."""
    for gundi_object_id, observation in events_to_patch:
        try:
            updated_at = observation.updated_at
            state = {
                "object_id": gundi_object_id,
                STATE_INAT_UPDATED_AT_KEY: (
                    updated_at.strftime(STATE_DATETIME_FMT)
                    if hasattr(updated_at, "strftime") else str(updated_at)
                ),
            }
            await state_manager.set_state(
                integration_id=str(integration.id),
                action_id="pull_events",
                state=state,
                source_id=str(observation.id),
            )
        except Exception as e:
            logger.exception(
                "Error saving state for patched observation %s: %s",
                observation.id,
                e,
                extra={"integration_id": str(integration.id), "attention_needed": True},
            )
            raise e


def _normalize_recorded_at(observed_on, created_at):
    """Return a timezone-aware datetime for Gundi recorded_at (date or datetime, naive or aware)."""
    if not observed_on:
        return created_at
    if isinstance(observed_on, date) and not isinstance(observed_on, datetime):
        return datetime.combine(observed_on, datetime.min.time(), tzinfo=timezone.utc)
    if getattr(observed_on, "tzinfo", None) is None:
        return observed_on.replace(tzinfo=timezone.utc)
    return observed_on


def _transform_inat_to_gundi_event(ob: Observation, config: PullEventsConfig):
    
    event = {
        "event_type": config.event_type,
        "recorded_at": _normalize_recorded_at(ob.observed_on, ob.created_at),
        "event_details": {
            "inat_id": str(ob.id),
            "captive": ob.captive,
            "location_obscured": ob.obscured,
            "created_at": ob.created_at,
            "place_guess": ob.place_guess,
            "quality_grade": ob.quality_grade,
            "species_guess": ob.species_guess,
            "updated_at": ob.updated_at,
            "inat_url": ob.uri
        }
    }

    if(ob.user):
        event['event_details']['user_id'] = ob.user.id
        event['event_details']['user_name'] = ob.user.name if ob.user.name else ob.user.login

    if(ob.location):
        event["location"] = {
            "lat": ob.location[0],
            "lon": ob.location[1] }

    if ob.place_ids:
        event["event_details"]["place_ids"] = ",".join(str(pid) for pid in ob.place_ids)

    if(ob.taxon):
        event["event_details"].update({
            "taxon_id": ob.taxon.id,
            "taxon_rank": ob.taxon.rank,
            "taxon_name": ob.taxon.name,
            "taxon_common_name": ob.taxon.preferred_common_name,
            "taxon_wikipedia_url": ob.taxon.wikipedia_url,
            "taxon_conservation_status": ob.taxon.conservation_status
        })

        if(ob.taxon.preferred_common_name):
            event["title"] = ob.taxon.preferred_common_name
        if ob.taxon.ancestor_ids:
            event["event_details"]["taxon_ancestors"] = ",".join(str(aid) for aid in ob.taxon.ancestor_ids)

    if(not event.get("title")):
        event["title"] = "Unknown" if not ob.species_guess else ob.species_guess

    event["title"] = config.event_prefix + event["title"]

    return event


@action_title("List Nearby iNaturalist Projects")
async def action_list_projects(integration: Integration, action_config: ListProjectsQuery):
    """Reference action: iNaturalist projects nearest the configured bounding box.

    Uses the public project-search endpoint (no auth), nearest-first, one page —
    the portal's combobox keeps free text for anything beyond the cap.
    """
    lat, lng, radius_km = bbox_to_search_circle(action_config.bounding_box)
    response = search_projects_near(lat, lng, radius_km)
    results = response.get("results", [])
    options = [
        ReferenceOption(value=str(project["id"]), label=project.get("title") or str(project["id"]))
        for project in results
        if project.get("id") is not None
    ]
    truncated = response.get("total_results", len(results)) > len(results)
    return ReferenceDataResponse(options=options, truncated=truncated).dict()


@action_title("List iNaturalist Annotation Terms")
async def action_list_annotation_terms(integration: Integration, action_config: ListAnnotationTermsQuery):
    """Reference action: iNaturalist annotation controlled terms (near-static vocabulary)."""
    terms = list_controlled_terms()
    options = [
        ReferenceOption(value=str(term["id"]), label=term.get("label") or str(term["id"]))
        for term in terms
        if term.get("id") is not None
    ]
    options.sort(key=lambda o: o.label or o.value)
    return ReferenceDataResponse(options=options, cache_ttl_seconds=3600).dict()


@action_title("List iNaturalist Annotation Values")
async def action_list_annotation_values(integration: Integration, action_config: ListAnnotationValuesQuery):
    """Reference action: the allowed values of one annotation controlled term."""
    terms = list_controlled_terms()
    term = next((t for t in terms if str(t.get("id")) == action_config.term), None)
    if term is None:
        raise ValueError(f"Unknown iNaturalist annotation term '{action_config.term}'.")
    options = [
        ReferenceOption(value=str(value["id"]), label=value.get("label") or str(value["id"]))
        for value in term.get("values", [])
        if value.get("id") is not None
    ]
    return ReferenceDataResponse(options=options, cache_ttl_seconds=3600).dict()


async def action_list_taxa(integration: Integration, action_config: ListTaxaQuery):
    """Reference action (typeahead): taxa matching the typed query.

    The taxa vocabulary is far too large for a default page, so an empty query
    returns no options with truncated=True — the portal's search widget only
    fetches once the operator has typed, and old widgets get a clean empty list.
    """
    query = (action_config.q or "").strip()
    if not query:
        return ReferenceDataResponse(options=[], truncated=True).dict()
    # search_taxa blocks (requests, plus a rate limiter that sleeps), so keep it
    # off the event loop: the portal calls this on every keystroke.
    response = await asyncio.to_thread(search_taxa, query)
    results = response.get("results", [])
    options = []
    for taxon in results:
        if taxon.get("id") is None:
            continue
        scientific = taxon.get("name") or str(taxon["id"])
        common = taxon.get("preferred_common_name")
        options.append(ReferenceOption(
            value=str(taxon["id"]),
            label=f"{common} ({scientific})" if common else scientific,
            description=taxon.get("rank"),
        ))
    truncated = response.get("total_results", len(results)) > len(results)
    return ReferenceDataResponse(options=options, truncated=truncated).dict()