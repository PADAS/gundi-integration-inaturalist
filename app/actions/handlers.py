import httpx
import logging

from datetime import datetime, timezone, timedelta
from math import ceil
from app.actions.configurations import AuthenticateConfig, PullEventsConfig
from app.services.activity_logger import activity_logger, log_activity
from app.services.gundi import send_events_to_gundi, update_event_in_gundi, send_event_attachments_to_gundi
from app.services.state import IntegrationStateManager
from gundi_core.schemas.v2 import Integration, LogLevel
from pyinaturalist import get_observations_v2, Observation, Annotation
from typing import Dict, List
from urllib.parse import urlparse
from urllib.request import urlretrieve

import re

GUNDI_SUBMISSION_CHUNK_SIZE = 100

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

async def handle_transformed_data(transformed_data, integration_id, action_id):
    try:
        response = await send_events_to_gundi(
            events=transformed_data,
            integration_id=integration_id
        )
    except httpx.HTTPError as e:
        msg = f'Sensors API returned error for integration_id: {integration_id}. Exception: {e}'
        logger.exception(
            msg,
            extra={
                'needs_attention': True,
                'integration_id': integration_id,
                'action_id': action_id
            }
        )
        return [msg]
    else:
        return response

def get_inaturalist_observations(integration: Integration, config: PullEventsConfig, since: datetime):

    nelat = nelng = swlat = swlng = None
    if(config.bounding_box):
        nelat, nelng, swlat, swlng = config.bounding_box

    target_taxa = []
    for taxa in config.taxa:
        target_taxa.append(str(taxa))
    target_taxa = ",".join(target_taxa)

    fields = ",".join(["observed_on", "created_at", "id", "captive", "obscured", "place_guess", "quality_grade", "species_guess", "updated_at", 
                       "uri", "photos", "user", "location", "place_ids", "taxon", "photos.large_url", "photos.url", "taxon.id", "taxon.rank", "taxon.name",
                       "taxon.preferred_common_name", "taxon.wikipedia_url", "taxon.conservation_status", "user.id", "user.name", "user.login",
                       "annotations.controlled_attribute_id", "annotations.controlled_value_id"])
    get_observations_params = { "page": 1,
                                "per_page": 0,
                                "updated_since": since,
                                "project_id" : config.projects,
                                "quality_grade" : config.quality_grade,
                                "taxon_id" : target_taxa, 
                                "order_by" : "updated_at", 
                                "order" : "asc"}
    if nelat and nelng and swlat and swlng:
        get_observations_params["nelat"] = nelat
        get_observations_params["nelng"] = nelng
        get_observations_params["swlat"] = swlat
        get_observations_params["swlng"] = swlng
    inat_count_req = get_observations_v2(**get_observations_params)
    inat_count = inat_count_req.get("total_results")
    pages = ceil(inat_count/200)

    observation_map = {}
    for page in range(1,pages+1):
        logger.debug(f"Loading page {page} of {pages} from iNaturalist")
        get_observations_v2_params = {
            "page" : page,
            "per_page" : 200, 
            "updated_since" : since, 
            "project_id" : config.projects,
            "quality_grade" : config.quality_grade, 
            "taxon_id" : target_taxa,
            "order_by" : 'updated_at', 
            "order":"asc", 
            "fields":fields
        }
        if nelat and nelng and swlat and swlng:
            get_observations_v2_params["nelat"] = nelat
            get_observations_v2_params["nelng"] = nelng
            get_observations_v2_params["swlat"] = swlat
            get_observations_v2_params["swlng"] = swlng
        response = get_observations_v2(**get_observations_v2_params)
        observations = Observation.from_json_list(response)

        logger.info(f"Loaded {len(observations)} observations from iNaturalist before annotation filters.")
        for o in observations:
            if(config.annotations):
                if(_match_annotations_to_config(o.annotations, config.annotations)):    
                    observation_map[o.id] = o
            else:
                observation_map[o.id] = o

    return observation_map

def chunk_list(list_a, chunk_size):
  for i in range(0, len(list_a), chunk_size):
    yield list_a[i:i + chunk_size]

@activity_logger()
async def action_pull_events(integration: Integration, action_config: PullEventsConfig):

    logger.info(f"Executing 'pull_events' action with integration {integration} and action_config {action_config}...")

    state = await state_manager.get_state(integration.id, "pull_events")

    last_run = state.get('updated_to') or state.get('last_run')
    now = datetime.now(tz=timezone.utc)
    if(last_run):
        load_since = datetime.strptime(last_run, '%Y-%m-%d %H:%M:%S%z')
    else:
        load_since = now - timedelta(days=action_config.days_to_load)

    observations = get_inaturalist_observations(integration, action_config, load_since)

    if not observations:
        msg = f"No new iNaturalist observations to process for integration ID: {str(integration.id)}."
        logger.info(msg)
        await log_activity(
            integration_id=integration.id,
            action_id="pull_events",
            level=LogLevel.WARNING,
            title=msg,
            data={"message": msg}
        )
        return {'result': {'events_extracted': 0,
                           'events_updated': 0,
                           'photos_attached': 0}}

    logger.info(f"Processing {len(observations)} observations from iNaturalist.")

    async def get_inaturalist_events_to_patch():
        # Get through the events and check if state_manager has it recorded from a previous execution
        patch_these_events = []
        process_these_events = []
        for event_id, observation in observations.items():
            saved_event = await state_manager.get_state(str(integration.id), "pull_events", str(event_id))
            if saved_event:
                # Event already exists, will patch it
                patch_these_events.append((saved_event.get("object_id"), observation))
            else:
                process_these_events.append(observation)
        return process_these_events, patch_these_events

    filtered_observations, events_to_patch = await get_inaturalist_events_to_patch()

    events_to_process = []

    updated_count = 0
    added_count = 0
    attachment_count = 0

    if filtered_observations:
        all_event_photos = {}
        newest = None
        for ob in filtered_observations:

            if(not newest or (newest < ob.created_at)):
                newest = ob.created_at

            e = _transform_inat_to_gundi_event(ob, action_config)
            events_to_process.append(e)

            inat_id = e['event_details']['inat_id']
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
                await save_events_state(response, to_add_chunk, integration)

    else:
        logger.info(f"No new iNaturalist observations to process for integration ID: {str(integration.id)}.")

    if events_to_patch:
        # Process events to patch
        logger.info(f"Updating {len(events_to_patch)} events from iNaturalist observations to Gundi for integration ID: {str(integration.id)}.")
        response = await patch_events(events_to_patch, action_config, integration)
        updated_count += len(response)

    # Taking the most recent 'updated_at' date from all the extracted obs from iNaturalist
    last_updated = max(ob.updated_at for ob in observations.values())

    logger.info(f"Updating state through {last_updated}")
    state = {"last_run": last_updated.strftime('%Y-%m-%d %H:%M:%S%z')}
    await state_manager.set_state(str(integration.id), "pull_events", state)
        
    return {'result': {'events_extracted': added_count,
                       'events_updated': updated_count,
                       'photos_attached': attachment_count}}


async def process_attachments(events, response, all_event_photos, integration):
    attachments_processed = 0
    for event, event_id in zip(events, response):
        inat_id = event['event_details']['inat_id']
        gundi_id = event_id['object_id']
        attachments = []
        try:
            for photo_id, photo_url in all_event_photos.get(inat_id, []):
                logger.info(f"Adding {photo_url} from iNat event {inat_id} to Gundi event {gundi_id}")
                fp = urlretrieve(photo_url)
                path = urlparse(photo_url).path
                ext = re.split(r".*\.", path)[1]
                filename = str(photo_id) + "." + ext
                attachments.append((filename, open(fp[0], 'rb')))

            response = await send_event_attachments_to_gundi(
                event_id=gundi_id,
                attachments=attachments,
                integration_id=str(integration.id)
            )
            if response:
                attachments_processed += len(attachments)
        except Exception as e:
            request = {
                "event_id": gundi_id,
                "attachments": attachments,
                "integration_id": str(integration.id)
            }
            message = f"Error while processing event attachments for event ID '{event_id['object_id']}'. Exception: {e}. Request: {request}"
            logger.exception(message, extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            })
            await log_activity(
                integration_id=integration.id,
                action_id="pull_events",
                level=LogLevel.WARNING,
                title=message,
                data={"message": message}
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


async def save_events_state(response, events, integration):
    for saved_event, event in zip(response, events):
        try:
            event_id = event["event_details"]["inat_id"]
            await state_manager.set_state(
                integration_id=str(integration.id),
                action_id="pull_events",
                state=saved_event,
                source_id=event_id
            )
        except Exception as e:
            message = f"Error while saving event ID '{event.get('event_id')}'. Exception: {e}."
            logger.exception(message, extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            })
            raise e


def _match_annotations_to_config(annotations: List[Annotation], config: Dict[int, List[int]]):
    annot_map = {}
    for annotation in annotations:
        if(annotation.term not in annot_map):
            annot_map[annotation.term] = []
        annot_map[annotation.term].append(annotation.value)

    for term, values in config:
        if(str(term) not in annot_map):
            return False
        for value in values:
            if(str(value) not in annot_map[str(term)]):
                return False
    
    return True


def _transform_inat_to_gundi_event(ob: Observation, config: PullEventsConfig):
    
    event = {
        "event_type": config.event_type,
        "recorded_at": ob.observed_on.replace(tzinfo=timezone.utc) if ob.observed_on else ob.created_at,
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

    if(ob.place_ids):
        event["event_details"]["place_ids"] = ",".join([str(int) for int in ob.place_ids])

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
        if(ob.taxon.ancestor_ids):
            event["event_details"]["taxon_ancestors"] = ",".join([str(int) for int in ob.taxon.ancestor_ids])

    if(not event.get("title")):
        event["title"] = "Unknown" if not ob.species_guess else ob.species_guess

    event["title"] = config.event_prefix + event["title"]

    return event