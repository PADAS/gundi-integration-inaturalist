import asyncio
from contextvars import ContextVar
from datetime import date, datetime, timedelta, timezone
from functools import wraps
import logging
import time
from typing import Dict, List, Optional
from uuid import uuid4

import httpx
import requests
from pyrate_limiter import BucketFullException
from gundi_core.schemas.v2 import Integration, LogLevel
from pyinaturalist import Observation

from gundi_client_v2.errors import GundiAPIError

from app import settings
from app.actions.configurations import (
    AddPhotosConfig,
    PullEventsConfig,
    ListProjectsQuery,
    ListAnnotationTermsQuery,
    ListAnnotationValuesQuery,
    ListTaxaQuery,
)
from app.actions import taxa_cache
from app.actions.core import ReferenceDataResponse, ReferenceOption, action_title
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger, ephemeral_run, log_action_activity
from app.services.gundi import (
    send_event_attachments_to_gundi,
    send_events_to_gundi,
    update_event_in_gundi,
)
from app.datasource.inaturalist import (
    INatRequestError,
    INatUnavailableError,
    _match_annotations_to_config,
    iter_observation_pages,
    bbox_to_search_circle,
    find_existing_projects,
    get_projects_by_values,
    list_controlled_terms,
    search_projects_near,
    search_taxa,
)
from app.services.state import IntegrationStateManager

GUNDI_SUBMISSION_CHUNK_SIZE = 100
# Per-observation state: photos still to add to its Gundi event, as [photo_id, url]
# pairs. pull_events writes the list when it creates the event; add_photos empties it.
STATE_PHOTOS_PENDING_KEY = "photos_pending"
# Held by the photo run working on an observation, so two runs never upload its photos twice.
STATE_PHOTOS_LOCK_PREFIX = "photos_lock."
# Seconds a photo run waits before trying failed photos again, while it has time left.
PHOTO_RETRY_WAIT_SECONDS = 30
# Photo runs in a row that add nothing: warnings until this many (about an hour), then errors.
PHOTO_RUNS_BEFORE_ERROR = 15
# Answers that mean a photo can't be added at all (iNat no longer has it, or the event is gone).
PHOTO_GONE_STATUSES = {403, 404, 410}
# Seconds one photo download may take, at most (less near the deadline).
PHOTO_DOWNLOAD_TIMEOUT_SECONDS = 120

# Pull-events state: single key for the cursor (legacy "updated_to" still read for backward compatibility)
STATE_LAST_RUN_KEY = "last_run"
STATE_DATETIME_FMT = "%Y-%m-%d %H:%M:%S%z"
# Per-observation state: when we last synced this observation to Gundi (so we only patch when it changes)
STATE_INAT_UPDATED_AT_KEY = "inat_updated_at"
# Per-integration run lock: held while a pull runs, expires if a run dies without releasing it.
# Each run writes its own token, so a run that outlives its lock can't release the next run's.
STATE_RUN_LOCK_SOURCE_ID = "run_lock"
RUN_LOCK_TTL_SECONDS = settings.MAX_ACTION_EXECUTION_TIME + 60
# Pulls in a row in which iNaturalist was down: warnings until this many, then an error.
STATE_INAT_DOWN_SOURCE_ID = "inat_down"
INAT_DOWN_RUNS_BEFORE_ERROR = 3
# The pull's own progress notices give up after this long, so a slow publisher
# can't hold a stopped run (and its lock) past the time limit by much.
PROGRESS_LOG_TIMEOUT_SECONDS = 5
# Kept free before the time limit when publishing the finished-pull summary.
SUMMARY_TIME_MARGIN_SECONDS = 2

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

async def _drop_unknown_projects(
        integration: Integration, projects: List[str], timeout: Optional[float] = None
) -> List[str]:
    """The saved projects that exist on iNaturalist, warning about the rest.

    iNat rejects an observations query whose projects are all unknown (422), so
    unknown projects are left out of the query. If the lookup itself fails, the
    projects are used as saved.
    """
    try:
        # pyinaturalist is synchronous: a thread keeps the other runs on this instance going.
        existing = await asyncio.wait_for(asyncio.to_thread(find_existing_projects, projects), timeout)
    except (requests.RequestException, BucketFullException, asyncio.TimeoutError) as e:
        # BucketFullException is pyinaturalist's rate-limit error, not a RequestException.
        logger.warning(f"Could not check the saved iNaturalist projects for integration ID: {integration.id}: {e!r}")
        return projects
    unknown = [p for p in projects if p not in existing]
    if unknown:
        if existing:
            msg = (f"These saved projects don't exist on iNaturalist, so the pull left them out: "
                   f"{', '.join(unknown)}.")
        else:
            msg = (f"The pull didn't run because none of the saved projects exist on iNaturalist "
                   f"({', '.join(unknown)}). Update the projects in the settings.")
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


async def _skip_unbounded_pull(integration: Integration):
    """Record that a pull with no project and no bounding box was refused."""
    msg = ("The pull didn't run because the settings have no project and no bounding box, "
           "so it would load observations from the whole world. Choose a project or draw a bounding box.")
    logger.error(f"{msg} Integration ID: {integration.id}.")
    try:
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_events",
            level=LogLevel.ERROR,
            title=msg,
        )
    except Exception as log_error:
        # Best-effort: the error is already in the logs.
        logger.warning(f"Could not publish the unbounded-pull error for integration ID: {integration.id}: {log_error}")


def chunk_list(list_a, chunk_size):
  for i in range(0, len(list_a), chunk_size):
    yield list_a[i:i + chunk_size]


def _plain_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 120:
        return _count(seconds, "second", "seconds")
    # Nearest minute: 8 min 59 s next to a 540 s limit should read as 9.
    # Half up, unlike round(), which sends 8.5 to 8.
    return f"{(seconds + 30) // 60} minutes"


# When the runner's clock for this run started, as near as the handler can see
# it: set before the activity logger publishes "started" and the lock is taken,
# both of which count against the time limit.
_run_started: ContextVar[Optional[float]] = ContextVar("inat_pull_run_started", default=None)


def _clock_run(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        token = _run_started.set(time.monotonic())
        try:
            return await func(*args, **kwargs)
        finally:
            _run_started.reset(token)
    return wrapper


def _capitalized(text: str) -> str:
    return text[:1].upper() + text[1:]


def _count(n: int, singular: str, plural: str) -> str:
    return f"{n} {singular if n == 1 else plural}"


class _PullProgress:
    """How far a pull got, so a run that is stopped can say where it was and
    whether the time went to iNaturalist or to Gundi."""

    def __init__(self):
        self.started = _run_started.get() or time.monotonic()
        self.step = "getting ready to read from iNaturalist"
        self.asked_inat = False
        self.inat_seconds: Optional[float] = None
        self.observations_read = 0
        self.events_sent = 0
        self.events_updated = 0
        self.photos_handed_off = 0
        # Set once the cursor for the next pull is saved: a stop after that
        # point (while publishing the summary) loses nothing.
        self.finished = False
        # Where the cursor was saved when the run reached the limit of new events.
        self.capped_at: Optional[datetime] = None
        self.chunks_done = 0

    def elapsed(self) -> float:
        return time.monotonic() - self.started

    def counts(self, at_least: bool = False) -> str:
        # A stopped run can't know about a batch that was in flight when it stopped.
        prefix = "at least " if at_least else ""
        text = (f"it sent {prefix}{_count(self.events_sent, 'new iNaturalist observation', 'new iNaturalist observations')} "
                f"to Gundi and updated {self.events_updated}")
        if self.photos_handed_off:
            text += f", and handed their {_count(self.photos_handed_off, 'photo', 'photos')} to a separate photo run"
        return text

    def inat_summary(self) -> str:
        if self.inat_seconds is not None:
            # Counted after the annotation filter, so "matching", not "sent". The
            # time includes the client's own rate limiting and parsing, hence
            # "reading from", not "iNaturalist took".
            found = _count(self.observations_read, "observation that matches", "observations that match")
            return (f"Reading from iNaturalist took {_plain_duration(self.inat_seconds)} "
                    f"and found {found} the settings")
        if self.asked_inat:
            return "Reading from iNaturalist hadn't finished"
        return "Reading from iNaturalist hadn't started"

    def data(self) -> dict:
        return {
            "step": self.step,
            "elapsed_seconds": round(self.elapsed()),
            "inaturalist_seconds": round(self.inat_seconds) if self.inat_seconds is not None else None,
            "observations_matched": self.observations_read,
            "events_sent": self.events_sent,
            "events_updated": self.events_updated,
            "photos_handed_off": self.photos_handed_off,
        }


async def _publish_progress_log(
        integration: Integration, title: str, level: LogLevel, data: dict, what: str,
        timeout: Optional[float] = None, action_id: str = "pull_events",
):
    """Best-effort and bounded: a slow publisher must not keep a pull running."""
    if timeout is None:
        timeout = PROGRESS_LOG_TIMEOUT_SECONDS
    if timeout <= 0:
        logger.info(f"Skipped the {what} for integration ID: {integration.id}: no time left before the limit.")
        return
    try:
        await asyncio.wait_for(
            log_action_activity(
                integration_id=integration.id,
                action_id=action_id,
                level=level,
                title=title,
                data=data,
            ),
            timeout=timeout,
        )
    except Exception as log_error:
        # asyncio.TimeoutError included. The runner records the outcome either way.
        logger.warning(f"Could not publish the {what} for integration ID: {integration.id}: {log_error!r}")


async def _log_pull_stopped(integration: Integration, progress: _PullProgress):
    """Say where a pull was when it was stopped (the runner's time limit, or the
    service shutting down). The runner logs the stop itself, without this detail."""
    msg = (f"The pull was cut off after {_plain_duration(progress.elapsed())} while {progress.step}. "
           f"Before that, {progress.counts(at_least=True)}. {progress.inat_summary()}.")
    logger.warning(f"{msg} Integration ID: {integration.id}.")
    await _publish_progress_log(integration, msg, LogLevel.WARNING, progress.data(), "stopped-pull notice")


async def _log_pull_finished(integration: Integration, progress: _PullProgress):
    """Counts in the title: the runner's own "completed" entry keeps them in its details."""
    msg = (f"Pull finished in {_plain_duration(progress.elapsed())}. {_capitalized(progress.counts())}. "
           f"{progress.inat_summary()}.")
    if progress.capped_at:
        msg += (f" It stopped after {settings.INAT_MAX_NEW_EVENTS_PER_PULL} new observations, the most one pull "
                f"sends. Observations changed on iNaturalist up to {_utc_minute(progress.capped_at)} UTC are in Gundi.")
    # Only the time left before the limit: the summary must never turn a pull
    # whose work is saved into a timeout that gets retried.
    time_left = settings.MAX_ACTION_EXECUTION_TIME - progress.elapsed() - SUMMARY_TIME_MARGIN_SECONDS
    await _publish_progress_log(
        integration, msg, LogLevel.INFO, progress.data(), "pull summary",
        timeout=min(PROGRESS_LOG_TIMEOUT_SECONDS, time_left),
    )


async def _log_pull_paused(integration: Integration, progress: _PullProgress, done_through: Optional[datetime]):
    """The pull stopped taking new work to stay inside the time limit. Info
    while it gets further each run; an error when it got nowhere, so a route
    that can't move shows as unhealthy."""
    if progress.chunks_done:
        msg = (f"Pull stopped after {_plain_duration(progress.elapsed())} so it would finish within its time "
               f"limit. Observations changed on iNaturalist up to {_utc_minute(done_through)} UTC are in Gundi: "
               f"{progress.counts()}. {progress.inat_summary()}.")
        level = LogLevel.INFO
    else:
        msg = (f"The pull didn't send anything to Gundi because it ran out of time while {progress.step}, "
               f"before any were ready to send.")
        level = LogLevel.ERROR
    log = logger.info if level == LogLevel.INFO else logger.error
    log(f"{msg} Integration ID: {integration.id}.")
    time_left = settings.MAX_ACTION_EXECUTION_TIME - progress.elapsed() - SUMMARY_TIME_MARGIN_SECONDS
    await _publish_progress_log(
        integration, msg, level, progress.data(), "paused-pull notice",
        timeout=min(PROGRESS_LOG_TIMEOUT_SECONDS, time_left),
    )


def _utc_minute(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")


async def _run_pull(integration: Integration, action_config: PullEventsConfig):
    progress = _PullProgress()
    try:
        return await _pull_events(integration, action_config, progress)
    except asyncio.CancelledError:
        # Once the cursor is saved, nothing was lost: only the summary was cut off.
        if not progress.finished:
            await _log_pull_stopped(integration, progress)
        raise


@action_title("Pull iNaturalist Observations")
@_clock_run
@activity_logger()
async def action_pull_events(integration: Integration, action_config: PullEventsConfig):
    # A redelivered run (transient failures are nacked) can overlap the next scheduled
    # run for the same integration, so only one pull runs per integration at a time.
    # Ephemeral runs persist no state, so they skip the lock.
    integration_id = str(integration.id)
    if ephemeral_run.get():
        return await _run_pull(integration, action_config)
    lock_token = uuid4().hex
    acquired = await state_manager.set_if_absent(
        integration_id, "pull_events", ttl_seconds=RUN_LOCK_TTL_SECONDS, source_id=STATE_RUN_LOCK_SOURCE_ID,
        value=lock_token,
    )
    if not acquired:
        msg = "The pull didn't run because the previous pull for this connection is still running."
        logger.info(f"{msg} Integration ID: {integration_id}.")
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
        return await _run_pull(integration, action_config)
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


async def _pull_events(integration: Integration, action_config: PullEventsConfig, progress: _PullProgress):

    # Log the id only: the Integration carries every action config, including
    # the auth row's api_key.
    logger.info(f"Executing 'pull_events' action with integration {integration.id} and action_config {action_config}...")

    if not action_config.projects and not action_config.bounding_box:
        # The config validator already rejects this; this guards configs that
        # bypass it, since the query would pull every observation worldwide.
        await _skip_unbounded_pull(integration)
        return _pull_result(0, 0, 0)

    state = await state_manager.get_state(integration.id, "pull_events")
    load_since = _get_load_since(state, action_config.days_to_load)

    # The budget covers the iNaturalist calls too: a thread can't be stopped,
    # but the run stops waiting for it.
    deadline = progress.started + _pull_time_budget()

    projects = action_config.projects
    if projects:
        progress.step = "checking the saved projects on iNaturalist"
        progress.asked_inat = True
        projects = await _drop_unknown_projects(integration, projects, timeout=_time_left(deadline))
        if not projects:
            # Pulling without the projects would widen the query to the whole
            # bounding box (or the world), so skip until the config is fixed.
            return _pull_result(0, 0, 0)

    progress.asked_inat = True
    annotations = action_config.annotations_dict
    pages = iter_observation_pages(
        load_since,
        bounding_box=action_config.bounding_box,
        taxa=action_config.taxa_str,
        projects=projects,
        quality_grade=action_config.quality_grade,
        annotations=annotations,
    )
    max_new = settings.INAT_MAX_NEW_EVENTS_PER_PULL
    # Photos are added by a separate photo run, so chunks only create and update events.
    chunk_size = GUNDI_SUBMISSION_CHUNK_SIZE
    # Page by page, oldest change first: each page is sent in chunks and the
    # cursor saved after each one, until max_new new observations, the end of
    # what changed, or the time budget.
    seen = set()  # pages at iNat's 10,000-result window boundaries repeat observations
    new_found = added_count = updated_count = attachment_count = 0
    done_through = None
    read_all = stopped_on_time = False
    try:
        while new_found < max_new:
            if time.monotonic() >= deadline:
                stopped_on_time = True
                break
            progress.step = "reading observations from iNaturalist"
            read_started = time.monotonic()
            try:
                # pyinaturalist is synchronous: a thread keeps the other runs on
                # this instance going, and lets a stop land while a page is read.
                page = await asyncio.wait_for(asyncio.to_thread(next, pages, None), _time_left(deadline))
            except asyncio.TimeoutError:
                # Our time budget ran out, not iNaturalist: logged as a stop on time.
                stopped_on_time = True
                break
            # Pages that came back only, so an unfinished read says so.
            progress.inat_seconds = (progress.inat_seconds or 0) + time.monotonic() - read_started
            if page is None:
                read_all = True
                break

            batch, new_ids, patch_targets = [], set(), {}
            progress.step = "checking which observations are new or changed"
            for ob in page:
                if ob.id in seen:
                    continue
                seen.add(ob.id)
                batch.append(ob)
                if not annotations or _match_annotations_to_config(ob.annotations, annotations):
                    progress.observations_read += 1
                    await _sort_observation(integration, ob, new_ids, patch_targets)
                if new_found + len(new_ids) >= max_new:
                    break
            new_found += len(new_ids)

            for chunk in chunk_list(batch, chunk_size):
                to_add = [ob for ob in chunk if ob.id in new_ids]
                to_patch = [(patch_targets[ob.id], ob) for ob in chunk if ob.id in patch_targets]
                # A chunk with nothing to send only moves the cursor, so it never waits on the time.
                if (to_add or to_patch) and time.monotonic() >= deadline:
                    stopped_on_time = True
                    break
                if to_add:
                    added, photos = await _send_new_observations(integration, action_config, to_add, progress)
                    added_count += added
                    attachment_count += photos
                if to_patch:
                    progress.step = "updating changed observations in Gundi"
                    response = await patch_events(to_patch, action_config, integration, progress)
                    updated_count += len(response)
                    progress.step = "saving which observations were updated"
                    await save_patched_events_state(to_patch, integration)
                done_through = chunk[-1].updated_at
                progress.chunks_done += 1
                # Inclusive: the next pull reads from this time again and skips what was saved.
                progress.step = "saving where the next pull starts"
                await state_manager.set_state(
                    str(integration.id), "pull_events", _build_pull_events_state(done_through)
                )
            if stopped_on_time:
                break
    except (INatUnavailableError, INatRequestError) as e:
        if not _inat_is_down(e):
            raise
        # What was sent before the read failed is saved, cursor included.
        await _handle_inat_down(integration, e)
        return _pull_result(added_count, updated_count, attachment_count)
    await _clear_inat_down(integration)
    progress.finished = True

    if read_all and not progress.observations_read:
        since = load_since.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
        # "Matching": the annotation filter may have dropped what iNaturalist sent.
        msg = f"No observations matching the settings were added or changed on iNaturalist since {since} UTC."
        logger.info(f"{msg} Integration ID: {integration.id}.")
        # Info, not warning: a quiet window is a normal outcome.
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_events",
            level=LogLevel.INFO,
            title=msg,
            data={"message": msg}
        )
        # Advance cursor so next run doesn't re-query the same window (avoids repeated heavy requests)
        now = datetime.now(tz=timezone.utc)
        await state_manager.set_state(
            str(integration.id), "pull_events", _build_pull_events_state(now)
        )
        return _pull_result(0, 0, 0)

    if stopped_on_time:
        await _log_pull_paused(integration, progress, done_through)
    else:
        if not read_all:
            progress.capped_at = done_through
        await _log_pull_finished(integration, progress)

    return _pull_result(added_count, updated_count, attachment_count)


def _pull_result(added: int, updated: int, photos: int) -> dict:
    # Gundi monitoring reads these numbers.
    return {'result': {'events_extracted': added,
                       'events_updated': updated,
                       'photos_attached': photos}}


def _time_left(deadline: float) -> float:
    return max(deadline - time.monotonic(), 0)


def _pull_time_budget() -> float:
    """Seconds a pull may spend before it stops taking new chunks: the tighter
    of the runner's and Cloud Run's limits, less a margin for the chunk in
    flight and the summary."""
    limit = min(settings.MAX_ACTION_EXECUTION_TIME, settings.INAT_PULL_TIME_LIMIT_SECONDS)
    return limit - settings.INAT_PULL_TIME_MARGIN_SECONDS


async def _sort_observation(integration: Integration, observation: Observation, new_ids: set, patch_targets: dict):
    """Add the observation's id to new_ids when it isn't in Gundi yet, or to
    patch_targets ({id: Gundi object_id}) when it changed since it was last
    synced. An observation already in sync goes in neither."""
    saved_event = await state_manager.get_state(str(integration.id), "pull_events", str(observation.id))
    if not saved_event:
        new_ids.add(observation.id)
        return
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
                return  # Already in sync, skip patch
        except (ValueError, TypeError):
            pass  # Bad or legacy value, patch to be safe
    patch_targets[observation.id] = saved_event.get("object_id")


async def _send_new_observations(integration, action_config, observations, progress: _PullProgress):
    """Send one chunk of new observations and save their state, listing each
    one's photos as pending; then start a photo run to add them. Recording the
    event before any photo work means a stop never sends it twice."""
    events = []
    pending_photos = {}  # inat_id -> [[photo_id, url], ...]
    inat_updated_at_map = {}  # inat_id -> updated_at for state we persist after create
    for ob in observations:
        e = _transform_inat_to_gundi_event(ob, action_config)
        events.append(e)
        inat_id = e['event_details']['inat_id']
        inat_updated_at_map[inat_id] = ob.updated_at
        if action_config.include_photos:
            pending_photos[inat_id] = [
                [photo.id, photo.large_url if photo.large_url else photo.url] for photo in ob.photos
            ]

    logger.info(f"Submitting {len(events)} iNaturalist observations to Gundi")
    progress.step = "sending new observations to Gundi"
    response = await send_events_to_gundi(events=events, integration_id=str(integration.id))
    progress.events_sent += len(response)
    if not response:
        return 0, 0
    progress.step = "saving which observations were sent"
    await save_events_state(response, events, integration, inat_updated_at_map, pending_photos)
    with_photos = [e['event_details']['inat_id'] for e in events[:len(response)]
                   if pending_photos.get(e['event_details']['inat_id'])]
    if with_photos and not ephemeral_run.get():
        progress.step = "starting a photo run for the new observations"
        await trigger_action(str(integration.id), "add_photos", AddPhotosConfig(observation_ids=with_photos))
        progress.photos_handed_off += sum(len(pending_photos[i]) for i in with_photos)
    return len(response), 0


def _inat_is_down(error: Exception) -> bool:
    """iNaturalist didn't answer, or answered with a server error (5xx): its
    side, and usually short-lived. A 4xx is a problem with the query."""
    if isinstance(error, INatUnavailableError):
        return True
    response = getattr(error, "response", None)
    return response is not None and response.status_code >= 500


async def _handle_inat_down(integration: Integration, error: Exception):
    """A warning while iNaturalist is down for a run or two; an error (so the
    route shows as unhealthy) from the INAT_DOWN_RUNS_BEFORE_ERROR-th pull in a row."""
    saved = await state_manager.get_state(str(integration.id), "pull_events", STATE_INAT_DOWN_SOURCE_ID)
    runs = (saved or {}).get("runs", 0) + 1
    await state_manager.set_state(
        str(integration.id), "pull_events", {"runs": runs}, source_id=STATE_INAT_DOWN_SOURCE_ID
    )
    reason = getattr(error, "message", None) or str(error)
    if runs >= INAT_DOWN_RUNS_BEFORE_ERROR:
        # Logged here rather than raised: a raised 5xx would be redelivered and
        # run again at once, adding load while iNaturalist is struggling.
        msg = (f"The pull didn't get observations from iNaturalist because {reason}. "
               f"This is pull {runs} in a row with a problem on iNaturalist's side.")
        level = LogLevel.ERROR
    else:
        msg = f"The pull didn't get observations from iNaturalist because {reason}."
        level = LogLevel.WARNING
    log = logger.error if level == LogLevel.ERROR else logger.warning
    log(f"{msg} Integration ID: {integration.id}.")
    await _publish_progress_log(
        integration, msg, level, {"reason": reason, "pulls_in_a_row": runs}, "iNaturalist outage notice"
    )


async def _clear_inat_down(integration: Integration):
    saved = await state_manager.get_state(str(integration.id), "pull_events", STATE_INAT_DOWN_SOURCE_ID)
    if saved and saved.get("runs"):
        await state_manager.delete_state(str(integration.id), "pull_events", STATE_INAT_DOWN_SOURCE_ID)


class _PhotoGone(Exception):
    """The photo can't be added at all: iNat no longer has it, or the Gundi event is gone."""


async def _add_photo(integration: Integration, gundi_id: str, photo_id, url: str, deadline: float):
    """Download one photo and attach it to the Gundi event. Raises _PhotoGone
    when it can never be added, and anything else when a later try may work."""
    filename = f"{photo_id}.{url.split('.')[-1]}"
    timeout = max(min(PHOTO_DOWNLOAD_TIMEOUT_SECONDS, deadline - time.monotonic()), 1)
    logger.info(f"Adding {url} to Gundi event {gundi_id}")
    async with httpx.AsyncClient(timeout=timeout, verify=False) as session:
        image_response = await session.get(url)
    if image_response.status_code in PHOTO_GONE_STATUSES:
        raise _PhotoGone("iNaturalist no longer has it")
    image_response.raise_for_status()
    try:
        await send_event_attachments_to_gundi(
            event_id=gundi_id, attachments=[(filename, image_response.content)], integration_id=str(integration.id)
        )
    except GundiAPIError as e:
        if getattr(e, "status_code", None) in PHOTO_GONE_STATUSES:
            raise _PhotoGone("its event is no longer in Gundi") from e
        raise


class _PhotoRun:
    """What one photo run did, for its log entry."""

    def __init__(self):
        self.started = _run_started.get() or time.monotonic()
        self.added = 0
        self.gone = 0
        self.observations_done = 0
        self.last_problem: Optional[str] = None


async def _add_pending_photos(integration: Integration, obs_id: str, deadline: float, run: _PhotoRun) -> bool:
    """Add one observation's pending photos, saving after each. True when none
    are left; False when time ran out or a photo failed (it stays pending)."""
    integration_id = str(integration.id)
    note = await state_manager.get_state(integration_id, "pull_events", obs_id)
    pending = list((note or {}).get(STATE_PHOTOS_PENDING_KEY) or [])
    while pending:
        if time.monotonic() >= deadline:
            return False
        photo_id, url = pending[0]
        try:
            await _add_photo(integration, note["object_id"], photo_id, url, deadline)
            run.added += 1
        except _PhotoGone as e:
            logger.warning(f"Photo {photo_id} for iNaturalist observation {obs_id} wasn't added because {e}.")
            run.gone += 1
        except Exception as e:
            logger.warning(f"Photo {photo_id} for iNaturalist observation {obs_id} wasn't added yet: {e!r}")
            run.last_problem = _plain_problem(e)
            return False
        pending.pop(0)
        note[STATE_PHOTOS_PENDING_KEY] = pending
        await state_manager.set_state(integration_id, "pull_events", note, source_id=obs_id)
    run.observations_done += 1
    return True


def _plain_problem(error: Exception) -> str:
    """Why a photo couldn't be added this time, in plain words."""
    if isinstance(error, httpx.TimeoutException):
        return "a photo download from iNaturalist didn't finish in time"
    if isinstance(error, httpx.TransportError):
        return "the connection to iNaturalist's photo server dropped"
    if isinstance(error, httpx.HTTPStatusError):
        return f"iNaturalist's photo server answered with an error ({error.response.status_code})"
    if isinstance(error, GundiAPIError):
        return "Gundi didn't accept a photo"
    return "a photo couldn't be added"


@action_title("Add iNaturalist Photos")
@_clock_run
@activity_logger()
async def action_add_photos(integration: Integration, action_config: AddPhotosConfig):
    """Add the pending photos of the given observations' Gundi events, until the
    time budget; then start another photo run for whatever is left."""
    integration_id = str(integration.id)
    run = _PhotoRun()
    deadline = run.started + _pull_time_budget()
    left = list(action_config.observation_ids)
    while left and time.monotonic() < deadline:
        failed = []
        for index, obs_id in enumerate(left):
            if time.monotonic() >= deadline:
                failed.extend(left[index:])
                break
            # Another photo run (a repeated message) may be on this observation: leave it to that run.
            lock_token = uuid4().hex
            lock_id = STATE_PHOTOS_LOCK_PREFIX + obs_id
            if not await state_manager.set_if_absent(
                    integration_id, "pull_events", ttl_seconds=RUN_LOCK_TTL_SECONDS, source_id=lock_id,
                    value=lock_token):
                continue
            try:
                if not await _add_pending_photos(integration, obs_id, deadline, run):
                    failed.append(obs_id)
            finally:
                await state_manager.delete_state_if_value(integration_id, "pull_events", lock_token, source_id=lock_id)
        left = failed
        if left and time.monotonic() + PHOTO_RETRY_WAIT_SECONDS < deadline and run.last_problem:
            await asyncio.sleep(PHOTO_RETRY_WAIT_SECONDS)

    made_progress = bool(run.added or run.gone)
    runs_without_progress = 0 if made_progress else action_config.runs_without_progress + 1
    if left:
        await trigger_action(integration_id, "add_photos", AddPhotosConfig(
            observation_ids=left, runs_without_progress=runs_without_progress))
    await _log_photo_run(integration, run, left, runs_without_progress)
    return {"result": {"photos_attached": run.added, "photos_not_available": run.gone,
                       "observations_left": len(left)}}


async def _log_photo_run(integration: Integration, run: _PhotoRun, left: List[str], runs_without_progress: int):
    took = _plain_duration(time.monotonic() - run.started)
    msg = f"Added {_count(run.added, 'photo', 'photos')} to iNaturalist observations in Gundi in {took}."
    if run.gone:
        msg += (f" {run.gone} of the photos couldn't be added because iNaturalist no longer has them "
                f"or their event is no longer in Gundi.")
    level = LogLevel.INFO
    if left:
        still = f"Photos for {_count(len(left), 'observation are', 'observations are')} still to add"
        still = still.replace("1 observation are", "1 observation is")
        msg += f" {still}, and another photo run was started for them."
        if runs_without_progress:
            problem = run.last_problem or "the time ran out"
            msg = (f"No photos were added to iNaturalist observations in Gundi because {problem}. "
                   f"{still}, and another photo run was started for them.")
            level = LogLevel.WARNING
            if runs_without_progress >= PHOTO_RUNS_BEFORE_ERROR:
                msg += f" This is photo run {runs_without_progress} in a row that couldn't add any."
                level = LogLevel.ERROR
    log = {LogLevel.INFO: logger.info, LogLevel.WARNING: logger.warning}.get(level, logger.error)
    log(f"{msg} Integration ID: {integration.id}.")
    await _publish_progress_log(integration, msg, level, {
        "photos_added": run.added, "photos_not_available": run.gone,
        "observations_done": run.observations_done, "observations_left": len(left),
        "runs_without_progress": runs_without_progress,
    }, "photo run summary", action_id="add_photos")


async def patch_events(events, updated_config_data, integration, progress: Optional[_PullProgress] = None):
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
            if progress:
                progress.events_updated += 1
    return responses


async def save_events_state(response, events, integration, inat_updated_at_map=None, pending_photos=None):
    """Persist Gundi event state per observation so we know what we created and when we last synced,
    and which of its photos are still to add."""
    inat_updated_at_map = inat_updated_at_map or {}
    pending_photos = pending_photos or {}
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
            if pending_photos.get(event_id):
                state[STATE_PHOTOS_PENDING_KEY] = pending_photos[event_id]
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
            # Keep the rest of the note: its photos may still be pending.
            saved = await state_manager.get_state(str(integration.id), "pull_events", str(observation.id))
            state = {
                **(saved or {}),
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

    With `ids` (the annotation's resolve param) it returns those projects
    instead, in the order and spelling asked (a saved slug stays the option's
    value), so the portal can label saved values the nearest-first page misses.
    """
    if action_config.ids:
        # Blocking (requests, plus a rate limiter that sleeps): off the event loop.
        found = await asyncio.to_thread(get_projects_by_values, action_config.ids)
        options = [
            ReferenceOption(value=value, label=found[value].get("title") or value)
            for value in action_config.ids
            if value in found
        ]
        return ReferenceDataResponse(options=options, truncated=False).dict()
    if action_config.bounding_box is None:
        return ReferenceDataResponse(options=[], truncated=False).dict()
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


def _taxon_option(taxon: dict) -> ReferenceOption:
    scientific = taxon.get("name") or str(taxon["id"])
    common = taxon.get("preferred_common_name")
    return ReferenceOption(
        value=str(taxon["id"]),
        label=f"{common} ({scientific})" if common else scientific,
        description=taxon.get("rank"),
    )


async def action_list_taxa(integration: Integration, action_config: ListTaxaQuery):
    """Reference action (typeahead): taxa matching the typed query.

    The taxa vocabulary is far too large for a default page, so an empty query
    returns no options with truncated=True — the portal's search widget only
    fetches once the operator has typed, and old widgets get a clean empty list.

    With `ids` (the annotation's resolve param) it returns those taxa instead,
    in the order asked, so the portal can label saved values. IDs iNat doesn't
    know are left out; the portal keeps showing them as raw values.
    """
    if action_config.ids:
        by_id = await taxa_cache.resolve_taxa(action_config.ids, taxa_cache.default_cache())
        options = [_taxon_option(by_id[i]) for i in action_config.ids if i in by_id]
        return ReferenceDataResponse(options=options, truncated=False).dict()
    query = (action_config.q or "").strip()
    if not query:
        return ReferenceDataResponse(options=[], truncated=True).dict()
    # search_taxa blocks (requests, plus a rate limiter that sleeps), so keep it
    # off the event loop: the portal calls this on every keystroke.
    response = await asyncio.to_thread(search_taxa, query)
    results = response.get("results", [])
    options = [_taxon_option(taxon) for taxon in results if taxon.get("id") is not None]
    truncated = response.get("total_results", len(results)) > len(results)
    return ReferenceDataResponse(options=options, truncated=truncated).dict()