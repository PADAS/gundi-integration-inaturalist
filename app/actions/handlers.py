import asyncio
from contextvars import ContextVar
from datetime import date, datetime, timedelta, timezone
from functools import wraps
import hashlib
import json
import logging
import time
from typing import Dict, List, Optional
from uuid import uuid4

import httpx
import pydantic
import requests
from pyrate_limiter import BucketFullException
from gundi_core.schemas.v2 import Integration, LogLevel
from pyinaturalist import Observation

from gundi_client_v2.errors import GundiAPIError

from app import settings
from app.actions.configurations import (
    AddPhotosConfig,
    BackfillContinueConfig,
    BackfillEventsConfig,
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
    iter_upload_pages,
    bbox_to_search_circle,
    find_existing_projects,
    get_projects_by_values,
    list_controlled_terms,
    search_projects_near,
    search_taxa,
)
from app.services.state import IntegrationStateManager
from app.services.utils import find_config_for_action

GUNDI_SUBMISSION_CHUNK_SIZE = 100
# Per-observation photo record, apart from the observation's own state so the two
# writers never overwrite each other: {"object_id", "pending": [[photo_id, url], ...]}.
# pull_events writes it when it creates the event; only add_photos changes it after that.
STATE_PHOTOS_PREFIX = "photos."
# Held by the photo run working on an observation, so two runs never upload its photos twice.
STATE_PHOTOS_LOCK_PREFIX = "photos_lock."
# Seconds a photo run waits before trying failed photos again, while it has time left.
PHOTO_RETRY_WAIT_SECONDS = 30
# Photo runs in a row that add nothing: warnings until this many (about an hour), then errors.
PHOTO_RUNS_BEFORE_ERROR = 15
# Answers that mean a photo can't be added at all. iNaturalist's photo storage answers
# 403 for a photo that no longer exists; for Gundi only a missing event counts.
INAT_PHOTO_GONE_STATUSES = {403, 404, 410}
GUNDI_EVENT_GONE_STATUSES = {404, 410}
# Seconds a photo run keeps before the hard limit, so an upload (which retries on
# its own) never runs past it.
PHOTO_HARD_LIMIT_MARGIN_SECONDS = 10
# Seconds one photo download may take, at most (less near the deadline).
PHOTO_DOWNLOAD_TIMEOUT_SECONDS = 120

# Pull-events state: single key for the cursor (legacy "updated_to" still read for backward compatibility)
STATE_LAST_RUN_KEY = "last_run"
STATE_DATETIME_FMT = "%Y-%m-%d %H:%M:%S%z"
# Per-observation state: the observation's updated_at when it was last sent (kept for reference;
# whether to patch is decided by STATE_SENT_HASH_KEY)
STATE_INAT_UPDATED_AT_KEY = "inat_updated_at"
# Per-observation state: hash of the event payload last sent, so we only patch when what we send changed
STATE_SENT_HASH_KEY = "sent_hash"
# Per-observation state written just before a new event is sent and replaced once Gundi accepts it.
# A row that still holds only this was being sent when a run was cut off: it may be in Gundi already,
# so it is never sent again (no duplicate events).
STATE_SENDING_KEY = "sending_since"
# Set by a pull that found the run lock taken; a backfill holding the lock hands over when it sees it.
STATE_PULL_WAITING_SOURCE_ID = "pull_waiting"
# Per-backfill progress, keyed by the backfill's window end: {"batch_index", "after_id", "created_so_far"}.
STATE_BACKFILL_PREFIX = "backfill."
# The most recently started backfill's settings and window, so a repeated start continues it.
STATE_BACKFILL_CURRENT = "backfill_current"
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
    """The datetime to use for updated_since: the stored cursor, but never
    further back than fallback_days, so a connection that fell behind reads
    recent changes instead of crawling months of old ones."""
    floor = datetime.now(tz=timezone.utc) - timedelta(days=fallback_days)
    raw = state.get(STATE_LAST_RUN_KEY) or state.get("updated_to")
    if not raw:
        return floor
    cursor = datetime.strptime(raw, STATE_DATETIME_FMT)
    if cursor.tzinfo is None:
        cursor = cursor.replace(tzinfo=timezone.utc)
    return max(cursor, floor)


def _stored_cursor(state: dict) -> Optional[datetime]:
    raw = (state or {}).get(STATE_LAST_RUN_KEY) or (state or {}).get("updated_to")
    if not raw:
        return None
    try:
        return datetime.strptime(raw, STATE_DATETIME_FMT)
    except (TypeError, ValueError):
        return None


def _build_pull_events_state(last_updated: datetime) -> dict:
    """Build the state dict to persist after a pull_events run."""
    return {STATE_LAST_RUN_KEY: last_updated.strftime(STATE_DATETIME_FMT)}

async def _drop_unknown_projects(
        integration: Integration, projects: List[str], timeout: Optional[float] = None,
        activity: str = "pull", action_id: str = "pull_events",
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
            msg = (f"These saved projects don't exist on iNaturalist, so the {activity} left them out: "
                   f"{', '.join(unknown)}.")
        else:
            msg = (f"The {activity} didn't run because none of the saved projects exist on iNaturalist "
                   f"({', '.join(unknown)}). Update the projects in the settings.")
        logger.warning(f"{msg} Integration ID: {integration.id}.")
        try:
            await log_action_activity(
                integration_id=integration.id,
                action_id=action_id,
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

    def __init__(self, max_age_days: int = 90, max_new_events: Optional[int] = None):
        self.started = _run_started.get() or time.monotonic()
        self.max_age_days = max_age_days
        self.max_new_events = max_new_events or settings.INAT_MAX_NEW_EVENTS_PER_PULL
        self.step = "getting ready to read from iNaturalist"
        self.asked_inat = False
        self.inat_seconds: Optional[float] = None
        self.observations_read = 0
        self.events_sent = 0
        self.events_updated = 0
        self.photos_handed_off = 0
        self.photos_left_out = 0
        self.skipped_too_old = 0
        self.skipped_unchanged = 0
        self.skipped_unconfirmed = 0
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
        skipped = []
        if self.skipped_too_old:
            skipped.append(f"{self.skipped_too_old} uploaded more than {self.max_age_days} days ago")
        if self.skipped_unchanged:
            skipped.append(f"{self.skipped_unchanged} with nothing changed")
        if self.skipped_unconfirmed:
            skipped.append(f"{self.skipped_unconfirmed} that an interrupted run may already have sent")
        if skipped:
            text += ", and skipped " + " and ".join(skipped)
        if self.photos_handed_off:
            text += f", and handed their {_count(self.photos_handed_off, 'photo', 'photos')} to a separate photo run"
            if self.photos_left_out:
                text += f" ({self.photos_left_out} left out by the photo limit)"
        elif self.photos_left_out:
            text += f", and left out their {_count(self.photos_left_out, 'photo', 'photos')} because of the photo limit"
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
            "photos_left_out": self.photos_left_out,
            "events_skipped_too_old": self.skipped_too_old,
            "events_skipped_unchanged": self.skipped_unchanged,
            "events_skipped_unconfirmed": self.skipped_unconfirmed,
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
        msg += (f" It stopped after {progress.max_new_events} new observations, the most one pull "
                f"sends. Observations changed on iNaturalist up to {_utc_minute(progress.capped_at)} UTC were checked.")
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
               f"limit. Observations changed on iNaturalist up to {_utc_minute(done_through)} UTC were checked: "
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
    progress = _PullProgress(action_config.max_age_days, action_config.max_new_events_per_run)
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
        try:
            # A backfill holding the lock checks this and hands over early, so pulls don't starve.
            await state_manager.set_if_absent(
                integration_id, "pull_events", ttl_seconds=RUN_LOCK_TTL_SECONDS,
                source_id=STATE_PULL_WAITING_SOURCE_ID,
            )
        except Exception:
            logger.exception(f"Could not leave the pull-waiting note for integration ID: {integration_id}.")
        msg = "The pull didn't run because the previous pull or a backfill for this connection is still running."
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
        return _pull_result(0, 0, 0)
    try:
        await state_manager.delete_state(integration_id, "pull_events", source_id=STATE_PULL_WAITING_SOURCE_ID)
    except Exception:
        logger.exception(f"Could not clear the pull-waiting note for integration ID: {integration_id}.")
    try:
        return await _run_pull(integration, action_config)
    finally:
        try:
            # Any pull that was skipped while this one ran is covered by it.
            await state_manager.delete_state(integration_id, "pull_events", source_id=STATE_PULL_WAITING_SOURCE_ID)
        except Exception:
            logger.exception(f"Could not clear the pull-waiting note for integration ID: {integration_id}.")
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
    # For cursor_lag_days when this run saves no cursor of its own: the stored
    # one, not load_since, whose days_to_load floor would hide how far behind it is.
    previous_cursor = _stored_cursor(state)

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
    max_new = action_config.max_new_events_per_run
    now = datetime.now(tz=timezone.utc)
    # Photos are added by a separate photo run, so chunks only create and update events.
    chunk_size = GUNDI_SUBMISSION_CHUNK_SIZE
    # Page by page, oldest change first: each page is sent in chunks and the
    # cursor saved after each one, until max_new new observations, the end of
    # what changed, or the time budget.
    seen = set()  # pages at iNat's 10,000-result window boundaries repeat observations
    new_found = added_count = updated_count = attachment_count = 0
    done_through = None
    read_all = stopped_on_time = False
    gundi_partial = 0  # new observations of a chunk that Gundi didn't accept
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

            batch, new_ids, patch_targets, photos_left_behind = [], set(), {}, set()
            outcomes = {}  # id -> what _sort_observation decided, counted once its chunk is saved
            progress.step = "checking which observations are new or changed"
            for ob in page:
                if ob.id in seen:
                    continue
                seen.add(ob.id)
                batch.append(ob)
                if not annotations or _match_annotations_to_config(ob.annotations, annotations):
                    progress.observations_read += 1
                    outcomes[ob.id] = await _sort_observation(integration, action_config, ob, new_ids,
                                                              patch_targets, photos_left_behind, now=now)
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
                    if added < len(to_add):
                        # The cursor stays before this chunk: the next pull sends the ones Gundi didn't take.
                        gundi_partial = len(to_add) - added
                        break
                for item in to_patch:
                    # One at a time, each saved as it lands: a chunk can hold 100 updates.
                    if time.monotonic() >= deadline:
                        stopped_on_time = True
                        break
                    progress.step = "updating changed observations in Gundi"
                    response = await patch_events([item], action_config, integration, progress)
                    updated_count += len(response)
                    progress.step = "saving which observations were updated"
                    await save_patched_events_state([item], integration, action_config)
                if stopped_on_time:
                    break
                to_resume = [ob.id for ob in chunk if ob.id in photos_left_behind]
                if to_resume and not ephemeral_run.get():
                    # Their photo run was never started, or stopped for good: start one.
                    progress.step = "starting a photo run for observations still missing photos"
                    await trigger_action(str(integration.id), "add_photos",
                                         AddPhotosConfig(observation_ids=[str(i) for i in to_resume]))
                done_through = chunk[-1].updated_at
                progress.chunks_done += 1
                # Inclusive: the next pull reads from this time again and skips what was saved.
                progress.step = "saving where the next pull starts"
                await state_manager.set_state(
                    str(integration.id), "pull_events", _build_pull_events_state(done_through)
                )
                # Counted with the cursor: a chunk that is read again next run isn't counted twice.
                chunk_outcomes = [outcomes.get(ob.id) for ob in chunk]
                progress.skipped_too_old += chunk_outcomes.count("too_old")
                progress.skipped_unchanged += chunk_outcomes.count("unchanged")
                progress.skipped_unconfirmed += chunk_outcomes.count("unconfirmed")
            if stopped_on_time or gundi_partial:
                break
    except (INatUnavailableError, INatRequestError) as e:
        if not _inat_is_down(e):
            raise
        # What was sent before the read failed is saved, cursor included.
        await _handle_inat_down(integration, e)
        return _pull_result(added_count, updated_count, attachment_count, progress, done_through or previous_cursor)
    await _clear_inat_down(integration)
    progress.finished = True

    if gundi_partial:
        msg = (f"Gundi didn't accept {_count(gundi_partial, 'new observation', 'new observations')} of the last "
               f"batch, so the pull stopped there; the next pull sends them again. Before that, {progress.counts()}.")
        logger.warning(f"{msg} Integration ID: {integration.id}.")
        await _publish_progress_log(integration, msg, LogLevel.WARNING, progress.data(), "partial-send notice")
        return _pull_result(added_count, updated_count, attachment_count, progress, done_through or previous_cursor)

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
        return _pull_result(0, 0, 0, progress, now)

    if stopped_on_time:
        await _log_pull_paused(integration, progress, done_through)
    else:
        if not read_all:
            progress.capped_at = done_through
        await _log_pull_finished(integration, progress)

    return _pull_result(added_count, updated_count, attachment_count, progress, done_through or previous_cursor)


def _pull_result(added: int, updated: int, photos: int, progress: Optional["_PullProgress"] = None,
                 cursor: Optional[datetime] = None) -> dict:
    # Gundi monitoring reads the first three numbers.
    lag = None
    if cursor is not None:
        if cursor.tzinfo is None:
            cursor = cursor.replace(tzinfo=timezone.utc)
        lag = round((datetime.now(tz=timezone.utc) - cursor).total_seconds() / 86400, 1)
    return {'result': {'events_extracted': added,
                       'events_updated': updated,
                       'photos_attached': photos,
                       'events_skipped_too_old': progress.skipped_too_old if progress else 0,
                       'events_skipped_unchanged': progress.skipped_unchanged if progress else 0,
                       'photos_left_out': progress.photos_left_out if progress else 0,
                       'cursor_lag_days': lag}}


def _time_left(deadline: float) -> float:
    return max(deadline - time.monotonic(), 0)


def _pull_time_budget() -> float:
    """Seconds a pull may spend before it stops taking new chunks: the tighter
    of the runner's and Cloud Run's limits, less a margin for the chunk in
    flight and the summary."""
    limit = min(settings.MAX_ACTION_EXECUTION_TIME, settings.INAT_PULL_TIME_LIMIT_SECONDS)
    return limit - settings.INAT_PULL_TIME_MARGIN_SECONDS


def _payload_hash(event: dict) -> str:
    """What was sent, as one string: equal payloads hash equal across runs."""
    return hashlib.sha256(json.dumps(event, sort_keys=True, default=str).encode()).hexdigest()


async def _sort_observation(integration: Integration, action_config: PullEventsConfig, observation: Observation,
                            new_ids: set, patch_targets: dict, photos_left_behind: set, *, now: datetime) -> str:
    """Sort one observation by what to do with it, and say which:

    - "too_old": uploaded more than max_age_days ago; nothing is stored for it.
    - "new": not in Gundi yet; its id goes in new_ids.
    - "update": in Gundi, and what we would send differs from what was last
      sent (or no hash was stored yet); goes in patch_targets ({id: object_id}).
    - "unchanged": in Gundi and nothing we send changed.
    - "unconfirmed": a run was cut off while sending it, so it may be in Gundi
      already; it is never sent again, to avoid a duplicate event.

    An observation in Gundi whose photos are still pending also goes in
    photos_left_behind, unless it is too old: past max_age_days a sighting
    gets nothing more, photos included (spec D3)."""
    created = observation.created_at
    if created is not None:
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if created < now - timedelta(days=action_config.max_age_days):
            return "too_old"
    saved_event = await state_manager.get_state(str(integration.id), "pull_events", str(observation.id))
    if not saved_event:
        new_ids.add(observation.id)
        return "new"
    if not saved_event.get("object_id") and saved_event.get(STATE_SENDING_KEY):
        return "unconfirmed"
    photos = await state_manager.get_state(str(integration.id), "pull_events", STATE_PHOTOS_PREFIX + str(observation.id))
    if (photos or {}).get("pending"):
        photos_left_behind.add(observation.id)
    payload = _transform_inat_to_gundi_event(observation, action_config)
    if saved_event.get(STATE_SENT_HASH_KEY) == _payload_hash(payload):
        return "unchanged"
    patch_targets[observation.id] = saved_event.get("object_id")
    return "update"


async def _send_new_observations(integration, action_config, observations, progress: _PullProgress):
    """Send one chunk of new observations and save their state, listing each
    one's photos as pending; then start a photo run to add them. Recording the
    event before any photo work means a stop never sends it twice."""
    events = []
    pending_photos = {}  # inat_id -> [[photo_id, url], ...]
    inat_updated_at_map = {}  # inat_id -> updated_at for state we persist after create
    left_out = {}  # inat_id -> photos not handed off because of max_photos
    for ob in observations:
        e = _transform_inat_to_gundi_event(ob, action_config)
        events.append(e)
        inat_id = e['event_details']['inat_id']
        inat_updated_at_map[inat_id] = ob.updated_at
        if action_config.include_photos:
            photos = list(ob.photos or [])
            kept = photos[:action_config.max_photos]
            if kept:
                pending_photos[inat_id] = [
                    [photo.id, photo.large_url if photo.large_url else photo.url] for photo in kept
                ]
            left_out[inat_id] = len(photos) - len(kept)

    logger.info(f"Submitting {len(events)} iNaturalist observations to Gundi")
    progress.step = "sending new observations to Gundi"
    response = await _send_marked(str(integration.id), [str(ob.id) for ob in observations], events)
    progress.events_sent += len(response)
    if not response:
        return 0, 0
    progress.photos_left_out += sum(left_out.get(e['event_details']['inat_id'], 0) for e in events[:len(response)])
    progress.step = "saving which observations were sent"
    await save_events_state(response, events, integration, inat_updated_at_map, pending_photos)
    with_photos = [e['event_details']['inat_id'] for e in events[:len(response)]
                   if pending_photos.get(e['event_details']['inat_id'])]
    if with_photos and not ephemeral_run.get():
        progress.step = "starting a photo run for the new observations"
        await trigger_action(str(integration.id), "add_photos", AddPhotosConfig(observation_ids=with_photos))
        progress.photos_handed_off += sum(len(pending_photos[i]) for i in with_photos)
    return len(response), 0


async def _send_marked(integration_id: str, inat_ids: List[str], events: List[dict]) -> list:
    """Send new events with a "being sent" note on each observation's row first.

    Gundi accepted what comes back; save_events_state then replaces the note with the
    event's state. The notes of events Gundi didn't take, or that failed, are removed.
    Only a run cut off between the send and the save leaves a note behind, and that
    observation is then never sent again (spec: no duplicate events)."""
    since = datetime.now(tz=timezone.utc).strftime(STATE_DATETIME_FMT)
    marked = []
    try:
        for inat_id in inat_ids:
            await state_manager.set_state(integration_id, "pull_events", {STATE_SENDING_KEY: since}, source_id=inat_id)
            marked.append(inat_id)
    except BaseException:
        # Nothing was sent: the notes written so far must not hide these sightings.
        await _clear_sending(integration_id, marked)
        raise
    try:
        response = await send_events_to_gundi(events=events, integration_id=integration_id) or []
    except (asyncio.CancelledError, asyncio.TimeoutError, httpx.TimeoutException):
        # Cut off or timed out waiting for Gundi's answer: Gundi may have created them,
        # so the notes stay and these sightings are never sent again.
        raise
    except Exception:
        # Gundi answered with an error, or the request never got through: nothing was created.
        await _clear_sending(integration_id, inat_ids)
        raise
    await _clear_sending(integration_id, inat_ids[len(response):])
    return response


async def _clear_sending(integration_id: str, inat_ids: List[str]):
    for inat_id in inat_ids:
        try:
            await state_manager.delete_state(integration_id, "pull_events", source_id=inat_id)
        except Exception:
            logger.exception(f"Could not clear a being-sent note for integration ID: {integration_id}.")


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


async def _add_photo(integration: Integration, gundi_id: str, photo_id, url: str, deadline: float,
                     hard_limit: float):
    """Download one photo and attach it to the Gundi event. Raises _PhotoGone
    when it can never be added, and anything else when a later try may work.
    Nothing here waits past hard_limit."""
    filename = f"{photo_id}.{url.split('.')[-1]}"
    timeout = max(min(PHOTO_DOWNLOAD_TIMEOUT_SECONDS, deadline - time.monotonic()), 1)
    logger.info(f"Adding {url} to Gundi event {gundi_id}")
    async with httpx.AsyncClient(timeout=timeout, verify=False) as session:
        image_response = await session.get(url)
    if image_response.status_code in INAT_PHOTO_GONE_STATUSES:
        raise _PhotoGone("iNaturalist no longer has it")
    image_response.raise_for_status()
    try:
        # The upload retries on its own; it gets only the time left before the hard limit.
        await asyncio.wait_for(send_event_attachments_to_gundi(
            event_id=gundi_id, attachments=[(filename, image_response.content)], integration_id=str(integration.id)
        ), max(hard_limit - time.monotonic(), 1))
    except GundiAPIError as e:
        if getattr(e, "status_code", None) in GUNDI_EVENT_GONE_STATUSES:
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


async def _add_pending_photos(integration: Integration, obs_id: str, deadline: float, hard_limit: float,
                              run: _PhotoRun) -> bool:
    """Add one observation's pending photos, saving after each. True when none
    are left; False when time ran out or a photo failed (it stays pending)."""
    integration_id = str(integration.id)
    record_id = STATE_PHOTOS_PREFIX + obs_id
    record = await state_manager.get_state(integration_id, "pull_events", record_id)
    pending = list((record or {}).get("pending") or [])
    while pending:
        if time.monotonic() >= deadline:
            return False
        photo_id, url = pending[0]
        try:
            await _add_photo(integration, record["object_id"], photo_id, url, deadline, hard_limit)
            run.added += 1
        except _PhotoGone as e:
            logger.warning(f"Photo {photo_id} for iNaturalist observation {obs_id} wasn't added because {e}.")
            run.gone += 1
        except Exception as e:
            logger.warning(f"Photo {photo_id} for iNaturalist observation {obs_id} wasn't added yet: {e!r}")
            run.last_problem = _plain_problem(e)
            return False
        pending.pop(0)
        if pending:
            await state_manager.set_state(integration_id, "pull_events", {**record, "pending": pending},
                                          source_id=record_id)
    await state_manager.delete_state(integration_id, "pull_events", record_id)
    run.observations_done += 1
    return True


def _plain_problem(error: Exception) -> str:
    """Why a photo couldn't be added this time, in plain words."""
    if isinstance(error, (httpx.TimeoutException, asyncio.TimeoutError)):
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
    hard_limit = (run.started + min(settings.MAX_ACTION_EXECUTION_TIME, settings.INAT_PULL_TIME_LIMIT_SECONDS)
                  - PHOTO_HARD_LIMIT_MARGIN_SECONDS)
    left = list(action_config.observation_ids)
    while left and time.monotonic() < deadline:
        failed = []
        for index, obs_id in enumerate(left):
            if time.monotonic() >= deadline:
                failed.extend(left[index:])
                break
            # Another photo run (a repeated message) may be on this observation. Keep it on
            # the list anyway: if that run died holding the lock, a later run must finish it.
            lock_token = uuid4().hex
            lock_id = STATE_PHOTOS_LOCK_PREFIX + obs_id
            if not await state_manager.set_if_absent(
                    integration_id, "pull_events", ttl_seconds=RUN_LOCK_TTL_SECONDS, source_id=lock_id,
                    value=lock_token):
                failed.append(obs_id)
                run.last_problem = run.last_problem or "another photo run was adding them at the same time"
                continue
            try:
                if not await _add_pending_photos(integration, obs_id, deadline, hard_limit, run):
                    failed.append(obs_id)
            finally:
                await state_manager.delete_state_if_value(integration_id, "pull_events", lock_token, source_id=lock_id)
        left = failed
        if not left or time.monotonic() >= deadline:
            break
        if time.monotonic() + PHOTO_RETRY_WAIT_SECONDS >= deadline:
            # Not enough time to wait before trying again: the next photo run carries on.
            break
        await asyncio.sleep(PHOTO_RETRY_WAIT_SECONDS)

    made_progress = bool(run.added or run.gone)
    runs_without_progress = 0 if made_progress else action_config.runs_without_progress + 1
    photos_left = 0
    if left:
        await trigger_action(integration_id, "add_photos", AddPhotosConfig(
            observation_ids=left, runs_without_progress=runs_without_progress))
        for obs_id in left:
            record = await state_manager.get_state(integration_id, "pull_events", STATE_PHOTOS_PREFIX + obs_id)
            photos_left += len((record or {}).get("pending") or [])
    await _log_photo_run(integration, run, left, photos_left, runs_without_progress)
    return {"result": {"photos_attached": run.added, "photos_not_available": run.gone,
                       "observations_left": len(left), "photos_left": photos_left}}


async def _log_photo_run(integration: Integration, run: _PhotoRun, left: List[str], photos_left: int,
                         runs_without_progress: int):
    took = _plain_duration(time.monotonic() - run.started)
    msg = f"Added {_count(run.added, 'photo', 'photos')} to iNaturalist observations in Gundi in {took}."
    if run.gone:
        msg += (f" {run.gone} of the photos couldn't be added because iNaturalist no longer has them "
                f"or their event is no longer in Gundi.")
    level = LogLevel.INFO
    if left:
        still = (f"{_count(photos_left, 'photo', 'photos')} for "
                 f"{_count(len(left), 'observation', 'observations')} {'is' if photos_left == 1 else 'are'} still to add")
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
        "observations_done": run.observations_done, "observations_left": len(left), "photos_left": photos_left,
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


async def save_events_state(response, events, integration, inat_updated_at_map=None, pending_photos=None,
                            extra: Optional[dict] = None):
    """Persist Gundi event state per observation so we know what we created and when we last synced,
    and which of its photos are still to add. `extra` is merged into each row (the backfill tags
    the rows it creates with its job key)."""
    inat_updated_at_map = inat_updated_at_map or {}
    pending_photos = pending_photos or {}
    for saved_event, event in zip(response, events):
        try:
            event_id = event["event_details"]["inat_id"]
            state = dict(saved_event)
            state.update(extra or {})
            state[STATE_SENT_HASH_KEY] = _payload_hash(event)
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
            if pending_photos.get(event_id):
                await state_manager.set_state(
                    integration_id=str(integration.id),
                    action_id="pull_events",
                    state={"object_id": state.get("object_id"), "pending": pending_photos[event_id]},
                    source_id=STATE_PHOTOS_PREFIX + event_id,
                )
        except Exception as e:
            inat_id = event.get("event_details", {}).get("inat_id", "unknown")
            message = f"Error while saving event ID '{inat_id}'. Exception: {e}."
            logger.exception(message, extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            })
            raise e


async def save_patched_events_state(events_to_patch, integration, action_config: PullEventsConfig):
    """After patching, update per-observation state so we don't patch again until what we send changes."""
    for gundi_object_id, observation in events_to_patch:
        try:
            updated_at = observation.updated_at
            # Merged into the saved row, so fields other writers keep there (the backfill's tags) survive.
            saved = await state_manager.get_state(str(integration.id), "pull_events", str(observation.id)) or {}
            state = {
                **saved,
                "object_id": gundi_object_id,
                STATE_INAT_UPDATED_AT_KEY: (
                    updated_at.strftime(STATE_DATETIME_FMT)
                    if hasattr(updated_at, "strftime") else str(updated_at)
                ),
                STATE_SENT_HASH_KEY: _payload_hash(_transform_inat_to_gundi_event(observation, action_config)),
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

    # "any" in the portal arrives as None (see PullEventsConfig).
    event["title"] = (config.event_prefix or "") + event["title"]

    return event


# --- Backfill: manual, creations only, up to max_events (spec §5) ---

# Seconds a backfill run waits between tries for the run lock.
BACKFILL_LOCK_WAIT_SECONDS = 10
# Runs in a row a backfill hands over because the lock stayed taken, before it stops.
BACKFILL_MAX_LOCK_WAITS = 3
# Runs in a row a backfill may end where it started (no page read in time), before it stops.
BACKFILL_MAX_RUNS_WITHOUT_PROGRESS = 3


def _backfill_result(created: int, job: BackfillContinueConfig, finished: bool,
                     uploaded_up_to: Optional[datetime]) -> dict:
    return {'result': {'events_extracted': created,
                       'events_updated': 0,
                       'photos_attached': 0,
                       'backfill_events_created': job.created_so_far,
                       'backfill_finished': finished,
                       'backfill_uploaded_up_to': uploaded_up_to.isoformat() if uploaded_up_to else None}}


def _backfill_data(job: BackfillContinueConfig, uploaded_up_to: Optional[datetime] = None) -> dict:
    """Counts and dates only: no observation ids in activity logs."""
    return {"events_created": job.created_so_far, "max_events": job.max_events,
            "window_from": job.created_from.isoformat(), "window_to": job.created_to.isoformat(),
            "uploaded_up_to": uploaded_up_to.isoformat() if uploaded_up_to else None}


async def _backfill_log(integration: Integration, level: LogLevel, msg: str, data: dict):
    log = {LogLevel.ERROR: logger.error, LogLevel.WARNING: logger.warning}.get(level, logger.info)
    log(f"{msg} Integration ID: {integration.id}.")
    await _publish_progress_log(integration, msg, level, data, "backfill notice", action_id="backfill_events")


def _day(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _backfill_key(job: BackfillContinueConfig) -> str:
    return STATE_BACKFILL_PREFIX + job.created_to.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%f")


def _reached(job: BackfillContinueConfig, uploaded_up_to: Optional[datetime], batch_count: int) -> str:
    """Where the backfill got to, for the logs."""
    if not uploaded_up_to:
        return ""
    where = f"sightings uploaded up to {_utc_minute(uploaded_up_to)} UTC were sent"
    if batch_count > 1:
        where += f" (taxa group {job.batch_index + 1} of {batch_count})"
    return where


# Seconds after a backfill ended during which a start with the same settings counts as a
# repeat of the one that just ended (a Pub/Sub redelivery), not a new pass.
BACKFILL_REPEAT_GRACE_SECONDS = 600
# Seconds a start waits for another start of the same connection to finish choosing its job.
BACKFILL_START_LOCK_SECONDS = 30
STATE_BACKFILL_STARTING = "backfill_starting"


def _filters_fingerprint(pull_config: PullEventsConfig) -> str:
    """What decides which sightings a backfill reads. A change means its saved position no
    longer points at the same list of sightings."""
    filters = {"taxa": pull_config.taxa_str, "projects": pull_config.projects,
               "bounding_box": pull_config.bounding_box, "quality_grade": pull_config.quality_grade,
               "annotations": pull_config.annotations_dict}
    return hashlib.sha256(json.dumps(filters, sort_keys=True, default=str).encode()).hexdigest()[:16]


async def _pull_settings(integration: Integration, job: BackfillContinueConfig) -> Optional[PullEventsConfig]:
    """The connection's pull settings, or None after logging why the backfill can't run."""
    pull_row = find_config_for_action(integration.configurations or [], "pull_events")
    if not pull_row:
        await _backfill_log(integration, LogLevel.ERROR,
                            "The backfill didn't run because the connection has no pull settings. "
                            "Save the pull settings (projects or area, event type) and run it again.",
                            _backfill_data(job))
        return None
    try:
        return PullEventsConfig.parse_obj(pull_row.data)
    except pydantic.ValidationError as e:
        fields = ", ".join(sorted({str(err["loc"][0]) for err in e.errors() if err.get("loc")})) or "the settings"
        await _backfill_log(integration, LogLevel.ERROR,
                            f"The backfill didn't run because the pull settings aren't valid ({fields}). "
                            f"Fix them and run it again.", _backfill_data(job))
        return None


@action_title("Backfill iNaturalist Observations")
@_clock_run
@activity_logger()
async def action_backfill_events(integration: Integration, action_config: BackfillEventsConfig):
    """Run by hand only: create events for sightings uploaded in the last
    days_back days, oldest first, until max_events. No updates, no photos."""
    created_to = datetime.now(tz=timezone.utc)
    placeholder = BackfillContinueConfig(created_from=created_to - timedelta(days=action_config.days_back),
                                         created_to=created_to, max_events=action_config.max_events)
    pull_config = await _pull_settings(integration, placeholder)
    if pull_config is None:
        return _backfill_result(0, placeholder, False, None)
    try:
        job = await _backfill_job_for(str(integration.id), action_config, _filters_fingerprint(pull_config))
    except BackfillStartBusy:
        await _backfill_log(integration, LogLevel.WARNING,
                            "The backfill didn't start because another start of this connection's backfill was "
                            "still being set up. Run it again in a minute.", _backfill_data(placeholder))
        return _backfill_result(0, placeholder, False, None)
    if job is None:
        return _backfill_result(0, placeholder, False, None)
    return await _backfill(integration, job, pull_config)


class BackfillStartBusy(Exception):
    """Another start of the same connection held the start lock the whole time."""


async def _backfill_job_for(integration_id: str, action_config: BackfillEventsConfig, filters: str,
                            now: Optional[datetime] = None) -> Optional[BackfillContinueConfig]:
    """The job this start belongs to (spec §5, B10).

    - Same settings and pull filters as a backfill that hasn't ended: continue it (same window,
      same limit), so a redelivered start or a second press of Run never opens a second one.
    - Same settings as one that ended less than BACKFILL_REPEAT_GRACE_SECONDS ago: a repeat of
      that start (None: nothing to do).
    - Otherwise a new window; a backfill still running for other settings is closed first.
    Starts of the same connection choose one at a time (a short start lock)."""
    now = now or datetime.now(tz=timezone.utc)
    token = uuid4().hex
    for _ in range(BACKFILL_START_LOCK_SECONDS):
        if await state_manager.set_if_absent(integration_id, "pull_events", ttl_seconds=BACKFILL_START_LOCK_SECONDS,
                                             source_id=STATE_BACKFILL_STARTING, value=token):
            break
        await asyncio.sleep(1)
    else:
        # Never choose a job without the lock: two starts could each open a window.
        raise BackfillStartBusy()
    try:
        current = await state_manager.get_state(integration_id, "pull_events", STATE_BACKFILL_CURRENT) or {}
        same = (current.get("days_back"), current.get("max_events"), current.get("filters")) == (
            action_config.days_back, action_config.max_events, filters)
        if current.get("created_to"):
            running = BackfillContinueConfig(created_from=current["created_from"], created_to=current["created_to"],
                                             max_events=current.get("max_events") or action_config.max_events,
                                             filters=current.get("filters") or "")
            marker = await state_manager.get_state(integration_id, "pull_events", _backfill_key(running)) or {}
            if same and not marker.get("done"):
                # Pick up where the chain is, so this start can also revive a chain whose run was cut off.
                running.seq = marker.get("seq", 0)
                running.batch_index = marker.get("batch_index", 0)
                running.after_id = marker.get("after_id", 0)
                running.created_so_far = marker.get("created_so_far", 0)
                return running
            if same and marker.get("ended_at"):
                ended = datetime.fromisoformat(marker["ended_at"])
                if (now - ended).total_seconds() < BACKFILL_REPEAT_GRACE_SECONDS:
                    logger.info(f"Backfill start for integration ID: {integration_id} repeats one that ended at "
                                f"{marker['ended_at']}; nothing to do.")
                    return None
            if not marker.get("done"):
                # A different backfill replaces this one: its runs see the marker and stop.
                await state_manager.set_state(
                    integration_id, "pull_events",
                    {**marker, "done": True, "superseded": True, "ended_at": now.isoformat()},
                    source_id=_backfill_key(running))
        job = BackfillContinueConfig(created_from=now - timedelta(days=action_config.days_back), created_to=now,
                                     max_events=action_config.max_events, filters=filters)
        await state_manager.set_state(
            integration_id, "pull_events",
            {"days_back": action_config.days_back, "max_events": action_config.max_events, "filters": filters,
             "created_from": job.created_from.isoformat(), "created_to": job.created_to.isoformat()},
            source_id=STATE_BACKFILL_CURRENT,
        )
        return job
    finally:
        try:
            await state_manager.delete_state_if_value(integration_id, "pull_events", token,
                                                      source_id=STATE_BACKFILL_STARTING)
        except Exception:
            logger.exception(f"Could not release the backfill start lock for integration ID: {integration_id}.")


@action_title("Continue iNaturalist Backfill")
@_clock_run
@activity_logger()
async def action_backfill_events_continue(integration: Integration, action_config: BackfillContinueConfig):
    """Started by a backfill run that handed over (time budget, a waiting pull, a taken lock)."""
    return await _backfill(integration, action_config)


async def _backfill_is_current(integration_id: str, job: BackfillContinueConfig) -> bool:
    """False once a different backfill was started for this connection (its window replaced
    this one in backfill_current). Kept apart from the position key, so checkpoint writes can't
    undo a replacement. A connection with no record (a chain from before it existed) is current."""
    current = await state_manager.get_state(integration_id, "pull_events", STATE_BACKFILL_CURRENT) or {}
    if not current.get("created_to"):
        return True
    return datetime.fromisoformat(str(current["created_to"])) == job.created_to


async def _backfill_is_stale(integration_id: str, job: BackfillContinueConfig) -> bool:
    """True when this run should quietly do nothing: its backfill ended (or was replaced),
    or a later run of the same chain already took over (a duplicate or late message)."""
    position = await state_manager.get_state(integration_id, "pull_events", _backfill_key(job)) or {}
    return bool(position.get("done")) or position.get("seq", 0) > job.seq


async def _run_followups(integration: Integration, job: BackfillContinueConfig, followups: list, logs: list):
    """Start the next runs (the backfill's continuation first), then publish the notes that say so.
    If the continuation can't be started, say the backfill stopped instead."""
    integration_id = str(integration.id)
    for action_id, config in sorted(followups, key=lambda f: f[0] != "backfill_events_continue"):
        try:
            await trigger_action(integration_id, action_id, config)
        except Exception as e:
            logger.exception(f"Could not start '{action_id}' after a backfill run for integration ID: {integration_id}.")
            if action_id == "backfill_events_continue":
                logs = [(LogLevel.ERROR,
                         f"The backfill stopped after creating {job.created_so_far:,} events because its next run "
                         f"couldn't be started ({e!r}). Run it again to continue; sightings already sent are "
                         f"skipped.", _backfill_data(job))]
    for level, msg, data in logs:
        await _backfill_log(integration, level, msg, data)


async def _backfill(integration: Integration, job: BackfillContinueConfig,
                    pull_config: Optional[PullEventsConfig] = None) -> dict:
    integration_id = str(integration.id)
    started = _run_started.get() or time.monotonic()
    deadline = started + _pull_time_budget()
    if pull_config is None:
        pull_config = await _pull_settings(integration, job)
        if pull_config is None:
            return _backfill_result(0, job, False, None)

    position = await state_manager.get_state(integration_id, "pull_events", _backfill_key(job)) or {}
    if position.get("done") or position.get("seq", 0) > job.seq:
        # Ended, replaced, or a later run of this chain already ran (a duplicate or late message).
        logger.info(f"Backfill run for integration ID: {integration_id} has nothing to do (ended or taken over).")
        job.created_so_far = max(job.created_so_far, position.get("created_so_far", 0))
        return _backfill_result(0, job, bool(position.get("finished")), None)
    if job.filters and _filters_fingerprint(pull_config) != job.filters:
        position = await state_manager.get_state(integration_id, "pull_events", _backfill_key(job)) or {}
        await state_manager.set_state(
            integration_id, "pull_events",
            {**position, "done": True, "settings_changed": True,
             "ended_at": datetime.now(tz=timezone.utc).isoformat()}, source_id=_backfill_key(job))
        await _backfill_log(
            integration, LogLevel.ERROR,
            f"The backfill stopped after creating {job.created_so_far:,} events because the pull settings "
            f"(projects, area, taxa, grades or annotations) changed while it ran. Run it again to backfill with "
            f"the new settings; sightings already sent are skipped.", _backfill_data(job))
        return _backfill_result(0, job, False, None)

    followups, logs = [], []  # runs to start and notes to publish once the lock is released
    if job.yield_to_pull:
        # The previous run started a pull and handed over: let that pull take the lock first.
        job.yield_to_pull = False
        await asyncio.sleep(BACKFILL_LOCK_WAIT_SECONDS)
    lock_token = uuid4().hex
    while not await state_manager.set_if_absent(
            integration_id, "pull_events", ttl_seconds=RUN_LOCK_TTL_SECONDS, source_id=STATE_RUN_LOCK_SOURCE_ID,
            value=lock_token):
        if time.monotonic() + BACKFILL_LOCK_WAIT_SECONDS < deadline:
            await asyncio.sleep(BACKFILL_LOCK_WAIT_SECONDS)
            continue
        if await _backfill_is_stale(integration_id, job):
            # The lock was held by this backfill's own other run, which has moved on.
            return _backfill_result(0, job, False, None)
        if job.lock_waits + 1 < BACKFILL_MAX_LOCK_WAITS:
            job.lock_waits += 1
            followups.append(("backfill_events_continue", job))
            logs.append((LogLevel.INFO, "The backfill is waiting for another run of this connection (a pull or "
                                        "another backfill) to finish; it tries again in a new run.",
                         _backfill_data(job)))
        else:
            logs.append((LogLevel.ERROR,
                         f"The backfill stopped after creating {job.created_so_far:,} events because other runs of "
                         f"this connection (a pull or another backfill) kept running. Run it again to continue; "
                         f"sightings already sent are skipped.", _backfill_data(job)))
        await _run_followups(integration, job, followups, logs)
        return _backfill_result(0, job, False, None)
    job.lock_waits = 0
    try:
        result = await _backfill_locked(integration, pull_config, job, deadline, followups, logs)
    finally:
        try:
            await state_manager.delete_state_if_value(
                integration_id, "pull_events", lock_token, source_id=STATE_RUN_LOCK_SOURCE_ID
            )
        except Exception:
            # The lock expires on its own; don't mask the run's own outcome.
            logger.exception(f"Error releasing the run lock after a backfill for integration ID: {integration_id}.")
    # After the release: a pull started here must find the lock free.
    await _run_followups(integration, job, followups, logs)
    return result


def _position(job: BackfillContinueConfig, **extra) -> dict:
    return {"seq": job.seq, "batch_index": job.batch_index, "after_id": job.after_id,
            "created_so_far": job.created_so_far, **extra}


async def _backfill_locked(integration: Integration, pull_config: PullEventsConfig, job: BackfillContinueConfig,
                           deadline: float, followups: list, logs: list) -> dict:
    integration_id = str(integration.id)
    window = f"between {_day(job.created_from)} and {_day(job.created_to)}"
    key = _backfill_key(job)

    saved = await state_manager.get_state(integration_id, "pull_events", key) or {}
    if saved.get("done") or saved.get("seq", 0) > job.seq:
        # Ended, replaced, or a later run of this chain already ran (a duplicate or late message).
        logger.info(f"Backfill {key} for integration ID: {integration_id} has nothing to do (ended or taken over).")
        return _backfill_result(0, job, bool(saved.get("finished")), None)
    # A redelivered or repeated run carries an older position: the saved one wins.
    job.created_so_far = max(job.created_so_far, saved.get("created_so_far", 0))
    if (saved.get("batch_index", 0), saved.get("after_id", 0)) > (job.batch_index, job.after_id):
        job.batch_index, job.after_id = saved["batch_index"], saved["after_id"]
    # The seq moves on only when this run hands over (see below), so a copy of this message
    # redelivered after a cut-off run can still pick the chain up from the saved position.
    start = (job.batch_index, job.after_id)

    projects = pull_config.projects
    if projects:
        projects = await _drop_unknown_projects(integration, projects, timeout=_time_left(deadline),
                                                activity="backfill", action_id="backfill_events")
        if not projects:
            return _backfill_result(0, job, False, None)
    annotations = pull_config.annotations_dict
    pages = iter_upload_pages(
        job.created_from, job.created_to,
        batch_index=job.batch_index, after_id=job.after_id,
        bounding_box=pull_config.bounding_box, taxa=pull_config.taxa_str, projects=projects,
        quality_grade=pull_config.quality_grade, annotations=annotations,
    )
    created = 0
    uploaded_up_to = None
    batch_count = 1
    pages_read = 0
    outcome = None  # "limit", "finished", "handover" or "pull"
    try:
        while outcome is None:
            if job.created_so_far >= job.max_events:
                outcome = "limit"
                break
            if time.monotonic() >= deadline:
                outcome = "handover"
                break
            # Checked once this run has read a page, so yielding to pulls never stalls the backfill.
            if pages_read and await _pull_is_waiting(integration_id):
                outcome = "pull"
                break
            try:
                item = await asyncio.wait_for(asyncio.to_thread(next, pages, None), _time_left(deadline))
            except asyncio.TimeoutError:
                outcome = "handover"
                break
            if item is None:
                outcome = "finished"
                break
            batch_index, batch_count, page = item
            pages_read += 1
            if batch_index != job.batch_index:
                job.batch_index, job.after_id = batch_index, 0
            for chunk in chunk_list(page, GUNDI_SUBMISSION_CHUNK_SIZE):
                if time.monotonic() >= deadline:
                    outcome = "handover"
                    break
                if not await _backfill_is_current(integration_id, job):
                    outcome = "superseded"
                    break
                to_add = []
                recovered = []  # ids created by this backfill after its last saved position
                last_seen = None
                for ob in chunk:
                    if job.created_so_far + len(recovered) + len(to_add) >= job.max_events:
                        break
                    last_seen = ob
                    if annotations and not _match_annotations_to_config(ob.annotations, annotations):
                        continue
                    row = await state_manager.get_state(integration_id, "pull_events", str(ob.id))
                    if row:
                        # Created by this backfill, in this taxa group, after its last saved position (a run
                        # that stopped between saving the event and saving its count): counted below.
                        if (row.get("backfill_job") == key and row.get("backfill_batch") == job.batch_index
                                and ob.id > job.after_id):
                            recovered.append(ob.id)
                        continue
                    to_add.append(ob)
                if to_add:
                    events = [_transform_inat_to_gundi_event(ob, pull_config) for ob in to_add]
                    response = await _send_marked(integration_id, [str(ob.id) for ob in to_add], events)
                    accepted = len(response)
                    if accepted:
                        await save_events_state(
                            response, events[:accepted], integration,
                            {e['event_details']['inat_id']: ob.updated_at
                             for e, ob in zip(events[:accepted], to_add[:accepted])},
                            extra={"backfill_job": key, "backfill_batch": job.batch_index},
                        )
                    created += accepted
                    job.created_so_far += accepted
                    if accepted < len(to_add):
                        # Gundi took part of the chunk: resume right after the last one it took.
                        last_seen = to_add[accepted - 1] if accepted else None
                        outcome = "handover"
                # Only recovered sightings up to where the next run resumes: the rest are met again then.
                resume_id = last_seen.id if last_seen is not None else job.after_id
                job.created_so_far += sum(1 for i in recovered if i <= resume_id)
                if last_seen is not None:
                    job.after_id = last_seen.id
                    uploaded_up_to = last_seen.created_at
                await state_manager.set_state(integration_id, "pull_events", _position(job), source_id=key)
                if outcome or job.created_so_far >= job.max_events:
                    break
    except (INatUnavailableError, INatRequestError) as e:
        if not _inat_is_down(e):
            raise
        reached = _reached(job, uploaded_up_to, batch_count)
        await _backfill_log(
            integration, LogLevel.ERROR,
            f"The backfill stopped after creating {job.created_so_far:,} events because {e}"
            + (f"; {reached}" if reached else "") + ". Run it again to continue; sightings already sent are skipped.",
            _backfill_data(job, uploaded_up_to))
        return _backfill_result(created, job, False, uploaded_up_to)

    reached = _reached(job, uploaded_up_to, batch_count)
    if outcome in ("pull", "handover") and not await _backfill_is_current(integration_id, job):
        outcome = "superseded"
    if outcome == "superseded":
        # A different backfill was started: this one stops without a word, its position kept as is.
        logger.info(f"Backfill {key} for integration ID: {integration_id} was replaced by a newer one; stopping.")
        return _backfill_result(created, job, False, uploaded_up_to)
    if outcome in ("pull", "handover"):
        # This run hands the chain on: copies of its own message now see a higher seq and stop.
        job.seq += 1
        await state_manager.set_state(integration_id, "pull_events", _position(job), source_id=key)
    if outcome == "pull":
        # A pull found the lock taken and was skipped: start it again, and let it go first.
        try:
            await state_manager.delete_state(integration_id, "pull_events", source_id=STATE_PULL_WAITING_SOURCE_ID)
        except Exception:
            logger.exception(f"Could not clear the pull-waiting note for integration ID: {integration_id}.")
        job.runs_without_progress = 0
        job.yield_to_pull = True
        followups.append(("backfill_events_continue", job))
        followups.append(("pull_events", None))
        logs.append((LogLevel.INFO,
                     f"Backfill paused for a pull and continues in a new run: {job.created_so_far:,} events created "
                     f"so far for sightings uploaded {window}" + (f"; {reached}." if reached else "."),
                     _backfill_data(job, uploaded_up_to)))
        return _backfill_result(created, job, False, uploaded_up_to)
    if outcome == "handover":
        if (job.batch_index, job.after_id) == start and not created:
            job.runs_without_progress += 1
        else:
            job.runs_without_progress = 0
        if job.runs_without_progress >= BACKFILL_MAX_RUNS_WITHOUT_PROGRESS:
            await _backfill_log(
                integration, LogLevel.ERROR,
                f"The backfill stopped after creating {job.created_so_far:,} events because "
                f"{job.runs_without_progress} runs in a row couldn't read anything new from iNaturalist in time. "
                f"Run it again later; sightings already sent are skipped.", _backfill_data(job, uploaded_up_to))
            return _backfill_result(created, job, False, uploaded_up_to)
        followups.append(("backfill_events_continue", job))
        logs.append((LogLevel.INFO,
                     f"Backfill continues in a new run: {job.created_so_far:,} events created so far for sightings "
                     f"uploaded {window}" + (f"; {reached}." if reached else "."), _backfill_data(job, uploaded_up_to)))
        return _backfill_result(created, job, False, uploaded_up_to)

    # Kept, not deleted: a late or repeated message for this backfill then does nothing.
    await state_manager.set_state(
        integration_id, "pull_events",
        _position(job, done=True, finished=outcome == "finished", ended_at=datetime.now(tz=timezone.utc).isoformat()),
        source_id=key,
    )
    if outcome == "limit":
        await _backfill_log(
            integration, LogLevel.INFO,
            f"Backfill stopped at its limit of {job.max_events:,} events"
            + (f": {reached}." if reached else ".")
            + " Run it again to continue; sightings already sent are skipped.", _backfill_data(job, uploaded_up_to))
        return _backfill_result(created, job, False, uploaded_up_to)
    await _backfill_log(
        integration, LogLevel.INFO,
        f"Backfill finished: created {job.created_so_far:,} events for sightings uploaded {window}.",
        _backfill_data(job, uploaded_up_to))
    return _backfill_result(created, job, True, uploaded_up_to)


async def _pull_is_waiting(integration_id: str) -> bool:
    try:
        return bool(await state_manager.get_state(integration_id, "pull_events", STATE_PULL_WAITING_SOURCE_ID))
    except Exception:
        logger.exception(f"Could not read the pull-waiting note for integration ID: {integration_id}.")
        return False


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


@action_title("Search iNaturalist Taxa")
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