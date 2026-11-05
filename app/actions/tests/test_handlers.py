"""Unit tests for app.actions.handlers."""

import logging
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from gundi_core.schemas.v2 import LogLevel

from app.conftest import async_return
from app.actions.handlers import (
    STATE_DATETIME_FMT,
    STATE_LAST_RUN_KEY,
    STATE_INAT_UPDATED_AT_KEY,
    STATE_SENT_HASH_KEY,
    _PullProgress,
    _build_pull_events_state,
    _payload_hash,
    _sort_observation,
    _get_load_since,
    _normalize_recorded_at,
    _transform_inat_to_gundi_event,
    action_pull_events,
    chunk_list,
)
from app.actions.configurations import PullEventsConfig


def _mock_read(mocker, observations=None, side_effect=None):
    """Patch the pull's iNaturalist read with one page holding observations (or
    what side_effect returns), in dict order. An exception side_effect is
    raised when the first page is read, as the real generator does."""
    def pages(*args, **kwargs):
        if isinstance(side_effect, BaseException):
            raise side_effect
        result = side_effect(*args, **kwargs) if side_effect else observations
        if isinstance(result, dict):
            if result:
                yield list(result.values())
        else:
            yield from result  # a generator from side_effect
    return mocker.patch("app.actions.handlers.iter_observation_pages", side_effect=pages)


def _nothing_sent(result):
    """The three numbers Gundi monitoring reads are zero, and the new counters are there."""
    r = result["result"]
    return ((r["events_extracted"], r["events_updated"], r["photos_attached"]) == (0, 0, 0)
            and {"events_skipped_too_old", "events_skipped_unchanged", "photos_left_out", "cursor_lag_days"} <= set(r))


# --- _get_load_since ---


def test_get_load_since_empty_state_uses_fallback_days():
    state = {}
    result = _get_load_since(state, 5)
    now = datetime.now(tz=timezone.utc)
    expected_floor = now - timedelta(days=6)
    expected_ceil = now - timedelta(days=4)
    assert expected_floor <= result <= expected_ceil
    assert result.tzinfo is not None


def _hours_ago(hours):
    return (datetime.now(tz=timezone.utc) - timedelta(hours=hours)).replace(microsecond=0)


def test_get_load_since_with_last_run_parses_and_returns():
    cursor = _hours_ago(2)
    state = {STATE_LAST_RUN_KEY: cursor.strftime(STATE_DATETIME_FMT)}
    result = _get_load_since(state, 5)
    assert result == cursor


def test_get_load_since_prefers_last_run_over_updated_to():
    cursor = _hours_ago(2)
    state = {
        STATE_LAST_RUN_KEY: cursor.strftime(STATE_DATETIME_FMT),
        "updated_to": _hours_ago(20).strftime(STATE_DATETIME_FMT),
    }
    result = _get_load_since(state, 5)
    assert result == cursor


def test_get_load_since_uses_updated_to_when_last_run_missing():
    cursor = _hours_ago(5)
    state = {"updated_to": cursor.strftime(STATE_DATETIME_FMT)}
    result = _get_load_since(state, 3)
    assert result == cursor


def test_get_load_since_never_starts_further_back_than_fallback_days():
    state = {STATE_LAST_RUN_KEY: "2026-06-22 18:52:00+0000"}
    result = _get_load_since(state, 7)
    floor = datetime.now(tz=timezone.utc) - timedelta(days=7)
    assert abs((result - floor).total_seconds()) < 5


# --- _build_pull_events_state ---


def test_build_pull_events_state():
    dt = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    result = _build_pull_events_state(dt)
    assert result == {STATE_LAST_RUN_KEY: "2024-06-01 12:00:00+0000"}
    # Round-trip
    parsed = datetime.strptime(result[STATE_LAST_RUN_KEY], STATE_DATETIME_FMT)
    assert parsed == dt


# --- chunk_list ---


def test_chunk_list_exact_multiple():
    assert list(chunk_list([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]]


def test_chunk_list_partial_final():
    assert list(chunk_list([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]


def test_chunk_list_empty():
    assert list(chunk_list([], 10)) == []


def test_chunk_list_single_chunk():
    assert list(chunk_list([1, 2, 3], 10)) == [[1, 2, 3]]


# --- _normalize_recorded_at ---


def test_normalize_recorded_at_none_returns_created_at():
    created = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert _normalize_recorded_at(None, created) is created


def test_normalize_recorded_at_date_only():
    d = date(2024, 5, 10)
    created = datetime(2024, 1, 1, tzinfo=timezone.utc)
    result = _normalize_recorded_at(d, created)
    assert result == datetime(2024, 5, 10, 0, 0, 0, tzinfo=timezone.utc)


def test_normalize_recorded_at_naive_datetime():
    naive = datetime(2024, 5, 10, 14, 30, 0)
    created = datetime(2024, 1, 1, tzinfo=timezone.utc)
    result = _normalize_recorded_at(naive, created)
    assert result.tzinfo == timezone.utc
    assert result.replace(tzinfo=None) == naive


def test_normalize_recorded_at_aware_datetime_unchanged():
    aware = datetime(2024, 5, 10, 14, 30, 0, tzinfo=timezone.utc)
    created = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert _normalize_recorded_at(aware, created) is aware


# --- _transform_inat_to_gundi_event ---


def _make_observation(
    id_=12345,
    observed_on=None,
    created_at=None,
    user=None,
    location=None,
    place_ids=None,
    taxon=None,
    **kwargs,
):
    """Minimal Observation-like object for transformation tests."""
    from pyinaturalist import Observation

    created_at = created_at or datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    data = {
        "id": id_,
        "observed_on": observed_on or "2024-06-15",
        "created_at": created_at,
        "captive": kwargs.get("captive", False),
        "obscured": kwargs.get("obscured", False),
        "place_guess": kwargs.get("place_guess"),
        "quality_grade": kwargs.get("quality_grade", "research"),
        "species_guess": kwargs.get("species_guess", "Some species"),
        "updated_at": kwargs.get("updated_at", created_at),
        "uri": kwargs.get("uri", "https://www.inaturalist.org/observations/12345"),
        "photos": kwargs.get("photos", []),
        "user": user,
        "location": location,
        "place_ids": place_ids or [],
        "taxon": taxon,
        "annotations": kwargs.get("annotations", []),
    }
    return Observation.from_json(data)


def test_transform_inat_to_gundi_event_minimal():
    ob = _make_observation()
    config = PullEventsConfig(
        days_to_load=3,
        taxa="1", bounding_box="[1, 1, 0, 0]",
        event_type="inat_observation",
        event_prefix="iNat: ",
    )
    event = _transform_inat_to_gundi_event(ob, config)
    assert event["event_type"] == "inat_observation"
    assert event["event_details"]["inat_id"] == "12345"
    assert event["title"] == "iNat: Some species"
    assert "recorded_at" in event
    assert event["event_details"]["place_guess"] is None


def test_transform_inat_to_gundi_event_with_user_location_taxon():
    ob = _make_observation(
        user={"id": 99, "name": "Jane", "login": "jane"},
        location=[-27.7, 16.7],
        place_ids=[1, 2, 3],
        taxon={
            "id": 120255,
            "rank": "species",
            "name": "Crassula muscosa",
            "preferred_common_name": "lizard's-tail",
            "wikipedia_url": "http://en.wikipedia.org/wiki/Crassula_muscosa",
            "ancestor_ids": [1, 2, 3],
        },
    )
    config = PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]", event_prefix="iNat: ")
    event = _transform_inat_to_gundi_event(ob, config)
    assert event["event_details"]["user_id"] == 99
    assert event["event_details"]["user_name"] == "Jane"
    assert event["location"] == {"lat": -27.7, "lon": 16.7}
    assert event["event_details"]["place_ids"] == "1,2,3"
    assert event["event_details"]["taxon_id"] == 120255
    assert event["event_details"]["taxon_name"] == "Crassula muscosa"
    assert event["title"] == "iNat: lizard's-tail"
    assert event["event_details"]["taxon_ancestors"] == "1,2,3"


def test_transform_inat_to_gundi_event_title_fallback_to_species_guess():
    ob = _make_observation(
        species_guess="Unknown Bird",
        taxon={"id": 1, "rank": "species", "name": "Spp", "preferred_common_name": None},
    )
    config = PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]", event_prefix="")
    event = _transform_inat_to_gundi_event(ob, config)
    assert event["title"] == "Unknown Bird"


# --- action_pull_events: no observations path (state still updated) ---


@pytest.mark.asyncio
async def test_action_pull_events_no_observations_updates_state(mocker):
    from uuid import UUID

    mock_state = AsyncMock()
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    _mock_read(mocker, {})
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    integration = MagicMock()
    integration.id = UUID("f03ec73e-f3fe-41b6-8597-3eb89dde5ae1")
    config = PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]", event_prefix="iNat: ")

    result = await action_pull_events(integration, config)

    assert result["result"]["events_extracted"] == 0
    assert result["result"]["events_updated"] == 0
    mock_state.set_state.assert_called_once()
    call_args = mock_state.set_state.call_args[0]
    assert call_args[0] == str(integration.id)
    assert call_args[1] == "pull_events"
    state = call_args[2]
    assert STATE_LAST_RUN_KEY in state
    # Should be a recent timestamp
    from datetime import datetime
    parsed = datetime.strptime(state[STATE_LAST_RUN_KEY], STATE_DATETIME_FMT)
    assert parsed.tzinfo is not None


@pytest.mark.asyncio
async def test_action_pull_events_skips_patch_when_observation_already_in_sync(mocker):
    """When what we would send hashes the same as what was last sent, we don't
    patch, even though updated_at moved (a comment, a favourite, an agreement)."""
    from uuid import UUID
    from pyinaturalist import Observation

    created = datetime.now(tz=timezone.utc) - timedelta(days=2)
    fixed_updated = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    ob = Observation.from_json({
        "id": 999,
        "observed_on": created.date().isoformat(),
        "created_at": created.isoformat(),
        "updated_at": fixed_updated,
        "captive": False,
        "obscured": False,
        "quality_grade": "research",
        "species_guess": "Bird",
        "uri": "https://www.inaturalist.org/observations/999",
        "photos": [],
        "user": None,
        "location": None,
        "place_ids": [],
        "taxon": None,
        "annotations": [],
    })
    observations_map = {999: ob}
    config = PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]", event_prefix="iNat: ")
    sent_hash = _payload_hash(_transform_inat_to_gundi_event(ob, config))

    async def get_state_side_effect(integration_id, action_id, source_id="no-source"):
        if source_id == "no-source":
            return {}
        if source_id == "999":
            return {
                "object_id": "gundi-uuid-999",
                STATE_INAT_UPDATED_AT_KEY: "2000-01-01 00:00:00+0000",
                STATE_SENT_HASH_KEY: sent_hash,
            }
        return {}

    mock_state = AsyncMock()
    mock_state.get_state.side_effect = get_state_side_effect
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    _mock_read(mocker, observations_map)
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    mock_patch_events = mocker.patch("app.actions.handlers.patch_events", new_callable=AsyncMock)

    integration = MagicMock()
    integration.id = UUID("f03ec73e-f3fe-41b6-8597-3eb89dde5ae1")

    result = await action_pull_events(integration, config)

    assert result["result"]["events_updated"] == 0
    assert result["result"]["events_extracted"] == 0
    assert result["result"]["events_skipped_unchanged"] == 1
    mock_patch_events.assert_not_called()




# --- action_pull_events: per-integration run lock ---


def _lock_test_integration():
    from uuid import UUID

    integration = MagicMock()
    integration.id = UUID("f03ec73e-f3fe-41b6-8597-3eb89dde5ae1")
    return integration


@pytest.mark.asyncio
async def test_action_pull_events_skips_when_another_run_holds_the_lock(mocker):
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = False
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    mock_get_observations = _mock_read(mocker)
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    result = await action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]"))

    assert _nothing_sent(result)
    mock_get_observations.assert_not_called()
    mock_state.delete_state_if_value.assert_not_called()


@pytest.mark.asyncio
async def test_action_pull_events_skip_survives_an_activity_log_failure(mocker):
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = False
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    mock_get_observations = _mock_read(mocker)
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock(side_effect=RuntimeError("publisher down")))
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    result = await action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]"))

    assert _nothing_sent(result)
    mock_get_observations.assert_not_called()


@pytest.mark.asyncio
async def test_action_pull_events_releases_the_lock_after_a_run(mocker):
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    _mock_read(mocker, {})
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    integration = _lock_test_integration()

    await action_pull_events(integration, PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]"))

    mock_state.set_if_absent.assert_called_once()
    assert mock_state.set_if_absent.call_args.kwargs["source_id"] == "run_lock"
    lock_token = mock_state.set_if_absent.call_args.kwargs["value"]
    mock_state.delete_state_if_value.assert_called_once_with(
        str(integration.id), "pull_events", lock_token, source_id="run_lock"
    )


@pytest.mark.asyncio
async def test_action_pull_events_releases_the_lock_when_the_run_fails(mocker):
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    _mock_read(mocker, side_effect=RuntimeError("iNat down"))
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    integration = _lock_test_integration()

    with pytest.raises(RuntimeError, match="iNat down"):
        await action_pull_events(integration, PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]"))

    lock_token = mock_state.set_if_absent.call_args.kwargs["value"]
    mock_state.delete_state_if_value.assert_called_once_with(
        str(integration.id), "pull_events", lock_token, source_id="run_lock"
    )


@pytest.mark.asyncio
async def test_action_pull_events_lock_release_failure_does_not_mask_the_result(mocker):
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    mock_state.get_state.return_value = {}
    mock_state.delete_state_if_value.side_effect = ConnectionError("redis down")
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    _mock_read(mocker, {})
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    result = await action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]"))

    assert result["result"]["events_extracted"] == 0


@pytest.mark.asyncio
async def test_action_pull_events_ephemeral_run_skips_the_lock(mocker):
    from app.services.activity_logger import ephemeral_run

    mock_state = AsyncMock()
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    _mock_read(mocker, {})
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    token = ephemeral_run.set(True)
    try:
        await action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]"))
    finally:
        ephemeral_run.reset(token)

    mock_state.set_if_absent.assert_not_called()
    mock_state.delete_state_if_value.assert_not_called()


class _InMemoryLockState:
    """Just the lock calls of IntegrationStateManager, with Redis semantics."""

    def __init__(self):
        self.locks = {}

    async def get_state(self, *args, **kwargs):
        return {}

    async def set_state(self, *args, **kwargs):
        pass

    async def set_if_absent(self, integration_id, action_id, *, ttl_seconds, source_id="no-source", value="1"):
        key = (integration_id, action_id, source_id)
        if key in self.locks:
            return False
        self.locks[key] = value
        return True

    async def delete_state(self, integration_id, action_id, source_id="no-source"):
        self.locks.pop((integration_id, action_id, source_id), None)

    async def delete_state_if_value(self, integration_id, action_id, value, source_id="no-source"):
        key = (integration_id, action_id, source_id)
        if self.locks.get(key) != value:
            return False
        del self.locks[key]
        return True


@pytest.mark.asyncio
async def test_action_pull_events_does_not_release_a_lock_taken_after_its_own_expired(mocker, caplog):
    # Run A outlives its lock; run B takes the expired lock while A is still running.
    # A's cleanup must leave B's lock in place, or a third run could overlap B.
    integration = _lock_test_integration()
    lock_key = (str(integration.id), "pull_events", "run_lock")
    state = _InMemoryLockState()
    mocker.patch("app.actions.handlers.state_manager", state)
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    def run_a_fetch(*args, **kwargs):
        state.locks.pop(lock_key)  # A's lock expires mid-run
        state.locks[lock_key] = "run-b-token"  # and run B takes it
        return {}

    _mock_read(mocker, side_effect=run_a_fetch)

    with caplog.at_level(logging.INFO, logger="app.actions.handlers"):
        await action_pull_events(integration, PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]"))

    assert state.locks[lock_key] == "run-b-token"
    assert "had already expired or been taken by another run" in caplog.text


# --- action_pull_events: saved projects that don't exist on iNaturalist ---


def _unknown_projects_test_setup(mocker, existing):
    from uuid import UUID

    mock_state = AsyncMock()
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    get_observations = _mock_read(mocker, {})
    find = mocker.patch("app.actions.handlers.find_existing_projects", side_effect=existing)
    activity = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    integration = MagicMock()
    integration.id = UUID("f03ec73e-f3fe-41b6-8597-3eb89dde5ae1")
    return integration, get_observations, find, activity


@pytest.mark.asyncio
async def test_action_pull_events_leaves_unknown_projects_out_of_the_query(mocker):
    integration, get_observations, _, activity = _unknown_projects_test_setup(
        mocker, existing=lambda values: ["real-project"]
    )
    config = PullEventsConfig(days_to_load=3, projects=["gone-project", "real-project"])

    await action_pull_events(integration, config)

    assert get_observations.call_args.kwargs["projects"] == ["real-project"]
    warning = activity.call_args_list[0].kwargs
    assert warning["level"] == LogLevel.WARNING
    assert "gone-project" in warning["title"]
    assert warning["data"] == {"unknown_projects": ["gone-project"]}


@pytest.mark.asyncio
async def test_action_pull_events_skips_when_no_saved_project_exists(mocker):
    integration, get_observations, _, activity = _unknown_projects_test_setup(
        mocker, existing=lambda values: []
    )
    config = PullEventsConfig(days_to_load=3, projects=["gone-project"], bounding_box="[1, 1, 0, 0]")

    result = await action_pull_events(integration, config)

    assert _nothing_sent(result)
    get_observations.assert_not_called()
    warning = activity.call_args.kwargs
    assert warning["level"] == LogLevel.WARNING
    assert warning["title"].startswith("The pull didn't run because none of the saved projects exist on iNaturalist")


@pytest.mark.asyncio
async def test_action_pull_events_uses_saved_projects_when_the_lookup_fails(mocker):
    import requests

    integration, get_observations, _, activity = _unknown_projects_test_setup(
        mocker, existing=requests.ConnectionError("iNat down")
    )
    config = PullEventsConfig(days_to_load=3, projects=["some-project"])

    await action_pull_events(integration, config)

    assert get_observations.call_args.kwargs["projects"] == ["some-project"]
    assert not any("unknown_projects" in (c.kwargs.get("data") or {}) for c in activity.call_args_list)


@pytest.mark.asyncio
async def test_action_pull_events_uses_saved_projects_when_the_lookup_is_rate_limited(mocker):
    from pyrate_limiter import BucketFullException, RequestRate

    integration, get_observations, _, activity = _unknown_projects_test_setup(
        mocker, existing=BucketFullException("inaturalist.org", RequestRate(60, 60), 30.0)
    )
    config = PullEventsConfig(days_to_load=3, projects=["some-project"])

    await action_pull_events(integration, config)

    assert get_observations.call_args.kwargs["projects"] == ["some-project"]
    assert not any("unknown_projects" in (c.kwargs.get("data") or {}) for c in activity.call_args_list)


@pytest.mark.asyncio
async def test_action_pull_events_does_not_look_up_projects_when_none_are_saved(mocker):
    integration, get_observations, find, _ = _unknown_projects_test_setup(
        mocker, existing=lambda values: values
    )

    await action_pull_events(integration, PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]"))

    find.assert_not_called()
    get_observations.assert_called_once()


@pytest.mark.asyncio
async def test_action_pull_events_drops_blank_saved_projects(mocker):
    # A config saved with a blank portal row still pulls with its real projects.
    integration, get_observations, find, _ = _unknown_projects_test_setup(
        mocker, existing=lambda values: values
    )
    config = PullEventsConfig.parse_obj({"days_to_load": 3, "projects": [" 123 ", None, ""]})

    await action_pull_events(integration, config)

    find.assert_called_once_with(["123"])
    assert get_observations.call_args.kwargs["projects"] == ["123"]


@pytest.mark.asyncio
async def test_action_pull_events_refuses_an_unbounded_config(mocker):
    # construct() skips validation, like a config built outside the runner.
    integration, get_observations, find, activity = _unknown_projects_test_setup(
        mocker, existing=lambda values: values
    )
    config = PullEventsConfig.construct(
        days_to_load=3, projects=None, bounding_box=None, taxa=["1"]
    )

    result = await action_pull_events(integration, config)

    assert _nothing_sent(result)
    get_observations.assert_not_called()
    find.assert_not_called()
    # Refused before any state is read.
    from app.actions import handlers
    handlers.state_manager.get_state.assert_not_called()
    error = activity.call_args.kwargs
    assert error["level"] == LogLevel.ERROR
    assert "no project and no bounding box" in error["title"]


# --- activity logs support reads ---


@pytest.mark.asyncio
async def test_action_pull_events_skip_says_the_previous_pull_is_still_running(mocker):
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = False
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    mock_log = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    await action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]"))

    assert mock_log.call_args.kwargs["title"] == "The pull didn't run because the previous pull or a backfill for this connection is still running."
    assert mock_log.call_args.kwargs["level"] == LogLevel.INFO


@pytest.mark.asyncio
async def test_action_pull_events_no_new_observations_is_info_with_the_window(mocker):
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    cursor = (datetime.now(tz=timezone.utc) - timedelta(hours=1)).replace(second=0, microsecond=0)
    mock_state.get_state.return_value = {STATE_LAST_RUN_KEY: cursor.strftime(STATE_DATETIME_FMT)}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    _mock_read(mocker, {})
    mock_log = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    await action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]"))

    assert mock_log.call_args.kwargs["title"] == f"No observations matching the settings were added or changed on iNaturalist since {cursor:%Y-%m-%d %H:%M} UTC."
    assert mock_log.call_args.kwargs["level"] == LogLevel.INFO


@pytest.mark.asyncio
async def test_a_stopped_pull_says_where_it_was(mocker):
    """The runner's time limit cancels the handler; before the cancellation
    goes on, the pull logs its step and how long iNaturalist took."""
    import asyncio

    observation = MagicMock()
    observation.id = 1
    observation.photos = []
    observation.created_at = datetime.now(tz=timezone.utc) - timedelta(days=1)
    observation.updated_at = datetime(2026, 10, 29, tzinfo=timezone.utc)
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    _mock_read(mocker, {1: observation})
    mocker.patch("app.actions.handlers._transform_inat_to_gundi_event",
                 return_value={"event_details": {"inat_id": "1"}})
    sending = asyncio.Event()

    async def hang(**kwargs):
        sending.set()
        await asyncio.Event().wait()

    mocker.patch("app.actions.handlers.send_events_to_gundi", side_effect=hang)
    mock_log = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    task = asyncio.ensure_future(action_pull_events(
        _lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]")
    ))
    await sending.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    stopped = mock_log.call_args.kwargs
    assert stopped["level"] == LogLevel.WARNING
    assert stopped["title"].startswith("The pull was cut off after 0 seconds while sending new observations to Gundi. ")
    assert ("Before that, it sent at least 0 new iNaturalist observations to Gundi and updated 0. "
            "Reading from iNaturalist took 0 seconds and found 1 observation that matches the settings.") in stopped["title"]
    assert stopped["data"]["observations_matched"] == 1
    mock_state.delete_state_if_value.assert_called_once()  # the lock is still released


@pytest.mark.asyncio
async def test_a_stopped_notice_gives_up_on_a_slow_publisher(mocker):
    """A slow publisher must not hold a stopped pull (and its lock) much past the time limit."""
    import asyncio
    import time

    from app.actions import handlers

    mocker.patch.object(handlers, "PROGRESS_LOG_TIMEOUT_SECONDS", 0.2)
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    started = asyncio.Event()

    async def hang_state(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    mock_state.get_state.side_effect = hang_state
    mocker.patch("app.actions.handlers.state_manager", mock_state)

    async def slow_publish(**kwargs):
        await asyncio.sleep(30)

    mocker.patch("app.actions.handlers.log_action_activity", side_effect=slow_publish)
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    task = asyncio.ensure_future(action_pull_events(
        _lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]")
    ))
    await started.wait()
    stop_requested = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert time.monotonic() - stop_requested < 2
    mock_state.delete_state_if_value.assert_called_once()


@pytest.mark.asyncio
async def test_a_pull_stopped_before_asking_inaturalist_says_so(mocker):
    import asyncio

    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    started = asyncio.Event()

    async def hang_state(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    mock_state.get_state.side_effect = hang_state
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    mock_log = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    task = asyncio.ensure_future(action_pull_events(
        _lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]")
    ))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    title = mock_log.call_args.kwargs["title"]
    assert "while getting ready to read from iNaturalist" in title
    assert "Reading from iNaturalist hadn't started" in title


@pytest.mark.asyncio
async def test_a_pull_stopped_after_saving_its_cursor_is_not_reported_as_stopped(mocker):
    """Cut off while publishing its summary: nothing was lost, so no "stopped" warning."""
    import asyncio

    observation = MagicMock()
    observation.id = 1
    observation.photos = []
    observation.created_at = datetime.now(tz=timezone.utc) - timedelta(days=1)
    observation.updated_at = datetime(2026, 10, 29, tzinfo=timezone.utc)
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    _mock_read(mocker, {1: observation})
    mocker.patch("app.actions.handlers._transform_inat_to_gundi_event",
                 return_value={"event_details": {"inat_id": "1"}})
    mocker.patch("app.actions.handlers.send_events_to_gundi", AsyncMock(return_value=[{"object_id": "g1"}]))
    mocker.patch("app.actions.handlers.save_events_state", AsyncMock())
    summary_started = asyncio.Event()
    titles = []

    async def log(**kwargs):
        titles.append(kwargs["title"])
        if kwargs["title"].startswith("Pull finished"):
            summary_started.set()
            await asyncio.Event().wait()

    mocker.patch("app.actions.handlers.log_action_activity", side_effect=log)
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    task = asyncio.ensure_future(action_pull_events(
        _lock_test_integration(),
        PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]", include_photos=False),
    ))
    await summary_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not any(t.startswith("The pull was cut off") for t in titles)


@pytest.mark.parametrize("seconds, text", [
    (45, "45 seconds"),
    (119, "119 seconds"),
    (120, "2 minutes"),
    (539, "9 minutes"),
    (510, "9 minutes"),
    (509, "8 minutes"),
])
def test_plain_duration_rounds_to_the_nearest_minute(seconds, text):
    from app.actions.handlers import _plain_duration

    assert _plain_duration(seconds) == text


def test_progress_messages_use_singular_for_one():
    from app.actions.handlers import _PullProgress

    progress = _PullProgress()
    progress.inat_seconds = 1
    progress.observations_read = 1
    progress.events_sent = 1
    progress.photos_handed_off = 1
    assert progress.counts() == ("it sent 1 new iNaturalist observation to Gundi and updated 0, "
                                 "and handed their 1 photo to a separate photo run")
    assert progress.inat_summary() == "Reading from iNaturalist took 1 second and found 1 observation that matches the settings"
    progress.observations_read = 2
    progress.events_sent = 0
    progress.photos_handed_off = 0
    assert progress.counts() == "it sent 0 new iNaturalist observations to Gundi and updated 0"
    assert progress.inat_summary() == "Reading from iNaturalist took 1 second and found 2 observations that match the settings"
    assert progress.counts(at_least=True) == "it sent at least 0 new iNaturalist observations to Gundi and updated 0"


@pytest.mark.asyncio
async def test_the_pull_summary_never_runs_past_the_time_limit(mocker):
    """With no time left, the summary is skipped rather than turning a saved pull into a timeout."""
    from app.actions import handlers
    from app.actions.handlers import _PullProgress, _log_pull_finished

    mocker.patch.object(handlers.settings, "MAX_ACTION_EXECUTION_TIME", 540)
    mock_log = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    progress = _PullProgress()
    progress.started -= 539  # one second before the limit, inside the margin
    progress.inat_seconds = 10

    await _log_pull_finished(MagicMock(), progress)

    mock_log.assert_not_called()


@pytest.mark.asyncio
async def test_the_pull_summary_gets_the_time_left(mocker):
    from app.actions import handlers
    from app.actions.handlers import _PullProgress, _log_pull_finished

    mocker.patch.object(handlers.settings, "MAX_ACTION_EXECUTION_TIME", 540)
    publish = mocker.patch("app.actions.handlers._publish_progress_log", AsyncMock())
    progress = _PullProgress()
    progress.started -= 535
    progress.inat_seconds = 10

    await _log_pull_finished(MagicMock(), progress)

    assert 2 <= publish.call_args.kwargs["timeout"] <= 3


@pytest.mark.asyncio
async def test_a_pull_stopped_while_checking_projects_says_so(mocker):
    import asyncio

    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    started = asyncio.Event()

    async def hang(integration, projects, **kwargs):
        started.set()
        await asyncio.Event().wait()

    mocker.patch("app.actions.handlers._drop_unknown_projects", side_effect=hang)
    mock_log = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    task = asyncio.ensure_future(action_pull_events(
        _lock_test_integration(), PullEventsConfig(days_to_load=3, projects=["p"])
    ))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    title = mock_log.call_args.kwargs["title"]
    assert "while checking the saved projects on iNaturalist" in title
    assert "Reading from iNaturalist hadn't finished" in title


@pytest.mark.asyncio
async def test_the_pull_clock_starts_before_the_started_entry_and_the_lock(mocker):
    """The time limit covers the "started" publish and the lock, so the pull's clock does too."""
    import asyncio

    from app.actions import handlers

    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    _mock_read(mocker, {})
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())

    async def slow_started(**kwargs):
        await asyncio.sleep(0.3)

    mocker.patch("app.services.activity_logger.publish_event", side_effect=slow_started)
    seen = {}
    real_init = handlers._PullProgress.__init__

    def spy(self, *args, **kwargs):
        real_init(self, *args, **kwargs)
        seen["elapsed"] = self.elapsed()

    mocker.patch.object(handlers._PullProgress, "__init__", spy)

    await action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]"))

    assert seen["elapsed"] >= 0.3


# --- the iNaturalist reads run in a thread ---


def _blocking(release, started):
    """A synchronous call that holds its thread until released, like pyinaturalist on a slow response."""
    def hold():
        started.set()
        release.wait(timeout=5)

    def pages():
        hold()
        yield from ()

    def call(first, *args, **kwargs):
        # find_existing_projects takes the project list and returns the ones that exist;
        # the observations read is a generator that holds while a page is read.
        if isinstance(first, list):
            hold()
            return first
        return pages()
    return call


@pytest.mark.asyncio
@pytest.mark.parametrize("slow_call, config_kwargs", [
    ("iter_observation_pages", {"bounding_box": "[1, 1, 0, 0]"}),
    ("find_existing_projects", {"projects": ["p"]}),
])
async def test_a_slow_inaturalist_call_does_not_hold_up_other_runs(mocker, slow_call, config_kwargs):
    """pyinaturalist is synchronous: run on the event loop, one slow read would
    stop every other run on the instance until it returned."""
    import asyncio
    import threading

    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    _mock_read(mocker, {})
    mocker.patch("app.actions.handlers.find_existing_projects", return_value=["p"])
    release, started = threading.Event(), threading.Event()
    mocker.patch(f"app.actions.handlers.{slow_call}", side_effect=_blocking(release, started))
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    task = asyncio.ensure_future(action_pull_events(
        _lock_test_integration(), PullEventsConfig(days_to_load=3, **config_kwargs)
    ))
    while not started.is_set():
        await asyncio.sleep(0.01)
    # The loop still runs other work while the call is held.
    await asyncio.wait_for(asyncio.sleep(0.01), timeout=1)
    assert not task.done()
    release.set()
    await asyncio.wait_for(task, timeout=5)


@pytest.mark.asyncio
async def test_a_pull_stopped_during_a_slow_read_says_so_right_away(mocker):
    """The runner's time limit can land while the read is still waiting on iNaturalist."""
    import asyncio
    import threading

    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    release, started = threading.Event(), threading.Event()
    _mock_read(mocker, side_effect=_blocking(release, started))
    mock_log = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    task = asyncio.ensure_future(action_pull_events(
        _lock_test_integration(), PullEventsConfig(days_to_load=3, bounding_box="[1, 1, 0, 0]")
    ))
    try:
        while not started.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
    finally:
        release.set()

    title = mock_log.call_args.kwargs["title"]
    assert "while reading observations from iNaturalist" in title
    assert "Reading from iNaturalist hadn't finished" in title
    mock_state.set_state.assert_not_called()  # the cursor stays where it was
    mock_state.delete_state_if_value.assert_called_once()



# --- the limit of new events and the time budget ---


def _observation(i, minute=None):
    ob = MagicMock()
    ob.id = i
    ob.photos = []
    ob.annotations = []
    ob.created_at = datetime.now(tz=timezone.utc) - timedelta(days=1)
    ob.updated_at = datetime(2026, 10, 29, 8, 0, tzinfo=timezone.utc) + timedelta(minutes=minute or i)
    return ob


def _pull_setup(mocker, count, saved_state=None, pages=None):
    """A pull over `count` observations (one page), or over `pages` (lists of
    observations), with state kept in a dict."""
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    store = dict(saved_state or {})
    mock_state.get_state.side_effect = lambda integration_id, action_id, source_id="no-source": store.get(source_id, {})

    async def set_state(integration_id, action_id, state, source_id="no-source"):
        store[source_id] = state
    mock_state.set_state.side_effect = set_state

    async def delete_state(integration_id, action_id, source_id="no-source"):
        store.pop(source_id, None)
    mock_state.delete_state.side_effect = delete_state
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    if pages is None:
        pages = [[_observation(i) for i in range(1, count + 1)]] if count else []
    pages_read = []

    def read_pages(*args, **kwargs):
        for page in pages:
            pages_read.append(page)
            yield page
    read = mocker.patch("app.actions.handlers.iter_observation_pages", side_effect=read_pages)
    read.pages_read = pages_read
    mocker.patch("app.actions.handlers._transform_inat_to_gundi_event",
                 side_effect=lambda ob, config: {"event_details": {"inat_id": str(ob.id)}})
    send = mocker.patch("app.actions.handlers.send_events_to_gundi",
                        side_effect=lambda events, integration_id: [{"object_id": "x"}] * len(events))

    async def save_sent(response, events, integration, inat_updated_at_map=None, pending_photos=None):
        for event in events:
            store[event["event_details"]["inat_id"]] = {"object_id": "x"}
    mocker.patch("app.actions.handlers.save_events_state", side_effect=save_sent)
    mocker.patch("app.actions.handlers.patch_events", AsyncMock(side_effect=lambda chunk, *a, **k: [{}] * len(chunk)))
    mocker.patch("app.actions.handlers.save_patched_events_state", AsyncMock())
    mock_log = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    return read, send, store, mock_log


def _config(**kwargs):
    return PullEventsConfig(days_to_load=3, bounding_box="[1, 1, 0, 0]", **kwargs)


def _titles(mock_log, level=None):
    return [c.kwargs["title"] for c in mock_log.call_args_list if level is None or c.kwargs["level"] == level]


def _synced(*ids, at="2026-10-29 07:00:00+0000", changed=False):
    """State for observations already in Gundi, last synced at `at`. Their
    stored hash matches the payload _pull_setup builds, unless `changed`."""
    return {str(i): {"object_id": f"g{i}", STATE_INAT_UPDATED_AT_KEY: at,
                     STATE_SENT_HASH_KEY: "stale" if changed else _payload_hash({"event_details": {"inat_id": str(i)}})}
            for i in ids}


def test_the_limit_of_new_events_defaults_to_150():
    from app import settings

    assert settings.INAT_MAX_NEW_EVENTS_PER_PULL == 150


@pytest.mark.asyncio
async def test_a_pull_stops_reading_once_it_has_the_limit_of_new_events(mocker):
    pages = [[_observation(1), _observation(2)], [_observation(3), _observation(4)], [_observation(5)]]
    read, send, store, mock_log = _pull_setup(mocker, 0, pages=pages)

    result = await action_pull_events(_lock_test_integration(), _config(max_new_events_per_run=3))

    assert len(read.pages_read) == 2  # the third page is never asked for
    assert result["result"]["events_extracted"] == 3
    assert store["no-source"][STATE_LAST_RUN_KEY] == "2026-10-29 08:03:00+0000"
    finished = [t for t in _titles(mock_log) if t.startswith("Pull finished")][-1]
    assert finished.endswith("It stopped after 3 new observations, the most one pull sends. "
                             "Observations changed on iNaturalist up to 2026-10-29 08:03 UTC were checked.")


@pytest.mark.asyncio
async def test_updates_found_on_the_way_are_sent_and_dont_count(mocker):
    read, send, store, _ = _pull_setup(mocker, 4, saved_state=_synced(1, 2, changed=True))

    result = await action_pull_events(_lock_test_integration(), _config(max_new_events_per_run=2))

    r = result["result"]
    assert (r["events_extracted"], r["events_updated"], r["photos_attached"]) == (2, 2, 0)


@pytest.mark.asyncio
async def test_observations_already_in_sync_move_the_cursor_past_them(mocker):
    _, send, store, _ = _pull_setup(mocker, 3, saved_state=_synced(1, 2, 3, at="2026-10-29 09:00:00+0000"))

    await action_pull_events(_lock_test_integration(), _config())

    send.assert_not_called()
    assert store["no-source"][STATE_LAST_RUN_KEY] == "2026-10-29 08:03:00+0000"


@pytest.mark.asyncio
async def test_a_pull_under_the_limit_does_not_mention_it(mocker):
    _, _, store, mock_log = _pull_setup(mocker, 3)

    await action_pull_events(_lock_test_integration(), _config())

    assert store["no-source"][STATE_LAST_RUN_KEY] == "2026-10-29 08:03:00+0000"
    assert "limit" not in [t for t in _titles(mock_log) if t.startswith("Pull finished")][-1]


@pytest.mark.asyncio
async def test_the_annotation_filter_leaves_other_observations_out_but_moves_past_them(mocker):
    matching = _observation(2)
    annotation = MagicMock(term=22, value=24)
    matching.annotations = [annotation]
    pages = [[_observation(1), matching, _observation(3)]]
    _, send, store, _ = _pull_setup(mocker, 0, pages=pages)

    await action_pull_events(_lock_test_integration(), _config(annotations={"22": ["24"]}))

    assert [e["event_details"]["inat_id"] for e in send.call_args.kwargs["events"]] == ["2"]
    assert store["no-source"][STATE_LAST_RUN_KEY] == "2026-10-29 08:03:00+0000"


@pytest.mark.asyncio
async def test_a_pull_stops_on_the_time_budget_with_its_progress_saved(mocker):
    """Out of time after the first chunk: the cursor goes to the end of that
    chunk and the run logs info, since the route is catching up."""
    from app.actions import handlers

    mocker.patch.object(handlers, "GUNDI_SUBMISSION_CHUNK_SIZE", 2)
    now = [1000.0]
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=lambda: now[0])
    _, send, store, mock_log = _pull_setup(mocker, 5)
    sent = send.side_effect

    def send_slowly(events, integration_id):
        now[0] += 250  # the first chunk uses up the 240 s budget
        return sent(events, integration_id)
    send.side_effect = send_slowly

    result = await action_pull_events(_lock_test_integration(), _config(include_photos=True))

    assert send.call_count == 1
    assert result["result"]["events_extracted"] == 2
    assert store["no-source"][STATE_LAST_RUN_KEY] == "2026-10-29 08:02:00+0000"
    paused = [t for t in _titles(mock_log, LogLevel.INFO) if t.startswith("Pull stopped after")]
    assert paused and ("Observations changed on iNaturalist up to 2026-10-29 08:02 UTC were checked: "
                       "it sent 2 new iNaturalist observations to Gundi") in paused[0]
    assert not _titles(mock_log, LogLevel.ERROR)


@pytest.mark.asyncio
async def test_a_pull_that_runs_out_of_time_before_any_work_logs_an_error(mocker):
    """No progress at all is a stuck route: an error, so it shows as unhealthy."""
    now = [1000.0]
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=lambda: now[0])
    pages = [[_observation(i) for i in range(1, 6)]]

    def slow_pages():
        now[0] += 250  # reading the page uses up the 240 s budget
        yield from pages
    read, send, store, mock_log = _pull_setup(mocker, 0, pages=slow_pages())

    await action_pull_events(_lock_test_integration(), _config())

    send.assert_not_called()
    assert "no-source" not in store  # the cursor stays where it was
    errors = _titles(mock_log, LogLevel.ERROR)
    assert errors and errors[0].startswith(
        "The pull didn't send anything to Gundi because it ran out of time while ")


def test_the_time_budget_uses_the_tighter_limit(mocker):
    from app.actions.handlers import _pull_time_budget

    mocker.patch("app.actions.handlers.settings.MAX_ACTION_EXECUTION_TIME", 540)
    mocker.patch("app.actions.handlers.settings.INAT_PULL_TIME_LIMIT_SECONDS", 300)
    mocker.patch("app.actions.handlers.settings.INAT_PULL_TIME_MARGIN_SECONDS", 60)
    assert _pull_time_budget() == 240


# --- iNaturalist down ---


def _inat_down_error(kind):
    import requests
    from app.datasource.inaturalist import INatRequestError, INatUnavailableError

    if kind == "unavailable":
        return INatUnavailableError("iNaturalist didn't answer in time")
    response = requests.Response()
    response.status_code = 503 if kind == "503" else 422
    response.reason = "Backend fetch failed" if kind == "503" else "Unprocessable"
    return INatRequestError(f"iNaturalist answered with an error ({response.status_code})",
                            response=response)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["unavailable", "503"])
async def test_inaturalist_down_is_a_warning_until_it_repeats(mocker, kind):
    _, _, store, mock_log = _pull_setup(mocker, 0)
    _mock_read(mocker, side_effect=_inat_down_error(kind))

    for _ in range(2):
        result = await action_pull_events(_lock_test_integration(), _config())
        assert _nothing_sent(result)
    warnings = [t for t in _titles(mock_log, LogLevel.WARNING)
                if t.startswith("The pull didn't get observations from iNaturalist because ")]
    assert len(warnings) == 2
    assert "iNaturalist" in warnings[0].split("because ", 1)[1]
    assert store["inat_down"] == {"runs": 2}

    # The third pull in a row is an error, logged by the pull itself: a raised
    # 5xx would be redelivered and run again at once.
    result = await action_pull_events(_lock_test_integration(), _config())
    assert _nothing_sent(result)
    errors = _titles(mock_log, LogLevel.ERROR)
    assert len(errors) == 1
    assert errors[0].startswith("The pull didn't get observations from iNaturalist because ")
    assert errors[0].endswith(" This is pull 3 in a row with a problem on iNaturalist's side.")
    assert store["inat_down"] == {"runs": 3}


@pytest.mark.asyncio
async def test_a_good_read_resets_the_inaturalist_down_count(mocker):
    _, _, store, _ = _pull_setup(mocker, 1, saved_state={"inat_down": {"runs": 2}})

    await action_pull_events(_lock_test_integration(), _config())

    assert "inat_down" not in store


@pytest.mark.asyncio
async def test_a_rejected_query_is_an_error_right_away(mocker):
    """A 4xx is a problem with the query, not iNaturalist being down."""
    from app.datasource.inaturalist import INatRequestError

    _pull_setup(mocker, 0)
    _mock_read(mocker, side_effect=_inat_down_error("422"))

    with pytest.raises(INatRequestError):
        await action_pull_events(_lock_test_integration(), _config())



@pytest.mark.asyncio
async def test_a_read_that_outlasts_the_time_budget_is_logged_as_running_out_of_time(mocker):
    """The read runs in a thread that can't be stopped, but the run stops
    waiting for it at the budget. iNaturalist may be answering fine, so the
    log says the pull ran out of time, not that iNaturalist had a problem."""
    import asyncio
    import threading

    _, _, store, mock_log = _pull_setup(mocker, 0)
    release, started = threading.Event(), threading.Event()
    _mock_read(mocker, side_effect=_blocking(release, started))
    mocker.patch("app.actions.handlers._pull_time_budget", return_value=0.2)
    try:
        result = await asyncio.wait_for(action_pull_events(_lock_test_integration(), _config()), timeout=5)
    finally:
        release.set()

    assert _nothing_sent(result)
    assert not [t for t in _titles(mock_log) if "didn't get observations from iNaturalist" in t]
    errors = _titles(mock_log, LogLevel.ERROR)
    assert errors == ["The pull didn't send anything to Gundi because it ran out of time while reading "
                      "observations from iNaturalist, before any were ready to send."]
    assert "inat_down" not in store
    assert "no-source" not in store  # the cursor stays where it was


@pytest.mark.asyncio
async def test_a_project_check_that_outlasts_the_time_budget_uses_the_saved_projects(mocker):
    import asyncio
    import threading

    read, _, _, _ = _pull_setup(mocker, 0)
    release, started = threading.Event(), threading.Event()
    mocker.patch("app.actions.handlers.find_existing_projects", side_effect=_blocking(release, started))
    time_left = iter([0.2] + [240] * 10)  # short for the project check, enough for the read
    mocker.patch("app.actions.handlers._time_left", side_effect=lambda deadline: next(time_left))
    try:
        await asyncio.wait_for(
            action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, projects=["p"])), timeout=5)
    finally:
        release.set()

    assert read.call_args.kwargs["projects"] == ["p"]



@pytest.mark.asyncio
async def test_each_page_is_sent_before_the_next_is_read(mocker):
    """Updates don't count toward the limit, so a run sends each page as it
    reads it and saves its place, rather than reading until time runs out."""
    pages = [[_observation(1), _observation(2)], [_observation(3)]]
    order = []
    read, send, store, _ = _pull_setup(mocker, 0, pages=pages)
    sent = send.side_effect

    def record(events, integration_id):
        order.append(("sent", [e["event_details"]["inat_id"] for e in events], len(read.pages_read)))
        return sent(events, integration_id)
    send.side_effect = record

    await action_pull_events(_lock_test_integration(), _config())

    assert order == [("sent", ["1", "2"], 1), ("sent", ["3"], 2)]


@pytest.mark.asyncio
async def test_an_observation_repeated_across_pages_is_sent_once(mocker):
    """iNat's 10,000-result windows overlap at their boundary timestamp."""
    boundary = _observation(2)
    pages = [[_observation(1), boundary], [boundary, _observation(3)]]
    _, send, _, _ = _pull_setup(mocker, 0, pages=pages)

    result = await action_pull_events(_lock_test_integration(), _config())

    sent = [e["event_details"]["inat_id"] for c in send.call_args_list for e in c.kwargs["events"]]
    assert sent == ["1", "2", "3"]
    assert result["result"]["events_extracted"] == 3


@pytest.mark.asyncio
async def test_inaturalist_failing_after_some_pages_keeps_what_was_sent(mocker):
    from app.datasource.inaturalist import INatUnavailableError

    def pages():
        yield [_observation(1), _observation(2)]
        raise INatUnavailableError("the connection to iNaturalist dropped")
    _, send, store, mock_log = _pull_setup(mocker, 0, pages=pages())

    result = await action_pull_events(_lock_test_integration(), _config())

    assert result["result"]["events_extracted"] == 2
    assert store["no-source"][STATE_LAST_RUN_KEY] == "2026-10-29 08:02:00+0000"
    assert _titles(mock_log, LogLevel.WARNING)[-1].startswith(
        "The pull didn't get observations from iNaturalist because the connection to iNaturalist dropped.")


@pytest.mark.asyncio
async def test_an_outage_notice_that_cant_be_published_doesnt_fail_the_pull(mocker):
    _pull_setup(mocker, 0)
    _mock_read(mocker, side_effect=_inat_down_error("unavailable"))
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock(side_effect=RuntimeError("publisher down")))

    result = await action_pull_events(_lock_test_integration(), _config())

    assert _nothing_sent(result)


# --- photos: pull_events hands them to a photo run ---


def _photo_obs_for_pull(i, photos):
    ob = _observation(i)
    ob.photos = [MagicMock(id=i * 100 + n, large_url=f"https://photos/{i}-{n}.jpg") for n in range(photos)]
    return ob


@pytest.mark.asyncio
async def test_new_events_are_recorded_with_their_photos_pending_and_a_photo_run_started(mocker, mock_trigger_action):
    from app.actions.handlers import save_events_state

    pages = [[_photo_obs_for_pull(1, 2), _photo_obs_for_pull(2, 0)]]
    _, _, store, mock_log = _pull_setup(mocker, 0, pages=pages)
    mocker.patch("app.actions.handlers.save_events_state", side_effect=save_events_state)

    result = await action_pull_events(_lock_test_integration(), _config(include_photos=True, max_photos=5))

    r = result["result"]
    assert (r["events_extracted"], r["events_updated"], r["photos_attached"], r["photos_left_out"]) == (2, 0, 0, 0)
    # The photo list lives in its own record, apart from the observation's state.
    assert store["photos.1"] == {"object_id": "x", "pending": [[100, "https://photos/1-0.jpg"],
                                                              [101, "https://photos/1-1.jpg"]]}
    assert "photos.2" not in store and "photos_pending" not in store["1"]
    integration_id, action_id, config = mock_trigger_action.call_args.args
    assert action_id == "add_photos" and config.observation_ids == ["1"]
    finished = [t for t in _titles(mock_log) if t.startswith("Pull finished")][-1]
    assert "and handed their 2 photos to a separate photo run" in finished


@pytest.mark.asyncio
async def test_without_photos_no_photo_run_is_started(mocker, mock_trigger_action):
    from app.actions.handlers import save_events_state

    _, _, store, _ = _pull_setup(mocker, 0, pages=[[_photo_obs_for_pull(1, 2)]])
    mocker.patch("app.actions.handlers.save_events_state", side_effect=save_events_state)

    await action_pull_events(_lock_test_integration(), _config(include_photos=False))

    mock_trigger_action.assert_not_called()
    assert "photos.1" not in store


@pytest.mark.asyncio
async def test_an_update_leaves_the_photo_list_alone(mocker):
    """The pull and the photo run never write the same record."""
    from app.actions.handlers import save_patched_events_state

    photos = {"object_id": "g1", "pending": [[100, "https://photos/1-0.jpg"]]}
    saved = {"1": {"object_id": "g1", STATE_INAT_UPDATED_AT_KEY: "2026-10-29 07:00:00+0000"}, "photos.1": photos}
    _, _, store, _ = _pull_setup(mocker, 1, saved_state=saved)
    mocker.patch("app.actions.handlers.save_patched_events_state", side_effect=save_patched_events_state)

    result = await action_pull_events(_lock_test_integration(), _config())

    assert result["result"]["events_updated"] == 1
    assert store["photos.1"] == photos
    assert store["1"][STATE_INAT_UPDATED_AT_KEY] == "2026-10-29 08:01:00+0000"


@pytest.mark.asyncio
async def test_a_pull_that_finds_photos_left_behind_starts_a_photo_run_for_them(mocker, mock_trigger_action):
    """If a pull stopped between recording events and starting their photo run,
    the next pull reads them again and starts it."""
    saved = {**_synced(1, 2, at="2026-10-29 09:00:00+0000"),
             "photos.2": {"object_id": "g2", "pending": [[200, "https://photos/2-0.jpg"]]}}
    _, send, _, _ = _pull_setup(mocker, 2, saved_state=saved)

    await action_pull_events(_lock_test_integration(), _config())

    send.assert_not_called()
    integration_id, action_id, config = mock_trigger_action.call_args.args
    assert action_id == "add_photos" and config.observation_ids == ["2"]


@pytest.mark.asyncio
async def test_updates_check_the_time_one_at_a_time(mocker):
    """A chunk can hold 100 updates; the run stops between them and keeps those done."""
    now = [1000.0]
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=lambda: now[0])
    _, _, store, _ = _pull_setup(mocker, 3, saved_state=_synced(1, 2, 3, changed=True))

    async def slow_patch(chunk, *args, **kwargs):
        now[0] += 250  # the first update uses up the 240 s budget
        return [{}] * len(chunk)
    patch = mocker.patch("app.actions.handlers.patch_events", AsyncMock(side_effect=slow_patch))
    saved = mocker.patch("app.actions.handlers.save_patched_events_state", AsyncMock())

    result = await action_pull_events(_lock_test_integration(), _config())

    assert patch.call_count == 1 and saved.call_count == 1
    assert result["result"]["events_updated"] == 1
    assert "no-source" not in store  # the chunk isn't done, so the cursor doesn't pass it


# --- photos: the add_photos run ---


def _photo_run_setup(mocker, notes, statuses=None, upload_error=None, locked=()):
    """A photo run over `notes` ({obs_id: [[photo_id, url], ...]}), with state in a dict.
    statuses maps a url to the download's HTTP status (200 by default) or an exception."""
    from app.actions.handlers import STATE_PHOTOS_LOCK_PREFIX

    store = {f"photos.{obs_id}": {"object_id": f"g{obs_id}", "pending": list(photos)}
             for obs_id, photos in notes.items()}
    for obs_id in locked:
        store[STATE_PHOTOS_LOCK_PREFIX + obs_id] = "someone-else"
    mock_state = AsyncMock()
    mock_state.get_state.side_effect = lambda i, a, source_id="no-source": dict(store.get(source_id) or {})

    async def set_state(i, a, state, source_id="no-source"):
        store[source_id] = state
    mock_state.set_state.side_effect = set_state

    async def set_if_absent(i, a, ttl_seconds=None, source_id="no-source", value=None):
        if source_id in store:
            return False
        store[source_id] = value
        return True
    mock_state.set_if_absent.side_effect = set_if_absent

    async def delete_if_value(i, a, value, source_id="no-source"):
        if store.get(source_id) == value:
            store.pop(source_id)
            return True
        return False
    mock_state.delete_state_if_value.side_effect = delete_if_value

    async def delete_state(i, a, source_id="no-source"):
        store.pop(source_id, None)
    mock_state.delete_state.side_effect = delete_state
    mocker.patch("app.actions.handlers.state_manager", mock_state)

    statuses = statuses or {}

    async def get(url):
        outcome = statuses.get(url, 200)
        if isinstance(outcome, BaseException):
            raise outcome
        response = MagicMock(status_code=outcome, content=b"\xff\xd8-raw-jpeg-bytes")
        if outcome >= 400:
            import httpx
            response.raise_for_status.side_effect = httpx.HTTPStatusError(
                "error", request=MagicMock(), response=MagicMock(status_code=outcome))
        return response
    client = MagicMock()
    client.get = AsyncMock(side_effect=get)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    mocker.patch("app.actions.handlers.httpx.AsyncClient", return_value=client)
    upload = mocker.patch("app.actions.handlers.send_event_attachments_to_gundi",
                          AsyncMock(side_effect=upload_error, return_value=[{}]))
    mock_log = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    mocker.patch("app.actions.handlers.asyncio.sleep", AsyncMock())
    return store, upload, mock_log


def _photos_config(*ids, runs_without_progress=0):
    from app.actions.configurations import AddPhotosConfig
    return AddPhotosConfig(observation_ids=list(ids), runs_without_progress=runs_without_progress)


@pytest.mark.asyncio
async def test_a_photo_run_adds_each_pending_photo_once_and_empties_the_list(mocker, mock_trigger_action):
    from app.actions.handlers import action_add_photos

    store, upload, mock_log = _photo_run_setup(mocker, {
        "1": [[100, "https://p/100.jpg"], [101, "https://p/101.jpg"]], "2": [[200, "https://p/200.jpg"]]})

    result = await action_add_photos(_lock_test_integration(), _photos_config("1", "2"))

    assert result["result"] == {"photos_attached": 3, "photos_not_available": 0, "observations_left": 0,
                                "photos_left": 0}
    assert [c.kwargs["event_id"] for c in upload.call_args_list] == ["g1", "g1", "g2"]
    assert [c.kwargs["attachments"][0][0] for c in upload.call_args_list] == ["100.jpg", "101.jpg", "200.jpg"]
    assert "photos.1" not in store and "photos.2" not in store  # finished records are removed
    mock_trigger_action.assert_not_called()
    assert _titles(mock_log, LogLevel.INFO)[-1].startswith("Added 3 photos to iNaturalist observations in Gundi in ")


@pytest.mark.asyncio
async def test_a_photo_run_that_runs_out_of_time_saves_its_place_and_starts_another(mocker, mock_trigger_action):
    """The photos already added are off the list, so the next run doesn't add them twice."""
    from app.actions.handlers import action_add_photos

    now = [1000.0]
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=lambda: now[0])
    store, upload, mock_log = _photo_run_setup(mocker, {
        "1": [[100, "https://p/100.jpg"], [101, "https://p/101.jpg"]], "2": [[200, "https://p/200.jpg"]]})

    async def slow_upload(**kwargs):
        now[0] += 250  # the first photo uses up the 240 s budget
        return [{}]
    upload.side_effect = slow_upload

    result = await action_add_photos(_lock_test_integration(), _photos_config("1", "2"))

    assert result["result"]["photos_attached"] == 1
    assert store["photos.1"]["pending"] == [[101, "https://p/101.jpg"]]
    integration_id, action_id, config = mock_trigger_action.call_args.args
    assert action_id == "add_photos"
    assert config.observation_ids == ["1", "2"] and config.runs_without_progress == 0
    info = _titles(mock_log, LogLevel.INFO)[-1]
    assert info.endswith("2 photos for 2 observations are still to add, and another photo run was started for them.")
    assert result["result"]["photos_left"] == 2


@pytest.mark.asyncio
async def test_a_photo_iNaturalist_no_longer_has_is_dropped_with_a_note(mocker, mock_trigger_action):
    from app.actions.handlers import action_add_photos

    store, upload, mock_log = _photo_run_setup(
        mocker, {"1": [[100, "https://p/100.jpg"], [101, "https://p/101.jpg"]]}, statuses={"https://p/100.jpg": 404})

    result = await action_add_photos(_lock_test_integration(), _photos_config("1"))

    assert result["result"] == {"photos_attached": 1, "photos_not_available": 1, "observations_left": 0,
                                "photos_left": 0}
    assert "photos.1" not in store
    mock_trigger_action.assert_not_called()
    assert ("1 of the photos couldn't be added because iNaturalist no longer has them or their event is "
            "no longer in Gundi.") in _titles(mock_log, LogLevel.INFO)[-1]


@pytest.mark.asyncio
async def test_a_photo_that_fails_for_now_stays_pending_and_is_tried_again(mocker, mock_trigger_action):
    """No photo lost: it stays on the list, the run waits and tries again while it
    has time, and a new photo run carries it on."""
    import httpx
    from app.actions.handlers import action_add_photos

    now = [1000.0]
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=lambda: now[0])
    store, upload, mock_log = _photo_run_setup(
        mocker, {"1": [[100, "https://p/100.jpg"]]}, statuses={"https://p/100.jpg": httpx.ConnectError("down")})
    sleep = mocker.patch("app.actions.handlers.asyncio.sleep", AsyncMock(side_effect=lambda s: now.__setitem__(0, now[0] + 200)))

    result = await action_add_photos(_lock_test_integration(), _photos_config("1", runs_without_progress=2))

    assert sleep.call_args_list and all(c.args == (30,) for c in sleep.call_args_list)
    assert result["result"]["photos_attached"] == 0
    assert store["photos.1"]["pending"] == [[100, "https://p/100.jpg"]]
    config = mock_trigger_action.call_args.args[2]
    assert config.observation_ids == ["1"] and config.runs_without_progress == 3
    assert _titles(mock_log, LogLevel.WARNING)[-1] == (
        "No photos were added to iNaturalist observations in Gundi because the connection to iNaturalist's "
        "photo server dropped. 1 photo for 1 observation is still to add, and another photo run was started for them.")


@pytest.mark.asyncio
async def test_photo_runs_that_keep_failing_turn_into_an_error(mocker, mock_trigger_action):
    import httpx
    from app.actions.handlers import PHOTO_RUNS_BEFORE_ERROR, action_add_photos

    _photo_run_setup(mocker, {"1": [[100, "https://p/100.jpg"]]},
                     statuses={"https://p/100.jpg": httpx.ConnectError("down")})
    mock_log = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    now = [1000.0]
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=lambda: now[0])
    mocker.patch("app.actions.handlers.asyncio.sleep", AsyncMock(side_effect=lambda s: now.__setitem__(0, now[0] + 300)))

    await action_add_photos(_lock_test_integration(),
                            _photos_config("1", runs_without_progress=PHOTO_RUNS_BEFORE_ERROR - 1))

    errors = _titles(mock_log, LogLevel.ERROR)
    assert errors and errors[-1].endswith(f"This is photo run {PHOTO_RUNS_BEFORE_ERROR} in a row that couldn't add any.")
    assert mock_trigger_action.called  # it keeps going until the photos are in


@pytest.mark.asyncio
async def test_a_photo_run_skips_an_observation_another_run_holds_but_keeps_it_on_the_list(mocker, mock_trigger_action):
    """Only one run adds an observation's photos. The other keeps it on its list,
    so it is finished even if the run holding it died."""
    from app.actions.handlers import action_add_photos

    now = [1000.0]
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=lambda: now[0])
    store, upload, _ = _photo_run_setup(mocker, {"1": [[100, "https://p/100.jpg"]], "2": [[200, "https://p/200.jpg"]]},
                                        locked=("1",))
    mocker.patch("app.actions.handlers.asyncio.sleep", AsyncMock(side_effect=lambda s: now.__setitem__(0, now[0] + 300)))

    result = await action_add_photos(_lock_test_integration(), _photos_config("1", "2"))

    assert [c.kwargs["event_id"] for c in upload.call_args_list] == ["g2"]
    assert result["result"]["observations_left"] == 1
    assert mock_trigger_action.call_args.args[2].observation_ids == ["1"]


@pytest.mark.asyncio
async def test_a_photo_run_with_too_little_time_to_wait_hands_off_instead_of_retrying(mocker, mock_trigger_action):
    import httpx
    from app.actions.handlers import action_add_photos

    now = [1000.0]
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=lambda: now[0])
    _, upload, _ = _photo_run_setup(mocker, {"1": [[100, "https://p/100.jpg"]]},
                                    statuses={"https://p/100.jpg": httpx.ConnectError("down")})
    client_get = mocker.patch("app.actions.handlers._add_photo", AsyncMock(side_effect=httpx.ConnectError("down")))
    mocker.patch("app.actions.handlers._pull_time_budget", return_value=20)  # less than the 30 s wait
    sleep = mocker.patch("app.actions.handlers.asyncio.sleep", AsyncMock())

    await action_add_photos(_lock_test_integration(), _photos_config("1"))

    assert client_get.call_count == 1
    sleep.assert_not_called()
    assert mock_trigger_action.call_args.args[2].observation_ids == ["1"]


@pytest.mark.asyncio
async def test_a_photo_gundi_refuses_stays_pending(mocker, mock_trigger_action):
    """A 403 from Gundi is about permissions, not a missing event: fixed later, the photo still goes in."""
    from gundi_client_v2.errors import GundiAPIError
    from app.actions.handlers import action_add_photos

    now = [1000.0]
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=lambda: now[0])
    store, _, _ = _photo_run_setup(mocker, {"1": [[100, "https://p/100.jpg"]]},
                                   upload_error=GundiAPIError(403, "Forbidden"))
    mocker.patch("app.actions.handlers.asyncio.sleep", AsyncMock(side_effect=lambda s: now.__setitem__(0, now[0] + 300)))

    result = await action_add_photos(_lock_test_integration(), _photos_config("1"))

    assert result["result"]["photos_not_available"] == 0
    assert store["photos.1"]["pending"] == [[100, "https://p/100.jpg"]]


@pytest.mark.asyncio
async def test_an_upload_never_waits_past_the_hard_limit(mocker, mock_trigger_action):
    import asyncio
    from app.actions.handlers import action_add_photos

    store, upload, _ = _photo_run_setup(mocker, {"1": [[100, "https://p/100.jpg"]]})
    mocker.patch("app.actions.handlers.settings.INAT_PULL_TIME_LIMIT_SECONDS", 10.2)  # hard limit 0.2 s away
    mocker.patch("app.actions.handlers._pull_time_budget", return_value=0.15)

    async def stuck(**kwargs):
        await asyncio.Event().wait()
    upload.side_effect = stuck

    result = await asyncio.wait_for(action_add_photos(_lock_test_integration(), _photos_config("1")), timeout=3)

    assert result["result"]["photos_attached"] == 0
    assert store["photos.1"]["pending"] == [[100, "https://p/100.jpg"]]


@pytest.mark.asyncio
async def test_a_photo_run_never_logs_photo_bytes(mocker, caplog):
    import logging
    from app.actions.handlers import action_add_photos

    _photo_run_setup(mocker, {"1": [[100, "https://p/100.jpg"]]}, upload_error=RuntimeError("upload failed"))
    now = [1000.0]
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=lambda: now[0])
    mocker.patch("app.actions.handlers.asyncio.sleep", AsyncMock(side_effect=lambda s: now.__setitem__(0, now[0] + 300)))

    with caplog.at_level(logging.DEBUG):
        await action_add_photos(_lock_test_integration(), _photos_config("1"))

    assert "raw-jpeg-bytes" not in caplog.text


def test_photo_runs_go_to_the_actions_topic_the_infra_creates(monkeypatch):
    """Deployed runners don't set INTEGRATION_COMMANDS_TOPIC; without a fallback,
    starting a photo run would fail on every pull with new photos."""
    import importlib
    from app.settings import integration

    monkeypatch.delenv("INTEGRATION_COMMANDS_TOPIC", raising=False)
    assert importlib.reload(integration).INTEGRATION_COMMANDS_TOPIC == "inaturalist-actions-topic"
    monkeypatch.setenv("INTEGRATION_COMMANDS_TOPIC", "another-topic")
    assert importlib.reload(integration).INTEGRATION_COMMANDS_TOPIC == "another-topic"
    monkeypatch.delenv("INTEGRATION_COMMANDS_TOPIC")
    importlib.reload(integration)


# --- pull limits: upload age, payload comparison, photo limit, counters ---


def _sort_state(mocker, saved=None, photos=None):
    mock_state = AsyncMock()
    store = {}
    if saved is not None:
        store["1"] = saved
    if photos is not None:
        store["photos.1"] = photos
    mock_state.get_state.side_effect = lambda integration_id, action_id, source_id="no-source": store.get(source_id)
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    return mock_state


def _sortable(created_days_ago, now, taxon="Puma concolor"):
    ob = MagicMock()
    ob.id = 1
    ob.created_at = now - timedelta(days=created_days_ago)
    return ob, {"event_details": {"inat_id": "1", "taxon_name": taxon}}


async def _sort(mocker, ob, payload, now, **config):
    mocker.patch("app.actions.handlers._transform_inat_to_gundi_event", side_effect=lambda o, c: payload)
    new_ids, targets, left_behind = set(), {}, set()
    outcome = await _sort_observation(_lock_test_integration(), _config(**config), ob, new_ids, targets,
                                      left_behind, now=now)
    return outcome, new_ids, targets, left_behind


NOW = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_sort_observation_skips_old_upload_without_storing(mocker):
    mock_state = _sort_state(mocker)
    ob, payload = _sortable(400, NOW)

    outcome, new_ids, targets, left_behind = await _sort(mocker, ob, payload, NOW)

    assert outcome == "too_old"
    assert not new_ids and not targets and not left_behind
    mock_state.get_state.assert_not_called()
    mock_state.set_state.assert_not_called()


@pytest.mark.asyncio
async def test_sort_observation_skips_old_upload_even_when_in_state(mocker):
    ob, payload = _sortable(400, NOW)
    _sort_state(mocker, saved={"object_id": "g1", STATE_SENT_HASH_KEY: "stale"})

    outcome, _, targets, _ = await _sort(mocker, ob, payload, NOW)

    assert outcome == "too_old" and not targets


@pytest.mark.asyncio
async def test_sort_observation_age_limit_comes_from_the_connection(mocker):
    _sort_state(mocker)
    ob, payload = _sortable(400, NOW)

    outcome, new_ids, _, _ = await _sort(mocker, ob, payload, NOW, max_age_days=500)

    assert outcome == "new" and new_ids == {1}


@pytest.mark.asyncio
async def test_sort_observation_creates_recent_upload_and_reads_naive_dates_as_utc(mocker):
    _sort_state(mocker)
    ob, payload = _sortable(89, NOW)
    ob.created_at = ob.created_at.replace(tzinfo=None)

    outcome, new_ids, _, _ = await _sort(mocker, ob, payload, NOW)

    assert outcome == "new" and new_ids == {1}


@pytest.mark.asyncio
async def test_sort_observation_skips_unchanged_payload(mocker):
    """A comment or a favourite moves updated_at but changes nothing we send."""
    ob, payload = _sortable(10, NOW)
    mock_state = _sort_state(mocker, saved={"object_id": "g1", STATE_INAT_UPDATED_AT_KEY: "2000-01-01 00:00:00+0000",
                                            STATE_SENT_HASH_KEY: _payload_hash(payload)})

    outcome, new_ids, targets, _ = await _sort(mocker, ob, payload, NOW)

    assert outcome == "unchanged" and not new_ids and not targets
    mock_state.set_state.assert_not_called()


@pytest.mark.asyncio
async def test_sort_observation_updates_changed_payload(mocker):
    ob, payload = _sortable(10, NOW, taxon="Panthera onca")
    _, old_payload = _sortable(10, NOW, taxon="Felidae")
    _sort_state(mocker, saved={"object_id": "g1", STATE_SENT_HASH_KEY: _payload_hash(old_payload)})

    outcome, _, targets, _ = await _sort(mocker, ob, payload, NOW)

    assert outcome == "update" and targets == {1: "g1"}


@pytest.mark.asyncio
async def test_sort_observation_patches_row_without_hash(mocker):
    """Rows saved before sent_hash existed are patched once, which stores the hash."""
    ob, payload = _sortable(10, NOW)
    _sort_state(mocker, saved={"object_id": "g1", STATE_INAT_UPDATED_AT_KEY: "2026-11-01 00:00:00+0000"})

    outcome, _, targets, _ = await _sort(mocker, ob, payload, NOW)

    assert outcome == "update" and targets == {1: "g1"}


@pytest.mark.asyncio
async def test_sort_observation_resumes_pending_photos(mocker):
    ob, payload = _sortable(10, NOW)
    _sort_state(mocker, saved={"object_id": "g1", STATE_SENT_HASH_KEY: _payload_hash(payload)},
                photos={"object_id": "g1", "pending": [[1, "u"]]})

    outcome, _, _, left_behind = await _sort(mocker, ob, payload, NOW)

    assert outcome == "unchanged" and left_behind == {1}


def test_payload_hash_is_stable_and_order_independent():
    a = {"title": "x", "event_details": {"taxon_id": 1, "created_at": datetime(2026, 1, 1)}}
    b = {"event_details": {"created_at": datetime(2026, 1, 1), "taxon_id": 1}, "title": "x"}
    assert _payload_hash(a) == _payload_hash(b)
    assert _payload_hash(a) != _payload_hash({**a, "title": "y"})


def test_transform_inat_to_gundi_event_has_no_updated_at():
    ob = _make_observation()
    event = _transform_inat_to_gundi_event(ob, PullEventsConfig(bounding_box="[1, 1, 0, 0]", event_prefix="iNat: "))
    assert "updated_at" not in event["event_details"]
    assert "created_at" in event["event_details"]


@pytest.mark.asyncio
@pytest.mark.parametrize("max_photos, handed, left_out, run_started", [
    (1, 1, 2, True), (0, 0, 3, False), (5, 3, 0, True),
])
async def test_a_pull_hands_the_first_max_photos_to_the_photo_run(
        mocker, mock_trigger_action, max_photos, handed, left_out, run_started):
    from app.actions.handlers import save_events_state

    _, _, store, mock_log = _pull_setup(mocker, 0, pages=[[_photo_obs_for_pull(1, 3)]])
    mocker.patch("app.actions.handlers.save_events_state", side_effect=save_events_state)

    result = await action_pull_events(_lock_test_integration(), _config(include_photos=True, max_photos=max_photos))

    assert len(store.get("photos.1", {}).get("pending", [])) == handed
    if handed:
        assert store["photos.1"]["pending"][0] == [100, "https://photos/1-0.jpg"]  # the first one, in order
    assert result["result"]["photos_left_out"] == left_out
    assert mock_trigger_action.called is run_started
    finished = [t for t in _titles(mock_log) if t.startswith("Pull finished")][-1]
    if handed and left_out:
        assert f"({left_out} left out by the photo limit)" in finished
    elif left_out:
        assert f"and left out their {left_out} photos because of the photo limit" in finished
    else:
        assert "left out" not in finished


@pytest.mark.asyncio
async def test_new_events_store_the_hash_of_what_was_sent(mocker):
    from app.actions.handlers import save_events_state

    _, _, store, _ = _pull_setup(mocker, 1)
    mocker.patch("app.actions.handlers.save_events_state", side_effect=save_events_state)

    await action_pull_events(_lock_test_integration(), _config())

    assert store["1"][STATE_SENT_HASH_KEY] == _payload_hash({"event_details": {"inat_id": "1"}})


@pytest.mark.asyncio
async def test_an_update_stores_the_hash_of_what_was_sent(mocker):
    from app.actions.handlers import save_patched_events_state

    _, _, store, _ = _pull_setup(mocker, 1, saved_state=_synced(1, changed=True))
    mocker.patch("app.actions.handlers.save_patched_events_state", side_effect=save_patched_events_state)

    result = await action_pull_events(_lock_test_integration(), _config())

    assert result["result"]["events_updated"] == 1
    assert store["1"]["object_id"] == "g1"
    assert store["1"][STATE_SENT_HASH_KEY] == _payload_hash({"event_details": {"inat_id": "1"}})


@pytest.mark.asyncio
async def test_a_pull_skips_old_uploads_and_moves_the_cursor_past_them(mocker):
    old = _observation(2)
    old.created_at = datetime.now(tz=timezone.utc) - timedelta(days=400)
    _, send, store, mock_log = _pull_setup(mocker, 0, pages=[[_observation(1), old]])

    result = await action_pull_events(_lock_test_integration(), _config())

    r = result["result"]
    assert (r["events_extracted"], r["events_skipped_too_old"]) == (1, 1)
    assert "2" not in store and "photos.2" not in store
    assert store["no-source"][STATE_LAST_RUN_KEY] == "2026-10-29 08:02:00+0000"
    finished = [t for t in _titles(mock_log) if t.startswith("Pull finished")][-1]
    assert "and skipped 1 uploaded more than 90 days ago" in finished


@pytest.mark.asyncio
async def test_a_pull_skips_observations_with_nothing_changed(mocker):
    _, send, _, mock_log = _pull_setup(mocker, 2, saved_state=_synced(1, 2))
    patch = mocker.patch("app.actions.handlers.patch_events", AsyncMock())

    result = await action_pull_events(_lock_test_integration(), _config())

    r = result["result"]
    assert (r["events_updated"], r["events_skipped_unchanged"]) == (0, 2)
    send.assert_not_called()
    patch.assert_not_called()
    finished = [t for t in _titles(mock_log) if t.startswith("Pull finished")][-1]
    assert "and skipped 2 with nothing changed" in finished


@pytest.mark.asyncio
async def test_a_pull_reports_how_far_behind_its_cursor_is(mocker):
    _, _, _, _ = _pull_setup(mocker, 1)

    result = await action_pull_events(_lock_test_integration(), _config())

    lag = result["result"]["cursor_lag_days"]
    expected = (datetime.now(tz=timezone.utc) - datetime(2026, 10, 29, 8, 1, tzinfo=timezone.utc)).total_seconds() / 86400
    assert isinstance(lag, float) and abs(lag - expected) <= 0.1


def test_progress_counts_mention_only_what_happened():
    progress = _PullProgress(max_age_days=90)
    progress.events_sent = 2
    assert "skipped" not in progress.counts() and "left out" not in progress.counts()

    progress.skipped_too_old = 3
    assert progress.counts().endswith("and updated 0, and skipped 3 uploaded more than 90 days ago")

    progress.skipped_unchanged = 4
    progress.photos_handed_off = 2
    progress.photos_left_out = 5
    assert progress.counts() == (
        "it sent 2 new iNaturalist observations to Gundi and updated 0, and skipped 3 uploaded more than "
        "90 days ago and 4 with nothing changed, and handed their 2 photos to a separate photo run "
        "(5 left out by the photo limit)")
    assert {"events_skipped_too_old": 3, "events_skipped_unchanged": 4, "photos_left_out": 5}.items() \
        <= progress.data().items()


def test_progress_counts_mention_photos_left_out_when_none_were_handed_off():
    progress = _PullProgress(max_age_days=90)
    progress.events_sent = 1
    progress.photos_left_out = 1
    assert progress.counts().endswith("and left out their 1 photo because of the photo limit")


@pytest.mark.asyncio
async def test_skips_are_counted_only_for_chunks_whose_cursor_was_saved(mocker):
    """A chunk cut off by the time budget is read again next run, so its skips
    are counted then, not twice."""
    now = [1000.0]
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=lambda: now[0])
    mocker.patch("app.actions.handlers.GUNDI_SUBMISSION_CHUNK_SIZE", 2)
    page = [_observation(1), _observation(2), _observation(3), _observation(4)]
    _, _, store, _ = _pull_setup(mocker, 0, pages=[page], saved_state={**_synced(1, 3), **_synced(2, 4, changed=True)})

    async def slow_patch(chunk, *args, **kwargs):
        now[0] += 250  # the first update uses up the 240 s budget
        return [{}] * len(chunk)
    mocker.patch("app.actions.handlers.patch_events", AsyncMock(side_effect=slow_patch))

    result = await action_pull_events(_lock_test_integration(), _config())

    # Chunk [1, 2]: 1 unchanged, 2 updated, cursor saved. Chunk [3, 4] never finished.
    assert result["result"]["events_skipped_unchanged"] == 1
    assert store["no-source"][STATE_LAST_RUN_KEY] == "2026-10-29 08:02:00+0000"


@pytest.mark.asyncio
async def test_a_pull_never_reads_from_further_back_than_days_to_load(mocker):
    _, _, _, _ = _pull_setup(mocker, 1, saved_state={"no-source": {STATE_LAST_RUN_KEY: "2026-06-22 18:52:00+0000"}})
    read = mocker.patch("app.actions.handlers.iter_observation_pages", side_effect=lambda *a, **k: iter([]))

    await action_pull_events(_lock_test_integration(),
                             PullEventsConfig(days_to_load=7, bounding_box="[1, 1, 0, 0]"))

    since = read.call_args.args[0]
    floor = datetime.now(tz=timezone.utc) - timedelta(days=7)
    assert abs((since - floor).total_seconds()) < 60


def _inat_json(updated_at, taxon_id=41944, comments=0):
    created = (datetime.now(tz=timezone.utc) - timedelta(days=3)).replace(microsecond=0)
    return {
        "id": 777, "observed_on": created.date().isoformat(), "created_at": created.isoformat(),
        "updated_at": updated_at.isoformat(), "captive": False, "obscured": False, "quality_grade": "needs_id",
        "species_guess": "Puma", "uri": "https://www.inaturalist.org/observations/777", "photos": [],
        "user": {"id": 5, "login": "obs", "name": "Observer"}, "location": [19.4, -99.1],
        "place_ids": [1, 6793, 9853], "comments_count": comments, "annotations": [],
        "taxon": {"id": taxon_id, "name": "Puma concolor", "rank": "species", "preferred_common_name": "Puma",
                  "ancestor_ids": [48460, 1, 2, 355675, 40151, 848317, 41573, 41944]},
    }


@pytest.mark.asyncio
async def test_created_then_touched_without_changes_is_not_updated(mocker):
    """Round trip with real observations: the hash stored at creation matches
    the payload rebuilt next run from the same data with a newer updated_at."""
    from pyinaturalist import Observation
    from app.actions.handlers import save_events_state

    store = {}
    mock_state = AsyncMock()

    async def set_state(integration_id, action_id, state, source_id="no-source"):
        store[source_id] = state
    mock_state.set_state.side_effect = set_state
    mock_state.get_state.side_effect = lambda integration_id, action_id, source_id="no-source": store.get(source_id)
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    config = _config(event_prefix="iNat: ")
    first = Observation.from_json(_inat_json(datetime.now(tz=timezone.utc) - timedelta(hours=2)))
    event = _transform_inat_to_gundi_event(first, config)
    await save_events_state([{"object_id": "g777"}], [event], _lock_test_integration())

    now = datetime.now(tz=timezone.utc)
    touched = Observation.from_json(_inat_json(now, comments=1))
    targets = {}
    outcome = await _sort_observation(_lock_test_integration(), config, touched, set(), targets, set(), now=now)
    assert outcome == "unchanged" and not targets

    reidentified = Observation.from_json(_inat_json(now, taxon_id=42007))
    outcome = await _sort_observation(_lock_test_integration(), config, reidentified, set(), targets, set(), now=now)
    assert outcome == "update" and targets == {777: "g777"}


@pytest.mark.asyncio
async def test_sort_observation_without_created_at_is_not_treated_as_old(mocker):
    _sort_state(mocker)
    ob, payload = _sortable(0, NOW)
    ob.created_at = None

    outcome, new_ids, _, _ = await _sort(mocker, ob, payload, NOW)

    assert outcome == "new" and new_ids == {1}


def test_transform_accepts_an_any_prefix():
    """"any" in the portal arrives as None; the title is then the species alone."""
    config = PullEventsConfig(bounding_box="[1, 1, 0, 0]", event_prefix="any")
    assert config.event_prefix is None
    event = _transform_inat_to_gundi_event(_make_observation(), config)
    assert not event["title"].startswith("None")


@pytest.mark.asyncio
async def test_cursor_lag_uses_the_stored_cursor_when_a_run_saves_none(mocker):
    """iNaturalist down before the first chunk: the lag still says how far behind the connection is."""
    from app.datasource.inaturalist import INatUnavailableError

    stored = datetime.now(tz=timezone.utc) - timedelta(days=10)
    _pull_setup(mocker, 0, saved_state={"no-source": {STATE_LAST_RUN_KEY: stored.strftime(STATE_DATETIME_FMT)}})

    def down(*args, **kwargs):
        raise INatUnavailableError("iNaturalist didn't answer in time")
        yield  # pragma: no cover
    mocker.patch("app.actions.handlers.iter_observation_pages", side_effect=down)

    result = await action_pull_events(_lock_test_integration(), _config())

    assert result["result"]["cursor_lag_days"] == pytest.approx(10.0, abs=0.1)
