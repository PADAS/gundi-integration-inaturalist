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
    _build_pull_events_state,
    _get_load_since,
    _normalize_recorded_at,
    _transform_inat_to_gundi_event,
    action_pull_events,
    chunk_list,
    process_attachments,
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


# --- _get_load_since ---


def test_get_load_since_empty_state_uses_fallback_days():
    state = {}
    result = _get_load_since(state, 5)
    now = datetime.now(tz=timezone.utc)
    expected_floor = now - timedelta(days=6)
    expected_ceil = now - timedelta(days=4)
    assert expected_floor <= result <= expected_ceil
    assert result.tzinfo is not None


def test_get_load_since_with_last_run_parses_and_returns():
    state = {STATE_LAST_RUN_KEY: "2024-01-15 12:00:00+0000"}
    result = _get_load_since(state, 5)
    assert result == datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


def test_get_load_since_prefers_last_run_over_updated_to():
    state = {
        STATE_LAST_RUN_KEY: "2024-02-01 00:00:00+0000",
        "updated_to": "2024-01-01 00:00:00+0000",
    }
    result = _get_load_since(state, 5)
    assert result == datetime(2024, 2, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_get_load_since_uses_updated_to_when_last_run_missing():
    state = {"updated_to": "2024-03-10 08:30:00+0000"}
    result = _get_load_since(state, 3)
    assert result == datetime(2024, 3, 10, 8, 30, 0, tzinfo=timezone.utc)


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
    """When state has inat_updated_at equal to observation.updated_at, we should not patch (avoids updating every run)."""
    from uuid import UUID
    from pyinaturalist import Observation

    fixed_updated = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    ob = Observation.from_json({
        "id": 999,
        "observed_on": "2024-06-15",
        "created_at": "2024-06-15T10:00:00+00:00",
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

    async def get_state_side_effect(integration_id, action_id, source_id="no-source"):
        if source_id == "no-source":
            return {STATE_LAST_RUN_KEY: "2024-06-01 00:00:00+0000"}
        if source_id == "999":
            return {
                "object_id": "gundi-uuid-999",
                STATE_INAT_UPDATED_AT_KEY: fixed_updated.strftime(STATE_DATETIME_FMT),
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
    config = PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]", event_prefix="iNat: ")

    result = await action_pull_events(integration, config)

    assert result["result"]["events_updated"] == 0
    assert result["result"]["events_extracted"] == 0
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

    assert result == {"result": {"events_extracted": 0, "events_updated": 0, "photos_attached": 0}}
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

    assert result == {"result": {"events_extracted": 0, "events_updated": 0, "photos_attached": 0}}
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


# --- process_attachments: error logging ---


@pytest.mark.asyncio
async def test_process_attachments_logs_gundi_error_detail(mocker):
    from gundi_client_v2.errors import GundiAPIError

    image_response = MagicMock()
    image_response.aread = AsyncMock(return_value=b"raw-image-bytes")
    session = AsyncMock()
    session.get.return_value = image_response
    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=session)
    client.__aexit__ = AsyncMock(return_value=False)
    mocker.patch("app.actions.handlers.httpx.AsyncClient", return_value=client)
    mocker.patch(
        "app.actions.handlers.send_event_attachments_to_gundi",
        AsyncMock(side_effect=GundiAPIError(413, "Request too large")),
    )
    mock_log_activity = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    integration = _lock_test_integration()

    processed = await process_attachments(
        events=[{"event_details": {"inat_id": "999"}}],
        response=[{"object_id": "gundi-uuid-999"}],
        all_event_photos={"999": [(42, "https://example.com/photo.jpg")]},
        integration=integration,
    )

    assert processed == 0
    log_data = mock_log_activity.call_args.kwargs["data"]
    assert log_data["server_response_body"] == "Request too large"


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

    assert result == {"result": {"events_extracted": 0, "events_updated": 0, "photos_attached": 0}}
    get_observations.assert_not_called()
    warning = activity.call_args.kwargs
    assert warning["level"] == LogLevel.WARNING
    assert "pull was skipped" in warning["title"]


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

    assert result == {"result": {"events_extracted": 0, "events_updated": 0, "photos_attached": 0}}
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

    assert mock_log.call_args.kwargs["title"] == "Pull skipped: the previous pull is still running."
    assert mock_log.call_args.kwargs["level"] == LogLevel.INFO


@pytest.mark.asyncio
async def test_action_pull_events_no_new_observations_is_info_with_the_window(mocker):
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    mock_state.get_state.return_value = {STATE_LAST_RUN_KEY: "2026-10-29 08:00:00+0000"}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    _mock_read(mocker, {})
    mock_log = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    await action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1", bounding_box="[1, 1, 0, 0]"))

    assert mock_log.call_args.kwargs["title"] == "No new or changed matching iNaturalist observations since 2026-10-29 08:00 UTC."
    assert mock_log.call_args.kwargs["level"] == LogLevel.INFO


@pytest.mark.asyncio
async def test_a_stopped_pull_says_where_it_was(mocker):
    """The runner's time limit cancels the handler; before the cancellation
    goes on, the pull logs its step and how long iNaturalist took."""
    import asyncio

    observation = MagicMock()
    observation.id = 1
    observation.photos = []
    observation.created_at = datetime(2026, 10, 29, tzinfo=timezone.utc)
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
    assert stopped["title"].startswith("The pull was stopped after 0 s while sending new observations to Gundi. ")
    assert "Reading from iNaturalist took 0 s, with 1 matching observation; at least 0 new observations sent to Gundi" in stopped["title"]
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
    observation.created_at = datetime(2026, 10, 29, tzinfo=timezone.utc)
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

    assert not any(t.startswith("The pull was stopped") for t in titles)


@pytest.mark.asyncio
async def test_process_attachments_counts_each_upload_as_it_lands(mocker):
    """A pull stopped mid-chunk should report the photos already uploaded."""
    import asyncio

    from app.actions.handlers import _PullProgress

    events = [{"event_details": {"inat_id": "1"}}, {"event_details": {"inat_id": "2"}}]
    response = [{"object_id": "g1"}, {"object_id": "g2"}]
    photos = {"1": [(10, "https://x/10.jpg"), (11, "https://x/11.jpg")], "2": [(20, "https://x/20.jpg")]}
    image = MagicMock()
    image.raise_for_status = MagicMock()
    image.aread = AsyncMock(return_value=b"img")
    client = AsyncMock()
    client.get.return_value = image
    mocker.patch("app.actions.handlers.httpx.AsyncClient", return_value=MagicMock(
        __aenter__=AsyncMock(return_value=client), __aexit__=AsyncMock(return_value=False)
    ))
    progress = _PullProgress()
    first_uploaded = asyncio.Event()

    async def send(event_id, attachments, integration_id):
        if event_id == "g2":
            first_uploaded.set()
            await asyncio.Event().wait()
        return [{}]

    mocker.patch("app.actions.handlers.send_event_attachments_to_gundi", side_effect=send)
    integration = MagicMock()

    task = asyncio.ensure_future(process_attachments(events, response, photos, integration, progress))
    await first_uploaded.wait()
    assert progress.photos_added == 2
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.parametrize("seconds, text", [
    (45, "45 s"),
    (119, "119 s"),
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
    progress.photos_added = 1
    assert progress.counts() == "1 new observation sent to Gundi, 0 updated, 1 photo added"
    assert progress.inat_summary() == "Reading from iNaturalist took 1 s, with 1 matching observation"
    progress.observations_read = 2
    progress.events_sent = 0
    progress.photos_added = 3
    assert progress.counts() == "0 new observations sent to Gundi, 0 updated, 3 photos added"
    assert progress.inat_summary() == "Reading from iNaturalist took 1 s, with 2 matching observations"
    assert progress.counts(at_least=True) == "at least 0 new observations sent to Gundi, 0 updated, 3 photos added"


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

    def spy(self):
        real_init(self)
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
    ob.created_at = datetime(2026, 10, 29, tzinfo=timezone.utc)
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

    async def save_sent(response, events, integration, inat_updated_at_map=None):
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


def _synced(*ids, at="2026-10-29 07:00:00+0000"):
    """State for observations already in Gundi, last synced at `at`."""
    return {str(i): {"object_id": f"g{i}", STATE_INAT_UPDATED_AT_KEY: at} for i in ids}


def test_the_limit_of_new_events_defaults_to_150():
    from app import settings

    assert settings.INAT_MAX_NEW_EVENTS_PER_PULL == 150


@pytest.mark.asyncio
async def test_a_pull_stops_reading_once_it_has_the_limit_of_new_events(mocker):
    mocker.patch("app.actions.handlers.settings.INAT_MAX_NEW_EVENTS_PER_PULL", 3)
    pages = [[_observation(1), _observation(2)], [_observation(3), _observation(4)], [_observation(5)]]
    read, send, store, mock_log = _pull_setup(mocker, 0, pages=pages)

    result = await action_pull_events(_lock_test_integration(), _config())

    assert len(read.pages_read) == 2  # the third page is never asked for
    assert result["result"]["events_extracted"] == 3
    assert store["no-source"][STATE_LAST_RUN_KEY] == "2026-10-29 08:03:00+0000"
    finished = [t for t in _titles(mock_log) if t.startswith("Pull finished")][-1]
    assert finished.endswith("It stopped at the limit of 3 new observations per pull, "
                             "with observations updated up to 2026-10-29 08:03 UTC done.")


@pytest.mark.asyncio
async def test_updates_found_on_the_way_are_sent_and_dont_count(mocker):
    mocker.patch("app.actions.handlers.settings.INAT_MAX_NEW_EVENTS_PER_PULL", 2)
    read, send, store, _ = _pull_setup(mocker, 4, saved_state=_synced(1, 2))

    result = await action_pull_events(_lock_test_integration(), _config())

    assert result["result"] == {"events_extracted": 2, "events_updated": 2, "photos_attached": 0}


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

    mocker.patch.object(handlers, "PHOTO_CHUNK_SIZE", 2)
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
    assert paused and "with observations updated up to 2026-10-29 08:02 UTC done: 2 new observations sent to Gundi" in paused[0]
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
        "The pull didn't send anything to Gundi because the time limit came before it finished its first observations.")


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
        return INatUnavailableError("no answer in time when asked for observations")
    response = requests.Response()
    response.status_code = 503 if kind == "503" else 422
    response.reason = "Backend fetch failed" if kind == "503" else "Unprocessable"
    return INatRequestError(f"iNaturalist answered with an error when asked for observations: {response.status_code}",
                            response=response)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["unavailable", "503"])
async def test_inaturalist_down_is_a_warning_until_it_repeats(mocker, kind):
    _, _, store, mock_log = _pull_setup(mocker, 0)
    _mock_read(mocker, side_effect=_inat_down_error(kind))

    for _ in range(2):
        result = await action_pull_events(_lock_test_integration(), _config())
        assert result == {"result": {"events_extracted": 0, "events_updated": 0, "photos_attached": 0}}
    warnings = [t for t in _titles(mock_log, LogLevel.WARNING) if "iNaturalist had a problem" in t]
    assert len(warnings) == 2
    assert warnings[0].startswith("The pull didn't get observations from iNaturalist because iNaturalist had a problem: ")
    assert store["inat_down"] == {"runs": 2}

    # The third pull in a row is an error, logged by the pull itself: a raised
    # 5xx would be redelivered and run again at once.
    result = await action_pull_events(_lock_test_integration(), _config())
    assert result == {"result": {"events_extracted": 0, "events_updated": 0, "photos_attached": 0}}
    errors = _titles(mock_log, LogLevel.ERROR)
    assert len(errors) == 1
    assert errors[0].startswith("The pull didn't get observations from iNaturalist because it had a problem 3 pulls in a row: ")
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
async def test_a_read_that_outlasts_the_time_budget_counts_as_inaturalist_not_answering(mocker):
    """The read runs in a thread that can't be stopped, but the run stops
    waiting for it at the budget, so it ends inside the time limit."""
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

    assert result == {"result": {"events_extracted": 0, "events_updated": 0, "photos_attached": 0}}
    warnings = _titles(mock_log, LogLevel.WARNING)
    assert warnings[-1] == ("The pull didn't get observations from iNaturalist because iNaturalist had a problem: "
                            "no answer in time when asked for observations.")
    assert store["inat_down"] == {"runs": 1}
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
        raise INatUnavailableError("the connection dropped when asking for observations")
    _, send, store, mock_log = _pull_setup(mocker, 0, pages=pages())

    result = await action_pull_events(_lock_test_integration(), _config())

    assert result["result"]["events_extracted"] == 2
    assert store["no-source"][STATE_LAST_RUN_KEY] == "2026-10-29 08:02:00+0000"
    assert _titles(mock_log, LogLevel.WARNING)[-1].startswith(
        "The pull didn't get observations from iNaturalist because iNaturalist had a problem: ")


@pytest.mark.asyncio
async def test_an_outage_notice_that_cant_be_published_doesnt_fail_the_pull(mocker):
    _pull_setup(mocker, 0)
    _mock_read(mocker, side_effect=_inat_down_error("unavailable"))
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock(side_effect=RuntimeError("publisher down")))

    result = await action_pull_events(_lock_test_integration(), _config())

    assert result == {"result": {"events_extracted": 0, "events_updated": 0, "photos_attached": 0}}
