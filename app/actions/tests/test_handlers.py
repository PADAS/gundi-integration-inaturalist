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
        taxa="1",
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
    config = PullEventsConfig(days_to_load=3, taxa="1", event_prefix="iNat: ")
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
    config = PullEventsConfig(days_to_load=3, taxa="1", event_prefix="")
    event = _transform_inat_to_gundi_event(ob, config)
    assert event["title"] == "Unknown Bird"


# --- action_pull_events: no observations path (state still updated) ---


@pytest.mark.asyncio
async def test_action_pull_events_no_observations_updates_state(mocker):
    from uuid import UUID

    mock_state = AsyncMock()
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    mocker.patch("app.actions.handlers.get_observations", return_value={})
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    integration = MagicMock()
    integration.id = UUID("f03ec73e-f3fe-41b6-8597-3eb89dde5ae1")
    config = PullEventsConfig(days_to_load=3, taxa="1", event_prefix="iNat: ")

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
    mocker.patch("app.actions.handlers.get_observations", return_value=observations_map)
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    mock_patch_events = mocker.patch("app.actions.handlers.patch_events", new_callable=AsyncMock)

    integration = MagicMock()
    integration.id = UUID("f03ec73e-f3fe-41b6-8597-3eb89dde5ae1")
    config = PullEventsConfig(days_to_load=3, taxa="1", event_prefix="iNat: ")

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
    mock_get_observations = mocker.patch("app.actions.handlers.get_observations")
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    result = await action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1"))

    assert result == {"result": {"events_extracted": 0, "events_updated": 0, "photos_attached": 0}}
    mock_get_observations.assert_not_called()
    mock_state.delete_state_if_value.assert_not_called()


@pytest.mark.asyncio
async def test_action_pull_events_skip_survives_an_activity_log_failure(mocker):
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = False
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    mock_get_observations = mocker.patch("app.actions.handlers.get_observations")
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock(side_effect=RuntimeError("publisher down")))
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    result = await action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1"))

    assert result == {"result": {"events_extracted": 0, "events_updated": 0, "photos_attached": 0}}
    mock_get_observations.assert_not_called()


@pytest.mark.asyncio
async def test_action_pull_events_releases_the_lock_after_a_run(mocker):
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    mocker.patch("app.actions.handlers.get_observations", return_value={})
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    integration = _lock_test_integration()

    await action_pull_events(integration, PullEventsConfig(days_to_load=3, taxa="1"))

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
    mocker.patch("app.actions.handlers.get_observations", side_effect=RuntimeError("iNat down"))
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    integration = _lock_test_integration()

    with pytest.raises(RuntimeError, match="iNat down"):
        await action_pull_events(integration, PullEventsConfig(days_to_load=3, taxa="1"))

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
    mocker.patch("app.actions.handlers.get_observations", return_value={})
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    result = await action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1"))

    assert result["result"]["events_extracted"] == 0


@pytest.mark.asyncio
async def test_action_pull_events_ephemeral_run_skips_the_lock(mocker):
    from app.services.activity_logger import ephemeral_run

    mock_state = AsyncMock()
    mock_state.get_state.return_value = {}
    mocker.patch("app.actions.handlers.state_manager", mock_state)
    mocker.patch("app.actions.handlers.get_observations", return_value={})
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    token = ephemeral_run.set(True)
    try:
        await action_pull_events(_lock_test_integration(), PullEventsConfig(days_to_load=3, taxa="1"))
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

    mocker.patch("app.actions.handlers.get_observations", side_effect=run_a_fetch)

    with caplog.at_level(logging.INFO, logger="app.actions.handlers"):
        await action_pull_events(integration, PullEventsConfig(days_to_load=3, taxa="1"))

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
    get_observations = mocker.patch("app.actions.handlers.get_observations", return_value={})
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

    await action_pull_events(integration, PullEventsConfig(days_to_load=3, taxa="1"))

    find.assert_not_called()
    get_observations.assert_called_once()
