"""Manual backfill: creations only, by upload window, up to max_events (spec §5)."""
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from gundi_core.schemas.v2 import LogLevel

from app.actions import handlers
from app.actions.configurations import BackfillContinueConfig, BackfillEventsConfig
from app.actions.handlers import STATE_SENT_HASH_KEY, _payload_hash
from app.datasource.inaturalist import INatUnavailableError

START = datetime(2024, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 11, 5, tzinfo=timezone.utc)
PULL_SETTINGS = {"bounding_box": "[1, 1, 0, 0]", "event_prefix": "iNat: ", "quality_grade": ["research"],
                 "taxa": ["41944"], "days_to_load": 3}


def _integration(pull_settings=PULL_SETTINGS):
    integration = MagicMock()
    integration.id = "f03ec73e-f3fe-41b6-8597-3eb89dde5ae1"
    rows = []
    if pull_settings is not None:
        row = MagicMock()
        row.action.value = "pull_events"
        row.data = dict(pull_settings)
        rows.append(row)
    integration.configurations = rows
    return integration


def _ob(i):
    ob = MagicMock()
    ob.id = i
    ob.annotations = []
    ob.photos = [MagicMock()]
    ob.created_at = START + timedelta(days=i)
    ob.updated_at = END
    return ob


def _job(**kwargs):
    return BackfillContinueConfig(created_from=START, created_to=END, max_events=kwargs.pop("max_events", 1000),
                                  **kwargs)


@pytest.fixture
def backfill(mocker):
    """State in a dict, pages from `pages` [(batch_index, batch_count, [obs])], sends that
    always succeed. Returns a namespace with everything to assert on."""
    ns = MagicMock()
    ns.store = {}
    ns.pages = []
    mock_state = AsyncMock()
    ns.lock_free = [True]

    async def set_if_absent(*args, **kwargs):
        return ns.lock_free.pop(0) if len(ns.lock_free) > 1 else ns.lock_free[0]
    mock_state.set_if_absent.side_effect = set_if_absent
    mock_state.get_state.side_effect = lambda integration_id, action_id, source_id="no-source": ns.store.get(source_id)

    async def set_state(integration_id, action_id, state, source_id="no-source"):
        ns.store[source_id] = state
    mock_state.set_state.side_effect = set_state

    async def delete_state(integration_id, action_id, source_id="no-source"):
        ns.store.pop(source_id, None)
    mock_state.delete_state.side_effect = delete_state
    mocker.patch.object(handlers, "state_manager", mock_state)
    ns.state = mock_state

    def read(*args, **kwargs):
        ns.read_args, ns.read_kwargs = args, kwargs
        yield from ns.pages
    ns.read = mocker.patch.object(handlers, "iter_upload_pages", side_effect=read)
    mocker.patch.object(handlers, "_transform_inat_to_gundi_event",
                        side_effect=lambda ob, config: {"event_details": {"inat_id": str(ob.id)}})
    ns.send = mocker.patch.object(
        handlers, "send_events_to_gundi",
        side_effect=lambda events, integration_id: [{"object_id": f"g{e['event_details']['inat_id']}"} for e in events])
    ns.update = mocker.patch.object(handlers, "update_event_in_gundi", AsyncMock())
    ns.trigger = mocker.patch.object(handlers, "trigger_action", AsyncMock())
    ns.log = mocker.patch.object(handlers, "log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    mocker.patch.object(handlers, "BACKFILL_LOCK_WAIT_SECONDS", 0)
    return ns


def _fake_asyncio(mocker, **overrides):
    """Replace only the handlers module's view of asyncio, never the real module the event loop uses."""
    import asyncio
    import types

    fake = types.SimpleNamespace(**{name: getattr(asyncio, name) for name in dir(asyncio) if not name.startswith("_")})
    for name, value in overrides.items():
        setattr(fake, name, value)
    mocker.patch.object(handlers, "asyncio", fake)


def _titles(ns, level=None):
    return [c.kwargs["title"] for c in ns.log.call_args_list if level is None or c.kwargs["level"] == level]


@pytest.mark.asyncio
async def test_backfill_reads_the_upload_window_with_the_pull_filters(backfill):
    await handlers._backfill(_integration(), _job())

    args, kwargs = backfill.read_args, backfill.read_kwargs
    assert args == (START, END)
    assert kwargs["batch_index"] == 0 and kwargs["after_id"] == 0
    assert kwargs["quality_grade"] == ["research"]
    assert kwargs["taxa"] == "41944"
    assert kwargs["bounding_box"] == [1.0, 1.0, 0.0, 0.0]


@pytest.mark.asyncio
async def test_backfill_window_comes_from_days_back(backfill):
    before = datetime.now(tz=timezone.utc)

    result = await handlers.action_backfill_events(_integration(), BackfillEventsConfig(days_back=1095))

    created_from, created_to = backfill.read_args
    assert before <= created_to <= datetime.now(tz=timezone.utc)
    assert created_to - created_from == timedelta(days=1095)
    assert result["result"]["backfill_finished"] is True


@pytest.mark.asyncio
async def test_backfill_creates_only_new_sightings_without_updates_or_photos(backfill):
    backfill.store["2"] = {"object_id": "g2"}
    backfill.pages = [(0, 1, [_ob(1), _ob(2), _ob(3)])]

    result = await handlers._backfill(_integration(), _job())

    sent = [e["event_details"]["inat_id"] for c in backfill.send.call_args_list for e in c.kwargs["events"]]
    assert sent == ["1", "3"]
    backfill.update.assert_not_called()
    assert not [c for c in backfill.trigger.call_args_list if c.args[1] == "add_photos"]
    assert not [k for k in backfill.store if k.startswith("photos.")]
    assert backfill.store["1"][STATE_SENT_HASH_KEY] == _payload_hash({"event_details": {"inat_id": "1"}})
    r = result["result"]
    assert (r["events_extracted"], r["events_updated"], r["photos_attached"]) == (2, 0, 0)
    assert r["backfill_finished"] is True and r["backfill_events_created"] == 2
    assert _titles(backfill)[-1] == ("Backfill finished: created 2 events for sightings uploaded "
                                     "between 2024-01-01 and 2026-11-05.")


@pytest.mark.asyncio
async def test_backfill_skips_sightings_the_annotation_filter_rejects(backfill, mocker):
    settings = {**PULL_SETTINGS, "annotations": [{"term": "22", "values": ["24"]}]}
    mocker.patch.object(handlers, "_match_annotations_to_config", side_effect=lambda annotations, wanted: False)
    backfill.pages = [(0, 1, [_ob(1)])]

    result = await handlers._backfill(_integration(settings), _job())

    backfill.send.assert_not_called()
    assert result["result"]["events_extracted"] == 0


@pytest.mark.asyncio
async def test_backfill_stops_at_max_events_and_says_how_far_it_got(backfill):
    backfill.pages = [(0, 1, [_ob(i) for i in range(1, 6)])]

    result = await handlers._backfill(_integration(), _job(max_events=3))

    assert backfill.send.call_args.kwargs["events"] == [{"event_details": {"inat_id": str(i)}} for i in (1, 2, 3)]
    r = result["result"]
    assert (r["events_extracted"], r["backfill_finished"]) == (3, False)
    assert r["backfill_uploaded_up_to"] == _ob(3).created_at.isoformat()
    assert _titles(backfill)[-1] == ("Backfill stopped at its limit of 3 events: sightings uploaded up to "
                                     "2024-01-04 00:00 UTC were sent. Run it again to continue; "
                                     "sightings already sent are skipped.")


@pytest.mark.asyncio
async def test_backfill_counts_events_created_by_earlier_runs_toward_the_limit(backfill):
    backfill.pages = [(0, 1, [_ob(i) for i in range(1, 6)])]

    result = await handlers._backfill(_integration(), _job(max_events=3, created_so_far=2))

    assert result["result"]["events_extracted"] == 1
    assert result["result"]["backfill_events_created"] == 3


@pytest.mark.asyncio
async def test_backfill_hands_over_at_the_time_budget_and_the_next_run_resumes(backfill, mocker):
    clock = [1000.0]
    mocker.patch.object(handlers, "time", MagicMock(monotonic=lambda: clock[0]))
    mocker.patch.object(handlers, "_pull_time_budget", return_value=100)
    real_send = backfill.send.side_effect

    def slow_send(events, integration_id):
        clock[0] += 150  # the first chunk uses up the budget
        return real_send(events, integration_id)
    backfill.send.side_effect = slow_send
    backfill.pages = [(0, 2, [_ob(1), _ob(2)]), (1, 2, [_ob(7)])]

    result = await handlers._backfill(_integration(), _job())

    handed = backfill.trigger.call_args
    assert handed.args[1] == "backfill_events_continue"
    job = handed.args[2]
    assert (job.batch_index, job.after_id, job.created_so_far) == (0, 2, 2)
    assert result["result"]["backfill_finished"] is False
    assert _titles(backfill, LogLevel.INFO)[-1].startswith("Backfill continues in a new run: 2 events created so far")

    # The continuation starts where the first run stopped.
    clock[0] = 5000.0
    backfill.send.side_effect = real_send
    backfill.pages = [(1, 2, [_ob(7)])]
    result = await handlers.action_backfill_events_continue(_integration(), job)
    assert (backfill.read_kwargs["batch_index"], backfill.read_kwargs["after_id"]) == (0, 2)
    assert result["result"]["backfill_events_created"] == 3 and result["result"]["backfill_finished"] is True


@pytest.mark.asyncio
async def test_backfill_waits_for_a_running_pull(backfill):
    backfill.lock_free = [False, False, True]
    backfill.pages = [(0, 1, [_ob(1)])]

    result = await handlers._backfill(_integration(), _job())

    assert result["result"]["events_extracted"] == 1
    backfill.state.delete_state_if_value.assert_called_once()


@pytest.mark.asyncio
async def test_backfill_gives_up_when_a_pull_keeps_running(backfill, mocker):
    clock = [1000.0]

    async def no_sleep(seconds):
        clock[0] += 60
    mocker.patch.object(handlers, "time", MagicMock(monotonic=lambda: clock[0]))
    _fake_asyncio(mocker, sleep=no_sleep)
    mocker.patch.object(handlers, "BACKFILL_LOCK_WAIT_SECONDS", 10)
    backfill.lock_free = [False]
    backfill.pages = [(0, 1, [_ob(1)])]

    result = await handlers._backfill(_integration(), _job())

    backfill.send.assert_not_called()
    assert result["result"]["events_extracted"] == 0
    handed = backfill.trigger.call_args
    assert handed.args[1] == "backfill_events_continue" and handed.args[2].lock_waits == 1
    assert "waiting for another run of this connection" in _titles(backfill, LogLevel.INFO)[-1]
    backfill.state.delete_state_if_value.assert_not_called()


@pytest.mark.asyncio
async def test_backfill_stops_with_an_error_when_inaturalist_is_down(backfill):
    def down(*args, **kwargs):
        raise INatUnavailableError("iNaturalist didn't answer in time")
        yield  # pragma: no cover
    backfill.read.side_effect = down

    result = await handlers._backfill(_integration(), _job())

    assert result["result"]["backfill_finished"] is False
    error = _titles(backfill, LogLevel.ERROR)[-1]
    assert error.startswith("The backfill stopped after creating 0 events because iNaturalist didn't answer in time")
    backfill.state.delete_state_if_value.assert_called_once()


@pytest.mark.asyncio
async def test_backfill_needs_the_pull_settings(backfill):
    result = await handlers._backfill(_integration(pull_settings=None), _job())

    backfill.read.assert_not_called()
    assert result["result"]["events_extracted"] == 0
    assert _titles(backfill, LogLevel.ERROR)[-1].startswith("The backfill didn't run because the connection has no "
                                                            "pull settings")


def test_backfill_settings_defaults_and_ranges():
    import pydantic

    config = BackfillEventsConfig()
    assert (config.days_back, config.max_events) == (30, 1000)
    for field, bad in (("days_back", 0), ("days_back", 3651), ("max_events", 0), ("max_events", 100_001)):
        with pytest.raises(pydantic.ValidationError):
            BackfillEventsConfig(**{field: bad})


@pytest.mark.asyncio
async def test_backfill_registers_as_a_manual_action_and_its_continuation_not_at_all(mocker):
    from app.services import self_registration

    gundi = MagicMock()
    gundi.register_integration_type = AsyncMock()
    await self_registration.register_integration_in_gundi(gundi, type_slug="inaturalist", type_name="iNaturalist")

    actions = {a["value"]: a for a in gundi.register_integration_type.call_args.args[0]["actions"]}
    assert actions["backfill_events"]["is_periodic_action"] is False
    assert actions["backfill_events"]["schema"].get("is_executable") is True
    assert "crontab_schedule" not in actions["backfill_events"]
    assert "backfill_events_continue" not in actions


def test_the_integration_type_name_is_iNaturalist_for_this_runner():
    from app.settings.integration import _type_name

    assert _type_name("inaturalist", None) == "iNaturalist"
    assert _type_name(" iNaturalist ", None) == "iNaturalist"
    assert _type_name("inaturalist", "Custom") == "Custom"  # the env var still wins
    assert _type_name("x_tracker", None) is None  # other slugs keep the template's derived name


@pytest.mark.asyncio
async def test_the_integration_type_registers_with_that_name(mocker):
    from app.services import self_registration

    mocker.patch.object(self_registration, "INTEGRATION_TYPE_NAME", "iNaturalist")
    gundi = MagicMock()
    gundi.register_integration_type = AsyncMock()
    await self_registration.register_integration_in_gundi(gundi, type_slug="inaturalist")

    data = gundi.register_integration_type.call_args.args[0]
    assert (data["name"], data["value"]) == ("iNaturalist", "inaturalist")


# --- round-3 cases ---


@pytest.mark.asyncio
async def test_backfill_yields_to_a_waiting_pull_after_reading_a_page(backfill):
    backfill.store["pull_waiting"] = 1
    backfill.pages = [(0, 1, [_ob(1)]), (0, 1, [_ob(2)])]

    result = await handlers._backfill(_integration(), _job())

    # The first page is always read, so yielding never stalls the backfill.
    sent = [e["event_details"]["inat_id"] for c in backfill.send.call_args_list for e in c.kwargs["events"]]
    assert sent == ["1"]
    started = [c.args[1] for c in backfill.trigger.call_args_list]
    # The continuation is started first (it waits before taking the lock), then the pull.
    assert started == ["backfill_events_continue", "pull_events"]
    job = [c.args[2] for c in backfill.trigger.call_args_list if c.args[1] == "backfill_events_continue"][-1]
    assert job.yield_to_pull is True and job.runs_without_progress == 0 and job.after_id == 1
    assert [c for c in backfill.state.delete_state.call_args_list if c.kwargs.get("source_id") == "pull_waiting"]
    assert result["result"]["backfill_finished"] is False
    backfill.state.delete_state_if_value.assert_called_once()  # the lock goes back for the pull


@pytest.mark.asyncio
async def test_a_continuation_that_yielded_waits_before_taking_the_lock(backfill, mocker):
    slept = []

    async def sleep(seconds):
        slept.append(seconds)
    _fake_asyncio(mocker, sleep=sleep)
    mocker.patch.object(handlers, "BACKFILL_LOCK_WAIT_SECONDS", 10)
    backfill.pages = [(0, 1, [_ob(1)])]

    await handlers._backfill(_integration(), _job(yield_to_pull=True))

    assert slept == [10]


@pytest.mark.asyncio
async def test_a_pull_that_finds_the_lock_taken_leaves_a_note_and_clears_it_when_it_runs(mocker):
    from uuid import UUID
    from app.actions.configurations import PullEventsConfig

    mock_state = AsyncMock()
    mock_state.set_if_absent.side_effect = [False, True, True]  # lock taken, note left; then lock free
    mocker.patch.object(handlers, "state_manager", mock_state)
    mocker.patch.object(handlers, "log_action_activity", AsyncMock())
    mocker.patch.object(handlers, "_run_pull", AsyncMock(return_value={"result": {}}))
    integration = MagicMock()
    integration.id = UUID("f03ec73e-f3fe-41b6-8597-3eb89dde5ae1")
    config = PullEventsConfig(bounding_box="[1, 1, 0, 0]")

    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())

    await handlers.action_pull_events(integration, config)
    noted = [c for c in mock_state.set_if_absent.call_args_list if c.kwargs.get("source_id") == "pull_waiting"]
    assert noted, "the skipped pull leaves the pull-waiting note"

    await handlers.action_pull_events(integration, config)
    cleared = [c for c in mock_state.delete_state.call_args_list if c.kwargs.get("source_id") == "pull_waiting"]
    assert cleared, "a pull that runs clears the note"


@pytest.mark.asyncio
async def test_backfill_resumes_from_saved_progress_when_a_message_is_redelivered(backfill):
    job = _job()
    backfill.store[handlers._backfill_key(job)] = {"batch_index": 0, "after_id": 5, "created_so_far": 300}

    result = await handlers._backfill(_integration(), job)

    assert backfill.read_kwargs["after_id"] == 5
    assert result["result"]["backfill_events_created"] == 300


@pytest.mark.asyncio
async def test_a_finished_backfill_leaves_a_done_marker_and_ignores_later_messages(backfill):
    job = _job()
    backfill.pages = [(0, 1, [_ob(1)])]

    await handlers._backfill(_integration(), job)

    marker = backfill.store[handlers._backfill_key(job)]
    assert marker["done"] is True and marker["finished"] is True and marker["created_so_far"] == 1

    # A late copy of an earlier continuation, with an older count, does nothing.
    backfill.send.reset_mock()
    backfill.pages = [(0, 1, [_ob(5)])]
    result = await handlers._backfill(_integration(), _job(created_so_far=0))
    backfill.send.assert_not_called()
    assert result["result"]["backfill_events_created"] == 1 and result["result"]["events_extracted"] == 0


@pytest.mark.asyncio
async def test_a_backfill_at_its_limit_also_ignores_later_messages(backfill):
    backfill.pages = [(0, 1, [_ob(1), _ob(2), _ob(3)])]
    await handlers._backfill(_integration(), _job(max_events=2))

    backfill.send.reset_mock()
    backfill.pages = [(0, 1, [_ob(3), _ob(4)])]
    await handlers._backfill(_integration(), _job(max_events=2, created_so_far=0))

    backfill.send.assert_not_called()


@pytest.mark.asyncio
async def test_a_retry_counts_sightings_saved_after_the_last_saved_position(backfill):
    """A run stopped between saving events and saving its count: the retry counts them, so the
    chain never goes past max_events."""
    job = _job(max_events=3)
    key = handlers._backfill_key(job)
    backfill.store[key] = {"batch_index": 0, "after_id": 0, "created_so_far": 0}
    backfill.store["1"] = {"object_id": "g1", "backfill_job": key, "backfill_batch": 0}
    backfill.store["2"] = {"object_id": "g2", "backfill_job": key, "backfill_batch": 0}
    backfill.store["3"] = {"object_id": "g3"}  # sent by the pull: not this backfill's
    backfill.pages = [(0, 1, [_ob(1), _ob(2), _ob(3), _ob(4), _ob(5)])]

    result = await handlers._backfill(_integration(), job)

    sent = [e["event_details"]["inat_id"] for c in backfill.send.call_args_list for e in c.kwargs["events"]]
    assert sent == ["4"]
    assert result["result"]["backfill_events_created"] == 3


@pytest.mark.asyncio
async def test_backfilled_rows_are_tagged_with_their_job(backfill):
    job = _job()
    backfill.pages = [(0, 1, [_ob(1)])]

    await handlers._backfill(_integration(), job)

    assert backfill.store["1"]["backfill_job"] == handlers._backfill_key(job)
    assert backfill.store["1"]["backfill_batch"] == 0


@pytest.mark.asyncio
async def test_backfill_stops_after_runs_without_progress(backfill, mocker):
    import asyncio

    async def never(awaitable, timeout):
        awaitable.close()
        raise asyncio.TimeoutError
    _fake_asyncio(mocker, wait_for=never)
    backfill.pages = [(0, 1, [_ob(1)])]

    result = await handlers._backfill(_integration(), _job(runs_without_progress=2))

    backfill.trigger.assert_not_called()
    assert result["result"]["backfill_finished"] is False
    assert "3 runs in a row couldn't read anything new" in _titles(backfill, LogLevel.ERROR)[-1]


@pytest.mark.asyncio
async def test_backfill_hands_over_when_a_read_outlasts_the_budget(backfill, mocker):
    import asyncio

    async def slow(awaitable, timeout):
        awaitable.close()
        raise asyncio.TimeoutError
    _fake_asyncio(mocker, wait_for=slow)
    backfill.pages = [(0, 1, [_ob(1)])]

    await handlers._backfill(_integration(), _job())

    job = backfill.trigger.call_args.args[2]
    assert job.runs_without_progress == 1


@pytest.mark.asyncio
async def test_backfill_checks_the_time_before_each_chunk(backfill, mocker):
    clock = [1000.0]
    mocker.patch.object(handlers, "time", MagicMock(monotonic=lambda: clock[0]))
    mocker.patch.object(handlers, "_pull_time_budget", return_value=100)
    mocker.patch.object(handlers, "GUNDI_SUBMISSION_CHUNK_SIZE", 2)
    real_send = backfill.send.side_effect

    def slow_send(events, integration_id):
        clock[0] += 150
        return real_send(events, integration_id)
    backfill.send.side_effect = slow_send
    backfill.pages = [(0, 1, [_ob(1), _ob(2), _ob(3), _ob(4)])]

    await handlers._backfill(_integration(), _job())

    assert backfill.send.call_count == 1  # the second chunk waits for the next run
    job = backfill.trigger.call_args.args[2]
    assert (job.after_id, job.created_so_far) == (2, 2)


@pytest.mark.asyncio
async def test_backfill_limit_reached_in_the_second_chunk_of_a_page(backfill, mocker):
    mocker.patch.object(handlers, "GUNDI_SUBMISSION_CHUNK_SIZE", 2)
    backfill.pages = [(0, 1, [_ob(i) for i in range(1, 6)])]

    result = await handlers._backfill(_integration(), _job(max_events=3))

    sent = [e["event_details"]["inat_id"] for c in backfill.send.call_args_list for e in c.kwargs["events"]]
    assert sent == ["1", "2", "3"]
    assert result["result"]["backfill_uploaded_up_to"] == _ob(3).created_at.isoformat()


@pytest.mark.asyncio
async def test_backfill_resumes_after_the_last_sighting_gundi_took(backfill):
    backfill.send.side_effect = lambda events, integration_id: [{"object_id": "g1"}]
    backfill.pages = [(0, 1, [_ob(1), _ob(2), _ob(3)])]

    result = await handlers._backfill(_integration(), _job())

    job = backfill.trigger.call_args.args[2]
    assert (job.after_id, job.created_so_far) == (1, 1)
    assert "2" not in backfill.store and result["result"]["events_extracted"] == 1


@pytest.mark.asyncio
async def test_backfill_with_only_unknown_projects_says_so(backfill, mocker):
    mocker.patch.object(handlers, "find_existing_projects", return_value=[])
    settings = {**PULL_SETTINGS, "projects": ["gone-project"]}

    result = await handlers._backfill(_integration(settings), _job())

    backfill.read.assert_not_called()
    warning = [c.kwargs for c in backfill.log.call_args_list if c.kwargs["level"] == LogLevel.WARNING][-1]
    assert warning["action_id"] == "backfill_events"
    assert warning["title"].startswith("The backfill didn't run because none of the saved projects exist")
    assert result["result"]["events_extracted"] == 0


@pytest.mark.asyncio
async def test_backfill_with_invalid_pull_settings_explains(backfill):
    result = await handlers._backfill(_integration({"bounding_box": "", "projects": []}), _job())

    backfill.read.assert_not_called()
    assert _titles(backfill, LogLevel.ERROR)[-1].startswith("The backfill didn't run because the pull settings "
                                                            "aren't valid")
    assert result["result"]["events_extracted"] == 0


@pytest.mark.asyncio
async def test_a_backfilled_sighting_is_unchanged_for_the_next_pull(backfill, mocker):
    """B9 with the real transform: the backfill's stored hash is the one the pull computes."""
    from pyinaturalist import Observation
    from app.actions.configurations import PullEventsConfig

    mocker.stopall()
    store = {}
    mock_state = AsyncMock()
    mock_state.set_if_absent.return_value = True
    mock_state.get_state.side_effect = lambda integration_id, action_id, source_id="no-source": store.get(source_id)

    async def set_state(integration_id, action_id, state, source_id="no-source"):
        store[source_id] = state
    mock_state.set_state.side_effect = set_state
    mocker.patch.object(handlers, "state_manager", mock_state)
    mocker.patch.object(handlers, "log_action_activity", AsyncMock())
    mocker.patch.object(handlers, "trigger_action", AsyncMock())
    mocker.patch.object(handlers, "send_events_to_gundi",
                        side_effect=lambda events, integration_id: [{"object_id": "g777"} for _ in events])
    created = datetime.now(tz=timezone.utc) - timedelta(days=2)
    data = {"id": 777, "observed_on": created.date().isoformat(), "created_at": created.isoformat(),
            "updated_at": created.isoformat(), "quality_grade": "needs_id", "species_guess": "Puma",
            "uri": "https://www.inaturalist.org/observations/777", "photos": [], "annotations": [],
            "taxon": {"id": 41944, "name": "Puma concolor", "rank": "species", "preferred_common_name": "Puma"}}
    mocker.patch.object(handlers, "iter_upload_pages",
                        side_effect=lambda *a, **k: iter([(0, 1, [Observation.from_json(data)])]))

    await handlers._backfill(_integration(), _job())

    touched = Observation.from_json({**data, "updated_at": datetime.now(tz=timezone.utc).isoformat()})
    outcome = await handlers._sort_observation(_integration(), PullEventsConfig.parse_obj(PULL_SETTINGS), touched,
                                               set(), {}, set(), now=datetime.now(tz=timezone.utc))
    assert outcome == "unchanged"


# --- second Copilot round ---


@pytest.mark.asyncio
async def test_a_pull_update_keeps_the_backfill_tags(backfill):
    from app.actions.configurations import PullEventsConfig

    backfill.store["7"] = {"object_id": "g7", "backfill_job": "backfill.x", "backfill_batch": 0, "sent_hash": "old"}

    await handlers.save_patched_events_state([("g7", _ob(7))], _integration(), PullEventsConfig.parse_obj(PULL_SETTINGS))

    row = backfill.store["7"]
    assert (row["backfill_job"], row["backfill_batch"]) == ("backfill.x", 0)
    assert row["sent_hash"] != "old" and row["object_id"] == "g7"


@pytest.mark.asyncio
async def test_a_redelivered_start_continues_the_same_backfill(backfill):
    config = BackfillEventsConfig(days_back=365, max_events=2)
    first = await handlers._backfill_job_for("i", config, "f", now=END)
    key = handlers._backfill_key(first)
    backfill.store[key] = {"batch_index": 0, "after_id": 1, "created_so_far": 1}

    again = await handlers._backfill_job_for("i", config, "f", now=END + timedelta(minutes=5))

    assert (again.created_from, again.created_to) == (first.created_from, first.created_to)


@pytest.mark.asyncio
async def test_a_start_after_the_backfill_ended_opens_a_new_window(backfill):
    config = BackfillEventsConfig(days_back=365, max_events=2)
    first = await handlers._backfill_job_for("i", config, "f", now=END)
    backfill.store[handlers._backfill_key(first)] = {"done": True, "created_so_far": 2,
                                                     "ended_at": END.isoformat()}

    again = await handlers._backfill_job_for("i", config, "f", now=END + timedelta(hours=1))

    assert again.created_to == END + timedelta(hours=1)


@pytest.mark.asyncio
async def test_a_start_with_other_settings_opens_a_new_window(backfill):
    first = await handlers._backfill_job_for("i", BackfillEventsConfig(days_back=365, max_events=2), "f", now=END)

    later = END + timedelta(minutes=1)
    again = await handlers._backfill_job_for("i", BackfillEventsConfig(days_back=30, max_events=2), "f", now=later)

    assert again.created_to == later and again.created_to - again.created_from == timedelta(days=30)
    # The first backfill is closed, so its runs stop.
    assert backfill.store[handlers._backfill_key(first)]["superseded"] is True


@pytest.mark.asyncio
async def test_a_sighting_in_two_taxa_groups_is_counted_once(backfill):
    """Taxa groups can overlap (an ancestor in one, a descendant in another)."""
    backfill.pages = [(0, 2, [_ob(1)]), (1, 2, [_ob(1), _ob(2), _ob(3)])]

    result = await handlers._backfill(_integration(), _job(max_events=3))

    sent = [e["event_details"]["inat_id"] for c in backfill.send.call_args_list for e in c.kwargs["events"]]
    assert sent == ["1", "2", "3"]
    assert result["result"]["backfill_events_created"] == 3


@pytest.mark.asyncio
async def test_a_partial_answer_counts_recovered_sightings_only_up_to_the_resume_point(backfill):
    job = _job()
    key = handlers._backfill_key(job)
    backfill.store["2"] = {"object_id": "g2", "backfill_job": key, "backfill_batch": 0}
    backfill.send.side_effect = lambda events, integration_id: [{"object_id": "g1"}]  # takes only the first
    backfill.pages = [(0, 1, [_ob(1), _ob(2), _ob(3)])]

    await handlers._backfill(_integration(), job)

    handed = [c.args[2] for c in backfill.trigger.call_args_list if c.args[1] == "backfill_events_continue"][-1]
    # Resumes after 1, so 2 is met (and counted) again by the next run, not now.
    assert (handed.after_id, handed.created_so_far) == (1, 1)


@pytest.mark.asyncio
async def test_follow_up_runs_start_after_the_lock_is_released(backfill):
    order = []
    backfill.state.delete_state_if_value.side_effect = lambda *a, **k: order.append("release")
    backfill.trigger.side_effect = lambda integration_id, action_id, config=None: order.append(action_id)
    backfill.store["pull_waiting"] = 1
    backfill.pages = [(0, 1, [_ob(1)]), (0, 1, [_ob(2)])]

    await handlers._backfill(_integration(), _job())

    assert order == ["release", "backfill_events_continue", "pull_events"]


# --- round 4: duplicates, settings changes, repeats, interrupted sends ---


@pytest.mark.asyncio
async def test_a_duplicate_continuation_does_nothing_once_the_chain_moved_on(backfill):
    job = _job(seq=3)
    backfill.store[handlers._backfill_key(job)] = {"seq": 4, "batch_index": 0, "after_id": 9, "created_so_far": 5}
    backfill.pages = [(0, 1, [_ob(10)])]

    result = await handlers._backfill(_integration(), job)

    backfill.send.assert_not_called()
    backfill.trigger.assert_not_called()
    assert not _titles(backfill, LogLevel.ERROR)
    assert result["result"]["backfill_events_created"] == 5


@pytest.mark.asyncio
async def test_the_seq_moves_on_only_when_a_run_hands_over(backfill, mocker):
    clock = [1000.0]
    mocker.patch.object(handlers, "time", MagicMock(monotonic=lambda: clock[0]))
    mocker.patch.object(handlers, "_pull_time_budget", return_value=100)
    real_send = backfill.send.side_effect

    def slow_send(events, integration_id):
        clock[0] += 150
        return real_send(events, integration_id)
    backfill.send.side_effect = slow_send
    job = _job(seq=2)
    backfill.pages = [(0, 1, [_ob(1)]), (0, 1, [_ob(2)])]

    await handlers._backfill(_integration(), job)

    handed = [c.args[2] for c in backfill.trigger.call_args_list if c.args[1] == "backfill_events_continue"][-1]
    assert handed.seq == 3 and backfill.store[handlers._backfill_key(job)]["seq"] == 3


@pytest.mark.asyncio
async def test_a_redelivered_message_after_a_cut_off_run_picks_the_chain_up(backfill):
    """The run with seq 2 was cut off after saving its position but before handing over:
    the redelivered copy (seq 2) continues from the saved position."""
    job = _job(seq=2)
    backfill.store[handlers._backfill_key(job)] = {"seq": 2, "batch_index": 0, "after_id": 1, "created_so_far": 1}
    backfill.pages = [(0, 1, [_ob(2)])]

    result = await handlers._backfill(_integration(), job)

    assert backfill.read_kwargs["after_id"] == 1
    assert result["result"]["backfill_events_created"] == 2


@pytest.mark.asyncio
async def test_a_manual_restart_revives_the_chain_from_its_saved_position(backfill):
    config = BackfillEventsConfig(days_back=365, max_events=50)
    first = await handlers._backfill_job_for("i", config, "f", now=END)
    backfill.store[handlers._backfill_key(first)] = {"seq": 4, "batch_index": 0, "after_id": 9, "created_so_far": 12}

    again = await handlers._backfill_job_for("i", config, "f", now=END + timedelta(minutes=30))

    assert (again.seq, again.after_id, again.created_so_far) == (4, 9, 12)


@pytest.mark.asyncio
async def test_a_superseded_backfill_stops_quietly(backfill):
    job = _job()
    backfill.store[handlers._backfill_key(job)] = {"done": True, "superseded": True, "created_so_far": 7}
    backfill.pages = [(0, 1, [_ob(1)])]

    await handlers._backfill(_integration(), job)

    backfill.send.assert_not_called()
    assert not _titles(backfill)


@pytest.mark.asyncio
async def test_changing_the_pull_filters_stops_the_backfill_and_says_so(backfill):
    from app.actions.configurations import PullEventsConfig

    started_with = handlers._filters_fingerprint(PullEventsConfig.parse_obj(PULL_SETTINGS))
    changed = {**PULL_SETTINGS, "taxa": ["41944", "42007"]}
    job = _job(filters=started_with)
    backfill.pages = [(0, 1, [_ob(1)])]

    await handlers._backfill(_integration(changed), job)

    backfill.send.assert_not_called()
    assert backfill.store[handlers._backfill_key(job)]["done"] is True
    assert "pull settings (projects, area, taxa, grades or annotations) changed" in _titles(backfill, LogLevel.ERROR)[-1]


@pytest.mark.asyncio
async def test_a_start_with_other_filters_opens_a_new_window(backfill):
    config = BackfillEventsConfig(days_back=365, max_events=2)
    first = await handlers._backfill_job_for("i", config, "filters-a", now=END)

    again = await handlers._backfill_job_for("i", config, "filters-b", now=END + timedelta(minutes=1))

    assert again.created_to != first.created_to and again.filters == "filters-b"


@pytest.mark.asyncio
async def test_a_repeated_start_right_after_the_backfill_ended_does_nothing(backfill):
    config = BackfillEventsConfig(days_back=365, max_events=2)
    first = await handlers._backfill_job_for("i", config, "f", now=END)
    backfill.store[handlers._backfill_key(first)] = {"done": True, "ended_at": END.isoformat(), "created_so_far": 2}

    again = await handlers._backfill_job_for("i", config, "f", now=END + timedelta(minutes=5))

    assert again is None


@pytest.mark.asyncio
async def test_starts_choose_their_job_one_at_a_time(backfill, mocker):
    slept = []

    async def sleep(seconds):
        slept.append(seconds)
    _fake_asyncio(mocker, sleep=sleep)
    backfill.lock_free = [False, False, True]  # another start holds the start lock twice

    await handlers._backfill_job_for("i", BackfillEventsConfig(), "f", now=END)

    assert slept == [1, 1]
    starting = [c for c in backfill.state.delete_state_if_value.call_args_list
                if c.kwargs.get("source_id") == "backfill_starting"]
    assert starting, "the start lock is released"


@pytest.mark.asyncio
async def test_a_backfill_start_with_bad_pull_settings_explains(backfill):
    result = await handlers.action_backfill_events(_integration({"bounding_box": "", "projects": []}),
                                                   BackfillEventsConfig())

    backfill.read.assert_not_called()
    assert result["result"]["events_extracted"] == 0
    assert "pull settings aren't valid" in _titles(backfill, LogLevel.ERROR)[-1]


@pytest.mark.asyncio
async def test_a_sighting_left_being_sent_by_a_cut_off_run_is_not_sent_again(backfill):
    backfill.store["1"] = {"sending_since": "2026-11-05 10:00:00+0000"}
    backfill.pages = [(0, 1, [_ob(1), _ob(2)])]

    await handlers._backfill(_integration(), _job())

    sent = [e["event_details"]["inat_id"] for c in backfill.send.call_args_list for e in c.kwargs["events"]]
    assert sent == ["2"]


@pytest.mark.asyncio
async def test_a_failed_send_clears_its_being_sent_notes(backfill):
    backfill.send.side_effect = RuntimeError("Gundi is down")
    backfill.pages = [(0, 1, [_ob(1), _ob(2)])]

    with pytest.raises(RuntimeError):
        await handlers._backfill(_integration(), _job())

    assert "1" not in backfill.store and "2" not in backfill.store


@pytest.mark.asyncio
async def test_if_the_next_run_cant_be_started_the_log_says_the_backfill_stopped(backfill, mocker):
    clock = [1000.0]
    mocker.patch.object(handlers, "time", MagicMock(monotonic=lambda: clock[0]))
    mocker.patch.object(handlers, "_pull_time_budget", return_value=100)
    real_send = backfill.send.side_effect

    def slow_send(events, integration_id):
        clock[0] += 150
        return real_send(events, integration_id)
    backfill.send.side_effect = slow_send
    backfill.trigger.side_effect = RuntimeError("topic unavailable")
    backfill.pages = [(0, 1, [_ob(1), _ob(2)]), (0, 1, [_ob(3)])]

    await handlers._backfill(_integration(), _job())

    assert not [t for t in _titles(backfill, LogLevel.INFO) if t.startswith("Backfill continues")]
    assert "its next run couldn't be started" in _titles(backfill, LogLevel.ERROR)[-1]


def test_empty_limits_in_saved_pull_settings_fall_back_to_defaults():
    from app import settings
    from app.actions.configurations import PullEventsConfig

    config = PullEventsConfig.parse_obj({**PULL_SETTINGS, "max_photos": None, "max_age_days": "",
                                         "max_new_events_per_run": None})
    assert (config.max_photos, config.max_age_days) == (1, 90)
    assert config.max_new_events_per_run == min(settings.INAT_MAX_NEW_EVENTS_PER_PULL, 1000)


# --- third Copilot round ---


@pytest.mark.asyncio
async def test_a_failed_note_write_clears_the_notes_already_written(backfill):
    writes = []
    real_set = backfill.state.set_state.side_effect

    async def flaky_set(integration_id, action_id, state, source_id="no-source"):
        if "sending_since" in state:
            writes.append(source_id)
            if len(writes) == 2:
                raise RuntimeError("Redis hiccup")
        return await real_set(integration_id, action_id, state, source_id)
    backfill.state.set_state.side_effect = flaky_set

    with pytest.raises(RuntimeError):
        await handlers._send_marked("i", ["1", "2", "3"], [{}, {}, {}])

    backfill.send.assert_not_called()
    assert "1" not in backfill.store and "2" not in backfill.store


@pytest.mark.asyncio
@pytest.mark.parametrize("cut_off", ["cancelled", "timeout"])
async def test_notes_stay_when_the_send_is_cut_off_or_times_out(backfill, cut_off):
    import asyncio
    import httpx

    backfill.send.side_effect = asyncio.CancelledError() if cut_off == "cancelled" else httpx.ReadTimeout("slow")

    with pytest.raises((asyncio.CancelledError, httpx.ReadTimeout)):
        await handlers._send_marked("i", ["1", "2"], [{}, {}])

    # Gundi may have created them: they stay marked and are never sent again.
    assert "sending_since" in backfill.store["1"] and "sending_since" in backfill.store["2"]


@pytest.mark.asyncio
async def test_a_start_that_never_gets_the_start_lock_stops(backfill, mocker):
    async def sleep(seconds):
        pass
    _fake_asyncio(mocker, sleep=sleep)
    backfill.lock_free = [False]

    with pytest.raises(handlers.BackfillStartBusy):
        await handlers._backfill_job_for("i", BackfillEventsConfig(), "f", now=END)
    assert "backfill_current" not in backfill.store


@pytest.mark.asyncio
async def test_a_busy_start_lock_is_reported_to_the_user(backfill, mocker):
    mocker.patch.object(handlers, "_backfill_job_for", AsyncMock(side_effect=handlers.BackfillStartBusy()))

    result = await handlers.action_backfill_events(_integration(), BackfillEventsConfig())

    assert result["result"]["events_extracted"] == 0
    assert "another start of this connection's backfill was still being set up" in _titles(backfill, LogLevel.WARNING)[-1]


@pytest.mark.asyncio
async def test_a_backfill_replaced_mid_run_stops_before_the_next_chunk(backfill, mocker):
    mocker.patch.object(handlers, "GUNDI_SUBMISSION_CHUNK_SIZE", 1)
    job = _job()
    backfill.store["backfill_current"] = {"created_to": END.isoformat()}
    real_send = backfill.send.side_effect

    def send_then_replace(events, integration_id):
        # A user starts a different backfill while this one is sending.
        backfill.store["backfill_current"] = {"created_to": (END + timedelta(hours=1)).isoformat()}
        return real_send(events, integration_id)
    backfill.send.side_effect = send_then_replace
    backfill.pages = [(0, 1, [_ob(1), _ob(2), _ob(3)])]

    await handlers._backfill(_integration(), job)

    assert backfill.send.call_count == 1
    assert not [c for c in backfill.trigger.call_args_list if c.args[1] == "backfill_events_continue"]


@pytest.mark.asyncio
async def test_a_replaced_backfill_doesnt_hand_over(backfill, mocker):
    clock = [1000.0]
    mocker.patch.object(handlers, "time", MagicMock(monotonic=lambda: clock[0]))
    mocker.patch.object(handlers, "_pull_time_budget", return_value=100)
    backfill.store["backfill_current"] = {"created_to": END.isoformat()}
    real_send = backfill.send.side_effect

    def slow_send_then_replace(events, integration_id):
        clock[0] += 150
        backfill.store["backfill_current"] = {"created_to": (END + timedelta(hours=1)).isoformat()}
        return real_send(events, integration_id)
    backfill.send.side_effect = slow_send_then_replace
    backfill.pages = [(0, 1, [_ob(1)]), (0, 1, [_ob(2)])]

    await handlers._backfill(_integration(), _job())

    backfill.trigger.assert_not_called()
