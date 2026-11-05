# iNaturalist Pull Limits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `pull_events` creates events only for sightings uploaded recently, updates an event only when what it sends has changed, sends at most a few photos per sighting, and keeps its cursor within `days_to_load` of now. Every limit is a connection setting with a default. A manual `backfill_events` action loads older sightings, creations only, up to `max_events`.

**Architecture:** All changes are in the connector's own code: `app/actions/configurations.py` (three settings), `app/actions/handlers.py` (one age check and one hash comparison in `_sort_observation`, one `max()` in `_get_load_since`, one slice in `_send_new_observations`, `sent_hash` in the two state writers, four counters) and `app/settings/integration.py` (comment only). Template code under `app/services/` is not touched. The iNaturalist query, the chunk-and-cursor loop, the patch call and the photo run are unchanged.

**Tech Stack:** Python 3.10, FastAPI action runner, `pyinaturalist` 0.19, pydantic v1, pytest + pytest-asyncio (strict mode: every async test needs `@pytest.mark.asyncio`) + pytest-mock.

**Spec:** `docs/superpowers/specs/2026-11-05-pull-limits-design.md`. Read §3 and §4 before starting.

**Out of scope for this plan:** dispatcher changes and changing a site's iNaturalist scope (spec §2 non-goals).

## Global Constraints

- **Age is `now − ob.created_at`**, never `updated_at`. `now` is `datetime.now(tz=timezone.utc)` taken once per run; naive `created_at` is treated as UTC.
- **A skipped observation stores nothing.** No state key, no photo key. It only moves the cursor as part of its chunk, exactly as an in-sync observation does today.
- **The update trigger is the payload hash.** `_payload_hash(event: dict) -> str` is `hashlib.sha256(json.dumps(event, sort_keys=True, default=str).encode()).hexdigest()`. The payload is the output of `_transform_inat_to_gundi_event`, which no longer contains `updated_at`. An observation in state is patched iff its `sent_hash` is missing or differs.
- **State row keeps its shape and `source_id`** (`str(inat_id)`): `object_id`, `inat_updated_at`, plus `sent_hash`. Written by `save_events_state` at creation and by `save_patched_events_state` after a patch. The pending-photos row is unchanged.
- **Defaults:** `max_age_days=90`, `max_photos=1`, `max_new_events_per_run=settings.INAT_MAX_NEW_EVENTS_PER_PULL`. Ranges: `max_age_days` 1–3650, `max_photos` 0–50, `max_new_events_per_run` 1–1000. `days_to_load` keeps 1–7.
- **`max_photos=0`** hands no photos and starts no photo run, even with `include_photos=True`. `include_photos=False` still wins over any `max_photos`.
- **`event_details.updated_at` is gone** from `_transform_inat_to_gundi_event`; `created_at` stays.
- **Result keys** (`_pull_result`): `events_extracted`, `events_updated`, `photos_attached` (unchanged, read by Gundi monitoring) plus `events_skipped_too_old`, `events_skipped_unchanged`, `photos_left_out`, `cursor_lag_days`. `cursor_lag_days` is `round((now − cursor).total_seconds() / 86400, 1)` using the last cursor the run wrote, or the stored cursor when the run wrote none (iNaturalist down, out of time before the first chunk); `None` only when there is neither.
- **Summary wording** (spec §4.2): "It sent N new iNaturalist observations to Gundi and updated M, and skipped K uploaded more than D days ago and U with nothing changed, and handed their P photos to a separate photo run (L left out by the photo limit)". The "skipped" clause lists only its non-zero parts and is omitted when both are zero; the "(L left out …)" clause only when L > 0.
- **Log lines never list observation ids, hashes or photo URLs**; counts only (existing rule in this repo).

## Review Focus

1. **An old sighting touched today.** Not in state, `created_at` 2 years ago: must not be created, must store nothing, and the chunk's cursor must still advance past it. Pinned in Task 3 (`test_sort_observation_skips_old_upload_without_storing`) and Task 5 (`test_pull_skips_old_uploads_and_moves_cursor`).
2. **A touch that changes nothing we send.** In state, `updated_at` newer than `inat_updated_at`, same payload (a comment was added): nothing is sent, nothing is written except a pending-photo resume. Pinned in Task 3 (`test_sort_observation_skips_unchanged_payload`).
3. **A row synced before this change.** In state with `object_id` and `inat_updated_at` but no `sent_hash`: patched once, and the patch writes the hash. Pinned in Task 3 (`test_sort_observation_patches_row_without_hash`) and Task 4 (`test_save_patched_events_state_stores_hash`).
4. **The cursor floor.** Stored `last_run` of 2026-06-22 with `days_to_load=7`: the run reads from 7 days ago, and the first saved cursor is newer than the floor. Pinned in Task 2 and Task 5.
5. **`max_photos` boundaries.** 1 hands exactly the first photo and counts the others; 0 hands none and does not call `trigger_action`; a value larger than the photo count hands them all. Pinned in Task 4.
6. **Existing connections.** A stored config without the new fields parses with the defaults, and `max_new_events_per_run` equals the env value so behaviour is unchanged until someone edits the connection. Pinned in Task 1.

---

## File Structure

| File | Responsibility |
|---|---|
| `app/actions/configurations.py` | `PullEventsConfig`: `max_age_days`, `max_photos`, `max_new_events_per_run`; form order |
| `app/settings/integration.py` | Comment on `INAT_MAX_NEW_EVENTS_PER_PULL`: now the connection default |
| `app/actions/handlers.py` | Cursor floor, age check, payload hash and comparison, photo slice, `sent_hash` in state, counters, summary wording, result keys |
| `app/actions/tests/test_configurations.py` | Defaults and ranges of the new fields |
| `app/actions/tests/test_handlers.py` | Unit tests of the changed helpers |
| `app/actions/tests/test_pull_events.py` | End-to-end pull: result keys, summary, cursor floor, unchanged skip |
| `CLAUDE.md` | Architecture step 3 and the state table: the hash trigger, `sent_hash`, the new settings; the backfill action |
| `app/datasource/inaturalist.py` | `iter_upload_pages`: one upload window, id ascending, resumable by taxa batch and id |
| `app/actions/configurations.py` | `BackfillEventsConfig` (generic, executable), `BackfillContinueConfig` (internal) |
| `app/actions/handlers.py` | `action_backfill_events`, `action_backfill_events_continue`, shared `_backfill` |
| `app/actions/tests/test_backfill.py` | Backfill behaviour |

---

### Task 1: Settings on the connection

**Files:**
- Modify: `app/actions/configurations.py:97-160` (`PullEventsConfig`, after `include_photos` at line 145; `ui_global_options.order` at line 148)
- Modify: `app/settings/integration.py:14-17` (comment)
- Test: `app/actions/tests/test_configurations.py` (append)

**Interfaces:**
- Produces, on `PullEventsConfig`:
  ```python
  max_age_days: int = FieldWithUIOptions(90, ge=1, le=3650, title="Max age (days)",
      description="Sightings uploaded to iNaturalist more than this many days ago are neither created nor updated in EarthRanger. Default: 90")
  max_photos: int = FieldWithUIOptions(1, ge=0, le=50, title="Photos per sighting",
      description="How many of a sighting's photos to send, in iNaturalist order. 0 sends none. Default: 1")
  max_new_events_per_run: int = FieldWithUIOptions(settings.INAT_MAX_NEW_EVENTS_PER_PULL, ge=1, le=1000,
      title="New events per run",
      description="Most new events one run sends; updates don't count. A larger backlog drains over several runs. Default: 150")
  ```
  `ui_options=UIOptions(widget="range")` on `max_age_days` only, to match `days_to_load`. Import `settings` from `app` if `configurations.py` does not already.
- Form order: `..., "include_photos", "max_photos", "max_age_days", "max_new_events_per_run", "run_on_schedule"`.

- [ ] **Step 1: Write the failing tests**

Append to `app/actions/tests/test_configurations.py` (use the file's existing `pytest` and `pydantic` imports):

```python
from app import settings
from app.actions.configurations import PullEventsConfig


def _config(**kwargs):
    return PullEventsConfig(bounding_box="[1, 1, 0, 0]", event_prefix="iNat: ", **kwargs)


def test_pull_limits_have_defaults():
    config = _config()
    assert config.max_age_days == 90
    assert config.max_photos == 1
    assert config.max_new_events_per_run == settings.INAT_MAX_NEW_EVENTS_PER_PULL


def test_pull_limits_accept_stored_config_without_them():
    stored = {"bounding_box": "[1, 1, 0, 0]", "event_prefix": "iNat: ", "days_to_load": 3}
    config = PullEventsConfig.parse_obj(stored)
    assert (config.max_age_days, config.max_photos) == (90, 1)


@pytest.mark.parametrize("field, bad", [
    ("max_age_days", 0), ("max_age_days", 3651),
    ("max_photos", -1), ("max_photos", 51),
    ("max_new_events_per_run", 0), ("max_new_events_per_run", 1001),
])
def test_pull_limits_reject_out_of_range(field, bad):
    with pytest.raises(pydantic.ValidationError):
        _config(**{field: bad})


def test_pull_limits_are_in_the_form_order():
    order = PullEventsConfig.__fields__["ui_global_options"].default.order
    assert order.index("include_photos") < order.index("max_photos") < order.index("max_age_days") \
        < order.index("max_new_events_per_run") < order.index("run_on_schedule")
```

- [ ] **Step 2: Run the tests and confirm they fail**

```bash
pytest app/actions/tests/test_configurations.py -k pull_limits -q
```

- [ ] **Step 3: Add the fields and the form order**

Add the three fields after `include_photos` and update `order`. Update the comment above `INAT_MAX_NEW_EVENTS_PER_PULL` in `app/settings/integration.py` to say it is the connection-level default of `max_new_events_per_run`.

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
pytest app/actions/tests/test_configurations.py -q
```

- [ ] **Step 5: Commit**

```bash
git add app/actions/configurations.py app/settings/integration.py app/actions/tests/test_configurations.py
git commit -m "Pull limits: max_age_days, max_photos and max_new_events_per_run on the connection"
```

---

### Task 2: The cursor never starts further back than `days_to_load`

**Files:**
- Modify: `app/actions/handlers.py:93-98` (`_get_load_since`)
- Test: `app/actions/tests/test_handlers.py:41-75` (the `_get_load_since` block)

**Interfaces:**
- `_get_load_since(state: dict, fallback_days: int) -> datetime` keeps its signature. Returns the stored cursor when it is newer than `now − fallback_days`, otherwise `now − fallback_days`. Always tz-aware UTC (a naive stored value is treated as UTC).

- [ ] **Step 1: Write the failing tests**

Add to the `_get_load_since` block:

```python
def test_get_load_since_never_starts_further_back_than_fallback_days():
    state = {"last_run": "2026-06-22 18:52:00+0000"}
    result = _get_load_since(state, 7)
    floor = datetime.now(tz=timezone.utc) - timedelta(days=7)
    assert abs((result - floor).total_seconds()) < 5


def test_get_load_since_keeps_a_recent_cursor():
    recent = datetime.now(tz=timezone.utc) - timedelta(hours=2)
    state = {"last_run": recent.strftime(STATE_DATETIME_FMT)}
    result = _get_load_since(state, 7)
    assert abs((result - recent).total_seconds()) < 1
```

The existing tests at lines 54–75 build cursors with fixed old dates and assert the cursor comes back. Change those fixtures to recent dates (`now − 1 h`) so they still assert "cursor wins when recent".

- [ ] **Step 2: Run and confirm they fail**

```bash
pytest app/actions/tests/test_handlers.py -k get_load_since -q
```

- [ ] **Step 3: Implement**

```python
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
```

Update the description of `days_to_load` in `configurations.py` (line 113): "…If the integration state contains a last_run value, data is loaded since that value, but never from further back than this many days."

- [ ] **Step 4: Run and confirm they pass**

```bash
pytest app/actions/tests/test_handlers.py -k get_load_since -q
```

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/configurations.py app/actions/tests/test_handlers.py
git commit -m "Pull limits: the cursor never starts further back than days_to_load"
```

---

### Task 3: Age check and payload comparison in `_sort_observation`

**Files:**
- Modify: `app/actions/handlers.py:574-603` (`_sort_observation`), `:433-480` (loop: `now`, `max_new`, counting), `:941-990` (`_transform_inat_to_gundi_event`: remove `"updated_at": ob.updated_at`), `:200-252` (`_PullProgress`: `skipped_too_old`, `skipped_unchanged`), add `_payload_hash` near the transform; add `import hashlib, json` and `STATE_SENT_HASH_KEY = "sent_hash"` next to `STATE_INAT_UPDATED_AT_KEY` (line 75)
- Test: `app/actions/tests/test_handlers.py` (the `_sort_observation` tests around lines 255–310; the `_transform` block at 135–225)

**Interfaces:**
- `_payload_hash(event: dict) -> str` per Global Constraints.
- `_sort_observation(integration, action_config, observation, new_ids: set, patch_targets: dict, photos_left_behind: set, *, now: datetime) -> str` returns `"too_old"`, `"new"`, `"update"` or `"unchanged"`; the loop counts on the return value and the sets/dict are filled as today. `patch_targets` keeps its shape (`{inat_id: object_id}`).

- [ ] **Step 1: Write the failing tests**

Replace the existing "should not patch when updated_at is equal" test (line ~261) and add (fixtures: `integration` and a `_config()` helper as the file already has; `state_manager` patched with `AsyncMock`s):

```python
@pytest.mark.asyncio
async def test_sort_observation_skips_old_upload_without_storing(mocker):
    state_manager = mocker.patch("app.actions.handlers.state_manager")
    state_manager.get_state = AsyncMock(return_value=None)
    now = datetime(2026, 11, 5, tzinfo=timezone.utc)
    ob = _observation(created_at=now - timedelta(days=400))
    new_ids, targets, left_behind = set(), {}, set()
    outcome = await _sort_observation(integration, _config(), ob, new_ids, targets, left_behind, now=now)
    assert outcome == "too_old"
    assert not new_ids and not targets and not left_behind
    state_manager.set_state.assert_not_called()


@pytest.mark.asyncio
async def test_sort_observation_skips_old_upload_even_when_in_state(mocker):
    # state present, created_at 400 days ago → "too_old", targets empty.


@pytest.mark.asyncio
async def test_sort_observation_creates_recent_upload(mocker):
    # not in state, created_at 89 days ago → "new"; naive created_at is treated as UTC.


@pytest.mark.asyncio
async def test_sort_observation_skips_unchanged_payload(mocker):
    # state {"object_id": "g1", "inat_updated_at": "2000-01-01 00:00:00+0000",
    #        "sent_hash": _payload_hash(_transform_inat_to_gundi_event(ob, config))}
    # ob.updated_at today → "unchanged", targets empty, nothing written.


@pytest.mark.asyncio
async def test_sort_observation_updates_changed_payload(mocker):
    # same state but sent_hash of a payload with another taxon → "update", targets == {ob.id: "g1"}.


@pytest.mark.asyncio
async def test_sort_observation_patches_row_without_hash(mocker):
    # state {"object_id": "g1", "inat_updated_at": "..."} (no sent_hash) → "update".


@pytest.mark.asyncio
async def test_sort_observation_resumes_pending_photos(mocker):
    # state present, same hash, photos state {"pending": [[1, "u"]]} → "unchanged" and id in photos_left_behind.


def test_payload_hash_is_stable_and_order_independent():
    a = {"title": "x", "event_details": {"taxon_id": 1, "created_at": datetime(2026, 1, 1)}}
    b = {"event_details": {"created_at": datetime(2026, 1, 1), "taxon_id": 1}, "title": "x"}
    assert _payload_hash(a) == _payload_hash(b)
    assert _payload_hash(a) != _payload_hash({**a, "title": "y"})


def test_transform_inat_to_gundi_event_has_no_updated_at():
    event = _transform_inat_to_gundi_event(_observation(), _config())
    assert "updated_at" not in event["event_details"]
    assert "created_at" in event["event_details"]
```

Adjust the existing transform tests (line 161 builds `updated_at`) so they stop asserting on it.

- [ ] **Step 2: Run and confirm they fail**

```bash
pytest app/actions/tests/test_handlers.py -k "sort_observation or payload_hash or transform" -q
```

- [ ] **Step 3: Implement**

```python
def _payload_hash(event: dict) -> str:
    """What was sent, as one string: equal payloads hash equal across runs."""
    return hashlib.sha256(json.dumps(event, sort_keys=True, default=str).encode()).hexdigest()


async def _sort_observation(integration, action_config, observation, new_ids: set, patch_targets: dict,
                            photos_left_behind: set, *, now: datetime) -> str:
    """"too_old" when the observation was uploaded more than max_age_days ago
    (nothing is stored for it); "new" when it isn't in Gundi yet; "update"
    when it is and what we would send differs from what was last sent (or no
    hash was stored yet); "unchanged" otherwise. One in Gundi whose photos
    are still pending also goes in photos_left_behind."""
    created = observation.created_at
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    if created < now - timedelta(days=action_config.max_age_days):
        return "too_old"
    saved_event = await state_manager.get_state(str(integration.id), "pull_events", str(observation.id))
    if not saved_event:
        new_ids.add(observation.id)
        return "new"
    photos = await state_manager.get_state(str(integration.id), "pull_events", STATE_PHOTOS_PREFIX + str(observation.id))
    if (photos or {}).get("pending"):
        photos_left_behind.add(observation.id)
    payload = _transform_inat_to_gundi_event(observation, action_config)
    if saved_event.get(STATE_SENT_HASH_KEY) == _payload_hash(payload):
        return "unchanged"
    patch_targets[observation.id] = saved_event.get("object_id")
    return "update"
```

In the loop: take `now = datetime.now(tz=timezone.utc)` once before the `while`; pass `action_config` and `now=now`; count `"too_old"` into `progress.skipped_too_old` and `"unchanged"` into `progress.skipped_unchanged`. Use `action_config.max_new_events_per_run` for `max_new` (line 433) and in `_log_pull_finished` (line 296). Remove the `updated_at` line from the transform. `patch_events` is unchanged: it already sends the whole transformed payload.

- [ ] **Step 4: Run the whole suite**

```bash
pytest -q
```

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_handlers.py
git commit -m "Pull limits: skip old uploads; update only when the payload changed"
```

---

### Task 4: `sent_hash` in state and `max_photos`

**Files:**
- Modify: `app/actions/handlers.py:605-635` (`_send_new_observations`), `:864-891` (`save_events_state`), `:893-920` (`save_patched_events_state`), `:200-252` (`_PullProgress.photos_left_out`)
- Test: `app/actions/tests/test_handlers.py` (`_send_new_observations` tests around 1395; `save_patched_events_state` around 1436)

**Interfaces:**
- `save_events_state(response, events, integration, inat_updated_at_map=None, pending_photos=None)` unchanged signature; writes `sent_hash = _payload_hash(event)` into each row.
- `save_patched_events_state(events_to_patch, integration, action_config)` gains `action_config` (to rebuild the payload) and writes `object_id`, `inat_updated_at`, `sent_hash`.
- `_send_new_observations` hands `ob.photos[:action_config.max_photos]` when `include_photos` and `max_photos > 0`; `progress.photos_left_out += len(ob.photos) − len(pending)` for each observation Gundi accepted (`events[:len(response)]`), to match `photos_handed_off`.

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.parametrize("max_photos, handed, left_out, run_started", [
    (1, 1, 2, True), (0, 0, 3, False), (5, 3, 0, True),
])
@pytest.mark.asyncio
async def test_send_new_observations_hands_first_max_photos(mocker, max_photos, handed, left_out, run_started):
    # one observation with 3 photos; assert the pending list length saved for it,
    # progress.photos_left_out and whether trigger_action was called.


@pytest.mark.asyncio
async def test_save_events_state_stores_sent_hash(mocker):
    # the row written for the observation has sent_hash == _payload_hash(event).


@pytest.mark.asyncio
async def test_save_patched_events_state_stores_hash(mocker):
    # row keeps object_id, has the new inat_updated_at and sent_hash of the rebuilt payload.
```

- [ ] **Step 2: Run and confirm they fail**

```bash
pytest app/actions/tests/test_handlers.py -k "send_new_observations or save_events_state or save_patched" -q
```

- [ ] **Step 3: Implement**

In `_send_new_observations`:

```python
        if action_config.include_photos and action_config.max_photos > 0:
            photos = ob.photos[:action_config.max_photos]
            pending_photos[inat_id] = [[p.id, p.large_url if p.large_url else p.url] for p in photos]
            left_out[inat_id] = len(ob.photos) - len(photos)
```

and after the send, `progress.photos_left_out += sum(left_out.get(e['event_details']['inat_id'], 0) for e in events[:len(response)])`.

In `save_events_state`, inside the loop: `state[STATE_SENT_HASH_KEY] = _payload_hash(event)`. In `save_patched_events_state`, build `state` with `STATE_SENT_HASH_KEY: _payload_hash(_transform_inat_to_gundi_event(observation, action_config))` and pass `action_config` from the loop's call site (line ~503).

- [ ] **Step 4: Run the suite**

```bash
pytest -q
```

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_handlers.py
git commit -m "Pull limits: sent_hash in state; first max_photos photos only"
```

---

### Task 5: Counters, summary wording, result keys and docs

**Files:**
- Modify: `app/actions/handlers.py:200-252` (`_PullProgress.counts()`, `data()`), `:288-300` (`_log_pull_finished`), `:555-560` (`_pull_result`) and its call sites (`:493`, `:540`, `:552`)
- Modify: `CLAUDE.md` (Architecture step 3, the state table, the result keys line, the env-vars paragraph)
- Test: `app/actions/tests/test_pull_events.py:13-56` (end-to-end), `:298-327` (chunked updates), `test_handlers.py` (`_PullProgress` tests around 820–835)

**Interfaces:**
- `_pull_result(added, updated, photos, *, skipped_too_old, skipped_unchanged, photos_left_out, cursor, now) -> dict` returns the keys of Global Constraints.
- `_PullProgress(max_age_days: int)`; `counts()` sentence per Global Constraints; `data()` adds the three counters.

- [ ] **Step 1: Write the failing tests**

In `test_execute_pull_observations_action` (`test_pull_events.py:13`): assert `events_skipped_too_old == 0`, `events_skipped_unchanged == 0`, `photos_left_out == 3` (the fixture's two observations carry 5 photos; with `max_photos=1` two are handed, three left out) and `cursor_lag_days` is a float (the recorded fixture observations are from 2024, so its value is large); change the summary assertion to:

```python
    assert ("It sent 2 new iNaturalist observations to Gundi and updated 0, "
            "and handed their 2 photos to a separate photo run (3 left out by the photo limit)") in summary
```

In `test_pull_events_saves_updated_observations_per_chunk` (`:298`): the stale state row must carry a `sent_hash` that differs from the fixture observations' payloads (any string does), so both are updates as before; the assertions stay.

Add `test_pull_skips_old_uploads_and_moves_cursor`: a page with one recent and one old observation (`created_at` 400 days ago), state empty → `events_extracted == 1`, `events_skipped_too_old == 1`, summary contains "and skipped 1 uploaded more than 90 days ago", and exactly one cursor save whose value is the chunk's last `updated_at`.

Add `test_pull_skips_unchanged_observations`: state rows whose `sent_hash` equals the fixture payloads → `events_updated == 0`, `events_skipped_unchanged == 2`, no `update_event_in_gundi` call, summary contains "2 with nothing changed".

Add `_PullProgress` tests: `counts()` with all new counters zero has neither clause; with `skipped_too_old=3, skipped_unchanged=0` has "and skipped 3 uploaded more than 90 days ago" only; with both and `photos_left_out` has all.

- [ ] **Step 2: Run and confirm they fail**

```bash
pytest app/actions/tests/test_pull_events.py app/actions/tests/test_handlers.py -k "pull_observations_action or skips_old or skips_unchanged or per_chunk or PullProgress or counts" -q
```

- [ ] **Step 3: Implement**

`counts()`:

```python
        text = (f"it sent {prefix}{_count(self.events_sent, 'new iNaturalist observation', 'new iNaturalist observations')} "
                f"to Gundi and updated {self.events_updated}")
        skipped = []
        if self.skipped_too_old:
            skipped.append(f"{self.skipped_too_old} uploaded more than {self.max_age_days} days ago")
        if self.skipped_unchanged:
            skipped.append(f"{self.skipped_unchanged} with nothing changed")
        if skipped:
            text += ", and skipped " + " and ".join(skipped)
        if self.photos_handed_off:
            text += f", and handed their {_count(self.photos_handed_off, 'photo', 'photos')} to a separate photo run"
            if self.photos_left_out:
                text += f" ({self.photos_left_out} left out by the photo limit)"
        return text
```

`_run_pull` builds `_PullProgress(action_config.max_age_days)`. `data()` adds `"events_skipped_too_old"`, `"events_skipped_unchanged"`, `"photos_left_out"`. `_pull_result` computes `cursor_lag_days` from the cursor it is given (`done_through`, or `now` on the nothing-matched path, or `None`).

Update `CLAUDE.md`: step 3 ("patch only if the payload hash differs from `sent_hash`, within `max_age_days`"), step 7 (`sent_hash`), the state table row, the result-keys convention (add the four keys), and the env-vars paragraph (`INAT_MAX_NEW_EVENTS_PER_PULL` is the default of the connection's `max_new_events_per_run`).

- [ ] **Step 4: Run the suite**

```bash
pytest -q
```

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_pull_events.py app/actions/tests/test_handlers.py CLAUDE.md
git commit -m "Pull limits: skipped and left-out counters, cursor lag in the run result"
```

---

### Task 6: Manual backfill (spec §5)

**Files:**
- Modify: `app/datasource/inaturalist.py` (after `iter_observation_pages`)
- Modify: `app/actions/configurations.py` (after `AddPhotosConfig`)
- Modify: `app/actions/handlers.py` (after `action_add_photos`); the pull's "lock taken" message names a backfill too
- Test: `app/datasource/tests/test_inaturalist.py` (append), `app/actions/tests/test_backfill.py` (new), `app/actions/tests/test_configurations.py` (append)

**Interfaces:**
- ```python
  def iter_upload_pages(created_from: datetime, created_to: datetime, *, batch_index: int = 0, after_id: int = 0,
                        bounding_box=None, taxa=None, projects=None, quality_grade=None, annotations=None
                        ) -> Iterator[Tuple[int, List[Observation]]]
  ```
  Same base query as `_observation_query` minus `updated_since`/`order`, plus `created_d1`, `created_d2` (ISO), `order_by="id"`, `order="asc"`, `id_above`, `per_page=INAT_PAGE_SIZE`, `fields`. Yields `(batch_index, batch_count, page)` until a page comes back short or empty, batch by batch, starting at `batch_index` with `after_id` and later batches from 0.
- ```python
  class BackfillEventsConfig(GenericActionConfiguration, ExecutableActionMixin):
      days_back: int = 30      # ge=1, le=3650
      max_events: int = 1000   # ge=1, le=100_000
  class BackfillContinueConfig(InternalActionConfiguration):
      created_from: datetime; created_to: datetime; max_events: int
      created_so_far: int = 0; batch_index: int = 0; after_id: int = 0
      lock_waits: int = 0; runs_without_progress: int = 0
  ```
- `action_backfill_events` (title "Backfill iNaturalist Observations") parses the pull settings, picks its job with `_backfill_job_for(integration_id, config, filters_fingerprint, now=None)` (spec B10: continue, repeat, or a new window that supersedes the previous one) and calls `_backfill`. `action_backfill_events_continue` (title "Continue iNaturalist Backfill") calls `_backfill` with its config. Both `@_clock_run @activity_logger()`.
- `_backfill(integration, job) -> dict`:
  1. Parse the connection's `pull_events` row (`_pull_settings`); none → error log "The backfill didn't run because the connection has no pull settings…", return. A saved position that is `done` or has a higher `seq` → return quietly (B11). A changed filter fingerprint → mark done, error log, return (B12).
  2. Parse the pull row; a `ValidationError` → error log naming the invalid fields, return. Take the run lock (`STATE_RUN_LOCK_SOURCE_ID`, same TTL); taken → retry every `BACKFILL_LOCK_WAIT_SECONDS = 10` until the time budget; still taken → hand over with `lock_waits + 1`, or after `BACKFILL_MAX_LOCK_WAITS = 3` an error log, return. A pull that finds the lock taken sets `pull_waiting` (TTL = lock TTL) and clears it when it runs.
  3. Take the saved position under `backfill.<window end>` when it is further along. Walk `iter_upload_pages` in a thread (as the pull does); before each page check the time and `pull_waiting`, before each chunk the time. Annotation-filter, keep sightings not in state, cut at `max_events − created_so_far`, send in chunks of `GUNDI_SUBMISSION_CHUNK_SIZE` with `send_events_to_gundi`, save the accepted ones with `save_events_state` (no pending photos). After each chunk set `after_id` to the last sighting examined (or the last one Gundi accepted, then hand over), `uploaded_up_to` to its `created_at`, and save the position. Unknown projects go through `_drop_unknown_projects(..., activity="backfill", action_id="backfill_events")`.
  4. Stop at `max_events` (summary "stopped at its limit") or the end of the window (summary "finished"), keeping the saved position with a `done` marker and `ended_at`; hand over at the time budget, a waiting pull, a read that outlasts the budget or a partial Gundi answer (the continuation, then any pull, are started after the lock is released; the info log is published only once the continuation started, otherwise an error log), counting `runs_without_progress` and stopping with an error at `BACKFILL_MAX_RUNS_WITHOUT_PROGRESS = 3`; stop with an error when iNaturalist is down. Release the lock in `finally`.
  5. Return the result of spec §5.3.

- [ ] **Step 1: Tests** — `test_backfill.py`: query arguments (window, id order, filters); skip in-state; no `update_event_in_gundi`, no `trigger_action("add_photos")`, no `photos.` rows; state has `sent_hash`; stop at `max_events` with the "limit" summary and `backfill_finished False`; end of window with the "finished" summary; time budget hands over with `batch_index`/`after_id`/`created_so_far` and the continuation resumes from them; lock taken then free → runs; lock never free → error, nothing sent; iNat down → error; no pull row → error. Registration: `backfill_events` registered with `is_periodic_action False` and `is_executable`; `backfill_events_continue` not registered.
- [ ] **Step 2: Implement** per the interfaces above.
- [ ] **Step 3: Run the suite**

```bash
pytest -q
```

- [ ] **Step 4: Commit**

```bash
git add app/datasource/inaturalist.py app/actions/configurations.py app/actions/handlers.py app/actions/tests app/datasource/tests CLAUDE.md
git commit -m "Pull limits: manual backfill, creations only, up to max_events"
```

---

## Verification before merge (manual)

1. `pytest -q` green.
2. Register the runner against the dev portal (`python -m app.register --slug inaturalist`), open an iNaturalist connection and confirm the three new fields render after "Include photos" with their defaults.
3. Run `pull_events` on a dev connection whose state has an old `last_run`: the first summary shows the cursor floor (reads from `days_to_load` days ago), `events_skipped_too_old` > 0 for old touched sightings, `cursor_lag_days` under 1 after the run.
4. In the destination EarthRanger, open one new event: one photo, no `Updated At` in the details.
5. Add a comment to that sighting on iNaturalist, run again: `events_skipped_unchanged` counts it, the EarthRanger history has no new line. Add an identification that changes the taxon, run again: one history line naming the taxon fields and the title.
6. Run "Backfill iNaturalist Observations" with `days_back=365`, `max_events=20`: 20 events created, no photos, the summary says it stopped at its limit and up to which upload date; run it again: 20 more, the first 20 are skipped. Confirm the portal shows a Run button for the action and that every action has its friendly name.
