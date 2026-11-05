# iNaturalist Pull Limits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `pull_events` creates events only for sightings uploaded recently, sends at most a few photos per sighting, never updates an event, and keeps its cursor within `days_to_load` of now. Every limit is a connection setting with a default.

**Architecture:** All changes are in the connector's own code: `app/actions/configurations.py` (three settings), `app/actions/handlers.py` (one age check in `_sort_observation`, one `max()` in `_get_load_since`, one slice in `_send_new_observations`, removal of the patch path, three counters) and `app/settings/integration.py` (comment only). Template code under `app/services/` is not touched. The iNaturalist query, the chunk-and-cursor loop and the photo run are unchanged.

**Tech Stack:** Python 3.10, FastAPI action runner, `pyinaturalist` 0.19, pydantic v1, pytest + pytest-asyncio (strict mode: every async test needs `@pytest.mark.asyncio`) + pytest-mock.

**Spec:** `docs/superpowers/specs/2026-11-05-pull-limits-design.md`. Read §3 and §4 before starting.

**Out of scope for this plan:** any event update rule, dispatcher changes, and changing a site's iNaturalist scope (spec §2 non-goals).

## Global Constraints

- **Age is `now − ob.created_at`**, never `updated_at`. `now` is `datetime.now(tz=timezone.utc)` taken once per run; naive `created_at` is treated as UTC.
- **A skipped observation stores nothing.** No state key, no photo key. It only moves the cursor as part of its chunk, exactly as an in-sync observation does today.
- **No patch path remains.** `patch_events`, `save_patched_events_state`, `patch_targets` and `update_event_in_gundi`'s import are removed from `handlers.py`. `app/services/gundi.py` keeps `update_event_in_gundi` untouched (template-shaped file).
- **State written at creation is unchanged** (`object_id`, `inat_updated_at`, pending photos), so a later update rule can start from it.
- **`events_updated` stays in `_pull_result`**, always `0`: Gundi monitoring reads the key.
- **Defaults:** `max_age_days=90`, `max_photos=1`, `max_new_events_per_run=settings.INAT_MAX_NEW_EVENTS_PER_PULL`. Ranges: `max_age_days` 1–3650, `max_photos` 0–50, `max_new_events_per_run` 1–1000. `days_to_load` keeps 1–7.
- **`max_photos=0`** hands no photos and starts no photo run, even with `include_photos=True`. `include_photos=False` still wins over any `max_photos`.
- **`event_details.updated_at` is gone** from `_transform_inat_to_gundi_event`; `created_at` stays.
- **Counters** live on `_PullProgress` (`skipped_too_old`, `photos_left_out`) and in `_pull_result` (`events_skipped_too_old`, `photos_left_out`, `cursor_lag_days`). `cursor_lag_days` is `round((now − cursor).total_seconds() / 86400, 1)` using the last cursor written by the run (`done_through`, or `now` when the run advanced the cursor because nothing matched); `None` when no cursor was written.
- **Summary wording** (spec §4.2): "It sent N new iNaturalist observations to Gundi and skipped K uploaded more than D days ago, and handed their P photos to a separate photo run (L left out by the photo limit)". The "skipped …" clause appears only when K > 0; the "(L left out …)" clause only when L > 0. "and updated M" is removed.
- **Log lines never list observation ids or photo URLs**; counts only (existing rule in this repo).

## Review Focus

1. **An old sighting touched today.** Not in state, `created_at` 2 years ago: must not be created, must store nothing, and the chunk's cursor must still advance past it. Pinned in Task 3 (`test_sort_observation_skips_old_upload_without_storing`) and Task 5 (`test_pull_skips_old_uploads_and_moves_cursor`).
2. **A sighting in state that changed.** `updated_at` newer than the stored `inat_updated_at`: nothing is sent, nothing is written except a pending-photo resume. Pinned in Task 3 (`test_sort_observation_never_patches`).
3. **The cursor floor.** Stored `last_run` of 2026-06-22 with `days_to_load=7`: the run reads from 7 days ago, and the first saved cursor is newer than the floor. Pinned in Task 2 and Task 5.
4. **`max_photos` boundaries.** 1 hands exactly the first photo and counts the others; 0 hands none and does not call `trigger_action`; a value larger than the photo count hands them all. Pinned in Task 4.
5. **Existing connections.** A stored config without the new fields parses with the defaults, and `max_new_events_per_run` equals the env value so behaviour is unchanged until someone edits the connection. Pinned in Task 1.

---

## File Structure

| File | Responsibility |
|---|---|
| `app/actions/configurations.py` | `PullEventsConfig`: `max_age_days`, `max_photos`, `max_new_events_per_run`; form order |
| `app/settings/integration.py` | Comment on `INAT_MAX_NEW_EVENTS_PER_PULL`: now the connection default |
| `app/actions/handlers.py` | Cursor floor, age check, photo slice, patch-path removal, counters, summary wording, result keys |
| `app/actions/tests/test_configurations.py` | Defaults and ranges of the new fields |
| `app/actions/tests/test_handlers.py` | Unit tests of the changed helpers; removal of patch-path tests |
| `app/actions/tests/test_pull_events.py` | End-to-end pull: result keys, summary, cursor floor, no patch |
| `README.md` | The three settings in the configuration section (if the README lists settings; otherwise skip) |

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
      description="Sightings uploaded to iNaturalist more than this many days ago are not sent to EarthRanger. Default: 90")
  max_photos: int = FieldWithUIOptions(1, ge=0, le=50, title="Photos per sighting",
      description="How many of a sighting's photos to send, in iNaturalist order. 0 sends none. Default: 1")
  max_new_events_per_run: int = FieldWithUIOptions(settings.INAT_MAX_NEW_EVENTS_PER_PULL, ge=1, le=1000,
      title="New events per run",
      description="Most new events one run sends. A larger backlog drains over several runs. Default: 150")
  ```
  `ui_options=UIOptions(widget="range")` on `max_age_days` only, to match `days_to_load`. Import `settings` from `app` if `configurations.py` does not already.
- Form order: `..., "include_photos", "max_photos", "max_age_days", "max_new_events_per_run", "run_on_schedule"`.

- [ ] **Step 1: Write the failing tests**

Append to `app/actions/tests/test_configurations.py`:

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

(Use whichever existing imports of `pytest` and `pydantic` the file already has.)

- [ ] **Step 2: Run the tests and confirm they fail**

```bash
pytest app/actions/tests/test_configurations.py -k pull_limits -q
```

Expected: failures on missing attributes.

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

Check the existing tests at lines 54–75: they build cursors with fixed old dates and assert the cursor comes back. Change those fixtures to recent dates (`now − 1 h`) so they still assert "cursor wins when recent".

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

Update the docstring of `days_to_load` in `configurations.py` (line 113): "…If the integration state contains a last_run value, data is loaded since that value, but never from further back than this many days."

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

### Task 3: Age check and no updates in `_sort_observation`

**Files:**
- Modify: `app/actions/handlers.py:574-603` (`_sort_observation`), `:433-520` (loop: drop `patch_targets` and the update branch), `:846-860` (`patch_events`, delete), `:893-920` (`save_patched_events_state`, delete), `:31-35` (import of `update_event_in_gundi`, drop), `:200-252` (`_PullProgress`: add `skipped_too_old`, remove `events_updated` from `counts()` text but keep it in `data()` as `0`)
- Test: `app/actions/tests/test_handlers.py` (the `_sort_observation` tests around lines 255–310; patch-path tests around 1036–1076 and 1436–1476)

**Interfaces:**
- `_sort_observation(integration, observation, new_ids: set, photos_left_behind: set, *, now: datetime, max_age_days: int) -> str` returns one of `"new"`, `"too_old"`, `"in_state"` (the loop counts on the return value; sets are still filled as today). `patch_targets` is gone.
- `_PullProgress.skipped_too_old: int`; `counts()` builds the sentence of Global Constraints; `data()` adds `"events_skipped_too_old"`.

- [ ] **Step 1: Write the failing tests**

Replace the existing "should not patch" test (line ~261) and add:

```python
@pytest.mark.asyncio
async def test_sort_observation_skips_old_upload_without_storing(mocker):
    state_manager = mocker.patch("app.actions.handlers.state_manager")
    state_manager.get_state = AsyncMock(return_value=None)
    now = datetime(2026, 11, 5, tzinfo=timezone.utc)
    ob = MagicMock(id=1, created_at=now - timedelta(days=400))
    new_ids, left_behind = set(), set()
    outcome = await _sort_observation(integration, ob, new_ids, left_behind, now=now, max_age_days=90)
    assert outcome == "too_old"
    assert not new_ids and not left_behind
    state_manager.set_state.assert_not_called()


@pytest.mark.asyncio
async def test_sort_observation_creates_recent_upload(mocker):
    # created_at 89 days ago → "new"; naive created_at is treated as UTC.


@pytest.mark.asyncio
async def test_sort_observation_never_patches(mocker):
    # state {"object_id": "g1", "inat_updated_at": "2000-01-01 00:00:00+0000"}, ob.updated_at today → "in_state",
    # new_ids empty, nothing written.


@pytest.mark.asyncio
async def test_sort_observation_resumes_pending_photos(mocker):
    # state present and photos state {"pending": [[1, "u"]]} → "in_state" and id in photos_left_behind.
```

Delete `test_pull_events_saves_updated_observations_per_chunk` (`test_pull_events.py:298-327`) and the handler tests that patch `patch_events` / `save_patched_events_state` (`test_handlers.py` ~1066-1076, ~1436-1476). Where those tests also covered the pause/stop-on-time behaviour, keep that coverage by making the fixture produce **new** observations instead of updates (the chunk loop is the same).

- [ ] **Step 2: Run and confirm they fail**

```bash
pytest app/actions/tests/test_handlers.py -k sort_observation -q
```

- [ ] **Step 3: Implement**

```python
async def _sort_observation(integration, observation, new_ids: set, photos_left_behind: set,
                            *, now: datetime, max_age_days: int) -> str:
    """"new" when the observation isn't in Gundi and was uploaded within
    max_age_days; "too_old" when it isn't in Gundi and is older (nothing is
    stored for it); "in_state" otherwise. An observation in Gundi is never
    updated. One in Gundi whose photos are still pending also goes in
    photos_left_behind."""
    saved_event = await state_manager.get_state(str(integration.id), "pull_events", str(observation.id))
    if not saved_event:
        created = observation.created_at
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if created < now - timedelta(days=max_age_days):
            return "too_old"
        new_ids.add(observation.id)
        return "new"
    photos = await state_manager.get_state(str(integration.id), "pull_events", STATE_PHOTOS_PREFIX + str(observation.id))
    if (photos or {}).get("pending"):
        photos_left_behind.add(observation.id)
    return "in_state"
```

In the loop (line ~467): take `now = datetime.now(tz=timezone.utc)` once before the `while`; call `_sort_observation(..., now=now, max_age_days=action_config.max_age_days)`; `if outcome == "too_old": progress.skipped_too_old += 1`. Use `action_config.max_new_events_per_run` instead of `settings.INAT_MAX_NEW_EVENTS_PER_PULL` for `max_new` (line 433) and in `_log_pull_finished` (line 296). Remove `patch_targets`, `to_patch`, the `for item in to_patch` block, `updated_count`, `patch_events`, `save_patched_events_state` and the `update_event_in_gundi` import. `_pull_result(added, photos, ...)` passes `0` for `events_updated`.

- [ ] **Step 4: Run the whole suite**

```bash
pytest -q
```

Expected: green, minus the deleted tests.

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_handlers.py app/actions/tests/test_pull_events.py
git commit -m "Pull limits: skip old uploads, never update an event"
```

---

### Task 4: `max_photos` and `updated_at` out of the payload

**Files:**
- Modify: `app/actions/handlers.py:605-635` (`_send_new_observations`), `:941-990` (`_transform_inat_to_gundi_event`: remove `"updated_at": ob.updated_at`), `:200-252` (`_PullProgress.photos_left_out`, `counts()`, `data()`)
- Test: `app/actions/tests/test_handlers.py` (`_transform` block at 135–225; `_send_new_observations` tests around 1395)

**Interfaces:**
- `_send_new_observations(integration, action_config, observations, progress) -> tuple[int, int]` unchanged signature. Pending photos per observation are `ob.photos[:action_config.max_photos]` when `include_photos` and `max_photos > 0`, else none. `progress.photos_left_out += len(ob.photos) − len(pending)` for each sent observation.

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.parametrize("max_photos, handed, left_out, run_started", [
    (1, 1, 2, True), (0, 0, 3, False), (5, 3, 0, True),
])
@pytest.mark.asyncio
async def test_send_new_observations_hands_first_max_photos(mocker, max_photos, handed, left_out, run_started):
    # one observation with 3 photos; assert pending list length, progress.photos_left_out and trigger_action calls.


def test_transform_inat_to_gundi_event_has_no_updated_at():
    event = _transform_inat_to_gundi_event(_observation(), _config())
    assert "updated_at" not in event["event_details"]
    assert "created_at" in event["event_details"]
```

Adjust the existing transform tests (line 161 builds `updated_at`) so they stop asserting on it.

- [ ] **Step 2: Run and confirm they fail**

```bash
pytest app/actions/tests/test_handlers.py -k "send_new_observations or transform" -q
```

- [ ] **Step 3: Implement**

In `_send_new_observations`:

```python
        if action_config.include_photos and action_config.max_photos > 0:
            photos = ob.photos[:action_config.max_photos]
            pending_photos[inat_id] = [[p.id, p.large_url if p.large_url else p.url] for p in photos]
            progress.photos_left_out += len(ob.photos) - len(photos)
```

(Count `photos_left_out` only for observations in `events[:len(response)]`, i.e. the ones Gundi accepted, to match `photos_handed_off`.) Remove the `updated_at` line from the transform.

- [ ] **Step 4: Run the suite**

```bash
pytest -q
```

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_handlers.py
git commit -m "Pull limits: first max_photos photos only; updated_at leaves the payload"
```

---

### Task 5: Counters, summary wording and result keys

**Files:**
- Modify: `app/actions/handlers.py:200-252` (`_PullProgress.counts()`, `data()`), `:288-300` (`_log_pull_finished`), `:555-560` (`_pull_result`), the loop's return sites (`:493`, `:540`, `:552`)
- Test: `app/actions/tests/test_pull_events.py:13-56` (end-to-end), `test_handlers.py` (`_PullProgress` tests around 820–835)

**Interfaces:**
- `_pull_result(added: int, photos: int, skipped_too_old: int, photos_left_out: int, cursor: Optional[datetime], now: datetime) -> dict` returns `{"result": {"events_extracted", "events_updated": 0, "photos_attached", "events_skipped_too_old", "photos_left_out", "cursor_lag_days"}}`.
- `_PullProgress.counts()` sentence per Global Constraints; `data()` adds the two counters.

- [ ] **Step 1: Write the failing tests**

In `test_execute_pull_observations_action` (`test_pull_events.py:13`): assert `events_skipped_too_old == 0`, `photos_left_out == 3` (the fixture's two observations carry 5 photos; with `max_photos=1` two are handed, three left out) and `cursor_lag_days` is a float `< 1`; change the summary assertion to:

```python
    assert ("It sent 2 new iNaturalist observations to Gundi, "
            "and handed their 2 photos to a separate photo run (3 left out by the photo limit)") in summary
```

Add `test_pull_skips_old_uploads_and_moves_cursor`: fixture page with one recent and one old observation (`created_at` 400 days ago), state empty → `events_extracted == 1`, `events_skipped_too_old == 1`, summary contains "and skipped 1 uploaded more than 90 days ago", and exactly one cursor save whose value is the chunk's last `updated_at`.

Add `_PullProgress` tests: `counts()` with `skipped_too_old=0, photos_left_out=0` has neither clause; with both non-zero has both.

- [ ] **Step 2: Run and confirm they fail**

```bash
pytest app/actions/tests/test_pull_events.py app/actions/tests/test_handlers.py -k "pull_observations_action or skips_old or PullProgress or counts" -q
```

- [ ] **Step 3: Implement**

`counts()`:

```python
        text = f"it sent {prefix}{_count(self.events_sent, 'new iNaturalist observation', 'new iNaturalist observations')} to Gundi"
        if self.skipped_too_old:
            text += f" and skipped {self.skipped_too_old} uploaded more than {self.max_age_days} days ago"
        if self.photos_handed_off:
            text += f", and handed their {_count(self.photos_handed_off, 'photo', 'photos')} to a separate photo run"
            if self.photos_left_out:
                text += f" ({self.photos_left_out} left out by the photo limit)"
        return text
```

(`_PullProgress.__init__` takes `max_age_days` from the config; `_run_pull` passes it.) `data()` adds `"events_skipped_too_old"` and `"photos_left_out"` and keeps `"events_updated": 0`. `_pull_result` computes `cursor_lag_days` from the cursor it is given.

- [ ] **Step 4: Run the suite**

```bash
pytest -q
```

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_pull_events.py app/actions/tests/test_handlers.py
git commit -m "Pull limits: skipped and left-out counters, cursor lag in the run result"
```

---

## Verification before merge (manual)

1. `pytest -q` green; `git grep -n "patch_events\|save_patched_events_state\|update_event_in_gundi" app/actions` returns nothing.
2. Register the runner against the dev portal (`python -m app.register --slug inaturalist`), open an iNaturalist connection and confirm the three new fields render after "Include photos" with their defaults.
3. Run `pull_events` on a dev connection whose state has an old `last_run`: the first summary shows the cursor floor (reads from `days_to_load` days ago), `events_skipped_too_old` > 0 for old touched sightings, `cursor_lag_days` under 1 after the run.
4. In the destination EarthRanger, open one new event: one photo, no `Updated At` in the details, history has only the creation line.
