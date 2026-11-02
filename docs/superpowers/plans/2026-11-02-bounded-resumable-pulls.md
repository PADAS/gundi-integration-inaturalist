# Bounded, Resumable Pulls Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `pull_events` run reads from iNaturalist without blocking the instance, processes one window at a time, saves the cursor after each window, stops before the time limit or at a per-run cap, and reports where it stopped.

**Architecture:** The window loop in `app/datasource/inaturalist.py` becomes a generator that yields one window at a time and honours a deadline. `app/actions/handlers.py::_pull_events` drives it from a worker thread, runs the existing send/patch/save pipeline per window, and writes the cursor after each one. No new services, state keys stay the same, and `get_observations` keeps its contract.

**Reference implementation:** `gundi-integration-movebank` `app/actions/handlers.py` at `e35e6fc`: `action_pull_events_for_individual` (window loop, per-window `set_state`, `MAXIMUM_RECORDS_PER_INDIVIDUAL`) and `action_backfill_events_for_individual` (`deadline = 0.8 * MAX_ACTION_EXECUTION_TIME`, per-window `scan_from`).

**Tech Stack:** Python 3.10, FastAPI action runner, `pyinaturalist` 0.19 (sync, `requests`), pydantic v1, pytest + pytest-asyncio + pytest-mock.

**Spec:** `docs/superpowers/specs/2026-11-02-bounded-resumable-pulls-design.md`. Read §3–§6 before starting. Ticket GUNDI-5908.

**Out of scope for this plan:** narrowing the IberoMAB route's configuration (operational, not code), the Cloud Run `timeoutSeconds` question (spec §7.1), quiet periods and density-based windows (spec non-goals).

## Global Constraints

- State keys are unchanged: cursor stays in `STATE_LAST_RUN_KEY` via `_build_pull_events_state`; per-observation state stays as GUNDI-5889 left it.
- The cursor is written only after every observation in a window has been sent or patched and its state saved. Never write a cursor past data that was not processed.
- `get_observations(since, *, bounding_box, taxa, projects, quality_grade, annotations)` keeps its signature and return type; it consumes the new generator.
- New constants, all in `app/actions/handlers.py` unless noted: `PULL_TIME_BUDGET_FRACTION = 0.8`, `MAX_OBSERVATIONS_PER_PULL = 2000`. Both read from settings with these defaults so they can be tuned per deployment.
- Deadline checks happen between page requests and between windows, never mid-request; a page in flight finishes.
- The stop entry is WARNING when the cursor moved during the run and ERROR when it did not. Wording follows the existing `_log_pull_stopped` style: plain words, no prediction of what the next run will do.
- No change to `app/services/` (template code), to `docker/`, or to the portal.
- Tests use mocked iNaturalist responses and the existing state-manager fixtures under `app/actions/tests/` and `app/datasource/tests/`; no production services.

## Review Focus

1. **Window re-read after a mid-window stop:** a run stopped after sending half a window must, on the next run, re-read that window and skip the sent half through per-observation state, sending the rest exactly once. Pinned in Task 4.
2. **Deadline inside the thread:** the generator must stop within one page request of the deadline even when iNaturalist is slow; the handler must not wait on a second `next()` after the deadline. Pinned in Tasks 2 and 3.
3. **Cursor monotonicity:** `cursor_reached` never goes backwards across windows, including the "10,000 share one timestamp" branch. Pinned in Task 1.
4. **No-progress run is an ERROR:** a run whose first window cannot finish inside the budget leaves the cursor where it was and logs ERROR, not WARNING. Pinned in Task 3.
5. **Other runs on the instance keep moving:** with the read in a thread, a concurrent small pull on the same loop completes while a slow read is in progress. Pinned in Task 2.

---

## File Structure

| File | Responsibility |
|---|---|
| `app/datasource/inaturalist.py` | `ObservationWindow`, `iter_observation_windows` (window loop with deadline); `get_observations` consumes it |
| `app/actions/handlers.py` | `_pull_events` window loop in a thread, `_process_window`, cursor save per window, budget and cap checks, `_log_pull_paused`, `_PullProgress` fields |
| `app/settings/__init__.py` | `PULL_TIME_BUDGET_FRACTION`, `MAX_OBSERVATIONS_PER_PULL` |
| `README.md` | New activity entries and the two settings |
| `app/datasource/tests/test_inaturalist.py` | Window generator, cursor monotonicity, deadline inside the thread |
| `app/actions/tests/test_handlers.py` | Per-window save order, stop entries, cap, resume after a mid-window stop, concurrency on one loop |

---

### Task 1: Window generator in the datasource

**Files:**
- Modify: `app/datasource/inaturalist.py` (`_get_observations_for_taxa_batch`, `get_observations`)
- Test: `app/datasource/tests/test_inaturalist.py`

**Interfaces:**
```python
class ObservationWindow(NamedTuple):
    observations: Dict[int, Observation]
    cursor_reached: datetime      # last updated_at seen, or the window end when it fit in one query
    more: bool                    # True when another window follows

def iter_observation_windows(
    since: datetime, *, bounding_box=None, taxa=None, projects=None,
    quality_grade=None, annotations=None, deadline: Optional[float] = None,
) -> Iterator[ObservationWindow]
```
`deadline` is a `time.monotonic()` value. When it passes, the generator finishes the current page, yields the partial window with `more=True`, and returns. `get_observations` becomes a thin wrapper that merges every window.

- [ ] **Step 1: Write the failing tests**
  - A backlog of 25,000 mocked observations yields three windows with strictly increasing `cursor_reached`; the merged result equals today's `get_observations` output.
  - The "all share one timestamp" branch still advances by one second and `cursor_reached` does not go backwards.
  - With a deadline already passed before the second page, the generator yields one partial window with `more=True` and stops after that page.
  - The taxa-batch path (several `taxon_id` batches) still yields windows per batch in order.
- [ ] **Step 2: Implement `iter_observation_windows`** by moving the `while True` loop out of `_get_observations_for_taxa_batch` into the generator and adding the deadline check before each page and before each new window.
- [ ] **Step 3: Reimplement `get_observations`** as `merge(iter_observation_windows(...))` and run the existing datasource suite unchanged.

### Task 2: Drive the generator from a worker thread

**Files:**
- Modify: `app/actions/handlers.py` (`_pull_events`)
- Test: `app/actions/tests/test_handlers.py`

- [ ] **Step 1: Write the failing tests**
  - `_pull_events` calls `asyncio.to_thread` for each window (assert on a patched `to_thread`).
  - A slow mocked read (a `threading.Event` the test releases) does not stop a second, small `_pull_events` on the same loop from finishing first.
- [ ] **Step 2: Replace the single `get_observations` call** with a loop: `window = await asyncio.to_thread(next, windows, None)`; `None` ends the loop. Pass `deadline` computed from `progress.started` and `PULL_TIME_BUDGET_FRACTION`.
- [ ] **Step 3: Keep `_PullProgress` accurate:** add `windows_read` and `cursor_reached`; `inat_seconds` accumulates across windows.

### Task 3: Process, save and stop per window

**Files:**
- Modify: `app/actions/handlers.py` (`_pull_events`, new `_log_pull_paused`)
- Test: `app/actions/tests/test_handlers.py`

- [ ] **Step 1: Write the failing tests**
  - After each window, `state_manager.set_state(..., "pull_events", _build_pull_events_state(cursor_reached))` is called once, and only after the window's send/patch/state calls.
  - A run that reaches the deadline after two full windows saves the second window's cursor and publishes a WARNING entry whose title names the elapsed time and the cursor.
  - A run whose first window cannot finish before the deadline leaves the cursor untouched and publishes an ERROR entry.
  - A run that reaches `MAX_OBSERVATIONS_PER_PULL` stops taking windows, saves the cursor and publishes an INFO entry saying more remain.
- [ ] **Step 2: Move the existing pipeline** (split new/changed, `send_events_to_gundi` in chunks, `process_attachments`, `save_events_state`, `patch_events`, `save_patched_events_state`) into `_process_window(...)` and call it per window.
- [ ] **Step 3: Write the cursor after `_process_window`** returns, then check `deadline` and the cap before asking for the next window.
- [ ] **Step 4: Add `_log_pull_paused`** next to `_log_pull_stopped`, choosing WARNING or ERROR from whether the cursor moved, using `_publish_progress_log` with the remaining-time timeout as `_log_pull_finished` does.

### Task 4: Resume correctness

**Files:**
- Test: `app/actions/tests/test_handlers.py`

- [ ] **Step 1: Write the test:** run 1 is cancelled (simulate `asyncio.CancelledError` from `send_events_to_gundi`) after sending the first 100-event chunk of window 2; assert the cursor equals window 1's `cursor_reached`. Run 2 with the same mocked data re-reads window 2, sends only the events without saved state, and ends with the cursor at window 2's `cursor_reached`.
- [ ] **Step 2: Fix anything the test exposes** in the order of saves inside `_process_window`.

### Task 5: Settings, docs and rollout

**Files:**
- Modify: `app/settings/__init__.py` (two settings with defaults), `README.md` (operations section), `local/.env.example` if present
- Test: none beyond existing settings tests

- [ ] **Step 1: Add `PULL_TIME_BUDGET_FRACTION` and `MAX_OBSERVATIONS_PER_PULL`** to settings with the defaults above.
- [ ] **Step 2: Document the new activity entries** (paused with cursor, paused without progress, cap reached) and the two settings in the README.
- [ ] **Step 3: Stage check:** point a stage route at project 296888 with a cursor months back, run `pull_events`, and confirm the run ends before the limit with the cursor advanced; trigger five small routes meanwhile and confirm they finish. Then raise the open question on Cloud Run `timeoutSeconds: 300` vs the 540 s action limit with whoever owns the deployment.

### Task 6: Verification

- [ ] Run `pytest app/datasource/tests app/actions/tests`; all green.
- [ ] Check the acceptance criteria in the spec one by one and tick them in the PR description.
