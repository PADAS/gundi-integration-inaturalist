# Bounded, Resumable Pulls in the iNaturalist Runner — Design

- **Date:** 2026-11-02
- **Ticket:** [GUNDI-5908](https://allenai.atlassian.net/browse/GUNDI-5908) (related: GUNDI-5889)
- **Repo:** `PADAS/gundi-integration-inaturalist` (action runner)
- **Reference:** `PADAS/gundi-integration-movebank` at `e35e6fc` (production release `release-20260915-e35e6fc`)
- **Status:** Draft for review

## 1. Context

`pull_events` reads every observation that changed since the route's saved cursor, sends the new ones to Gundi with their photos, patches the changed ones, and saves the cursor. For most routes that is seconds of work. For a route that follows a very large project it is not: "Ciencia Ciudadana en Iberoamérica" follows project 296888 (38.5M observations across Latin America), its cursor has been at 2026-06-20 with about 3M updated observations behind it, and every run restarts there.

What happens today, from the code and the 2 Nov 2026 production logs:

| | Today | Effect |
|---|---|---|
| Read | One call to `get_observations` (`pyinaturalist` on `requests`, blocking) for the whole backlog | The `uvicorn` process (one per Cloud Run instance, no `--workers`, `containerConcurrency: 8`) stops serving the other runs on that instance. On instance `f360aa`, from 20:25 to 20:41:33 UTC the only log lines were this read; at 20:41:33, 13 runs timed out and 10 queued runs started in the same second. |
| Windows | `_get_observations_for_taxa_batch` already walks the backlog in windows of at most `INAT_MAX_RESULTS_PER_QUERY` (10,000), advancing `updated_since` | Windows are read, but nothing is sent or saved until all of them are |
| Cursor | `STATE_LAST_RUN_KEY` written once at the end of `_pull_events` | A run cut off by the 540 s limit (`settings.MAX_ACTION_EXECUTION_TIME`) saves nothing; the next run starts from the same place |
| Time budget | None inside the read; `_PullProgress` and `_log_pull_stopped` only describe a cancelled run afterwards | Runs are cut off rather than stopping |
| Cap per run | None | One run tries to drain the whole backlog |
| Health | A run cut off inside the read logs its error late (the timeout cannot fire while the loop is blocked) | The route shows healthy for stretches while never finishing a run |

GUNDI-5889 (merged) already saves per-observation state after each 100-event chunk sent or patched, so observations are not re-sent. It does not move the cursor.

## 2. Goals

- A `pull_events` run finishes inside the time limit and leaves the cursor further along than it found it, on any route.
- One route's read never holds up the other routes on the same instance.
- A route that cannot make progress shows as unhealthy within the hour.
- Existing routes keep their cursors and settings; no migration.

### Non-goals

- Replacing `pyinaturalist` with an async client. Running the read in a worker thread gives the same isolation at far lower cost.
- Quiet periods after empty windows, or density-based window sizing (Movebank §3.5 and `_compute_batch_window`). iNaturalist windows are bounded by result count, and an empty window already returns in seconds.
- Changes to the portal, the backend health rule, Cloud Run settings or route configuration. Narrowing the IberoMAB route (fewer places or taxa) is a separate operational step; this design makes a route like it drain over several runs instead of stalling.

## 3. Reference: how the Movebank runner handles the same workload

File references are to `gundi-integration-movebank` `app/actions/handlers.py` at `e35e6fc`. In the last 7 days of production logs the Movebank runner had no 540 s timeouts.

| # | Mechanism | Where |
|---|---|---|
| M1 | **Non-blocking read.** `movebank-client` 1.3.2 is built on `httpx.AsyncClient`; `get_individual_events_by_time` is an async generator | `movebank_client/client.py` |
| M2 | **Small windows, cursor saved after each.** The loop reads one `batch_window_size` (default 5 days) at a time and calls `state_manager.set_state(..., CURSOR_STATE_ACTION_ID, ...)` after every window that returned events | `action_pull_events_for_individual`, lines 321–400 |
| M3 | **Cap per run.** The loop stops once `total_observations_sent >= MAXIMUM_RECORDS_PER_INDIVIDUAL` (2000); a window may overshoot and is processed whole so its cursor stays consistent | lines 40, 321 |
| M4 | **Time budget inside the run.** `deadline = time.monotonic() + 0.8 * settings.MAX_ACTION_EXECUTION_TIME`, checked before each window and each request; `scan_from` persisted after every window; on `asyncio.CancelledError` the persisted position is the recovery, not cleanup code. `MAX_RECORDS_PER_BACKFILL_STEP` (10,000) is a second backstop | `action_backfill_events_for_individual`, lines 853–990 |
| M5 | **Quiet period** after an empty window (`QUIET_PERIOD_SECONDS`, 3600) | line 42 |

## 4. Key decisions

| # | Decision | Why |
|---|---|---|
| D1 | Run every `pyinaturalist` call in `_pull_events` with `asyncio.to_thread`: the observation read and the project check in `_drop_unknown_projects` (`find_existing_projects`), keeping `pyinaturalist` | Same effect as M1 for the other runs on the instance; `action_list_projects` and `action_list_taxa` already do this. A thread cannot be cancelled, so D3 is also needed |
| D2 | Process and save **one window at a time**: the existing 10,000-observation windows become the unit of work, each followed by the cursor save | M2. The window loop already exists; only its output and the save point move |
| D3 | A **time budget** for the whole run, read and processing alike: `PULL_TIME_BUDGET_FRACTION × MAX_ACTION_EXECUTION_TIME` (proposed 0.8) from `_PullProgress.started`, checked between pages, between windows and between 100-event chunks while processing. The 100-event chunk is the commit unit: its per-observation state is saved (GUNDI-5889) before the next chunk starts, so a run can stop between chunks without losing or repeating work | M4. Stopping is a decision the run makes with its progress saved, not something done to it. A window can hold 10,000 observations, so a budget on the read alone could leave too little time to send them |
| D9 | With more than `TAXA_BATCH_SIZE` (100) configured taxa the read runs one query per taxa batch; the saved cursor is the **minimum** point reached across all batches, advanced after each round of one window per batch | A cursor saved from batch A alone would skip batch B's older observations on resume. The minimum re-reads a little for the batches that were ahead; GUNDI-5889's per-observation state drops what was already sent |
| D4 | A **cap per run**, `MAX_OBSERVATIONS_PER_PULL` (proposed 2000), checked between windows | M3. A backlog drains over scheduled runs instead of one |
| D5 | The cursor is written only after every observation in the window has been sent or patched and its state saved | A run stopped mid-window re-reads that window; GUNDI-5889's per-observation state skips what was already sent, so nothing is lost or duplicated |
| D6 | A stop on the budget logs **WARNING** when the cursor moved and **ERROR** when it did not | ERROR feeds the existing 3-errors-per-hour health rule, so a stuck route shows unhealthy; a route that is catching up stays visible without being flagged |
| D7 | `get_observations` keeps its signature; `iter_observation_windows` is the new primitive underneath | Existing callers and tests stay unchanged |
| D8 | Budget fraction and cap are settings with defaults | Tunable per deployment without a release, as Movebank's `MIN_BACKFILL_WINDOW_SECONDS` and `MAX_RECORDS_PER_BACKFILL_WINDOW` are |

## 5. Architecture

```
action_pull_events ──▶ _run_pull ──▶ _pull_events (event loop)
                                        │  deadline = started + 0.8 × MAX_ACTION_EXECUTION_TIME
                                        │
                                        ├─ await to_thread(_drop_unknown_projects' find_existing_projects)
                                        │
                                        ├─ await to_thread(next, windows) ──▶ iter_observation_windows (worker thread)
                                        │        ◀── (observations, cursor_reached, more)       pages of 200, one window per taxa batch per round,
                                        │                                                       checks deadline between pages
                                        ├─ _process_window: split new/changed → per chunk of 100: send (+photos) → save per-observation state → check deadline
                                        │                   → patch per chunk the same way
                                        ├─ set_state(STATE_LAST_RUN_KEY = cursor_reached)      only after the whole window is processed
                                        │
                                        └─ repeat until: no more windows | deadline | MAX_OBSERVATIONS_PER_PULL
                                             then _log_pull_finished | _log_pull_paused (WARNING / ERROR)
```

### 5.1 Code layout

- `app/datasource/inaturalist.py` — `iter_observation_windows(...)` (the window loop from `_get_observations_for_taxa_batch`, as a generator with a `deadline`); `get_observations` consumes it.
- `app/actions/handlers.py` — `_drop_unknown_projects` runs `find_existing_projects` in a thread; `_pull_events` drives the generator from a thread, calls `_process_window` per window with the deadline checked between chunks, saves the cursor, checks budget and cap; `_log_pull_paused` next to `_log_pull_stopped`; `_PullProgress` gains `windows_read` and `cursor_reached`.
- `app/settings/__init__.py` — `PULL_TIME_BUDGET_FRACTION`, `MAX_OBSERVATIONS_PER_PULL`.

No change to `app/services/` (template code) or `docker/`.

## 6. Behaviour

### 6.1 Windows and the cursor

`iter_observation_windows` yields `(observations, cursor_reached, more)` per window. For one taxa batch (the common case: no taxa, or up to 100), `cursor_reached` is the last `updated_at` seen in the window, or the window end when the window fit in one query. With several taxa batches the generator takes one window from each batch in turn (round-robin), keeps a reached point per batch, and reports `cursor_reached` as the minimum across batches, treating a batch that has no more windows as reached at the run's start time (D9). The "10,000 share one timestamp" step stays as it is. `cursor_reached` never goes backwards.

`_pull_events` runs the existing pipeline on each window, then writes the cursor as `cursor_reached` (D5). The per-chunk per-observation saves from GUNDI-5889 are unchanged and are what make the chunk the commit unit.

### 6.2 Stopping on the budget

The deadline is checked at three points: in the generator before each page request and before each new window, and in the handler before each 100-event chunk while sending or patching. When the generator reaches it, it finishes the page in flight, yields what it has with `more=True`, and returns. When the handler reaches it mid-window, it finishes the chunk in flight (whose per-observation state is then saved), does not write the cursor for that window, and stops; the next run re-reads the window and skips the chunks already sent. Photo downloads inside a chunk keep the existing 120 s client timeout, so one chunk is bounded. The run then logs through `_publish_progress_log`:

- cursor moved → WARNING, e.g. "Pull stopped after 7 minutes with the cursor at 2026-06-22 21:17 UTC because the time limit was close. 1,840 new observations sent to Gundi, 12 updated, 95 photos added."
- cursor did not move → ERROR, same shape, "…and the cursor did not move."

Wording follows `_log_pull_stopped`: plain words, no prediction of what the next run will do. `_log_pull_stopped` stays for the case where the runner cancels the task anyway.

### 6.3 Stopping on the cap

Once observations processed in this run reach `MAX_OBSERVATIONS_PER_PULL`, the handler stops taking windows, saves the cursor and logs at INFO that more remain. A window may overshoot the cap and is processed whole (M3).

## 7. Open questions

1. Production has Cloud Run `timeoutSeconds: 300` while the action limit is 540 s; 20 to 60 runs a day end with "maximum request timeout" at the Cloud Run layer. Should the service timeout be raised to match (Movebank has the same 300 s), or the action limit lowered?
2. Is 0.8 the right fraction when one 200-row page plus photo downloads can take 10 to 20 s? A fixed margin (for example 60 s) may be easier to reason about.
3. Should the cap count observations read or events sent? Read bounds iNaturalist traffic and is simpler; sent bounds Gundi traffic.

## 8. Acceptance criteria

1. A `pull_events` run on a route with millions of observations behind its cursor returns before `MAX_ACTION_EXECUTION_TIME`, with the cursor further along than it started and an activity entry saying where it stopped.
2. While such a run is in progress, five small routes triggered on the same instance complete normally (reproduce against the 2 Nov 2026 logs for instance `f360aa`, 20:25 to 20:41:33 UTC).
3. A run that cannot advance the cursor at all logs an ERROR entry each time, so the route becomes unhealthy within the hour.
4. Existing routes keep their saved cursors; no reconfiguration and no state migration.
5. Observations are not re-sent: a re-read window skips what GUNDI-5889's per-observation state already recorded.
6. `get_observations` keeps its signature and the current datasource tests pass unchanged.
