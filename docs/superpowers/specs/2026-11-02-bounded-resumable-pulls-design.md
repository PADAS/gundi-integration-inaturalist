# Bounded, resumable pulls in the iNaturalist runner

Status: proposed. Ticket: [GUNDI-5908](https://allenai.atlassian.net/browse/GUNDI-5908).

## Intent

A `pull_events` run should always finish inside the runner's time limit, leave the saved cursor further along than it found it, and never hold up the other routes that share its Cloud Run server. The Movebank runner (`gundi-integration-movebank`, release `e35e6fc`) already behaves this way for the same kind of workload, so this design copies its mechanisms rather than inventing new ones.

## Current behavior

- `app/actions/handlers.py::_pull_events` reads every observation behind the cursor in one call to `app/datasource/inaturalist.py::get_observations`. That function uses `pyinaturalist` on top of `requests`, so the call blocks the event loop of the `uvicorn` process for as long as the read takes.
- The runner is one `uvicorn` process per Cloud Run instance (`docker/Dockerfile`, no `--workers`) with `containerConcurrency: 8` in production. While one read blocks, the other runs on that instance cannot progress, and their `asyncio.wait_for` timeouts cannot fire until the read pauses. On 2 Nov 2026, instance `f360aa` logged only the "Ciencia Ciudadana en Iberoamérica" read from 20:25 to 20:41:33 UTC; at 20:41:33, 13 runs timed out and 10 queued runs started in the same second.
- `_get_observations_for_taxa_batch` already walks a large backlog in windows of at most `INAT_MAX_RESULTS_PER_QUERY` (10,000) observations, advancing `updated_since` to the last `updated_at` seen. It returns one combined map at the end, so nothing is sent or saved until every window is read.
- The cursor (`STATE_LAST_RUN_KEY`) is saved once, at the end of `_pull_events`, as `max(updated_at)` of everything read. A run cut off by the 540 s limit (`settings.MAX_ACTION_EXECUTION_TIME`) saves nothing and the next run starts from the same cursor. The IberoMAB route has had its cursor at 2026-06-20 with about 3M updated observations behind it, and every run restarts there.
- There is no cap on observations per run and no time budget inside the read. `_PullProgress` tracks elapsed time and `_log_pull_stopped` reports where a cancelled run was, but only after the fact.
- GUNDI-5889 (merged) already saves per-observation state after each 100-event chunk sent or patched, so re-sending is avoided. It does not move the cursor.

## What Movebank does

File references are to `gundi-integration-movebank` `app/actions/handlers.py` at `e35e6fc`.

1. **Non-blocking reads.** `movebank-client` 1.3.2 is built on `httpx.AsyncClient`; `get_individual_events_by_time` is an async generator. A slow Movebank response never stops other runs on the instance.
2. **Small windows, cursor saved after each.** `action_pull_events_for_individual` loops over windows of `batch_window_size` (density-based via `_compute_batch_window`, default 5 days) and calls `state_manager.set_state(..., CURSOR_STATE_ACTION_ID, ...)` after every window that returned events. A cancelled run loses at most the window in flight.
3. **Per-run cap.** The loop stops when `total_observations_sent >= MAXIMUM_RECORDS_PER_INDIVIDUAL` (2000). A backlog drains over several scheduled runs instead of one.
4. **Time budget inside the run.** `action_backfill_events_for_individual` sets `deadline = time.monotonic() + 0.8 * settings.MAX_ACTION_EXECUTION_TIME`, checks it before each window and each sensor request, persists `scan_from` after every window, and on `asyncio.CancelledError` relies on that persisted position rather than on cleanup code. `MAX_RECORDS_PER_BACKFILL_STEP` (10,000) is a second backstop.
5. **Quiet period.** After an empty window the individual is skipped for `QUIET_PERIOD_SECONDS` (3600) so capacity goes to individuals with data.

## Proposed design

Numbers below are proposals to confirm in review.

### 1. Read off the event loop

Run the iNaturalist read with `asyncio.to_thread`, as `action_list_projects` already does for `get_projects_by_values` and `action_list_taxa` for `get_taxa_by_ids`. A thread cannot be cancelled by `wait_for`, so the read must also be told when to stop (section 3); `to_thread` alone only frees the other runs on the instance.

### 2. Process and save one window at a time

Turn the window loop in `_get_observations_for_taxa_batch` into a sync generator, `iter_observation_windows(...)`, that yields `(observations, cursor_reached)` per window, where `cursor_reached` is the last `updated_at` of that window (or the window end when the window fit in one query). `get_observations` keeps its current signature for existing callers and tests by consuming the generator.

`_pull_events` pulls one window at a time (`await asyncio.to_thread(next, windows, None)`), and for each window runs the existing pipeline: split into new and changed, send new events in `GUNDI_SUBMISSION_CHUNK_SIZE` chunks with photos, patch changed ones, save per-observation state, then save the cursor as `cursor_reached`. The per-chunk state saves from GUNDI-5889 stay as they are.

Cursor rule: the cursor is written only after every observation in the window has been sent or patched and its state saved, so a run stopped mid-window re-reads that window and the per-observation state skips what was already sent. The "same timestamp beyond 10,000" step in `_get_observations_for_taxa_batch` is kept.

### 3. Time budget

`_pull_events` computes `deadline = started + PULL_TIME_BUDGET_FRACTION * settings.MAX_ACTION_EXECUTION_TIME` (proposed fraction 0.8, as Movebank) from `_PullProgress.started`, and passes it into `iter_observation_windows`. The generator checks the deadline before each page request and before each new window; when it is reached it finishes the current page, yields what it has for that window with `cursor_reached` at the last `updated_at` seen, and stops. The handler also checks the deadline before starting a new window.

When a run stops on the budget:

- it saves the cursor reached (section 2), then
- logs an activity entry through `_publish_progress_log`: **WARNING** when the cursor moved ("Pull stopped after 7 minutes with the cursor at 2026-06-22 21:17 UTC because the time limit was close"), **ERROR** when it did not move at all. An ERROR marks a stuck route unhealthy through the existing 3-errors-per-hour rule; a WARNING keeps a route that is catching up visible without flagging it.

`_log_pull_stopped` stays for the case where the runner cancels the task anyway.

### 4. Cap per run

Add `MAX_OBSERVATIONS_PER_PULL` (proposed 2000, as `MAXIMUM_RECORDS_PER_INDIVIDUAL`). The handler stops taking windows once the observations processed in this run reach it, saving the cursor and logging at INFO that more remain. A window may overshoot the cap, as in Movebank; everything read in a window is processed so its cursor is consistent.

### 5. Out of scope

- Quiet period after an empty window: iNaturalist routes run every 10 minutes and an empty window already returns in seconds; revisit if capacity becomes the issue.
- Density-based window sizing: iNaturalist windows are bounded by result count, not time, so the 10,000 limit already plays that role.
- Replacing `pyinaturalist` with an async client. `to_thread` gives the same isolation at far lower cost.
- Changes to the portal, backend, health rule or route configuration. Narrowing the IberoMAB route (fewer places or taxa) is a separate operational step and still worth doing; this design makes a route like it drain over several runs instead of stalling.

## Open questions

1. The production service has Cloud Run `timeoutSeconds: 300` while the runner's own limit is 540 s; 20 to 60 runs a day end with "maximum request timeout" at the Cloud Run layer. Should the service timeout be raised to match (Movebank has the same 300 s setting), or the action limit lowered?
2. Is 0.8 the right budget fraction given that a single 200-row page plus photo downloads can take 10 to 20 s? A fixed margin (for example 60 s) may be easier to reason about.
3. Should the per-run cap count observations read or events sent? Read is simpler and bounds iNaturalist traffic; sent bounds Gundi traffic.

## Acceptance criteria

1. A `pull_events` run on a route with millions of observations behind its cursor returns before `MAX_ACTION_EXECUTION_TIME`, with the cursor further along than it started and an activity log entry saying where it stopped.
2. While such a run is in progress, five small routes triggered on the same instance complete normally (reproduce with the 2 Nov 2026 logs for instance `f360aa`).
3. A run that cannot advance the cursor at all logs an ERROR activity entry each time, so the route becomes unhealthy within the hour.
4. Existing routes keep their saved cursors; no reconfiguration and no state migration.
5. Observations are not re-sent: the per-observation state from GUNDI-5889 still skips anything already sent in a window that is re-read.
6. `get_observations` keeps its signature and current tests pass unchanged.
