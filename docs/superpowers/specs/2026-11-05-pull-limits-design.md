# iNaturalist pull limits — Design

- **Date:** 2026-11-05
- **Ticket:** to be filed (GUNDI, Connector work)
- **Repo:** `PADAS/gundi-integration-inaturalist`
- **Status:** Draft for review

## 1. Context

`pull_events` reads every observation whose iNaturalist `updated_at` moved since the last run, creates an event for each one that is not in state yet, patches the event of each one that is, and hands every photo of a new observation to `add_photos`.

Measured on production on 2026-11-04/05, two routes:

| | Iberomab (route `29f94656…` → iberomab-jovenes) | Huatulco (route `570ee7bf…` → complejohuatulco) |
|---|---|---|
| Schedule, cap | every 10 min, 150 new events per run, always hit | hourly, 150 per run |
| Cursor (`last_run`) | 2026-06-22, 4.5 months behind | 2026-07-21 |
| Scope on iNaturalist | project 296888 + a bounding box over Latin America: 24.2 M observations, ~16k uploads/day, ~54k touches/day | 103k observations, 30 uploads/day, 95 touches/day |
| Photos per new event | 1.6 (56% have one; up to 11) | 2.7 (up to 17) |
| ER dispatcher, 20 h | delivered 12,013 events + 12,795 photos; dead-lettered 6,270 events (34%) + 22,088 photos (63%) | load is GFW on the same dispatcher and ER's 429 "too many concurrent event creates" |
| Event updates | 0–1 per run; over a sighting's life, 836 updates for 520 sightings, one per touch (an identification, an agreement, a comment, a favourite, an edit) | 3–14 per run |

Problems:

- **Old sightings arrive as new events.** The query is by touch time, so a sighting uploaded in 2020 that someone identifies today is not in state and is created, with all its photos. Of 23,456 events created in 72 h, 36% were uploaded more than 90 days before the touch that synced them.
- **The backlog never drains.** 54k touches a day against a cap of 21,600 new events a day: the cursor only falls further behind, and every event reaches EarthRanger months late.
- **Photos are most of the dead letters.** Two or three photos per event, 200 KB–1 MB each, through a dispatcher that delivers one request at a time.
- **Updates carry no decision.** Any touch re-sends the whole event; EarthRanger's history shows `Updated At` among the changed fields every time.
- **The per-run cap is deployment-wide.** `INAT_MAX_NEW_EVENTS_PER_PULL` is an env var, so one busy site cannot be throttled without touching the others.

## 2. Goals

- A site gets new sightings as they are uploaded, not months later, and not old sightings that were merely touched.
- Fewer messages per sighting to Gundi and EarthRanger: one event and one photo by default.
- Every limit is a setting on the Gundi connection with a default, so support can tune one site in the portal.
- Each rule is one check that a reader can verify from a sighting and its upload date.

### Non-goals

- **Event updates.** This spec removes them; §4 D2. A later spec may add a deliberate update rule (for example, once when a sighting reaches research grade).
- **Dispatcher behaviour** (retries of attachments whose event never landed, scaling). Out of scope.
- **Narrowing a site's iNaturalist scope.** Iberomab's ~16k uploads a day stay close to its dispatcher's capacity even with this change; that is a configuration decision for the site (research grade only, a smaller area), not code.
- **Changes to the action-runner template** (`app/services/`).

## 3. Key decisions

| # | Decision | Why |
|---|---|---|
| D1 | **Upload date is the only age.** A sighting's age is `now − created_at` (iNaturalist upload time). `updated_at` decides nothing. | It is the one date a user can read on the sighting's page, and it never moves. |
| D2 | **No event updates.** An observation already in state is left alone, whatever changed on iNaturalist. The patch path (`patch_events`, `save_patched_events_state`, `patch_targets`) is removed. | Every touch was re-sending the whole event; the only wanted update (grade promotion) needs its own rule, and a rule that sends nothing is the easiest to reason about. The state still records `inat_updated_at` so a later rule can start from it. |
| D3 | **`max_age_days` (default 90), one check.** A sighting uploaded more than N days ago is skipped: not created, no photos. Nothing is stored for it. | Keeps old sightings that get touched out of EarthRanger. Skipping is free, so a run gets through the backlog pages quickly. |
| D4 | **The cursor never starts further back than `days_to_load`.** `_get_load_since` returns `max(stored cursor, now − days_to_load)`. | One line. On Iberomab it moves the cursor from June to last week. Sightings uploaded in the gap are not backfilled (they would be older than the floor anyway on a site this far behind). |
| D5 | **`max_photos` (default 1).** Only the first N of `ob.photos` are handed to the photo run. | Photos are 63% of the dead letters; the first photo is the one iNaturalist shows as the sighting's picture. |
| D6 | **`max_new_events_per_run` on the connection (default = `INAT_MAX_NEW_EVENTS_PER_PULL`, 150).** | A busy site can be throttled alone. The env var stays as the default so existing connections behave as today. |
| D7 | **`updated_at` leaves the event payload.** | It changed on every send and showed up in EarthRanger's history as a changed field. `created_at` stays. |
| D8 | **Counters, not a new log.** The existing run summary gains `events_skipped_too_old`, `photos_left_out` and `cursor_lag_days`; the summary title mentions skipped sightings and left-out photos. | The summary already exists (`_PullProgress`); three fields and two phrases add no logic path. |
| D9 | **`days_to_load` keeps its range (1–7).** | It is now also the cursor floor; a wider range would reopen the backlog. |

## 4. Behaviour

For each observation `pull_events` reads (same query as today: `updated_since` cursor, projects, bounding box, taxa, quality grade, annotations):

| The observation is… | Decision | Counter |
|---|---|---|
| Not in state, `created_at ≥ now − max_age_days` | Create the event; hand its first `max_photos` photos to `add_photos` | `events_extracted`, `photos_handed_off`, `photos_left_out` |
| Not in state, uploaded earlier | Skip; store nothing | `events_skipped_too_old` |
| In state | Skip (a pending photo list still resumes, as today) | — |

The cursor advances exactly as today: per chunk, to the chunk's last `updated_at`. Skipped observations move the cursor like any other, so the same old sighting is not re-read next run unless it is touched again.

### 4.1 Settings (`PullEventsConfig`)

| Setting | Default | Range | New? |
|---|---|---|---|
| `max_age_days` | 90 | 1–3650 | new |
| `max_photos` | 1 | 0–50 | new; 0 = no photos even when `include_photos` is on |
| `max_new_events_per_run` | `settings.INAT_MAX_NEW_EVENTS_PER_PULL` (150) | 1–1000 | new on the connection |
| `days_to_load` | 3 | 1–7 | exists; now also the cursor floor |
| `include_photos` | true | | exists |
| `quality_grade`, `projects`, `bounding_box`, `taxa`, `annotations`, `event_type`, `event_prefix` | | | exist, unchanged |

Form order: the three new fields go after `include_photos`, before `run_on_schedule`.

### 4.2 Run result and summary

`_pull_result` returns, as today plus the new keys:

```json
{"result": {"events_extracted": 150, "events_updated": 0, "photos_attached": 0,
            "events_skipped_too_old": 980, "photos_left_out": 94, "cursor_lag_days": 0.3}}
```

`events_updated` stays, always 0, so Gundi monitoring keeps its field. `cursor_lag_days` is `(now − saved cursor)` in days, one decimal, taken after the last cursor save.

Summary title (existing sentence, extended):

> Pull finished in 14 seconds. It sent 150 new iNaturalist observations to Gundi and skipped 980 uploaded more than 90 days ago, and handed their 150 photos to a separate photo run (94 left out by the photo limit). Reading from iNaturalist took 9 seconds and found 1,400 observations that match the settings. It stopped after 150 new observations, the most one pull sends. …

"and updated 0" is dropped from the sentence.

## 5. Expected effect

| | Today | After |
|---|---|---|
| Iberomab creations | 900/h of mostly old sightings; cursor in June | Cursor jumps to last week; only uploads from the last 90 days; ~14,500 new uploads a day are in scope (its dispatcher delivers ~45,000 messages a day at best, so the site's scope still needs a look) |
| Iberomab updates | one per touch | none |
| Photos per event | 1.6 (Huatulco 2.7) | 1 |
| Huatulco | cursor in July | current within a day; 30 uploads a day |

## 6. Changes from today

| Today | After |
|---|---|
| Any touched observation not in state is created | Only if uploaded within `max_age_days` |
| Any touched observation in state is patched with the whole event | Never patched |
| All photos handed to `add_photos` | First `max_photos` |
| `event_details.updated_at` sent | Not sent |
| Cursor starts at `last_run`, however old | At most `days_to_load` back |
| Cap from the env var only | Per connection, env var as default |
| Summary: "sent N … and updated M" | "sent N … and skipped K uploaded more than 90 days ago", photos left out |

## 7. Testing

- **Unit** (`app/actions/tests/test_handlers.py`):
  - `_get_load_since`: stored cursor older than the floor → floor; newer → cursor; no cursor → floor.
  - `_sort_observation`: not in state and recent → new; not in state and old → skipped, nothing stored; in state → neither, pending photos still resumed.
  - `_transform_inat_to_gundi_event`: no `updated_at` key; `created_at` kept.
  - `_send_new_observations`: `max_photos` 1 hands one photo, counts the rest as left out; 0 hands none and starts no photo run.
  - `_PullProgress.counts()` and `data()` with the new counters.
- **Action** (`app/actions/tests/test_pull_events.py`): the end-to-end pull asserts the new result keys and summary wording; the chunked-updates test is replaced by one asserting that an observation in state is never patched.
- **Config** (`test_configurations.py`): defaults and ranges of the three new fields; `max_new_events_per_run` defaults to the env value.

## 8. Rollout

1. Deploy; existing connections get the defaults (90 days, 1 photo, 150 per run).
2. On Iberomab and Huatulco, read the first summaries: `cursor_lag_days` should drop under 1 within a few runs; `events_skipped_too_old` large at first, then small.
3. Watch the ER dispatchers' dead-letter counts for a day.
4. Raise with the Iberomab site whether research-only or a smaller area is wanted; `max_new_events_per_run` is the throttle meanwhile.

## 9. Risks and open items

- **Sightings uploaded during a site's gap are not backfilled** (D4). For Iberomab that is June–October; they are all older than the default `max_age_days` anyway. A site that wants them can set `max_age_days` higher and `days_to_load` to 7 and wait for touches.
- **No updates at all** departs from the iNaturalist user's feedback that the quality grade matters (Slack, #gundi, 2026-11-04). A later spec can add one update when a sighting reaches research grade; the state keeps `inat_updated_at` and `object_id` for it.
- **Existing tests of the patch path** are deleted with the code; the chunk-and-cursor behaviour they also covered keeps its own tests on the creation path.
