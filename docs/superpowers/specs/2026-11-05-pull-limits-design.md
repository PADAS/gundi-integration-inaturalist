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
| Event updates | 0–1 per run now; over a sighting's life, 836 updates for 520 sightings, one per touch (an identification, an agreement, a comment, a favourite, an edit) | 3–14 per run |

Problems:

- **Old sightings arrive as new events.** The query is by touch time, so a sighting uploaded in 2020 that someone identifies today is not in state and is created, with all its photos. Of 23,456 events created in 72 h, 36% were uploaded more than 90 days before the touch that synced them.
- **The backlog never drains.** 54k touches a day against a cap of 21,600 new events a day: the cursor only falls further behind, and every event reaches EarthRanger months late.
- **Photos are most of the dead letters.** Two or three photos per event, 200 KB–1 MB each, through a dispatcher that delivers one request at a time.
- **Updates are sent for touches that change nothing EarthRanger shows.** The trigger is `updated_at` moving, which iNaturalist does for comments, favourites, agreements and edits too; the payload carries `updated_at`, so EarthRanger's history lists `Updated At` among the changed fields every time, even when nothing else changed.
- **The per-run cap is deployment-wide.** `INAT_MAX_NEW_EVENTS_PER_PULL` is an env var, so one busy site cannot be throttled without touching the others.

## 2. Goals

- A site gets new sightings as they are uploaded, not months later, and not old sightings that were merely touched.
- An event is updated only when something EarthRanger shows has changed, and the history line names only those fields.
- Fewer messages per sighting to Gundi and EarthRanger: one event and at most three photos by default.
- Every limit is a setting on the Gundi connection with a default, so support can tune one site in the portal.
- Each rule is one check that a reader can verify from a sighting's page and its upload date.
- A user can load older sightings on demand, within a limit they choose, without changing the regular pull (§5).

### Non-goals

- **Dispatcher behaviour** (retries of attachments whose event never landed, scaling). Out of scope.
- **Narrowing a site's iNaturalist scope.** Iberomab's ~16k uploads a day stay close to its dispatcher's capacity even with this change; that is a configuration decision for the site (research grade only, a smaller area), not code.
- **Changes to the action-runner template** (`app/services/`).

## 3. Key decisions

| # | Decision | Why |
|---|---|---|
| D1 | **Upload date is the only age.** A sighting's age is `now − created_at` (iNaturalist upload time). `updated_at` decides nothing. | It is the one date a user can read on the sighting's page, and it never moves. |
| D2 | **Update when what we send changed.** The event payload (D7: without `updated_at`) is built for every observation in state that iNaturalist returns; it is sent only when it differs from the payload last sent, which state records as a hash. | "We update when something we send to EarthRanger changed" is one sentence a reader can check against the sighting. A comment, a favourite or an agreement that changes no mapped field sends nothing. The whole payload goes (EarthRanger replaces `event_details` wholesale on PATCH), so the history shows exactly the fields that differ. |
| D3 | **`max_age_days` (default 150, about 5 months), one check for creations and updates.** A sighting uploaded more than N days ago is skipped: not created, not updated, no photos. Nothing is stored for it. | Keeps old sightings that get touched out of EarthRanger, and stops updates to sightings nobody is watching any more. Skipping is free, so a run gets through backlog pages quickly. |
| D4 | **The cursor never starts further back than `days_to_load`.** `_get_load_since` returns `max(stored cursor, now − days_to_load)`. | One line. On Iberomab it moves the cursor from June to last week. Sightings uploaded in the gap are not backfilled (on a site this far behind they are older than the age floor anyway). |
| D5 | **`max_photos` (default 3).** Only the first N of `ob.photos` are handed to the photo run. | Photos are 63% of the dead letters; three covers most sightings (56% have one, 20% two) while capping the long tail, and the first photo is the one iNaturalist shows as the sighting's picture. |
| D6 | **`max_new_events_per_run` on the connection (default = `INAT_MAX_NEW_EVENTS_PER_PULL`, 150).** | A busy site can be throttled alone. The env var stays as the default so existing connections behave as today. Updates do not count against it, as today. |
| D7 | **`updated_at` leaves the event payload.** | It changed on every send, made every payload differ, and showed up in EarthRanger's history as a changed field. `created_at` stays. |
| D8 | **Counters, not a new log.** The existing run summary gains `events_skipped_too_old`, `events_skipped_unchanged`, `photos_left_out` and `cursor_lag_days`; the summary title mentions skipped sightings and left-out photos. | The summary already exists (`_PullProgress`); four fields and two phrases add no logic path. |
| D9 | **`days_to_load` keeps its range (1–7).** | It is now also the cursor floor; a wider range would reopen the backlog. |
| D10 | **State gains `sent_hash`** next to `object_id` and `inat_updated_at`, written at creation and after every patch. Rows without it (synced before this change) are patched once on their next touch, which seeds the hash. | One string per observation; the existing per-observation row, same `source_id`. The one-time patch of old rows is bounded by `max_age_days`. |

## 4. Behaviour

For each observation `pull_events` reads (same query as today: `updated_since` cursor, projects, bounding box, taxa, quality grade, annotations):

| The observation is… | Decision | Counter |
|---|---|---|
| Uploaded more than `max_age_days` ago (in state or not) | Skip; store nothing | `events_skipped_too_old` |
| Not in state | Create the event; hand its first `max_photos` photos to `add_photos`; store `object_id`, `inat_updated_at`, `sent_hash` | `events_extracted`, `photos_handed_off`, `photos_left_out` |
| In state, payload hash ≠ `sent_hash` (or no `sent_hash` yet) | Patch the event with the whole payload; store the new `inat_updated_at` and `sent_hash` | `events_updated` |
| In state, payload hash = `sent_hash` | Skip; store nothing (a pending photo list still resumes, as today) | `events_skipped_unchanged` |

The hash is SHA-256 of the payload serialised with sorted keys and `default=str`, so datetimes and `None` serialise the same way every run. The cursor advances exactly as today: per chunk, to the chunk's last `updated_at`. Skipped observations move the cursor like any other.

What a user sees in EarthRanger: one history line per real change, naming the fields that differ (for example `Quality Grade, Taxon Name, Taxon Common Name, Title`); never a line for a comment or a favourite; never `Updated At`.

### 4.1 Settings (`PullEventsConfig`)

| Setting | Default | Range | New? |
|---|---|---|---|
| `max_age_days` | 150 (about 5 months) | 1–3650 | new |
| `max_photos` | 3 | 0–50 | new; 0 = no photos even when `include_photos` is on |
| `max_new_events_per_run` | `settings.INAT_MAX_NEW_EVENTS_PER_PULL` (150) | 1–1000 | new on the connection |
| `days_to_load` | 3 | 1–7 | exists; now also the cursor floor |
| `include_photos` | true | | exists |
| `quality_grade`, `projects`, `bounding_box`, `taxa`, `annotations`, `event_type`, `event_prefix` | | | exist, unchanged |

Form order: the three new fields go after `include_photos`, before `run_on_schedule`.

### 4.2 Run result and summary

`_pull_result` returns, as today plus the new keys:

```json
{"result": {"events_extracted": 150, "events_updated": 12, "photos_attached": 0,
            "events_skipped_too_old": 980, "events_skipped_unchanged": 398,
            "photos_left_out": 94, "cursor_lag_days": 0.3}}
```

`cursor_lag_days` is `(now − cursor)` in days, one decimal: the last cursor this run saved, or the stored one when it saved none (iNaturalist down, out of time before the first chunk).

Summary title (existing sentence, extended):

> Pull finished in 14 seconds. It sent 150 new iNaturalist observations to Gundi and updated 12, and skipped 980 uploaded more than 150 days ago and 398 with nothing changed, and handed their 150 photos to a separate photo run (94 left out by the photo limit). Reading from iNaturalist took 9 seconds and found 1,540 observations that match the settings. …

The "skipped …" clause lists only the non-zero parts; the "(… left out …)" clause appears only when something was left out.

## 5. Backfill (`backfill_events`)

The regular pull only looks `days_to_load` back and only creates sightings uploaded within `max_age_days`. Loading older sightings is a separate action that a user starts by hand.

### 5.1 Decisions

| # | Decision | Why |
|---|---|---|
| B1 | **Manual only.** `backfill_events` is a generic, executable action (`GenericActionConfiguration` + `ExecutableActionMixin`). Only pull actions are scheduled, so it never runs on its own. | A backfill is a one-off decision by a person. |
| B2 | **Creations only, from the sighting's current data.** Each sighting in the window that is not in state is created with what iNaturalist shows now. Sightings already in state are skipped. No updates, no photos. | Simplest useful backfill; photos are most of the load (§1). |
| B3 | **Window by upload date, oldest first.** `days_back` (default 30, 1–3650) sets the window: sightings uploaded from `now − days_back` to the moment the backfill started. They are read in upload order (iNaturalist id ascending, `created_d1`/`created_d2`). | The same reference date as everything else (D1). Upload order never changes, so the walk is exact and resumable by id. |
| B4 | **`max_events` (default 1000, 1–100,000) stops the backfill.** When that many events have been created, the backfill ends, even if the window is not finished. The final log says how far it got. | The user decides the cost up front. A 3-year window that holds 800 sightings finishes; one that holds 50,000 stops at 1,000 and says up to which upload date it reached. |
| B5 | **Same filters as the pull.** Projects, bounding box, taxa, quality grade, annotations, event type and prefix come from the connection's `pull_events` settings. `max_age_days`, `max_photos` and `max_new_events_per_run` do not apply. | One place to describe what a connection is about. The backfill's own two settings are its limits. |
| B6 | **Same run lock as the pull, and pulls go first.** A backfill run holds the connection's run lock, so a pull and a backfill never create the same sighting twice. A pull that finds the lock taken skips, as today, and leaves a short-lived `pull_waiting` note. Once a backfill run has read at least one page, it checks the note before each further page; if set, it clears the note, releases the lock, then starts a new backfill run (which waits 10 s before taking the lock) and `pull_events`, so the pull goes first and the backfill never stalls. A pull that has run clears the note, since it covers any pull that was skipped meanwhile. Follow-up runs are always started after the lock is released. A backfill run that finds the lock taken waits for it (every 10 s) within its time budget, then hands over to a new run; after 3 runs in a row without the lock it stops with an error. | Reuses the existing guarantee, and a long backfill never holds back new sightings for more than one pull interval. |
| B7 | **Asynchronous chain of runs.** Each run stops before the pull's time budget (well under Cloud Run's request timeout), checking the time before each page and each chunk, and starts `backfill_events_continue` (internal, through the actions topic) with where it is: window, taxa batch, last id, events created so far. The same position is saved after each chunk under `backfill.<window end>`, and a run takes whichever is further along. Each event row the backfill creates is tagged with that key and its taxa group (a pull that later updates the row keeps the tags), so a run that stopped between saving events and saving its count is corrected by the next one: tagged rows in the same taxa group past the saved position are counted, only up to where the next run resumes, and the chain never goes past `max_events`. When the backfill finishes or reaches its limit, the key keeps a `done` marker, so a late or repeated message for it does nothing. After 3 runs in a row that end where they started (nothing read in time), it stops with an error. | Long windows need many runs, and GCP times out long requests. |
| B8 | **Running it again is safe.** Sightings already in state are skipped, so re-running the same window only re-reads. | The answer to "it stopped, now what": run it again, with a later `days_back` or a higher `max_events`. |
| B9 | **State as if the pull created it.** A backfilled sighting gets the same state row (`object_id`, `inat_updated_at`, `sent_hash`) and no pending photos. | The regular pull then treats it like any other: an update only when its payload changes and only within `max_age_days`; never photos. |

| B10 | **One backfill per connection at a time.** Starts choose their job one at a time (a 30 s start lock); a start that can't get it in 30 s doesn't start and asks to run it again. The start records its settings, its pull-filter fingerprint and its window (`backfill_current`). A start with the same `days_back`, `max_events` and filters while that backfill hasn't ended (a redelivered message, or Run pressed twice) continues it with the same window and limit; one within 10 minutes after it ended is a repeat and does nothing. Any other start opens a new window and closes the previous backfill (marked `superseded`), whose runs then stop. | A repeated start never creates a second `max_events` worth of events, and two backfills never run side by side. |
| B11 | **One chain per backfill.** Each run carries a sequence number. A run raises the saved number only when it hands over to the next run, so a copy of an earlier message (Pub/Sub delivers at least once) finds a higher saved number and does nothing, without logging, while a copy redelivered after a run was cut off before handing over picks the chain up from the saved position. A start with the same settings takes the saved number and position, so it also revives a chain that stopped. A backfill replaced by a different start (its window is no longer in `backfill_current`) stops before its next chunk and never hands over; this check is separate from the position record, so checkpoint writes can't undo it. The run lock's value names the backfill holding it: a copy of a backfill that finds its own backfill holding the lock exits at once, without waiting or logging. Copies that all waited on another holder (a pull) claim that wait once (`backfill_wait.<key>.<seq>.<n>`), so only one of them hands over or reports a stop. The saved position also keeps the last upload date sent, so a retry that stops at once still reports it. | Duplicate messages cost one quiet read, never a second chain, and no chain is stranded. |
| B12 | **Pull filters are fixed for a backfill.** If projects, area, taxa, grades or annotations change while it runs, its saved position no longer points at the same sightings, so it stops with an error and asks to run it again. | Never reports "finished" after skipping part of the window. |
| B13 | **No duplicate events after a cut-off run.** Before new events are sent (pull and backfill), each observation's row gets a "being sent" note, replaced by its state once Gundi accepts it and removed when the send fails or Gundi doesn't take it. A note left by a run cut off between the send and the save, or by a send that timed out waiting for Gundi's answer, means the event may already be in Gundi, so that sighting is skipped from then on (the pull counts it as "may already have sent"). If writing the notes fails before anything is sent, the notes already written are removed. | Rare cut-offs can leave a sighting out; they never create a second event for it. |
| B14 | **The pull stops at a partial Gundi answer.** If Gundi accepts only part of a chunk of new events, the pull doesn't move its cursor past that chunk and logs a warning; the next pull sends the rest. | The cursor never skips sightings that were never created. |
| B15 | **Backfilled sightings get no photos, recent ones included.** The action's description says so in the portal. | The backfill sends creations only (B2); a user backfilling recent days should know. |

### 5.2 Settings (`BackfillEventsConfig`)

| Setting | Default | Range |
|---|---|---|
| `days_back` | 30 | 1–3650 |
| `max_events` | 1000 | 1–100,000 |

### 5.3 Logs and result

Each run returns `{"result": {"events_extracted": <created this run>, "events_updated": 0, "photos_attached": 0, "backfill_events_created": <total so far>, "backfill_finished": <bool>, "backfill_uploaded_up_to": <ISO time or null>}}`.

The last run logs one summary:

> Backfill finished: created 640 events for sightings uploaded between 2023-11-05 and 2026-11-05.

> Backfill stopped at its limit of 1,000 events: sightings uploaded up to 2024-03-18 14:02 UTC were sent. Run it again to continue; sightings already sent are skipped.

With more than one taxa group (more than 100 saved taxa, each group read from the start of the window), the position adds "(taxa group 1 of 2)".

A run that hands over to the next one logs at info level how many it created so far and up to which upload time. A run that can't get the lock in time logs at info level that it tries again in a new run. A backfill that stops early (the lock taken for 3 runs, no progress for 3 runs, iNaturalist down, no or invalid pull settings, the pull filters changed, its next run couldn't be started) logs an error saying why and, where it applies, that running it again is safe; when none of the saved projects exist on iNaturalist it logs a warning naming them. A run that finds its backfill already ended, replaced, or taken over by a later run of the same chain does nothing and logs nothing. Notes about a next run are published only after that run was started. Log data holds counts and dates only.

### 5.4 How a user runs it

1. On the connection, open the "Backfill iNaturalist Observations" action, set `days_back` and `max_events`, save (the action needs saved settings before it can run).
2. Run it. Read the activity log for the summary.

## 6. Expected effect

| | Today | After |
|---|---|---|
| Iberomab creations | 900/h of mostly old sightings; cursor in June | Cursor jumps to last week; only uploads from the last 150 days; ~14,500 new uploads a day are in scope (its dispatcher delivers ~45,000 messages a day at best, so the site's scope still needs a look) |
| Updates | one per touch; `Updated At` in every history line | only when a mapped field changed, within 150 days of upload; the 72 h diff of creation-vs-update payloads shows ~12% of updates changed nothing but `updated_at`, and no update at all goes out for sightings older than 90 days (36% of what was being touched) |
| Photos per event | 1.6 (Huatulco 2.7) | at most 3 (Iberomab about 1.4: 56% of sightings have 1, 20% have 2) |
| Huatulco | cursor in July | current within a day; 30 uploads a day |

## 7. Changes from today

| Today | After |
|---|---|
| Any touched observation not in state is created | Only if uploaded within `max_age_days` |
| Any touched observation in state is patched | Only if the payload differs from the last one sent, and only within `max_age_days` |
| All photos handed to `add_photos` | First `max_photos` |
| `event_details.updated_at` sent | Not sent |
| Cursor starts at `last_run`, however old | At most `days_to_load` back |
| Cap from the env var only | Per connection, env var as default |
| State row: `object_id`, `inat_updated_at` | plus `sent_hash` |
| Summary: "sent N … and updated M" | plus skipped counts and photos left out |
| No way to load older sightings | `backfill_events`, manual, creations only, bounded by `max_events` |

## 8. Testing

- **Unit** (`app/actions/tests/test_handlers.py`):
  - `_get_load_since`: stored cursor older than the floor → floor; newer → cursor; no cursor → floor.
  - `_sort_observation`: old upload (in state or not) → too old, nothing stored; not in state and recent → new; in state with a different hash → update; in state with the same hash → unchanged; in state without `sent_hash` → update; pending photos still resumed.
  - `_payload_hash`: stable across runs for equal payloads; differs when any mapped field differs; ignores key order.
  - `_transform_inat_to_gundi_event`: no `updated_at` key; `created_at` kept.
  - `_send_new_observations` / `save_events_state`: `sent_hash` stored at creation; `max_photos` 1 hands one photo and counts the rest; 0 hands none and starts no photo run.
  - `save_patched_events_state`: stores the new `sent_hash` and `inat_updated_at`, keeps `object_id`.
  - `_PullProgress.counts()` and `data()` with the new counters.
- **Action** (`app/actions/tests/test_pull_events.py`): the end-to-end pull asserts the new result keys and summary wording; the chunked-updates test keeps its purpose (a run that stops partway through updates keeps the finished chunks and the cursor) with observations whose hash differs.
- **Config** (`test_configurations.py`): defaults and ranges of the three new fields; `max_new_events_per_run` defaults to the env value; backfill defaults and ranges.
- **Backfill** (`test_backfill.py`): the query is by upload window, id ascending, with the pull's filters; sightings in state are skipped; no updates, no photos, state written with `sent_hash`; stops at `max_events` and says up to which upload time; finishes at the end of the window; hands over to `backfill_events_continue` at the time budget with its progress, and the continuation resumes from it; waits for and respects the run lock; iNaturalist down stops with an error; missing pull settings stop with an error; registered as a non-periodic action.

## 9. Rollout

1. Deploy; existing connections get the defaults (150 days, 3 photos, 150 per run).
2. On Iberomab and Huatulco, read the first summaries: `cursor_lag_days` should drop under 1 within a few runs; `events_skipped_too_old` large at first, then small; `events_updated` at first includes the one-time hash seeding of rows synced before this change.
3. Watch the ER dispatchers' dead-letter counts for a day. Photo lists recorded before the deploy, and photo runs already queued, still send all their photos, so the photo count falls once that backlog has cleared.
4. Raise with the Iberomab site whether research-only or a smaller area is wanted; `max_new_events_per_run` is the throttle meanwhile.

## 10. Risks and open items

- **Sightings uploaded during a site's gap are not backfilled** (D4). For Iberomab that is June–October; they are all older than the default `max_age_days` anyway. A site that wants them can set `max_age_days` higher and `days_to_load` to 7 and wait for touches.
- **One-time patches for old rows** (D10): every sighting synced before this change is patched once on its next touch, within `max_age_days`. On a site that is current this is a trickle; on Iberomab the age floor removes most of it.
- **A mapped field that flaps** (for example `place_guess` edited back and forth) sends an update each time. That is the rule working as stated; no special case.
- **The iNaturalist user's feedback** (Slack, #gundi, 2026-11-04) asked to compare the taxon and the quality grade. Both are mapped fields, so every change to either is sent; so is any other mapped change (location, captive, obscured, place, user name).
