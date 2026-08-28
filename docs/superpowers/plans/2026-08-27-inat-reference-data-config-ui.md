# iNaturalist Reference-Data Config UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the Gundi portal live, selectable values for the `pull_events` config — quality grade as a static enum, annotation terms/values and nearby projects via reference actions — following the approved reference-data design already prototyped in gundi-integration-cmore and gundi-integration-earthranger.

**Architecture:** Port the Phase-0 reference-action contract (marker config base class + `ReferenceDataResponse` envelope + gated self-registration + stateless execution in `action_runner`) from the cmore/ER repos, then add three iNat-specific reference actions (`list_projects`, `list_annotation_terms`, `list_annotation_values`) that wrap public, unauthenticated iNaturalist API endpoints via `pyinaturalist`. Annotate `PullEventsConfig.ui_schema()` with `gundi:reference` keys (inert to portals without reference support). Reshape the `annotations` config field from a JSON-encoded string into structured rows so dropdowns can attach, with full backward compatibility for stored legacy configs.

**Tech Stack:** Python 3.10, pydantic v1, FastAPI, pyinaturalist 0.19.0 (pinned; no new dependencies), pytest + pytest-asyncio + pytest-mock.

**Spec:** `/Users/chrisdo/padas/gundi-integration-cmore/docs/superpowers/specs/2026-07-31-reference-data-config-ui-design.md` (the approved cross-repo design). Reference implementations to mirror: `gundi-integration-cmore` (contract, registration gating, action_runner patch, drift test) and `gundi-integration-earthranger` (`_reference` helper, `ui_schema()` override, reference handlers).

## Global Constraints

- Repo: `/Users/chrisdo/padas/gundi-integration-inaturalist`. Run all tests with `.venv/bin/python -m pytest` from the repo root (the repo venv has the pinned deps).
- **No new dependencies.** `requirements*.in` / `requirements.txt` must not change.
- `REGISTER_REFERENCE_ACTIONS` defaults to **False** (env-gated) — reference actions must NOT be sent to Gundi registration until the platform accepts `"type": "reference"`.
- `gundi:reference` annotations must **never set `ui:widget`** (forward compatibility: old portals must keep rendering plain text fields).
- `taxa` stays a comma-separated string — out of scope (needs a typeahead extension to the RFC; see Design Notes).
- Backward compatibility is mandatory: stored configs with `annotations` as a JSON string or dict, empty strings, and quality-grade case/space variants (e.g. `"Needs ID"`) must keep validating.
- The pull action's result shape `{"result": {"events_extracted", "events_updated", "photos_attached"}}` and all existing state-key conventions are untouched.
- Reference actions use only **unauthenticated** iNat endpoints (`/v1/controlled_terms`, `/v1/projects`) — this repo has no auth plumbing (`AuthenticateConfig` has no handler and `api_key` is unused).
- New per-file code follows the file's existing comment density and idiom.
- Commit after each task with the message given in the task.

## Design Notes / RFC follow-ups (do not implement, record only)

1. **Taxa dropdowns need a typeahead contract extension.** iNat has `/v1/taxa/autocomplete?q=` but `gundi:reference` params only support literals and `$data` form references — there is no way to pass the user's typed text. Raise against the RFC.
2. **`$data` from a primitive array element is under-specified in the spec.** This plan uses the interpretation that resolution starts at the node containing the annotated element (the array itself), so from an element of `values` inside an `AnnotationFilter` row, `"../term"` climbs from the `values` array to the row object. Same logic gives `"../bounding_box"` from a `projects` element (projects array → root config). Flag to the portal team when the widget lands.
3. `list_projects` is scoped by the integration's configured `bounding_box` (center + covering radius, clamped to 1–500 km) because project search is only useful geographically here and requires no auth. While `bounding_box` is empty the portal won't fetch (spec: empty `$data` dependency → stay free text), which is correct behavior.

---

### Task 1: Reference-action contract + gated self-registration

**Files:**
- Modify: `app/actions/core.py` (add marker class + envelope models; extend `typing` import)
- Modify: `app/services/core.py` (add `REFERENCE` to `ActionTypeEnum`)
- Modify: `app/settings/integration.py` (add `REGISTER_REFERENCE_ACTIONS`)
- Modify: `app/services/self_registration.py` (skip-when-gated + type branch)
- Create: `app/actions/tests/test_reference_actions.py`

**Interfaces:**
- Consumes: existing `ActionConfiguration`, `ActionTypeEnum`, `register_integration_in_gundi(gundi_client, type_slug=...)`.
- Produces: `app.actions.core.ReferenceActionConfiguration` (marker base, subclass of `ActionConfiguration`), `app.actions.core.ReferenceOption(value, label=None, description=None, group=None)`, `app.actions.core.ReferenceDataResponse(options, cache_ttl_seconds=300, truncated=False)`, `app.settings.REGISTER_REFERENCE_ACTIONS: bool`. Later tasks import all of these by these exact names. (`app/actions/__init__.py` does `from .core import *`, so the new classes are importable from `app.actions` too — needed by `self_registration.py`'s existing import style.)

- [ ] **Step 1: Write the failing tests**

Create `app/actions/tests/test_reference_actions.py`:

```python
"""Tests for the reference-action contract, registration gating, and the iNat reference actions."""

import pytest
from unittest.mock import AsyncMock, MagicMock


def test_reference_contract_types():
    from app.actions.core import (
        ActionConfiguration,
        ReferenceActionConfiguration,
        ReferenceDataResponse,
        ReferenceOption,
    )

    assert issubclass(ReferenceActionConfiguration, ActionConfiguration)

    response = ReferenceDataResponse(
        options=[ReferenceOption(value="22", label="Evidence of Presence")]
    )
    data = response.dict()
    assert data["options"][0]["value"] == "22"
    assert data["options"][0]["description"] is None
    assert data["options"][0]["group"] is None
    assert data["cache_ttl_seconds"] == 300
    assert data["truncated"] is False


def test_action_type_enum_has_reference():
    from app.services.core import ActionTypeEnum

    assert ActionTypeEnum.REFERENCE.value == "reference"


def _dummy_reference_handlers():
    from app.actions.core import ReferenceActionConfiguration

    class DummyQuery(ReferenceActionConfiguration):
        pass

    async def action_list_dummy(integration, action_config: DummyQuery):
        return {"options": []}

    return {"list_dummy": (action_list_dummy, DummyQuery, None)}


@pytest.mark.asyncio
async def test_reference_actions_skipped_from_registration_by_default(mocker):
    from app.services import self_registration

    mocker.patch.object(self_registration, "action_handlers", _dummy_reference_handlers())
    gundi_client = MagicMock()
    gundi_client.register_integration_type = AsyncMock(return_value={})

    await self_registration.register_integration_in_gundi(gundi_client, type_slug="inaturalist")

    data = gundi_client.register_integration_type.call_args.args[0]
    assert data["actions"] == []


@pytest.mark.asyncio
async def test_reference_actions_registered_with_reference_type_when_enabled(mocker):
    from app.services import self_registration

    mocker.patch.object(self_registration, "action_handlers", _dummy_reference_handlers())
    mocker.patch.object(self_registration, "REGISTER_REFERENCE_ACTIONS", True)
    gundi_client = MagicMock()
    gundi_client.register_integration_type = AsyncMock(return_value={})

    await self_registration.register_integration_in_gundi(gundi_client, type_slug="inaturalist")

    data = gundi_client.register_integration_type.call_args.args[0]
    assert [a["value"] for a in data["actions"]] == ["list_dummy"]
    assert data["actions"][0]["type"] == "reference"
    assert data["actions"][0]["is_periodic_action"] is False
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest app/actions/tests/test_reference_actions.py -v`
Expected: FAIL — `ImportError: cannot import name 'ReferenceActionConfiguration'` (and the enum test fails with `AttributeError: REFERENCE`).

- [ ] **Step 3: Add the contract to `app/actions/core.py`**

Change the typing import at the top from `from typing import Optional` to:

```python
from typing import List, Optional
```

After the existing `class GenericActionConfiguration(ActionConfiguration):` block, add (mirrors `gundi-integration-cmore/app/actions/core.py`):

```python
class ReferenceActionConfiguration(ActionConfiguration):
    """Marker base for reference-data actions: the config model IS the query.

    Reference actions are stateless — they store no configuration of their
    own; callers (the Gundi portal) supply query params via config_overrides.
    They return a ReferenceDataResponse dict. Spec: gundi-integration-cmore
    docs/superpowers/specs/2026-07-31-reference-data-config-ui-design.md.
    """


class ReferenceOption(BaseModel):
    value: str
    label: Optional[str] = None        # portal defaults label to value
    description: Optional[str] = None  # tooltip / help text
    group: Optional[str] = None        # optional grouping for long lists


class ReferenceDataResponse(BaseModel):
    options: List[ReferenceOption]
    cache_ttl_seconds: int = 300       # portal-side cache hint
    truncated: bool = False            # true if the list was capped
```

- [ ] **Step 4: Add `REFERENCE` to `app/services/core.py`**

In `class ActionTypeEnum(str, Enum)` add after `GENERIC = "generic"`:

```python
    REFERENCE = "reference"
```

- [ ] **Step 5: Add the setting to `app/settings/integration.py`**

Replace the file's single comment line so the file reads (mirrors cmore's `app/settings/integration.py`, same `environs` package the template uses):

```python
# Add your integration-specific settings here
from environs import Env

env = Env()
env.read_env()

# Phase 0 of the reference-data design (gundi-integration-cmore docs/superpowers/
# specs/2026-07-31-reference-data-config-ui-design.md): reference actions are only
# registered in Gundi once the platform accepts the "reference" action type.
# Until then this stays off so self-registration never sends a type the API
# would reject.
REGISTER_REFERENCE_ACTIONS = env.bool("REGISTER_REFERENCE_ACTIONS", False)
```

First verify the import style matches `app/settings/base.py` (open it; if base.py uses `import environ` / `environ.Env()` instead of `from environs import Env`, use base.py's style — the assertion that matters is `REGISTER_REFERENCE_ACTIONS` exported from `app.settings`).

- [ ] **Step 6: Gate registration in `app/services/self_registration.py`**

Extend the existing import from `app.actions` to include the marker:

```python
from app.actions import (
    action_handlers,
    AuthActionConfiguration,
    PullActionConfiguration,
    PushActionConfiguration,
    ExecutableActionMixin,
    InternalActionConfiguration,
    ReferenceActionConfiguration,
)
```

Extend the settings import:

```python
from app.settings import (
    INTEGRATION_TYPE_SLUG,
    INTEGRATION_TYPE_NAME,
    INTEGRATION_SERVICE_URL,
    REGISTER_REFERENCE_ACTIONS,
)
```

In the action loop, directly after the `InternalActionConfiguration` skip block, add:

```python
        if issubclass(config_model, ReferenceActionConfiguration) and not REGISTER_REFERENCE_ACTIONS:
            logger.info(
                f"Skipping reference action '{action_id}' "
                "(REGISTER_REFERENCE_ACTIONS is off until the platform supports the 'reference' type)."
            )
            continue
```

And make the type branch check the reference marker first:

```python
        if issubclass(config_model, ReferenceActionConfiguration):
            action_type = ActionTypeEnum.REFERENCE.value
        elif issubclass(config_model, AuthActionConfiguration):
            action_type = ActionTypeEnum.AUTHENTICATION.value
        elif issubclass(config_model, PullActionConfiguration):
            action_type = ActionTypeEnum.PULL_DATA.value
        elif issubclass(config_model, PushActionConfiguration):
            action_type = ActionTypeEnum.PUSH_DATA.value
        else:
            action_type = ActionTypeEnum.GENERIC.value
```

- [ ] **Step 7: Run the new tests — expect PASS**

Run: `.venv/bin/python -m pytest app/actions/tests/test_reference_actions.py -v`
Expected: 4 passed.

- [ ] **Step 8: Run the full suite to catch regressions**

Run: `.venv/bin/python -m pytest`
Expected: all pass.

- [ ] **Step 9: Commit**

```bash
git add app/actions/core.py app/services/core.py app/settings/integration.py app/services/self_registration.py app/actions/tests/test_reference_actions.py
git commit -m "feat: reference-action contract and gated self-registration (reference-data Phase 0)"
```

---

### Task 2: Stateless reference-action execution in action_runner

**Files:**
- Modify: `app/services/action_runner.py` (three hunks, ported from `gundi-integration-cmore/app/services/action_runner.py`)
- Test: `app/actions/tests/test_reference_actions.py` (append)

**Interfaces:**
- Consumes: `ReferenceActionConfiguration` from Task 1; existing `execute_action(integration_id, action_id, config_overrides=None, ...)`.
- Produces: `execute_action` runs a reference action with no stored config and no/partial `config_overrides`; handler errors on reference actions are reported with `config_data=None` (never the integration's stored configurations, which contain secrets).

- [ ] **Step 1: Read the local file and confirm names**

Open `app/services/action_runner.py`. Confirm: (a) the handlers registry name it looks up actions in (`action_handlers`, imported from `app.actions`), (b) `_handle_error(...)` is called with keyword `config_data=` on the two handler-exception paths near the end of `execute_action`, (c) the missing-config branch reads `if not action_config and not config_overrides:`. These match the cmore diff this task ports.

- [ ] **Step 2: Write the failing tests**

Append to `app/actions/tests/test_reference_actions.py`:

```python
def _dummy_query_and_handler(fail=False):
    from app.actions.core import ReferenceActionConfiguration

    class DummyQuery(ReferenceActionConfiguration):
        term: str = "default"

    seen = {}

    async def action_list_dummy(integration, action_config: DummyQuery):
        if fail:
            raise ValueError("boom")
        seen["config"] = action_config
        return {"options": []}

    return DummyQuery, action_list_dummy, seen


def _patch_runner(mocker, handlers, integration):
    from app.services import action_runner

    mocker.patch.object(action_runner, "action_handlers", handlers)
    config_manager = MagicMock()
    config_manager.get_integration_details = AsyncMock(return_value=integration)
    config_manager.get_action_configuration = AsyncMock(return_value=None)
    mocker.patch.object(action_runner, "config_manager", config_manager)
    return action_runner


@pytest.mark.asyncio
async def test_execute_reference_action_without_stored_config(
    mocker, inaturalist_integration_v2, mock_publish_event
):
    DummyQuery, handler, seen = _dummy_query_and_handler()
    action_runner = _patch_runner(
        mocker, {"list_dummy": (handler, DummyQuery, None)}, inaturalist_integration_v2
    )
    mocker.patch.object(action_runner, "publish_event", mock_publish_event)

    result = await action_runner.execute_action(
        integration_id=str(inaturalist_integration_v2.id),
        action_id="list_dummy",
        config_overrides={"term": "22"},
    )

    assert result == {"options": []}
    assert seen["config"].term == "22"


@pytest.mark.asyncio
async def test_execute_reference_action_with_no_overrides_is_not_404(
    mocker, inaturalist_integration_v2, mock_publish_event
):
    """A zero-param reference query (no stored config, no overrides) is a
    legitimate, complete request — not a missing-configuration error."""
    DummyQuery, handler, seen = _dummy_query_and_handler()
    action_runner = _patch_runner(
        mocker, {"list_dummy": (handler, DummyQuery, None)}, inaturalist_integration_v2
    )
    mocker.patch.object(action_runner, "publish_event", mock_publish_event)

    result = await action_runner.execute_action(
        integration_id=str(inaturalist_integration_v2.id),
        action_id="list_dummy",
    )

    assert result == {"options": []}
    assert seen["config"].term == "default"


@pytest.mark.asyncio
async def test_reference_action_errors_never_carry_stored_configurations(
    mocker, inaturalist_integration_v2, mock_publish_event
):
    DummyQuery, handler, _ = _dummy_query_and_handler(fail=True)
    action_runner = _patch_runner(
        mocker, {"list_dummy": (handler, DummyQuery, None)}, inaturalist_integration_v2
    )
    mocker.patch.object(action_runner, "publish_event", mock_publish_event)
    handle_error = mocker.patch.object(
        action_runner, "_handle_error", AsyncMock(return_value="error-response")
    )

    result = await action_runner.execute_action(
        integration_id=str(inaturalist_integration_v2.id),
        action_id="list_dummy",
        config_overrides={"term": "22"},
    )

    assert result == "error-response"
    assert handle_error.call_args.kwargs["config_data"] is None
```

Note: `inaturalist_integration_v2` comes from `app/actions/tests/conftest.py`; `mock_publish_event` from `app/conftest.py`. If `execute_action` internally references other collaborators these mocks don't cover (check the traceback), patch them the same way `test_pull_events.py` does — but keep `get_action_configuration` returning `None`, that's the point of the test.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest app/actions/tests/test_reference_actions.py -v`
Expected: the two new success-path tests FAIL — the no-overrides one gets the 404 missing-configuration response instead of `{"options": []}`; the redaction test fails because `config_data` is the configurations dict, not None.

- [ ] **Step 4: Port the three cmore hunks into `app/services/action_runner.py`**

(1) Extend the import:

```python
from app.actions.core import PullActionConfiguration, ReferenceActionConfiguration
```

(2) Just above the `action_config = await config_manager.get_action_configuration(...)` line, add — and extend the condition on the missing-config branch:

```python
    # Reference actions are stateless: they never have stored config, and a
    # caller sending no config_overrides (e.g. a zero-param query like
    # list_annotation_terms) is a legitimate, complete request — not a 404.
    # Required query params still fail Pydantic validation below with a 422,
    # which is the correct signal to the portal.
    is_reference_action = isinstance(config_model, type) and issubclass(
        config_model, ReferenceActionConfiguration
    )

    # Get the configuration needed to execute the action
    action_config = await config_manager.get_action_configuration(integration_id, action_id)
    if not action_config and not config_overrides and not is_reference_action:
```

(3) Just above the `try:  # Execute the action handler with a timeout` block, add — then use it in BOTH handler-error paths (the `asyncio.TimeoutError` one and the generic `except Exception` one), replacing `config_data={"configurations": [c.dict() for c in integration.configurations]}` with `config_data=handler_error_config_data`:

```python
    # Reference actions are portal-invoked at interactive-fetch frequency (e.g.
    # every dropdown open), so a handler failure is routine rather than
    # exceptional (an unknown term after a vocabulary change, a transient 5xx
    # from the iNat API). Unlike other action types, their errors must not
    # carry the integration's stored configurations — which can include raw
    # secrets — into the published IntegrationActionFailed event or the JSON
    # error response.
    handler_error_config_data = None if is_reference_action else {
        "configurations": [c.dict() for c in integration.configurations]
    }
```

- [ ] **Step 5: Run the tests — expect PASS**

Run: `.venv/bin/python -m pytest app/actions/tests/test_reference_actions.py -v`
Expected: all pass.

- [ ] **Step 6: Run the full suite**

Run: `.venv/bin/python -m pytest`
Expected: all pass (existing action_runner behavior for pull/auth actions unchanged).

- [ ] **Step 7: Commit**

```bash
git add app/services/action_runner.py app/actions/tests/test_reference_actions.py
git commit -m "feat: execute stateless reference actions without stored config; redact secrets from their errors"
```

---

### Task 3: Quality grade as a static enum

**Files:**
- Modify: `app/actions/configurations.py` (field type only)
- Create: `app/actions/tests/test_configurations.py`

**Interfaces:**
- Consumes: existing `PullEventsConfig` and its `validate_quality_grade` pre-validator (kept as-is — it normalizes case/space variants before the Literal check runs).
- Produces: `PullEventsConfig.quality_grade: Optional[List[Literal["casual", "needs_id", "research"]]]` — runtime values remain plain `str`, so `get_observations(quality_grade=...)` is unaffected. JSON schema gains `items.enum`, which today's portal already renders as a multi-select (no reference widget needed).

- [ ] **Step 1: Write the failing tests**

Create `app/actions/tests/test_configurations.py`:

```python
"""Tests for PullEventsConfig field schemas and validators."""

import pydantic
import pytest

from app.actions.configurations import PullEventsConfig


def test_quality_grade_schema_enumerates_values():
    schema = PullEventsConfig.schema()
    assert schema["properties"]["quality_grade"]["items"]["enum"] == [
        "casual", "needs_id", "research",
    ]


def test_quality_grade_enum_matches_pyinaturalist():
    """Drift guard: our hardcoded Literal must track pyinaturalist's vocabulary."""
    from pyinaturalist.constants import QUALITY_GRADES

    schema = PullEventsConfig.schema()
    assert set(schema["properties"]["quality_grade"]["items"]["enum"]) == set(QUALITY_GRADES)


def test_quality_grade_still_normalizes_legacy_variants():
    config = PullEventsConfig(days_to_load=3, quality_grade=["Needs ID", "research"])
    assert config.quality_grade == ["needs_id", "research"]


def test_quality_grade_invalid_value_raises():
    with pytest.raises(pydantic.ValidationError):
        PullEventsConfig(days_to_load=3, quality_grade=[0])
```

- [ ] **Step 2: Run the tests to verify the schema ones fail**

Run: `.venv/bin/python -m pytest app/actions/tests/test_configurations.py -v`
Expected: the two schema tests FAIL with `KeyError: 'enum'` (plain `List[str]` has no items enum); the two validator tests PASS already.

- [ ] **Step 3: Change the field type**

In `app/actions/configurations.py`, change the typing import to include `Literal`:

```python
from typing import Optional, List, Dict, Literal
```

Change the field (keep title; extend the description since values are now selectable):

```python
    quality_grade: Optional[List[Literal["casual", "needs_id", "research"]]] = pydantic.Field(
        None,
        title="Quality Grade",
        description="If present, only observations that have one of the selected quality grades will be included.",
    )
```

Keep `validate_quality_grade` exactly as-is (it runs `pre=True`, so legacy variants are normalized before the Literal check).

- [ ] **Step 4: Run the tests — expect PASS**

Run: `.venv/bin/python -m pytest app/actions/tests/test_configurations.py -v`
Expected: 4 passed.

- [ ] **Step 5: Run the full suite**

Run: `.venv/bin/python -m pytest`
Expected: all pass. If a fixture in `app/actions/tests/conftest.py` asserts the old quality_grade field description verbatim, update that fixture string to match.

- [ ] **Step 6: Commit**

```bash
git add app/actions/configurations.py app/actions/tests/test_configurations.py
git commit -m "feat: render quality_grade as a static enum multi-select in the portal"
```

---

### Task 4: Reshape `annotations` into structured rows (backward compatible)

**Files:**
- Modify: `app/actions/configurations.py` (new `AnnotationFilter` model, field retype, validator swap, `annotations_dict` property)
- Modify: `app/actions/handlers.py:82` (call site: `annotations=action_config.annotations_dict`)
- Test: `app/actions/tests/test_configurations.py` (append)

**Interfaces:**
- Consumes: `PullEventsConfig`; `get_observations(..., annotations: Optional[Dict])` in `app/datasource/inaturalist.py` (signature unchanged).
- Produces: `AnnotationFilter(term: str, values: List[str])`; `PullEventsConfig.annotations: Optional[List[AnnotationFilter]]`; `PullEventsConfig.annotations_dict -> Optional[Dict[str, List[str]]]` property that reproduces the legacy dict shape `_match_annotations_to_config` consumes. Task 7 attaches `gundi:reference` to `annotations.items.term` and `annotations.items.values.items`.

- [ ] **Step 1: Write the failing tests**

Append to `app/actions/tests/test_configurations.py`:

```python
def test_annotations_accepts_legacy_json_string():
    config = PullEventsConfig(days_to_load=3, annotations='{"22": ["24", "25"], "1": ["2"]}')
    assert [(f.term, f.values) for f in config.annotations] == [
        ("22", ["24", "25"]), ("1", ["2"]),
    ]
    assert config.annotations_dict == {"22": ["24", "25"], "1": ["2"]}


def test_annotations_accepts_legacy_dict_with_int_keys():
    config = PullEventsConfig(days_to_load=3, annotations={22: [24, 25]})
    assert config.annotations_dict == {"22": ["24", "25"]}


def test_annotations_accepts_structured_rows():
    config = PullEventsConfig(
        days_to_load=3, annotations=[{"term": "22", "values": ["24"]}]
    )
    assert config.annotations_dict == {"22": ["24"]}


@pytest.mark.parametrize("raw", [None, "", "   ", "{}"])
def test_annotations_empty_inputs_mean_no_filter(raw):
    config = PullEventsConfig(days_to_load=3, annotations=raw)
    assert config.annotations_dict is None


def test_annotations_invalid_json_raises():
    with pytest.raises(pydantic.ValidationError):
        PullEventsConfig(days_to_load=3, annotations="{not json")


def test_annotations_schema_is_structured_rows():
    schema = PullEventsConfig.schema()
    prop = schema["properties"]["annotations"]
    assert prop["type"] == "array"
    row = schema["definitions"]["AnnotationFilter"]["properties"]
    assert row["term"]["type"] == "string"
    assert row["values"]["type"] == "array"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest app/actions/tests/test_configurations.py -v`
Expected: new tests FAIL (`annotations_dict` doesn't exist; structured rows are rejected by the current `str` field).

- [ ] **Step 3: Implement the reshape in `app/actions/configurations.py`**

Add above `PullEventsConfig`:

```python
class AnnotationFilter(pydantic.BaseModel):
    term: str = pydantic.Field(
        ...,
        title="Annotation term",
        description="iNaturalist controlled term ID (e.g. '22' for Evidence of Presence).",
    )
    values: List[str] = pydantic.Field(
        default_factory=list,
        title="Allowed values",
        description="Controlled value IDs required for this term (e.g. '24' for Organism).",
    )
```

Replace the `annotations` field:

```python
    annotations: Optional[List[AnnotationFilter]] = pydantic.Field(
        None,
        title="Annotations",
        description=(
            "Annotation filters. Each row selects a controlled term and the values required "
            "for that term — e.g. term 22 (Evidence of Presence) with values 24 (Organism) "
            "or 25 (Scat). All terms listed must be present on an observation; within a "
            "term, all listed values must be present. Legacy JSON-string configs "
            '(e.g. {"22": ["24", "25"]}) are still accepted.'
        ),
    )
```

Replace the `validate_json` validator with:

```python
    @pydantic.validator("annotations", pre=True, always=True)
    def coerce_annotations(cls, v):
        # Legacy configs stored this as a JSON-encoded string (or raw dict) of
        # {term: [values]}; coerce both into the structured row shape.
        if v is None:
            return None
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None
            try:
                v = json.loads(v)
            except Exception:
                raise ValueError(f"Could not parse json: {v}")
        if isinstance(v, dict):
            return [
                {"term": str(term), "values": [str(value) for value in (values or [])]}
                for term, values in v.items()
            ]
        return v
```

Add the property below the validators (before `class Config`):

```python
    @property
    def annotations_dict(self) -> Optional[Dict[str, List[str]]]:
        """The {term: [values]} shape the datasource's annotation matcher consumes."""
        if not self.annotations:
            return None
        return {f.term: f.values for f in self.annotations}
```

- [ ] **Step 4: Update the call site in `app/actions/handlers.py`**

In `action_pull_events`, change the `get_observations(...)` kwarg from `annotations=action_config.annotations` to:

```python
        annotations=action_config.annotations_dict,
```

- [ ] **Step 5: Check for other readers**

Run: `grep -rn "\.annotations" app/ handler_test_configuration.py --include="*.py" | grep -v tests | grep -v "annotations_dict"`
Expected: only the field definition/validator in `configurations.py` and `observation.annotations` uses in `datasource/inaturalist.py` (those are iNat observation objects, not config — leave them). If anything else reads the config field expecting a dict, switch it to `annotations_dict`.

- [ ] **Step 6: Run the full suite**

Run: `.venv/bin/python -m pytest`
Expected: all pass — existing fixtures use `''` and dict shapes for annotations, both covered by the coercion.

- [ ] **Step 7: Commit**

```bash
git add app/actions/configurations.py app/actions/handlers.py app/actions/tests/test_configurations.py
git commit -m "feat: structure the annotations config as term/values rows (legacy JSON-string configs still accepted)"
```

---

### Task 5: Datasource lookup helpers (controlled terms, project search, bbox geometry)

**Files:**
- Modify: `app/datasource/inaturalist.py`
- Test: `app/datasource/tests/test_inaturalist.py` (append)

**Interfaces:**
- Consumes: `pyinaturalist.get_controlled_terms`, `pyinaturalist.get_projects` (both verified importable from the top-level package at the pinned 0.19.0).
- Produces: `list_controlled_terms() -> List[Dict]` (unwrapped `results`), `search_projects_near(lat: float, lng: float, radius_km: float) -> Dict` (raw iNat response with `total_results`/`results`), `bbox_to_search_circle(bounding_box: List[float]) -> Tuple[float, float, float]` (center_lat, center_lng, radius_km clamped to [1, 500]), constants `PROJECTS_PAGE_SIZE = 200`, `MAX_PROJECT_SEARCH_RADIUS_KM = 500.0`. Task 6's handlers import these three functions by name.

- [ ] **Step 1: Write the failing tests**

Append to `app/datasource/tests/test_inaturalist.py`:

```python
def test_bbox_to_search_circle_centers_and_covers_box():
    from app.datasource.inaturalist import bbox_to_search_circle

    # ~0.3 degree box around Seattle: [ne_lat, ne_lng, sw_lat, sw_lng]
    lat, lng, radius_km = bbox_to_search_circle([47.7, -122.2, 47.4, -122.5])
    assert lat == pytest.approx(47.55)
    assert lng == pytest.approx(-122.35)
    # Half-diagonal of the box: ~16.7 km latitude leg, ~11.2 km longitude leg
    assert 15 < radius_km < 30


def test_bbox_to_search_circle_clamps_radius():
    from app.datasource.inaturalist import bbox_to_search_circle

    _, _, huge = bbox_to_search_circle([60.0, 170.0, -60.0, -170.0])
    assert huge == 500.0
    _, _, tiny = bbox_to_search_circle([47.5001, -122.4999, 47.5, -122.5])
    assert tiny == 1.0


def test_list_controlled_terms_unwraps_results(mocker):
    from app.datasource import inaturalist

    mocker.patch.object(
        inaturalist, "get_controlled_terms",
        return_value={"total_results": 1, "results": [
            {"id": 22, "label": "Evidence of Presence", "values": []},
        ]},
    )
    assert inaturalist.list_controlled_terms() == [
        {"id": 22, "label": "Evidence of Presence", "values": []},
    ]


def test_search_projects_near_passes_circle_params(mocker):
    from app.datasource import inaturalist

    get_projects = mocker.patch.object(
        inaturalist, "get_projects", return_value={"total_results": 0, "results": []}
    )
    inaturalist.search_projects_near(47.55, -122.35, 21.0)
    get_projects.assert_called_once_with(
        lat=47.55, lng=-122.35, radius=21.0, order_by="distance", per_page=200
    )
```

(If the file doesn't already import `pytest`, add `import pytest` at its top.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest app/datasource/tests/test_inaturalist.py -v -k "bbox or controlled or projects_near"`
Expected: FAIL with ImportError/AttributeError on the new names.

- [ ] **Step 3: Implement in `app/datasource/inaturalist.py`**

Change the math import (currently `from math import ceil`) to:

```python
from math import asin, ceil, cos, radians, sin, sqrt
```

Extend the pyinaturalist import:

```python
from pyinaturalist import (
    Annotation,
    Observation,
    get_controlled_terms,
    get_observations_v2,
    get_projects,
)
```

Extend `Tuple` in the typing import: `from typing import Dict, List, Optional, Tuple`.

Add near the other module constants:

```python
# Reference-action project search: one page of nearest projects; the portal's
# combobox allows free text for anything beyond it (truncated=True signals the cap).
PROJECTS_PAGE_SIZE = 200
MAX_PROJECT_SEARCH_RADIUS_KM = 500.0
```

Add the functions (below `_match_annotations_to_config` is fine):

```python
def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    lat1, lng1, lat2, lng2 = map(radians, (lat1, lng1, lat2, lng2))
    a = sin((lat2 - lat1) / 2) ** 2 + cos(lat1) * cos(lat2) * sin((lng2 - lng1) / 2) ** 2
    return 2 * 6371.0 * asin(sqrt(a))


def bbox_to_search_circle(bounding_box: List[float]) -> Tuple[float, float, float]:
    """Center and covering radius (km) of a [ne_lat, ne_lng, sw_lat, sw_lng] box.

    iNat project search is point+radius, not box, so the box is approximated by
    the circle through its corners. Radius is clamped to [1, 500] km: a floor so
    a tiny box still finds its local projects, a cap because a continent-sized
    search circle returns noise anyway (the portal keeps free text for the rest).
    """
    ne_lat, ne_lng, sw_lat, sw_lng = bounding_box[:4]
    center_lat = (ne_lat + sw_lat) / 2
    center_lng = (ne_lng + sw_lng) / 2
    radius_km = _haversine_km(center_lat, center_lng, ne_lat, ne_lng)
    return center_lat, center_lng, min(max(radius_km, 1.0), MAX_PROJECT_SEARCH_RADIUS_KM)


def list_controlled_terms() -> List[Dict]:
    """All iNaturalist annotation controlled terms (public endpoint, no auth)."""
    response = get_controlled_terms()
    return response.get("results", []) if isinstance(response, dict) else []


def search_projects_near(lat: float, lng: float, radius_km: float) -> Dict:
    """Nearest-first iNaturalist projects within radius_km of a point (public endpoint)."""
    return get_projects(
        lat=lat, lng=lng, radius=radius_km, order_by="distance", per_page=PROJECTS_PAGE_SIZE
    )
```

- [ ] **Step 4: Run the tests — expect PASS**

Run: `.venv/bin/python -m pytest app/datasource/tests/test_inaturalist.py -v`
Expected: all pass (new and pre-existing).

- [ ] **Step 5: Commit**

```bash
git add app/datasource/inaturalist.py app/datasource/tests/test_inaturalist.py
git commit -m "feat: datasource lookups for controlled terms and nearby projects"
```

---

### Task 6: Reference query models + the three reference action handlers

**Files:**
- Modify: `app/actions/configurations.py` (extract `parse_bounding_box`, add three query models)
- Modify: `app/actions/handlers.py` (three `action_list_*` handlers)
- Test: `app/actions/tests/test_reference_actions.py` (append)

**Interfaces:**
- Consumes: `ReferenceActionConfiguration`, `ReferenceOption`, `ReferenceDataResponse` (Task 1); `list_controlled_terms`, `search_projects_near`, `bbox_to_search_circle` (Task 5).
- Produces: `ListProjectsQuery(bounding_box: str)` (validator parses to `List[float]`), `ListAnnotationTermsQuery()` (no params), `ListAnnotationValuesQuery(term: str)` in `app/actions/configurations.py`; handlers `action_list_projects`, `action_list_annotation_terms`, `action_list_annotation_values` in `app/actions/handlers.py`, each returning a `ReferenceDataResponse` dict. `parse_bounding_box(v)` module function in `configurations.py`. Task 7's `gundi:reference` annotations name these actions (`list_projects`, `list_annotation_terms`, `list_annotation_values`) and param keys (`bounding_box`, `term`).

- [ ] **Step 1: Write the failing tests**

Append to `app/actions/tests/test_reference_actions.py`:

```python
CONTROLLED_TERMS = [
    {"id": 1, "label": "Life Stage", "values": [
        {"id": 2, "label": "Adult"}, {"id": 3, "label": "Teneral"},
    ]},
    {"id": 22, "label": "Evidence of Presence", "values": [
        {"id": 24, "label": "Organism"}, {"id": 25, "label": "Scat"},
    ]},
]


@pytest.mark.asyncio
async def test_list_annotation_terms(mocker, inaturalist_integration_v2):
    from app.actions import handlers
    from app.actions.configurations import ListAnnotationTermsQuery

    mocker.patch.object(handlers, "list_controlled_terms", return_value=CONTROLLED_TERMS)

    result = await handlers.action_list_annotation_terms(
        inaturalist_integration_v2, ListAnnotationTermsQuery()
    )

    assert [(o["value"], o["label"]) for o in result["options"]] == [
        ("22", "Evidence of Presence"), ("1", "Life Stage"),
    ]
    assert result["cache_ttl_seconds"] == 3600


@pytest.mark.asyncio
async def test_list_annotation_values_for_term(mocker, inaturalist_integration_v2):
    from app.actions import handlers
    from app.actions.configurations import ListAnnotationValuesQuery

    mocker.patch.object(handlers, "list_controlled_terms", return_value=CONTROLLED_TERMS)

    result = await handlers.action_list_annotation_values(
        inaturalist_integration_v2, ListAnnotationValuesQuery(term="22")
    )

    assert [(o["value"], o["label"]) for o in result["options"]] == [
        ("24", "Organism"), ("25", "Scat"),
    ]


@pytest.mark.asyncio
async def test_list_annotation_values_unknown_term_raises(mocker, inaturalist_integration_v2):
    from app.actions import handlers
    from app.actions.configurations import ListAnnotationValuesQuery

    mocker.patch.object(handlers, "list_controlled_terms", return_value=CONTROLLED_TERMS)

    with pytest.raises(ValueError, match="99"):
        await handlers.action_list_annotation_values(
            inaturalist_integration_v2, ListAnnotationValuesQuery(term="99")
        )


@pytest.mark.asyncio
async def test_list_projects_searches_the_bounding_box_circle(mocker, inaturalist_integration_v2):
    from app.actions import handlers
    from app.actions.configurations import ListProjectsQuery

    search = mocker.patch.object(
        handlers, "search_projects_near",
        return_value={"total_results": 2, "results": [
            {"id": 100, "title": "Puget Sound Seabirds"},
            {"id": 200, "title": "WA Invasives"},
        ]},
    )

    result = await handlers.action_list_projects(
        inaturalist_integration_v2,
        ListProjectsQuery(bounding_box="[47.7, -122.2, 47.4, -122.5]"),
    )

    lat, lng, radius_km = search.call_args.args
    assert lat == pytest.approx(47.55)
    assert lng == pytest.approx(-122.35)
    assert 15 < radius_km < 30
    # Nearest-first API order is preserved (not re-sorted alphabetically)
    assert [(o["value"], o["label"]) for o in result["options"]] == [
        ("100", "Puget Sound Seabirds"), ("200", "WA Invasives"),
    ]
    assert result["truncated"] is False


@pytest.mark.asyncio
async def test_list_projects_flags_truncation(mocker, inaturalist_integration_v2):
    from app.actions import handlers
    from app.actions.configurations import ListProjectsQuery

    mocker.patch.object(
        handlers, "search_projects_near",
        return_value={"total_results": 300, "results": [{"id": 100, "title": "P"}]},
    )

    result = await handlers.action_list_projects(
        inaturalist_integration_v2,
        ListProjectsQuery(bounding_box="[47.7, -122.2, 47.4, -122.5]"),
    )
    assert result["truncated"] is True


def test_list_projects_query_rejects_bad_bounding_box():
    import pydantic
    from app.actions.configurations import ListProjectsQuery

    with pytest.raises(pydantic.ValidationError):
        ListProjectsQuery(bounding_box="not json")
    with pytest.raises(pydantic.ValidationError):
        ListProjectsQuery(bounding_box="[1, 2, 3]")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest app/actions/tests/test_reference_actions.py -v`
Expected: new tests FAIL with ImportError on the query models/handlers.

- [ ] **Step 3: Extract `parse_bounding_box` and add the query models in `app/actions/configurations.py`**

Add the import at the top: `from .core import PullActionConfiguration, AuthActionConfiguration, ExecutableActionMixin, ReferenceActionConfiguration`.

Add a module-level function above `PullEventsConfig`, moving the body of `validate_bounding_box` verbatim (keep every message string identical, including the pre-existing `{v[2]}` in the SW-longitude message):

```python
def parse_bounding_box(v):
    """Parse and validate a JSON-encoded '[ne_lat, ne_lng, sw_lat, sw_lng]' string."""
    if not v:
        return None
    v = json.loads(v)
    if len(v) != 4:
        raise ValueError("Did not receive four values in bounding box configuration.")
    for i in range(0, 4):
        try:
            v[i] = float(v[i])
        except Exception:
            raise ValueError(f"Could not parse bounding box values {v}.")

    if v[0] < -90 or v[0] > 90:
        raise ValueError(f"NE Latitude {v[0]} must be between -90 and 90")
    if v[1] < -180 or v[1] > 180:
        raise ValueError(f"NE Longitude {v[1]} must be between -180 and 180")
    if v[2] < -90 or v[2] > 90:
        raise ValueError(f"SW Latitude {v[2]} must be between -90 and 90")
    if v[3] < -180 or v[3] > 180:
        raise ValueError(f"SW Longitude {v[2]} must be between -180 and 180")
    if v[0] <= v[2]:
        raise ValueError(f"NE Latitude {v[0]} must be greater than SW Latitude {v[2]}")
    if v[1] <= v[3]:
        raise ValueError(f"NE Longitude {v[1]} must be greater than SW Longitude {v[3]}")
    return v
```

Change `PullEventsConfig.validate_bounding_box` to delegate:

```python
    @pydantic.validator("bounding_box", always=True)
    def validate_bounding_box(cls, v):
        return parse_bounding_box(v)
```

Add the query models at the bottom of the file:

```python
class ListProjectsQuery(ReferenceActionConfiguration):
    """Reference query: iNaturalist projects near the configured bounding box."""
    bounding_box: str = pydantic.Field(
        ...,
        title="Bounding box",
        description="Same JSON format as the pull_events bounding_box: [ne_lat, ne_lng, sw_lat, sw_lng].",
    )

    @pydantic.validator("bounding_box")
    def validate_bounding_box(cls, v):
        parsed = parse_bounding_box(v)
        if parsed is None:
            raise ValueError("bounding_box is required to search for nearby projects.")
        return parsed


class ListAnnotationTermsQuery(ReferenceActionConfiguration):
    """Reference query: all iNaturalist annotation controlled terms (no params)."""


class ListAnnotationValuesQuery(ReferenceActionConfiguration):
    """Reference query: the allowed values of one annotation controlled term."""
    term: str = pydantic.Field(..., title="Term ID")
```

- [ ] **Step 4: Add the handlers in `app/actions/handlers.py`**

Extend the imports:

```python
from app.actions.configurations import (
    PullEventsConfig,
    ListProjectsQuery,
    ListAnnotationTermsQuery,
    ListAnnotationValuesQuery,
)
from app.actions.core import ReferenceDataResponse, ReferenceOption
from app.datasource.inaturalist import (
    get_observations,
    bbox_to_search_circle,
    list_controlled_terms,
    search_projects_near,
)
```

Add at the bottom of the file:

```python
async def action_list_projects(integration: Integration, action_config: ListProjectsQuery):
    """Reference action: iNaturalist projects nearest the configured bounding box.

    Uses the public project-search endpoint (no auth), nearest-first, one page —
    the portal's combobox keeps free text for anything beyond the cap.
    """
    lat, lng, radius_km = bbox_to_search_circle(action_config.bounding_box)
    response = search_projects_near(lat, lng, radius_km)
    results = response.get("results", [])
    options = [
        ReferenceOption(value=str(project["id"]), label=project.get("title") or str(project["id"]))
        for project in results
        if project.get("id") is not None
    ]
    truncated = response.get("total_results", len(options)) > len(options)
    return ReferenceDataResponse(options=options, truncated=truncated).dict()


async def action_list_annotation_terms(integration: Integration, action_config: ListAnnotationTermsQuery):
    """Reference action: iNaturalist annotation controlled terms (near-static vocabulary)."""
    terms = list_controlled_terms()
    options = [
        ReferenceOption(value=str(term["id"]), label=term.get("label") or str(term["id"]))
        for term in terms
        if term.get("id") is not None
    ]
    options.sort(key=lambda o: o.label or o.value)
    return ReferenceDataResponse(options=options, cache_ttl_seconds=3600).dict()


async def action_list_annotation_values(integration: Integration, action_config: ListAnnotationValuesQuery):
    """Reference action: the allowed values of one annotation controlled term."""
    terms = list_controlled_terms()
    term = next((t for t in terms if str(t.get("id")) == action_config.term), None)
    if term is None:
        raise ValueError(f"Unknown iNaturalist annotation term '{action_config.term}'.")
    options = [
        ReferenceOption(value=str(value["id"]), label=value.get("label") or str(value["id"]))
        for value in term.get("values", [])
        if value.get("id") is not None
    ]
    return ReferenceDataResponse(options=options, cache_ttl_seconds=3600).dict()
```

Note: `action_list_projects` calls `search_projects_near(lat, lng, radius_km)` positionally — the test asserts `call_args.args`.

- [ ] **Step 5: Run the tests — expect PASS**

Run: `.venv/bin/python -m pytest app/actions/tests/test_reference_actions.py -v`
Expected: all pass.

- [ ] **Step 6: Run the full suite**

Run: `.venv/bin/python -m pytest`
Expected: all pass. Watch specifically for registration-related tests: the three new handlers now appear in `action_handlers`, and without `REGISTER_REFERENCE_ACTIONS` they must be skipped from registration (Task 1's gating covers this — if any test snapshots the registered-actions list, it must not contain the `list_*` actions).

- [ ] **Step 7: Commit**

```bash
git add app/actions/configurations.py app/actions/handlers.py app/actions/tests/test_reference_actions.py
git commit -m "feat: reference actions for nearby projects and annotation terms/values"
```

---

### Task 7: `gundi:reference` annotations on PullEventsConfig + drift guard

**Files:**
- Modify: `app/actions/configurations.py` (`_reference` helper + `ui_schema()` override on `PullEventsConfig`)
- Test: `app/actions/tests/test_configurations.py` (append)

**Interfaces:**
- Consumes: `UISchemaModelMixin.ui_schema()` classmethod (via `super()`), action ids and query models from Task 6.
- Produces: `PullEventsConfig.ui_schema()` gains inert `gundi:reference` annotations; existing `ui:order` and the `days_to_load` range widget are preserved.

- [ ] **Step 1: Write the failing tests**

Append to `app/actions/tests/test_configurations.py`:

```python
def _collect_gundi_references(node, found):
    if isinstance(node, dict):
        if "gundi:reference" in node:
            found.append((node, node["gundi:reference"]))
        for value in node.values():
            _collect_gundi_references(value, found)


def test_gundi_reference_annotations_match_registered_reference_actions():
    """Drift guard: every gundi:reference annotation must name a real reference
    action whose query model has the declared params, and must never set
    ui:widget (forward-compat: old portals ignore the annotation)."""
    from app.actions.core import ReferenceActionConfiguration, discover_actions

    handlers = discover_actions(module_name="app.actions.handlers", prefix="action_")
    reference_actions = {
        action_id: config_model
        for action_id, (func, config_model, data_model) in handlers.items()
        if issubclass(config_model, ReferenceActionConfiguration)
    }

    found = []
    _collect_gundi_references(PullEventsConfig.ui_schema(), found)

    assert {ref["action"] for _, ref in found} == {
        "list_projects", "list_annotation_terms", "list_annotation_values",
    }
    for node, ref in found:
        assert ref["target"] == "self"
        assert ref["allow_free_text"] is True
        assert ref["action"] in reference_actions
        query_fields = set(reference_actions[ref["action"]].__fields__)
        assert set(ref.get("params", {})) <= query_fields
        assert "ui:widget" not in node


def test_gundi_reference_annotations_sit_on_the_right_nodes():
    ui = PullEventsConfig.ui_schema()

    projects_ref = ui["projects"]["items"]["gundi:reference"]
    assert projects_ref["action"] == "list_projects"
    assert projects_ref["params"] == {"bounding_box": {"$data": "../bounding_box"}}

    term_ref = ui["annotations"]["items"]["term"]["gundi:reference"]
    assert term_ref["action"] == "list_annotation_terms"
    assert term_ref["params"] == {}

    values_ref = ui["annotations"]["items"]["values"]["items"]["gundi:reference"]
    assert values_ref["action"] == "list_annotation_values"
    assert values_ref["params"] == {"term": {"$data": "../term"}}


def test_ui_schema_override_preserves_existing_ui_options():
    ui = PullEventsConfig.ui_schema()
    assert "ui:order" in ui
    assert ui["days_to_load"] == {"ui:widget": "range"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest app/actions/tests/test_configurations.py -v`
Expected: new tests FAIL — no annotations found (`set() == {...}` assertion error).

- [ ] **Step 3: Add the helper and the override in `app/actions/configurations.py`**

Add the module-level helper (above `PullEventsConfig`; mirrors ER's `configurations.py`):

```python
def _reference(action: str, params: Optional[dict] = None) -> dict:
    """Build a gundi:reference ui_schema annotation (spec: gundi-integration-cmore
    docs/superpowers/specs/2026-07-31-reference-data-config-ui-design.md §2).
    Deliberately does NOT set ui:widget — portals without reference support
    must keep rendering plain text fields. All iNat lookups target "self":
    this integration's own runner answers every query."""
    return {
        "action": action,
        "target": "self",
        "params": params or {},
        "allow_free_text": True,
    }
```

Add the classmethod to `PullEventsConfig` (after `ui_global_options`, before the validators):

```python
    @classmethod
    def ui_schema(cls, *args, **kwargs):
        """Annotate fields with gundi:reference so the portal renders live
        dropdowns fed by this runner's reference actions. Inert to portals
        without reference support. $data paths resolve from the node holding
        the annotated element: '../bounding_box' climbs from the projects
        array to the root config; '../term' climbs from a values array to
        its AnnotationFilter row."""
        base = super().ui_schema(*args, **kwargs)
        base["projects"] = {"items": {"gundi:reference": _reference(
            "list_projects", params={"bounding_box": {"$data": "../bounding_box"}},
        )}}
        base["annotations"] = {"items": {
            "term": {"gundi:reference": _reference("list_annotation_terms")},
            "values": {"items": {"gundi:reference": _reference(
                "list_annotation_values", params={"term": {"$data": "../term"}},
            )}},
        }}
        return base
```

- [ ] **Step 4: Run the tests — expect PASS**

Run: `.venv/bin/python -m pytest app/actions/tests/test_configurations.py -v`
Expected: all pass.

- [ ] **Step 5: Run the full suite**

Run: `.venv/bin/python -m pytest`
Expected: all pass. If a conftest fixture snapshots the pull_events `ui_schema` payload, update the snapshot to include the new keys.

- [ ] **Step 6: Commit**

```bash
git add app/actions/configurations.py app/actions/tests/test_configurations.py
git commit -m "feat: gundi:reference ui_schema annotations for projects and annotation dropdowns"
```

---

### Task 8: Documentation + final verification

**Files:**
- Modify: `CLAUDE.md` (repo root)

**Interfaces:**
- Consumes: everything above.
- Produces: repo docs that keep the next engineer from re-deriving the design.

- [ ] **Step 1: Update `CLAUDE.md`**

In the **Architecture** section, after the "Configuration" subsection, add:

```markdown
### Reference actions (selectable config values)

`action_list_projects`, `action_list_annotation_terms`, and `action_list_annotation_values`
in `app/actions/handlers.py` are **reference actions** (marker base
`ReferenceActionConfiguration` in `app/actions/core.py`): stateless queries the Gundi
portal calls through `/v1/actions/execute` with `config_overrides` to populate config-form
dropdowns. They wrap public, unauthenticated iNat endpoints. Design spec:
`gundi-integration-cmore/docs/superpowers/specs/2026-07-31-reference-data-config-ui-design.md`.

- They are hidden from registration unless `REGISTER_REFERENCE_ACTIONS=true`
  (`app/settings/integration.py`) — flip it only once the Gundi API accepts
  `"type": "reference"`.
- `PullEventsConfig.ui_schema()` carries `gundi:reference` annotations (helper
  `_reference` in `configurations.py`). Never set `ui:widget` on those nodes — old
  portals must keep plain text inputs. A drift test in
  `app/actions/tests/test_configurations.py` ties annotations to real actions.
- `annotations` config is now a list of `AnnotationFilter` rows (`term`, `values`);
  legacy JSON-string/dict configs are coerced by a pre-validator. Downstream code uses
  `action_config.annotations_dict` (the legacy `{term: [values]}` shape).
- `quality_grade` is a `List[Literal[...]]` — the enum must track
  `pyinaturalist.constants.QUALITY_GRADES` (drift-tested).
- Taxa dropdowns are deliberately absent: they need a typeahead extension to the
  reference-data contract (`/v1/taxa/autocomplete` exists but the portal can't pass
  typed text yet).
```

Also update the existing "Configuration" bullet that says `annotations` is accepted as a JSON-encoded string: reword to "accepted as structured rows; legacy JSON-encoded strings/dicts are coerced".

- [ ] **Step 2: Final full-suite run**

Run: `.venv/bin/python -m pytest -q`
Expected: all pass.

- [ ] **Step 3: Confirm requirements untouched**

Run: `git status --porcelain requirements.in requirements-base.in requirements-dev.in requirements.txt`
Expected: no output (constraint: no dependency changes; CI re-runs pip-compile).

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: reference actions, annotations reshape, and quality-grade enum"
```
