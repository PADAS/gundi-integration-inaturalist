# Taxa Typeahead Consumer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the iNaturalist `taxa` config field a typeahead multi-select: a `list_taxa` reference action wrapping iNat's autocomplete endpoint, plus the `search`-block ui_schema annotation from the typeahead contract extension.

**Architecture:** Stacks on branch `feature/reference-data-config` (PR #29 — the reference-action contract, handlers, and drift tests). `taxa` reshapes from a comma-separated string into `List[str]` in the schema (a dropdown attaches to array items); the existing list→string pre-validator inverts to string→list, and a `taxa_str` property keeps the datasource's string interface intact at the one call site. `action_list_taxa` follows the exact shape of the three existing reference actions.

**Tech Stack:** Python 3.10, pydantic v1, pyinaturalist 0.19.0 (`get_taxa_autocomplete` is a top-level export; public endpoint, no auth), pytest via `.venv/bin/python -m pytest` from the repo root.

**Spec:** `cdip:docs/superpowers/specs/2026-08-27-typeahead-reference-data-design.md` (branch `docs/typeahead-reference-data`, commit b4dc414), Sections 1 and 4. Patterns to follow live in this repo's `docs/superpowers/plans/2026-08-27-inat-reference-data-config-ui.md` and the code it produced.

## Global Constraints

- Base branch for this work: `feature/reference-data-config` (create `feature/taxa-typeahead` from it).
- No new dependencies — `requirements*` untouched.
- Backward compat is mandatory: stored configs with `taxa` as a comma-separated string (`"12345, 67890"`), a list, or empty must keep validating; `get_observations(taxa: Optional[str])` keeps its signature and still receives the comma-joined string.
- The `gundi:reference` annotation never sets `ui:widget`; the `search` block is `{"param": "q", "min_chars": 2}`; `q` is optional on the query model and an empty/absent `q` returns `options: []`, `truncated: true` (spec §4 — no default page for taxa).
- Reference actions use only unauthenticated endpoints.
- TDD throughout; commit after each task with the message given; end commit messages with:

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>

---

### Task 1: Reshape `taxa` to a list (string interface preserved downstream)

**Files:**
- Modify: `app/actions/configurations.py` (field, validator inversion, `taxa_str` property)
- Modify: `app/actions/handlers.py` (call site: `taxa=action_config.taxa_str`)
- Test: `app/actions/tests/test_configurations.py` (append)

**Interfaces:**
- Consumes: `PullEventsConfig`; `get_observations(..., taxa: Optional[str])` (unchanged).
- Produces: `PullEventsConfig.taxa: Optional[List[str]]`; `PullEventsConfig.taxa_str -> Optional[str]` property (comma-joined, `None` when empty). Task 4 annotates `taxa.items`.

- [ ] **Step 1: Write the failing tests**

Append to `app/actions/tests/test_configurations.py`:

```python
def test_taxa_accepts_legacy_comma_string():
    config = PullEventsConfig(days_to_load=3, taxa="12345, 67890,  ,99")
    assert config.taxa == ["12345", "67890", "99"]
    assert config.taxa_str == "12345,67890,99"


def test_taxa_accepts_list_and_coerces_ints():
    config = PullEventsConfig(days_to_load=3, taxa=[12345, "67890"])
    assert config.taxa == ["12345", "67890"]
    assert config.taxa_str == "12345,67890"


@pytest.mark.parametrize("raw", [None, "", "   ", []])
def test_taxa_empty_inputs_mean_no_filter(raw):
    config = PullEventsConfig(days_to_load=3, taxa=raw)
    assert config.taxa_str is None


def test_taxa_schema_is_string_array():
    schema = PullEventsConfig.schema()
    prop = schema["properties"]["taxa"]
    assert prop["type"] == "array"
    assert prop["items"]["type"] == "string"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest app/actions/tests/test_configurations.py -v -k taxa`
Expected: FAIL — `taxa_str` doesn't exist; the string field rejects nothing but coerces the wrong way.

- [ ] **Step 3: Implement the reshape**

In `app/actions/configurations.py`, change the field:

```python
    taxa: Optional[List[str]] = pydantic.Field(
        None,
        title="Taxa IDs",
        description=(
            "iNaturalist taxa IDs for which to load observations. Legacy comma-separated "
            "strings (e.g. '12345, 67890') are still accepted."
        ),
    )
```

Replace `coerce_taxa_list_to_str` with the inverse coercion:

```python
    @pydantic.validator("taxa", pre=True, always=True)
    def coerce_taxa_to_list(cls, v):
        # Legacy configs stored this as a comma-separated string; the portal now
        # submits a list. Coerce both (and scalars) into a list of id strings.
        if v is None:
            return None
        if isinstance(v, str):
            v = v.split(",")
        elif not isinstance(v, (list, tuple, set)):
            v = [v]
        cleaned = [str(t).strip() for t in v if t is not None and str(t).strip()]
        return cleaned or None
```

Add the property next to `annotations_dict`:

```python
    @property
    def taxa_str(self) -> Optional[str]:
        """The comma-joined string shape the datasource consumes."""
        if not self.taxa:
            return None
        return ",".join(self.taxa)
```

In `app/actions/handlers.py`, change the `get_observations(...)` kwarg to:

```python
        taxa=action_config.taxa_str,
```

- [ ] **Step 4: Run the full suite**

Run: `.venv/bin/python -m pytest`
Expected: all pass. Known touchpoints if anything fails: the conftest fixture `inaturalist_integration_v2_with_taxa_string` (its stored string value must now parse to a list — the coercion covers it) and any test asserting `config.taxa` equals a string — update those assertions to the list shape, never the validator.

- [ ] **Step 5: Commit**

```bash
git add app/actions/configurations.py app/actions/handlers.py app/actions/tests/test_configurations.py
git commit -m "feat: taxa config becomes a string list (legacy comma-string configs still accepted)"
```

---

### Task 2: Datasource `search_taxa` wrapper

**Files:**
- Modify: `app/datasource/inaturalist.py`
- Test: `app/datasource/tests/test_inaturalist.py` (append)

**Interfaces:**
- Consumes: `pyinaturalist.get_taxa_autocomplete` (verify import: `.venv/bin/python -c "from pyinaturalist import get_taxa_autocomplete"`).
- Produces: `search_taxa(q: str) -> Dict` (raw response with `total_results`/`results`). Task 3's handler imports it by name.

- [ ] **Step 1: Write the failing test**

Append to `app/datasource/tests/test_inaturalist.py`:

```python
def test_search_taxa_passes_query(mocker):
    from app.datasource import inaturalist

    get_taxa_autocomplete = mocker.patch.object(
        inaturalist, "get_taxa_autocomplete", return_value={"total_results": 0, "results": []}
    )
    inaturalist.search_taxa("leo")
    get_taxa_autocomplete.assert_called_once_with(q="leo")
```

- [ ] **Step 2: Run it to verify it fails**

Run: `.venv/bin/python -m pytest app/datasource/tests/test_inaturalist.py -v -k search_taxa`
Expected: FAIL — `search_taxa` doesn't exist.

- [ ] **Step 3: Implement**

In `app/datasource/inaturalist.py`, add `get_taxa_autocomplete` to the existing `from pyinaturalist import (...)` block, and add below `search_projects_near`:

```python
def search_taxa(q: str) -> Dict:
    """iNaturalist taxa matching a typed query (public autocomplete endpoint)."""
    return get_taxa_autocomplete(q=q)
```

- [ ] **Step 4: Run the datasource tests — expect PASS**

Run: `.venv/bin/python -m pytest app/datasource/tests/test_inaturalist.py -v`

- [ ] **Step 5: Commit**

```bash
git add app/datasource/inaturalist.py app/datasource/tests/test_inaturalist.py
git commit -m "feat: datasource lookup for taxa autocomplete"
```

---

### Task 3: `ListTaxaQuery` + `action_list_taxa`

**Files:**
- Modify: `app/actions/configurations.py` (query model)
- Modify: `app/actions/handlers.py` (handler + import)
- Test: `app/actions/tests/test_reference_actions.py` (append)

**Interfaces:**
- Consumes: `ReferenceActionConfiguration`, `ReferenceOption`, `ReferenceDataResponse` (existing); `search_taxa` (Task 2).
- Produces: `ListTaxaQuery(q: Optional[str])` in `configurations.py`; `action_list_taxa` in `handlers.py` returning a `ReferenceDataResponse` dict. Task 4's annotation names `list_taxa` and param `q`.

- [ ] **Step 1: Write the failing tests**

Append to `app/actions/tests/test_reference_actions.py`:

```python
TAXA_RESPONSE = {
    "total_results": 40,
    "results": [
        {"id": 41955, "name": "Panthera pardus", "preferred_common_name": "Leopard", "rank": "species"},
        {"id": 41963, "name": "Panthera", "rank": "genus"},
    ],
}


@pytest.mark.asyncio
async def test_list_taxa_labels_and_truncation(mocker, inaturalist_integration_v2):
    from app.actions import handlers
    from app.actions.configurations import ListTaxaQuery

    search = mocker.patch.object(handlers, "search_taxa", return_value=TAXA_RESPONSE)

    result = await handlers.action_list_taxa(
        inaturalist_integration_v2, ListTaxaQuery(q="leopard")
    )

    search.assert_called_once_with("leopard")
    assert [(o["value"], o["label"], o["description"]) for o in result["options"]] == [
        ("41955", "Leopard (Panthera pardus)", "species"),
        ("41963", "Panthera", "genus"),
    ]
    assert result["truncated"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("q", [None, "", "   "])
async def test_list_taxa_empty_query_returns_no_default_page(mocker, inaturalist_integration_v2, q):
    from app.actions import handlers
    from app.actions.configurations import ListTaxaQuery

    search = mocker.patch.object(handlers, "search_taxa")

    result = await handlers.action_list_taxa(inaturalist_integration_v2, ListTaxaQuery(q=q))

    search.assert_not_called()
    assert result["options"] == []
    assert result["truncated"] is True
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/python -m pytest app/actions/tests/test_reference_actions.py -v -k taxa`
Expected: ImportError on `ListTaxaQuery` / `action_list_taxa`.

- [ ] **Step 3: Implement**

In `app/actions/configurations.py`, next to the other query models:

```python
class ListTaxaQuery(ReferenceActionConfiguration):
    """Reference query: iNaturalist taxa matching a typed search (typeahead).

    q is optional by contract convention (typeahead spec §1): a widget that
    predates search support fetches without it and must get a clean empty
    response, never a 422.
    """
    q: Optional[str] = pydantic.Field(None, title="Search text")
```

In `app/actions/handlers.py`, extend the configurations import with `ListTaxaQuery` and the datasource import with `search_taxa`, then add below `action_list_annotation_values`:

```python
async def action_list_taxa(integration: Integration, action_config: ListTaxaQuery):
    """Reference action (typeahead): taxa matching the typed query.

    The taxa vocabulary is far too large for a default page, so an empty query
    returns no options with truncated=True — the portal's search widget only
    fetches once the operator has typed, and old widgets get a clean empty list.
    """
    query = (action_config.q or "").strip()
    if not query:
        return ReferenceDataResponse(options=[], truncated=True).dict()
    response = search_taxa(query)
    results = response.get("results", [])
    options = []
    for taxon in results:
        if taxon.get("id") is None:
            continue
        scientific = taxon.get("name") or str(taxon["id"])
        common = taxon.get("preferred_common_name")
        options.append(ReferenceOption(
            value=str(taxon["id"]),
            label=f"{common} ({scientific})" if common else scientific,
            description=taxon.get("rank"),
        ))
    truncated = response.get("total_results", len(results)) > len(results)
    return ReferenceDataResponse(options=options, truncated=truncated).dict()
```

- [ ] **Step 4: Run the reference-action tests — expect PASS, then the full suite**

Run: `.venv/bin/python -m pytest app/actions/tests/test_reference_actions.py -v` then `.venv/bin/python -m pytest`
Expected: all pass EXCEPT the drift test `test_gundi_reference_annotations_match_registered_reference_actions`, which pins the exact action set — it does not fail yet (the annotation set is still the old three and `list_taxa` carries no annotation until Task 4), but if it asserts the discovered-reference-action set anywhere, update expectations in Task 4, not here.

- [ ] **Step 5: Commit**

```bash
git add app/actions/configurations.py app/actions/handlers.py app/actions/tests/test_reference_actions.py
git commit -m "feat: list_taxa typeahead reference action"
```

---

### Task 4: `search` annotation on taxa + drift-test extension + docs

**Files:**
- Modify: `app/actions/configurations.py` (`_reference` search support + ui_schema override)
- Modify: `app/actions/tests/test_configurations.py` (drift test + node test)
- Modify: `CLAUDE.md` (taxa convention rewrite)

**Interfaces:**
- Consumes: `_reference(action, params)` helper, `PullEventsConfig.ui_schema()` override, the drift test added by the reference-data plan.
- Produces: `taxa.items` annotated with `search: {"param": "q", "min_chars": 2}`; drift test also validates `search.param` against query models.

- [ ] **Step 1: Extend the failing tests**

In `app/actions/tests/test_configurations.py`, update the drift test's expected action set to include `"list_taxa"`, and extend the per-annotation loop with search validation:

```python
    for node, ref in found:
        assert ref["target"] == "self"
        assert ref["allow_free_text"] is True
        assert ref["action"] in reference_actions
        query_fields = set(reference_actions[ref["action"]].__fields__)
        assert set(ref.get("params", {})) <= query_fields
        assert "ui:widget" not in node
        if "search" in ref:
            assert ref["search"]["param"] in query_fields
            assert ref["search"]["param"] not in ref.get("params", {})
            assert isinstance(ref["search"].get("min_chars", 2), int)
```

And append a node-placement test:

```python
def test_taxa_gundi_reference_is_a_search_annotation():
    ui = PullEventsConfig.ui_schema()
    taxa_ref = ui["taxa"]["items"]["gundi:reference"]
    assert taxa_ref["action"] == "list_taxa"
    assert taxa_ref["params"] == {}
    assert taxa_ref["search"] == {"param": "q", "min_chars": 2}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/python -m pytest app/actions/tests/test_configurations.py -v`
Expected: the drift test FAILS (action-set mismatch) and the node test FAILS (`KeyError: 'taxa'`).

- [ ] **Step 3: Implement**

Extend `_reference` in `app/actions/configurations.py` with an optional search argument (keep the docstring's no-ui:widget note):

```python
def _reference(action: str, params: Optional[dict] = None, search: Optional[dict] = None) -> dict:
```

body:

```python
    annotation = {
        "action": action,
        "target": "self",
        "params": params or {},
        "allow_free_text": True,
    }
    if search:
        annotation["search"] = search
    return annotation
```

In `PullEventsConfig.ui_schema()`, add before `return base`:

```python
        base["taxa"] = {"items": {"gundi:reference": _reference(
            "list_taxa", search={"param": "q", "min_chars": 2},
        )}}
```

- [ ] **Step 4: Update CLAUDE.md**

In the Configuration section, replace the `taxa` bullet ("taxa is a **comma-separated string**, not a list...") with:

```markdown
- `taxa` is a **list of ID strings** in the config model; a pydantic pre-validator coerces
  legacy comma-separated strings (and scalars). The datasource boundary stays a string:
  downstream code uses `action_config.taxa_str` (comma-joined), and `get_observations`
  is unchanged.
```

In the "Reference actions" section, replace the "Taxa dropdowns are deliberately absent" bullet with:

```markdown
- `taxa` is a typeahead: `action_list_taxa` (wrapping `/v1/taxa/autocomplete`) plus a
  `search: {param: "q", min_chars: 2}` annotation per the typeahead contract extension
  (`cdip:docs/superpowers/specs/2026-08-27-typeahead-reference-data-design.md`). `q` is
  optional; empty query returns no options with `truncated: true`.
```

- [ ] **Step 5: Run the full suite**

Run: `.venv/bin/python -m pytest`
Expected: all pass; requirements files untouched (`git status --porcelain requirements*` → empty).

- [ ] **Step 6: Commit**

```bash
git add app/actions/configurations.py app/actions/tests/test_configurations.py CLAUDE.md
git commit -m "feat: taxa typeahead annotation (search block) with drift-test coverage"
```
