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
    config = PullEventsConfig(days_to_load=3, taxa="1", quality_grade=["Needs ID", "research"])
    assert config.quality_grade == ["needs_id", "research"]


def test_quality_grade_invalid_value_raises():
    with pytest.raises(pydantic.ValidationError):
        PullEventsConfig(days_to_load=3, quality_grade=[0])


def test_annotations_accepts_legacy_json_string():
    config = PullEventsConfig(days_to_load=3, taxa="1", annotations='{"22": ["24", "25"], "1": ["2"]}')
    assert [(f.term, f.values) for f in config.annotations] == [
        ("22", ["24", "25"]), ("1", ["2"]),
    ]
    assert config.annotations_dict == {"22": ["24", "25"], "1": ["2"]}


def test_annotations_accepts_legacy_dict_with_int_keys():
    config = PullEventsConfig(days_to_load=3, taxa="1", annotations={22: [24, 25]})
    assert config.annotations_dict == {"22": ["24", "25"]}


def test_annotations_accepts_structured_rows():
    config = PullEventsConfig(
        days_to_load=3, taxa="1", annotations=[{"term": "22", "values": ["24"]}]
    )
    assert config.annotations_dict == {"22": ["24"]}


def test_annotations_duplicate_term_rows_merge_values():
    config = PullEventsConfig(
        days_to_load=3,
        taxa="1",
        annotations=[
            {"term": "22", "values": ["24"]},
            {"term": "22", "values": ["25", "24"]},
        ],
    )
    assert config.annotations_dict == {"22": ["24", "25"]}


@pytest.mark.parametrize("raw", [None, "", "   ", "{}"])
def test_annotations_empty_inputs_mean_no_filter(raw):
    config = PullEventsConfig(days_to_load=3, taxa="1", annotations=raw)
    assert config.annotations_dict is None


def test_annotations_invalid_json_raises():
    with pytest.raises(pydantic.ValidationError):
        PullEventsConfig(days_to_load=3, taxa="1", annotations="{not json")


def test_annotations_schema_is_structured_rows():
    schema = PullEventsConfig.schema()
    prop = schema["properties"]["annotations"]
    assert prop["type"] == "array"
    row = schema["definitions"]["AnnotationFilter"]["properties"]
    assert row["term"]["type"] == "string"
    assert row["values"]["type"] == "array"


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
        "list_projects", "list_annotation_terms", "list_annotation_values", "list_taxa",
    }
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
            # The search param must be optional: widgets that predate search
            # fetch without it and must get an empty list, not a 422.
            assert not reference_actions[ref["action"]].__fields__[ref["search"]["param"]].required
            min_chars = ref["search"]["min_chars"]
            assert type(min_chars) is int and min_chars >= 1


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


def test_taxa_accepts_legacy_comma_string():
    config = PullEventsConfig(days_to_load=3, taxa="12345, 67890,  ,99")
    assert config.taxa == ["12345", "67890", "99"]
    assert config.taxa_str == "12345,67890,99"


def test_taxa_accepts_list_and_coerces_ints():
    config = PullEventsConfig(days_to_load=3, taxa=[12345, "67890"])
    assert config.taxa == ["12345", "67890"]
    assert config.taxa_str == "12345,67890"


def test_taxa_drops_duplicates_and_splits_list_items():
    config = PullEventsConfig(days_to_load=3, taxa=["1, 2", "1", 2, "3"])
    assert config.taxa == ["1", "2", "3"]


@pytest.mark.parametrize("raw", [["leopard"], "Panthera pardus", [True], [{"a": 1}], 1.5])
def test_taxa_rejects_values_that_are_not_ids(raw):
    """A typed name would be sent as taxon_id and fail every scheduled pull."""
    with pytest.raises(pydantic.ValidationError):
        PullEventsConfig(days_to_load=3, taxa=raw)


@pytest.mark.parametrize("raw", [None, "", "   ", []])
def test_taxa_empty_inputs_mean_no_filter(raw):
    config = PullEventsConfig(days_to_load=3, taxa=raw)
    assert config.taxa_str is None


def test_taxa_schema_is_string_array():
    schema = PullEventsConfig.schema()
    prop = schema["properties"]["taxa"]
    assert prop["type"] == "array"
    assert prop["items"]["type"] == "string"


def test_taxa_gundi_reference_is_a_search_annotation():
    ui = PullEventsConfig.ui_schema()
    taxa_ref = ui["taxa"]["items"]["gundi:reference"]
    assert taxa_ref["action"] == "list_taxa"
    assert taxa_ref["params"] == {}
    assert taxa_ref["search"] == {"param": "q", "min_chars": 2}


def test_bounding_box_requests_the_bbox_widget():
    ui = PullEventsConfig.ui_schema()
    assert ui["bounding_box"] == {"ui:widget": "bbox"}


def test_schema_requires_taxa_and_bounding_box_only_without_projects():
    """The portal enforces this rule from the registered schema: a project is
    enough on its own; without one, taxa and a bounding box are both required."""
    schema = PullEventsConfig.schema()
    assert schema["required"] == ["days_to_load"]
    assert schema["if"] == {
        "properties": {
            "projects": {"anyOf": [{"type": "null"}, {"type": "array", "maxItems": 0}]}
        }
    }
    assert schema["then"]["required"] == ["days_to_load", "taxa", "bounding_box"]
    assert schema["else"] == {"required": ["days_to_load"]}


@pytest.mark.parametrize("value,accepted", [
    ("[1, 1, 0, 0]", True),
    ("", False),
    ("  ", False),
])
def test_schema_requires_real_bounding_box_without_projects(value, accepted):
    """`required` only checks the key is present; the branch's pattern stops a
    blank box, which would otherwise mean a pull with no area filter."""
    import re

    rule = PullEventsConfig.schema()["then"]["properties"]["bounding_box"]
    assert rule["type"] == "string"
    # JSON-schema patterns are unanchored, like re.search.
    assert bool(re.search(rule["pattern"], value)) is accepted


def test_schema_requires_nonempty_taxa_without_projects():
    """Without a project, an empty taxa list must count as missing."""
    rule = PullEventsConfig.schema()["then"]["properties"]["taxa"]
    assert rule == {"type": "array", "minItems": 1}


@pytest.mark.parametrize("value,accepted", [
    ("12345", True),
    ("", False),
    (" 1", False),
    ("leopard", False),
    ("1,2", False),
])
def test_schema_taxa_items_must_be_numeric_ids(value, accepted):
    """The typeahead allows free text; the portal must reject typed names,
    which iNat would refuse on every scheduled pull."""
    import re

    pattern = PullEventsConfig.schema()["properties"]["taxa"]["items"]["pattern"]
    assert bool(re.search(pattern, value)) is accepted


def test_config_without_projects_or_taxa_still_parses():
    """The rule lives only in the portal schema; the runner keeps accepting
    existing configs so saved connections run unchanged."""
    config = PullEventsConfig(days_to_load=3, bounding_box="[1, 1, 0, 0]")
    assert config.projects is None and config.taxa is None


@pytest.mark.parametrize("projects", [[""], ["  "], ["123", ""], [" \t"]])
def test_blank_project_ids_are_rejected(projects):
    """A blank entry would count as a project in the schema rule yet be dropped
    by pyinaturalist, leaving a query with no project, taxa or area filter."""
    with pytest.raises(pydantic.ValidationError):
        PullEventsConfig(days_to_load=3, projects=projects)


def test_schema_rejects_blank_project_ids():
    """The portal validates against the generated schema, so blank entries must
    be excluded there too, not only at runtime."""
    items = PullEventsConfig.schema()["properties"]["projects"]["items"]
    assert items["type"] == "string"
    assert items["minLength"] == 1
    assert items["pattern"] == r"^\s*\S"


def test_project_ids_are_trimmed():
    config = PullEventsConfig(days_to_load=3, projects=[" 123 "])
    assert config.projects == ["123"]


def test_schema_explains_project_or_taxa_with_bounding_box_rule():
    """The portal shows the root description under the section heading and each
    field description under its field; conditional requirements get no star."""
    schema = PullEventsConfig.schema()
    assert schema["description"] == (
        "Brings iNaturalist observations in as events. "
        "Choose at least one project, or enter taxa IDs together with a bounding box."
    )
    props = schema["properties"]
    assert "Leave empty to filter by taxa and area instead." in props["projects"]["description"]
    assert "Required when no project is selected." in props["taxa"]["description"]
    assert props["bounding_box"]["description"] == (
        "Required when no project is selected. "
        "Draw a box on the map or enter its coordinates."
    )


def test_bounding_box_title_is_short():
    assert PullEventsConfig.schema()["properties"]["bounding_box"]["title"] == "Bounding box"


@pytest.mark.parametrize("taxa, bounding_box", [
    (None, None),
    ("", ""),
    (" , ", None),
    ([], None),
])
def test_rejects_config_with_no_project_taxa_or_bounding_box(taxa, bounding_box):
    with pytest.raises(pydantic.ValidationError, match="Choose at least one project"):
        PullEventsConfig(days_to_load=3, projects=[], taxa=taxa, bounding_box=bounding_box)


@pytest.mark.parametrize("filters", [
    {"projects": ["123"]},
    {"taxa": "12345"},
    {"bounding_box": "[1, 1, 0, 0]"},
])
def test_accepts_legacy_config_with_any_single_filter(filters):
    PullEventsConfig(days_to_load=3, **filters)


@pytest.mark.parametrize("taxa, has_filter", [
    (["12345"], True),
    (["12345", "678"], True),
    ([], False),
    ([" "], False),
])
def test_filter_check_accepts_taxa_as_a_list(taxa, has_filter):
    # The taxa field may hold a list of IDs instead of a comma-separated string.
    values = {"projects": [], "taxa": taxa, "bounding_box": None}
    if has_filter:
        assert PullEventsConfig.require_some_filter(values) == values
    else:
        with pytest.raises(ValueError, match="Choose at least one project"):
            PullEventsConfig.require_some_filter(values)
