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


def test_annotations_duplicate_term_rows_merge_values():
    config = PullEventsConfig(
        days_to_load=3,
        annotations=[
            {"term": "22", "values": ["24"]},
            {"term": "22", "values": ["25", "24"]},
        ],
    )
    assert config.annotations_dict == {"22": ["24", "25"]}


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
            assert isinstance(ref["search"].get("min_chars", 2), int)


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
