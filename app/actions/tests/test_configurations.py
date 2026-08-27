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
