"""Unit tests for app.datasource.inaturalist."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from app.datasource.inaturalist import (
    OBSERVATION_FIELDS,
    get_observations,
    _match_annotations_to_config,
)


# --- _match_annotations_to_config (via get_observations or direct if we expose it) ---
# We test the filtering behavior via get_observations with mocked API + annotations param.


def _annotation(term: int, value: int):
    """Minimal annotation-like object with .term and .value."""
    return MagicMock(term=term, value=value)


def test_match_annotations_to_config_empty_annotations_empty_config():
    assert _match_annotations_to_config([], {}) is True


def test_match_annotations_to_config_empty_annotations_non_empty_config():
    assert _match_annotations_to_config([], {"22": ["24"]}) is False


def test_match_annotations_to_config_matching():
    annotations = [
        _annotation(22, 24),
        _annotation(1, 2),
    ]
    config = {"22": [24], "1": [2]}
    assert _match_annotations_to_config(annotations, config) is True


def test_match_annotations_to_config_string_keys_and_values():
    """Config from JSON has string keys/values; annotation term/value may be int."""
    annotations = [_annotation(22, 24), _annotation(1, 2)]
    config = {"22": ["24"], "1": ["2"]}
    assert _match_annotations_to_config(annotations, config) is True


def test_match_annotations_to_config_missing_term():
    annotations = [_annotation(22, 24)]
    config = {"22": [24], "1": [2]}
    assert _match_annotations_to_config(annotations, config) is False


def test_match_annotations_to_config_missing_value():
    annotations = [_annotation(22, 24), _annotation(1, 99)]
    config = {"22": [24], "1": [2]}
    assert _match_annotations_to_config(annotations, config) is False


# --- get_observations ---


def test_get_observations_empty_result(mocker):
    mocker.patch("app.datasource.inaturalist.get_observations_v2", return_value={"total_results": 0, "results": []})
    result = get_observations(datetime(2024, 1, 1, tzinfo=timezone.utc))
    assert result == {}


def test_get_observations_one_page(mocker):
    from pyinaturalist import Observation

    obs_dict = {
        "id": 100,
        "observed_on": "2024-06-01",
        "created_at": "2024-06-01T12:00:00+00:00",
        "updated_at": "2024-06-01T12:00:00+00:00",
        "captive": False,
        "obscured": False,
        "quality_grade": "research",
        "species_guess": "Bird",
        "uri": "https://www.inaturalist.org/observations/100",
        "photos": [],
        "user": None,
        "location": None,
        "place_ids": [],
        "taxon": None,
        "annotations": [],
    }
    mock_response = {"total_results": 1, "page": 1, "per_page": 200, "results": [obs_dict]}

    def side_effect(**kwargs):
        if kwargs.get("per_page") == 0:
            return {"total_results": 1, "results": []}
        return mock_response

    mocker.patch("app.datasource.inaturalist.get_observations_v2", side_effect=side_effect)
    result = get_observations(datetime(2024, 1, 1, tzinfo=timezone.utc))
    assert len(result) == 1
    assert 100 in result
    assert isinstance(result[100], Observation)


def test_get_observations_passes_bounding_box_and_filters(mocker):
    call_params = []

    def capture(**kwargs):
        call_params.append(kwargs.copy())
        if kwargs.get("per_page") == 0:
            return {"total_results": 0, "results": []}
        return {"total_results": 0, "results": []}

    mocker.patch("app.datasource.inaturalist.get_observations_v2", side_effect=capture)
    get_observations(
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        bounding_box=[10.0, 20.0, -10.0, -20.0],
        taxa="123, 456",
        projects=["p1"],
        quality_grade=["research"],
    )
    assert len(call_params) >= 1
    first = call_params[0]
    assert first.get("nelat") == 10.0
    assert first.get("nelng") == 20.0
    assert first.get("swlat") == -10.0
    assert first.get("swlng") == -20.0
    assert first.get("taxon_id") == "123,456"
    assert first.get("project_id") == ["p1"]
    assert first.get("quality_grade") == ["research"]


def test_get_observations_annotation_filter_includes_only_matching(mocker):
    from pyinaturalist import Observation

    obs_with_annot = {
        "id": 1,
        "observed_on": "2024-06-01",
        "created_at": "2024-06-01T12:00:00+00:00",
        "updated_at": "2024-06-01T12:00:00+00:00",
        "captive": False,
        "obscured": False,
        "quality_grade": "research",
        "species_guess": "Bird",
        "uri": "https://www.inaturalist.org/observations/1",
        "photos": [],
        "user": None,
        "location": None,
        "place_ids": [],
        "taxon": None,
        "annotations": [
            {"controlled_attribute_id": 22, "controlled_value_id": 24},
        ],
    }
    obs_without_annot = {**obs_with_annot, "id": 2, "annotations": []}
    mock_response = {
        "total_results": 2,
        "page": 1,
        "per_page": 200,
        "results": [obs_with_annot, obs_without_annot],
    }

    def side_effect(**kwargs):
        if kwargs.get("per_page") == 0:
            return {"total_results": 2, "results": []}
        return mock_response

    mocker.patch("app.datasource.inaturalist.get_observations_v2", side_effect=side_effect)
    result = get_observations(
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        annotations={"22": [24]},
    )
    # Only the observation with matching annotation (22=24) should be included
    assert len(result) == 1
    assert 1 in result


def test_get_observations_windows_past_10k_pagination_cap(mocker):
    """iNat rejects pagination past 10,000 results (page * per_page > 10,000 -> 500).

    When a query window matches more than 10,000 observations, get_observations
    must advance updated_since past the last fetched observation and keep going,
    instead of requesting page 51 and crashing.
    """
    from datetime import timedelta

    import requests

    total = 10_400
    base = datetime(2026, 6, 19, tzinfo=timezone.utc)

    def make_obs(i):
        return {
            "id": i,
            "updated_at": (base + timedelta(seconds=i)).isoformat(),
            "photos": [],
            "annotations": [],
        }

    all_obs = [make_obs(i) for i in range(1, total + 1)]

    def side_effect(**kwargs):
        page = kwargs.get("page", 1)
        per_page = kwargs.get("per_page")
        if per_page and page * per_page > 10_000:
            raise requests.exceptions.HTTPError(
                "500 Server Error: Internal Server Error"
            )
        since = kwargs["updated_since"]
        matching = [
            o for o in all_obs
            if datetime.fromisoformat(o["updated_at"]) >= since
        ]
        if per_page == 0:
            return {"total_results": len(matching), "results": []}
        start = (page - 1) * per_page
        return {
            "total_results": len(matching),
            "page": page,
            "per_page": per_page,
            "results": matching[start:start + per_page],
        }

    mocker.patch("app.datasource.inaturalist.get_observations_v2", side_effect=side_effect)
    result = get_observations(base)
    assert len(result) == total
    assert set(result.keys()) == set(range(1, total + 1))


def test_get_observations_advances_past_pathological_shared_timestamp(mocker):
    """When 10,000+ observations share the exact updated_since timestamp, the
    overflow within that second is unreachable (iNat can't paginate past 10k),
    but fetching must still advance past the timestamp and load the rest of
    the backlog instead of stopping — otherwise the persisted cursor stays
    pinned and every run re-fetches the same 50 pages.
    """
    from datetime import timedelta

    import requests

    base = datetime(2026, 6, 19, tzinfo=timezone.utc)
    shared_ts = 10_400  # all at updated_at == base (the cursor timestamp)
    later = 300  # strictly after the pathological second

    def make_obs(i, updated_at):
        return {
            "id": i,
            "updated_at": updated_at.isoformat(),
            "photos": [],
            "annotations": [],
        }

    all_obs = [make_obs(i, base) for i in range(1, shared_ts + 1)]
    all_obs += [
        make_obs(shared_ts + i, base + timedelta(seconds=i))
        for i in range(1, later + 1)
    ]

    def side_effect(**kwargs):
        page = kwargs.get("page", 1)
        per_page = kwargs.get("per_page")
        if per_page and page * per_page > 10_000:
            raise requests.exceptions.HTTPError(
                "500 Server Error: Internal Server Error"
            )
        since = kwargs["updated_since"]
        matching = [
            o for o in all_obs
            if datetime.fromisoformat(o["updated_at"]) >= since
        ]
        if per_page == 0:
            return {"total_results": len(matching), "results": []}
        start = (page - 1) * per_page
        return {
            "total_results": len(matching),
            "page": page,
            "per_page": per_page,
            "results": matching[start:start + per_page],
        }

    mocker.patch("app.datasource.inaturalist.get_observations_v2", side_effect=side_effect)
    result = get_observations(base)
    # The first 10,000 of the shared-timestamp observations plus every later
    # observation; only the 400 overflow within the shared second are skipped.
    assert len(result) == 10_000 + later
    assert all(shared_ts + i in result for i in range(1, later + 1))


def test_observation_fields_constant():
    assert "id" in OBSERVATION_FIELDS
    assert "observed_on" in OBSERVATION_FIELDS
    assert "taxon.id" in OBSERVATION_FIELDS
