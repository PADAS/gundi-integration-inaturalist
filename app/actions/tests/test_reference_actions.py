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


@pytest.mark.asyncio
async def test_reference_action_missing_required_param_is_422(
    mocker, inaturalist_integration_v2, mock_publish_event
):
    """A reference query with a required param and no overrides must fail
    pydantic validation (422 path), not 404 and not execute."""
    from app.actions.configurations import ListProjectsQuery
    from app.actions.handlers import action_list_projects

    action_runner = _patch_runner(
        mocker,
        {"list_projects": (action_list_projects, ListProjectsQuery, None)},
        inaturalist_integration_v2,
    )
    mocker.patch.object(action_runner, "publish_event", mock_publish_event)
    handle_error = mocker.patch.object(
        action_runner, "_handle_error", AsyncMock(return_value="validation-error-response")
    )

    result = await action_runner.execute_action(
        integration_id=str(inaturalist_integration_v2.id),
        action_id="list_projects",
    )

    assert result == "validation-error-response"
    import pydantic
    assert isinstance(handle_error.call_args.args[0], pydantic.ValidationError)


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
