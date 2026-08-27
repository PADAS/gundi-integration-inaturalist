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
