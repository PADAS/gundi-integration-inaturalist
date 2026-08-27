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
