import datetime
import pydantic
import pytest
from unittest.mock import MagicMock, call
from app.conftest import async_return
from app.actions.configurations import PullEventsConfig
from app.datasource.inaturalist import get_observations, TAXA_BATCH_SIZE
from app.services.action_runner import execute_action


@pytest.mark.asyncio
async def test_execute_pull_observations_action(
        mocker, mock_gundi_client_v2, mock_state_manager, inaturalist_integration_v2,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, mock_config_manager,
        mock_get_observations_v2, mock_publish_event, mock_gundi_client_v2_class
):
    mock_config_manager.get_integration_details.return_value = async_return(inaturalist_integration_v2)
    mock_config_manager.get_action_configuration.return_value = async_return(inaturalist_integration_v2.configurations[0])
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mock_state_manager.get_state.return_value = async_return({})
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    mocker.patch("app.datasource.inaturalist.get_observations_v2", mock_get_observations_v2)

    response = await execute_action(
        integration_id=str(inaturalist_integration_v2.id),
        action_id="pull_events"
    )
    assert "result" in response
    assert response["result"].get("events_extracted") == 2
    assert response["result"].get("events_updated") == 0
    assert response["result"].get("photos_attached") == 5
    assert mock_get_observations_v2.called
    assert mock_get_observations_v2.call_count == 2


@pytest.mark.asyncio
async def test_execute_pull_observations_action_without_bounding_box(
        mocker, mock_gundi_client_v2, mock_state_manager, inaturalist_integration_v2_without_bounding_box,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, mock_config_manager,
        mock_get_observations_v2, mock_publish_event, mock_gundi_client_v2_class
):
    mock_config_manager.get_integration_details.return_value = async_return(inaturalist_integration_v2_without_bounding_box)
    mock_config_manager.get_action_configuration.return_value = async_return(inaturalist_integration_v2_without_bounding_box.configurations[0])
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mock_state_manager.get_state.return_value = async_return({})
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    mocker.patch("app.datasource.inaturalist.get_observations_v2", mock_get_observations_v2)

    response = await execute_action(
        integration_id=str(inaturalist_integration_v2_without_bounding_box.id),
        action_id="pull_events"
    )
    assert "result" in response
    assert response["result"].get("events_extracted") == 2
    assert response["result"].get("events_updated") == 0
    assert response["result"].get("photos_attached") == 5
    assert mock_get_observations_v2.called
    assert mock_get_observations_v2.call_count == 2


@pytest.mark.asyncio
async def test_execute_pull_observations_action_with_taxa_string(
        mocker, mock_gundi_client_v2, mock_state_manager, inaturalist_integration_v2_with_taxa_string,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, mock_config_manager,
        mock_get_observations_v2, mock_publish_event, mock_gundi_client_v2_class
):
    mock_config_manager.get_integration_details.return_value = async_return(inaturalist_integration_v2_with_taxa_string)
    mock_config_manager.get_action_configuration.return_value = async_return(inaturalist_integration_v2_with_taxa_string.configurations[0])
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mock_state_manager.get_state.return_value = async_return({})
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    mock_inat = mocker.patch("app.datasource.inaturalist.get_observations_v2", mock_get_observations_v2)

    response = await execute_action(
        integration_id=str(inaturalist_integration_v2_with_taxa_string.id),
        action_id="pull_events"
    )
    assert "result" in response
    assert mock_inat.called
    # taxa string "1633134, 1314810, 1128559" is a single batch — count call + 1 page call
    assert mock_inat.call_count == 2
    count_call_kwargs = mock_inat.call_args_list[0][1]
    assert count_call_kwargs["taxon_id"] == "1633134,1314810,1128559"


def test_taxa_validator_keeps_list():
    config = PullEventsConfig(taxa=["123", "456", "789"], days_to_load=3, event_prefix="iNat: ")
    assert config.taxa == ["123", "456", "789"]
    assert config.taxa_str == "123,456,789"


def test_taxa_validator_splits_string():
    config = PullEventsConfig(taxa="123, 456, 789", days_to_load=3, event_prefix="iNat: ")
    assert config.taxa == ["123", "456", "789"]
    assert config.taxa_str == "123,456,789"


def test_taxa_validator_handles_empty_list():
    config = PullEventsConfig(taxa=[], days_to_load=3, event_prefix="iNat: ")
    assert config.taxa is None
    assert config.taxa_str is None


def test_taxa_validator_handles_none():
    config = PullEventsConfig(taxa=None, days_to_load=3, event_prefix="iNat: ")
    assert config.taxa is None


def test_quality_grade_validator_accepts_valid_values():
    config = PullEventsConfig(quality_grade=["casual", "needs_id", "research"], days_to_load=3, event_prefix="iNat: ")
    assert config.quality_grade == ["casual", "needs_id", "research"]


def test_quality_grade_validator_normalizes_case_spaces_and_whitespace():
    config = PullEventsConfig(quality_grade=["needs id", "Research", " casual "], days_to_load=3, event_prefix="iNat: ")
    assert config.quality_grade == ["needs_id", "research", "casual"]


def test_quality_grade_validator_coerces_scalar_string_to_list():
    config = PullEventsConfig(quality_grade="research", days_to_load=3, event_prefix="iNat: ")
    assert config.quality_grade == ["research"]


def test_quality_grade_validator_coerces_comma_separated_string():
    config = PullEventsConfig(quality_grade="research, needs id", days_to_load=3, event_prefix="iNat: ")
    assert config.quality_grade == ["research", "needs_id"]


def test_quality_grade_validator_filters_empty_entries():
    config = PullEventsConfig(quality_grade=["research", ""], days_to_load=3, event_prefix="iNat: ")
    assert config.quality_grade == ["research"]


def test_quality_grade_validator_rejects_invalid_values():
    with pytest.raises(pydantic.ValidationError) as exc_info:
        PullEventsConfig(quality_grade=["reasearch"], days_to_load=3, event_prefix="iNat: ")
    assert "reasearch" in str(exc_info.value)
    assert "casual" in str(exc_info.value)


def test_quality_grade_validator_allows_none():
    config = PullEventsConfig(quality_grade=None, days_to_load=3, event_prefix="iNat: ")
    assert config.quality_grade is None


@pytest.mark.parametrize("bad_scalar", [0, False, 5, 0.0])
def test_quality_grade_validator_rejects_mistyped_scalars_with_clear_message(bad_scalar):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        PullEventsConfig(quality_grade=bad_scalar, days_to_load=3, event_prefix="iNat: ")
    message = str(exc_info.value)
    assert "must be one of" in message
    assert "not a valid list" not in message
    assert "not iterable" not in message


def test_quality_grade_validator_rejects_falsy_non_string_entries():
    with pytest.raises(pydantic.ValidationError) as exc_info:
        PullEventsConfig(quality_grade=[0], days_to_load=3, event_prefix="iNat: ")
    assert "0" in str(exc_info.value)


def test_quality_grade_validator_rejects_boolean_entries():
    with pytest.raises(pydantic.ValidationError):
        PullEventsConfig(quality_grade=[False], days_to_load=3, event_prefix="iNat: ")


def test_quality_grade_validator_drops_none_entries():
    config = PullEventsConfig(quality_grade=["research", None], days_to_load=3, event_prefix="iNat: ")
    assert config.quality_grade == ["research"]


def test_quality_grade_validator_coerces_empty_string_to_empty_list():
    config = PullEventsConfig(quality_grade="", days_to_load=3, event_prefix="iNat: ")
    assert config.quality_grade == []


def test_quality_grade_validator_coerces_whitespace_string_to_empty_list():
    config = PullEventsConfig(quality_grade="  ", days_to_load=3, event_prefix="iNat: ")
    assert config.quality_grade == []


def test_quality_grade_validator_allows_empty_list():
    config = PullEventsConfig(quality_grade=[], days_to_load=3, event_prefix="iNat: ")
    assert config.quality_grade == []


def test_quality_grades_constant_comes_from_pyinaturalist():
    from pyinaturalist.constants import QUALITY_GRADES as PYINAT_QUALITY_GRADES
    from app.actions.configurations import QUALITY_GRADES
    assert QUALITY_GRADES is PYINAT_QUALITY_GRADES


def test_get_observations_batches_taxa(mocker):
    mock_inat = mocker.patch("app.datasource.inaturalist.get_observations_v2")
    mock_inat.return_value = {"total_results": 0, "page": 1, "per_page": 0, "results": []}

    taxa_ids = [str(i) for i in range(250)]
    taxa_str = ",".join(taxa_ids)

    get_observations(datetime.datetime.now(), taxa=taxa_str)

    # 250 IDs → 3 batches (100, 100, 50) → 3 count calls (per_page=0), 0 page calls (total=0)
    assert mock_inat.call_count == 3
    taxon_id_args = [c[1]["taxon_id"] for c in mock_inat.call_args_list]
    assert taxon_id_args[0] == ",".join(taxa_ids[:TAXA_BATCH_SIZE])
    assert taxon_id_args[1] == ",".join(taxa_ids[TAXA_BATCH_SIZE:TAXA_BATCH_SIZE * 2])
    assert taxon_id_args[2] == ",".join(taxa_ids[TAXA_BATCH_SIZE * 2:])
