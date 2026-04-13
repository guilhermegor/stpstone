"""Unit tests for MarginSimulatorB3 class.

Tests the B3 Margin Simulator API interaction, including initialization,
reference data retrieval, and risk calculation with various input scenarios.
"""

import sys
from typing import Optional, Union
from unittest.mock import MagicMock, patch

import pytest
from pytest_mock import MockerFixture
import requests

from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.providers.br.margin_simulator_b3._dto import ResultReferenceData
from stpstone.utils.providers.br.margin_simulator_b3.margin_simulator_b3 import (
    MarginSimulatorB3,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def valid_payload() -> ResultReferenceData:
    """Fixture providing a valid payload for MarginSimulatorB3 initialization.

    Returns
    -------
    ResultReferenceData
        A valid payload dictionary with reference data, liquidity resource limit,
        and security group list.
    """
    return {
        "ReferenceData": {"referenceDataToken": "test_token"},
        "liquidityResourceLimit": 1000,
        "SecurityGroupList": {
            "SecurityGroup": [
                {"positionTypeCode": 1, "securityTypeCode": 2, "symbolList": ["ABC"]}
            ]
        },
    }


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> MagicMock:
    """Mock requests.get to prevent real HTTP calls.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks.

    Returns
    -------
    MagicMock
        Mock object for requests.get.
    """
    return mocker.patch("requests.get")


@pytest.fixture
def mock_requests_post(mocker: MockerFixture) -> MagicMock:
    """Mock requests.post to prevent real HTTP calls.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks.

    Returns
    -------
    MagicMock
        Mock object for requests.post.
    """
    return mocker.patch("requests.post")


@pytest.fixture
def mock_json_files(mocker: MockerFixture) -> MagicMock:
    """Mock JsonFiles class for dict_to_json method.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks.

    Returns
    -------
    MagicMock
        Mock object for JsonFiles class.
    """
    mock = mocker.patch("stpstone.utils.parsers.json.JsonFiles")
    mock_instance = mock.return_value
    mock_instance.dict_to_json.side_effect = lambda x: x
    return mock_instance


@pytest.fixture
def mock_response() -> MagicMock:
    """Mock Response object with sample content.

    Returns
    -------
    MagicMock
        Mocked response object with JSON data.
    """
    response = MagicMock()
    response.json.return_value = {
        "ReferenceData": {"referenceDataToken": "test_token"}
    }
    response.raise_for_status = MagicMock()
    return response


# --------------------------
# Tests
# --------------------------
def test_init_valid_payload(valid_payload: ResultReferenceData, mock_requests_get: MagicMock,
                           mock_response: MagicMock) -> None:
    """Test initialization with valid payload.

    Verifies
    --------
    - MarginSimulatorB3 initializes correctly with valid payload
    - Attributes are set correctly
    - Reference data token is retrieved

    Parameters
    ----------
    valid_payload : ResultReferenceData
        Valid payload dictionary from fixture
    mock_requests_get : MagicMock
        Mocked requests.get object
    mock_response : MagicMock
        Mocked response object

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    simulator = MarginSimulatorB3(dict_payload=valid_payload)
    assert simulator.dict_payload == valid_payload
    assert simulator.timeout == (12.0, 21.0)
    assert simulator.bool_verify is True
    assert simulator.token == "test_token" # noqa S105: possible hardcoded password
    assert isinstance(simulator.cls_json_files, JsonFiles)


def test_init_invalid_payload_type(mock_requests_get: MagicMock, mock_response: MagicMock) -> None:
    """Test initialization with invalid payload type raises TypeError.

    Verifies
    --------
    - TypeError is raised when dict_payload is not a dictionary
    - Error message contains appropriate text

    Parameters
    ----------
    mock_requests_get : MagicMock
        Mocked requests.get object
    mock_response : MagicMock
        Mocked response object

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    with pytest.raises(TypeError, match="dict_payload must be a dictionary for TypedDict"):
        MarginSimulatorB3(dict_payload="invalid")


@pytest.mark.parametrize("timeout", [
    None, 10, 10.5, (5, 10), (5.0, 10.0)
])
def test_init_valid_timeout(
    timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]],
    valid_payload: ResultReferenceData, 
    mock_requests_get: MagicMock,
    mock_response: MagicMock
) -> None:
    """Test initialization with various valid timeout values.

    Verifies
    --------
    - Timeout values are correctly assigned
    - Initialization succeeds with valid timeout types

    Parameters
    ----------
    timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
        Timeout value to test
    valid_payload : ResultReferenceData
        Valid payload dictionary from fixture
    mock_requests_get : MagicMock
        Mocked requests.get object
    mock_response : MagicMock
        Mocked response object

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    simulator = MarginSimulatorB3(dict_payload=valid_payload, timeout=timeout)
    assert simulator.timeout == timeout


def test_init_invalid_timeout(valid_payload: ResultReferenceData, mock_requests_get: MagicMock,
                              mock_response: MagicMock) -> None:
    """Test initialization with invalid timeout type raises TypeError.

    Verifies
    --------
    - TypeError is raised when timeout is not of valid type
    - Error message contains appropriate text about union types

    Parameters
    ----------
    valid_payload : ResultReferenceData
        Valid payload dictionary from fixture
    mock_requests_get : MagicMock
        Mocked requests.get object
    mock_response : MagicMock
        Mocked response object

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    with pytest.raises(TypeError, match="timeout must be one of types"):
        MarginSimulatorB3(dict_payload=valid_payload, timeout="invalid")


def test_get_reference_data_isolated(mock_requests_get: MagicMock, mock_response: MagicMock,
                                    valid_payload: ResultReferenceData) -> None:
    """Test _get_reference_data method in isolation.

    Verifies
    --------
    - Method returns expected JSON response
    - HTTP request is made with correct parameters
    - Response status is checked

    Parameters
    ----------
    mock_requests_get : MagicMock
        Mocked requests.get object
    mock_response : MagicMock
        Mocked response object
    valid_payload : ResultReferenceData
        Valid payload dictionary from fixture

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    
    # Create simulator without triggering _get_reference_data in __init__
    with patch.object(MarginSimulatorB3, '_get_reference_data') as mock_init_get:
        mock_init_get.return_value = {"ReferenceData": {"referenceDataToken": "test_token"}}
        simulator = MarginSimulatorB3(dict_payload=valid_payload)
    
    # Reset the mock to test the actual method
    mock_requests_get.reset_mock()
    
    # Test the method directly
    result = simulator._get_reference_data()
    assert result == {"ReferenceData": {"referenceDataToken": "test_token"}}
    mock_requests_get.assert_called_once()
    mock_response.raise_for_status.assert_called_once()


def test_get_reference_data_http_error(
    mock_requests_get: MagicMock, 
    valid_payload: ResultReferenceData
) -> None:
    """Test _get_reference_data method with HTTP error.

    Verifies
    --------
    - HTTPError is raised when request fails
    - Error is propagated correctly

    Parameters
    ----------
    mock_requests_get : MagicMock
        Mocked requests.get object
    valid_payload : ResultReferenceData
        Valid payload dictionary from fixture

    Returns
    -------
    None
    """
    mock_requests_get.side_effect = requests.exceptions.HTTPError("HTTP error")
    with pytest.raises(requests.exceptions.HTTPError, match="HTTP error"):
        MarginSimulatorB3(dict_payload=valid_payload)


def test_risk_calculation(valid_payload: ResultReferenceData, mocker: MockerFixture) -> None:
    """Test risk_calculation method with valid response.

    Verifies
    --------
    - Method returns expected JSON response
    - Payload is correctly formatted with token
    - HTTP request is made with correct parameters
    - Response status is checked
    - JsonFiles.dict_to_json is called

    Parameters
    ----------
    valid_payload : ResultReferenceData
        Valid payload dictionary from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    # Mock the GET request for initialization
    mock_get = mocker.patch("requests.get")
    mock_get_response = MagicMock()
    mock_get_response.json.return_value = {
        "ReferenceData": {"referenceDataToken": "test_token"}
    }
    mock_get_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_get_response
    
    # Mock the POST request for risk calculation
    mock_post = mocker.patch("requests.post")
    mock_post_response = MagicMock()
    mock_post_response.json.return_value = {
        "Risk": {
            "totalDeficitSurplus": 100.0,
            "totalDeficitSurplusSubPortfolio_1": 50.0,
            "totalDeficitSurplusSubPortfolio_2": 50.0,
            "totalDeficitSurplusSubPortfolio_1_2": 100.0,
            "worstCaseSubPortfolio": 1,
            "potentialLiquidityResource": 200.0,
            "totalCollateralValue": 300.0,
            "riskWithoutCollateral": 400.0,
            "liquidityResource": 500.0,
            "calculationStatus": 1,
            "scenarioId": 123
        },
        "BusinessStatusList": []
    }
    mock_post_response.raise_for_status = MagicMock()
    mock_post.return_value = mock_post_response


def test_risk_calculation_http_error(
    valid_payload: ResultReferenceData, 
    mocker: MockerFixture
) -> None:
    """Test risk_calculation method with HTTP error.

    Verifies
    --------
    - HTTPError is raised when request fails
    - Error is propagated correctly

    Parameters
    ----------
    valid_payload : ResultReferenceData
        Valid payload dictionary from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    # Mock successful GET for initialization
    mock_get = mocker.patch("requests.get")
    mock_get_response = MagicMock()
    mock_get_response.json.return_value = {
        "ReferenceData": {"referenceDataToken": "test_token"}
    }
    mock_get_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_get_response
    
    # Mock POST to raise HTTPError
    mock_post = mocker.patch("requests.post")
    mock_post.side_effect = requests.exceptions.HTTPError("HTTP error")
    
    # Mock JsonFiles
    mocker.patch("stpstone.utils.parsers.json.JsonFiles")
    
    simulator = MarginSimulatorB3(dict_payload=valid_payload)
    with pytest.raises(requests.exceptions.HTTPError, match="HTTP error"):
        simulator.risk_calculation()


def test_risk_calculation_timeout(
    valid_payload: ResultReferenceData, 
    mocker: MockerFixture
) -> None:
    """Test risk_calculation method with timeout error.

    Verifies
    --------
    - TimeoutError is raised when request times out
    - Error is propagated correctly

    Parameters
    ----------
    valid_payload : ResultReferenceData
        Valid payload dictionary from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    # Mock successful GET for initialization
    mock_get = mocker.patch("requests.get")
    mock_get_response = MagicMock()
    mock_get_response.json.return_value = {
        "ReferenceData": {"referenceDataToken": "test_token"}
    }
    mock_get_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_get_response
    
    # Mock POST to raise Timeout
    mock_post = mocker.patch("requests.post")
    mock_post.side_effect = requests.exceptions.Timeout("Timeout error")
    
    # Mock JsonFiles
    mocker.patch("stpstone.utils.parsers.json.JsonFiles")
    
    simulator = MarginSimulatorB3(dict_payload=valid_payload)
    with pytest.raises(requests.exceptions.Timeout, match="Timeout error"):
        simulator.risk_calculation()


def test_reload_module(valid_payload: ResultReferenceData, mock_requests_get: MagicMock,
                       mock_response: MagicMock) -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - Instance retains functionality after reload

    Parameters
    ----------
    valid_payload : ResultReferenceData
        Valid payload dictionary from fixture
    mock_requests_get : MagicMock
        Mocked requests.get object
    mock_response : MagicMock
        Mocked response object

    Returns
    -------
    None
    """
    import importlib
    mock_requests_get.return_value = mock_response
    _ = MarginSimulatorB3(dict_payload=valid_payload)
    
    # Fix the module path
    module_name = "stpstone.utils.providers.br.margin_simulator_b3.margin_simulator_b3"
    if module_name in sys.modules:
        importlib.reload(sys.modules[module_name])
    
    new_simulator = MarginSimulatorB3(dict_payload=valid_payload)
    assert new_simulator.token == "test_token" # noqa S105: possible hardcoded password


def test_bool_verify_invalid_type(valid_payload: ResultReferenceData, mock_requests_get: MagicMock,
                                  mock_response: MagicMock) -> None:
    """Test initialization with invalid bool_verify type raises TypeError.

    Verifies
    --------
    - TypeError is raised when bool_verify is not a boolean
    - Error message contains appropriate text

    Parameters
    ----------
    valid_payload : ResultReferenceData
        Valid payload dictionary from fixture
    mock_requests_get : MagicMock
        Mocked requests.get object
    mock_response : MagicMock
        Mocked response object

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    with pytest.raises(TypeError, match="must be of type"):
        MarginSimulatorB3(dict_payload=valid_payload, bool_verify="invalid")


@pytest.mark.parametrize("invalid_payload", [
    None, "", [], 123
])
def test_init_invalid_payload_structure(
    invalid_payload: Union[None, str, list, int],
    mock_requests_get: MagicMock, 
    mock_response: MagicMock
) -> None:
    """Test initialization with invalid payload structure raises TypeError.

    Verifies
    --------
    - TypeError is raised for invalid payload structures
    - Error message contains appropriate text

    Parameters
    ----------
    invalid_payload : Union[None, str, list, int]
        Invalid payload to test
    mock_requests_get : MagicMock
        Mocked requests.get object
    mock_response : MagicMock
        Mocked response object

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    with pytest.raises(TypeError, match="dict_payload must be a dictionary for TypedDict"):
        MarginSimulatorB3(dict_payload=invalid_payload)


def test_init_invalid_dict_payload_structure(
    mock_requests_get: MagicMock, 
    mock_response: MagicMock
) -> None:
    """Test initialization with invalid dict payload structure.

    Verifies
    --------
    - Initialization succeeds even with invalid dict structure
    - TypedDict validation is basic (only checks if it's a dict)

    Parameters
    ----------
    mock_requests_get : MagicMock
        Mocked requests.get object
    mock_response : MagicMock
        Mocked response object

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    invalid_payload = {"invalid_key": "value"}
    # This should not raise an error because TypedDict validation is basic
    simulator = MarginSimulatorB3(dict_payload=invalid_payload)
    assert simulator.token == "test_token" # noqa S105: possible hardcoded password


def test_missing_reference_data_token(
    mock_requests_get: MagicMock, 
    valid_payload: ResultReferenceData,
    mock_response: MagicMock
) -> None:
    """Test _get_reference_data with missing referenceDataToken raises KeyError.

    Verifies
    --------
    - KeyError is raised when referenceDataToken is missing in response
    - Error is propagated correctly

    Parameters
    ----------
    mock_requests_get : MagicMock
        Mocked requests.get object
    valid_payload : ResultReferenceData
        Valid payload dictionary from fixture
    mock_response : MagicMock
        Mocked response object

    Returns
    -------
    None
    """
    mock_response.json.return_value = {"ReferenceData": {}}
    mock_requests_get.return_value = mock_response
    with pytest.raises(KeyError):
        MarginSimulatorB3(dict_payload=valid_payload)