"""Unit tests for B3 LINE API client implementation.

This module contains comprehensive unit tests for the B3 LINE API client,
covering all classes and methods with normal operations, edge cases, error
conditions, and type validations. Tests use mocking to simulate API responses,
ensuring no real network calls are made.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

from datetime import date, datetime
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.utils.providers.br.line_b3 import ConnectionApi


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_request(mocker: MockerFixture) -> Mock:
    """Fixture mocking the requests.request function for API calls.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks.

    Returns
    -------
    Mock
        Mock object for requests.request with a side_effect simulating API responses.

    Notes
    -----
    The mock simulates successful responses for authentication endpoints
    (/token/authorization, /oauth/token) and generic success for others.
    """
    mock = mocker.patch("requests.request")
    mock_response = Mock(spec=Response)
    mock_response.status_code = 200

    def side_effect(*args: tuple, **kwargs: dict) -> Response:
        """Side effect for the mock request function.

        Parameters
        ----------
        args : tuple
            Positional arguments for the request function.
        kwargs : dict
            Keyword arguments for the request function.

        Returns
        -------
        Response
            Mocked response object with appropriate JSON data.
        """
        if kwargs.get("url", "").endswith("/api/v1.0/token/authorization"):
            mock_response.json.return_value = {"header": "test_header"}
            return mock_response
        elif kwargs.get("url", "").endswith("/api/oauth/token"):
            mock_response.json.return_value = {
                "access_token": "test_token",
                "refresh_token": "refresh",
                "expires_in": 5000,
            }
            return mock_response
        mock_response.json.return_value = {"success": True}
        return mock_response

    mock.side_effect = side_effect
    return mock


@pytest.fixture
def connection_api(mock_request: Mock) -> ConnectionApi:
    """Fixture providing a ConnectionApi instance with valid credentials.

    Parameters
    ----------
    mock_request : Mock
        Mocked requests.request to prevent real network calls.

    Returns
    -------
    ConnectionApi
        Instance initialized with test credentials, with mocked token retrieval.

    Notes
    -----
    Depends on mock_request to ensure no real HTTP calls are made during
    initialization (which calls access_token).
    """
    return ConnectionApi(
        client_id="test_client",
        client_secret="test_secret",  # noqa S106: possible hardcoded password
        broker_code="1234",
        category_code="5678",
        hostname_api_line_b3="https://test.api.com",
    )


@pytest.fixture
def mock_response() -> Response:
    """Fixture providing a mock Response object.

    Returns
    -------
    Response
        Mock Response object with default success status and JSON data.
    """
    response = Mock(spec=Response)
    response.status_code = 200
    response.json.return_value = {"success": True}
    return response


@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample date.

    Returns
    -------
    date
        Sample date (2025-01-01) for testing.
    """
    return date(2025, 1, 1)


@pytest.fixture
def sample_datetime() -> datetime:
    """Fixture providing a sample datetime.

    Returns
    -------
    datetime
        Sample datetime (2025-01-01 12:00) for testing.
    """
    return datetime(2025, 1, 1, 12, 0)


# --------------------------
# ConnectionApi Tests
# --------------------------
class TestConnectionApi:
    """Test cases for ConnectionApi class."""

    def test_init_empty_credentials(self) -> None:
        """Test initialization with empty credentials raises ValueError.

        Verifies that providing empty credentials raises a ValueError with the
        expected message.

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="All credentials must be non-empty"):
            ConnectionApi("", "secret", "1234", "5678")

    def test_validate_url_invalid(self) -> None:
        """Test URL validation with invalid URL raises ValueError.

        Verifies that non-https or empty URLs raise ValueError with appropriate
        messages.

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="URL must start with https://"):
            ConnectionApi("client", "secret", "1234", "5678", "http://invalid.com")
        with pytest.raises(ValueError, match="URL cannot be empty"):
            ConnectionApi("client", "secret", "1234", "5678", "")