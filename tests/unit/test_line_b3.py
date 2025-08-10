"""Unit tests for B3 LINE API client implementation.

This module contains comprehensive unit tests for the B3 LINE API client,
covering all classes and methods with normal operations, edge cases, error
conditions, and type validations.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

from datetime import date, datetime
from unittest.mock import ANY, Mock

import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.utils.providers.br.line_b3 import (
    AccountsData,
    ConnectionApi,
    DocumentsData,
    Monitoring,
    Operations,
    Professional,
    ProfilesData,
    Resources,
    SystemEventManagement,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def connection_api() -> ConnectionApi:
    """Fixture providing a ConnectionApi instance with valid credentials.

    Returns
    -------
    ConnectionApi
        Instance initialized with test credentials
    """
    return ConnectionApi(
        client_id="test_client",
        client_secret="test_secret", # noqa S106: possible hardcoded password
        broker_code="1234",
        category_code="5678",
        hostname_api_line_b3="https://test.api.com",
    )


@pytest.fixture
def mock_request(mocker: MockerFixture) -> Mock:
    """Fixture mocking the requests.request function.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    Mock
        Mock object for requests.request
    """
    mock = mocker.patch("requests.request")
    mock_response = Mock(spec=Response)
    mock_response.status_code = 200

    def side_effect(*args: tuple, **kwargs: dict) -> Response:
        """Mock side effect for requests.request.
        
        Parameters
        ----------
        args : tuple
            Positional arguments for requests.request
        kwargs : dict
            Keyword arguments for requests.request

        Returns
        -------
        Response
            Mocked response object
        """
        if kwargs["url"].endswith("/api/v1.0/token/authorization"):
            mock_response.json.return_value = {"header": "test_header"}
        elif kwargs["url"].endswith("/api/oauth/token"):
            mock_response.json.return_value = {
                "access_token": "test_token",
                "refresh_token": "refresh",
                "expires_in": 5000,
            }
        else:
            mock_response.json.return_value = {"success": True}
        return mock_response

    mock.side_effect = side_effect
    return mock

@pytest.fixture
def mock_response() -> Response:
    """Fixture providing a mock Response object.

    Returns
    -------
    Response
        Mock Response object with default success status
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
        Sample date for testing
    """
    return date(2025, 1, 1)


@pytest.fixture
def sample_datetime() -> datetime:
    """Fixture providing a sample datetime.

    Returns
    -------
    datetime
        Sample datetime for testing
    """
    return datetime(2025, 1, 1, 12, 0)


# --------------------------
# ConnectionApi Tests
# --------------------------
class TestConnectionApi:
    """Test cases for ConnectionApi class."""

    def test_init_valid_credentials(self) -> None:
        """Test initialization with valid credentials.

        Verifies
        --------
        - Instance is created with valid inputs
        - Attributes are correctly set
        - No exceptions are raised

        Returns
        -------
        None
        """
        conn = ConnectionApi(
            client_id="test_client",
            client_secret="test_secret", # noqa S106: possible hardcoded password
            broker_code="1234",
            category_code="5678",
            hostname_api_line_b3="https://test.api.com",
        )
        assert conn.client_id == "test_client"
        assert conn.client_secret == "test_secret" # noqa S106: possible hardcoded password
        assert conn.broker_code == "1234"
        assert conn.category_code == "5678"
        assert conn.hostname_api_line_b3 == "https://test.api.com"

    def test_init_empty_credentials(self) -> None:
        """Test initialization with empty credentials raises ValueError.

        Verifies
        --------
        - Empty credentials raise ValueError
        - Error message matches expected

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="All credentials must be non-empty"):
            ConnectionApi("", "secret", "1234", "5678")

    def test_validate_url_invalid(self) -> None:
        """Test URL validation with invalid URL raises ValueError.

        Verifies
        --------
        - Non-https URL raises ValueError
        - Empty URL raises ValueError
        - Error messages match expected

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="URL must start with https://"):
            ConnectionApi("client", "secret", "1234", "5678", "http://invalid.com")
        with pytest.raises(ValueError, match="URL cannot be empty"):
            ConnectionApi("client", "secret", "1234", "5678", "")

    def test_auth_header_success(
        self, connection_api: ConnectionApi, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test auth_header with successful response.

        Verifies
        --------
        - Successful API call returns header
        - Correct headers are sent
        - Status code is checked

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = {"header": "test_header"}
        mock_request.return_value = mock_response
        result = connection_api.auth_header()
        assert result == "test_header"
        mock_request.assert_called_once()
        call_args = mock_request.call_args[1]
        assert call_args["url"] == "https://test.api.com/api/v1.0/token/authorization"
        assert call_args["headers"]["Accept"] == "application/json"

    def test_auth_header_max_retries(
        self, connection_api: ConnectionApi, mock_request: Mock
    ) -> None:
        """Test auth_header exceeding max retries raises ValueError.

        Verifies
        --------
        - Exceeding retries raises ValueError
        - Error message matches expected

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture
        mock_request : Mock
            Mocked requests.request

        Returns
        -------
        None
        """
        mock_request.side_effect = Exception("Request failed")
        with pytest.raises(ValueError, match="Maximum retry attempts exceeded"):
            connection_api.auth_header(int_max_retrieves=1)

    def test_access_token_success(
        self, connection_api: ConnectionApi, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test access_token with successful response.

        Verifies
        --------
        - Successful token retrieval
        - Correct headers and parameters
        - Token is returned

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.side_effect = [
            {"header": "test_header"},
            {
                "access_token": "test_token",
                "refresh_token": "refresh",
                "expires_in": 5000,
            },
        ]
        mock_request.return_value = mock_response
        result = connection_api.access_token()
        assert result == "test_token"
        assert mock_request.call_count == 2

    def test_app_request_success(
        self, connection_api: ConnectionApi, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test app_request with successful GET request.

        Verifies
        --------
        - Successful request returns JSON data
        - Correct headers and URL
        - Status code is checked

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"data": "test"}]
        mock_request.return_value = mock_response
        result = connection_api.app_request(method="GET", app="/test")
        assert result == [{"data": "test"}]
        mock_request.assert_called_once()
        call_args = mock_request.call_args[1]
        assert call_args["url"] == "https://test.api.com/test"
        assert call_args["headers"]["Authorization"].startswith("Bearer ")

    def test_app_request_retry(
        self, connection_api: ConnectionApi, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test app_request with retry on 401 error.

        Verifies
        --------
        - Retry logic on 401 error
        - Token refresh on 401
        - Successful retry returns data

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response_401 = Mock(spec=Response)
        mock_response_401.status_code = 401
        mock_response.json.side_effect = [
            {"header": "test_header"},
            {
                "access_token": "new_token",
                "refresh_token": "refresh",
                "expires_in": 5000,
            },
            [{"data": "test"}],
        ]
        mock_request.side_effect = [mock_response_401, mock_response, mock_response]
        result = connection_api.app_request(method="GET", app="/test", bool_retry_if_error=True)
        assert result == [{"data": "test"}]
        assert mock_request.call_count == 3

    def test_app_request_invalid_method(self, connection_api: ConnectionApi) -> None:
        """Test app_request with invalid HTTP method.

        Verifies
        --------
        - Invalid method raises ValueError

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Request failed after retries"):
            connection_api.app_request(method="INVALID", app="/test")


# --------------------------
# Operations Tests
# --------------------------
class TestOperations:
    """Test cases for Operations class."""

    @pytest.fixture
    def operations(self, connection_api: ConnectionApi) -> Operations:
        """Fixture providing an Operations instance.

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture

        Returns
        -------
        Operations
            Operations instance
        """
        return Operations(
            client_id=connection_api.client_id,
            client_secret=connection_api.client_secret,
            broker_code=connection_api.broker_code,
            category_code=connection_api.category_code,
            hostname_api_line_b3=connection_api.hostname_api_line_b3,
        )

    def test_exchange_limits(
        self, operations: Operations, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test exchange_limits method.

        Verifies
        --------
        - Correct endpoint is called
        - Returns expected data

        Parameters
        ----------
        operations : Operations
            Operations instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"limit": 1000}]
        mock_request.return_value = mock_response
        result = operations.exchange_limits()
        assert result == [{"limit": 1000}]
        mock_request.assert_called_once_with(
            method="GET",
            url="https://test.api.com/api/v1.0/exchangeLimits/spxi/1234",
            headers=ANY,
            params=None,
            data=None,
            timeout=10,
        )

    def test_groups_authorized_markets(
        self, operations: Operations, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test groups_authorized_markets method.

        Verifies
        --------
        - Correct endpoint is called
        - Returns expected data

        Parameters
        ----------
        operations : Operations
            Operations instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"id": "group1"}]
        mock_request.return_value = mock_response
        result = operations.groups_authorized_markets()
        assert result == [{"id": "group1"}]
        mock_request.assert_called_once_with(
            method="GET",
            url="https://test.api.com/api/v1.0/exchangeLimits/autorizedMarkets",
            headers=ANY,
            params=None,
            data=None,
            timeout=10,
        )

    def test_instruments_per_group(
        self, operations: Operations, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test instruments_per_group method.

        Verifies
        --------
        - Correct endpoint and payload
        - Returns expected data

        Parameters
        ----------
        operations : Operations
            Operations instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"instrumentSymbol": "TEST"}]
        mock_request.return_value = mock_response
        result = operations.intruments_per_group("group1")
        assert result == [{"instrumentSymbol": "TEST"}]
        mock_request.assert_called_once_with(
            method="POST",
            url="https://test.api.com/api/v1.0/exchangeLimits/findInstruments",
            headers=ANY,
            params=None,
            data=ANY,
            timeout=10,
        )

    def test_authorized_markets_instruments(
        self, operations: Operations, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test authorized_markets_instruments method.

        Verifies
        --------
        - Combines market groups and instruments correctly
        - Returns expected dictionary structure

        Parameters
        ----------
        operations : Operations
            Operations instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.side_effect = [
            [{"id": "group1", "name": "Group One"}],
            [{"instrumentSymbol": "TEST", "instrumentAsset": "ASSET", "limitSpci": 100}],
        ]
        mock_request.return_value = mock_response
        result = operations.authorized_markets_instruments()
        assert result == {
            "group1": {
                "id": "group1",
                "name": "Group One",
                "assets_associated": [
                    {
                        "instrument_symbol": "TEST",
                        "instrument_asset": "ASSET",
                        "limit_spci": 100,
                        "limit_spvi": None,
                    }
                ],
            }
        }


# --------------------------
# Resources Tests
# --------------------------
class TestResources:
    """Test cases for Resources class."""

    @pytest.fixture
    def resources(self, connection_api: ConnectionApi) -> Resources:
        """Fixture providing a Resources instance.

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture

        Returns
        -------
        Resources
            Resources instance
        """
        return Resources(
            client_id=connection_api.client_id,
            client_secret=connection_api.client_secret,
            broker_code=connection_api.broker_code,
            category_code=connection_api.category_code,
            hostname_api_line_b3=connection_api.hostname_api_line_b3,
        )

    def test_instrument_informations(
        self, resources: Resources, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test instrument_informations method.

        Verifies
        --------
        - Correct endpoint is called
        - Returns expected data

        Parameters
        ----------
        resources : Resources
            Resources instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"id": "1"}]
        mock_request.return_value = mock_response
        result = resources.instrument_informations()
        assert result == [{"id": "1"}]
        mock_request.assert_called_once_with(
            method="GET",
            url="https://test.api.com/api/v1.0/symbol",
            headers=ANY,
            params=None,
            data=None,
            timeout=10,
        )

    def test_instrument_infos_exchange_limits(
        self, resources: Resources, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test instrument_infos_exchange_limits method.

        Verifies
        --------
        - Correctly merges dataframes
        - Returns expected dictionary

        Parameters
        ----------
        resources : Resources
            Resources instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.side_effect = [
            [{"instrumentId": "1", "limit": 100}],
            [{"id": "1", "symbol": "TEST"}],
        ]
        mock_request.return_value = mock_response
        result = resources.instrument_infos_exchange_limits()
        assert result == {"TEST": {"id": "1", "symbol": "TEST", "instrumentId": "1", "limit": 100}}

    def test_instrument_id_by_symbol(
        self, resources: Resources, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test instrument_id_by_symbol method.

        Verifies
        --------
        - Correct endpoint with symbol
        - Returns expected data

        Parameters
        ----------
        resources : Resources
            Resources instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"id": "TEST"}]
        mock_request.return_value = mock_response
        result = resources.instrument_id_by_symbol("TEST")
        assert result == [{"id": "TEST"}]
        mock_request.assert_called_once_with(
            method="GET",
            url="https://test.api.com/api/v1.0/symbol/TEST",
            headers=ANY,
            params=None,
            data=None,
            timeout=10,
        )


# --------------------------
# AccountsData Tests
# --------------------------
class TestAccountsData:
    """Test cases for AccountsData class."""

    @pytest.fixture
    def accounts_data(self, connection_api: ConnectionApi) -> AccountsData:
        """Fixture providing an AccountsData instance.

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture

        Returns
        -------
        AccountsData
            AccountsData instance
        """
        return AccountsData(
            client_id=connection_api.client_id,
            client_secret=connection_api.client_secret,
            broker_code=connection_api.broker_code,
            category_code=connection_api.category_code,
            hostname_api_line_b3=connection_api.hostname_api_line_b3,
        )

    def test_client_infos(
        self, accounts_data: AccountsData, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test client_infos method.

        Verifies
        --------
        - Correct endpoint and parameters
        - Returns expected data

        Parameters
        ----------
        accounts_data : AccountsData
            AccountsData instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"account": "123"}]
        mock_request.return_value = mock_response
        result = accounts_data.client_infos("123")
        assert result == [{"account": "123"}]
        mock_request.assert_called_once_with(
            method="GET",
            url="https://test.api.com/api/v1.0/account",
            headers=ANY,
            params={"participantCode": "1234", "pnpCode": "5678", "accountCode": "123"},
            data=None,
            timeout=10,
        )

    def test_spxi_get(
        self, accounts_data: AccountsData, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test spxi_get method.

        Verifies
        --------
        - Correct endpoint and parameters
        - Returns expected data

        Parameters
        ----------
        accounts_data : AccountsData
            AccountsData instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"limit": 100}]
        mock_request.return_value = mock_response
        result = accounts_data.spxi_get("acc123")
        assert result == [{"limit": 100}]
        mock_request.assert_called_once_with(
            method="GET",
            url="https://test.api.com/api/v1.0/account/acc123/lmt/spxi",
            headers=ANY,
            params={"accId": "acc123"},
            data=None,
            timeout=10,
        )


# --------------------------
# DocumentsData Tests
# --------------------------
class TestDocumentsData:
    """Test cases for DocumentsData class."""

    @pytest.fixture
    def documents_data(self, connection_api: ConnectionApi) -> DocumentsData:
        """Fixture providing a DocumentsData instance.

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture

        Returns
        -------
        DocumentsData
            DocumentsData instance
        """
        return DocumentsData(
            client_id=connection_api.client_id,
            client_secret=connection_api.client_secret,
            broker_code=connection_api.broker_code,
            category_code=connection_api.category_code,
            hostname_api_line_b3=connection_api.hostname_api_line_b3,
        )

    def test_doc_info(
        self, documents_data: DocumentsData, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test doc_info method.

        Verifies
        --------
        - Correct endpoint and parameters
        - Returns expected data

        Parameters
        ----------
        documents_data : DocumentsData
            DocumentsData instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"doc": "123"}]
        mock_request.return_value = mock_response
        result = documents_data.doc_info("123")
        assert result == [{"doc": "123"}]
        mock_request.assert_called_once_with(
            method="GET",
            url="https://test.api.com/api/v1.0/document",
            headers=ANY,
            params={"participantCode": "1234", "pnpCode": "5678", "documentCode": "123"},
            data=None,
            timeout=10,
        )

    def test_doc_profile(
        self, documents_data: DocumentsData, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test doc_profile method.

        Verifies
        --------
        - Correct endpoint
        - Returns expected profile data

        Parameters
        ----------
        documents_data : DocumentsData
            DocumentsData instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = {"profileFull": "1", "profileName": "Test"}
        mock_request.return_value = mock_response
        result = documents_data.doc_profile("doc123")
        assert result == {"profile_id": "1", "profile_name": "Test"}
        mock_request.assert_called_once_with(
            method="GET",
            url="https://test.api.com/api/v2.0/document/v2.0/document/doc123",
            headers=ANY,
            params=None,
            data=None,
            timeout=10,
        )


# --------------------------
# Professional Tests
# --------------------------
class TestProfessional:
    """Test cases for Professional class."""

    @pytest.fixture
    def professional(self, connection_api: ConnectionApi) -> Professional:
        """Fixture providing a Professional instance.

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture

        Returns
        -------
        Professional
            Professional instance
        """
        return Professional(
            client_id=connection_api.client_id,
            client_secret=connection_api.client_secret,
            broker_code=connection_api.broker_code,
            category_code=connection_api.category_code,
            hostname_api_line_b3=connection_api.hostname_api_line_b3,
        )

    def test_professional_code_get(
        self, professional: Professional, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test professional_code_get method.

        Verifies
        --------
        - Correct endpoint and parameters
        - Returns expected data

        Parameters
        ----------
        professional : Professional
            Professional instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"code": "prof123"}]
        mock_request.return_value = mock_response
        result = professional.professional_code_get()
        assert result == [{"code": "prof123"}]
        mock_request.assert_called_once_with(
            method="GET",
            url="https://test.api.com/api/v1.0/operationsProfessionalParticipant/code",
            headers=ANY,
            params={"participantCode": "1234", "pnpCode": "5678"},
            data=None,
            timeout=10,
        )

    def test_professional_historic_position(
        self,
        professional: Professional,
        mock_request: Mock,
        mock_response: Response,
        sample_date: date,
    ) -> None:
        """Test professional_historic_position method.

        Verifies
        --------
        - Correct endpoint and payload
        - Date formatting
        - Returns expected data

        Parameters
        ----------
        professional : Professional
            Professional instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"position": "data"}]
        mock_request.return_value = mock_response
        result = professional.professional_historic_position(
            professional_code="prof123", dt_start=sample_date, dt_end=sample_date
        )
        assert result == [{"position": "data"}]
        mock_request.assert_called_once_with(
            method="POST",
            url="https://api.line.trd.cert.bvmfnet.com.br/api/v2.0/position/hstry",
            headers=ANY,
            params=None,
            data=ANY,
            timeout=10,
        )


# --------------------------
# ProfilesData Tests
# --------------------------
class TestProfilesData:
    """Test cases for ProfilesData class."""

    @pytest.fixture
    def profiles_data(self, connection_api: ConnectionApi) -> ProfilesData:
        """Fixture providing a ProfilesData instance.

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture

        Returns
        -------
        ProfilesData
            ProfilesData instance
        """
        return ProfilesData(
            client_id=connection_api.client_id,
            client_secret=connection_api.client_secret,
            broker_code=connection_api.broker_code,
            category_code=connection_api.category_code,
            hostname_api_line_b3=connection_api.hostname_api_line_b3,
        )

    def test_risk_profile(
        self, profiles_data: ProfilesData, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test risk_profile method.

        Verifies
        --------
        - Correct endpoint
        - Returns expected data

        Parameters
        ----------
        profiles_data : ProfilesData
            ProfilesData instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"profile": "conservative"}]
        mock_request.return_value = mock_response
        result = profiles_data.risk_profile()
        assert result == [{"profile": "conservative"}]
        mock_request.assert_called_once_with(
            method="GET",
            url="https://test.api.com/api/v1.0/riskProfile",
            headers=ANY,
            params=None,
            data=None,
            timeout=10,
        )


# --------------------------
# Monitoring Tests
# --------------------------
class TestMonitoring:
    """Test cases for Monitoring class."""

    @pytest.fixture
    def monitoring(self, connection_api: ConnectionApi) -> Monitoring:
        """Fixture providing a Monitoring instance.

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture

        Returns
        -------
        Monitoring
            Monitoring instance
        """
        return Monitoring(
            client_id=connection_api.client_id,
            client_secret=connection_api.client_secret,
            broker_code=connection_api.broker_code,
            category_code=connection_api.category_code,
            hostname_api_line_b3=connection_api.hostname_api_line_b3,
        )

    def test_alerts(
        self, monitoring: Monitoring, mock_request: Mock, mock_response: Response
    ) -> None:
        """Test alerts method.

        Verifies
        --------
        - Correct endpoint
        - Returns expected data

        Parameters
        ----------
        monitoring : Monitoring
            Monitoring instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"alert": "test"}]
        mock_request.return_value = mock_response
        result = monitoring.alerts()
        assert result == [{"alert": "test"}]
        mock_request.assert_called_once_with(
            method="GET",
            url="https://test.api.com/api/v1.0/alert/lastalerts?filterRead=true",
            headers=ANY,
            params=None,
            data=None,
            timeout=10,
        )


# --------------------------
# SystemEventManagement Tests
# --------------------------
class TestSystemEventManagement:
    """Test cases for SystemEventManagement class."""

    @pytest.fixture
    def system_event_management(self, connection_api: ConnectionApi) -> SystemEventManagement:
        """Fixture providing a SystemEventManagement instance.

        Parameters
        ----------
        connection_api : ConnectionApi
            ConnectionApi instance from fixture

        Returns
        -------
        SystemEventManagement
            SystemEventManagement instance
        """
        return SystemEventManagement(
            client_id=connection_api.client_id,
            client_secret=connection_api.client_secret,
            broker_code=connection_api.broker_code,
            category_code=connection_api.category_code,
            hostname_api_line_b3=connection_api.hostname_api_line_b3,
        )

    def test_report(
        self,
        system_event_management: SystemEventManagement,
        mock_request: Mock,
        mock_response: Response,
        sample_date: date,
    ) -> None:
        """Test report method.

        Verifies
        --------
        - Correct endpoint and payload
        - Date formatting
        - Returns expected data

        Parameters
        ----------
        system_event_management : SystemEventManagement
            SystemEventManagement instance from fixture
        mock_request : Mock
            Mocked requests.request
        mock_response : Response
            Mocked Response object
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"event": "test"}]
        mock_request.return_value = mock_response
        result = system_event_management.report(sample_date, sample_date)
        assert result == [{"event": "test"}]
        mock_request.assert_called_once_with(
            method="POST",
            url="https://test.api.com/api/v1.0/systemEvent",
            headers=ANY,
            params=None,
            data=ANY,
            timeout=10,
        )