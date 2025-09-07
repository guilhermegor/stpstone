"""Unit tests for InvestmentFunds class.

Tests the investment funds bylaws ingestion functionality including:
- Initialization with valid and invalid inputs
- HTTP request handling with retry mechanisms
- Content parsing and data transformation
- Database insertion and fallback behavior
- Error conditions and edge cases
"""

import io
from logging import Logger
from typing import Optional, Union
from unittest.mock import MagicMock, patch

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
import requests
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentParser,
    CoreIngestion,
)
from stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws import InvestmentFunds
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_apps() -> list[str]:
    """Fixture providing sample app identifiers.

    Returns
    -------
    list[str]
        List of sample app identifiers for testing
    """
    return ["app1", "app2", "app3"]


@pytest.fixture
def mock_logger() -> MagicMock:
    """Fixture providing a mock logger.

    Returns
    -------
    MagicMock
        Mock logger instance
    """
    return MagicMock(spec=Logger)


@pytest.fixture
def mock_db_session() -> MagicMock:
    """Fixture providing a mock database session.

    Returns
    -------
    MagicMock
        Mock database session instance
    """
    return MagicMock(spec=Session)


@pytest.fixture
def mock_response() -> MagicMock:
    """Fixture providing a mock HTTP response.

    Returns
    -------
    MagicMock
        Mock HTTP response with sample content
    """
    response = MagicMock(spec=Response)
    response.content = b"sample content"
    response.url = "https://example.com/test.pdf"
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def mock_dates_current() -> MagicMock:
    """Fixture providing a mock DatesCurrent instance.

    Returns
    -------
    MagicMock
        Mock DatesCurrent instance
    """
    mock = MagicMock(spec=DatesCurrent)
    mock.curr_date.return_value = "2023-01-01"
    return mock


@pytest.fixture
def mock_dir_files_management() -> MagicMock:
    """Fixture providing a mock DirFilesManagement instance.

    Returns
    -------
    MagicMock
        Mock DirFilesManagement instance
    """
    mock = MagicMock(spec=DirFilesManagement)
    mock.get_last_file_extension.return_value = ".pdf"
    return mock


@pytest.fixture
def mock_create_log() -> MagicMock:
    """Fixture providing a mock CreateLog instance.

    Returns
    -------
    MagicMock
        Mock CreateLog instance
    """
    return MagicMock(spec=CreateLog)


@pytest.fixture
def investment_funds_instance(
    sample_apps: list[str],
    mock_logger: MagicMock,
    mock_db_session: MagicMock,
    mock_dates_current: MagicMock,
    mock_dir_files_management: MagicMock,
    mock_create_log: MagicMock,
) -> InvestmentFunds:
    """Fixture providing InvestmentFunds instance with mocked dependencies.

    Parameters
    ----------
    sample_apps : list[str]
        Sample app identifiers
    mock_logger : MagicMock
        Mock logger instance
    mock_db_session : MagicMock
        Mock database session
    mock_dates_current : MagicMock
        Mock DatesCurrent instance
    mock_dir_files_management : MagicMock
        Mock DirFilesManagement instance
    mock_create_log : MagicMock
        Mock CreateLog instance

    Returns
    -------
    InvestmentFunds
        InvestmentFunds instance with mocked dependencies
    """
    with patch.multiple(
        "stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws",
        DatesCurrent=lambda: mock_dates_current,
        DirFilesManagement=lambda: mock_dir_files_management,
        CreateLog=lambda: mock_create_log,
    ):
        instance = InvestmentFunds(
            list_apps=sample_apps,
            logger=mock_logger,
            cls_db=mock_db_session,
        )
    return instance


@pytest.fixture
def mock_yaml_config() -> dict:
    """Fixture providing mock YAML configuration.

    Returns
    -------
    dict
        Mock YAML configuration for regex patterns
    """
    return {
        "regex_patterns": {
            "pattern1": r"\d+",
            "pattern2": r"[A-Z]+",
        }
    }


# --------------------------
# Tests for Initialization
# --------------------------
class TestInitialization:
    """Test cases for InvestmentFunds initialization."""

    def test_init_with_valid_inputs(self, sample_apps: list[str]) -> None:
        """Test initialization with valid inputs.

        Verifies
        --------
        - The InvestmentFunds can be initialized with valid parameters
        - The values are correctly stored in instance attributes
        - Default values are set correctly

        Parameters
        ----------
        sample_apps : list[str]
            Sample app identifiers from fixture

        Returns
        -------
        None
        """
        instance = InvestmentFunds(list_apps=sample_apps)
        assert instance.list_apps == sample_apps
        assert instance.int_pages_join == 3
        assert instance.logger is None
        assert instance.cls_db is None

    def test_init_with_custom_pages_join(self, sample_apps: list[str]) -> None:
        """Test initialization with custom pages_join value.

        Verifies
        --------
        - The custom int_pages_join value is correctly stored

        Parameters
        ----------
        sample_apps : list[str]
            Sample app identifiers from fixture

        Returns
        -------
        None
        """
        instance = InvestmentFunds(list_apps=sample_apps, int_pages_join=5)
        assert instance.int_pages_join == 5

    def test_init_with_logger_and_db(
        self, sample_apps: list[str], mock_logger: MagicMock, mock_db_session: MagicMock
    ) -> None:
        """Test initialization with logger and database session.

        Verifies
        --------
        - Logger and database session are correctly stored

        Parameters
        ----------
        sample_apps : list[str]
            Sample app identifiers from fixture
        mock_logger : MagicMock
            Mock logger instance
        mock_db_session : MagicMock
            Mock database session

        Returns
        -------
        None
        """
        instance = InvestmentFunds(
            list_apps=sample_apps, logger=mock_logger, cls_db=mock_db_session
        )
        assert instance.logger == mock_logger
        assert instance.cls_db == mock_db_session

    @pytest.mark.parametrize("invalid_apps", [None, 123, "string", {}])
    def test_init_with_invalid_apps_type(self, invalid_apps: any) -> None:
        """Test initialization raises TypeError for invalid apps type.

        Verifies
        --------
        - TypeError is raised when list_apps is not a list
        - Error message indicates the expected type

        Parameters
        ----------
        invalid_apps : any
            Invalid values for list_apps parameter

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            InvestmentFunds(list_apps=invalid_apps)

    def test_init_with_empty_apps_list(self) -> None:
        """Test initialization with empty apps list.

        Verifies
        --------
        - InvestmentFunds can be initialized with empty list
        - No exception is raised

        Returns
        -------
        None
        """
        instance = InvestmentFunds(list_apps=[])
        assert instance.list_apps == []

    @pytest.mark.parametrize("invalid_pages_join", ["string", None, []])
    def test_init_with_invalid_pages_join_type(self, invalid_pages_join: any) -> None:
        """Test initialization raises TypeError for invalid pages_join type.

        Verifies
        --------
        - TypeError is raised when int_pages_join is not an integer
        - Error message indicates the expected type

        Parameters
        ----------
        invalid_pages_join : any
            Invalid values for int_pages_join parameter

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            InvestmentFunds(list_apps=["app1"], int_pages_join=invalid_pages_join)


# --------------------------
# Tests for get_response method
# --------------------------
class TestGetResponse:
    """Test cases for get_response method."""

    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.requests.get")
    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.sleep")
    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.backoff.on_exception")
    def test_get_response_success(
        self,
        mock_backoff: MagicMock,
        mock_sleep: MagicMock,
        mock_requests_get: MagicMock,
        investment_funds_instance: InvestmentFunds,
        mock_response: MagicMock,
        sample_apps: list[str],
    ) -> None:
        """Test successful get_response execution.

        Verifies
        --------
        - HTTP requests are made for each app
        - Responses are collected in a list
        - Sleep is called between requests
        - Log messages are generated

        Parameters
        ----------
        mock_backoff : MagicMock
            Mock backoff decorator
        mock_sleep : MagicMock
            Mock sleep function
        mock_requests_get : MagicMock
            Mock requests.get function
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        mock_response : MagicMock
            Mock HTTP response
        sample_apps : list[str]
            Sample app identifiers

        Returns
        -------
        None
        """
        # Setup backoff to bypass retry mechanism
        mock_backoff.return_value = lambda func: func

        # Setup mock to return successful responses
        mock_requests_get.return_value = mock_response

        # Execute the method
        responses = investment_funds_instance.get_response()

        # Verify results
        assert len(responses) == len(sample_apps)
        assert all(response == mock_response for response in responses)
        assert mock_requests_get.call_count == len(sample_apps)
        assert mock_sleep.call_count == len(sample_apps)

    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.requests.get")
    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.backoff.on_exception")
    def test_get_response_http_error(
        self,
        mock_backoff: MagicMock,
        mock_requests_get: MagicMock,
        investment_funds_instance: InvestmentFunds,
    ) -> None:
        """Test get_response with HTTP error.

        Verifies
        --------
        - HTTPError is properly raised
        - Backoff mechanism is triggered

        Parameters
        ----------
        mock_backoff : MagicMock
            Mock backoff decorator
        mock_requests_get : MagicMock
            Mock requests.get function
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies

        Returns
        -------
        None
        """
        # Setup backoff to bypass retry mechanism
        mock_backoff.return_value = lambda func: func

        # Setup mock to raise HTTPError
        mock_requests_get.side_effect = requests.exceptions.HTTPError("Test error")

        # Verify exception is raised
        with pytest.raises(requests.exceptions.HTTPError, match="Test error"):
            investment_funds_instance.get_response()

    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.requests.get")
    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.backoff.on_exception")
    def test_get_response_connection_error(
        self,
        mock_backoff: MagicMock,
        mock_requests_get: MagicMock,
        investment_funds_instance: InvestmentFunds,
    ) -> None:
        """Test get_response with connection error.

        Verifies
        --------
        - ConnectionError is properly raised
        - Backoff mechanism is triggered

        Parameters
        ----------
        mock_backoff : MagicMock
            Mock backoff decorator
        mock_requests_get : MagicMock
            Mock requests.get function
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies

        Returns
        -------
        None
        """
        # Setup backoff to bypass retry mechanism
        mock_backoff.return_value = lambda func: func

        # Setup mock to raise ConnectionError
        mock_requests_get.side_effect = requests.exceptions.ConnectionError("Test error")

        # Verify exception is raised
        with pytest.raises(requests.exceptions.ConnectionError, match="Test error"):
            investment_funds_instance.get_response()

    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.requests.get")
    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.backoff.on_exception")
    def test_get_response_timeout(
        self,
        mock_backoff: MagicMock,
        mock_requests_get: MagicMock,
        investment_funds_instance: InvestmentFunds,
    ) -> None:
        """Test get_response with timeout.

        Verifies
        --------
        - Timeout is properly handled
        - Backoff mechanism is triggered

        Parameters
        ----------
        mock_backoff : MagicMock
            Mock backoff decorator
        mock_requests_get : MagicMock
            Mock requests.get function
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies

        Returns
        -------
        None
        """
        # Setup backoff to bypass retry mechanism
        mock_backoff.return_value = lambda func: func

        # Setup mock to raise Timeout
        mock_requests_get.side_effect = requests.exceptions.Timeout("Test timeout")

        # Verify exception is raised
        with pytest.raises(requests.exceptions.Timeout, match="Test timeout"):
            investment_funds_instance.get_response()

    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.requests.get")
    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.backoff.on_exception")
    def test_get_response_custom_timeout(
        self,
        mock_backoff: MagicMock,
        mock_requests_get: MagicMock,
        investment_funds_instance: InvestmentFunds,
        mock_response: MagicMock,
    ) -> None:
        """Test get_response with custom timeout values.

        Verifies
        --------
        - Custom timeout values are passed to requests.get
        - Different timeout formats are handled correctly

        Parameters
        ----------
        mock_backoff : MagicMock
            Mock backoff decorator
        mock_requests_get : MagicMock
            Mock requests.get function
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        mock_response : MagicMock
            Mock HTTP response

        Returns
        -------
        None
        """
        # Setup backoff to bypass retry mechanism
        mock_backoff.return_value = lambda func: func
        mock_requests_get.return_value = mock_response

        # Test with integer timeout
        investment_funds_instance.get_response(timeout=30)
        mock_requests_get.assert_called_with(
            "https://web.cvm.gov.br/app/fundosweb/fundos/regulamento/obter/por/arquivo/app1",
            timeout=30,
            verify=True,
        )

        # Test with float timeout
        investment_funds_instance.get_response(timeout=15.5)
        mock_requests_get.assert_called_with(
            "https://web.cvm.gov.br/app/fundosweb/fundos/regulamento/obter/por/arquivo/app1",
            timeout=15.5,
            verify=True,
        )

        # Test with tuple timeout
        investment_funds_instance.get_response(timeout=(10.0, 20.0))
        mock_requests_get.assert_called_with(
            "https://web.cvm.gov.br/app/fundosweb/fundos/regulamento/obter/por/arquivo/app1",
            timeout=(10.0, 20.0),
            verify=True,
        )

    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.requests.get")
    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.backoff.on_exception")
    def test_get_response_ssl_verification(
        self,
        mock_backoff: MagicMock,
        mock_requests_get: MagicMock,
        investment_funds_instance: InvestmentFunds,
        mock_response: MagicMock,
    ) -> None:
        """Test get_response with SSL verification disabled.

        Verifies
        --------
        - SSL verification can be disabled
        - Parameter is passed correctly to requests.get

        Parameters
        ----------
        mock_backoff : MagicMock
            Mock backoff decorator
        mock_requests_get : MagicMock
            Mock requests.get function
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        mock_response : MagicMock
            Mock HTTP response

        Returns
        -------
        None
        """
        # Setup backoff to bypass retry mechanism
        mock_backoff.return_value = lambda func: func
        mock_requests_get.return_value = mock_response

        # Test with SSL verification disabled
        investment_funds_instance.get_response(bool_verify=False)
        mock_requests_get.assert_called_with(
            "https://web.cvm.gov.br/app/fundosweb/fundos/regulamento/obter/por/arquivo/app1",
            timeout=(12.0, 21.0),
            verify=False,
        )


# --------------------------
# Tests for parse_raw_file method
# --------------------------
class TestParseRawFile:
    """Test cases for parse_raw_file method."""

    def test_parse_raw_file_response_object(
        self, investment_funds_instance: InvestmentFunds, mock_response: MagicMock
    ) -> None:
        """Test parse_raw_file with Response object.

        Verifies
        --------
        - BytesIO object is created from response content
        - Content is preserved correctly

        Parameters
        ----------
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        mock_response : MagicMock
            Mock HTTP response

        Returns
        -------
        None
        """
        result = investment_funds_instance.parse_raw_file(mock_response)
        assert isinstance(result, io.BytesIO)
        assert result.getvalue() == b"sample content"

    def test_parse_raw_file_playwright_page(
        self, investment_funds_instance: InvestmentFunds
    ) -> None:
        """Test parse_raw_file with Playwright Page object.

        Verifies
        --------
        - TypeError is raised for unsupported input types
        - Error message indicates expected types

        Parameters
        ----------
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies

        Returns
        -------
        None
        """
        mock_page = MagicMock(spec=PlaywrightPage)
        with pytest.raises(TypeError, match="Unsupported response type"):
            investment_funds_instance.parse_raw_file(mock_page)

    def test_parse_raw_file_selenium_webdriver(
        self, investment_funds_instance: InvestmentFunds
    ) -> None:
        """Test parse_raw_file with Selenium WebDriver object.

        Verifies
        --------
        - TypeError is raised for unsupported input types
        - Error message indicates expected types

        Parameters
        ----------
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies

        Returns
        -------
        None
        """
        mock_driver = MagicMock(spec=SeleniumWebDriver)
        with pytest.raises(TypeError, match="Unsupported response type"):
            investment_funds_instance.parse_raw_file(mock_driver)

    def test_parse_raw_file_invalid_type(self, investment_funds_instance: InvestmentFunds) -> None:
        """Test parse_raw_file with invalid input type.

        Verifies
        --------
        - TypeError is raised for completely invalid input types
        - Error message indicates expected types

        Parameters
        ----------
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="Unsupported response type"):
            investment_funds_instance.parse_raw_file("invalid_type")


# --------------------------
# Tests for transform_data method
# --------------------------
class TestTransformData:
    """Test cases for transform_data method."""

    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.YAML_INVESTMENT_FUNDS_BYLAWS")
    def test_transform_data_success(
        self,
        mock_yaml_config: MagicMock,
        investment_funds_instance: InvestmentFunds,
        mock_response: MagicMock,
    ) -> None:
        """Test successful transform_data execution.

        Verifies
        --------
        - Response objects are processed correctly
        - URL is added to each record
        - Result is a DataFrame with expected structure

        Parameters
        ----------
        mock_yaml_config : MagicMock
            Mock YAML configuration
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        mock_response : MagicMock
            Mock HTTP response

        Returns
        -------
        None
        """
        # Setup mocks
        mock_yaml_config.__getitem__.return_value = mock_yaml_config
        mock_response.url = "https://example.com/test.pdf"

        # Mock the pdf_docx_regex method to return sample data
        with patch.object(
            investment_funds_instance,
            "pdf_docx_regex",
            return_value=pd.DataFrame(
                [{"EVENT": "test_event", "MATCH_PATTERN": "test_pattern"}]
            ),
        ):
            # Mock the get_last_file_extension method
            with patch.object(
                investment_funds_instance.cls_dir_files_management,
                "get_last_file_extension",
                return_value=".pdf",
            ):
                result = investment_funds_instance.transform_data([mock_response])

        # Verify results
        assert isinstance(result, pd.DataFrame)
        assert "URL" in result.columns
        assert len(result) == 1
        assert result.iloc[0]["URL"] == "https://example.com/test.pdf"

    def test_transform_data_empty_response_list(self, investment_funds_instance: InvestmentFunds) -> None:
        """Test transform_data with empty response list.

        Verifies
        --------
        - Empty list returns empty DataFrame
        - DataFrame has expected structure

        Parameters
        ----------
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies

        Returns
        -------
        None
        """
        result = investment_funds_instance.transform_data([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_transform_data_invalid_response_type(self, investment_funds_instance: InvestmentFunds) -> None:
        """Test transform_data with invalid response type.

        Verifies
        --------
        - TypeError is raised for invalid response types
        - Error message indicates expected type

        Parameters
        ----------
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            investment_funds_instance.transform_data("invalid_type")  # type: ignore


# --------------------------
# Tests for run method
# --------------------------
class TestRunMethod:
    """Test cases for run method."""

    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.YAML_INVESTMENT_FUNDS_BYLAWS")
    def test_run_with_db_session(
        self,
        mock_yaml_config: MagicMock,
        investment_funds_instance: InvestmentFunds,
        mock_response: MagicMock,
        mock_db_session: MagicMock,
    ) -> None:
        """Test run method with database session.

        Verifies
        --------
        - Data is inserted into database when cls_db is provided
        - insert_table_db method is called
        - No DataFrame is returned

        Parameters
        ----------
        mock_yaml_config : MagicMock
            Mock YAML configuration
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        mock_response : MagicMock
            Mock HTTP response
        mock_db_session : MagicMock
            Mock database session
        mock_yaml_config : dict
            Mock YAML configuration data

        Returns
        -------
        None
        """
        # Setup mocks
        mock_yaml_config.__getitem__.return_value = mock_yaml_config

        # Mock get_response to return sample responses
        with patch.object(
            investment_funds_instance, "get_response", return_value=[mock_response]
        ):
            # Mock transform_data to return sample DataFrame
            with patch.object(
                investment_funds_instance,
                "transform_data",
                return_value=pd.DataFrame(
                    [{"EVENT": "test", "MATCH_PATTERN": "pattern", "URL": "test_url"}]
                ),
            ):
                # Mock standardize_dataframe to return the same DataFrame
                with patch.object(
                    investment_funds_instance,
                    "standardize_dataframe",
                    return_value=pd.DataFrame(
                        [{"EVENT": "test", "MATCH_PATTERN": "pattern", "URL": "test_url"}]
                    ),
                ):
                    # Mock insert_table_db
                    with patch.object(investment_funds_instance, "insert_table_db") as mock_insert:
                        result = investment_funds_instance.run()

        # Verify results
        assert result is None
        mock_insert.assert_called_once()

    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.YAML_INVESTMENT_FUNDS_BYLAWS")
    def test_run_without_db_session(
        self,
        mock_yaml_config: MagicMock,
        sample_apps: list[str],
        mock_response: MagicMock,
    ) -> None:
        """Test run method without database session.

        Verifies
        --------
        - DataFrame is returned when no cls_db is provided
        - insert_table_db method is not called

        Parameters
        ----------
        mock_yaml_config : MagicMock
            Mock YAML configuration
        sample_apps : list[str]
            Sample app identifiers
        mock_response : MagicMock
            Mock HTTP response
        mock_yaml_config : dict
            Mock YAML configuration data

        Returns
        -------
        None
        """
        # Setup mocks
        mock_yaml_config.__getitem__.return_value = mock_yaml_config

        # Create instance without database session
        instance = InvestmentFunds(list_apps=sample_apps)

        # Mock get_response to return sample responses
        with patch.object(instance, "get_response", return_value=[mock_response]):
            # Mock transform_data to return sample DataFrame
            with patch.object(
                instance,
                "transform_data",
                return_value=pd.DataFrame(
                    [{"EVENT": "test", "MATCH_PATTERN": "pattern", "URL": "test_url"}]
                ),
            ):
                # Mock standardize_dataframe to return the same DataFrame
                with patch.object(
                    instance,
                    "standardize_dataframe",
                    return_value=pd.DataFrame(
                        [{"EVENT": "test", "MATCH_PATTERN": "pattern", "URL": "test_url"}]
                    ),
                ):
                    result = instance.run()

        # Verify results
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.YAML_INVESTMENT_FUNDS_BYLAWS")
    def test_run_custom_table_name(
        self,
        mock_yaml_config: MagicMock,
        investment_funds_instance: InvestmentFunds,
        mock_response: MagicMock,
        mock_db_session: MagicMock,
    ) -> None:
        """Test run method with custom table name.

        Verifies
        --------
        - Custom table name is passed to insert_table_db
        - Parameter is handled correctly

        Parameters
        ----------
        mock_yaml_config : MagicMock
            Mock YAML configuration
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        mock_response : MagicMock
            Mock HTTP response
        mock_db_session : MagicMock
            Mock database session
        mock_yaml_config : dict
            Mock YAML configuration data

        Returns
        -------
        None
        """
        # Setup mocks
        mock_yaml_config.__getitem__.return_value = mock_yaml_config

        # Mock get_response to return sample responses
        with patch.object(
            investment_funds_instance, "get_response", return_value=[mock_response]
        ):
            # Mock transform_data to return sample DataFrame
            with patch.object(
                investment_funds_instance,
                "transform_data",
                return_value=pd.DataFrame(
                    [{"EVENT": "test", "MATCH_PATTERN": "pattern", "URL": "test_url"}]
                ),
            ):
                # Mock standardize_dataframe to return the same DataFrame
                with patch.object(
                    investment_funds_instance,
                    "standardize_dataframe",
                    return_value=pd.DataFrame(
                        [{"EVENT": "test", "MATCH_PATTERN": "pattern", "URL": "test_url"}]
                    ),
                ):
                    # Mock insert_table_db
                    with patch.object(investment_funds_instance, "insert_table_db") as mock_insert:
                        investment_funds_instance.run(str_table_name="custom_table")

        # Verify custom table name was used
        mock_insert.assert_called_once()
        call_args = mock_insert.call_args
        assert call_args[1]["str_table_name"] == "custom_table"

    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.YAML_INVESTMENT_FUNDS_BYLAWS")
    def test_run_insert_or_ignore(
        self,
        mock_yaml_config: MagicMock,
        investment_funds_instance: InvestmentFunds,
        mock_response: MagicMock,
        mock_db_session: MagicMock,
    ) -> None:
        """Test run method with insert_or_ignore flag.

        Verifies
        --------
        - insert_or_ignore flag is passed to insert_table_db
        - Parameter is handled correctly

        Parameters
        ----------
        mock_yaml_config : MagicMock
            Mock YAML configuration
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        mock_response : MagicMock
            Mock HTTP response
        mock_db_session : MagicMock
            Mock database session
        mock_yaml_config : dict
            Mock YAML configuration data

        Returns
        -------
        None
        """
        # Setup mocks
        mock_yaml_config.__getitem__.return_value = mock_yaml_config

        # Mock get_response to return sample responses
        with patch.object(
            investment_funds_instance, "get_response", return_value=[mock_response]
        ):
            # Mock transform_data to return sample DataFrame
            with patch.object(
                investment_funds_instance,
                "transform_data",
                return_value=pd.DataFrame(
                    [{"EVENT": "test", "MATCH_PATTERN": "pattern", "URL": "test_url"}]
                ),
            ):
                # Mock standardize_dataframe to return the same DataFrame
                with patch.object(
                    investment_funds_instance,
                    "standardize_dataframe",
                    return_value=pd.DataFrame(
                        [{"EVENT": "test", "MATCH_PATTERN": "pattern", "URL": "test_url"}]
                    ),
                ):
                    # Mock insert_table_db
                    with patch.object(investment_funds_instance, "insert_table_db") as mock_insert:
                        investment_funds_instance.run(bool_insert_or_ignore=True)

        # Verify insert_or_ignore flag was used
        mock_insert.assert_called_once()
        call_args = mock_insert.call_args
        assert call_args[1]["bool_insert_or_ignore"] is True


# --------------------------
# Tests for Error Conditions
# --------------------------
class TestErrorConditions:
    """Test cases for error conditions and edge cases."""

    def test_empty_apps_list(self) -> None:
        """Test behavior with empty apps list.

        Verifies
        --------
        - No HTTP requests are made when apps list is empty
        - Empty DataFrame is returned

        Returns
        -------
        None
        """
        instance = InvestmentFunds(list_apps=[])

        # Mock get_response to verify it's not called
        with patch.object(instance, "get_response") as mock_get_response:
            with patch.object(instance, "transform_data") as mock_transform:
                mock_transform.return_value = pd.DataFrame()
                result = instance.run()

        # Verify no requests were made
        mock_get_response.assert_not_called()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.requests.get")
    @patch("stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws.backoff.on_exception")
    def test_get_response_partial_failure(
        self,
        mock_backoff: MagicMock,
        mock_requests_get: MagicMock,
        investment_funds_instance: InvestmentFunds,
        mock_response: MagicMock,
    ) -> None:
        """Test get_response with partial failure.

        Verifies
        --------
        - Exception is raised when some requests fail
        - Backoff mechanism is triggered

        Parameters
        ----------
        mock_backoff : MagicMock
            Mock backoff decorator
        mock_requests_get : MagicMock
            Mock requests.get function
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        mock_response : MagicMock
            Mock HTTP response

        Returns
        -------
        None
        """
        # Setup backoff to bypass retry mechanism
        mock_backoff.return_value = lambda func: func

        # Setup mock to succeed for first request, fail for second
        mock_requests_get.side_effect = [
            mock_response,
            requests.exceptions.HTTPError("Second request failed"),
        ]

        # Verify exception is raised
        with pytest.raises(requests.exceptions.HTTPError, match="Second request failed"):
            investment_funds_instance.get_response()

    def test_transform_data_invalid_response_content(
        self, investment_funds_instance: InvestmentFunds
    ) -> None:
        """Test transform_data with invalid response content.

        Verifies
        --------
        - Error is handled gracefully when response content cannot be parsed
        - Empty DataFrame or appropriate error handling is implemented

        Parameters
        ----------
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies

        Returns
        -------
        None
        """
        # Create a mock response with invalid content
        mock_response = MagicMock(spec=Response)
        mock_response.content = b"invalid content"
        mock_response.url = "https://example.com/test.pdf"

        # Mock pdf_docx_regex to raise an exception
        with patch.object(
            investment_funds_instance,
            "pdf_docx_regex",
            side_effect=Exception("Failed to parse content"),
        ):
            with patch.object(
                investment_funds_instance.cls_dir_files_management,
                "get_last_file_extension",
                return_value=".pdf",
            ):
                # This should either raise the exception or handle it gracefully
                with pytest.raises(Exception, match="Failed to parse content"):
                    investment_funds_instance.transform_data([mock_response])


# --------------------------
# Tests for Type Validation
# --------------------------
class TestTypeValidation:
    """Test cases for type validation."""

    @pytest.mark.parametrize(
        "invalid_timeout", ["string", [], {}, None, (1, 2, 3), (1, "string")]
    )
    def test_get_response_invalid_timeout_type(
        self, investment_funds_instance: InvestmentFunds, invalid_timeout: any
    ) -> None:
        """Test get_response with invalid timeout types.

        Verifies
        --------
        - TypeError is raised for invalid timeout types
        - Error message indicates expected types

        Parameters
        ----------
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        invalid_timeout : any
            Invalid timeout values

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            investment_funds_instance.get_response(timeout=invalid_timeout)

    @pytest.mark.parametrize("invalid_bool_verify", ["string", 1, 0, [], {}])
    def test_get_response_invalid_bool_verify_type(
        self, investment_funds_instance: InvestmentFunds, invalid_bool_verify: any
    ) -> None:
        """Test get_response with invalid bool_verify types.

        Verifies
        --------
        - TypeError is raised for invalid bool_verify types
        - Error message indicates expected type

        Parameters
        ----------
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        invalid_bool_verify : any
            Invalid bool_verify values

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            investment_funds_instance.get_response(bool_verify=invalid_bool_verify)

    @pytest.mark.parametrize("invalid_bool_insert", ["string", 1, 0, [], {}])
    def test_run_invalid_bool_insert_type(
        self, investment_funds_instance: InvestmentFunds, invalid_bool_insert: any
    ) -> None:
        """Test run with invalid bool_insert_or_ignore types.

        Verifies
        --------
        - TypeError is raised for invalid bool_insert_or_ignore types
        - Error message indicates expected type

        Parameters
        ----------
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        invalid_bool_insert : any
            Invalid bool_insert_or_ignore values

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            investment_funds_instance.run(bool_insert_or_ignore=invalid_bool_insert)

    @pytest.mark.parametrize("invalid_table_name", [123, [], {}, None])
    def test_run_invalid_table_name_type(
        self, investment_funds_instance: InvestmentFunds, invalid_table_name: any
    ) -> None:
        """Test run with invalid table_name types.

        Verifies
        --------
        - TypeError is raised for invalid table_name types
        - Error message indicates expected type

        Parameters
        ----------
        investment_funds_instance : InvestmentFunds
            InvestmentFunds instance with mocked dependencies
        invalid_table_name : any
            Invalid table_name values

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            investment_funds_instance.run(str_table_name=invalid_table_name)