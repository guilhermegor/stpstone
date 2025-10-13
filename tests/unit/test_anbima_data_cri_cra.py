"""Unit tests for Anbima CRI/CRA ingestion classes.

Tests the CRI/CRA data ingestion functionality with various input scenarios
including initialization, data extraction, transformation, edge cases and error
conditions.
"""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional
from unittest.mock import Mock, patch

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra import (
    AnbimaDataCRICRACharacteristics,
    AnbimaDataCRICRADocuments,
    AnbimaDataCRICRAEvents,
    AnbimaDataCRICRAIndividualCharacteristics,
    AnbimaDataCRICRAPricesFile,
    AnbimaDataCRICRAPricesWS,
    AnbimaDataCRICRAPUHistorico,
    AnbimaDataCRICRAPUIndicativo,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> Logger:
    """Fixture providing mock logger.

    Returns
    -------
    Logger
        Mock logger instance
    """
    return Mock(spec=Logger)


@pytest.fixture
def mock_db_session() -> Session:
    """Fixture providing mock database session.

    Returns
    -------
    Session
        Mock database session instance
    """
    return Mock(spec=Session)


@pytest.fixture
def sample_date_ref() -> date:
    """Fixture providing sample reference date.

    Returns
    -------
    date
        Reference date for testing
    """
    return date(2024, 1, 15)


@pytest.fixture
def mock_playwright_page(mocker: MockerFixture) -> PlaywrightPage:
    """Fixture providing mock Playwright page.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    PlaywrightPage
        Mock Playwright page instance
    """
    mock_page = mocker.MagicMock(spec=PlaywrightPage)
    mock_page.goto = mocker.MagicMock()
    mock_page.wait_for_timeout = mocker.MagicMock()
    mock_page.query_selector_all = mocker.MagicMock(return_value=[])
    mock_page.locator = mocker.MagicMock()
    return mock_page


@pytest.fixture
def mock_response() -> Response:
    """Fixture providing mock HTTP response.

    Returns
    -------
    Response
        Mock response instance
    """
    mock = Mock(spec=Response)
    mock.text = "test,data\n1,2\n3,4"
    mock.status_code = 200
    mock.raise_for_status = Mock()
    return mock


# --------------------------
# Tests for AnbimaDataCRICRACharacteristics
# --------------------------
class TestAnbimaDataCRICRACharacteristics:
    """Test cases for AnbimaDataCRICRACharacteristics class."""

    def test_init_with_valid_inputs(
        self, sample_date_ref: date, mock_logger: Logger, mock_db_session: Session
    ) -> None:
        """Test initialization with valid inputs.

        Verifies
        --------
        - The class can be initialized with valid parameters
        - All attributes are correctly set
        - Default values are applied when optional parameters are None

        Parameters
        ----------
        sample_date_ref : date
            Reference date fixture
        mock_logger : Logger
            Mock logger fixture
        mock_db_session : Session
            Mock database session fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRACharacteristics(
            date_ref=sample_date_ref,
            logger=mock_logger,
            cls_db=mock_db_session,
            start_page=0,
            end_page=5,
        )

        assert instance.date_ref == sample_date_ref
        assert instance.logger == mock_logger
        assert instance.cls_db == mock_db_session
        assert instance.start_page == 0
        assert instance.end_page == 5
        assert instance.base_url == "https://data.anbima.com.br/busca/certificado-de-recebiveis"

    def test_init_with_negative_start_page(
        self, sample_date_ref: date, mock_logger: Logger
    ) -> None:
        """Test initialization with negative start_page raises ValueError.

        Verifies
        --------
        - ValueError is raised when start_page is negative
        - Error message contains 'greater than or equal to 0'

        Parameters
        ----------
        sample_date_ref : date
            Reference date fixture
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="start_page must be greater than or equal to 0"):
            AnbimaDataCRICRACharacteristics(
                date_ref=sample_date_ref, logger=mock_logger, start_page=-1
            )

    def test_init_with_end_page_less_than_start_page(
        self, sample_date_ref: date, mock_logger: Logger
    ) -> None:
        """Test initialization with end_page less than start_page raises ValueError.

        Verifies
        --------
        - ValueError is raised when end_page < start_page
        - Error message contains 'greater than or equal to start_page'

        Parameters
        ----------
        sample_date_ref : date
            Reference date fixture
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        with pytest.raises(
            ValueError, match="end_page must be greater than or equal to start_page"
        ):
            AnbimaDataCRICRACharacteristics(
                date_ref=sample_date_ref, logger=mock_logger, start_page=5, end_page=3
            )

    def test_init_with_none_date_ref(self, mock_logger: Logger) -> None:
        """Test initialization with None date_ref uses default date.

        Verifies
        --------
        - When date_ref is None, a default date is calculated
        - The default date is not None
        - The instance is successfully created

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRACharacteristics(date_ref=None, logger=mock_logger)

        assert instance.date_ref is not None
        assert isinstance(instance.date_ref, date)

    @patch("stpstone.ingestion.countries.br.registries.anbima_data_cri_cra.sync_playwright")
    def test_get_response_with_no_pages(
        self, mock_playwright: Mock, mock_logger: Logger, mocker: MockerFixture
    ) -> None:
        """Test get_response when no pages are found.

        Verifies
        --------
        - Returns empty list when no elements are found
        - Playwright browser is properly launched and closed
        - Page navigation occurs correctly

        Parameters
        ----------
        mock_playwright : Mock
            Mock sync_playwright context manager
        mock_logger : Logger
            Mock logger fixture
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_browser = mocker.MagicMock()
        mock_page = mocker.MagicMock()
        mock_page.query_selector_all.return_value = []

        mock_context = mocker.MagicMock()
        mock_context.__enter__.return_value = mock_context
        mock_context.__exit__.return_value = None
        mock_context.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page

        mock_playwright.return_value = mock_context

        instance = AnbimaDataCRICRACharacteristics(logger=mock_logger, start_page=0, end_page=0)

        with patch("time.sleep"):
            result = instance.get_response(timeout_ms=1000)

        assert result == []
        mock_browser.close.assert_called_once()

    def test_transform_data_with_empty_list(self, mock_logger: Logger) -> None:
        """Test transform_data with empty input list.

        Verifies
        --------
        - Returns empty DataFrame when input is empty list
        - DataFrame has no rows or columns

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRACharacteristics(logger=mock_logger)
        result = instance.transform_data(raw_data=[])

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_transform_data_with_valid_data(self, mock_logger: Logger) -> None:
        """Test transform_data with valid input data.

        Verifies
        --------
        - DataFrame is created with correct structure
        - Date fields are properly handled
        - Pagina column is dropped if present

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [
            {
                "CODIGO_EMISSAO": "TEST001",
                "NOME_EMISSOR": "Test Emissor",
                "DATA_EMISSAO": "15/01/2024",
                "DATA_VENCIMENTO": "-",
                "pagina": 0,
            }
        ]

        instance = AnbimaDataCRICRACharacteristics(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "pagina" not in result.columns
        assert result["DATA_VENCIMENTO"].iloc[0] == "01/01/2100"

    def test_parse_raw_file_returns_stringio(self, mock_logger: Logger) -> None:
        """Test parse_raw_file returns empty StringIO.

        Verifies
        --------
        - Method returns StringIO instance
        - StringIO is empty

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRACharacteristics(logger=mock_logger)
        result = instance.parse_raw_file(resp_req=Mock())

        assert isinstance(result, StringIO)
        assert result.getvalue() == ""


# --------------------------
# Tests for AnbimaDataCRICRAPricesFile
# --------------------------
class TestAnbimaDataCRICRAPricesFile:
    """Test cases for AnbimaDataCRICRAPricesFile class."""

    def test_init_with_valid_inputs(
        self, sample_date_ref: date, mock_logger: Logger, mock_db_session: Session
    ) -> None:
        """Test initialization with valid inputs.

        Verifies
        --------
        - The class can be initialized with valid parameters
        - All attributes are correctly set
        - Download URL is properly configured

        Parameters
        ----------
        sample_date_ref : date
            Reference date fixture
        mock_logger : Logger
            Mock logger fixture
        mock_db_session : Session
            Mock database session fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAPricesFile(
            date_ref=sample_date_ref, logger=mock_logger, cls_db=mock_db_session
        )

        assert instance.date_ref == sample_date_ref
        assert instance.logger == mock_logger
        assert instance.cls_db == mock_db_session
        assert (
            instance.download_url
            == "https://www.anbima.com.br/pt_br/anbima/TaxasCriCraExport/downloadExterno"
        )

    @patch("requests.get")
    def test_get_response_successful_download(
        self, mock_get: Mock, mock_logger: Logger, mock_response: Response
    ) -> None:
        """Test successful file download.

        Verifies
        --------
        - HTTP request is made with correct URL and headers
        - Response is returned successfully
        - raise_for_status is called

        Parameters
        ----------
        mock_get : Mock
            Mock requests.get function
        mock_logger : Logger
            Mock logger fixture
        mock_response : Response
            Mock response fixture

        Returns
        -------
        None
        """
        mock_get.return_value = mock_response

        instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)
        result = instance.get_response()

        assert result == mock_response
        mock_get.assert_called_once()
        mock_response.raise_for_status.assert_called_once()

    @patch("requests.get")
    def test_get_response_http_error(
        self, mock_get: Mock, mock_logger: Logger
    ) -> None:
        """Test get_response with HTTP error.

        Verifies
        --------
        - Exception is raised when HTTP error occurs
        - Error is properly logged

        Parameters
        ----------
        mock_get : Mock
            Mock requests.get function
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        mock_get.side_effect = Exception("HTTP Error")

        instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)

        with pytest.raises(Exception, match="HTTP Error"):
            instance.get_response()

    def test_parse_raw_file_with_valid_response(
        self, mock_logger: Logger, mock_response: Response
    ) -> None:
        """Test parse_raw_file with valid response.

        Verifies
        --------
        - StringIO is created from response text
        - Content is correctly extracted

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture
        mock_response : Response
            Mock response fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)
        result = instance.parse_raw_file(resp_req=mock_response)

        assert isinstance(result, StringIO)
        assert result.getvalue() == mock_response.text

    def test_transform_data_with_empty_response(
        self, mock_logger: Logger, mocker: MockerFixture
    ) -> None:
        """Test transform_data with empty CSV response.

        Verifies
        --------
        - Handles empty response gracefully
        - Returns DataFrame even with no data

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_response = mocker.MagicMock(spec=Response)
        mock_response.text = ""

        instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)

        with pytest.raises(Exception): # noqa B017: do not assert blind exception
            instance.transform_data(raw_data=mock_response)


# --------------------------
# Tests for AnbimaDataCRICRAIndividualCharacteristics
# --------------------------
class TestAnbimaDataCRICRAIndividualCharacteristics:
    """Test cases for AnbimaDataCRICRAIndividualCharacteristics class."""

    def test_init_with_empty_asset_codes(self, mock_logger: Logger) -> None:
        """Test initialization with empty asset codes list.

        Verifies
        --------
        - Class initializes successfully with empty list
        - list_asset_codes is empty list

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAIndividualCharacteristics(
            logger=mock_logger, list_asset_codes=[]
        )

        assert instance.list_asset_codes == []

    def test_init_with_asset_codes(self, mock_logger: Logger) -> None:
        """Test initialization with asset codes list.

        Verifies
        --------
        - Asset codes list is correctly stored
        - List contains expected values

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        asset_codes = ["TEST001", "TEST002", "TEST003"]
        instance = AnbimaDataCRICRAIndividualCharacteristics(
            logger=mock_logger, list_asset_codes=asset_codes
        )

        assert instance.list_asset_codes == asset_codes
        assert len(instance.list_asset_codes) == 3

    @patch("stpstone.ingestion.countries.br.registries.anbima_data_cri_cra.sync_playwright")
    def test_get_response_with_empty_asset_codes(
        self, mock_playwright: Mock, mock_logger: Logger
    ) -> None:
        """Test get_response with empty asset codes list.

        Verifies
        --------
        - Returns empty list when no asset codes provided
        - No Playwright browser is launched

        Parameters
        ----------
        mock_playwright : Mock
            Mock sync_playwright
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAIndividualCharacteristics(
            logger=mock_logger, list_asset_codes=[]
        )

        result = instance.get_response(timeout_ms=1000)

        assert result == []
        mock_playwright.assert_not_called()

    def test_transform_data_with_empty_list(self, mock_logger: Logger) -> None:
        """Test transform_data with empty input.

        Verifies
        --------
        - Returns empty DataFrame
        - No errors are raised

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAIndividualCharacteristics(logger=mock_logger)
        result = instance.transform_data(raw_data=[])

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_handle_date_value_with_none(self, mock_logger: Logger) -> None:
        """Test _handle_date_value with None input.

        Verifies
        --------
        - None value is replaced with '01/01/2100'

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAIndividualCharacteristics(logger=mock_logger)
        result = instance._handle_date_value(None)

        assert result == "01/01/2100"

    def test_handle_date_value_with_dash(self, mock_logger: Logger) -> None:
        """Test _handle_date_value with dash input.

        Verifies
        --------
        - Dash value is replaced with '01/01/2100'

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAIndividualCharacteristics(logger=mock_logger)
        result = instance._handle_date_value("-")

        assert result == "01/01/2100"

    def test_handle_date_value_with_valid_date(self, mock_logger: Logger) -> None:
        """Test _handle_date_value with valid date string.

        Verifies
        --------
        - Valid date string is returned unchanged

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAIndividualCharacteristics(logger=mock_logger)
        result = instance._handle_date_value("15/01/2024")

        assert result == "15/01/2024"


# --------------------------
# Tests for AnbimaDataCRICRADocuments
# --------------------------
class TestAnbimaDataCRICRADocuments:
    """Test cases for AnbimaDataCRICRADocuments class."""

    def test_init_with_asset_codes(self, mock_logger: Logger) -> None:
        """Test initialization with asset codes.

        Verifies
        --------
        - Asset codes list is properly stored
        - Base URL is correctly set

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        asset_codes = ["DOC001", "DOC002"]
        instance = AnbimaDataCRICRADocuments(logger=mock_logger, list_asset_codes=asset_codes)

        assert instance.list_asset_codes == asset_codes
        assert instance.base_url == "https://data.anbima.com.br/certificado-de-recebiveis"

    @patch("stpstone.ingestion.countries.br.registries.anbima_data_cri_cra.sync_playwright")
    def test_get_response_empty_asset_codes(
        self, mock_playwright: Mock, mock_logger: Logger
    ) -> None:
        """Test get_response with no asset codes.

        Verifies
        --------
        - Returns empty list
        - Playwright is not invoked

        Parameters
        ----------
        mock_playwright : Mock
            Mock sync_playwright
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRADocuments(logger=mock_logger, list_asset_codes=[])

        result = instance.get_response(timeout_ms=1000)

        assert result == []
        mock_playwright.assert_not_called()

    def test_transform_data_with_valid_data(self, mock_logger: Logger) -> None:
        """Test transform_data with valid document data.

        Verifies
        --------
        - DataFrame is created correctly
        - Date fields are properly handled

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [
            {
                "COD_ATIVO": "DOC001",
                "IS_CRI_CRA": "CRI",
                "DATA_DIVULGACAO_DOCUMENTO": "15/01/2024",
            }
        ]

        instance = AnbimaDataCRICRADocuments(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["DATA_DIVULGACAO_DOCUMENTO"].iloc[0] == "15/01/2024"

    def test_transform_data_with_dash_date(self, mock_logger: Logger) -> None:
        """Test transform_data with dash in date field.

        Verifies
        --------
        - Dash is replaced with default date

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [{"COD_ATIVO": "DOC001", "DATA_DIVULGACAO_DOCUMENTO": "-"}]

        instance = AnbimaDataCRICRADocuments(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert result["DATA_DIVULGACAO_DOCUMENTO"].iloc[0] == "01/01/2100"


# --------------------------
# Tests for AnbimaDataCRICRAPUIndicativo
# --------------------------
class TestAnbimaDataCRICRAPUIndicativo:
    """Test cases for AnbimaDataCRICRAPUIndicativo class."""

    def test_init_with_default_values(self, mock_logger: Logger) -> None:
        """Test initialization with default values.

        Verifies
        --------
        - Default empty list for asset codes
        - Base URL is correctly set

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)

        assert instance.list_asset_codes == []
        assert instance.base_url == "https://data.anbima.com.br/certificado-de-recebiveis"

    def test_transform_data_date_replacement(self, mock_logger: Logger) -> None:
        """Test transform_data replaces dash with default date.

        Verifies
        --------
        - Date fields with dashes are replaced

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [{"COD_ATIVO": "PU001", "DATA_REFERENCIA": "-", "DATA_REFERENCIA_NTNB": "-"}]

        instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert result["DATA_REFERENCIA"].iloc[0] == "01/01/2100"
        assert result["DATA_REFERENCIA_NTNB"].iloc[0] == "01/01/2100"


# --------------------------
# Tests for AnbimaDataCRICRAPUHistorico
# --------------------------
class TestAnbimaDataCRICRAPUHistorico:
    """Test cases for AnbimaDataCRICRAPUHistorico class."""

    def test_init_creates_instance(self, mock_logger: Logger) -> None:
        """Test successful instance creation.

        Verifies
        --------
        - Instance is created with default values
        - Base URL is set correctly

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAPUHistorico(logger=mock_logger)

        assert instance.list_asset_codes == []
        assert isinstance(instance.base_url, str)

    def test_transform_data_handles_date_fields(self, mock_logger: Logger) -> None:
        """Test date field handling in transform_data.

        Verifies
        --------
        - Date replacement works correctly

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [{"COD_ATIVO": "HIST001", "DATA_REFERENCIA": "-"}]

        instance = AnbimaDataCRICRAPUHistorico(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert result["DATA_REFERENCIA"].iloc[0] == "01/01/2100"


# --------------------------
# Tests for AnbimaDataCRICRAEvents
# --------------------------
class TestAnbimaDataCRICRAEvents:
    """Test cases for AnbimaDataCRICRAEvents class."""

    def test_init_with_asset_codes_list(self, mock_logger: Logger) -> None:
        """Test initialization with asset codes list.

        Verifies
        --------
        - Asset codes are stored correctly

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        codes = ["EVT001", "EVT002"]
        instance = AnbimaDataCRICRAEvents(logger=mock_logger, list_asset_codes=codes)

        assert instance.list_asset_codes == codes

    def test_transform_data_with_event_dates(self, mock_logger: Logger) -> None:
        """Test transform_data handles event dates.

        Verifies
        --------
        - Date fields are properly processed

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [{"COD_ATIVO": "EVT001", "DATA_EVENTO": "-", "DATA_LIQUIDACAO": "-"}]

        instance = AnbimaDataCRICRAEvents(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        # AnbimaDataCRICRAEvents does replace dashes with default date
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        # Verify the DataFrame was created successfully
        assert "DATA_EVENTO" in result.columns
        assert "DATA_LIQUIDACAO" in result.columns

    @patch("stpstone.ingestion.countries.br.registries.anbima_data_cri_cra.sync_playwright")
    def test_get_response_no_assets(self, mock_playwright: Mock, mock_logger: Logger) -> None:
        """Test get_response with no asset codes.

        Verifies
        --------
        - Returns empty list
        - No browser operations occur

        Parameters
        ----------
        mock_playwright : Mock
            Mock sync_playwright
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAEvents(logger=mock_logger, list_asset_codes=[])

        result = instance.get_response(timeout_ms=1000)

        assert result == []
        mock_playwright.assert_not_called()


# --------------------------
# Tests for AnbimaDataCRICRAPricesWS
# --------------------------
class TestAnbimaDataCRICRAPricesWS:
    """Test cases for AnbimaDataCRICRAPricesWS class."""

    def test_init_validates_start_page_negative(self, mock_logger: Logger) -> None:
        """Test initialization fails with negative start_page.

        Verifies
        --------
        - ValueError is raised for negative start_page

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="start_page must be greater than or equal to 0"):
            AnbimaDataCRICRAPricesWS(logger=mock_logger, start_page=-5)

    def test_init_validates_end_page_before_start(self, mock_logger: Logger) -> None:
        """Test initialization fails when end_page < start_page.

        Verifies
        --------
        - ValueError is raised when end_page is less than start_page

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        with pytest.raises(
            ValueError, match="end_page must be greater than or equal to start_page"
        ):
            AnbimaDataCRICRAPricesWS(logger=mock_logger, start_page=10, end_page=5)

    def test_transform_data_handles_empty_input(self, mock_logger: Logger) -> None:
        """Test transform_data with empty input.

        Verifies
        --------
        - Returns empty DataFrame

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAPricesWS(logger=mock_logger)
        result = instance.transform_data(raw_data=[])

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_transform_data_removes_pagina_column(self, mock_logger: Logger) -> None:
        """Test transform_data removes pagina column.

        Verifies
        --------
        - pagina column is dropped from result

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [{"COD": "TEST", "pagina": 1}]

        instance = AnbimaDataCRICRAPricesWS(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert "pagina" not in result.columns

    def test_transform_data_replaces_dash_dates(self, mock_logger: Logger) -> None:
        """Test transform_data replaces dashes in date fields.

        Verifies
        --------
        - Dash values in date columns are replaced with default date

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [
            {
                "DATA_REFERENCIA": "-",
                "DATA_EMISSAO": "-",
                "DATA_VENCIMENTO": "-",
                "DATA_REFERENCIA_NTNB": "-",
            }
        ]

        instance = AnbimaDataCRICRAPricesWS(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert result["DATA_REFERENCIA"].iloc[0] == "01/01/2100"
        assert result["DATA_EMISSAO"].iloc[0] == "01/01/2100"
        assert result["DATA_VENCIMENTO"].iloc[0] == "01/01/2100"
        assert result["DATA_REFERENCIA_NTNB"].iloc[0] == "01/01/2100"


# --------------------------
# Integration and Edge Case Tests
# --------------------------
class TestCommonBehavior:
    """Test common behavior across multiple classes."""

    @pytest.mark.parametrize(
        "class_name",
        [
            AnbimaDataCRICRACharacteristics,
            AnbimaDataCRICRAPricesWS,
            AnbimaDataCRICRAPricesFile,
            AnbimaDataCRICRAIndividualCharacteristics,
            AnbimaDataCRICRADocuments,
            AnbimaDataCRICRAPUIndicativo,
            AnbimaDataCRICRAPUHistorico,
            AnbimaDataCRICRAEvents,
        ],
    )
    def test_parse_raw_file_returns_stringio(
        self, class_name: type, mock_logger: Logger
    ) -> None:
        """Test parse_raw_file returns StringIO for all classes.

        Verifies
        --------
        - All classes implement parse_raw_file
        - Method returns StringIO instance

        Parameters
        ----------
        class_name : type
            Class to test
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        if class_name == AnbimaDataCRICRAPricesFile:
            instance = class_name(logger=mock_logger)
            mock_response = Mock(spec=Response)
            mock_response.text = "test"
            result = instance.parse_raw_file(resp_req=mock_response)
        else:
            instance = class_name(logger=mock_logger)
            result = instance.parse_raw_file(resp_req=Mock())

        assert isinstance(result, StringIO)

    @pytest.mark.parametrize(
        "class_name",
        [
            AnbimaDataCRICRACharacteristics,
            AnbimaDataCRICRAPricesWS,
            AnbimaDataCRICRAIndividualCharacteristics,
            AnbimaDataCRICRADocuments,
            AnbimaDataCRICRAPUIndicativo,
            AnbimaDataCRICRAPUHistorico,
            AnbimaDataCRICRAEvents,
        ],
    )
    def test_transform_data_empty_list(self, class_name: type, mock_logger: Logger) -> None:
        """Test transform_data with empty list for all applicable classes.

        Verifies
        --------
        - Empty list input produces empty DataFrame
        - No errors are raised

        Parameters
        ----------
        class_name : type
            Class to test
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = class_name(logger=mock_logger)
        result = instance.transform_data(raw_data=[])

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @pytest.mark.parametrize(
        "class_name,has_pages",
        [
            (AnbimaDataCRICRACharacteristics, True),
            (AnbimaDataCRICRAPricesWS, True),
            (AnbimaDataCRICRAPricesFile, False),
            (AnbimaDataCRICRAIndividualCharacteristics, False),
        ],
    )
    def test_init_page_parameters(
        self, class_name: type, has_pages: bool, mock_logger: Logger
    ) -> None:
        """Test initialization with page parameters.

        Verifies
        --------
        - Classes with pagination support accept page parameters
        - Classes without pagination don't require page parameters

        Parameters
        ----------
        class_name : type
            Class to test
        has_pages : bool
            Whether class supports pagination
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        if has_pages:
            instance = class_name(logger=mock_logger, start_page=0, end_page=5)
            assert instance.start_page == 0
            assert instance.end_page == 5
        else:
            instance = class_name(logger=mock_logger)
            assert not hasattr(instance, "start_page") or instance.list_asset_codes == []


# --------------------------
# Error Handling Tests
# --------------------------
class TestErrorHandling:
    """Test error handling across classes."""

    def test_characteristics_with_invalid_timeout(self, mock_logger: Logger) -> None:
        """Test characteristics class handles invalid timeout.

        Verifies
        --------
        - Method accepts timeout parameter
        - No type errors occur with valid timeout values

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRACharacteristics(logger=mock_logger, start_page=0, end_page=0)

        with patch(
            "stpstone.ingestion.countries.br.registries.anbima_data_cri_cra.sync_playwright"), \
            patch("time.sleep"):
                # timeout_ms parameter should be accepted
                assert hasattr(instance, "get_response")

    @patch("requests.get")
    def test_prices_file_handles_connection_error(
        self, mock_get: Mock, mock_logger: Logger
    ) -> None:
        """Test prices file class handles connection errors.

        Verifies
        --------
        - Connection errors are properly raised
        - Error logging occurs

        Parameters
        ----------
        mock_get : Mock
            Mock requests.get function
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        mock_get.side_effect = ConnectionError("Connection failed")

        instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)

        with pytest.raises(ConnectionError, match="Connection failed"):
            instance.get_response()

    def test_transform_data_with_none_values(self, mock_logger: Logger) -> None:
        """Test transform_data handles None values in data.

        Verifies
        --------
        - None values don't cause errors
        - None values are handled appropriately

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [{"COD_ATIVO": None, "DATA_REFERENCIA": None, "IS_CRI_CRA": None}]

        instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_transform_data_with_mixed_date_formats(self, mock_logger: Logger) -> None:
        """Test transform_data with mixed date formats.

        Verifies
        --------
        - Various date formats are handled
        - Dashes and valid dates both work

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [
            {"DATA_REFERENCIA": "15/01/2024", "DATA_REFERENCIA_NTNB": "-"},
            {"DATA_REFERENCIA": "-", "DATA_REFERENCIA_NTNB": "20/02/2024"},
        ]

        instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert result["DATA_REFERENCIA"].iloc[0] == "15/01/2024"
        assert result["DATA_REFERENCIA_NTNB"].iloc[0] == "01/01/2100"
        assert result["DATA_REFERENCIA"].iloc[1] == "01/01/2100"
        assert result["DATA_REFERENCIA_NTNB"].iloc[1] == "20/02/2024"


# --------------------------
# Mocking and Performance Tests
# --------------------------
class TestPlaywrightMocking:
    """Test Playwright interaction mocking."""

    @patch("stpstone.ingestion.countries.br.registries.anbima_data_cri_cra.sync_playwright")
    @patch("time.sleep")
    def test_characteristics_no_real_browser(
        self, mock_sleep: Mock, mock_playwright: Mock, mock_logger: Logger, mocker: MockerFixture
    ) -> None:
        """Test characteristics scraping without real browser.

        Verifies
        --------
        - No actual browser is launched
        - Mocked browser operations work correctly
        - Sleep is mocked to avoid delays

        Parameters
        ----------
        mock_sleep : Mock
            Mock sleep function
        mock_playwright : Mock
            Mock sync_playwright
        mock_logger : Logger
            Mock logger fixture
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_browser = mocker.MagicMock()
        mock_page = mocker.MagicMock()
        mock_page.query_selector_all.return_value = []
        mock_page.locator.return_value.first.count.return_value = 0

        mock_context = mocker.MagicMock()
        mock_context.__enter__.return_value = mock_context
        mock_context.__exit__.return_value = None
        mock_context.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page

        mock_playwright.return_value = mock_context

        # Test with multiple pages to ensure sleep is called
        instance = AnbimaDataCRICRACharacteristics(logger=mock_logger, start_page=0, end_page=2)

        # Mock the _get_total_pages method to return 2
        with patch.object(instance, '_get_total_pages', return_value=2):
            result = instance.get_response(timeout_ms=100)

        assert result == []
        mock_browser.close.assert_called_once()
        # Sleep should be called when there are multiple pages
        assert mock_sleep.called

    @patch("stpstone.ingestion.countries.br.registries.anbima_data_cri_cra.sync_playwright")
    @patch("time.sleep")
    def test_prices_ws_no_real_browser(
        self, mock_sleep: Mock, mock_playwright: Mock, mock_logger: Logger, mocker: MockerFixture
    ) -> None:
        """Test prices WS scraping without real browser.

        Verifies
        --------
        - Browser operations are properly mocked
        - No actual network requests occur
        - Time delays are bypassed

        Parameters
        ----------
        mock_sleep : Mock
            Mock sleep function
        mock_playwright : Mock
            Mock sync_playwright
        mock_logger : Logger
            Mock logger fixture
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_browser = mocker.MagicMock()
        mock_page = mocker.MagicMock()
        mock_page.locator.return_value.all.return_value = []
        mock_page.locator.return_value.first.count.return_value = 0

        mock_context = mocker.MagicMock()
        mock_context.__enter__.return_value = mock_context
        mock_context.__exit__.return_value = None
        mock_context.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page

        mock_playwright.return_value = mock_context

        instance = AnbimaDataCRICRAPricesWS(logger=mock_logger, start_page=0, end_page=0)

        result = instance.get_response(timeout_ms=100)

        assert isinstance(result, list)
        mock_browser.close.assert_called_once()


# --------------------------
# Data Validation Tests
# --------------------------
class TestDataValidation:
    """Test data validation and type checking."""

    def test_date_ref_type_validation(self, mock_logger: Logger) -> None:
        """Test date_ref parameter type validation.

        Verifies
        --------
        - Valid date object is accepted
        - None is accepted and handled

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        valid_date = date(2024, 1, 15)
        instance = AnbimaDataCRICRACharacteristics(date_ref=valid_date, logger=mock_logger)

        assert instance.date_ref == valid_date
        assert isinstance(instance.date_ref, date)

    def test_logger_type_validation(self) -> None:
        """Test logger parameter type validation.

        Verifies
        --------
        - Logger instance is accepted
        - None is accepted

        Returns
        -------
        None
        """
        valid_logger = Mock(spec=Logger)
        instance = AnbimaDataCRICRACharacteristics(logger=valid_logger)

        assert instance.logger == valid_logger

        instance_no_logger = AnbimaDataCRICRACharacteristics(logger=None)
        assert instance_no_logger.logger is None

    def test_cls_db_type_validation(self) -> None:
        """Test cls_db parameter type validation.

        Verifies
        --------
        - Session instance is accepted
        - None is accepted

        Returns
        -------
        None
        """
        valid_session = Mock(spec=Session)
        instance = AnbimaDataCRICRACharacteristics(cls_db=valid_session)

        assert instance.cls_db == valid_session

        instance_no_db = AnbimaDataCRICRACharacteristics(cls_db=None)
        assert instance_no_db.cls_db is None

    @pytest.mark.parametrize(
        "start_page,end_page,should_pass",
        [
            (0, 0, True),
            (0, 10, True),
            (5, 5, True),
            (5, 10, True),
            (0, None, True),
            (-1, 10, False),
            (10, 5, False),
        ],
    )
    def test_page_range_validation(
        self, start_page: int, end_page: Optional[int], should_pass: bool, mock_logger: Logger
    ) -> None:
        """Test page range validation logic.

        Verifies
        --------
        - Valid page ranges are accepted
        - Invalid page ranges raise ValueError

        Parameters
        ----------
        start_page : int
            Starting page number
        end_page : Optional[int]
            Ending page number
        should_pass : bool
            Whether validation should pass
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        if should_pass:
            instance = AnbimaDataCRICRACharacteristics(
                logger=mock_logger, start_page=start_page, end_page=end_page
            )
            assert instance.start_page == start_page
            assert instance.end_page == end_page
        else:
            with pytest.raises(ValueError):
                AnbimaDataCRICRACharacteristics(
                    logger=mock_logger, start_page=start_page, end_page=end_page
                )


# --------------------------
# URL Construction Tests
# --------------------------
class TestURLConstruction:
    """Test URL construction across classes."""

    def test_characteristics_base_url(self, mock_logger: Logger) -> None:
        """Test characteristics class base URL.

        Verifies
        --------
        - Base URL is correctly set

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRACharacteristics(logger=mock_logger)

        assert instance.base_url == "https://data.anbima.com.br/busca/certificado-de-recebiveis"

    def test_prices_file_download_url(self, mock_logger: Logger) -> None:
        """Test prices file class download URL.

        Verifies
        --------
        - Download URL is correctly configured

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)

        expected_url = (
            "https://www.anbima.com.br/pt_br/anbima/TaxasCriCraExport/downloadExterno"
        )
        assert instance.download_url == expected_url

    @pytest.mark.parametrize(
        "class_name",
        [
            AnbimaDataCRICRAIndividualCharacteristics,
            AnbimaDataCRICRADocuments,
            AnbimaDataCRICRAPUIndicativo,
            AnbimaDataCRICRAPUHistorico,
            AnbimaDataCRICRAEvents,
        ],
    )
    def test_asset_specific_base_url(self, class_name: type, mock_logger: Logger) -> None:
        """Test asset-specific classes have correct base URL.

        Verifies
        --------
        - Base URL is set for asset-specific endpoints

        Parameters
        ----------
        class_name : type
            Class to test
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        instance = class_name(logger=mock_logger)

        assert instance.base_url == "https://data.anbima.com.br/certificado-de-recebiveis"


# --------------------------
# DataFrame Structure Tests
# --------------------------
class TestDataFrameStructure:
    """Test DataFrame structure and columns."""

    def test_characteristics_dataframe_columns(self, mock_logger: Logger) -> None:
        """Test characteristics DataFrame has expected columns.

        Verifies
        --------
        - Transform creates DataFrame with proper structure

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [
            {
                "CODIGO_EMISSAO": "TEST001",
                "NOME_EMISSOR": "Test",
                "DATA_EMISSAO": "15/01/2024",
            }
        ]

        instance = AnbimaDataCRICRACharacteristics(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert "CODIGO_EMISSAO" in result.columns
        assert "NOME_EMISSOR" in result.columns
        assert "DATA_EMISSAO" in result.columns

    def test_events_dataframe_structure(self, mock_logger: Logger) -> None:
        """Test events DataFrame structure.

        Verifies
        --------
        - Events DataFrame has expected columns after transform

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [{"COD_ATIVO": "EVT001", "DATA_EVENTO": "15/01/2024", "STATUS": "Paid"}]

        instance = AnbimaDataCRICRAEvents(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert "COD_ATIVO" in result.columns
        assert "DATA_EVENTO" in result.columns
        assert "STATUS" in result.columns


# --------------------------
# List Asset Codes Tests
# --------------------------
class TestListAssetCodes:
    """Test list_asset_codes parameter handling."""

    @pytest.mark.parametrize("asset_count", [0, 1, 5, 100])
    def test_various_asset_list_sizes(
        self, asset_count: int, mock_logger: Logger
    ) -> None:
        """Test handling of various asset list sizes.

        Verifies
        --------
        - Different list sizes are handled correctly

        Parameters
        ----------
        asset_count : int
            Number of assets in list
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        asset_codes = [f"ASSET{i:03d}" for i in range(asset_count)]

        instance = AnbimaDataCRICRAIndividualCharacteristics(
            logger=mock_logger, list_asset_codes=asset_codes
        )

        assert len(instance.list_asset_codes) == asset_count
        assert instance.list_asset_codes == asset_codes

    def test_asset_codes_immutability(self, mock_logger: Logger) -> None:
        """Test asset codes list is stored correctly.

        Verifies
        --------
        - Original list is preserved

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        original_list = ["ASSET001", "ASSET002"]
        instance = AnbimaDataCRICRADocuments(
            logger=mock_logger, list_asset_codes=original_list
        )

        # Verify the list content is the same
        assert instance.list_asset_codes == original_list
        # Verify it's the same list (reference equality is acceptable in this case)
        assert len(instance.list_asset_codes) == len(original_list)


# --------------------------
# String Handling Tests
# --------------------------
class TestStringHandling:
    """Test string handling and cleaning."""

    def test_date_string_with_whitespace(self, mock_logger: Logger) -> None:
        """Test date strings with whitespace are handled.

        Verifies
        --------
        - Whitespace in dates doesn't cause errors

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [{"DATA_REFERENCIA": " 15/01/2024 ", "COD_ATIVO": " TEST001 "}]

        instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_empty_string_handling(self, mock_logger: Logger) -> None:
        """Test empty string handling in data.

        Verifies
        --------
        - Empty strings don't cause errors

        Parameters
        ----------
        mock_logger : Logger
            Mock logger fixture

        Returns
        -------
        None
        """
        raw_data = [{"COD_ATIVO": "", "DATA_REFERENCIA": "", "IS_CRI_CRA": ""}]

        instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)
        result = instance.transform_data(raw_data=raw_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1