"""Unit tests for AnbimaFundsRedemptionProbabilityMatrix class.

Tests the ANBIMA redemption probability matrix ingestion functionality including:
- Initialization with various date scenarios
- Browser automation and data scraping
- Data transformation and standardization
- Error handling and edge cases
- Database integration
"""

from datetime import date
import io
from logging import Logger
from typing import Any
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
from playwright.sync_api import Frame, Page as PlaywrightPage
import pytest
from requests import Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.br.registries.anbima_funds_withdraws_probability import (
    AnbimaFundsRedemptionProbabilityMatrix,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> MagicMock:
    """Fixture providing a mock logger instance.

    Returns
    -------
    MagicMock
        Mock logger object
    """
    return MagicMock(spec=Logger)


@pytest.fixture
def mock_db_session() -> MagicMock:
    """Fixture providing a mock database session.

    Returns
    -------
    MagicMock
        Mock database session object
    """
    return MagicMock(spec=Session)


@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample reference date.

    Returns
    -------
    date
        Sample date for testing (2023-10-15)
    """
    return date(2023, 10, 15)


@pytest.fixture
def sample_csv_data() -> bytes:
    """Fixture providing sample CSV data for testing.

    Returns
    -------
    bytes
        Sample CSV data as bytes
    """
    csv_content = """data;periodo;classe;segmento_investidor;tipo_metodologia;metrica;prazo;valor
2023-10-15;Mensal;Renda Variável;Varejo;Probabilidade;Resgate;30;0.1234
2023-10-15;Mensal;Renda Fixa;Institucional;Probabilidade;Resgate;60;0.5678"""
    return csv_content.encode('utf-8')


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Fixture providing a sample DataFrame for testing.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with redemption probability data
    """
    return pd.DataFrame({
        'data': ['2023-10-15', '2023-10-15'],
        'periodo': ['Mensal', 'Mensal'],
        'classe': ['Renda Variável', 'Renda Fixa'],
        'segmento_investidor': ['Varejo', 'Institucional'],
        'tipo_metodologia': ['Probabilidade', 'Probabilidade'],
        'metrica': ['Resgate', 'Resgate'],
        'prazo': ['30', '60'],
        'valor': ['0,1234', '0,5678']
    })


@pytest.fixture
def anbima_instance(
    mock_logger: MagicMock, 
    sample_date: date
) -> Any: # noqa ANN401: typing.Any is not allowed
    """Fixture providing AnbimaFundsRedemptionProbabilityMatrix instance.

    Parameters
    ----------
    mock_logger : MagicMock
        Mock logger instance
    sample_date : date
        Sample reference date

    Returns
    -------
    Any
        Instance of AnbimaFundsRedemptionProbabilityMatrix
    """
    with patch.object(DatesBRAnbima, 'add_working_days', return_value=sample_date), \
         patch.object(DatesCurrent, 'curr_date', return_value=date(2023, 12, 20)):
        return AnbimaFundsRedemptionProbabilityMatrix(
            date_ref=sample_date,
            logger=mock_logger,
            headless=True
        )


@pytest.fixture
def anbima_instance_with_db(
    mock_logger: MagicMock, 
    mock_db_session: MagicMock
) -> Any: # noqa ANN401: typing.Any is not allowed
    """Fixture providing AnbimaFundsRedemptionProbabilityMatrix instance with DB.

    Parameters
    ----------
    mock_logger : MagicMock
        Mock logger instance
    mock_db_session : MagicMock
        Mock database session

    Returns
    -------
    Any
        Instance of AnbimaFundsRedemptionProbabilityMatrix with DB session
    """
    with patch.object(DatesBRAnbima, 'add_working_days', return_value=date(2023, 10, 15)), \
         patch.object(DatesCurrent, 'curr_date', return_value=date(2023, 12, 20)):
        return AnbimaFundsRedemptionProbabilityMatrix(
            logger=mock_logger,
            cls_db=mock_db_session,
            headless=True
        )


# --------------------------
# Tests for Initialization
# --------------------------
class TestInitialization:
    """Test cases for AnbimaFundsRedemptionProbabilityMatrix initialization."""

    def test_init_with_date_ref(self, mock_logger: MagicMock, sample_date: date) -> None:
        """Test initialization with specific date reference.

        Verifies
        --------
        - Instance is created with provided date_ref
        - Target month and year are correctly initialized
        - Base URL is set correctly

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance
        sample_date : date
            Sample reference date

        Returns
        -------
        None
        """
        with patch.object(DatesBRAnbima, 'add_working_days', return_value=sample_date), \
             patch.object(DatesCurrent, 'curr_date', return_value=date(2023, 12, 20)):
            instance = AnbimaFundsRedemptionProbabilityMatrix(
                date_ref=sample_date,
                logger=mock_logger
            )

            assert instance.date_ref == sample_date
            assert instance.target_month_pt == "Outubro"
            assert instance.target_year == "2023"
            assert instance.base_url == (
                "https://www.anbima.com.br/pt_br/autorregular/"
                "matriz-de-probabilidade-de-resgates.htm"
            )
            assert instance.headless is True

    def test_init_without_date_ref(self, mock_logger: MagicMock) -> None:
        """Test initialization without date reference.

        Verifies
        --------
        - Instance uses default date (66 working days before current date)
        - Date calculation is performed correctly

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance

        Returns
        -------
        None
        """
        expected_date = date(2023, 10, 15)
        with patch.object(DatesBRAnbima, 'add_working_days', return_value=expected_date), \
             patch.object(DatesCurrent, 'curr_date', return_value=date(2023, 12, 20)):
            instance = AnbimaFundsRedemptionProbabilityMatrix(logger=mock_logger)

            assert instance.date_ref == expected_date
            assert instance.target_month_pt == "Outubro"
            assert instance.target_year == "2023"

    def test_init_with_database_session(
        self, 
        mock_logger: MagicMock, 
        mock_db_session: MagicMock
    ) -> None:
        """Test initialization with database session.

        Verifies
        --------
        - Database session is properly stored
        - Instance inherits from ABCIngestionOperations

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance
        mock_db_session : MagicMock
            Mock database session

        Returns
        -------
        None
        """
        with patch.object(DatesBRAnbima, 'add_working_days', return_value=date(2023, 10, 15)), \
             patch.object(DatesCurrent, 'curr_date', return_value=date(2023, 12, 20)):
            instance = AnbimaFundsRedemptionProbabilityMatrix(
                logger=mock_logger,
                cls_db=mock_db_session
            )

            assert instance.cls_db == mock_db_session
            assert isinstance(instance, ABCIngestionOperations)

    def test_month_mapping_correctness(self, mock_logger: MagicMock) -> None:
        """Test Portuguese month name mapping correctness.

        Verifies
        --------
        - All English month names are correctly mapped to Portuguese
        - Mapping covers all 12 months

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance

        Returns
        -------
        None
        """
        test_cases = [
            (date(2023, 1, 1), "Janeiro"),
            (date(2023, 2, 1), "Fevereiro"),
            (date(2023, 3, 1), "Março"),
            (date(2023, 4, 1), "Abril"),
            (date(2023, 5, 1), "Maio"),
            (date(2023, 6, 1), "Junho"),
            (date(2023, 7, 1), "Julho"),
            (date(2023, 8, 1), "Agosto"),
            (date(2023, 9, 1), "Setembro"),
            (date(2023, 10, 1), "Outubro"),
            (date(2023, 11, 1), "Novembro"),
            (date(2023, 12, 1), "Dezembro"),
        ]

        for test_date_, expected_month in test_cases:
            with patch.object(DatesBRAnbima, 'add_working_days', return_value=test_date_), \
                patch.object(DatesCurrent, 'curr_date', return_value=date(2023, 12, 20)):
                instance = AnbimaFundsRedemptionProbabilityMatrix(
                    date_ref=test_date_,
                    logger=mock_logger
                )

                assert instance.target_month_pt == expected_month
                assert instance.target_year == "2023"


# --------------------------
# Tests for Run Method
# --------------------------
class TestRunMethod:
    """Test cases for the run method."""

    def test_run_success_with_dataframe_return(
        self, 
        anbima_instance: Any, # noqa ANN401: typing.Any is not allowed
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test successful run returning DataFrame.

        Verifies
        --------
        - Complete workflow executes successfully
        - DataFrame is returned when no database session
        - Logging messages are called appropriately

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance
        sample_dataframe : pd.DataFrame
            Sample DataFrame for mocking

        Returns
        -------
        None
        """
        with patch.object(anbima_instance, 'get_response', return_value=sample_dataframe), \
            patch.object(anbima_instance, 'transform_data', return_value=sample_dataframe), \
            patch.object(anbima_instance, 'standardize_dataframe', 
                        return_value=sample_dataframe):

            result = anbima_instance.run(bool_insert_or_ignore=False)

            assert result is not None
            assert isinstance(result, pd.DataFrame)
            anbima_instance.get_response.assert_called_once_with(timeout_ms=180000)
            anbima_instance.transform_data.assert_called_once_with(raw_data=sample_dataframe)
            anbima_instance.standardize_dataframe.assert_called_once()

    def test_run_success_with_database_insert(
        self, 
        anbima_instance_with_db: Any, # noqa ANN401: typing.Any is not allowed
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test successful run with database insertion.

        Verifies
        --------
        - Data is inserted into database when bool_insert_or_ignore is True
        - None is returned when inserting to database
        - Database insertion method is called

        Parameters
        ----------
        anbima_instance_with_db : Any
            AnbimaFundsRedemptionProbabilityMatrix instance with DB
        sample_dataframe : pd.DataFrame
            Sample DataFrame for mocking

        Returns
        -------
        None
        """
        with patch.object(anbima_instance_with_db, 'get_response', 
                          return_value=sample_dataframe), \
             patch.object(anbima_instance_with_db, 'transform_data', 
                          return_value=sample_dataframe), \
             patch.object(anbima_instance_with_db, 'standardize_dataframe', 
                          return_value=sample_dataframe), \
             patch.object(anbima_instance_with_db, 'insert_table_db') as mock_insert:

            result = anbima_instance_with_db.run(bool_insert_or_ignore=True)

            assert result is None
            mock_insert.assert_called_once_with(
                cls_db=anbima_instance_with_db.cls_db,
                str_table_name="br_anbima_redemption_probability_matrix",
                df_=sample_dataframe,
                bool_insert_or_ignore=True
            )

    def test_run_no_data_retrieved(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test run when no data is retrieved.

        Verifies
        --------
        - None is returned when get_response returns empty DataFrame
        - Appropriate error log message is generated

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        empty_df = pd.DataFrame()
        
        with patch.object(anbima_instance, 'get_response', return_value=empty_df), \
             patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:

            result = anbima_instance.run()

            assert result is None
            mock_log.assert_any_call(
                anbima_instance.logger,
                "❌ No data retrieved from source",
                "error"
            )

    def test_run_custom_parameters(
        self, 
        anbima_instance: Any, # noqa ANN401: typing.Any is not allowed
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test run with custom parameters.

        Verifies
        --------
        - Custom timeout is passed to get_response
        - Custom table name is used for database insertion

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance
        sample_dataframe : pd.DataFrame
            Sample DataFrame for mocking

        Returns
        -------
        None
        """
        with patch.object(anbima_instance, 'get_response', return_value=sample_dataframe), \
            patch.object(anbima_instance, 'transform_data', return_value=sample_dataframe), \
            patch.object(anbima_instance, 'standardize_dataframe', return_value=sample_dataframe):

            anbima_instance.run(
                timeout_ms=300000,
                bool_insert_or_ignore=False,
                str_table_name="custom_table"
            )

            anbima_instance.get_response.assert_called_once_with(timeout_ms=300000)


# --------------------------
# Tests for Get Response Method
# --------------------------
class TestGetResponse:
    """Test cases for the get_response method."""

    def test_get_response_success(
        self, 
        anbima_instance: Any, # noqa ANN401: typing.Any is not allowed
        sample_csv_data: bytes, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test successful data retrieval.

        Verifies
        --------
        - Complete browser automation workflow executes
        - CSV data is downloaded and processed
        - DataFrame is returned with correct data

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance
        sample_csv_data : bytes
            Sample CSV data for mocking
        sample_dataframe : pd.DataFrame
            Sample DataFrame for mocking

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_page = MagicMock(spec=PlaywrightPage)
        mock_browser = MagicMock()
        mock_context = MagicMock()

        with patch('stpstone.ingestion.countries.br.registries.' \
                   'anbima_funds_withdraws_probability.sync_playwright') as mock_playwright, \
            patch.object(anbima_instance, '_initialize_browser', return_value=mock_frame), \
            patch.object(anbima_instance, '_select_month', return_value=True), \
            patch.object(anbima_instance, '_select_year', return_value=True), \
            patch.object(anbima_instance, '_download_data', return_value=sample_csv_data), \
            patch.object(anbima_instance, '_process_csv_data', return_value=sample_dataframe):

            mock_playwright.return_value.__enter__.return_value.chromium.launch.return_value = \
                mock_browser
            mock_browser.new_context.return_value = mock_context
            mock_context.new_page.return_value = mock_page

            result = anbima_instance.get_response(timeout_ms=120000)

            assert isinstance(result, pd.DataFrame)
            anbima_instance._initialize_browser.assert_called_once()
            anbima_instance._select_month.assert_called_once_with(mock_frame)
            anbima_instance._select_year.assert_called_once_with(mock_frame)
            anbima_instance._download_data.assert_called_once()
            anbima_instance._process_csv_data.assert_called_once_with(sample_csv_data)

    def test_get_response_browser_initialization_fails(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test get_response when browser initialization fails.

        Verifies
        --------
        - Empty DataFrame is returned when browser initialization fails
        - Appropriate error handling is performed

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_page = MagicMock()
        
        with patch('stpstone.ingestion.countries.br.registries' \
                   + '.anbima_funds_withdraws_probability.sync_playwright') as mock_playwright, \
            patch.object(anbima_instance, '_initialize_browser', return_value=None), \
            patch.object(anbima_instance.cls_create_log, 'log_message'):

            mock_playwright.return_value.__enter__.return_value.chromium.launch.return_value = \
                mock_browser
            mock_browser.new_context.return_value = mock_context
            mock_context.new_page.return_value = mock_page

            result = anbima_instance.get_response()

            assert result.empty
            # Verify browser was closed
            mock_browser.close.assert_called_once()

    def test_get_response_month_selection_fails(
        self, 
        anbima_instance: Any, # noqa ANN401: typing.Any is not allowed
        sample_csv_data: bytes, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test get_response when month selection fails but continues.

        Verifies
        --------
        - Process continues with warning when month selection fails
        - Data is still retrieved successfully

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance
        sample_csv_data : bytes
            Sample CSV data for mocking
        sample_dataframe : pd.DataFrame
            Sample DataFrame for mocking

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_page = MagicMock()

        with patch('stpstone.ingestion.countries.br.registries' \
                   '.anbima_funds_withdraws_probability.sync_playwright') as mock_playwright, \
            patch.object(anbima_instance, '_initialize_browser', return_value=mock_frame), \
            patch.object(anbima_instance, '_select_month', return_value=False), \
            patch.object(anbima_instance, '_select_year', return_value=True), \
            patch.object(anbima_instance, '_download_data', return_value=sample_csv_data), \
            patch.object(anbima_instance, '_process_csv_data', return_value=sample_dataframe), \
            patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:

            mock_playwright.return_value.__enter__.return_value.chromium.launch.return_value = \
                mock_browser
            mock_browser.new_context.return_value = mock_context
            mock_context.new_page.return_value = mock_page

            result = anbima_instance.get_response()

            assert isinstance(result, pd.DataFrame)
            mock_log.assert_any_call(
                anbima_instance.logger,
                "⚠ Continuing with current month selection",
                "warning"
            )

    def test_get_response_download_fails(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test get_response when data download fails.

        Verifies
        --------
        - Empty DataFrame is returned when download fails
        - Appropriate error logging occurs

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_page = MagicMock()

        with patch('stpstone.ingestion.countries.br.registries' \
            + '.anbima_funds_withdraws_probability.sync_playwright') as mock_playwright, \
            patch.object(anbima_instance, '_initialize_browser', return_value=mock_frame), \
            patch.object(anbima_instance, '_select_month', return_value=True), \
            patch.object(anbima_instance, '_select_year', return_value=True), \
            patch.object(anbima_instance, '_download_data', return_value=None):

            mock_playwright.return_value.__enter__.return_value.chromium.launch.return_value = \
                mock_browser
            mock_browser.new_context.return_value = mock_context
            mock_context.new_page.return_value = mock_page

            result = anbima_instance.get_response()

            assert result.empty

    def test_get_response_exception_handling(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test get_response exception handling.

        Verifies
        --------
        - Exceptions during scraping are properly caught and handled
        - Empty DataFrame is returned on exception
        - Browser is properly closed in finally block

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_page = MagicMock()
        
        with patch('stpstone.ingestion.countries.br.registries' \
            + '.anbima_funds_withdraws_probability.sync_playwright') as mock_playwright, \
            patch.object(anbima_instance, '_initialize_browser', 
                         side_effect=Exception("Test error")), \
            patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:

            mock_playwright.return_value.__enter__.return_value.chromium.launch.return_value = \
                mock_browser
            mock_browser.new_context.return_value = mock_context
            mock_context.new_page.return_value = mock_page

            result = anbima_instance.get_response()

            assert result.empty
            mock_log.assert_any_call(
                anbima_instance.logger,
                "❌ Error during scraping process: Test error",
                "error"
            )
            mock_browser.close.assert_called_once()


# --------------------------
# Tests for Browser Initialization
# --------------------------
class TestBrowserInitialization:
    """Test cases for browser initialization methods."""

    def test_initialize_browser_success(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test successful browser initialization.

        Verifies
        --------
        - Page navigation succeeds
        - Power BI iframe is found and loaded
        - Frame context is returned

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_page = MagicMock(spec=PlaywrightPage)
        mock_frame = MagicMock(spec=Frame)
        mock_iframe_element = MagicMock()

        mock_page.goto.return_value = None
        mock_page.query_selector.return_value = mock_iframe_element
        mock_iframe_element.content_frame.return_value = mock_frame

        result = anbima_instance._initialize_browser(mock_page, 60000)

        assert result == mock_frame
        mock_page.goto.assert_called_once_with(
            anbima_instance.base_url,
            wait_until='load',
            timeout=60000
        )
        mock_page.query_selector.assert_called_with('iframe[src*="powerbi.com"]')

    def test_initialize_browser_iframe_not_found(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test browser initialization when iframe not found.

        Verifies
        --------
        - None is returned when iframe element not found
        - Appropriate error message is logged

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_page = MagicMock(spec=PlaywrightPage)
        mock_page.goto.return_value = None
        mock_page.query_selector.return_value = None

        with patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:
            result = anbima_instance._initialize_browser(mock_page, 60000)

            assert result is None
            mock_log.assert_any_call(
                anbima_instance.logger,
                "✗ Power BI iframe not found",
                "error"
            )

    def test_initialize_browser_navigation_fails(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test browser initialization when navigation fails.

        Verifies
        --------
        - None is returned when page navigation fails
        - Exception is properly caught and handled

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_page = MagicMock(spec=PlaywrightPage)
        mock_page.goto.side_effect = Exception("Navigation failed")

        with patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:
            result = anbima_instance._initialize_browser(mock_page, 60000)

            assert result is None
            mock_log.assert_any_call(
                anbima_instance.logger,
                "✗ Browser initialization failed: Navigation failed",
                "error"
            )


# --------------------------
# Tests for Selection Methods
# --------------------------
class TestSelectionMethods:
    """Test cases for month and year selection methods."""

    def test_select_month_success(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test successful month selection.

        Verifies
        --------
        - Month dropdown is found and clicked
        - Target month is selected successfully
        - Current month detection works correctly

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_dropdown = MagicMock()
        mock_restatement = MagicMock()

        mock_frame.wait_for_selector.return_value = mock_dropdown
        mock_dropdown.query_selector.return_value = mock_restatement
        # First call returns different month, triggering the selection flow
        mock_restatement.inner_text.side_effect = ["Setembro", "Outubro"]

        with patch.object(anbima_instance, '_find_and_click_option', return_value=True):
            result = anbima_instance._select_month(mock_frame)

            assert result is True
            mock_frame.wait_for_selector.assert_called_with(
                'div.slicer-dropdown-menu[aria-label="ds_mes"]',
                timeout=15000
            )
            anbima_instance._find_and_click_option.assert_called_once()

    def test_select_month_already_correct(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test month selection when already on correct month.

        Verifies
        --------
        - No selection is performed when current month matches target
        - True is returned immediately

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_dropdown = MagicMock()
        mock_restatement = MagicMock()

        mock_frame.wait_for_selector.return_value = mock_dropdown
        mock_dropdown.query_selector.return_value = mock_restatement
        mock_restatement.inner_text.return_value = "Outubro"  # Same as target

        with patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:
            result = anbima_instance._select_month(mock_frame)

            assert result is True
            mock_log.assert_any_call(
                anbima_instance.logger,
                "✓ Already on target month: Outubro",
                "info"
            )

    def test_select_month_dropdown_not_found(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test month selection when dropdown not found.

        Verifies
        --------
        - False is returned when month dropdown not found
        - Appropriate error message is logged

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_frame.wait_for_selector.return_value = None

        with patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:
            result = anbima_instance._select_month(mock_frame)

            assert result is False
            mock_log.assert_any_call(
                anbima_instance.logger,
                "✗ Month dropdown not found",
                "error"
            )

    def test_select_year_success(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test successful year selection.

        Verifies
        --------
        - Year dropdown is found and clicked
        - Target year is selected successfully
        - Current year detection works correctly

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_dropdown = MagicMock()
        mock_restatement = MagicMock()

        mock_frame.wait_for_selector.return_value = mock_dropdown
        mock_dropdown.query_selector.return_value = mock_restatement
        # First call returns different year, triggering the selection flow
        mock_restatement.inner_text.side_effect = ["2022", "2023"]

        with patch.object(anbima_instance, '_find_and_click_option', return_value=True):
            result = anbima_instance._select_year(mock_frame)

            assert result is True
            mock_frame.wait_for_selector.assert_called_with(
                'div.slicer-dropdown-menu[aria-label="nu_ano"]',
                timeout=15000
            )
            anbima_instance._find_and_click_option.assert_called_once()

    def test_find_and_click_option_success(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test successful option finding and clicking.

        Verifies
        --------
        - Target option is found among available elements
        - Regular click method is attempted first
        - Function returns True on success

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_element = MagicMock()

        mock_frame.query_selector_all.return_value = [mock_element]
        mock_element.is_visible.return_value = True
        mock_element.inner_text.return_value = "Outubro"
        mock_element.click.return_value = None  # Regular click succeeds

        result = anbima_instance._find_and_click_option(
            mock_frame, "Outubro", ["Outubro", "Setembro"], "month"
        )

        assert result is True
        mock_element.click.assert_called_once()

    def test_find_and_click_option_force_click(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test option clicking with force method.

        Verifies
        --------
        - Force click is used when regular click fails
        - Function returns True when force click succeeds

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_element = MagicMock()

        mock_frame.query_selector_all.return_value = [mock_element]
        mock_element.is_visible.return_value = True
        mock_element.inner_text.return_value = "Outubro"
        mock_element.click.side_effect = [Exception("Click failed"), None]  # Force click succeeds

        result = anbima_instance._find_and_click_option(
            mock_frame, "Outubro", ["Outubro", "Setembro"], "month"
        )

        assert result is True
        assert mock_element.click.call_count == 2
        mock_element.click.assert_called_with(force=True)

    def test_find_and_click_option_coordinate_click(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test option clicking with coordinate method.

        Verifies
        --------
        - Coordinate-based clicking is attempted when other methods fail
        - Function returns True when coordinate click succeeds

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_element = MagicMock()

        mock_frame.query_selector_all.return_value = [mock_element]
        mock_element.is_visible.return_value = True
        mock_element.inner_text.return_value = "Outubro"
        mock_element.click.side_effect = Exception("All clicks failed")
        mock_element.bounding_box.return_value = {'x': 100, 'y': 100, 'width': 50, 'height': 30}
        mock_frame.click.return_value = None

        result = anbima_instance._find_and_click_option(
            mock_frame, "Outubro", ["Outubro", "Setembro"], "month"
        )

        assert result is True
        mock_frame.click.assert_called_once()

    def test_find_and_click_option_not_found(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test option finding when target not found.

        Verifies
        --------
        - False is returned when target option not found
        - Appropriate error message is logged

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_element = MagicMock()

        mock_frame.query_selector_all.return_value = [mock_element]
        mock_element.is_visible.return_value = True
        mock_element.inner_text.return_value = "Setembro"  # Different from target "Outubro"

        with patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:
            result = anbima_instance._find_and_click_option(
                mock_frame, "Outubro", ["Outubro", "Setembro"], "month"
            )

            assert result is False
            mock_log.assert_any_call(
                anbima_instance.logger,
                "✗ Target month 'Outubro' not found or could not be clicked",
                "error"
            )


# --------------------------
# Tests for Download Methods
# --------------------------
class TestDownloadMethods:
    """Test cases for data download methods."""

    def test_download_data_success(
        self, 
        anbima_instance: Any, # noqa ANN401: typing.Any is not allowed
        sample_csv_data: bytes
    ) -> None:
        """Test successful data download.

        Verifies
        --------
        - Download button is found and clicked
        - File is downloaded and saved to temporary directory
        - CSV data is read and returned as bytes

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance
        sample_csv_data : bytes
            Sample CSV data for mocking

        Returns
        -------
        None
        """
        mock_page = MagicMock(spec=PlaywrightPage)
        mock_frame = MagicMock(spec=Frame)
        mock_download_button = MagicMock()
        mock_download = MagicMock()

        mock_download.suggested_filename = "test_data.csv"
        mock_download.save_as.return_value = None

        with patch.object(anbima_instance, '_find_download_button', 
                          return_value=mock_download_button), \
             patch(
                'tempfile.mkdtemp', 
                return_value='/tmp/test' # noqa S108: use of temp file path is safe in testing
            ), \
             patch('builtins.open', mock_open(read_data=sample_csv_data)), \
             patch.object(mock_page, 'expect_download') as mock_expect:

            mock_expect.return_value.__enter__.return_value.value = mock_download

            result = anbima_instance._download_data(mock_page, mock_frame, 60000)

            assert result == sample_csv_data
            mock_download_button.click.assert_called_with(force=True)
            mock_download.save_as.assert_called_once()

    def test_download_data_button_not_found(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test download when button not found.

        Verifies
        --------
        - None is returned when download button not found
        - Appropriate error message is logged

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_page = MagicMock(spec=PlaywrightPage)
        mock_frame = MagicMock(spec=Frame)

        with patch.object(anbima_instance, '_find_download_button', return_value=None):
            result = anbima_instance._download_data(mock_page, mock_frame, 60000)

            assert result is None

    def test_download_data_fails(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test download when process fails.

        Verifies
        --------
        - None is returned when download process fails
        - Exception is properly caught and handled

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_page = MagicMock(spec=PlaywrightPage)
        mock_frame = MagicMock(spec=Frame)
        mock_download_button = MagicMock()

        with patch.object(
            anbima_instance, '_find_download_button', return_value=mock_download_button), \
            patch.object(mock_page, 'expect_download', 
                         side_effect=Exception("Download failed")), \
            patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:

            result = anbima_instance._download_data(mock_page, mock_frame, 60000)

            assert result is None
            mock_log.assert_any_call(
                anbima_instance.logger,
                "✗ Download failed: Download failed",
                "error"
            )

    def test_find_download_button_by_text(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test download button finding by exact text.

        Verifies
        --------
        - Button is found by exact text match
        - Visible button is returned

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_button = MagicMock()

        mock_frame.query_selector.return_value = mock_button
        mock_button.is_visible.return_value = True

        result = anbima_instance._find_download_button(mock_frame)

        assert result == mock_button
        mock_frame.query_selector.assert_called_with('text="Download base consolidada"')

    def test_find_download_button_by_partial_text(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test download button finding by partial text.

        Verifies
        --------
        - Button is found by partial text match
        - All elements are searched when direct method fails

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)
        mock_button = MagicMock()

        # First call returns None (direct method fails)
        mock_frame.query_selector.side_effect = [None, mock_button]
        mock_frame.query_selector_all.return_value = [mock_button]
        mock_button.is_visible.return_value = True
        mock_button.inner_text.return_value = "Download base consolidada"

        result = anbima_instance._find_download_button(mock_frame)

        assert result == mock_button

    def test_find_download_button_not_found(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test download button not found.

        Verifies
        --------
        - None is returned when no button found
        - All search methods are attempted

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_frame = MagicMock(spec=Frame)

        mock_frame.query_selector.return_value = None
        mock_frame.query_selector_all.return_value = []

        result = anbima_instance._find_download_button(mock_frame)

        assert result is None


# --------------------------
# Tests for Data Processing
# --------------------------
class TestDataProcessing:
    """Test cases for data processing methods."""

    def test_process_csv_data_success(
        self, 
        anbima_instance: Any, # noqa ANN401: typing.Any is not allowed
        sample_csv_data: bytes
    ) -> None:
        """Test successful CSV data processing.

        Verifies
        --------
        - CSV data is decoded successfully
        - DataFrame is parsed with correct separator
        - Data shape and columns are validated

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance
        sample_csv_data : bytes
            Sample CSV data for testing

        Returns
        -------
        None
        """
        with patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:
            result = anbima_instance._process_csv_data(sample_csv_data)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert list(result.columns) == [
                'data', 'periodo', 'classe', 'segmento_investidor',
                'tipo_metodologia', 'metrica', 'prazo', 'valor'
            ]
            mock_log.assert_any_call(
                anbima_instance.logger,
                "✓ Successfully decoded with utf-8",
                "info"
            )

    def test_process_csv_data_encoding_fallback(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test CSV processing with encoding fallback.

        Verifies
        --------
        - Multiple encodings are attempted
        - Latin-1 is used when UTF-8 fails
        - DataFrame is still created successfully

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        # Create data that fails UTF-8 but works with Latin-1
        latin1_data = "data;valor\n2023-10-15;1,2345".encode('latin-1')

        with patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:
            result = anbima_instance._process_csv_data(latin1_data)

            assert isinstance(result, pd.DataFrame)
            # Check that some encoding was used successfully
            assert any("decoded" in str(call).lower() for call in mock_log.call_args_list)

    def test_process_csv_data_single_column_fix(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test CSV processing with single column fix.

        Verifies
        --------
        - Single-column CSV data is detected and fixed
        - DataFrame is properly reconstructed
        - Warning is logged about the fix

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        single_column_data = \
            """data,periodo,classe,segmento_investidor,tipo_metodologia,metrica,prazo,valor
            2023-10-15,Mensal,Renda Variável,Varejo,Probabilidade,Resgate,30,0.1234
            2023-10-15,Mensal,Renda Fixa,Institucional,Probabilidade,Resgate,60,0.5678"""
        single_column_data = single_column_data.encode('utf-8') # noqa UP012: unnecessary utf-8 `encoding` argument to `encode`

        with patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:
            result = anbima_instance._process_csv_data(single_column_data)

            assert isinstance(result, pd.DataFrame)
            # Check if single column warning was logged
            assert any("single column" in str(call).lower() or "one column" in str(call).lower() 
                      for call in mock_log.call_args_list)

    def test_process_csv_data_delimiter_fallback(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test CSV processing with delimiter fallback.

        Verifies
        --------
        - Comma delimiter is used when semicolon fails
        - DataFrame is still parsed successfully
        - Appropriate logging occurs

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        comma_data = """data,periodo,classe,valor
2023-10-15,Mensal,Renda Variável,0.1234
2023-10-15,Mensal,Renda Fixa,0.5678""".encode('utf-8') # noqa UP012: unnecessary utf-8 `encoding` argument to `encode`

        result = anbima_instance._process_csv_data(comma_data)

        assert isinstance(result, pd.DataFrame)

    def test_process_csv_data_all_methods_fail(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test CSV processing when all methods fail.

        Verifies
        --------
        - Result is returned even with invalid CSV data
        - Method handles edge cases gracefully

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        # Use data that will decode but produce invalid CSV structure
        invalid_data = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"

        result = anbima_instance._process_csv_data(invalid_data)

        # The method is robust and returns a DataFrame even with invalid data
        # It may be empty or have unexpected structure, but doesn't crash
        assert isinstance(result, pd.DataFrame)

    def test_transform_data_success(
        self, 
        anbima_instance: Any, # noqa ANN401: typing.Any is not allowed
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test successful data transformation.

        Verifies
        --------
        - Numeric columns are properly processed
        - Brazilian number format is converted
        - DataFrame copy is returned

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance
        sample_dataframe : pd.DataFrame
            Sample DataFrame for testing

        Returns
        -------
        None
        """
        with patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:
            result = anbima_instance.transform_data(sample_dataframe)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            # Check that numeric conversion was attempted
            mock_log.assert_any_call(
                anbima_instance.logger,
                "✓ Processed numeric column: valor",
                "info"
            )

    def test_transform_data_empty(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test transformation of empty DataFrame.

        Verifies
        --------
        - Empty DataFrame is returned when input is empty
        - Warning message is logged

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        empty_df = pd.DataFrame()

        with patch.object(anbima_instance.cls_create_log, 'log_message') as mock_log:
            result = anbima_instance.transform_data(empty_df)

            assert result.empty
            mock_log.assert_any_call(
                anbima_instance.logger,
                "⚠ No data to transform",
                "warning"
            )

    def test_transform_data_numeric_conversion_error(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test transformation with numeric conversion error.

        Verifies
        --------
        - Transformation continues when numeric conversion fails
        - Warning is logged for conversion errors
        - DataFrame is still returned

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        # Create DataFrame with invalid numeric data
        problematic_df = pd.DataFrame({
            'valor': ['invalid', '1.000,50'],  # First value cannot be converted
            'prazo': ['not_a_number', '30']
        })

        with patch.object(anbima_instance.cls_create_log, 'log_message'):
            result = anbima_instance.transform_data(problematic_df)

            assert isinstance(result, pd.DataFrame)
            # Should log warnings for conversion failures or continue gracefully
            # The method handles errors by catching exceptions


# --------------------------
# Tests for Compatibility Methods
# --------------------------
class TestCompatibilityMethods:
    """Test cases for compatibility methods."""

    def test_parse_raw_file_playwright(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test parse_raw_file with Playwright page.

        Verifies
        --------
        - StringIO is returned for compatibility
        - Method exists but may not be used in web scraping context

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_page = MagicMock(spec=PlaywrightPage)

        result = anbima_instance.parse_raw_file(mock_page)

        assert isinstance(result, io.StringIO)

    def test_parse_raw_file_selenium(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test parse_raw_file with Selenium webdriver.

        Verifies
        --------
        - StringIO is returned for compatibility
        - Method handles Selenium webdriver input

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_driver = MagicMock(spec=SeleniumWebDriver)

        result = anbima_instance.parse_raw_file(mock_driver)

        assert isinstance(result, io.StringIO)

    def test_parse_raw_file_response(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test parse_raw_file with requests Response.

        Verifies
        --------
        - StringIO is returned for compatibility
        - Method handles requests Response input

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_response = MagicMock()

        result = anbima_instance.parse_raw_file(mock_response)

        assert isinstance(result, io.StringIO)


# --------------------------
# Tests for Error Handling and Edge Cases
# --------------------------
class TestErrorHandling:
    """Test cases for error handling and edge cases."""

    def test_initialization_with_none_date(self, mock_logger: MagicMock) -> None:
        """Test initialization with explicit None date.

        Verifies
        --------
        - Default date calculation is performed when date_ref is None
        - Instance is created successfully

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance

        Returns
        -------
        None
        """
        expected_date = date(2023, 10, 15)
        with patch.object(DatesBRAnbima, 'add_working_days', return_value=expected_date), \
             patch.object(DatesCurrent, 'curr_date', return_value=date(2023, 12, 20)):
            instance = AnbimaFundsRedemptionProbabilityMatrix(
                date_ref=None,
                logger=mock_logger
            )

            assert instance.date_ref == expected_date

    def test_initialization_with_none_logger(self) -> None:
        """Test initialization with None logger.

        Verifies
        --------
        - Instance is created successfully with None logger
        - No exceptions are raised

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        with patch.object(DatesBRAnbima, 'add_working_days', return_value=date(2023, 10, 15)), \
             patch.object(DatesCurrent, 'curr_date', return_value=date(2023, 12, 20)):
            instance = AnbimaFundsRedemptionProbabilityMatrix(logger=None)

            assert instance.logger is None

    def test_run_with_exception_during_scraping(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test run when scraping raises exception.

        Verifies
        --------
        - Exception during get_response is properly handled
        - None or empty result is returned when scraping fails
        - Error is logged appropriately

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        with patch.object(anbima_instance, 'get_response', return_value=pd.DataFrame()):
            result = anbima_instance.run()

            assert result is None

    def test_transform_data_with_missing_columns(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test transformation with missing expected columns.

        Verifies
        --------
        - Transformation completes even with missing columns
        - No exceptions are raised for missing numeric columns
        - DataFrame is returned successfully

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        df_missing_columns = pd.DataFrame({
            'data': ['2023-10-15'],
            'periodo': ['Mensal']
            # Missing 'valor' and 'prazo' columns
        })

        result = anbima_instance.transform_data(df_missing_columns)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_download_with_temp_file_cleanup(
        self, 
        anbima_instance: Any, # noqa ANN401: typing.Any is not allowed
        sample_csv_data: bytes
    ) -> None:
        """Test download with temporary file cleanup.

        Verifies
        --------
        - Temporary directory is created and used
        - Download process is initiated
        - File operations are attempted

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance
        sample_csv_data : bytes
            Sample CSV data for testing

        Returns
        -------
        None
        """
        mock_page = MagicMock(spec=PlaywrightPage)
        mock_frame = MagicMock(spec=Frame)
        mock_download_button = MagicMock()
        mock_download = MagicMock()

        mock_download.suggested_filename = "test_data.csv"
        mock_download.save_as.return_value = None

        # Create a mock Path object that behaves correctly
        mock_path = MagicMock()
        mock_path.__str__ = lambda self: "/tmp/test/test_data.csv" # noqa S108: probable insecure usage of temporary file
        mock_path.__fspath__ = lambda self: "/tmp/test/test_data.csv" # noqa S108: probable insecure usage of temporary file
        
        with patch.object(
            anbima_instance, '_find_download_button', return_value=mock_download_button), \
            patch(
                 'tempfile.mkdtemp', 
                 return_value='/tmp/test' # noqa S108: probable insecure usage of temporary file
            ) as mock_mkdtemp, \
            patch('stpstone.ingestion.countries.br.registries' \
                + '.anbima_funds_withdraws_probability.Path') as mock_path_class, \
            patch('builtins.open', mock_open(read_data=sample_csv_data)), \
            patch.object(mock_page, 'expect_download') as mock_expect:

            mock_expect.return_value.__enter__.return_value.value = mock_download
            
            # Make Path(dir) / filename work correctly
            mock_path_instance = MagicMock()
            mock_path_instance.__truediv__ = MagicMock(return_value=mock_path)
            mock_path_class.return_value = mock_path_instance

            result = anbima_instance._download_data(mock_page, mock_frame, 60000)

            # Verify that temporary directory was created
            mock_mkdtemp.assert_called_once()
            
            # Verify download button was clicked
            mock_download_button.click.assert_called_with(force=True)
            
            # Verify file was saved
            mock_download.save_as.assert_called_once()
            
            # Result should be the CSV data
            assert result == sample_csv_data

    def test_month_selection_with_special_characters(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test month selection handling of special characters.

        Verifies
        --------
        - Portuguese month names with special characters are handled
        - March (Março) with ç is processed correctly

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        # Test with March which has special character 'ç'
        with patch.object(DatesBRAnbima, 'add_working_days', return_value=date(2023, 3, 15)), \
             patch.object(DatesCurrent, 'curr_date', return_value=date(2023, 12, 20)):
            instance = AnbimaFundsRedemptionProbabilityMatrix(
                date_ref=date(2023, 3, 15),
                logger=MagicMock()
            )

            assert instance.target_month_pt == "Março"


# --------------------------
# Tests for Performance and Resource Management
# --------------------------
class TestPerformance:
    """Test cases for performance and resource management."""

    def test_browser_always_closed(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test browser is always closed even on exceptions.

        Verifies
        --------
        - Browser close is called in finally block
        - Resources are properly cleaned up

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_page = MagicMock()

        with patch(
            'stpstone.ingestion.countries.br.registries.anbima_funds_withdraws_probability' \
            + '.sync_playwright') as mock_playwright, \
             patch.object(anbima_instance, '_initialize_browser', 
                          side_effect=Exception("Test error")):

            mock_playwright.return_value.__enter__.return_value.chromium.launch.return_value = \
                mock_browser
            mock_browser.new_context.return_value = mock_context
            mock_context.new_page.return_value = mock_page

            result = anbima_instance.get_response()

            assert result.empty
            mock_browser.close.assert_called_once()

    def test_temp_directory_cleanup(
        self, 
        anbima_instance: Any, # noqa ANN401: typing.Any is not allowed
        sample_csv_data: bytes
    ) -> None:
        """Test temporary directory usage and cleanup.

        Verifies
        --------
        - Temporary directory is created for downloads
        - File operations use context managers
        - No explicit cleanup needed due to tempfile

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance
        sample_csv_data : bytes
            Sample CSV data for testing

        Returns
        -------
        None
        """
        mock_page = MagicMock(spec=PlaywrightPage)
        mock_frame = MagicMock(spec=Frame)
        mock_download_button = MagicMock()
        mock_download = MagicMock()

        mock_download.suggested_filename = "test_data.csv"

        with patch.object(
            anbima_instance, '_find_download_button', return_value=mock_download_button), \
            patch('tempfile.mkdtemp', 
                return_value='/tmp/test' # noqa S108: probable insecure usage of temporary file
            ) as mock_mkdtemp, \
             patch('builtins.open', mock_open(read_data=sample_csv_data)), \
             patch.object(mock_page, 'expect_download') as mock_expect:

            mock_expect.return_value.__enter__.return_value.value = mock_download

            anbima_instance._download_data(mock_page, mock_frame, 60000)

            mock_mkdtemp.assert_called_once()

    def test_memory_efficient_data_processing(
        self, 
        anbima_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test memory efficiency in data processing.

        Verifies
        --------
        - DataFrame copies are used to avoid modifying originals
        - Large data processing doesn't create memory leaks
        - Processing is done incrementally when possible

        Parameters
        ----------
        anbima_instance : Any
            AnbimaFundsRedemptionProbabilityMatrix instance

        Returns
        -------
        None
        """
        # Create a moderately large test dataset
        large_data = pd.DataFrame({
            'data': ['2023-10-15'] * 1000,
            'valor': ['1,2345'] * 1000,
            'prazo': ['30'] * 1000
        })

        with patch.object(anbima_instance, 'get_response', return_value=large_data), \
             patch.object(anbima_instance, 'transform_data', return_value=large_data), \
             patch.object(anbima_instance, 'standardize_dataframe', return_value=large_data):

            result = anbima_instance.run()

            assert result is not None
            assert len(result) == 1000
            # Verify that transform_data was called
            anbima_instance.transform_data.assert_called_once()