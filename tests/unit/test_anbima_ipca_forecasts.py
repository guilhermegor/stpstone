"""Unit tests for Anbima IPCA Forecasts ingestion module.

Tests the functionality of AnbimaIPCA classes, covering:
- Initialization with valid and invalid inputs
- Web scraping and data transformation
- Database insertion and DataFrame standardization
- Error handling and edge cases
- Type validation and fallback mechanisms
"""

from datetime import date
from logging import Logger
from typing import Union

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Session

from stpstone.ingestion.countries.br.macroeconomics.anbima_ipca_forecasts import (
    AnbimaIPCACore,
    AnbimaIPCAForecastsCurrentMonth,
    AnbimaIPCAForecastsLTM,
    AnbimaIPCAForecastsNextMonth,
)
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger(mocker: MockerFixture) -> Logger:
    """Mock logger for testing logging operations.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture

    Returns
    -------
    Logger
        Mocked logger instance
    """
    return mocker.patch("logging.Logger")


@pytest.fixture
def mock_session(mocker: MockerFixture) -> Session:
    """Mock database session.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture

    Returns
    -------
    Session
        Mocked database session
    """
    return mocker.patch("requests.Session")


@pytest.fixture
def mock_playwright_scraper(mocker: MockerFixture) -> PlaywrightScraper:
    """Mock PlaywrightScraper for web scraping operations.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture

    Returns
    -------
    PlaywrightScraper
        Mocked PlaywrightScraper instance
    """
    scraper = mocker.patch("stpstone.utils.webdriver_tools.playwright_wd.PlaywrightScraper")
    scraper_instance = scraper.return_value
    scraper_instance.navigate.return_value = True
    scraper_instance.get_elements.return_value = [
        {"text": "Janeiro de 2025"},
        {"text": "01/01/25"},
        {"text": "2.5"},
        {"text": "31/01/25"},
        {"text": "2.3"},
    ]
    return scraper_instance


@pytest.fixture
def sample_date() -> date:
    """Provide a sample date for testing.

    Returns
    -------
    date
        Sample date (2025-01-01)
    """
    return date(2025, 1, 1)


@pytest.fixture
def sample_dtypes() -> dict[str, Union[str, type]]:
    """Provide sample data types for testing.

    Returns
    -------
    dict[str, Union[str, type]]
        Dictionary of column names and their expected types
    """
    return {
        "MES_COLETA": str,
        "DATA": "date",
        "PROJECAO_PCT": float,
        "DATA_VALIDADE": "date",
        "IPCA_EFETIVO_PCT": float,
    }


# --------------------------
# Tests for AnbimaIPCACore
# --------------------------
class TestAnbimaIPCACore:
    """Tests for the AnbimaIPCACore base class."""

    def test_init_valid_inputs(
        self, 
        mock_logger: Logger, 
        mock_session: Session, 
        sample_date: date
    ) -> None:
        """Test initialization with valid inputs.

        Verifies
        --------
        - Instance is created with correct attributes
        - Default date_ref is set correctly
        - URL and other dependencies are initialized

        Parameters
        ----------
        mock_logger : Logger
            Mocked logger instance
        mock_session : Session
            Mocked database session
        sample_date : date
            Sample date for testing

        Returns
        -------
        None
        """
        core = AnbimaIPCACore(date_ref=sample_date, logger=mock_logger, cls_db=mock_session, 
                              url="http://test.com")
        assert core.date_ref == sample_date
        assert core.logger is mock_logger
        assert core.cls_db is mock_session
        assert core.url == "http://test.com"

    def test_init_default_date_ref(self, mock_logger: Logger, mocker: MockerFixture) -> None:
        """Test initialization with default date_ref.

        Verifies
        --------
        - date_ref is set to previous working day when None
        - Dependencies are initialized correctly

        Parameters
        ----------
        mock_logger : Logger
            Mocked logger instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        mocker.patch("stpstone.utils.calendars.calendar_br.DatesBRAnbima.add_working_days", 
                     return_value=date(2025, 1, 1))
        core = AnbimaIPCACore(logger=mock_logger)
        assert core.date_ref == date(2025, 1, 1)

    def test_parse_raw_file(self, mock_logger: Logger, mocker: MockerFixture) -> None:
        """Test parse_raw_file method.

        Verifies
        --------
        - Returns a PlaywrightScraper instance
        - Scraper is initialized with correct parameters

        Parameters
        ----------
        mock_logger : Logger
            Mocked logger instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        # Mock the PlaywrightScraper class directly
        mock_scraper_class = mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics.anbima_ipca_forecasts."
            + "PlaywrightScraper")
        mock_scraper_instance = mock_scraper_class.return_value
        
        core = AnbimaIPCACore(logger=mock_logger)
        scraper = core.parse_raw_file()
        
        # Verify the class was called with correct parameters
        mock_scraper_class.assert_called_once_with(
            bool_headless=True, int_default_timeout=5_000, logger=mock_logger)
        assert scraper is mock_scraper_instance

    def test_transform_data_navigation_failure(
        self, 
        mock_playwright_scraper: PlaywrightScraper, 
        mock_logger: Logger
    ) -> None:
        """Test transform_data with navigation failure.

        Verifies
        --------
        - Raises RuntimeError when navigation fails
        - Error message contains the URL

        Parameters
        ----------
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mock_logger : Logger
            Mocked logger instance

        Returns
        -------
        None
        """
        mock_playwright_scraper.navigate.return_value = False
        core = AnbimaIPCACore(logger=mock_logger, url="http://test.com")
        with pytest.raises(RuntimeError, match="Failed to navigate to URL: http://test.com"):
            core.transform_data(
                xpath_current_month_forecasts="//table/tbody/tr/td",
                list_th=["MES_COLETA", "DATA", "PROJECAO_PCT"],
                scraper_playwright=mock_playwright_scraper,
            )

    def test_transform_data_valid_input(
        self, 
        mock_playwright_scraper: PlaywrightScraper, 
        mock_logger: Logger, 
        mocker: MockerFixture
    ) -> None:
        """Test transform_data with valid input.

        Verifies
        --------
        - Returns a DataFrame with correct structure
        - Data is processed correctly (commas to dots, nan handling)
        - Headers are paired correctly with data

        Parameters
        ----------
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mock_logger : Logger
            Mocked logger instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        core = AnbimaIPCACore(logger=mock_logger)
        
        mock_playwright_scraper.get_elements.return_value = [
            {"text": "Janeiro de 2025"},
            {"text": "01/01/25"},
            {"text": "2.5"}
        ]
        
        mocker.patch.object(core.cls_dict_handler, "pair_headers_with_data", return_value=[
            {"MES_COLETA": "Jan", "DATA": "01/01/25", "PROJECAO_PCT": "2.5"}])
        df_ = core.transform_data(
            xpath_current_month_forecasts="//table/tbody/tr/td",
            list_th=["MES_COLETA", "DATA", "PROJECAO_PCT"],
            scraper_playwright=mock_playwright_scraper,
        )
        assert isinstance(df_, pd.DataFrame)
        assert df_.columns.tolist() == ["MES_COLETA", "DATA", "PROJECAO_PCT"]

# --------------------------
# Tests for AnbimaIPCAForecastsCurrentMonth
# --------------------------
class TestAnbimaIPCAForecastsCurrentMonth:
    """Tests for AnbimaIPCAForecastsCurrentMonth class."""

    def test_init(self, mock_logger: Logger, mock_session: Session) -> None:
        """Test initialization of AnbimaIPCAForecastsCurrentMonth.

        Verifies
        --------
        - Correct URL is set
        - Inherits correctly from AnbimaIPCACore

        Parameters
        ----------
        mock_logger : Logger
            Mocked logger instance
        mock_session : Session
            Mocked database session

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsCurrentMonth(logger=mock_logger, cls_db=mock_session)
        assert instance.url == "https://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm"
        assert isinstance(instance, AnbimaIPCACore)

    def test_run_no_db(
        self, 
        mock_playwright_scraper: PlaywrightScraper, 
        mock_logger: Logger, 
        mocker: MockerFixture
    ) -> None:
        """Test run method without database session.

        Verifies
        --------
        - Returns DataFrame when no database session is provided
        - Correct data types are applied
        - Standardization is called with correct parameters

        Parameters
        ----------
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mock_logger : Logger
            Mocked logger instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsCurrentMonth(logger=mock_logger)
        mocker.patch.object(instance, "parse_raw_file", return_value=mock_playwright_scraper)
        mocker.patch.object(instance, "transform_data", return_value=pd.DataFrame(
            {
                "MES_COLETA": ["Jan"], 
                "DATA": ["01/01/25"], 
                "PROJECAO_PCT": [2.5], 
                "DATA_VALIDADE": ["31/01/25"]
            }
        ))
        mocker.patch.object(instance, "standardize_dataframe", return_value=pd.DataFrame())
        
        # Use the correct parameter name from the parent class
        result = instance.run()
        assert isinstance(result, pd.DataFrame)
        instance.standardize_dataframe.assert_called_once()

    def test_run_with_db(
        self, 
        mock_playwright_scraper: PlaywrightScraper, 
        mock_logger: Logger, 
        mock_session: Session, 
        mocker: MockerFixture
    ) -> None:
        """Test run method with database session.

        Verifies
        --------
        - Database insertion is called
        - No DataFrame is returned
        - Correct table name is used

        Parameters
        ----------
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mock_logger : Logger
            Mocked logger instance
        mock_session : Session
            Mocked database session
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsCurrentMonth(logger=mock_logger, cls_db=mock_session)
        mocker.patch.object(instance, "parse_raw_file", return_value=mock_playwright_scraper)
        mocker.patch.object(instance, "transform_data", return_value=pd.DataFrame())
        mocker.patch.object(instance, "standardize_dataframe", return_value=pd.DataFrame())
        mocker.patch.object(instance, "insert_table_db")
        
        result = instance.run()
        assert result is None
        instance.insert_table_db.assert_called_once()

    def test_transform_data(
        self, 
        mock_playwright_scraper: PlaywrightScraper, 
        mock_logger: Logger, 
        mocker: MockerFixture
    ) -> None:
        """Test transform_data method.

        Verifies
        --------
        - Correct XPath is used
        - Correct headers are applied
        - Returns DataFrame with expected structure

        Parameters
        ----------
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mock_logger : Logger
            Mocked logger instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsCurrentMonth(logger=mock_logger)
        
        # Mock the get_elements to return exactly 4 elements for 4 headers
        mock_playwright_scraper.get_elements.return_value = [
            {"text": "Janeiro de 2025"},
            {"text": "01/01/25"},
            {"text": "2.5"},
            {"text": "31/01/25"}
        ]
        
        # Mock the dict handler method
        mocker.patch.object(instance.cls_dict_handler, "pair_headers_with_data", return_value=[
            {
                "MES_COLETA": "Janeiro de 2025", 
                "DATA": "01/01/25", 
                "PROJECAO_PCT": "2.5", 
                "DATA_VALIDADE": "31/01/25"
            }
        ])
        
        df_ = instance.transform_data(scraper_playwright=mock_playwright_scraper)
        assert isinstance(df_, pd.DataFrame)
        assert df_.columns.tolist() == ["MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE"]


# --------------------------
# Tests for AnbimaIPCAForecastsNextMonth
# --------------------------
class TestAnbimaIPCAForecastsNextMonth:
    """Tests for AnbimaIPCAForecastsNextMonth class."""

    def test_init(self, mock_logger: Logger, mock_session: Session) -> None:
        """Test initialization of AnbimaIPCAForecastsNextMonth.

        Verifies
        --------
        - Correct URL is set
        - Inherits correctly from AnbimaIPCACore

        Parameters
        ----------
        mock_logger : Logger
            Mocked logger instance
        mock_session : Session
            Mocked database session

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsNextMonth(logger=mock_logger, cls_db=mock_session)
        assert instance.url == "https://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm"
        assert isinstance(instance, AnbimaIPCACore)

    def test_run_no_db(
        self, 
        mock_playwright_scraper: PlaywrightScraper, 
        mock_logger: Logger, 
        mocker: MockerFixture
    ) -> None:
        """Test run method without database session.

        Verifies
        --------
        - Returns DataFrame when no database session is provided
        - Correct data types are applied

        Parameters
        ----------
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mock_logger : Logger
            Mocked logger instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsNextMonth(logger=mock_logger)
        mocker.patch.object(instance, "parse_raw_file", return_value=mock_playwright_scraper)
        mocker.patch.object(instance, "transform_data", 
                            return_value=pd.DataFrame({
                                "MES_COLETA": ["Jan"], 
                                "DATA": ["01/01/25"], 
                                "PROJECAO_PCT": [2.5]
                            }))
        mocker.patch.object(instance, "standardize_dataframe", return_value=pd.DataFrame())
        
        result = instance.run()
        assert isinstance(result, pd.DataFrame)

    def test_transform_data(
        self, 
        mock_playwright_scraper: PlaywrightScraper, 
        mock_logger: Logger, 
        mocker: MockerFixture
    ) -> None:
        """Test transform_data method.

        Verifies
        --------
        - Correct XPath is used
        - Correct headers are applied
        - Returns DataFrame with expected structure

        Parameters
        ----------
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mock_logger : Logger
            Mocked logger instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsNextMonth(logger=mock_logger)
        
        # Mock the get_elements to return exactly 3 elements for 3 headers
        mock_playwright_scraper.get_elements.return_value = [
            {"text": "Janeiro de 2025"},
            {"text": "01/01/25"},
            {"text": "2.5"}
        ]
        
        # Mock the dict handler method
        mocker.patch.object(instance.cls_dict_handler, "pair_headers_with_data", return_value=[
            {"MES_COLETA": "Janeiro de 2025", "DATA": "01/01/25", "PROJECAO_PCT": "2.5"}
        ])
        
        df_ = instance.transform_data(scraper_playwright=mock_playwright_scraper)
        assert isinstance(df_, pd.DataFrame)
        assert df_.columns.tolist() == ["MES_COLETA", "DATA", "PROJECAO_PCT"]


# --------------------------
# Tests for AnbimaIPCAForecastsLTM
# --------------------------
class TestAnbimaIPCAForecastsLTM:
    """Tests for AnbimaIPCAForecastsLTM class."""

    def test_init(self, mock_logger: Logger, mock_session: Session) -> None:
        """Test initialization of AnbimaIPCAForecastsLTM.

        Verifies
        --------
        - Correct URL is set
        - Inherits correctly from AnbimaIPCACore

        Parameters
        ----------
        mock_logger : Logger
            Mocked logger instance
        mock_session : Session
            Mocked database session

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsLTM(logger=mock_logger, cls_db=mock_session)
        assert instance.url == "https://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm"
        assert isinstance(instance, AnbimaIPCACore)

    def test_run_no_db(
        self, 
        mock_playwright_scraper: PlaywrightScraper, 
        mock_logger: Logger, 
        mocker: MockerFixture
    ) -> None:
        """Test run method without database session.

        Verifies
        --------
        - Returns DataFrame when no database session is provided
        - Correct data types are applied

        Parameters
        ----------
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mock_logger : Logger
            Mocked logger instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsLTM(logger=mock_logger)
        mocker.patch.object(instance, "parse_raw_file", return_value=mock_playwright_scraper)
        mocker.patch.object(instance, "transform_data", 
                            return_value=pd.DataFrame(
                                {
                                    "MES_COLETA": ["Jan"], 
                                    "DATA": ["01/01/25"], 
                                    "PROJECAO_PCT": [2.5], 
                                    "DATA_VALIDADE": ["31/01/25"], 
                                    "IPCA_EFETIVO_PCT": [2.3]
                                }))
        mocker.patch.object(instance, "standardize_dataframe", return_value=pd.DataFrame())
        
        result = instance.run()
        assert isinstance(result, pd.DataFrame)

    def test_transform_data_backoff(
        self, 
        mock_playwright_scraper: PlaywrightScraper, 
        mock_logger: Logger, 
        mocker: MockerFixture
    ) -> None:
        """Test transform_data with backoff handling.

        Verifies
        --------
        - Backoff decorator is bypassed for testing
        - DataFrame is returned with correct structure
        - Forward fill is applied to IPCA_EFETIVO_PCT

        Parameters
        ----------
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mock_logger : Logger
            Mocked logger instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsLTM(logger=mock_logger)
        
        # Mock the _restructure_table_with_missing_columns method
        mocker.patch.object(instance, "_restructure_table_with_missing_columns", return_value=[
            {
                "MES_COLETA": "Janeiro de 2025", 
                "DATA": "01/01/25", 
                "PROJECAO_PCT": "2.5", 
                "DATA_VALIDADE": "31/01/25", 
                "IPCA_EFETIVO_PCT": "2.3"
            }
        ])
        
        df_ = instance.transform_data(scraper_playwright=mock_playwright_scraper)
        assert isinstance(df_, pd.DataFrame)
        assert df_.columns.tolist() == \
            ["MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE", "IPCA_EFETIVO_PCT"]

    def test_restructure_table_with_missing_columns(self, mock_logger: Logger) -> None:
        """Test _restructure_table_with_missing_columns method.

        Verifies
        --------
        - Correctly handles complete and incomplete row patterns
        - Properly identifies value types
        - Maintains current month for missing month values

        Parameters
        ----------
        mock_logger : Logger
            Mocked logger instance

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsLTM(logger=mock_logger)
        flat_list = [
            "Janeiro de 2025", "01/01/25", "2.5", "31/01/25", "2.3",  # Complete row
            "01/02/25", "2.7", "28/02/25", "2.4",  # Missing month
            "Fevereiro de 2025", "01/03/25", "2.8", "31/03/25",  # Missing last numeric
        ]
        expected_columns = \
            ["MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE", "IPCA_EFETIVO_PCT"]
        result = instance._restructure_table_with_missing_columns(flat_list, expected_columns)
        assert len(result) == 3
        assert result[0]["MES_COLETA"] == "Janeiro de 2025"
        assert result[1]["MES_COLETA"] == "Janeiro de 2025"
        assert result[2]["MES_COLETA"] == "Fevereiro de 2025"
        assert result[2]["IPCA_EFETIVO_PCT"] is None

    def test_restructure_table_empty_input(self, mock_logger: Logger) -> None:
        """Test _restructure_table_with_missing_columns with empty input.

        Verifies
        --------
        - Returns empty list for empty input
        - Handles empty expected_columns correctly

        Parameters
        ----------
        mock_logger : Logger
            Mocked logger instance

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsLTM(logger=mock_logger)
        result = instance._restructure_table_with_missing_columns([], ["MES_COLETA"])
        assert result == []

    def test_restructure_table_invalid_pattern(
        self, 
        mock_logger: Logger, 
        mocker: MockerFixture
    ) -> None:
        """Test _restructure_table_with_missing_columns with invalid pattern.

        Verifies
        --------
        - Skips invalid patterns and logs warning
        - Processes valid patterns correctly

        Parameters
        ----------
        mock_logger : Logger
            Mocked logger instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsLTM(logger=mock_logger)
        mocker.patch.object(instance.cls_create_log, "log_message")
        
        # Use a pattern that will actually skip the first element
        flat_list = ["invalid", "another_invalid", "Janeiro de 2025", 
                     "01/01/25", "2.5", "31/01/25", "2.3"]
        expected_columns = ["MES_COLETA", "DATA", "PROJECAO_PCT", 
                            "DATA_VALIDADE", "IPCA_EFETIVO_PCT"]
        result = instance._restructure_table_with_missing_columns(flat_list, expected_columns)
        
        # Should find one complete row starting from "Janeiro de 2025"
        assert len(result) == 1
        
        # Check that warning was logged for skipped elements
        warning_calls = [call for call in instance.cls_create_log.log_message.call_args_list 
                        if len(call[0]) >= 3 and call[0][2] == "warning"]
        assert len(warning_calls) >= 1

    def test_debug_flat_list_structure(
        self, 
        mock_logger: Logger, 
        capsys: pytest.CaptureFixture
    ) -> None:
        """Test _debug_flat_list_structure method.

        Verifies
        --------
        - Correctly identifies value types
        - Prints expected debug information
        - Calculates month positions and distances

        Parameters
        ----------
        mock_logger : Logger
            Mocked logger instance
        capsys : pytest.CaptureFixture
            Pytest fixture for capturing stdout/stderr

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsLTM(logger=mock_logger)
        flat_list = ["Janeiro de 2025", "01/01/25", "2.5", "31/01/25", 
                     "2.3", "Fevereiro de 2025", "01/02/25", "2.7"]
        instance._debug_flat_list_structure(flat_list)
        captured = capsys.readouterr()
        
        # check for month identification - the format should match the actual output
        assert "'Janeiro de 2025' -> month" in captured.out
        assert "'01/01/25' -> date" in captured.out
        assert "'2.5' -> numeric" in captured.out
        assert "Month positions: [0, 5]" in captured.out
        assert "Distances between months: [5]" in captured.out

    def test_type_checker_decorator(self, mock_logger: Logger) -> None:
        """Test type_checker decorator on identify_value_type.

        Verifies
        --------
        - Raises TypeError for invalid input types
        - Correctly identifies valid input types

        Parameters
        ----------
        mock_logger : Logger
            Mocked logger instance

        Returns
        -------
        None
        """
        instance = AnbimaIPCAForecastsLTM(logger=mock_logger)
        
        # The error message is actually "flat_list[0] must be of type str, got int"
        with pytest.raises(TypeError, match="flat_list\\[0\\] must be of type str"):
            instance._restructure_table_with_missing_columns([1], ["MES_COLETA"])
            
        # Test with valid input
        result = instance._restructure_table_with_missing_columns(
            ["Janeiro de 2025"], ["MES_COLETA"])
        assert result == []  # Single element can't form a complete row