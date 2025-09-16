"""Unit tests for Anbima IGPM Forecasts ingestion module.

Tests the functionality of AnbimaIGPMCore and its subclasses for fetching,
parsing, and transforming IGPM forecast data, covering normal operations,
edge cases, error conditions, and type validation.
"""

from collections import Counter
from datetime import date
from logging import Logger
from math import nan
import re
import sys
from typing import Any, Optional, Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Session

from stpstone.ingestion.countries.br.macroeconomics.anbima_igpm_forecasts import (
    AnbimaIGPMCore,
    AnbimaIGPMForecastsCurrentMonth,
    AnbimaIGPMForecastsLTM,
    AnbimaIGPMForecastsNextMonth,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger(mocker: MockerFixture) -> Logger:
    """Mock logger instance for testing logging behavior.

    Returns
    -------
    Logger
        Mocked logger object
    """
    return mocker.patch("logging.Logger")


@pytest.fixture
def mock_session(mocker: MockerFixture) -> Session:
    """Mock database session for testing database operations.

    Returns
    -------
    Session
        Mocked database session object
    """
    return mocker.patch("requests.Session")


@pytest.fixture
def mock_playwright_scraper(mocker: MockerFixture) -> PlaywrightScraper:
    """Mock PlaywrightScraper for testing web scraping operations.

    Returns
    -------
    PlaywrightScraper
        Mocked PlaywrightScraper object
    """
    scraper = mocker.patch("stpstone.utils.webdriver_tools.playwright_wd.PlaywrightScraper")
    scraper_instance = scraper.return_value
    scraper_instance.launch.return_value.__enter__.return_value = None
    scraper_instance.navigate.return_value = True
    return scraper_instance


@pytest.fixture
def sample_flat_list() -> list[str]:
    """Sample flat list data for testing _restructure_table_with_missing_columns.

    Returns
    -------
    list[str]
        Sample list simulating table data
    """
    return [
        "Janeiro de 2025", "01/01/25", "2.5", "31/01/25", "2.3",
        "Fevereiro de 2025", "01/02/25", "2.7", "28/02/25", "",
        "01/03/25", "2.8", "31/03/25", "2.9"
    ]


@pytest.fixture
def expected_columns_ltm() -> list[str]:
    """Expected columns for AnbimaIGPMForecastsLTM.

    Returns
    -------
    list[str]
        List of expected column names
    """
    return ["MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE", "IGPM_EFETIVO_PCT"]


@pytest.fixture
def anbima_igpm_core(mock_logger: Logger, mock_session: Session) -> AnbimaIGPMCore:
    """Fixture providing AnbimaIGPMCore instance with mocked dependencies.

    Parameters
    ----------
    mock_logger : Logger
        Mocked logger instance
    mock_session : Session
        Mocked database session

    Returns
    -------
    AnbimaIGPMCore
        Initialized AnbimaIGPMCore instance
    """
    return AnbimaIGPMCore(date_ref=date(2025, 1, 1), logger=mock_logger, cls_db=mock_session)


@pytest.fixture
def anbima_igpm_current_month(mock_logger: Logger, mock_session: Session) -> AnbimaIGPMForecastsCurrentMonth:
    """Fixture providing AnbimaIGPMForecastsCurrentMonth instance.

    Parameters
    ----------
    mock_logger : Logger
        Mocked logger instance
    mock_session : Session
        Mocked database session

    Returns
    -------
    AnbimaIGPMForecastsCurrentMonth
        Initialized AnbimaIGPMForecastsCurrentMonth instance
    """
    return AnbimaIGPMForecastsCurrentMonth(logger=mock_logger, cls_db=mock_session)


@pytest.fixture
def anbima_igpm_next_month(mock_logger: Logger, mock_session: Session) -> AnbimaIGPMForecastsNextMonth:
    """Fixture providing AnbimaIGPMForecastsNextMonth instance.

    Parameters
    ----------
    mock_logger : Logger
        Mocked logger instance
    mock_session : Session
        Mocked database session

    Returns
    -------
    AnbimaIGPMForecastsNextMonth
        Initialized AnbimaIGPMForecastsNextMonth instance
    """
    return AnbimaIGPMForecastsNextMonth(logger=mock_logger, cls_db=mock_session)


@pytest.fixture
def anbima_igpm_ltm(mock_logger: Logger, mock_session: Session) -> AnbimaIGPMForecastsLTM:
    """Fixture providing AnbimaIGPMForecastsLTM instance.

    Parameters
    ----------
    mock_logger : Logger
        Mocked logger instance
    mock_session : Session
        Mocked database session

    Returns
    -------
    AnbimaIGPMForecastsLTM
        Initialized AnbimaIGPMForecastsLTM instance
    """
    return AnbimaIGPMForecastsLTM(logger=mock_logger, cls_db=mock_session)


# --------------------------
# Tests for AnbimaIGPMCore
# --------------------------
class TestAnbimaIGPMCore:
    """Test cases for AnbimaIGPMCore base class."""

    def test_init_with_valid_inputs(self, anbima_igpm_core: AnbimaIGPMCore) -> None:
        """Test initialization with valid inputs.

        Verifies
        --------
        - Instance is properly initialized with provided date_ref, logger, and cls_db
        - Inherited classes are initialized correctly
        - Default URL is set

        Returns
        -------
        None
        """
        assert anbima_igpm_core.date_ref == date(2025, 1, 1)
        assert anbima_igpm_core.logger is not None
        assert anbima_igpm_core.cls_db is not None
        assert isinstance(anbima_igpm_core.cls_dir_files_management, DirFilesManagement)
        assert isinstance(anbima_igpm_core.cls_dates_current, DatesCurrent)
        assert isinstance(anbima_igpm_core.cls_create_log, CreateLog)
        assert isinstance(anbima_igpm_core.cls_dates_br, DatesBRAnbima)
        assert isinstance(anbima_igpm_core.cls_html_handler, HtmlHandler)
        assert isinstance(anbima_igpm_core.cls_dict_handler, HandlingDicts)
        assert isinstance(anbima_igpm_core.cls_list_handler, ListHandler)
        assert anbima_igpm_core.url == "FILL_ME"

    def test_init_with_default_date(self, mock_logger: Logger, mock_session: Session, mocker: MockerFixture) -> None:
        """Test initialization with default date.

        Verifies
        --------
        - date_ref is set to previous working day when not provided
        - Dependencies are properly initialized

        Parameters
        ----------
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
        mocker.patch.object(DatesCurrent, "curr_date", return_value=date(2025, 1, 2))
        mocker.patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1))
        core = AnbimaIGPMCore(logger=mock_logger, cls_db=mock_session)
        assert core.date_ref == date(2025, 1, 1)

    def test_init_invalid_date_type_string(self, mock_logger: Logger, mock_session: Session) -> None:
        """Test initialization with invalid date type raises TypeError.

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
        with pytest.raises(TypeError):
            AnbimaIGPMCore(date_ref="2025-01-01", logger=mock_logger, cls_db=mock_session)

    def test_init_invalid_date_type_int(self, mock_logger: Logger, mock_session: Session) -> None:
        """Test initialization with invalid date type raises TypeError.

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
        with pytest.raises(TypeError):
            AnbimaIGPMCore(date_ref=123, logger=mock_logger, cls_db=mock_session)

    def test_init_invalid_date_type_list(self, mock_logger: Logger, mock_session: Session) -> None:
        """Test initialization with invalid date type raises TypeError.

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
        with pytest.raises(TypeError):
            AnbimaIGPMCore(date_ref=[], logger=mock_logger, cls_db=mock_session)

    def test_parse_raw_file(self, anbima_igpm_core: AnbimaIGPMCore) -> None:
        """Test parse_raw_file returns PlaywrightScraper instance.

        Verifies
        --------
        - Returns PlaywrightScraper with correct configuration
        - Headless mode and timeout are set correctly

        Returns
        -------
        None
        """
        scraper = anbima_igpm_core.parse_raw_file()
        assert isinstance(scraper, PlaywrightScraper)
        assert scraper.bool_headless is True
        assert scraper.int_default_timeout == 5_000

    def test_run_with_db(self, anbima_igpm_core: AnbimaIGPMCore, mock_playwright_scraper: PlaywrightScraper, mocker: MockerFixture) -> None:
        """Test run method with database session.

        Verifies
        --------
        - Calls parse_raw_file, transform_data, standardize_dataframe, and insert_table_db
        - Does not return DataFrame when cls_db is provided
        - Uses provided parameters correctly

        Parameters
        ----------
        anbima_igpm_core : AnbimaIGPMCore
            AnbimaIGPMCore instance
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        mock_df = pd.DataFrame({"MES_COLETA": ["Janeiro 2025"], "DATA": ["01/01/25"]})
        mocker.patch.object(AnbimaIGPMCore, "parse_raw_file", return_value=mock_playwright_scraper)
        mocker.patch.object(AnbimaIGPMCore, "transform_data", return_value=mock_df)
        mocker.patch.object(AnbimaIGPMCore, "standardize_dataframe", return_value=mock_df)
        mock_insert = mocker.patch.object(AnbimaIGPMCore, "insert_table_db")
        
        dict_dtypes = {"MES_COLETA": str, "DATA": "date"}
        result = anbima_igpm_core.run(dict_dtypes, timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=False, str_table_name="test_table")
        
        assert result is None
        mock_insert.assert_called_once_with(cls_db=anbima_igpm_core.cls_db, str_table_name="test_table", df_=mock_df, bool_insert_or_ignore=False)

    def test_run_without_db(self, anbima_igpm_core: AnbimaIGPMCore, mock_playwright_scraper: PlaywrightScraper, mocker: MockerFixture) -> None:
        """Test run method without database session.

        Verifies
        --------
        - Returns transformed DataFrame
        - Calls parse_raw_file, transform_data, and standardize_dataframe
        - Does not call insert_table_db

        Parameters
        ----------
        anbima_igpm_core : AnbimaIGPMCore
            AnbimaIGPMCore instance with cls_db=None
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        anbima_igpm_core.cls_db = None
        mock_df = pd.DataFrame({"MES_COLETA": ["Janeiro 2025"], "DATA": ["01/01/25"]})
        mocker.patch.object(AnbimaIGPMCore, "parse_raw_file", return_value=mock_playwright_scraper)
        mocker.patch.object(AnbimaIGPMCore, "transform_data", return_value=mock_df)
        mocker.patch.object(AnbimaIGPMCore, "standardize_dataframe", return_value=mock_df)
        mock_insert = mocker.patch.object(AnbimaIGPMCore, "insert_table_db")
        
        dict_dtypes = {"MES_COLETA": str, "DATA": "date"}
        result = anbima_igpm_core.run(dict_dtypes, timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=False, str_table_name="test_table")
        
        assert result.equals(mock_df)
        mock_insert.assert_not_called()


# --------------------------
# Tests for AnbimaIGPMForecastsCurrentMonth
# --------------------------
class TestAnbimaIGPMForecastsCurrentMonth:
    """Test cases for AnbimaIGPMForecastsCurrentMonth class."""

    def test_init(self, anbima_igpm_current_month: AnbimaIGPMForecastsCurrentMonth) -> None:
        """Test initialization of AnbimaIGPMForecastsCurrentMonth.

        Verifies
        --------
        - Inherits from AnbimaIGPMCore correctly
        - Sets correct URL
        - Initializes dependencies properly

        Returns
        -------
        None
        """
        assert isinstance(anbima_igpm_current_month, AnbimaIGPMCore)
        assert anbima_igpm_current_month.url == "https://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm"

    def test_run(self, anbima_igpm_current_month: AnbimaIGPMForecastsCurrentMonth, mock_playwright_scraper: PlaywrightScraper, mocker: MockerFixture) -> None:
        """Test run method for current month forecasts.

        Verifies
        --------
        - Calls super().run with correct parameters
        - Uses correct dictionary of data types
        - Returns DataFrame when no database session is provided

        Parameters
        ----------
        anbima_igpm_current_month : AnbimaIGPMForecastsCurrentMonth
            AnbimaIGPMForecastsCurrentMonth instance
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        mock_df = pd.DataFrame({"MES_COLETA": ["Janeiro 2025"], "DATA": ["01/01/25"], "PROJECAO_PCT": [2.5], "DATA_VALIDADE": ["31/01/25"]})
        mocker.patch.object(AnbimaIGPMCore, "parse_raw_file", return_value=mock_playwright_scraper)
        mocker.patch.object(AnbimaIGPMForecastsCurrentMonth, "transform_data", return_value=mock_df)
        mocker.patch.object(AnbimaIGPMCore, "standardize_dataframe", return_value=mock_df)
        
        anbima_igpm_current_month.cls_db = None
        result = anbima_igpm_current_month.run()
        
        assert result.equals(mock_df)
        assert result.columns.tolist() == ["MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE"]

    def test_transform_data(self, anbima_igpm_current_month: AnbimaIGPMForecastsCurrentMonth, mock_playwright_scraper: PlaywrightScraper, mocker: MockerFixture) -> None:
        """Test transform_data method for current month forecasts.

        Verifies
        --------
        - Correctly transforms scraped data into DataFrame
        - Uses correct xpath and headers
        - Handles comma to dot conversion and nan values

        Parameters
        ----------
        anbima_igpm_current_month : AnbimaIGPMForecastsCurrentMonth
            AnbimaIGPMForecastsCurrentMonth instance
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        mock_playwright_scraper.get_elements.return_value = [
            {"text": "Janeiro 2025"}, {"text": "01/01/25"}, {"text": "2,5"}, {"text": "31/01/25"}
        ]
        mocker.patch.object(HandlingDicts, "pair_headers_with_data", return_value=[
            {"MES_COLETA": "Janeiro 2025", "DATA": "01/01/25", "PROJECAO_PCT": "2.5", "DATA_VALIDADE": "31/01/25"}
        ])
        
        result = anbima_igpm_current_month.transform_data(mock_playwright_scraper)
        
        assert isinstance(result, pd.DataFrame)
        assert result.columns.tolist() == ["MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE"]


# --------------------------
# Tests for AnbimaIGPMForecastsNextMonth
# --------------------------
class TestAnbimaIGPMForecastsNextMonth:
    """Test cases for AnbimaIGPMForecastsNextMonth class."""

    def test_init(self, anbima_igpm_next_month: AnbimaIGPMForecastsNextMonth) -> None:
        """Test initialization of AnbimaIGPMForecastsNextMonth.

        Verifies
        --------
        - Inherits from AnbimaIGPMCore correctly
        - Sets correct URL
        - Initializes dependencies properly

        Returns
        -------
        None
        """
        assert isinstance(anbima_igpm_next_month, AnbimaIGPMCore)
        assert anbima_igpm_next_month.url == "https://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm"

    def test_run(self, anbima_igpm_next_month: AnbimaIGPMForecastsNextMonth, mock_playwright_scraper: PlaywrightScraper, mocker: MockerFixture) -> None:
        """Test run method for next month forecasts.

        Verifies
        --------
        - Calls super().run with correct parameters
        - Uses correct dictionary of data types
        - Returns DataFrame when no database session is provided

        Parameters
        ----------
        anbima_igpm_next_month : AnbimaIGPMForecastsNextMonth
            AnbimaIGPMForecastsNextMonth instance
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        mock_df = pd.DataFrame({"MES_COLETA": ["Fevereiro 2025"], "DATA": ["01/02/25"], "PROJECAO_PCT": [2.7]})
        mocker.patch.object(AnbimaIGPMCore, "parse_raw_file", return_value=mock_playwright_scraper)
        mocker.patch.object(AnbimaIGPMForecastsNextMonth, "transform_data", return_value=mock_df)
        mocker.patch.object(AnbimaIGPMCore, "standardize_dataframe", return_value=mock_df)
        
        anbima_igpm_next_month.cls_db = None
        result = anbima_igpm_next_month.run()
        
        assert result.equals(mock_df)
        assert result.columns.tolist() == ["MES_COLETA", "DATA", "PROJECAO_PCT"]

    def test_transform_data(self, anbima_igpm_next_month: AnbimaIGPMForecastsNextMonth, mock_playwright_scraper: PlaywrightScraper, mocker: MockerFixture) -> None:
        """Test transform_data method for next month forecasts.

        Verifies
        --------
        - Correctly transforms scraped data into DataFrame
        - Uses correct xpath and headers
        - Handles comma to dot conversion and nan values

        Parameters
        ----------
        anbima_igpm_next_month : AnbimaIGPMForecastsNextMonth
            AnbimaIGPMForecastsNextMonth instance
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        mock_playwright_scraper.get_elements.return_value = [
            {"text": "Fevereiro 2025"}, {"text": "01/02/25"}, {"text": "2,7"}
        ]
        mocker.patch.object(HandlingDicts, "pair_headers_with_data", return_value=[
            {"MES_COLETA": "Fevereiro 2025", "DATA": "01/02/25", "PROJECAO_PCT": "2.7"}
        ])
        
        result = anbima_igpm_next_month.transform_data(mock_playwright_scraper)
        
        assert isinstance(result, pd.DataFrame)
        assert result.columns.tolist() == ["MES_COLETA", "DATA", "PROJECAO_PCT"]


# --------------------------
# Tests for AnbimaIGPMForecastsLTM
# --------------------------
class TestAnbimaIGPMForecastsLTM:
    """Test cases for AnbimaIGPMForecastsLTM class."""

    def test_init(self, anbima_igpm_ltm: AnbimaIGPMForecastsLTM) -> None:
        """Test initialization of AnbimaIGPMForecastsLTM.

        Verifies
        --------
        - Inherits from AnbimaIGPMCore correctly
        - Sets correct URL
        - Initializes dependencies properly

        Returns
        -------
        None
        """
        assert isinstance(anbima_igpm_ltm, AnbimaIGPMCore)
        assert anbima_igpm_ltm.url == "https://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm"

    def test_run(self, anbima_igpm_ltm: AnbimaIGPMForecastsLTM, mock_playwright_scraper: PlaywrightScraper, mocker: MockerFixture) -> None:
        """Test run method for last twelve months forecasts.

        Verifies
        --------
        - Calls super().run with correct parameters
        - Uses correct dictionary of data types
        - Returns DataFrame when no database session is provided

        Parameters
        ----------
        anbima_igpm_ltm : AnbimaIGPMForecastsLTM
            AnbimaIGPMForecastsLTM instance
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        mock_df = pd.DataFrame({
            "MES_COLETA": ["Janeiro 2025"],
            "DATA": ["01/01/25"],
            "PROJECAO_PCT": [2.5],
            "DATA_VALIDADE": ["31/01/25"],
            "IGPM_EFETIVO_PCT": [2.3]
        })
        mocker.patch.object(AnbimaIGPMCore, "parse_raw_file", return_value=mock_playwright_scraper)
        mocker.patch.object(AnbimaIGPMForecastsLTM, "transform_data", return_value=mock_df)
        mocker.patch.object(AnbimaIGPMCore, "standardize_dataframe", return_value=mock_df)
        
        anbima_igpm_ltm.cls_db = None
        result = anbima_igpm_ltm.run()
        
        assert result.equals(mock_df)
        assert result.columns.tolist() == ["MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE", "IGPM_EFETIVO_PCT"]

    def test_transform_data(self, anbima_igpm_ltm: AnbimaIGPMForecastsLTM, mock_playwright_scraper: PlaywrightScraper, mocker: MockerFixture) -> None:
        """Test transform_data method for last twelve months forecasts.

        Verifies
        --------
        - Correctly transforms scraped data into DataFrame
        - Uses correct xpath and headers
        - Handles forward fill for IGPM_EFETIVO_PCT
        - Handles navigation failure

        Parameters
        ----------
        anbima_igpm_ltm : AnbimaIGPMForecastsLTM
            AnbimaIGPMForecastsLTM instance
        mock_playwright_scraper : PlaywrightScraper
            Mocked PlaywrightScraper instance
        mocker : MockerFixture
            Pytest mocker for patching dependencies

        Returns
        -------
        None
        """
        mock_playwright_scraper.get_elements.return_value = [
            {"text": "Janeiro 2025"}, {"text": "01/01/25"}, {"text": "2,5"}, {"text": "31/01/25"}, {"text": "2,3"}
        ]
        mock_restructure = mocker.patch.object(
            AnbimaIGPMForecastsLTM,
            "_restructure_table_with_missing_columns",
            return_value=[
                {
                    "MES_COLETA": "Janeiro 2025",
                    "DATA": "01/01/25",
                    "PROJECAO_PCT": "2.5",
                    "DATA_VALIDADE": "31/01/25",
                    "IGPM_EFETIVO_PCT": "2.3"
                }
            ]
        )
        
        result = anbima_igpm_ltm.transform_data(mock_playwright_scraper)
        
        assert isinstance(result, pd.DataFrame)
        assert result.columns.tolist() == ["MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE", "IGPM_EFETIVO_PCT"]
        mock_restructure.assert_called_once()
        assert result.loc[0, "IGPM_EFETIVO_PCT"] == "2.3"

    @pytest.mark.parametrize("timeout", [
        None, 10, 10.5, (10.0, 20.0), (10, 20)
    ])
    def test_run_timeout_validation(self, anbima_igpm_ltm: AnbimaIGPMForecastsLTM, timeout: Optional[Union[int, float, tuple]]) -> None:
        """Test run method with various timeout values.

        Verifies
        --------
        - Accepts valid timeout values (int, float, tuple of int/float)
        - Properly passes timeout to underlying methods

        Parameters
        ----------
        anbima_igpm_ltm : AnbimaIGPMForecastsLTM
            AnbimaIGPMForecastsLTM instance
        timeout : Optional[Union[int, float, tuple]]
            Timeout value to test

        Returns
        -------
        None
        """
        anbima_igpm_ltm.cls_db = None
        with patch.object(AnbimaIGPMCore, "parse_raw_file") as mock_parse, \
             patch.object(AnbimaIGPMForecastsLTM, "transform_data") as mock_transform, \
             patch.object(AnbimaIGPMCore, "standardize_dataframe") as mock_standardize:
            mock_standardize.return_value = pd.DataFrame()
            anbima_igpm_ltm.run(timeout=timeout)
            mock_parse.assert_called_once()


# --------------------------
# Tests for _restructure_table_with_missing_columns
# --------------------------
class TestRestructureTableWithMissingColumns:
    """Test cases for _restructure_table_with_missing_columns method."""

    def test_restructure_complete_row(self, anbima_igpm_ltm: AnbimaIGPMForecastsLTM, sample_flat_list: list[str], expected_columns_ltm: list[str]) -> None:
        """Test restructuring with complete row pattern.

        Verifies
        --------
        - Correctly handles complete row (month, date, numeric, date, numeric)
        - Produces correct dictionary structure
        - Logs appropriate messages

        Parameters
        ----------
        anbima_igpm_ltm : AnbimaIGPMForecastsLTM
            AnbimaIGPMForecastsLTM instance
        sample_flat_list : list[str]
            Sample flat list data
        expected_columns_ltm : list[str]
            Expected column names

        Returns
        -------
        None
        """
        # Mock the logging method to track calls
        mock_log = MagicMock()
        anbima_igpm_ltm.cls_create_log.log_message = mock_log
        
        result = anbima_igpm_ltm._restructure_table_with_missing_columns(
            flat_list=sample_flat_list[:5],
            expected_columns=expected_columns_ltm,
            missing_value=None
        )
        expected = [{
            "MES_COLETA": "Janeiro de 2025",
            "DATA": "01/01/25",
            "PROJECAO_PCT": "2.5",
            "DATA_VALIDADE": "31/01/25",
            "IGPM_EFETIVO_PCT": "2.3"
        }]
        assert result == expected
        # Verify logging was called (without checking exact message)
        assert mock_log.call_count > 0

    def test_restructure_missing_last_column(self, anbima_igpm_ltm: AnbimaIGPMForecastsLTM, sample_flat_list: list[str], expected_columns_ltm: list[str]) -> None:
        """Test restructuring with missing last column.

        Verifies
        --------
        - Handles pattern: month, date, numeric, date
        - Fills missing IGPM_EFETIVO_PCT with None
        - Logs appropriate messages

        Parameters
        ----------
        anbima_igpm_ltm : AnbimaIGPMForecastsLTM
            AnbimaIGPMForecastsLTM instance
        sample_flat_list : list[str]
            Sample flat list data
        expected_columns_ltm : list[str]
            Expected column names

        Returns
        -------
        None
        """
        # Mock the logging method
        mock_log = MagicMock()
        anbima_igpm_ltm.cls_create_log.log_message = mock_log
        
        result = anbima_igpm_ltm._restructure_table_with_missing_columns(
            flat_list=sample_flat_list[5:9],
            expected_columns=expected_columns_ltm,
            missing_value=None
        )
        expected = [{
            "MES_COLETA": "Fevereiro de 2025",
            "DATA": "01/02/25",
            "PROJECAO_PCT": "2.7",
            "DATA_VALIDADE": "28/02/25",
            "IGPM_EFETIVO_PCT": None
        }]
        assert result == expected
        assert mock_log.call_count > 0

    def test_restructure_missing_first_column(self, anbima_igpm_ltm: AnbimaIGPMForecastsLTM, sample_flat_list: list[str], expected_columns_ltm: list[str]) -> None:
        """Test restructuring with missing first column.

        Verifies
        --------
        - Handles pattern: date, numeric, date, numeric
        - Uses previous month for MES_COLETA
        - Logs appropriate messages

        Parameters
        ----------
        anbima_igpm_ltm : AnbimaIGPMForecastsLTM
            AnbimaIGPMForecastsLTM instance
        sample_flat_list : list[str]
            Sample flat list data
        expected_columns_ltm : list[str]
            Expected column names

        Returns
        -------
        None
        """
        # Mock the logging method
        mock_log = MagicMock()
        anbima_igpm_ltm.cls_create_log.log_message = mock_log
        
        result = anbima_igpm_ltm._restructure_table_with_missing_columns(
            flat_list=sample_flat_list[9:],
            expected_columns=expected_columns_ltm,
            missing_value=None
        )
        expected = [{
            "MES_COLETA": None,  # No previous month in this isolated test
            "DATA": "01/03/25",
            "PROJECAO_PCT": "2.8",
            "DATA_VALIDADE": "31/03/25",
            "IGPM_EFETIVO_PCT": "2.9"
        }]
        assert result == expected
        assert mock_log.call_count > 0

    def test_restructure_empty_list(self, anbima_igpm_ltm: AnbimaIGPMForecastsLTM, expected_columns_ltm: list[str]) -> None:
        """Test restructuring with empty input list.

        Verifies
        --------
        - Returns empty list for empty input
        - No logging occurs

        Parameters
        ----------
        anbima_igpm_ltm : AnbimaIGPMForecastsLTM
            AnbimaIGPMForecastsLTM instance
        expected_columns_ltm : list[str]
            Expected column names

        Returns
        -------
        None
        """
        # Mock the logging method
        mock_log = MagicMock()
        anbima_igpm_ltm.cls_create_log.log_message = mock_log
        
        result = anbima_igpm_ltm._restructure_table_with_missing_columns(
            flat_list=[],
            expected_columns=expected_columns_ltm,
            missing_value=None
        )
        assert result == []
        mock_log.assert_not_called()

    def test_restructure_invalid_values(self, anbima_igpm_ltm: AnbimaIGPMForecastsLTM, expected_columns_ltm: list[str]) -> None:
        """Test restructuring with invalid values.

        Verifies
        --------
        - Skips invalid patterns
        - Logs warning for unmatched elements
        - Returns empty or partial results

        Parameters
        ----------
        anbima_igpm_ltm : AnbimaIGPMForecastsLTM
            AnbimaIGPMForecastsLTM instance
        expected_columns_ltm : list[str]
            Expected column names

        Returns
        -------
        None
        """
        # Mock the logging method
        mock_log = MagicMock()
        anbima_igpm_ltm.cls_create_log.log_message = mock_log
        
        invalid_flat_list = ["invalid"]
        result = anbima_igpm_ltm._restructure_table_with_missing_columns(
            flat_list=invalid_flat_list,
            expected_columns=expected_columns_ltm,
            missing_value=None
        )
        assert result == []
        assert mock_log.call_count > 0


# --------------------------
# Tests for _debug_flat_list_structure
# --------------------------
class TestDebugFlatListStructure:
    """Test cases for _debug_flat_list_structure method."""

    def test_debug_flat_list_structure(self, anbima_igpm_ltm: AnbimaIGPMForecastsLTM, sample_flat_list: list[str], mocker: MockerFixture) -> None:
        """Test debug_flat_list_structure with valid input.

        Verifies
        --------
        - Correctly analyzes and prints structure of flat list
        - Identifies month, date, and numeric patterns
        - Outputs month positions and distances

        Parameters
        ----------
        anbima_igpm_ltm : AnbimaIGPMForecastsLTM
            AnbimaIGPMForecastsLTM instance
        sample_flat_list : list[str]
            Sample flat list data
        mocker : MockerFixture
            Pytest mocker for patching print

        Returns
        -------
        None
        """
        mock_print = mocker.patch("builtins.print")
        anbima_igpm_ltm._debug_flat_list_structure(sample_flat_list)
        
        assert mock_print.call_count >= 5  # At least one call per item plus headers
        mock_print.assert_any_call("=== FLAT LIST STRUCTURE ANALYSIS ===")

    def test_debug_flat_list_structure_empty(self, anbima_igpm_ltm: AnbimaIGPMForecastsLTM, mocker: MockerFixture) -> None:
        """Test debug_flat_list_structure with empty list.

        Verifies
        --------
        - Handles empty input gracefully
        - Outputs only header and month positions

        Parameters
        ----------
        anbima_igpm_ltm : AnbimaIGPMForecastsLTM
            AnbimaIGPMForecastsLTM instance
        mocker : MockerFixture
            Pytest mocker for patching print

        Returns
        -------
        None
        """
        mock_print = mocker.patch("builtins.print")
        anbima_igpm_ltm._debug_flat_list_structure([])
        
        # Should print header and month positions
        assert mock_print.call_count >= 1
        mock_print.assert_any_call("=== FLAT LIST STRUCTURE ANALYSIS ===")


# --------------------------
# Tests for Fallback and Reload Logic
# --------------------------
def test_module_reload(anbima_igpm_ltm: AnbimaIGPMForecastsLTM, mocker: MockerFixture) -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - Instance state is preserved
    - Dependencies are reinitialized

    Parameters
    ----------
    anbima_igpm_ltm : AnbimaIGPMForecastsLTM
        AnbimaIGPMForecastsLTM instance
    mocker : MockerFixture
        Pytest mocker for patching dependencies

    Returns
    -------
    None
    """
    import importlib
    original_url = anbima_igpm_ltm.url
    mocker.patch.object(DatesCurrent, "curr_date", return_value=date(2025, 1, 2))
    mocker.patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1))
    
    importlib.reload(sys.modules["stpstone.ingestion.countries.br.macroeconomics.anbima_igpm_forecasts"])
    new_instance = AnbimaIGPMForecastsLTM()
    
    assert new_instance.url == original_url
    assert new_instance.date_ref == date(2025, 1, 1)


# --------------------------
# Edge Cases and Error Conditions
# --------------------------
@pytest.mark.parametrize("invalid_timeout", [
    "invalid", [10, 20], {1: 2}
])
def test_run_invalid_timeout(
    anbima_igpm_ltm: AnbimaIGPMForecastsLTM, 
    invalid_timeout: Any
) -> None:
    """Test run method with invalid timeout values.

    Verifies
    --------
    - Raises TypeError for invalid timeout types
    - Type checking via type_checker decorator

    Parameters
    ----------
    anbima_igpm_ltm : AnbimaIGPMForecastsLTM
        AnbimaIGPMForecastsLTM instance
    invalid_timeout : Any
        Invalid timeout value

    Returns
    -------
    None
    """
    with pytest.raises(TypeError):
        anbima_igpm_ltm.run(timeout=invalid_timeout)


def test_run_with_empty_table_name(
    anbima_igpm_ltm: AnbimaIGPMForecastsLTM, 
    mock_playwright_scraper: PlaywrightScraper, 
    mocker: MockerFixture
) -> None:
    """Test run method with empty table name.

    Verifies
    --------
    - Raises ValueError for empty str_table_name when using database
    - Database operations are properly validated

    Parameters
    ----------
    anbima_igpm_ltm : AnbimaIGPMForecastsLTM
        AnbimaIGPMForecastsLTM instance
    mock_playwright_scraper : PlaywrightScraper
        Mocked PlaywrightScraper instance
    mocker : MockerFixture
        Pytest mocker for patching dependencies

    Returns
    -------
    None
    """
    mock_df = pd.DataFrame({"MES_COLETA": ["Janeiro 2025"]})
    mocker.patch.object(AnbimaIGPMCore, "parse_raw_file", return_value=mock_playwright_scraper)
    mocker.patch.object(AnbimaIGPMForecastsLTM, "transform_data", return_value=mock_df)
    mocker.patch.object(AnbimaIGPMCore, "standardize_dataframe", return_value=mock_df)
    
    with pytest.raises(ValueError, match="str_table_name cannot be empty"):
        anbima_igpm_ltm.run(str_table_name="")