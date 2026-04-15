"""Unit tests for EtfDBVettaFi ingestion class.

Tests the ingestion functionality with various scenarios including:
- Initialization with valid and default inputs
- Selenium WebDriver response handling
- Data parsing (pass-through) and transformation
- Private row-scraping helper
- Database insertion and fallback logic
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.countries.us.registries.etfdb_vettafi import (
    _DEFAULT_SLUGS,
    EtfDBVettaFi,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date() -> date:
    """Provide a fixed reference date.

    Returns
    -------
    date
        Fixed date for consistent testing.
    """
    return date(2025, 1, 2)


@pytest.fixture
def etfdb_instance(sample_date: date) -> EtfDBVettaFi:
    """Provide an EtfDBVettaFi instance with a fixed date and single slug.

    Parameters
    ----------
    sample_date : date
        Fixed reference date.

    Returns
    -------
    EtfDBVettaFi
        Configured instance for unit testing.
    """
    return EtfDBVettaFi(date_ref=sample_date, list_slugs=["VNQ"])


@pytest.fixture
def mock_web_driver() -> MagicMock:
    """Provide a mocked Selenium WebDriver.

    Returns
    -------
    MagicMock
        Mocked WebDriver with find_element configured.
    """
    return MagicMock(spec=SeleniumWebDriver)


def _make_row_element(symbol: str, holding: str, weight: str) -> MagicMock:
    """Build a mock <tr> element with the expected cell structure.

    Parameters
    ----------
    symbol : str
        Ticker symbol text.
    holding : str
        Holding name text.
    weight : str
        Weight text including the percent sign (e.g. "5.00%").

    Returns
    -------
    MagicMock
        Mocked row element with child td mocks.

    Raises
    ------
    NoSuchElementException
        Propagated from the inner side-effect when an unexpected xpath is used.
    """
    def _make_cell(text: str, has_anchor: bool = False) -> MagicMock:
        """Build a mock td element.

        Parameters
        ----------
        text : str
            The text content of the cell.
        has_anchor : bool
            Whether the cell contains an anchor child element, by default False.

        Returns
        -------
        MagicMock
            Mocked td element.
        """
        cell = MagicMock()
        cell.text = text
        if has_anchor:
            anchor = MagicMock()
            anchor.text = text
            cell.find_element.return_value = anchor
        else:
            cell.find_element.return_value = cell
        return cell

    symbol_cell = _make_cell(symbol, has_anchor=True)
    holding_cell = _make_cell(holding)
    weight_cell = _make_cell(weight)

    row = MagicMock()

    def _find_element_side_effect(by: object, xpath: str) -> MagicMock:
        """Route find_element calls to the correct mock cell.

        Parameters
        ----------
        by : object
            Selenium By locator strategy (unused in mock).
        xpath : str
            XPath expression identifying which cell to return.

        Returns
        -------
        MagicMock
            The mock cell matching the given xpath.

        Raises
        ------
        NoSuchElementException
            When the xpath does not match any known cell.
        """
        if xpath == "./td[1]/a":
            return symbol_cell.find_element.return_value
        if xpath == "./td[2]":
            return holding_cell
        if xpath == "./td[3]":
            return weight_cell
        raise NoSuchElementException(f"Unexpected xpath: {xpath}")

    row.find_element.side_effect = _find_element_side_effect
    return row


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
    """Test initialization with explicit inputs.

    Verifies
    --------
    - All attributes are set correctly.
    - Helper objects are instantiated.

    Parameters
    ----------
    sample_date : date
        Fixed reference date.

    Returns
    -------
    None
    """
    instance = EtfDBVettaFi(
        date_ref=sample_date,
        list_slugs=["VNQ"],
        int_wait_load_seconds=20,
        bool_headless=True,
        bool_incognito=True,
    )
    assert instance.date_ref == sample_date
    assert instance.list_slugs == ["VNQ"]
    assert instance.int_wait_load_seconds == 20
    assert instance.bool_headless is True
    assert instance.bool_incognito is True
    assert isinstance(instance.cls_dates_current, DatesCurrent)
    assert isinstance(instance.cls_dates_br, DatesBRAnbima)
    assert isinstance(instance.cls_create_log, CreateLog)
    assert isinstance(instance.cls_dir_files_management, DirFilesManagement)


def test_init_default_slugs() -> None:
    """Test that default slugs are applied when list_slugs is None.

    Verifies
    --------
    - Default slug list equals _DEFAULT_SLUGS.

    Returns
    -------
    None
    """
    instance = EtfDBVettaFi(date_ref=date(2025, 1, 2))
    assert instance.list_slugs == list(_DEFAULT_SLUGS)


def test_init_default_date() -> None:
    """Test that the date defaults to the previous working day.

    Verifies
    --------
    - date_ref is set via DatesBRAnbima when not provided.

    Returns
    -------
    None
    """
    with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 2)):
        instance = EtfDBVettaFi()
        assert instance.date_ref == date(2025, 1, 2)


def test_parse_raw_file_passes_through(
    etfdb_instance: EtfDBVettaFi,
    mock_web_driver: MagicMock,
) -> None:
    """Test that parse_raw_file returns the WebDriver unchanged.

    Verifies
    --------
    - The same object passed in is returned.

    Parameters
    ----------
    etfdb_instance : EtfDBVettaFi
        Instance under test.
    mock_web_driver : MagicMock
        Mocked WebDriver.

    Returns
    -------
    None
    """
    result = etfdb_instance.parse_raw_file(mock_web_driver)
    assert result is mock_web_driver


def test_td_th_parser_with_rows(
    etfdb_instance: EtfDBVettaFi,
    mock_web_driver: MagicMock,
) -> None:
    """Test _td_th_parser scrapes rows correctly until NoSuchElementException.

    Verifies
    --------
    - Correct number of rows extracted.
    - SYMBOL, HOLDING, WEIGHT values are accurate.

    Parameters
    ----------
    etfdb_instance : EtfDBVettaFi
        Instance under test.
    mock_web_driver : MagicMock
        Mocked WebDriver.

    Returns
    -------
    None

    Raises
    ------
    NoSuchElementException
        Raised internally by the mock to signal end of table; caught by the parser.
    """
    row_1 = _make_row_element("AMT", "American Tower", "5.00%")
    row_2 = _make_row_element("PLD", "Prologis", "4.50%")

    call_count = 0

    def _find_tr(by: object, xpath: str) -> MagicMock:
        """Return rows in order, then raise NoSuchElementException.

        Parameters
        ----------
        by : object
            Selenium By locator strategy (unused in mock).
        xpath : str
            XPath expression for the row to locate.

        Returns
        -------
        MagicMock
            The next mock row in sequence.

        Raises
        ------
        NoSuchElementException
            After both rows have been returned.
        """
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return row_1
        if call_count == 2:
            return row_2
        raise NoSuchElementException("End of table")

    mock_web_driver.find_element.side_effect = _find_tr

    rows = etfdb_instance._td_th_parser(mock_web_driver)
    assert len(rows) == 2
    assert rows[0]["SYMBOL"] == "AMT"
    assert rows[0]["HOLDING"] == "American Tower"
    assert rows[0]["WEIGHT"] == pytest.approx(0.05, rel=1e-4)
    assert rows[1]["SYMBOL"] == "PLD"
    assert rows[1]["WEIGHT"] == pytest.approx(0.045, rel=1e-4)


def test_td_th_parser_empty_table_returns_sentinel(
    etfdb_instance: EtfDBVettaFi,
    mock_web_driver: MagicMock,
) -> None:
    """Test _td_th_parser returns sentinel row when table is empty.

    Verifies
    --------
    - Sentinel record {"SYMBOL": "ERROR", "HOLDING": "ERROR", "WEIGHT": 0.0} is returned.

    Parameters
    ----------
    etfdb_instance : EtfDBVettaFi
        Instance under test.
    mock_web_driver : MagicMock
        Mocked WebDriver.

    Returns
    -------
    None
    """
    mock_web_driver.find_element.side_effect = NoSuchElementException("Empty")
    rows = etfdb_instance._td_th_parser(mock_web_driver)
    assert rows == [{"SYMBOL": "ERROR", "HOLDING": "ERROR", "WEIGHT": 0.0}]


def test_transform_data_returns_dataframe(
    etfdb_instance: EtfDBVettaFi,
    mock_web_driver: MagicMock,
) -> None:
    """Test transform_data wraps _td_th_parser output in a DataFrame.

    Verifies
    --------
    - Return type is pd.DataFrame.
    - Expected columns are present.

    Parameters
    ----------
    etfdb_instance : EtfDBVettaFi
        Instance under test.
    mock_web_driver : MagicMock
        Mocked WebDriver.

    Returns
    -------
    None
    """
    expected_rows = [{"SYMBOL": "VNQ", "HOLDING": "Vanguard RE ETF", "WEIGHT": 0.10}]
    with patch.object(etfdb_instance, "_td_th_parser", return_value=expected_rows):
        df_ = etfdb_instance.transform_data(mock_web_driver)
    assert isinstance(df_, pd.DataFrame)
    assert list(df_.columns) == ["SYMBOL", "HOLDING", "WEIGHT"]
    assert df_.iloc[0]["SYMBOL"] == "VNQ"


def test_run_without_db(
    etfdb_instance: EtfDBVettaFi,
    mock_web_driver: MagicMock,
) -> None:
    """Test run returns a DataFrame when no database session is configured.

    Verifies
    --------
    - get_response is called once per slug.
    - transform_data is called.
    - standardize_dataframe is called.
    - A DataFrame is returned.

    Parameters
    ----------
    etfdb_instance : EtfDBVettaFi
        Instance under test.
    mock_web_driver : MagicMock
        Mocked WebDriver.

    Returns
    -------
    None
    """
    mock_web_driver.quit = MagicMock()
    sample_df = pd.DataFrame({"SYMBOL": ["AMT"], "HOLDING": ["American Tower"], "WEIGHT": [0.05]})
    standardized_df = pd.DataFrame(
        {"SLUG": ["VNQ"], "SYMBOL": ["AMT"], "HOLDING": ["American Tower"], "WEIGHT": [0.05]}
    )

    with patch.object(etfdb_instance, "get_response", return_value=mock_web_driver) as mock_get, \
         patch.object(etfdb_instance, "transform_data", return_value=sample_df) as mock_trt, \
         patch.object(
             etfdb_instance, "standardize_dataframe", return_value=standardized_df
         ) as mock_std:
        result = etfdb_instance.run()

    assert isinstance(result, pd.DataFrame)
    mock_get.assert_called_once()
    mock_trt.assert_called_once()
    mock_std.assert_called_once()


def test_run_with_db_calls_insert(
    etfdb_instance: EtfDBVettaFi,
    mock_web_driver: MagicMock,
) -> None:
    """Test run calls insert_table_db and returns None when cls_db is set.

    Verifies
    --------
    - insert_table_db is called with the correct table name.
    - None is returned.

    Parameters
    ----------
    etfdb_instance : EtfDBVettaFi
        Instance under test.
    mock_web_driver : MagicMock
        Mocked WebDriver.

    Returns
    -------
    None
    """
    mock_web_driver.quit = MagicMock()
    mock_db = MagicMock()
    etfdb_instance.cls_db = mock_db

    sample_df = pd.DataFrame({"SYMBOL": ["AMT"], "HOLDING": ["American Tower"], "WEIGHT": [0.05]})
    standardized_df = pd.DataFrame(
        {"SLUG": ["VNQ"], "SYMBOL": ["AMT"], "HOLDING": ["American Tower"], "WEIGHT": [0.05]}
    )

    with patch.object(etfdb_instance, "get_response", return_value=mock_web_driver), \
         patch.object(etfdb_instance, "transform_data", return_value=sample_df), \
         patch.object(etfdb_instance, "standardize_dataframe", return_value=standardized_df), \
         patch.object(etfdb_instance, "insert_table_db") as mock_insert:
        result = etfdb_instance.run()

    assert result is None
    mock_insert.assert_called_once_with(
        cls_db=mock_db,
        str_table_name="us_etfdb_vettafi_reits",
        df_=standardized_df,
        bool_insert_or_ignore=False,
    )


def test_run_empty_slug_list() -> None:
    """Test run returns None immediately when list_slugs is empty.

    Verifies
    --------
    - Early return with None when list_slugs is [].

    Returns
    -------
    None
    """
    instance = EtfDBVettaFi(date_ref=date(2025, 1, 2), list_slugs=[])
    result = instance.run()
    assert result is None


def test_reload_module() -> None:
    """Test that the module reloads cleanly.

    Verifies
    --------
    - A fresh instance after reload has correct defaults.

    Returns
    -------
    None
    """
    import importlib

    import stpstone.ingestion.countries.us.registries.etfdb_vettafi as mod

    importlib.reload(mod)
    instance = mod.EtfDBVettaFi(date_ref=date(2025, 1, 2))
    assert instance.list_slugs == list(mod._DEFAULT_SLUGS)
