"""Unit tests for BVMFVBOVTradingVolumes class.

Tests the BVMF BOV Negotiation Volumes ingestion functionality, covering:
- Initialization with valid and invalid inputs
- Response fetching and parsing
- Data transformation
- Full ingestion process
- Edge cases and error conditions
"""

from datetime import date
import importlib
import sys
from typing import Union
from unittest.mock import MagicMock, patch

import bs4
import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.exchange.bvmf_bov_volumes import BVMFVBOVTradingVolumes
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.parsers.str import StrHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
    """Mock Response object with sample HTML content.

    Returns
    -------
    Response
        Mocked response with sample table HTML
    """
    response = MagicMock(spec=Response)
    response.content = b"""
        <html>
        <body>
        <table>
            <tr><td>MERCADO A VISTA</td><td>1000</td><td>1000.50</td><td>12000</td><td>12000.75
            </td></tr>
            <tr><td>MERCADO FUTURO</td><td>2000</td><td>2000.25</td><td>24000</td><td>24000.50
            </td></tr>
        </table>
        </body>
        </html>
    """
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response

@pytest.fixture
def mock_html_root() -> bs4.BeautifulSoup:
    """Mock BeautifulSoup object with sample table structure.
    
    Creates HTML with 12 tables so index [11] works.

    Returns
    -------
    bs4.BeautifulSoup
        Mocked parsed HTML content
    """
    # Create HTML with 12 tables (indices 0-11) so the code accessing index [11] works
    tables = []
    for i in range(12):
        if i == 11:  # The actual data table
            tables.append("""
            <table>
                <tr><th>Header1</th><th>Header2</th><th>Header3</th><th>Header4</th>
                          <th>Header5</th></tr>
                <tr><td>Row1</td><td>Data1</td><td>Data2</td><td>Data3</td><td>Data4</td></tr>
                <tr><td>MERCADO A VISTA</td><td>1000</td><td>1.000,50</td><td>12000</td>
                          <td>12.000,75</td></tr>
                <tr><td>MERCADO FUTURO</td><td>2000</td><td>2.000,25</td><td>24000</td>
                          <td>24.000,50</td></tr>
            </table>
            """)
        else:  # Empty placeholder tables
            tables.append("<table></table>")
    
    html_content = f"""
        <html>
        <body>
        {''.join(tables)}
        </body>
        </html>
    """
    return bs4.BeautifulSoup(html_content, "html.parser")

@pytest.fixture
def mock_empty_html_root() -> bs4.BeautifulSoup:
    """Mock BeautifulSoup object with empty table structure.

    Returns
    -------
    bs4.BeautifulSoup
        Mocked parsed HTML content with no tables
    """
    return bs4.BeautifulSoup("<html><body></body></html>", "html.parser")

@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample date.

    Returns
    -------
    date
        Sample date set to 2025-09-01
    """
    return date(2025, 9, 1)

@pytest.fixture
def bvmf_instance(sample_date: date) -> BVMFVBOVTradingVolumes:
    """Fixture providing BVMFVBOVTradingVolumes instance.

    Parameters
    ----------
    sample_date : date
        Sample date for initialization

    Returns
    -------
    BVMFVBOVTradingVolumes
        Instance initialized with sample date
    """
    return BVMFVBOVTradingVolumes(date_ref=sample_date)

@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> MagicMock:
    """Mock requests.get to prevent real HTTP calls.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    MagicMock
        Mock object for requests.get
    """
    return mocker.patch("stpstone.ingestion.countries.br.exchange.bvmf_bov_volumes.requests.get")

@pytest.fixture
def mock_sleep(mocker: MockerFixture) -> MagicMock:
    """Mock time.sleep to eliminate delays.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    MagicMock
        Mock object for time.sleep
    """
    return mocker.patch("stpstone.ingestion.countries.br.exchange.bvmf_bov_volumes.sleep")

@pytest.fixture(autouse=True)
def mock_backoff(mocker: MockerFixture) -> None:
    """Mock backoff decorator to bypass retry delays.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    """
    mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
    """Test initialization with valid inputs.

    Verifies
    -------
    - Instance is created with valid date
    - URL is correctly formatted with date
    - All helper classes are properly initialized

    Parameters
    ----------
    sample_date : date
        Sample date for initialization

    Returns
    -------
    None
    """
    instance = BVMFVBOVTradingVolumes(date_ref=sample_date)
    assert instance.date_ref == sample_date
    assert "strDtReferencia=09/2025" in instance.url
    assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
    assert isinstance(instance.cls_dates_current, DatesCurrent)
    assert isinstance(instance.cls_create_log, CreateLog)
    assert isinstance(instance.cls_dates_br, DatesBRAnbima)
    assert isinstance(instance.cls_html_handler, HtmlHandler)
    assert isinstance(instance.cls_str_handler, StrHandler)
    assert isinstance(instance.cls_num_handler, NumHandler)
    assert isinstance(instance.cls_dict_handler, HandlingDicts)

def test_init_default_date() -> None:
    """Test initialization with default date.

    Verifies
    -------
    - Instance uses default date (7 working days ago)
    - URL is correctly formatted with default date

    Returns
    -------
    None
    """
    instance = BVMFVBOVTradingVolumes()
    assert isinstance(instance.date_ref, date)
    assert f"strDtReferencia={instance.date_ref.strftime('%m/%Y')}" in instance.url

def test_get_response_success(
    bvmf_instance: BVMFVBOVTradingVolumes,
    mock_requests_get: MagicMock,
    mock_response: Response,
    mock_html_root: bs4.BeautifulSoup,
) -> None:
    """Test successful response fetching.

    Verifies
    -------
    - HTTP request is made with correct parameters
    - Response is returned when tables are found
    - No sleep calls are made on success

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked response object
    mock_html_root : bs4.BeautifulSoup
        Mocked parsed HTML content

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    with patch.object(bvmf_instance, "parse_raw_file", return_value=mock_html_root):
        result = bvmf_instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
    assert result == mock_response
    mock_requests_get.assert_called_once_with(
        bvmf_instance.url, timeout=(12.0, 21.0), verify=True
    )

def test_get_response_empty_tables(
    bvmf_instance: BVMFVBOVTradingVolumes,
    mock_requests_get: MagicMock,
    mock_response: Response,
    mock_empty_html_root: bs4.BeautifulSoup,
    mock_sleep: MagicMock,
) -> None:
    """Test response fetching with empty tables up to max retries.

    Verifies
    -------
    - ValueError is raised after max retries
    - Sleep is called between retries
    - Correct number of HTTP requests are made

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked response object
    mock_empty_html_root : bs4.BeautifulSoup
        Mocked empty HTML content
    mock_sleep : MagicMock
        Mocked time.sleep function

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    with patch.object(bvmf_instance, "parse_raw_file", return_value=mock_empty_html_root), \
        pytest.raises(ValueError, match="Failed to get response after 5 attempts"):
        bvmf_instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
    assert mock_requests_get.call_count == 6
    assert mock_sleep.call_count == 5

@pytest.mark.parametrize("invalid_timeout", [
    "invalid",
    [1, 2],
])
def test_get_response_invalid_timeout_type_error(
    bvmf_instance: BVMFVBOVTradingVolumes,
    mock_requests_get: MagicMock,
    invalid_timeout: Union[str, list],
) -> None:
    """Test get_response with invalid timeout values that should raise TypeError.

    Verifies
    -------
    - TypeError is raised for invalid timeout types
    - Appropriate error message is included

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested
    mock_requests_get : MagicMock
        Mocked requests.get function
    invalid_timeout : Union[str, list]
        Invalid timeout values to test

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="timeout must be one of types"):
        bvmf_instance.get_response(timeout=invalid_timeout)

def test_get_response_with_valid_edge_case_timeouts(
    bvmf_instance: BVMFVBOVTradingVolumes,
    mock_requests_get: MagicMock,
    mock_response: Response,
    mock_html_root: bs4.BeautifulSoup,
) -> None:
    """Test get_response with edge case timeout values that are valid for requests.

    Verifies that timeout values like None, negative numbers, and negative tuples
    are handled properly by the requests library without raising AttributeError.

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked response object
    mock_html_root : bs4.BeautifulSoup
        Mocked parsed HTML content

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    
    # Test each edge case timeout value
    test_timeouts = [None, -1, (-1.0, 2.0)]
    
    for timeout_val in test_timeouts:
        with patch.object(bvmf_instance, "parse_raw_file", return_value=mock_html_root):
            # These should work fine as requests.get accepts these timeout values
            result = bvmf_instance.get_response(timeout=timeout_val, bool_verify=True)
            assert result == mock_response

def test_parse_raw_file(
    bvmf_instance: BVMFVBOVTradingVolumes,
    mock_response: Response,
    mock_html_root: bs4.BeautifulSoup,
) -> None:
    """Test parsing of raw response content.

    Verifies
    -------
    - HTML content is correctly parsed into BeautifulSoup object
    - Parser method is called correctly

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested
    mock_response : Response
        Mocked response object
    mock_html_root : bs4.BeautifulSoup
        Mocked parsed HTML content

    Returns
    -------
    None
    """
    with patch.object(bvmf_instance.cls_html_handler, "bs_parser", return_value=mock_html_root) \
        as mock_parser:
        result = bvmf_instance.parse_raw_file(mock_response)
    assert isinstance(result, bs4.BeautifulSoup)
    mock_parser.assert_called_once_with(resp_req=mock_response)

def test_transform_data(
    bvmf_instance: BVMFVBOVTradingVolumes,
    mock_html_root: bs4.BeautifulSoup,
) -> None:
    """Test transformation of HTML data into DataFrame.

    Verifies
    -------
    - DataFrame is created with correct columns and data
    - Numeric values are properly parsed
    - String values are properly formatted

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested
    mock_html_root : bs4.BeautifulSoup
        Mocked parsed HTML content

    Returns
    -------
    None
    """
    result = bvmf_instance.transform_data(mock_html_root)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == [
        "MERCADO", "NEGOCIACOES", "VOLUME_BRL", "NEGOCIACOES_12M", "VOLUME_BRL_12M"
    ]
    assert len(result) == 2
    assert result.iloc[0]["MERCADO"] == "MERCADO_A_VISTA"
    assert result.iloc[0]["NEGOCIACOES"] == 1000
    assert result.iloc[0]["VOLUME_BRL"] == pytest.approx(1000.50)
    assert result.iloc[1]["MERCADO"] == "MERCADO_FUTURO"
    assert result.iloc[1]["NEGOCIACOES"] == 2000
    assert result.iloc[1]["VOLUME_BRL"] == pytest.approx(2000.25)

@pytest.mark.parametrize("invalid_input", [
    None,
    "",
    123,
    [],
])
def test_transform_data_invalid_input(
    bvmf_instance: BVMFVBOVTradingVolumes,
    invalid_input: Union[None, str, int, list],
) -> None:
    """Test transform_data with invalid input types.

    Verifies
    -------
    - TypeError is raised for invalid input types
    - Appropriate error message is included

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested
    invalid_input : Union[None, str, int, list]
        Invalid input types to test

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="html_root must be of type"):
        bvmf_instance.transform_data(invalid_input)

def test_td_parser_numeric(
    bvmf_instance: BVMFVBOVTradingVolumes,
) -> None:
    """Test parsing of numeric table data.

    Verifies
    -------
    - Numeric strings are correctly converted to floats
    - Brazilian number format is properly handled

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested

    Returns
    -------
    None
    """
    element = bs4.BeautifulSoup("<td>1.234,56</td>", "html.parser").td
    result = bvmf_instance._td_parser(element)
    assert isinstance(result, float)
    assert result == pytest.approx(1234.56)

def test_td_parser_string(
    bvmf_instance: BVMFVBOVTradingVolumes,
) -> None:
    """Test parsing of string table data.

    Verifies
    -------
    - Strings are properly formatted (upper case, no spaces, etc.)
    - Special characters are handled correctly

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested

    Returns
    -------
    None
    """
    element = bs4.BeautifulSoup("<td>Mercado de Futuro</td>", "html.parser").td
    result = bvmf_instance._td_parser(element)
    assert result == "MERCADO_FUTURO"

@pytest.mark.parametrize("invalid_element", [
    None,
    "",
    123,
])
def test_td_parser_invalid_input(
    bvmf_instance: BVMFVBOVTradingVolumes,
    invalid_element: Union[None, str, int],
) -> None:
    """Test _td_parser with invalid input types.

    Verifies
    -------
    - TypeError is raised for invalid input types
    - Appropriate error message is included

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested
    invalid_element : Union[None, str, int]
        Invalid input types to test

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="el must be of type"):
        bvmf_instance._td_parser(invalid_element)

def test_run_without_db(
    bvmf_instance: BVMFVBOVTradingVolumes,
    mock_requests_get: MagicMock,
    mock_response: Response,
    mock_html_root: bs4.BeautifulSoup,
) -> None:
    """Test full ingestion process without database.

    Verifies
    -------
    - Complete ingestion process returns DataFrame
    - DataFrame has correct structure and data
    - No database operations are attempted

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked response object
    mock_html_root : bs4.BeautifulSoup
        Mocked parsed HTML content

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    with patch.object(bvmf_instance, "parse_raw_file", return_value=mock_html_root), \
        patch.object(bvmf_instance, "standardize_dataframe") as mock_standardize:
        mock_df = pd.DataFrame({
            "MERCADO": ["MERCADO_A_VISTA", "MERCADO_FUTURO"],
            "NEGOCIACOES": [1000, 2000],
            "VOLUME_BRL": [1000.50, 2000.25],
            "NEGOCIACOES_12M": [12000, 24000],
            "VOLUME_BRL_12M": [12000.75, 24000.50],
        })
        mock_standardize.return_value = mock_df
        result = bvmf_instance.run(timeout=(12.0, 21.0), bool_verify=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == [
        "MERCADO", "NEGOCIACOES", "VOLUME_BRL", "NEGOCIACOES_12M", "VOLUME_BRL_12M"
    ]

def test_run_with_db(
    bvmf_instance: BVMFVBOVTradingVolumes,
    mock_requests_get: MagicMock,
    mock_response: Response,
    mock_html_root: bs4.BeautifulSoup,
) -> None:
    """Test full ingestion process with database.

    Verifies
    -------
    - Database insertion is attempted
    - No DataFrame is returned when database is provided
    - Correct table name is used

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked response object
    mock_html_root : bs4.BeautifulSoup
        Mocked parsed HTML content

    Returns
    -------
    None
    """
    mock_db = MagicMock(spec=Session)
    bvmf_instance.cls_db = mock_db
    mock_requests_get.return_value = mock_response
    with patch.object(bvmf_instance, "parse_raw_file", return_value=mock_html_root), \
        patch.object(bvmf_instance, "standardize_dataframe") as mock_standardize, \
        patch.object(bvmf_instance, "insert_table_db") as mock_insert:
        mock_df = pd.DataFrame({
            "MERCADO": ["MERCADO_A_VISTA", "MERCADO_FUTURO"],
            "NEGOCIACOES": [1000, 2000],
            "VOLUME_BRL": [1000.50, 2000.25],
            "NEGOCIACOES_12M": [12000, 24000],
            "VOLUME_BRL_12M": [12000.75, 24000.50],
        })
        mock_standardize.return_value = mock_df
        result = bvmf_instance.run(
            timeout=(12.0, 21.0),
            bool_verify=True,
            str_table_name="test_table"
        )
    assert result is None
    mock_insert.assert_called_once()
    assert mock_insert.call_args.kwargs["str_table_name"] == "test_table"

def test_run_timeout_error(
    bvmf_instance: BVMFVBOVTradingVolumes,
    mock_requests_get: MagicMock,
) -> None:
    """Test handling of timeout errors during run.

    Verifies
    -------
    - TimeoutError is properly propagated
    - No DataFrame is returned on failure

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested
    mock_requests_get : MagicMock
        Mocked requests.get function

    Returns
    -------
    None
    """
    mock_requests_get.side_effect = TimeoutError("Request timeout")
    with pytest.raises(TimeoutError, match="Request timeout"):
        bvmf_instance.run(timeout=(12.0, 21.0), bool_verify=True)

def test_module_reload(
    bvmf_instance: BVMFVBOVTradingVolumes,
    sample_date: date,
) -> None:
    """Test module reloading preserves state.

    Verifies
    -------
    - Instance state is preserved after reload
    - URL and date remain consistent

    Parameters
    ----------
    bvmf_instance : BVMFVBOVTradingVolumes
        Instance of the class being tested
    sample_date : date
        Sample date for initialization

    Returns
    -------
    None
    """
    original_url = bvmf_instance.url
    importlib.reload(sys.modules["stpstone.ingestion.countries.br.exchange.bvmf_bov_volumes"])
    new_instance = BVMFVBOVTradingVolumes(date_ref=sample_date)
    assert new_instance.url == original_url
    assert new_instance.date_ref == sample_date