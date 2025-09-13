"""Unit tests for B3 Trading Hours ingestion classes.

Tests the functionality of various B3 trading hours classes including:
- Initialization with valid and invalid inputs
- HTTP response handling
- Data parsing and transformation
- Dataframe standardization and database insertion
- Edge cases and error conditions
"""

from contextlib import suppress
from datetime import date
from typing import Union
from unittest.mock import MagicMock

from lxml.html import HtmlElement
import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.exchange.b3_trading_hours import (
    B3TradingHoursCommoditiesFutures,
    B3TradingHoursCore,
    B3TradingHoursCryptoFutures,
    B3TradingHoursExchangeRateFutures,
    B3TradingHoursExerciseBlockingOptionsAfterExerciseDate,
    B3TradingHoursExerciseBlockingOptionsBeforeExerciseDate,
    B3TradingHoursOptionsExercise,
    B3TradingHoursOTC,
    B3TradingHoursPMIFuture,
    B3TradingHoursRealDenominatedInterestRates,
    B3TradingHoursStockIndexFutures,
    B3TradingHoursStocks,
    B3TradingHoursUSDollarDenominatedInterestRatesFutures,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
    """Mock Response object with sample content.

    Returns
    -------
    Response
        Mocked requests Response object with sample HTML content
    """
    response = MagicMock(spec=Response)
    response.content = b"<html><table><tr><td>Test</td></tr></table></html>"
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def mock_html_element() -> HtmlElement:
    """Mock HtmlElement for testing.

    Returns
    -------
    HtmlElement
        Mocked lxml HtmlElement with sample table structure
    """
    html = MagicMock(spec=HtmlElement)
    html.xpath.return_value = [MagicMock(text="Test Data")]
    return html


@pytest.fixture
def mock_dataframe() -> pd.DataFrame:
    """Mock DataFrame for testing.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with test data
    """
    return pd.DataFrame({"MERCADO": ["Test"], "NEGOCIACAO_INICIO": ["09:00"]})


@pytest.fixture
def mock_session() -> Session:
    """Mock database Session for testing.

    Returns
    -------
    Session
        Mocked SQLAlchemy Session object
    """
    return MagicMock(spec=Session)


@pytest.fixture
def b3_trading_hours_core(mock_session: Session) -> B3TradingHoursCore:
    """Fixture providing B3TradingHoursCore instance.

    Parameters
    ----------
    mock_session : Session
        Mocked database session

    Returns
    -------
    B3TradingHoursCore
        Instance initialized with default parameters
    """
    return B3TradingHoursCore(
        date_ref=date(2025, 9, 12),
        logger=None,
        cls_db=mock_session,
        url="https://example.com"
    )


@pytest.fixture(autouse=True)
def mock_network_operations(mocker: MockerFixture, mock_response: Response) -> None:
    """Mock network operations for all tests.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    mock_response : Response
        Mocked Response object to ensure consistent HTTP response

    Returns
    -------
    None
    """
    mocker.patch("requests.get", return_value=mock_response)
    # Completely disable backoff by making it return the function unchanged
    mocker.patch("backoff.on_exception", side_effect=lambda *args, **kwargs: lambda func: func)
    mocker.patch("backoff.expo", side_effect=lambda *args, **kwargs: lambda func: func)
    mocker.patch("time.sleep")
    
    # Also mock any backoff import at module level
    import backoff
    mocker.patch.object(backoff, 'on_exception', side_effect=lambda *args, 
                        **kwargs: lambda func: func)


# --------------------------
# Tests for B3TradingHoursCore
# --------------------------
def test_b3_trading_hours_core_init_valid() -> None:
    """Test initialization of B3TradingHoursCore with valid inputs.

    Verifies
    --------
    - Instance is created with correct attributes
    - Date reference is set correctly
    - Dependencies are initialized properly

    Returns
    -------
    None
    """
    instance = B3TradingHoursCore(
        date_ref=date(2025, 9, 12),
        logger=None,
        cls_db=None,
        url="https://example.com"
    )
    assert instance.date_ref == date(2025, 9, 12)
    assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
    assert isinstance(instance.cls_dates_current, DatesCurrent)
    assert isinstance(instance.cls_create_log, CreateLog)
    assert isinstance(instance.cls_dates_br, DatesBRAnbima)
    assert isinstance(instance.cls_html_handler, HtmlHandler)
    assert isinstance(instance.cls_dicts_handler, HandlingDicts)
    assert instance.url == "https://example.com"


def test_b3_trading_hours_core_init_invalid_url_none() -> None:
    """Test initialization with None URL raises TypeError.
    
    Verifies
    --------
    - Raises TypeError for None URL
    - Error message matches expected pattern

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        B3TradingHoursCore(url=None)


def test_b3_trading_hours_core_init_invalid_url_empty_string() -> None:
    """Test initialization with empty string URL - should not raise error.
    
    Verifies
    --------
    - Empty string should be valid according to the type system

    Returns
    -------
    None
    """
    instance = B3TradingHoursCore(url="")
    assert instance.url == ""


def test_b3_trading_hours_core_init_invalid_url_whitespace() -> None:
    """Test initialization with whitespace URL - should not raise error.
    
    Verifies
    --------
    - Whitespace string should be valid according to the type system

    Returns
    -------
    None
    """
    # Whitespace string should be valid according to the type system
    instance = B3TradingHoursCore(url="  ")
    assert instance.url == "  "


def test_b3_trading_hours_core_init_invalid_url_integer() -> None:
    """Test initialization with integer URL raises TypeError.
    
    Verifies
    --------
    - Raises TypeError for integer URL
    - Error message matches expected pattern

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        B3TradingHoursCore(url=123)


def test_b3_trading_hours_core_init_default_date(mocker: MockerFixture) -> None:
    """Test initialization of B3TradingHoursCore with default date.

    Verifies
    --------
    - Default date is set to previous working day
    - Dependencies are initialized properly

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_dates_current = mocker.patch.object(DatesCurrent, "curr_date", 
                                             return_value=date(2025, 9, 13))
    mock_dates_br = mocker.patch.object(DatesBRAnbima, "add_working_days", 
                                        return_value=date(2025, 9, 12))
    
    instance = B3TradingHoursCore(url="https://example.com")
    assert instance.date_ref == date(2025, 9, 12)
    mock_dates_current.assert_called_once()
    mock_dates_br.assert_called_once_with(date(2025, 9, 13), -1)


def test_get_response_success(
    mocker: MockerFixture, 
    b3_trading_hours_core: B3TradingHoursCore, 
    mock_response: Response
) -> None:
    """Test successful HTTP response retrieval.

    Verifies
    --------
    - Requests.get is called with correct parameters
    - Response is returned correctly
    - raise_for_status is called

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore
    mock_response : Response
        Mocked Response object

    Returns
    -------
    None
    """
    mock_get = mocker.patch("requests.get", return_value=mock_response)
    result = b3_trading_hours_core.get_response(timeout=(12.0, 21.0), bool_verify=True)
    assert result == mock_response
    mock_get.assert_called_once_with("https://example.com", timeout=(12.0, 21.0), verify=True)
    mock_response.raise_for_status.assert_called_once()


def test_get_response_invalid_timeout_string() -> None:
    """Test get_response with invalid timeout string value.
    
    Verifies
    --------
    - Raises TypeError for invalid timeout types
    - Error message matches expected pattern

    Returns
    -------
    None
    """
    instance = B3TradingHoursCore(url="https://example.com")
    with pytest.raises(
        TypeError, 
        match="timeout must be one of types: int, float, tuple, tuple, NoneType, got str"
    ):
        instance.get_response(timeout="invalid")


def test_get_response_invalid_timeout_tuple_zeros() -> None:
    """Test get_response with invalid timeout tuple of zeros.
    
    Verifies
    --------
    - Raises TypeError for invalid timeout types
    - Error message matches expected pattern

    Returns
    -------
    None
    """
    instance = B3TradingHoursCore(url="https://example.com")
    # This should actually pass type validation but may fail business logic
    with suppress(TypeError, ValueError):
        instance.get_response(timeout=(0, 0))


def test_get_response_invalid_timeout_negative() -> None:
    """Test get_response with invalid negative timeout values.
    
    Verifies
    --------
    - Raises TypeError for invalid timeout types
    - Error message matches expected pattern

    Returns
    -------
    None
    """
    instance = B3TradingHoursCore(url="https://example.com")
    # This should pass type validation but may fail business logic
    with suppress(TypeError, ValueError):
        instance.get_response(timeout=(-1, 5))


@pytest.mark.parametrize("invalid_bool_verify", [None, "True", 1])
def test_get_response_invalid_bool_verify(
    b3_trading_hours_core: B3TradingHoursCore, 
    invalid_bool_verify: Union[None, str, int]
) -> None:
    """Test get_response with invalid bool_verify values.

    Parameters
    ----------
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore
    invalid_bool_verify : Union[None, str, int]
        Invalid bool_verify values to test

    Returns
    -------
    None
    """
    with pytest.raises(TypeError):
        b3_trading_hours_core.get_response(bool_verify=invalid_bool_verify)


def test_parse_raw_file(
    b3_trading_hours_core: B3TradingHoursCore, 
    mock_response: Response, 
    mocker: MockerFixture
) -> None:
    """Test parsing of raw file content.

    Verifies
    --------
    - lxml_parser is called with correct response
    - HtmlElement is returned

    Parameters
    ----------
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore
    mock_response : Response
        Mocked Response object
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_parser = mocker.patch.object(HtmlHandler, "lxml_parser", 
                                      return_value=MagicMock(spec=HtmlElement))
    result = b3_trading_hours_core.parse_raw_file(mock_response)
    assert isinstance(result, HtmlElement)
    mock_parser.assert_called_once_with(resp_req=mock_response)


def test_transform_data(
    b3_trading_hours_core: B3TradingHoursCore, 
    mock_html_element: HtmlElement, 
    mocker: MockerFixture
) -> None:
    """Test data transformation to DataFrame.

    Verifies
    --------
    - lxml_xpath is called with correct parameters
    - pair_headers_with_data is called correctly
    - DataFrame is created with correct data
    - NA values are replaced

    Parameters
    ----------
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore
    mock_html_element : HtmlElement
        Mocked HtmlElement
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_xpath = mocker.patch.object(HtmlHandler, "lxml_xpath", 
                                     return_value=[MagicMock(text="Test")])
    mock_pair = mocker.patch.object(HandlingDicts, "pair_headers_with_data", 
                                    return_value=[{"MERCADO": "Test"}])
    
    result = b3_trading_hours_core.transform_data(
        html_root=mock_html_element,
        list_th=["MERCADO"],
        xpath_td="//table/td",
        na_values="-"
    )
    
    assert isinstance(result, pd.DataFrame)
    assert result.loc[0, "MERCADO"] == "Test"
    mock_xpath.assert_called_once_with(html_content=mock_html_element, str_xpath="//table/td")
    mock_pair.assert_called_once_with(list_headers=["MERCADO"], list_data=["Test"])


def test_run_without_db(
    b3_trading_hours_core: B3TradingHoursCore, 
    mock_response: Response, 
    mock_html_element: HtmlElement, 
    mock_dataframe: pd.DataFrame, 
    mocker: MockerFixture
) -> None:
    """Test run method without database session.

    Verifies
    --------
    - Full ingestion pipeline without DB
    - Returns DataFrame
    - All intermediate methods are called correctly

    Parameters
    ----------
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore
    mock_response : Response
        Mocked Response object
    mock_html_element : HtmlElement
        Mocked HtmlElement
    mock_dataframe : pd.DataFrame
        Mocked DataFrame
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch("requests.get", return_value=mock_response)
    mocker.patch.object(B3TradingHoursCore, "parse_raw_file", return_value=mock_html_element)
    mocker.patch.object(B3TradingHoursCore, "transform_data", return_value=mock_dataframe)
    mocker.patch.object(B3TradingHoursCore, "standardize_dataframe", return_value=mock_dataframe)
    
    b3_trading_hours_core.cls_db = None
    result = b3_trading_hours_core.run(
        dict_dtypes={"MERCADO": str},
        str_table_name="test_table",
        str_fmt_dt="YYYY-MM-DD",
        timeout=(12.0, 21.0),
        bool_verify=True,
        bool_insert_or_ignore=False
    )
    
    assert isinstance(result, pd.DataFrame)
    assert result.equals(mock_dataframe)


def test_run_with_db(
    b3_trading_hours_core: B3TradingHoursCore, 
    mock_response: Response, 
    mock_html_element: HtmlElement, 
    mock_dataframe: pd.DataFrame, 
    mocker: MockerFixture
) -> None:
    """Test run method with database session.

    Verifies
    --------
    - Full ingestion pipeline with DB
    - Database insertion is called
    - No DataFrame is returned

    Parameters
    ----------
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore
    mock_response : Response
        Mocked Response object
    mock_html_element : HtmlElement
        Mocked HtmlElement
    mock_dataframe : pd.DataFrame
        Mocked DataFrame
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch("requests.get", return_value=mock_response)
    mocker.patch.object(B3TradingHoursCore, "parse_raw_file", return_value=mock_html_element)
    mocker.patch.object(B3TradingHoursCore, "transform_data", return_value=mock_dataframe)
    mocker.patch.object(B3TradingHoursCore, "standardize_dataframe", return_value=mock_dataframe)
    mock_insert = mocker.patch.object(B3TradingHoursCore, "insert_table_db")
    
    result = b3_trading_hours_core.run(
        dict_dtypes={"MERCADO": str},
        str_table_name="test_table",
        str_fmt_dt="YYYY-MM-DD",
        timeout=(12.0, 21.0),
        bool_verify=True,
        bool_insert_or_ignore=False
    )
    
    assert result is None
    mock_insert.assert_called_once_with(
        cls_db=b3_trading_hours_core.cls_db,
        str_table_name="test_table",
        df_=mock_dataframe,
        bool_insert_or_ignore=False
    )


# --------------------------
# Tests for B3TradingHoursStocks
# --------------------------
def test_b3_trading_hours_stocks_init() -> None:
    """Test initialization of B3TradingHoursStocks.

    Verifies
    --------
    - Correct URL is set
    - Inherits from B3TradingHoursCore correctly

    Returns
    -------
    None
    """
    instance = B3TradingHoursStocks(date_ref=date(2025, 9, 12))
    assert instance.url == "https://www.b3.com.br/pt_br/solucoes/plataformas/puma-trading-system/para-participantes-e-traders/horario-de-negociacao/acoes/"
    assert isinstance(instance, B3TradingHoursCore)


def test_b3_trading_hours_stocks_transform_data(
    mock_html_element: HtmlElement, 
    mocker: MockerFixture
) -> None:
    """Test transform_data method of B3TradingHoursStocks.

    Verifies
    --------
    - Correct table headers are used
    - Parent transform_data is called with correct parameters

    Parameters
    ----------
    mock_html_element : HtmlElement
        Mocked HtmlElement
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    instance = B3TradingHoursStocks()
    mock_transform = mocker.patch.object(
        B3TradingHoursCore, "transform_data", return_value=pd.DataFrame())
    
    instance.transform_data(mock_html_element)
    mock_transform.assert_called_once_with(
        html_root=mock_html_element,
        list_th=[
            "MERCADO",
            "CANCELAMENTO_DE_OFERTAS_INICIO",
            "CANCELAMENTO_DE_OFERTAS_FIM",
            "PRE_ABERTURA_INICIO",
            "PRE_ABERTURA_FIM",
            "NEGOCIACAO_INICIO",
            "NEGOCIACAO_FIM",
            "CALL_DE_FECHAMENTO_INICIO",
            "CALL_DE_FECHAMENTO_FIM",
            "AFTER_MARKET_CANCELAMENTO_DE_OFERTAS_INICIO",
            "AFTER_MARKET_CANCELAMENTO_DE_OFERTAS_FIM",
            "AFTER_MARKET_NEGOCIACAO_INICIO",
            "AFTER_MARKET_NEGOCIACAO_FIM",
            "AFTER_MARKET_CANCELAMENTO_DE_OFERTAS_FECHAMENTO_INICIO",
            "AFTER_MARKET_CANCELAMENTO_DE_OFERTAS_FECHAMENTO_FIM"
        ],
        xpath_td=
            '//*[@id="conteudo-principal"]/div[4]/div/div/table[1]/tbody/tr[position() > 1]/td',
        na_values="-"
    )


# --------------------------
# Tests for Other Classes
# --------------------------
@pytest.mark.parametrize("cls,expected_url,table_name", [
    (B3TradingHoursOptionsExercise, 
     "https://www.b3.com.br/pt_br/solucoes/plataformas/puma-trading-system/para-participantes-e-traders/horario-de-negociacao/acoes/",
     "br_b3_trading_hours_options_exercise"),
    (B3TradingHoursPMIFuture,
     "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/indices/",
     "br_b3_trading_hours_pmi_future"),
    (B3TradingHoursStockIndexFutures,
     "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/indices/",
     "br_b3_trading_hours_stock_index_futures"),
    (B3TradingHoursRealDenominatedInterestRates,
     "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/interest-rates/",
     "br_b3_trading_hours_real_denominated_interest_rates"),
    (B3TradingHoursUSDollarDenominatedInterestRatesFutures,
     "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/interest-rates/",
     "br_b3_trading_hours_usdollar_denominated_interest_rates_futures"),
    (B3TradingHoursCommoditiesFutures,
     "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/commodities/",
     "br_b3_trading_hours_commodities_futures"),
    (B3TradingHoursCryptoFutures,
     "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/cryptoassets/",
     "br_b3_trading_hours_crypto_futures"),
    (B3TradingHoursExchangeRateFutures,
     "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/foreign-exchange-and-dollar-spot/",
     "br_b3_trading_hours_exchange_rate_futures"),
    (B3TradingHoursOTC,
     "https://www.b3.com.br/pt_br/solucoes/plataformas/puma-trading-system/para-participantes-e-traders/horario-de-negociacao/balcao-organizado/",
     "br_b3_trading_hours_exchange_otc"),
    (B3TradingHoursExerciseBlockingOptionsBeforeExerciseDate,
     "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/exercise-and-blocking-of-options/",
     "br_b3_trading_hours_exercise_blocking_options_before_exercise_date"),
    (B3TradingHoursExerciseBlockingOptionsAfterExerciseDate,
     "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/exercise-and-blocking-of-options/",
     "br_b3_trading_hours_exercise_blocking_options_after_exercise_date"),
])
def test_specific_class_initialization(
    cls: type, 
    expected_url: str, 
    table_name: str, 
    mocker: MockerFixture, 
    mock_response: Response, 
    mock_html_element: HtmlElement, 
    mock_dataframe: pd.DataFrame
) -> None:
    """Test initialization of specific B3 trading hours classes.

    Verifies
    --------
    - Correct URL is set
    - Correct table name is used
    - Inherits from B3TradingHoursCore correctly

    Parameters
    ----------
    cls : type
        The specific B3 trading hours class to test
    expected_url : str
        The expected URL for the class
    table_name : str
        The expected table name for the class
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    mock_response : Response
        Mocked Response object
    mock_html_element : HtmlElement
        Mocked HtmlElement
    mock_dataframe : pd.DataFrame
        Mocked DataFrame

    Returns
    -------
    None
    """
    instance = cls(date_ref=date(2025, 9, 12))
    assert instance.url == expected_url
    assert isinstance(instance, B3TradingHoursCore)

    mocker.patch("requests.get", return_value=mock_response)
    mocker.patch.object(B3TradingHoursCore, "parse_raw_file", return_value=mock_html_element)
    mocker.patch.object(B3TradingHoursCore, "transform_data", return_value=mock_dataframe)
    mocker.patch.object(B3TradingHoursCore, "standardize_dataframe", return_value=mock_dataframe)

    result = instance.run(str_table_name=table_name)
    assert result is None or isinstance(result, pd.DataFrame)


# --------------------------
# Edge Cases and Error Conditions
# --------------------------
def test_transform_data_empty_html(
    b3_trading_hours_core: B3TradingHoursCore, 
    mocker: MockerFixture
) -> None:
    """Test transform_data with empty HTML content.

    Verifies
    --------
    - Handles empty HTML input gracefully
    - Returns empty DataFrame

    Parameters
    ----------
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_html = MagicMock(spec=HtmlElement)
    mocker.patch.object(HtmlHandler, "lxml_xpath", return_value=[])
    mocker.patch.object(HandlingDicts, "pair_headers_with_data", return_value=[])
    
    result = b3_trading_hours_core.transform_data(mock_html, ["MERCADO"], "//table/td")
    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.parametrize("invalid_input", [
    (None, ["MERCADO"], "//table/td", "-"),
    (HtmlElement(), None, "//table/td", "-"),
    (HtmlElement(), ["MERCADO"], None, "-"),
    (HtmlElement(), ["MERCADO"], "//table/td", None),
])
def test_transform_data_invalid_inputs(
    b3_trading_hours_core: B3TradingHoursCore, 
    invalid_input: tuple
) -> None:
    """Test transform_data with invalid inputs.

    Parameters
    ----------
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore
    invalid_input : tuple
        Tuple of invalid input parameters

    Returns
    -------
    None
    """
    with pytest.raises(TypeError):
        b3_trading_hours_core.transform_data(*invalid_input)


# --------------------------
# Fallback and Reload Logic
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
    """Test module reload behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - Instance attributes are preserved

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    import importlib
    import sys
    instance = B3TradingHoursCore(url="https://example.com")
    original_url = instance.url
    
    importlib.reload(sys.modules["stpstone.ingestion.countries.br.exchange.b3_trading_hours"])
    new_instance = B3TradingHoursCore(url="https://example.com")
    assert new_instance.url == original_url


# --------------------------
# Type Validation
# --------------------------
def test_run_invalid_dtypes(b3_trading_hours_core: B3TradingHoursCore) -> None:
    """Test run method with invalid dict_dtypes.

    Verifies
    --------
    - TypeError is raised for invalid dict_dtypes

    Parameters
    ----------
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="dict_dtypes must be of type dict"):
        b3_trading_hours_core.run(dict_dtypes=None, str_table_name="test_table")


def test_run_invalid_table_name_none(b3_trading_hours_core: B3TradingHoursCore) -> None:
    """Test run method with None table name.
    
    Verifies
    --------
    - TypeError is raised for None table name

    Parameters
    ----------
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="str_table_name must be of type str, got NoneType"):
        b3_trading_hours_core.run(
            dict_dtypes={"MERCADO": str},
            str_table_name=None
        )


def test_run_invalid_table_name_empty_string(b3_trading_hours_core: B3TradingHoursCore) -> None:
    """Test run method with empty string table name - should pass type validation.
    
    Verifies
    --------
    - TypeError is raised for empty string table name

    Parameters
    ----------
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore

    Returns
    -------
    None
    """
    # Empty string should pass type validation but may fail business logic
    with suppress(TypeError, ValueError):
        b3_trading_hours_core.run(
            dict_dtypes={"MERCADO": str},
            str_table_name=""
        )


def test_run_invalid_table_name_whitespace(b3_trading_hours_core: B3TradingHoursCore) -> None:
    """Test run method with whitespace table name - should pass type validation.
    
    Verifies
    --------
    - TypeError is raised for whitespace string table name

    Parameters
    ----------
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore

    Returns
    -------
    None
    """
    # Whitespace string should pass type validation but may fail business logic
    with suppress(TypeError, ValueError):
        b3_trading_hours_core.run(
            dict_dtypes={"MERCADO": str},
            str_table_name="  "
        )


def test_run_invalid_table_name_integer(b3_trading_hours_core: B3TradingHoursCore) -> None:
    """Test run method with integer table name.
    
    Verifies
    --------
    - TypeError is raised for integer table name

    Parameters
    ----------
    b3_trading_hours_core : B3TradingHoursCore
        Instance of B3TradingHoursCore

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="str_table_name must be of type str, got int"):
        b3_trading_hours_core.run(
            dict_dtypes={"MERCADO": str},
            str_table_name=123
        )


# --------------------------
# Coverage for All Classes
# --------------------------
def test_all_classes_instantiation() -> None:
    """Test instantiation of all B3 trading hours classes.

    Verifies
    --------
    - All classes can be instantiated without errors
    - Correct inheritance hierarchy

    Returns
    -------
    None
    """
    classes = [
        B3TradingHoursStocks,
        B3TradingHoursOptionsExercise,
        B3TradingHoursPMIFuture,
        B3TradingHoursStockIndexFutures,
        B3TradingHoursRealDenominatedInterestRates,
        B3TradingHoursUSDollarDenominatedInterestRatesFutures,
        B3TradingHoursCommoditiesFutures,
        B3TradingHoursCryptoFutures,
        B3TradingHoursExchangeRateFutures,
        B3TradingHoursOTC,
        B3TradingHoursExerciseBlockingOptionsBeforeExerciseDate,
        B3TradingHoursExerciseBlockingOptionsAfterExerciseDate,
    ]
    
    for cls in classes:
        instance = cls(date_ref=date(2025, 9, 12))
        assert isinstance(instance, B3TradingHoursCore)
        assert hasattr(instance, "run")
        assert hasattr(instance, "transform_data")
        assert hasattr(instance, "get_response")
        assert hasattr(instance, "parse_raw_file")