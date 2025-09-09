"""Unit tests for B3TheoreticalPortfolio and derived classes.

Tests the ingestion functionality for B3 theoretical portfolio data, covering:
- Initialization with various inputs
- HTTP response handling
- Data parsing and transformation
- Database operations
- Edge cases and error conditions
"""

from datetime import date
import importlib
from logging import Logger
import sys
from typing import TypedDict, Union
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response
from sqlalchemy.orm import Session

from stpstone.ingestion.countries.br.exchange.b3_indexes_theor_portfs import (
    B3TheoreticalPortfolio,
    B3TheoricalPortfolioIBOV,
    B3TheoricalPortfolioIBRA,
    B3TheoricalPortfolioIBRX50,
)
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


# --------------------------
# Type Definitions
# --------------------------
class ReturnParseRawFile(TypedDict):
    """Type definition for parse_raw_file return value."""

    results: list[dict[str, Union[str, int, float]]]


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
    """Mock Response object with sample B3 portfolio data.

    Returns
    -------
    Response
        Mocked requests Response object
    """
    response = MagicMock(spec=Response)
    response.json.return_value = {
        "results": [
            {"SEGMENT": "Stock", "COD": "PETR4", "ASSET": "Petrobras", "PART": 5.2},
            {"SEGMENT": "Stock", "COD": "VALE3", "ASSET": "Vale", "PART": 4.8}
        ],
        "header": {"DATE_HEADER": "01/01/23", "TEXT_HEADER": "Test"},
        "page": {"PAGE_NUMBER": 1, "PAGE_SIZE": 120, "TOTAL_RECORDS": 2, "TOTAL_PAGES": 1}
    }
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


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
        Mocked requests.get function
    """
    return mocker.patch("requests.get")


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> MagicMock:
    """Mock backoff.on_exception to bypass retries.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    MagicMock
        Mocked backoff.on_exception decorator
    """
    return mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


@pytest.fixture
def sample_date() -> date:
    """Provide a sample date for testing.

    Returns
    -------
    date
        Sample date object
    """
    return date(2023, 1, 1)


@pytest.fixture
def mock_logger() -> Logger:
    """Mock Logger instance.

    Returns
    -------
    Logger
        Mocked logger object
    """
    return MagicMock(spec=Logger)


@pytest.fixture
def mock_db_session() -> Session:
    """Mock database session.

    Returns
    -------
    Session
        Mocked SQLAlchemy session
    """
    mock_session = MagicMock(spec=Session)
    # Add the insert method that was missing
    mock_session.insert = MagicMock()
    return mock_session


@pytest.fixture
def b3_portfolio(
    sample_date: date, 
    mock_logger: Logger, 
    mock_db_session: Session
) -> B3TheoreticalPortfolio:
    """Fixture for B3TheoreticalPortfolio instance.

    Parameters
    ----------
    sample_date : date
        Sample date for initialization
    mock_logger : Logger
        Mocked logger instance
    mock_db_session : Session
        Mocked database session

    Returns
    -------
    B3TheoreticalPortfolio
        Initialized portfolio instance
    """
    return B3TheoreticalPortfolio(date_ref=sample_date, logger=mock_logger, cls_db=mock_db_session)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(
    sample_date: date, 
    mock_logger: Logger, 
    mock_db_session: Session
) -> None:
    """Test initialization with valid inputs.

    Verifies
    -------
    - Instance is properly initialized
    - Attributes are correctly set
    - Inherited classes are properly initialized

    Parameters
    ----------
    sample_date : date
        Sample date for initialization
    mock_logger : Logger
        Mocked logger instance
    mock_db_session : Session
        Mocked database session

    Returns
    -------
    None
    """
    portfolio = B3TheoreticalPortfolio(date_ref=sample_date, logger=mock_logger, 
                                       cls_db=mock_db_session)
    assert portfolio.date_ref == sample_date
    assert portfolio.logger == mock_logger
    assert portfolio.cls_db == mock_db_session
    assert isinstance(portfolio.cls_dates_br, DatesBRAnbima)
    assert portfolio.url == "FILL_ME"


@pytest.mark.parametrize("portfolio_class,url", [
    (B3TheoricalPortfolioIBOV, "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgiOiJJQk9WIiwic2VnbWVudCI6IjEifQ=="),
    (B3TheoricalPortfolioIBRX50, "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgiOiJJQlhMIiwic2VnbWVudCI6IjEifQ=="),
    (B3TheoricalPortfolioIBRA, "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgiOiJJQlJBIiwic2VnbWVudCI6IjEifQ=="),
])
def test_derived_classes_urls(portfolio_class: type, url: str, sample_date: date) -> None:
    """Test URL initialization in derived classes.

    Parameters
    ----------
    portfolio_class : type
        The derived portfolio class to test
    url : str
        Expected URL for the class
    sample_date : date
        Sample date for initialization

    Returns
    -------
    None
    """
    portfolio = portfolio_class(date_ref=sample_date)
    assert portfolio.url == url


def test_init_default_date_ref(mock_logger: Logger, mocker: MockerFixture) -> None:
    """Test initialization with default date reference.

    Verifies
    -------
    - Default date is set to previous working day
    - Other attributes are properly initialized

    Parameters
    ----------
    mock_logger : Logger
        Mocked logger instance
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_dates_br = mocker.patch.object(
        DatesBRAnbima, "add_working_days", return_value=date(2023, 1, 1))
    portfolio = B3TheoreticalPortfolio(logger=mock_logger)
    assert portfolio.date_ref == date(2023, 1, 1)
    mock_dates_br.assert_called_once()


def test_get_response_success(
    b3_portfolio: B3TheoreticalPortfolio, 
    mock_requests_get: MagicMock, 
    mock_response: Response, 
    mock_backoff: MagicMock
) -> None:
    """Test successful HTTP response retrieval.

    Verifies
    -------
    - Requests.get is called with correct parameters
    - Response is returned correctly
    - Status check is performed

    Parameters
    ----------
    b3_portfolio : B3TheoreticalPortfolio
        Portfolio instance
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked Response object
    mock_backoff : MagicMock
        Mocked backoff decorator

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    result = b3_portfolio.get_response(timeout=(12.0, 21.0), bool_verify=True)
    assert result == mock_response
    mock_requests_get.assert_called_once_with(b3_portfolio.url, timeout=(12.0, 21.0), verify=True)
    mock_response.raise_for_status.assert_called_once()


@pytest.mark.parametrize("timeout", [5, 10.5, (5.0, 10.0)])
def test_get_response_timeout_variations(
    b3_portfolio: B3TheoreticalPortfolio, 
    mock_requests_get: MagicMock, 
    mock_response: Response, 
    mock_backoff: MagicMock, 
    timeout: Union[int, float, tuple[float, float]]
) -> None:
    """Test get_response with different timeout values.

    Parameters
    ----------
    b3_portfolio : B3TheoreticalPortfolio
        Portfolio instance
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked Response object
    mock_backoff : MagicMock
        Mocked backoff decorator
    timeout : Union[int, float, tuple[float, float]]
        Timeout value to test

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    result = b3_portfolio.get_response(timeout=timeout)
    assert result == mock_response
    mock_requests_get.assert_called_once_with(b3_portfolio.url, timeout=timeout, verify=True)


def test_parse_raw_file(
    b3_portfolio: B3TheoreticalPortfolio, 
    mock_response: Response, 
    mocker: MockerFixture
) -> None:
    """Test parsing of raw file content.

    Verifies
    -------
    - JSON response is correctly parsed
    - Additional keys are added to results
    - Correct structure of returned list

    Parameters
    ----------
    b3_portfolio : B3TheoreticalPortfolio
        Portfolio instance
    mock_response : Response
        Mocked Response object
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch.object(b3_portfolio.cls_handling_dicts, "add_key_value_to_dicts", 
                        side_effect=lambda list_ser, key: list_ser)
    result = b3_portfolio.parse_raw_file(mock_response)
    assert isinstance(result, list)
    assert len(result) == 2
    assert all(isinstance(item, dict) for item in result)
    assert result[0]["SEGMENT"] == "Stock"
    assert result[0]["COD"] == "PETR4"


def test_transform_data(b3_portfolio: B3TheoreticalPortfolio) -> None:
    """Test transformation of parsed data to DataFrame.

    Verifies
    -------
    - Input list is correctly transformed to DataFrame
    - Column types are preserved
    - Data integrity is maintained

    Parameters
    ----------
    b3_portfolio : B3TheoreticalPortfolio
        Portfolio instance

    Returns
    -------
    None
    """
    input_data = [
        {"SEGMENT": "Stock", "COD": "PETR4", "PART": 5.2},
        {"SEGMENT": "Stock", "COD": "VALE3", "PART": 4.8}
    ]
    df_ = b3_portfolio.transform_data(input_data)
    assert isinstance(df_, pd.DataFrame)
    assert len(df_) == 2
    assert list(df_.columns) == ["SEGMENT", "COD", "PART"]
    assert df_["SEGMENT"].iloc[0] == "Stock"
    assert df_["PART"].iloc[0] == pytest.approx(5.2, rel=1e-3)


def test_run_without_db(
    mock_requests_get: MagicMock, 
    mock_response: Response, 
    mocker: MockerFixture, 
    sample_date: date, 
    mock_logger: Logger
) -> None:
    """Test run method without database session.

    Verifies
    -------
    - Full ingestion pipeline without DB
    - Returns transformed DataFrame
    - All steps are executed correctly

    Parameters
    ----------
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked Response object
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    sample_date : date
        Sample date for initialization
    mock_logger : Logger
        Mocked logger instance

    Returns
    -------
    None
    """
    b3_portfolio = B3TheoreticalPortfolio(date_ref=sample_date, logger=mock_logger, cls_db=None)
    
    mock_requests_get.return_value = mock_response
    mocker.patch.object(b3_portfolio, "parse_raw_file", return_value=[
        {"SEGMENT": "Stock", "COD": "PETR4", "PART": 5.2}
    ])
    mocker.patch.object(b3_portfolio, "standardize_dataframe", return_value=pd.DataFrame([
        {"SEGMENT": "STOCK", "COD": "PETR4", "PART": 5.2}
    ]))
    result = b3_portfolio.run()
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result["SEGMENT"].iloc[0] == "STOCK"


def test_run_with_db(
    b3_portfolio: B3TheoreticalPortfolio, 
    mock_requests_get: MagicMock, 
    mock_response: Response, 
    mock_db_session: Session, 
    mocker: MockerFixture
) -> None:
    """Test run method with database session.

    Verifies
    -------
    - Full ingestion pipeline with DB insertion
    - Database insertion is called
    - No DataFrame returned

    Parameters
    ----------
    b3_portfolio : B3TheoreticalPortfolio
        Portfolio instance
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked Response object
    mock_db_session : Session
        Mocked database session
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    mocker.patch.object(b3_portfolio, "parse_raw_file", return_value=[
        {"SEGMENT": "Stock", "COD": "PETR4", "PART": 5.2}
    ])
    mock_standardize = mocker.patch.object(b3_portfolio, "standardize_dataframe", 
                                           return_value=pd.DataFrame([
        {"SEGMENT": "STOCK", "COD": "PETR4", "PART": 5.2}
    ]))
    mock_insert = mocker.patch.object(b3_portfolio, "insert_table_db")
    result = b3_portfolio.run(str_table_name="test_table")
    assert result is None
    mock_insert.assert_called_once_with(
        cls_db=mock_db_session,
        str_table_name="test_table",
        df_=mock_standardize.return_value,
        bool_insert_or_ignore=False
    )


@pytest.mark.parametrize("portfolio_class,table_name", [
    (B3TheoricalPortfolioIBOV, "br_b3_theorical_portfolio_ibov"),
    (B3TheoricalPortfolioIBRX50, "br_b3_theorical_portfolio_ibrx50"),
    (B3TheoricalPortfolioIBRA, "br_b3_theorical_portfolio_ibra"),
])
def test_derived_classes_run(
    portfolio_class: type, 
    table_name: str, 
    sample_date: date, 
    mock_requests_get: MagicMock, 
    mock_response: Response, 
    mocker: MockerFixture
) -> None:
    """Test run method for derived classes.

    Parameters
    ----------
    portfolio_class : type
        The derived portfolio class to test
    table_name : str
        Expected table name for DB insertion
    sample_date : date
        Sample date for initialization
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked Response object
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    portfolio = portfolio_class(date_ref=sample_date)
    mock_requests_get.return_value = mock_response
    mocker.patch.object(portfolio, "parse_raw_file", return_value=[
        {"SEGMENT": "Stock", "COD": "PETR4", "PART": 5.2}
    ])
    mocker.patch.object(portfolio, "standardize_dataframe", return_value=pd.DataFrame([
        {"SEGMENT": "STOCK", "COD": "PETR4", "PART": 5.2}
    ]))
    result = portfolio.run()
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1


def test_parse_raw_file_invalid_response(
    b3_portfolio: B3TheoreticalPortfolio, 
    mocker: MockerFixture
) -> None:
    """Test parse_raw_file with invalid response.

    Verifies
    -------
    - Handles invalid JSON response gracefully
    - Raises appropriate exception

    Parameters
    ----------
    b3_portfolio : B3TheoreticalPortfolio
        Portfolio instance
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_response = MagicMock(spec=Response)
    mock_response.json.side_effect = ValueError("Invalid JSON")
    with pytest.raises(ValueError, match="Invalid JSON"):
        b3_portfolio.parse_raw_file(mock_response)


def test_transform_data_empty_input(b3_portfolio: B3TheoreticalPortfolio) -> None:
    """Test transform_data with empty input list.

    Verifies
    -------
    - Handles empty input correctly
    - Returns empty DataFrame

    Parameters
    ----------
    b3_portfolio : B3TheoreticalPortfolio
        Portfolio instance

    Returns
    -------
    None
    """
    df_ = b3_portfolio.transform_data([])
    assert isinstance(df_, pd.DataFrame)
    assert len(df_) == 0
    assert df_.empty


def test_run_timeout_error(
    b3_portfolio: B3TheoreticalPortfolio, 
    mock_requests_get: MagicMock, 
    mock_backoff: MagicMock
) -> None:
    """Test run method handling timeout errors.

    Verifies
    -------
    - TimeoutError is properly propagated
    - Backoff is bypassed in tests

    Parameters
    ----------
    b3_portfolio : B3TheoreticalPortfolio
        Portfolio instance
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_backoff : MagicMock
        Mocked backoff decorator

    Returns
    -------
    None
    """
    from requests.exceptions import Timeout
    mock_requests_get.side_effect = Timeout("Request timed out")
    with pytest.raises(Timeout, match="Request timed out"):
        b3_portfolio.run()


def test_reload_module(mocker: MockerFixture, sample_date: date) -> None:
    """Test module reloading behavior.

    Verifies
    -------
    - Module can be reloaded without errors
    - Instance initialization survives reload

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    sample_date : date
        Sample date for initialization

    Returns
    -------
    None
    """
    mocker.patch("requests.get")  # Ensure no real HTTP calls
    _ = B3TheoreticalPortfolio(date_ref=sample_date)
    # Fix the module path to match the actual import
    importlib.reload(sys.modules[
        "stpstone.ingestion.countries.br.exchange.b3_indexes_theor_portfs"])
    new_portfolio = B3TheoreticalPortfolio(date_ref=sample_date)
    assert new_portfolio.date_ref == sample_date
    assert new_portfolio.url == "FILL_ME"


def test_init_none_date_ref(mock_logger: Logger, mocker: MockerFixture) -> None:
    """Test initialization with None date_ref.

    Verifies
    -------
    - Falls back to previous working day
    - Other attributes are properly initialized

    Parameters
    ----------
    mock_logger : Logger
        Mocked logger instance
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_dates_br = mocker.patch.object(
        DatesBRAnbima, "add_working_days", return_value=date(2023, 1, 1))
    portfolio = B3TheoreticalPortfolio(date_ref=None, logger=mock_logger)
    assert portfolio.date_ref == date(2023, 1, 1)
    mock_dates_br.assert_called_once()


def test_standardize_dataframe_types(
    b3_portfolio: B3TheoreticalPortfolio, 
    mocker: MockerFixture
) -> None:
    """Test standardize_dataframe with type enforcement.

    Verifies
    -------
    - Correct column types are enforced
    - Case conversion is applied correctly

    Parameters
    ----------
    b3_portfolio : B3TheoreticalPortfolio
        Portfolio instance
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    df_ = pd.DataFrame([
        {"segment": "Stock", "cod": "PETR4", "part": 5.2, "date_header": "01/01/23"}
    ])
    mock_standardize = mocker.patch.object(b3_portfolio, "standardize_dataframe", return_value=df_)
    result = b3_portfolio.standardize_dataframe(
        df_=df_,
        date_ref=b3_portfolio.date_ref,
        dict_dtypes={
            "SEGMENT": str,
            "COD": str,
            "PART": float,
            "DATE_HEADER": "date"
        },
        str_fmt_dt="DD/MM/YY",
        url=b3_portfolio.url,
        cols_from_case="camel",
        cols_to_case="upper_constant"
    )
    assert result.equals(df_)
    mock_standardize.assert_called_once()


def test_insert_table_db_called(
    b3_portfolio: B3TheoreticalPortfolio, 
    mock_db_session: Session, 
    mocker: MockerFixture
) -> None:
    """Test database insertion call.

    Verifies
    -------
    - insert_table_db is called with correct parameters
    - No return value when DB session is provided

    Parameters
    ----------
    b3_portfolio : B3TheoreticalPortfolio
        Portfolio instance
    mock_db_session : Session
        Mocked database session
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    df_ = pd.DataFrame([{"SEGMENT": "STOCK", "COD": "PETR4", "PART": 5.2}])
    mock_insert = mocker.patch.object(b3_portfolio, "insert_table_db")
    b3_portfolio.cls_db = mock_db_session
    b3_portfolio.insert_table_db(cls_db=mock_db_session, str_table_name="test_table", 
                                 df_=df_, bool_insert_or_ignore=True)
    mock_insert.assert_called_once_with(cls_db=mock_db_session, str_table_name="test_table", 
                                        df_=df_, bool_insert_or_ignore=True)


def test_run_invalid_table_name(
    mock_requests_get: MagicMock, 
    mock_response: Response, 
    mocker: MockerFixture, 
    sample_date: date, 
    mock_logger: Logger
) -> None:
    """Test run with invalid table name.

    Verifies
    -------
    - Raises ValueError for empty table name when DB session is provided
    - Validates table name requirement

    Parameters
    ----------
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked Response object
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    sample_date : date
        Sample date for initialization
    mock_logger : Logger
        Mocked logger instance

    Returns
    -------
    None
    """
    # Create portfolio WITH DB session to trigger validation
    mock_db = MagicMock(spec=Session)
    mock_db.insert = MagicMock()
    b3_portfolio = B3TheoreticalPortfolio(date_ref=sample_date, logger=mock_logger, cls_db=mock_db)
    
    mock_requests_get.return_value = mock_response
    mocker.patch.object(b3_portfolio, "parse_raw_file", return_value=[
        {"SEGMENT": "Stock", "COD": "PETR4", "PART": 5.2}
    ])
    mocker.patch.object(b3_portfolio, "standardize_dataframe", return_value=pd.DataFrame([
        {"SEGMENT": "STOCK", "COD": "PETR4", "PART": 5.2}
    ]))
    
    # Test that empty table name raises ValueError
    with pytest.raises(ValueError, match="str_table_name cannot be empty"):
        b3_portfolio.run(str_table_name="")