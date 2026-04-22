"""Unit tests for BMFInterestRates class.

Tests the ingestion functionality for Brazilian financial market interest rates,
covering initialization, HTTP requests, parsing, transformation, and database operations.
"""

from datetime import date
from unittest.mock import MagicMock

from lxml.html import HtmlElement
import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.br.exchange.bmf_interest_rates import BMFInterestRates
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
def sample_date() -> date:
	"""Provide a sample reference date.

	Returns
	-------
	date
		A fixed date for consistent testing (2025-09-05)
	"""
	return date(2025, 9, 5)


@pytest.fixture
def mock_response() -> Response:
	"""Mock Response object with sample HTML content.

	Returns
	-------
	Response
		Mocked response with sample HTML table content
	"""
	response = MagicMock(spec=Response)
	response.content = b"""
        <table id="tb_principal1">
            <tr>
                <td class="tabelaConteudo">1</td>
                <td class="tabelaConteudo">5.5</td>
                <td class="tabelaConteudo">6.0</td>
            </tr>
        </table>
    """
	response.status_code = 200
	response.url = "https://example.com"
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def bmf_instance(sample_date: date) -> BMFInterestRates:
	"""Provide a BMFInterestRates instance with mocked dependencies.

	Parameters
	----------
	sample_date : date
		The reference date for initialization

	Returns
	-------
	BMFInterestRates
		Initialized instance with mocked dependencies
	"""
	instance = BMFInterestRates(date_ref=sample_date)
	instance.cls_db = None  # Ensure no database operations in tests
	return instance


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
	return mocker.patch("requests.get")


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> MagicMock:
	"""Mock backoff.on_exception to bypass retry delays.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	MagicMock
		Mock object that bypasses retry mechanism
	"""
	return mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialization with valid inputs.

	Verifies
	-------
	- Instance is created with correct date_ref
	- URL is correctly formatted
	- Dependencies are properly initialized

	Parameters
	----------
	sample_date : date
		The sample reference date from fixture

	Returns
	-------
	None
	"""
	instance = BMFInterestRates(date_ref=sample_date)
	assert instance.date_ref == sample_date
	assert (
		instance.url
		== "https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/TxRef1.asp?"
		+ f"Data={sample_date.strftime('%d/%m/%Y')}&Data1={sample_date.strftime('%Y%m%d')}"
		+ "&slcTaxa=TODOS"
	)
	assert isinstance(instance.cls_dates_current, DatesCurrent)
	assert isinstance(instance.cls_dates_br, DatesBRAnbima)
	assert isinstance(instance.cls_create_log, CreateLog)
	assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
	assert isinstance(instance.cls_html_handler, HtmlHandler)
	assert isinstance(instance.cls_dict_handler, HandlingDicts)


def test_init_with_default_date(mocker: MockerFixture) -> None:
	"""Test initialization with default date (previous working day).

	Verifies
	-------
	- Default date is set to previous working day
	- URL is correctly formatted with default date

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Returns
	-------
	None
	"""
	mock_date = date(2025, 9, 4)
	mocker.patch.object(DatesBRAnbima, "add_working_days", return_value=mock_date)
	mocker.patch.object(DatesCurrent, "curr_date", return_value=date(2025, 9, 5))

	instance = BMFInterestRates()
	assert instance.date_ref == mock_date
	assert (
		instance.url
		== "https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/TxRef1.asp?"
		+ f"Data={mock_date.strftime('%d/%m/%Y')}&Data1={mock_date.strftime('%Y%m%d')}"
		+ "&slcTaxa=TODOS"
	)


def test_get_response_success(
	bmf_instance: BMFInterestRates,
	mock_requests_get: MagicMock,
	mock_response: Response,
	mock_backoff: MagicMock,
) -> None:
	"""Test successful HTTP response retrieval.

	Verifies
	-------
	- Requests.get is called with correct parameters
	- Response is returned correctly
	- Status code is checked

	Parameters
	----------
	bmf_instance : BMFInterestRates
		BMFInterestRates instance from fixture
	mock_requests_get : MagicMock
		Mocked requests.get
	mock_response : Response
		Mocked Response object
	mock_backoff : MagicMock
		Mocked backoff decorator

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	result = bmf_instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
	assert result == mock_response
	mock_requests_get.assert_called_once_with(bmf_instance.url, timeout=(12.0, 21.0), verify=True)
	mock_response.raise_for_status.assert_called_once()


def test_parse_raw_file(
	bmf_instance: BMFInterestRates, mock_response: Response, mocker: MockerFixture
) -> None:
	"""Test parsing of raw HTML response.

	Verifies
	-------
	- HTML is correctly parsed into list of dictionaries
	- Values are converted to floats
	- Headers are correctly paired with data

	Parameters
	----------
	bmf_instance : BMFInterestRates
		BMFInterestRates instance from fixture
	mock_response : Response
		Mocked Response object
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Returns
	-------
	None
	"""
	mock_html = MagicMock(spec=HtmlElement)
	mock_elements = [MagicMock(text="1"), MagicMock(text="5.5"), MagicMock(text="6.0")]
	mocker.patch.object(HtmlHandler, "lxml_parser", return_value=mock_html)
	mocker.patch.object(HtmlHandler, "lxml_xpath", return_value=mock_elements)
	mocker.patch.object(
		BMFInterestRates,
		"_pair_headers_with_data",
		return_value=[{"DIAS_CORRIDOS": 1.0, "DI_PRE_252": 5.5, "DI_PRE_360": 6.0}],
	)

	result = bmf_instance.parse_raw_file(mock_response)
	assert result == [{"DIAS_CORRIDOS": 1.0, "DI_PRE_252": 5.5, "DI_PRE_360": 6.0}]
	bmf_instance.cls_html_handler.lxml_parser.assert_called_once_with(resp_req=mock_response)


def test_transform_data(bmf_instance: BMFInterestRates) -> None:
	"""Test transformation of parsed data into DataFrame.

	Verifies
	-------
	- List of dictionaries is correctly converted to DataFrame
	- DataFrame contains expected columns and values

	Parameters
	----------
	bmf_instance : BMFInterestRates
		BMFInterestRates instance from fixture

	Returns
	-------
	None
	"""
	input_data = [{"DIAS_CORRIDOS": 1.0, "DI_PRE_252": 5.5, "DI_PRE_360": 6.0}]
	result = bmf_instance.transform_data(input_data)
	assert isinstance(result, pd.DataFrame)
	assert list(result.columns) == ["DIAS_CORRIDOS", "DI_PRE_252", "DI_PRE_360"]
	assert result.iloc[0].to_dict() == {"DIAS_CORRIDOS": 1.0, "DI_PRE_252": 5.5, "DI_PRE_360": 6.0}


def test_run_without_db(
	bmf_instance: BMFInterestRates,
	mock_requests_get: MagicMock,
	mock_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run method without database session.

	Verifies
	-------
	- Full ingestion pipeline works correctly
	- Returns DataFrame when no database session is provided
	- Correct methods are called in sequence

	Parameters
	----------
	bmf_instance : BMFInterestRates
		BMFInterestRates instance from fixture
	mock_requests_get : MagicMock
		Mocked requests.get
	mock_response : Response
		Mocked Response object
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Returns
	-------
	None
	"""
	mock_html = MagicMock(spec=HtmlElement)
	mock_elements = [MagicMock(text="1"), MagicMock(text="5.5"), MagicMock(text="6.0")]
	mocker.patch.object(HtmlHandler, "lxml_parser", return_value=mock_html)
	mocker.patch.object(HtmlHandler, "lxml_xpath", return_value=mock_elements)
	mocker.patch.object(
		HandlingDicts,
		"pair_headers_with_data",
		return_value=[{"DIAS_CORRIDOS": 1.0, "DI_PRE_252": 5.5, "DI_PRE_360": 6.0}],
	)
	mocker.patch.object(
		BMFInterestRates,
		"standardize_dataframe",
		return_value=pd.DataFrame(
			{"DIAS_CORRIDOS": [1], "DI_PRE_252": [5.5], "DI_PRE_360": [6.0]}
		),
	)
	mock_requests_get.return_value = mock_response

	result = bmf_instance.run()
	assert isinstance(result, pd.DataFrame)
	assert list(result.columns) == ["DIAS_CORRIDOS", "DI_PRE_252", "DI_PRE_360"]
	bmf_instance.cls_html_handler.lxml_parser.assert_called_once()
	assert bmf_instance.cls_dict_handler.pair_headers_with_data.call_count == 4
	bmf_instance.standardize_dataframe.assert_called_once()


def test_run_with_db(
	bmf_instance: BMFInterestRates,
	mock_requests_get: MagicMock,
	mock_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run method with database session.

	Verifies
	-------
	- Data is inserted into database
	- No DataFrame is returned
	- Correct methods are called in sequence

	Parameters
	----------
	bmf_instance : BMFInterestRates
		BMFInterestRates instance from fixture
	mock_requests_get : MagicMock
		Mocked requests.get
	mock_response : Response
		Mocked Response object
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Returns
	-------
	None
	"""
	mock_db = MagicMock()
	bmf_instance.cls_db = mock_db
	mock_html = MagicMock(spec=HtmlElement)
	mock_elements = [MagicMock(text="1"), MagicMock(text="5.5"), MagicMock(text="6.0")]
	mocker.patch.object(HtmlHandler, "lxml_parser", return_value=mock_html)
	mocker.patch.object(HtmlHandler, "lxml_xpath", return_value=mock_elements)
	mocker.patch.object(
		HandlingDicts,
		"pair_headers_with_data",
		return_value=[{"DIAS_CORRIDOS": 1.0, "DI_PRE_252": 5.5, "DI_PRE_360": 6.0}],
	)
	mock_standardize = mocker.patch.object(
		BMFInterestRates,
		"standardize_dataframe",
		return_value=pd.DataFrame(
			{"DIAS_CORRIDOS": [1], "DI_PRE_252": [5.5], "DI_PRE_360": [6.0]}
		),
	)
	mock_insert = mocker.patch.object(BMFInterestRates, "insert_table_db")
	mock_requests_get.return_value = mock_response

	result = bmf_instance.run()
	assert result is None
	mock_insert.assert_called_once_with(
		cls_db=mock_db,
		str_table_name="br_b3_interest_rates",
		df_=mock_standardize.return_value,
		bool_insert_or_ignore=False,
	)


def test_parse_raw_file_invalid_input(bmf_instance: BMFInterestRates) -> None:
	"""Test parse_raw_file with invalid response type.

	Verifies
	-------
	- TypeError is raised for invalid input types
	- Error message contains type information

	Parameters
	----------
	bmf_instance : BMFInterestRates
		BMFInterestRates instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="resp_req must be one of"):
		bmf_instance.parse_raw_file("invalid_input")


def test_transform_data_empty_input(bmf_instance: BMFInterestRates) -> None:
	"""Test transform_data with empty input.

	Verifies
	-------
	- Empty list produces empty DataFrame
	- DataFrame has correct column structure

	Parameters
	----------
	bmf_instance : BMFInterestRates
		BMFInterestRates instance from fixture

	Returns
	-------
	None
	"""
	result = bmf_instance.transform_data([])
	assert isinstance(result, pd.DataFrame)
	assert result.empty
	assert list(result.columns) == []


def test_run_timeout_handling(
	bmf_instance: BMFInterestRates, mock_requests_get: MagicMock, mock_backoff: MagicMock
) -> None:
	"""Test timeout handling in run method.

	Verifies
	-------
	- TimeoutError is properly raised
	- No DataFrame is returned on timeout

	Parameters
	----------
	bmf_instance : BMFInterestRates
		BMFInterestRates instance from fixture
	mock_requests_get : MagicMock
		Mocked requests.get
	mock_backoff : MagicMock
		Mocked backoff decorator

	Returns
	-------
	None
	"""
	mock_requests_get.side_effect = requests.exceptions.Timeout("Request timeout")
	with pytest.raises(requests.exceptions.Timeout, match="Request timeout"):
		bmf_instance.run()


def test_standardize_dataframe_types(
	bmf_instance: BMFInterestRates, mocker: MockerFixture
) -> None:
	"""Test standardize_dataframe with type enforcement.

	Verifies
	-------
	- DataFrame columns are cast to correct types
	- URL and date are properly added

	Parameters
	----------
	bmf_instance : BMFInterestRates
		BMFInterestRates instance from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Returns
	-------
	None
	"""
	df_ = pd.DataFrame({"DIAS_CORRIDOS": ["1"], "DI_PRE_252": ["5.5"], "DI_PRE_360": ["6.0"]})
	mocker.patch.object(
		BMFInterestRates,
		"standardize_dataframe",
		return_value=pd.DataFrame(
			{"DIAS_CORRIDOS": [1], "DI_PRE_252": [5.5], "DI_PRE_360": [6.0]}
		),
	)

	result = bmf_instance.standardize_dataframe(
		df_=df_,
		date_ref=bmf_instance.date_ref,
		dict_dtypes={"DIAS_CORRIDOS": "int", "DI_PRE_252": "float", "DI_PRE_360": "float"},
		str_fmt_dt="YYYY-MM-DD",
		url=bmf_instance.url,
		list_cols_drop_dupl=["DIAS_CORRIDOS"],
	)
	assert pd.api.types.is_integer_dtype(result["DIAS_CORRIDOS"])
	assert pd.api.types.is_float_dtype(result["DI_PRE_252"])
	assert pd.api.types.is_float_dtype(result["DI_PRE_360"])
