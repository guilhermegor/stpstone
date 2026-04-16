"""Unit tests for B3FuturesClosingAdj class.

Tests the B3 Futures Closing Adjustments ingestion functionality with various input scenarios
including:
- Initialization with valid and invalid inputs
- HTTP response handling
- HTML parsing and data transformation
- Database operations and DataFrame standardization
- Edge cases and error conditions
"""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union
from unittest.mock import MagicMock, patch

from lxml.html import HtmlElement
import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.exchange.b3_futures_closing_adj import B3FuturesClosingAdj
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
def mock_response(mocker: MockerFixture) -> Response:
	"""Mock HTTP Response object with sample content.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects

	Returns
	-------
	Response
		Mocked Response object with HTML content
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.text = (
		"<html><body><table id='tblDadosAjustes'><tbody><tr><td>Test</td>"
		+ "</tr></tbody></table></body></html>"
	)
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def mock_html_element(mocker: MockerFixture) -> HtmlElement:
	"""Mock lxml HtmlElement with sample table data.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects

	Returns
	-------
	HtmlElement
		Mocked HTML element with table structure
	"""
	html = mocker.MagicMock(spec=HtmlElement)
	html.xpath.return_value = [mocker.MagicMock(text="Test Data")]
	return html


@pytest.fixture
def mock_dataframe() -> pd.DataFrame:
	"""Fixture providing sample DataFrame for testing.

	Returns
	-------
	pd.DataFrame
		Sample DataFrame with expected columns and data
	"""
	return pd.DataFrame(
		{
			"MERCADORIA": ["Test"],
			"VENCIMENTO": ["2023-01-01"],
			"PRECO_DE_AJUSTE_ANTERIOR": [100.0],
			"PRECO_DE_AJUSTE_ATUAL": [101.0],
			"VARIACAO": [1.0],
			"VALOR_DO_AJUSTE_POR_CONTRATO_BRL": [10.0],
		}
	)


@pytest.fixture
def b3_instance(mocker: MockerFixture) -> B3FuturesClosingAdj:
	"""Fixture providing B3FuturesClosingAdj instance with mocked dependencies.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects

	Returns
	-------
	B3FuturesClosingAdj
		Instance initialized with mocked dependencies
	"""
	# Mock the date dependencies to ensure consistent behavior
	mock_dates_current = mocker.MagicMock(spec=DatesCurrent)
	mock_dates_current.curr_date.return_value = date(2023, 1, 2)

	mock_dates_br = mocker.MagicMock(spec=DatesBRAnbima)
	mock_dates_br.add_working_days.return_value = date(2023, 1, 1)

	# Patch the classes before instantiation
	with (
		patch(
			"stpstone.ingestion.countries.br.exchange.b3_futures_closing_adj.DatesCurrent",
			return_value=mock_dates_current,
		),
		patch(
			"stpstone.ingestion.countries.br.exchange.b3_futures_closing_adj.DatesBRAnbima",
			return_value=mock_dates_br,
		),
	):
		return B3FuturesClosingAdj()


@pytest.fixture
def mock_dates_current(mocker: MockerFixture) -> DatesCurrent:
	"""Mock DatesCurrent class for consistent date handling.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects

	Returns
	-------
	DatesCurrent
		Mocked DatesCurrent instance
	"""
	dates_current = mocker.MagicMock(spec=DatesCurrent)
	dates_current.curr_date.return_value = date(2023, 1, 2)
	return dates_current


@pytest.fixture
def mock_dates_br(mocker: MockerFixture) -> DatesBRAnbima:
	"""Mock DatesBRAnbima class for consistent date handling.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects

	Returns
	-------
	DatesBRAnbima
		Mocked DatesBRAnbima instance
	"""
	dates_br = mocker.MagicMock(spec=DatesBRAnbima)
	dates_br.add_working_days.return_value = date(2023, 1, 1)
	return dates_br


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(b3_instance: B3FuturesClosingAdj) -> None:
	"""Test initialization with valid inputs.

	Verifies
	-------
	- Instance is properly initialized with default or provided values
	- All required attributes are set
	- URL is correctly formatted with date

	Parameters
	----------
	b3_instance : B3FuturesClosingAdj
		Instance to be tested

	Returns
	-------
	None
	"""
	assert isinstance(b3_instance.date_ref, date)
	assert isinstance(b3_instance.cls_html_handler, HtmlHandler)
	assert isinstance(b3_instance.cls_dict_handler, HandlingDicts)
	assert isinstance(b3_instance.cls_dir_files_management, DirFilesManagement)
	assert isinstance(b3_instance.cls_create_log, CreateLog)
	assert b3_instance.url.startswith("https://www2.bmf.com.br")
	# Check that the date appears in the URL (URL encoded format)
	expected_date = b3_instance.date_ref.strftime("%d%%2F%m%%2F%Y")
	assert expected_date in b3_instance.url


def test_init_with_custom_date(mocker: MockerFixture) -> None:
	"""Test initialization with a custom reference date.

	Verifies
	-------
	- Custom date is properly set
	- URL reflects the custom date
	- Other attributes remain valid

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects

	Returns
	-------
	None
	"""
	custom_date = date(2022, 12, 31)

	# Mock dependencies
	mock_dates_current = mocker.MagicMock(spec=DatesCurrent)
	mock_dates_br = mocker.MagicMock(spec=DatesBRAnbima)

	with (
		patch(
			"stpstone.ingestion.countries.br.exchange.b3_futures_closing_adj.DatesCurrent",
			return_value=mock_dates_current,
		),
		patch(
			"stpstone.ingestion.countries.br.exchange.b3_futures_closing_adj.DatesBRAnbima",
			return_value=mock_dates_br,
		),
	):
		instance = B3FuturesClosingAdj(date_ref=custom_date)

	assert instance.date_ref == custom_date
	# Check URL encoded format
	expected_date = custom_date.strftime("%d%%2F%m%%2F%Y")
	assert expected_date in instance.url


def test_init_with_logger_and_db(mocker: MockerFixture) -> None:
	"""Test initialization with logger and database session.

	Verifies
	-------
	- Logger and database session are properly assigned
	- Default date handling works as expected

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects

	Returns
	-------
	None
	"""
	mock_logger = mocker.MagicMock(spec=Logger)
	mock_db = mocker.MagicMock(spec=Session)

	# Mock dependencies
	mock_dates_current = mocker.MagicMock(spec=DatesCurrent)
	mock_dates_current.curr_date.return_value = date(2023, 1, 2)
	mock_dates_br = mocker.MagicMock(spec=DatesBRAnbima)
	mock_dates_br.add_working_days.return_value = date(2023, 1, 1)

	with (
		patch(
			"stpstone.ingestion.countries.br.exchange.b3_futures_closing_adj.DatesCurrent",
			return_value=mock_dates_current,
		),
		patch(
			"stpstone.ingestion.countries.br.exchange.b3_futures_closing_adj.DatesBRAnbima",
			return_value=mock_dates_br,
		),
	):
		instance = B3FuturesClosingAdj(logger=mock_logger, cls_db=mock_db)

	assert instance.logger == mock_logger
	assert instance.cls_db == mock_db


def test_get_response_success(
	b3_instance: B3FuturesClosingAdj, mock_response: Response, mocker: MockerFixture
) -> None:
	"""Test successful HTTP response retrieval.

	Verifies
	-------
	- get_response returns valid Response object
	- Correct headers and timeout are used
	- raise_for_status is called

	Parameters
	----------
	b3_instance : B3FuturesClosingAdj
		B3FuturesClosingAdj instance from fixture
	mock_response : Response
		Mocked Response object
	mocker : MockerFixture
		Pytest mocker for patching requests

	Returns
	-------
	None
	"""
	mock_get = mocker.patch("requests.get", return_value=mock_response)
	with patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func):
		response = b3_instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
	assert response == mock_response
	mock_response.raise_for_status.assert_called_once()
	mock_get.assert_called_once()


def test_parse_raw_file(
	b3_instance: B3FuturesClosingAdj, mock_response: Response, mocker: MockerFixture
) -> None:
	"""Test parsing of raw file content.

	Verifies
	-------
	- parse_raw_file returns StringIO object
	- HTML handler is called correctly

	Parameters
	----------
	b3_instance : B3FuturesClosingAdj
		B3FuturesClosingAdj instance from fixture
	mock_response : Response
		Mocked Response object
	mocker : MockerFixture
		Pytest mocker for patching HTML handler

	Returns
	-------
	None
	"""
	mock_stringio = mocker.MagicMock(spec=StringIO)
	mocker.patch.object(b3_instance.cls_html_handler, "lxml_parser", return_value=mock_stringio)
	result = b3_instance.parse_raw_file(mock_response)
	assert result == mock_stringio
	b3_instance.cls_html_handler.lxml_parser.assert_called_once_with(resp_req=mock_response)


def test_transform_data(
	b3_instance: B3FuturesClosingAdj, mock_html_element: HtmlElement, mocker: MockerFixture
) -> None:
	"""Test data transformation into DataFrame.

	Verifies
	-------
	- transform_data returns a DataFrame
	- HTML parsing and data pairing are correct
	- Data cleaning (replace commas, etc.) is applied

	Parameters
	----------
	b3_instance : B3FuturesClosingAdj
		B3FuturesClosingAdj instance from fixture
	mock_html_element : HtmlElement
		Mocked HTML element
	mocker : MockerFixture
		Pytest mocker for patching HTML and dict handlers

	Returns
	-------
	None
	"""
	mock_data = ["Test", "2023-01-01", "100,00", "101,00", "1,00", "10,00"]
	mock_elements = [mocker.MagicMock(text=x) for x in mock_data]

	mocker.patch.object(b3_instance.cls_html_handler, "lxml_xpath", return_value=mock_elements)

	# Mock the data pairing to return list of dictionaries
	mock_paired_data = [
		{
			"MERCADORIA": "Test",
			"VENCIMENTO": "2023-01-01",
			"PRECO_DE_AJUSTE_ANTERIOR": "100.00",
			"PRECO_DE_AJUSTE_ATUAL": "101.00",
			"VARIACAO": "1.00",
			"VALOR_DO_AJUSTE_POR_CONTRATO_BRL": "10.00",
		}
	]
	mocker.patch.object(
		b3_instance.cls_dict_handler, "pair_headers_with_data", return_value=mock_paired_data
	)

	result = b3_instance.transform_data(mock_html_element)
	assert isinstance(result, pd.DataFrame)
	b3_instance.cls_html_handler.lxml_xpath.assert_called_once()
	b3_instance.cls_dict_handler.pair_headers_with_data.assert_called_once()


def test_run_without_db(
	b3_instance: B3FuturesClosingAdj,
	mock_response: Response,
	mock_html_element: HtmlElement,
	mock_dataframe: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run method without database session.

	Verifies
	-------
	- run returns a DataFrame when no database session is provided
	- All steps (response, parsing, transformation, standardization) are called
	- Correct parameters are passed through

	Parameters
	----------
	b3_instance : B3FuturesClosingAdj
		B3FuturesClosingAdj instance from fixture
	mock_response : Response
		Mocked Response object
	mock_html_element : HtmlElement
		Mocked HTML element
	mock_dataframe : pd.DataFrame
		Mocked DataFrame
	mocker : MockerFixture
		Pytest mocker for patching dependencies

	Returns
	-------
	None
	"""
	_ = mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(b3_instance, "parse_raw_file", return_value=mock_html_element)
	mocker.patch.object(b3_instance, "transform_data", return_value=mock_dataframe)
	mocker.patch.object(b3_instance, "standardize_dataframe", return_value=mock_dataframe)

	with patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func):
		result = b3_instance.run(timeout=(12.0, 21.0), bool_verify=True)

	assert result.equals(mock_dataframe)
	b3_instance.parse_raw_file.assert_called_once_with(mock_response)
	b3_instance.transform_data.assert_called_once_with(html_root=mock_html_element)
	b3_instance.standardize_dataframe.assert_called_once()


def test_run_with_db(
	b3_instance: B3FuturesClosingAdj,
	mock_response: Response,
	mock_html_element: HtmlElement,
	mock_dataframe: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run method with database session.

	Verifies
	-------
	- run inserts data into database when session is provided
	- No DataFrame is returned
	- Database insertion is called with correct parameters

	Parameters
	----------
	b3_instance : B3FuturesClosingAdj
		B3FuturesClosingAdj instance from fixture
	mock_response : Response
		Mocked Response object
	mock_html_element : HtmlElement
		Mocked HTML element
	mock_dataframe : pd.DataFrame
		Mocked DataFrame
	mocker : MockerFixture
		Pytest mocker for patching dependencies

	Returns
	-------
	None
	"""
	mock_db = mocker.MagicMock(spec=Session)
	b3_instance.cls_db = mock_db
	_ = mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(b3_instance, "parse_raw_file", return_value=mock_html_element)
	mocker.patch.object(b3_instance, "transform_data", return_value=mock_dataframe)
	mocker.patch.object(b3_instance, "standardize_dataframe", return_value=mock_dataframe)
	mocker.patch.object(b3_instance, "insert_table_db")

	with patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func):
		result = b3_instance.run(bool_insert_or_ignore=True, str_table_name="test_table")

	assert result is None
	b3_instance.insert_table_db.assert_called_once_with(
		cls_db=mock_db, str_table_name="test_table", df_=mock_dataframe, bool_insert_or_ignore=True
	)


@pytest.mark.parametrize("timeout", [(12.0, 21.0), 10, 10.0, (5.0, 10.0), (5, 10)])
def test_run_various_timeouts(
	b3_instance: B3FuturesClosingAdj,
	mock_response: Response,
	mock_html_element: HtmlElement,
	mock_dataframe: pd.DataFrame,
	mocker: MockerFixture,
	timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]],
) -> None:
	"""Test run method with various timeout configurations.

	Verifies
	-------
	- Different timeout types are handled correctly
	- Response is still processed correctly

	Parameters
	----------
	b3_instance : B3FuturesClosingAdj
		B3FuturesClosingAdj instance from fixture
	mock_response : Response
		Mocked Response object
	mock_html_element : HtmlElement
		Mocked HTML element
	mock_dataframe : pd.DataFrame
		Mocked DataFrame
	mocker : MockerFixture
		Pytest mocker for patching dependencies
	timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
		Various timeout configurations to test

	Returns
	-------
	None
	"""
	_ = mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(b3_instance, "parse_raw_file", return_value=mock_html_element)
	mocker.patch.object(b3_instance, "transform_data", return_value=mock_dataframe)
	mocker.patch.object(b3_instance, "standardize_dataframe", return_value=mock_dataframe)

	with patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func):
		result = b3_instance.run(timeout=timeout)

	assert result.equals(mock_dataframe)


def test_empty_html_response(
	b3_instance: B3FuturesClosingAdj, mock_response: Response, mocker: MockerFixture
) -> None:
	"""Test handling of empty HTML response.

	Verifies
	-------
	- Empty HTML content is handled gracefully
	- DataFrame is still returned with correct structure

	Parameters
	----------
	b3_instance : B3FuturesClosingAdj
		B3FuturesClosingAdj instance from fixture
	mock_response : Response
		Mocked Response object
	mocker : MockerFixture
		Pytest mocker for patching dependencies

	Returns
	-------
	None
	"""
	mock_response.text = (
		"<html><body><table id='tblDadosAjustes'><tbody></tbody></table></body></html>"
	)
	_ = mocker.patch("requests.get", return_value=mock_response)

	# Mock html element instead of string
	mock_html_element = mocker.MagicMock(spec=HtmlElement)
	mocker.patch.object(
		b3_instance.cls_html_handler, "lxml_parser", return_value=mock_html_element
	)
	mocker.patch.object(b3_instance.cls_html_handler, "lxml_xpath", return_value=[])
	mocker.patch.object(b3_instance.cls_dict_handler, "pair_headers_with_data", return_value=[])

	# Mock standardize_dataframe to return empty DataFrame
	empty_df = pd.DataFrame()
	mocker.patch.object(b3_instance, "standardize_dataframe", return_value=empty_df)

	with patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func):
		result = b3_instance.run()

	assert isinstance(result, pd.DataFrame)
	assert result.empty


@pytest.mark.parametrize("invalid_input", [None, "invalid", 123, [], {}])
def test_invalid_response_type(
	b3_instance: B3FuturesClosingAdj,
	invalid_input: Union[None, str, int, list, dict],
	mocker: MockerFixture,
) -> None:
	"""Test handling of invalid response types in parse_raw_file.

	Verifies
	-------
	- Invalid response types raise appropriate errors
	- Type checking is enforced

	Parameters
	----------
	b3_instance : B3FuturesClosingAdj
		B3FuturesClosingAdj instance from fixture
	invalid_input : Union[None, str, int, list, dict]
		Invalid input types to test
	mocker : MockerFixture
		Pytest mocker for patching dependencies

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be one of types"):
		b3_instance.parse_raw_file(invalid_input)
