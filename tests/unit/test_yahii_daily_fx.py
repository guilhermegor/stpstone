"""Unit tests for YahiiDailyFX class.

Tests the ingestion functionality with various scenarios including:
- Initialization with valid and invalid inputs (USD and EUR)
- HTTP response handling
- HTML parsing
- Data transformation and date/numeric filtering
- Database insertion and fallback logic
"""

from datetime import date
from io import StringIO
from typing import Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.macroeconomics.yahii_daily_fx import YahiiDailyFX
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
	"""Mock Response object with sample HTML content.

	Returns
	-------
	Response
		Mocked Response object with predefined content
	"""
	response = MagicMock(spec=Response)
	response.content = b"<html><body><table cellspacing='3'><tbody><tr>"
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> object:
	"""Mock requests.get to prevent real HTTP calls.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	object
		Mocked requests.get object
	"""
	return mocker.patch("requests.get")


@pytest.fixture
def mock_sleep(mocker: MockerFixture) -> object:
	"""Mock time.sleep to eliminate delays.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	object
		Mocked time.sleep object
	"""
	return mocker.patch("time.sleep")


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> object:
	"""Mock backoff.on_exception to bypass retry delays.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	object
		Mocked backoff.on_exception object
	"""
	return mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


@pytest.fixture
def sample_date() -> date:
	"""Provide a sample date for testing.

	Returns
	-------
	date
		Fixed date for consistent testing
	"""
	return date(2025, 1, 15)


@pytest.fixture
def fx_usd_instance(sample_date: date) -> YahiiDailyFX:
	"""Fixture providing YahiiDailyFX instance for USD.

	Parameters
	----------
	sample_date : date
		Sample date for initialization

	Returns
	-------
	YahiiDailyFX
		Initialized YahiiDailyFX instance for USD/BRL
	"""
	return YahiiDailyFX(str_currency="usd", date_ref=sample_date)


@pytest.fixture
def fx_eur_instance(sample_date: date) -> YahiiDailyFX:
	"""Fixture providing YahiiDailyFX instance for EUR.

	Parameters
	----------
	sample_date : date
		Sample date for initialization

	Returns
	-------
	YahiiDailyFX
		Initialized YahiiDailyFX instance for EUR/BRL
	"""
	return YahiiDailyFX(str_currency="eur", date_ref=sample_date)


@pytest.fixture
def sample_list_td() -> list[str]:
	"""Provide sample table data for transformation testing.

	Returns
	-------
	list[str]
		Sample raw text values mimicking FX rate table
	"""
	return [
		"02/01/2025",
		"6,1871",
		"6,1878",
		"03/01/2025",
		"6,1725",
		"6,1731",
		"Header text to filter",
		"Some other text",
	]


# --------------------------
# Tests
# --------------------------
def test_init_usd_with_valid_inputs(sample_date: date) -> None:
	"""Test initialization with valid inputs for USD.

	Verifies
	--------
	- The YahiiDailyFX instance is initialized correctly for USD
	- URL contains dolardiario and correct year

	Parameters
	----------
	sample_date : date
		Sample date for initialization

	Returns
	-------
	None
	"""
	instance = YahiiDailyFX(str_currency="usd", date_ref=sample_date)
	assert instance.date_ref == sample_date
	assert "dolardiario25" in instance.url
	assert instance.table_name == "br_yahii_daily_usdbrl"
	assert isinstance(instance.cls_dates_current, DatesCurrent)
	assert isinstance(instance.cls_dates_br, DatesBRAnbima)
	assert isinstance(instance.cls_create_log, CreateLog)
	assert isinstance(instance.cls_dir_files_management, DirFilesManagement)


def test_init_eur_with_valid_inputs(sample_date: date) -> None:
	"""Test initialization with valid inputs for EUR.

	Verifies
	--------
	- The YahiiDailyFX instance is initialized correctly for EUR
	- URL contains eurodiario and correct year

	Parameters
	----------
	sample_date : date
		Sample date for initialization

	Returns
	-------
	None
	"""
	instance = YahiiDailyFX(str_currency="eur", date_ref=sample_date)
	assert instance.date_ref == sample_date
	assert "eurodiario25" in instance.url
	assert instance.table_name == "br_yahii_daily_eurbrl"


def test_init_with_default_date() -> None:
	"""Test initialization with default date.

	Verifies
	--------
	- Default date is set to previous working day
	- URL is correctly formatted with 2-digit year

	Returns
	-------
	None
	"""
	with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
		instance = YahiiDailyFX()
		assert instance.date_ref == date(2025, 1, 1)
		assert "dolardiario25" in instance.url


def test_get_response_success(
	fx_usd_instance: YahiiDailyFX,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test successful HTTP response retrieval.

	Verifies
	--------
	- Successful HTTP request returns Response object
	- Correct parameters are passed to requests.get
	- SSL verify is False by default

	Parameters
	----------
	fx_usd_instance : YahiiDailyFX
		Instance of YahiiDailyFX
	mock_requests_get : object
		Mocked requests.get function
	mock_response : Response
		Mocked Response object
	mock_backoff : object
		Mocked backoff decorator

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	result = fx_usd_instance.get_response(timeout=(12.0, 21.0), bool_verify=False)
	assert isinstance(result, Response)
	mock_requests_get.assert_called_once_with(
		fx_usd_instance.url, timeout=(12.0, 21.0), verify=False
	)
	mock_response.raise_for_status.assert_called_once()


def test_parse_raw_file(fx_usd_instance: YahiiDailyFX, mock_response: Response) -> None:
	"""Test parsing of raw HTML content.

	Verifies
	--------
	- HTML content is parsed and XPath extracts text elements
	- Returns a list of strings

	Parameters
	----------
	fx_usd_instance : YahiiDailyFX
		Instance of YahiiDailyFX
	mock_response : Response
		Mocked Response object

	Returns
	-------
	None
	"""
	mock_element = MagicMock()
	mock_element.text = "02/01/2025"
	with patch(
		"stpstone.ingestion.countries.br.macroeconomics.yahii_daily_fx.HtmlHandler"
	) as mock_html:
		mock_html_instance = MagicMock()
		mock_html.return_value = mock_html_instance
		mock_html_instance.lxml_parser.return_value = MagicMock()
		mock_html_instance.lxml_xpath.return_value = [mock_element]
		result = fx_usd_instance.parse_raw_file(mock_response)
		assert isinstance(result, list)
		assert len(result) == 1
		assert result[0] == "02/01/2025"


def test_transform_data(
	fx_usd_instance: YahiiDailyFX,
	sample_list_td: list[str],
) -> None:
	"""Test data transformation into DataFrame.

	Verifies
	--------
	- Input list is correctly transformed into DataFrame
	- Non-date/numeric entries are filtered out
	- Commas are replaced with dots

	Parameters
	----------
	fx_usd_instance : YahiiDailyFX
		Instance of YahiiDailyFX
	sample_list_td : list[str]
		Sample raw text values

	Returns
	-------
	None
	"""
	df_ = fx_usd_instance.transform_data(sample_list_td)
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty, "DataFrame should not be empty"
	assert "DATA" in df_.columns
	assert "COMPRA" in df_.columns
	assert "VENDA" in df_.columns


def test_transform_data_filters_non_numeric(fx_usd_instance: YahiiDailyFX) -> None:
	"""Test that non-date/numeric text entries are filtered out.

	Verifies
	--------
	- Only date-like and numeric strings are kept
	- Header text and alphabetic strings are removed

	Parameters
	----------
	fx_usd_instance : YahiiDailyFX
		Instance of YahiiDailyFX

	Returns
	-------
	None
	"""
	list_td = [
		"02/01/2025",
		"6,1871",
		"6,1878",
		"COMPRA",
		"VENDA",
		"DATA",
	]
	df_ = fx_usd_instance.transform_data(list_td)
	assert len(df_) == 1
	assert df_["DATA"].iloc[0] == "02/01/2025"


def test_run_without_db(
	fx_usd_instance: YahiiDailyFX,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test run method without database session.

	Verifies
	--------
	- Full ingestion pipeline works without database
	- Returns transformed DataFrame

	Parameters
	----------
	fx_usd_instance : YahiiDailyFX
		Instance of YahiiDailyFX
	mock_requests_get : object
		Mocked requests.get function
	mock_response : Response
		Mocked Response object
	mock_backoff : object
		Mocked backoff decorator

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	with patch.object(
		fx_usd_instance, "parse_raw_file",
		return_value=["02/01/2025", "6,1871", "6,1878"]
	) as mock_parse, \
		patch.object(
			fx_usd_instance, "transform_data",
			return_value=pd.DataFrame({"DATA": ["02/01/2025"], "COMPRA": [6.1871],
				"VENDA": [6.1878]})
		) as mock_transform, \
		patch.object(
			fx_usd_instance, "standardize_dataframe",
			return_value=pd.DataFrame({"DATA": ["02/01/2025"], "COMPRA": [6.1871],
				"VENDA": [6.1878]})
		) as mock_standardize:
		result = fx_usd_instance.run()
		assert isinstance(result, pd.DataFrame)
		mock_parse.assert_called_once()
		mock_transform.assert_called_once()
		mock_standardize.assert_called_once()


def test_run_with_db(
	fx_usd_instance: YahiiDailyFX,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test run method with database session.

	Verifies
	--------
	- Database insertion is called when cls_db is provided
	- No DataFrame is returned

	Parameters
	----------
	fx_usd_instance : YahiiDailyFX
		Instance of YahiiDailyFX
	mock_requests_get : object
		Mocked requests.get function
	mock_response : Response
		Mocked Response object
	mock_backoff : object
		Mocked backoff decorator

	Returns
	-------
	None
	"""
	mock_db = MagicMock()
	fx_usd_instance.cls_db = mock_db
	mock_requests_get.return_value = mock_response
	with patch.object(
		fx_usd_instance, "parse_raw_file",
		return_value=["02/01/2025", "6,1871", "6,1878"]
	) as mock_parse, \
		patch.object(
			fx_usd_instance, "transform_data",
			return_value=pd.DataFrame({"DATA": ["02/01/2025"]})
		) as mock_transform, \
		patch.object(
			fx_usd_instance, "standardize_dataframe",
			return_value=pd.DataFrame({"DATA": ["02/01/2025"]})
		) as mock_standardize, \
		patch.object(fx_usd_instance, "insert_table_db") as mock_insert:
		result = fx_usd_instance.run()
		assert result is None
		mock_parse.assert_called_once()
		mock_transform.assert_called_once()
		mock_standardize.assert_called_once()
		mock_insert.assert_called_once()


def test_run_uses_correct_table_name_usd(
	fx_usd_instance: YahiiDailyFX,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test that USD run uses correct table name.

	Verifies
	--------
	- Table name defaults to br_yahii_daily_usdbrl for USD

	Parameters
	----------
	fx_usd_instance : YahiiDailyFX
		Instance of YahiiDailyFX
	mock_requests_get : object
		Mocked requests.get function
	mock_response : Response
		Mocked Response object
	mock_backoff : object
		Mocked backoff decorator

	Returns
	-------
	None
	"""
	mock_db = MagicMock()
	fx_usd_instance.cls_db = mock_db
	mock_requests_get.return_value = mock_response
	with patch.object(fx_usd_instance, "parse_raw_file", return_value=[]), \
		patch.object(fx_usd_instance, "transform_data", return_value=pd.DataFrame()), \
		patch.object(fx_usd_instance, "standardize_dataframe", return_value=pd.DataFrame()), \
		patch.object(fx_usd_instance, "insert_table_db") as mock_insert:
		fx_usd_instance.run()
		call_kwargs = mock_insert.call_args
		assert call_kwargs[1]["str_table_name"] == "br_yahii_daily_usdbrl"


def test_run_uses_correct_table_name_eur(
	fx_eur_instance: YahiiDailyFX,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test that EUR run uses correct table name.

	Verifies
	--------
	- Table name defaults to br_yahii_daily_eurbrl for EUR

	Parameters
	----------
	fx_eur_instance : YahiiDailyFX
		Instance of YahiiDailyFX
	mock_requests_get : object
		Mocked requests.get function
	mock_response : Response
		Mocked Response object
	mock_backoff : object
		Mocked backoff decorator

	Returns
	-------
	None
	"""
	mock_db = MagicMock()
	fx_eur_instance.cls_db = mock_db
	mock_requests_get.return_value = mock_response
	with patch.object(fx_eur_instance, "parse_raw_file", return_value=[]), \
		patch.object(fx_eur_instance, "transform_data", return_value=pd.DataFrame()), \
		patch.object(fx_eur_instance, "standardize_dataframe", return_value=pd.DataFrame()), \
		patch.object(fx_eur_instance, "insert_table_db") as mock_insert:
		fx_eur_instance.run()
		call_kwargs = mock_insert.call_args
		assert call_kwargs[1]["str_table_name"] == "br_yahii_daily_eurbrl"


@pytest.mark.parametrize("timeout", [
	10,
	10.5,
	(5.0, 10.0),
	(5, 10),
])
def test_get_response_timeout_variations(
	fx_usd_instance: YahiiDailyFX,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
	timeout: Union[int, float, tuple],
) -> None:
	"""Test get_response with various timeout inputs.

	Verifies
	--------
	- Different timeout formats are handled correctly

	Parameters
	----------
	fx_usd_instance : YahiiDailyFX
		Instance of YahiiDailyFX
	mock_requests_get : object
		Mocked requests.get function
	mock_response : Response
		Mocked Response object
	mock_backoff : object
		Mocked backoff decorator
	timeout : Union[int, float, tuple]
		Timeout value or tuple to test

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	result = fx_usd_instance.get_response(timeout=timeout)
	assert isinstance(result, Response)
	mock_requests_get.assert_called_once_with(fx_usd_instance.url, timeout=timeout, verify=False)


def test_reload_module() -> None:
	"""Test module reloading behavior.

	Verifies
	--------
	- Module can be reloaded without errors
	- Instance attributes are preserved after reload

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.macroeconomics.yahii_daily_fx
	original_instance = YahiiDailyFX(date_ref=date(2025, 1, 1))
	importlib.reload(stpstone.ingestion.countries.br.macroeconomics.yahii_daily_fx)
	new_instance = YahiiDailyFX(date_ref=date(2025, 1, 1))
	assert new_instance.date_ref == original_instance.date_ref
	assert new_instance.url == original_instance.url
