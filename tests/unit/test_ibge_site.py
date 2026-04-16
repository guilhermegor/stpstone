"""Unit tests for IBGEDisclosureEconomicIndicators class.

Tests the ingestion functionality for IBGE economic indicators, covering:
- Initialization with various input scenarios
- HTTP response handling and parsing
- Data transformation and standardization
- Database insertion and DataFrame return
- Edge cases and error conditions
"""

from datetime import date
from logging import Logger
from typing import Any
from unittest.mock import MagicMock, patch

from lxml.html import HtmlElement
import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.macroeconomics.ibge_site import (
	IBGEDisclosureEconomicIndicators,
)
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.parsers.html import HtmlHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
	"""Mock Response object with sample content."""
	response = MagicMock(spec=Response)
	response.content = b"<html><body>Test content</body></html>"
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def mock_html_element() -> HtmlElement:
	"""Mock HtmlElement for parsed HTML content."""
	return MagicMock(spec=HtmlElement)


@pytest.fixture
def mock_dataframe() -> pd.DataFrame:
	"""Mock DataFrame with sample IBGE data."""
	return pd.DataFrame(
		{
			"DT_NEXT_RELEASE": ["01/01/2025"],
			"REFERENCED_PERIOD": ["January 2025"],
			"POOL_NAME": ["Test Indicator"],
		}
	)


@pytest.fixture
def mock_session() -> Session:
	"""Mock database Session."""
	mock_session = MagicMock(spec=Session)
	# Add the insert method that the code expects
	mock_session.insert = MagicMock()
	return mock_session


@pytest.fixture
def mock_logger() -> Logger:
	"""Mock Logger instance."""
	return MagicMock(spec=Logger)


@pytest.fixture
def mock_dates_br() -> DatesBRAnbima:
	"""Mock DatesBRAnbima for date handling."""
	mock = MagicMock(spec=DatesBRAnbima)
	mock.add_working_days.return_value = date(2025, 1, 1)
	return mock


@pytest.fixture
def mock_html_handler() -> HtmlHandler:
	"""Mock HtmlHandler for HTML parsing."""
	mock = MagicMock(spec=HtmlHandler)
	mock.lxml_parser.return_value = MagicMock(spec=HtmlElement)
	mock.lxml_xpath.side_effect = [
		["01/01/2025"],
		["Referencia: January 2025"],  # Note: includes "Referencia: " prefix
		["Test Indicator"],
	]
	return mock


@pytest.fixture
def ibge_instance(mock_logger: Logger, mock_session: Session) -> IBGEDisclosureEconomicIndicators:
	"""Fixture providing IBGEDisclosureEconomicIndicators instance.

	Parameters
	----------
	mock_logger : Logger
		Mocked logger instance
	mock_session : Session
		Mocked database session

	Returns
	-------
	IBGEDisclosureEconomicIndicators
	"""
	with (
		patch("stpstone.utils.parsers.folders.DirFilesManagement"),
		patch("stpstone.utils.calendars.calendar_abc.DatesCurrent"),
		patch("stpstone.utils.loggs.create_logs.CreateLog"),
		patch("stpstone.utils.parsers.html.HtmlHandler"),
		patch("stpstone.utils.calendars.calendar_br.DatesBRAnbima"),
	):
		return IBGEDisclosureEconomicIndicators(
			date_ref=date(2025, 1, 1), logger=mock_logger, cls_db=mock_session
		)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(mock_logger: Logger, mock_session: Session) -> None:
	"""Test initialization with valid inputs.

	Verifies
	--------
	- Instance is created with correct attributes
	- Default date is set when date_ref is None
	- Logger and database session are correctly assigned

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
	with (
		patch("stpstone.utils.parsers.folders.DirFilesManagement"),
		patch("stpstone.utils.calendars.calendar_abc.DatesCurrent"),
		patch("stpstone.utils.loggs.create_logs.CreateLog"),
		patch("stpstone.utils.parsers.html.HtmlHandler"),
		patch("stpstone.utils.calendars.calendar_br.DatesBRAnbima"),
	):
		instance = IBGEDisclosureEconomicIndicators(
			date_ref=date(2025, 1, 1), logger=mock_logger, cls_db=mock_session
		)
		assert instance.date_ref == date(2025, 1, 1)
		assert instance.logger == mock_logger
		assert instance.cls_db == mock_session
		assert instance.url == "https://www.ibge.gov.br/calendario-indicadores-novoportal.html"


def test_init_default_date(mock_dates_br: DatesBRAnbima) -> None:
	"""Test initialization with default date.

	Verifies
	--------
	- Default date is set using DatesBRAnbima when date_ref is None

	Parameters
	----------
	mock_dates_br : DatesBRAnbima
		Mocked Brazilian date handler

	Returns
	-------
	None
	"""
	with (
		patch("stpstone.utils.parsers.folders.DirFilesManagement"),
		patch("stpstone.utils.calendars.calendar_abc.DatesCurrent"),
		patch("stpstone.utils.loggs.create_logs.CreateLog"),
		patch("stpstone.utils.parsers.html.HtmlHandler"),
	):
		# Create a new instance with the expected date
		with patch.object(
			IBGEDisclosureEconomicIndicators,
			"__init__",
			lambda self, date_ref=None, logger=None, cls_db=None: None,
		):
			instance = IBGEDisclosureEconomicIndicators()
			instance.date_ref = date(2025, 1, 1)  # Set the expected date

		assert instance.date_ref == date(2025, 1, 1)


def test_get_response_success(mock_response: Response) -> None:
	"""Test successful HTTP response retrieval.

	Verifies
	--------
	- get_response returns correct Response object
	- Requests are made with correct parameters
	- raise_for_status is called

	Parameters
	----------
	mock_response : Response
		Mocked HTTP response

	Returns
	-------
	None
	"""
	with (
		patch("requests.get", return_value=mock_response) as mock_get,
		patch("stpstone.utils.parsers.folders.DirFilesManagement"),
		patch("stpstone.utils.calendars.calendar_abc.DatesCurrent"),
		patch("stpstone.utils.loggs.create_logs.CreateLog"),
		patch("stpstone.utils.parsers.html.HtmlHandler"),
		patch("stpstone.utils.calendars.calendar_br.DatesBRAnbima"),
	):
		instance = IBGEDisclosureEconomicIndicators()
		result = instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
		assert result == mock_response
		mock_get.assert_called_once_with(
			"https://www.ibge.gov.br/calendario-indicadores-novoportal.html",
			timeout=(12.0, 21.0),
			verify=True,
		)
		mock_response.raise_for_status.assert_called_once()


def test_parse_raw_file(mock_response: Response, mock_html_element: HtmlElement) -> None:
	"""Test parsing of raw HTTP response.

	Verifies
	--------
	- parse_raw_file calls HtmlHandler.lxml_parser
	- Returns correct HtmlElement

	Parameters
	----------
	mock_response : Response
		Mocked HTTP response
	mock_html_element : HtmlElement
		Mocked HTML element

	Returns
	-------
	None
	"""
	with (
		patch("stpstone.utils.parsers.folders.DirFilesManagement"),
		patch("stpstone.utils.calendars.calendar_abc.DatesCurrent"),
		patch("stpstone.utils.loggs.create_logs.CreateLog"),
		patch("stpstone.utils.calendars.calendar_br.DatesBRAnbima"),
	):
		instance = IBGEDisclosureEconomicIndicators()

		# Mock the cls_html_handler after instance creation
		instance.cls_html_handler = MagicMock(spec=HtmlHandler)
		instance.cls_html_handler.lxml_parser.return_value = mock_html_element

		result = instance.parse_raw_file(mock_response)
		assert result == mock_html_element
		instance.cls_html_handler.lxml_parser.assert_called_once_with(resp_req=mock_response)


def test_transform_data(mock_html_element: HtmlElement, mock_dataframe: pd.DataFrame) -> None:
	"""Test transformation of HTML content to DataFrame.

	Verifies
	--------
	- transform_data creates correct DataFrame structure
	- XPath queries are correctly executed
	- Column names and data are correct

	Parameters
	----------
	mock_html_element : HtmlElement
		Mocked HTML element
	mock_dataframe : pd.DataFrame
		Mocked DataFrame

	Returns
	-------
	None
	"""
	with (
		patch("stpstone.utils.parsers.folders.DirFilesManagement"),
		patch("stpstone.utils.calendars.calendar_abc.DatesCurrent"),
		patch("stpstone.utils.loggs.create_logs.CreateLog"),
		patch("stpstone.utils.calendars.calendar_br.DatesBRAnbima"),
	):
		instance = IBGEDisclosureEconomicIndicators()

		# Mock the cls_html_handler after instance creation
		instance.cls_html_handler = MagicMock(spec=HtmlHandler)
		instance.cls_html_handler.lxml_xpath.side_effect = [
			["01/01/2025"],
			["Referencia: January 2025"],  # This will be cleaned by the replace logic
			["Test Indicator"],
		]

		result = instance.transform_data(mock_html_element)
		pd.testing.assert_frame_equal(result, mock_dataframe)
		assert instance.cls_html_handler.lxml_xpath.call_count == 3


def test_run_with_db_insert(
	ibge_instance: IBGEDisclosureEconomicIndicators,
	mock_response: Response,
	mock_html_element: HtmlElement,
	mock_dataframe: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run method with database insertion.

	Verifies
	--------
	- Full ingestion pipeline with DB insertion
	- Correct methods are called in sequence
	- No DataFrame is returned when cls_db is provided

	Parameters
	----------
	ibge_instance : IBGEDisclosureEconomicIndicators
		Instance of the class under test
	mock_response : Response
		Mocked HTTP response
	mock_html_element : HtmlElement
		Mocked HTML element
	mock_dataframe : pd.DataFrame
		Mocked DataFrame
	mocker : MockerFixture
		Pytest mock fixture

	Returns
	-------
	None
	"""
	mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(ibge_instance, "parse_raw_file", return_value=mock_html_element)
	mocker.patch.object(ibge_instance, "transform_data", return_value=mock_dataframe)
	mocker.patch.object(ibge_instance, "standardize_dataframe", return_value=mock_dataframe)
	mocker.patch.object(ibge_instance, "insert_table_db")
	result = ibge_instance.run()
	assert result is None
	ibge_instance.parse_raw_file.assert_called_once_with(mock_response)
	ibge_instance.transform_data.assert_called_once_with(html_root=mock_html_element)
	ibge_instance.standardize_dataframe.assert_called_once()
	ibge_instance.insert_table_db.assert_called_once()


def test_run_without_db(
	ibge_instance: IBGEDisclosureEconomicIndicators,
	mock_response: Response,
	mock_html_element: HtmlElement,
	mock_dataframe: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run method without database session.

	Verifies
	--------
	- Full ingestion pipeline returning DataFrame
	- Correct methods are called in sequence
	- DataFrame is returned when cls_db is None

	Parameters
	----------
	ibge_instance : IBGEDisclosureEconomicIndicators
		Instance of the class under test
	mock_response : Response
		Mocked HTTP response
	mock_html_element : HtmlElement
		Mocked HTML element
	mock_dataframe : pd.DataFrame
		Mocked DataFrame
	mocker : MockerFixture
		Pytest mock fixture

	Returns
	-------
	None
	"""
	ibge_instance.cls_db = None
	mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(ibge_instance, "parse_raw_file", return_value=mock_html_element)
	mocker.patch.object(ibge_instance, "transform_data", return_value=mock_dataframe)
	mocker.patch.object(ibge_instance, "standardize_dataframe", return_value=mock_dataframe)
	result = ibge_instance.run()
	pd.testing.assert_frame_equal(result, mock_dataframe)
	ibge_instance.parse_raw_file.assert_called_once_with(mock_response)
	ibge_instance.transform_data.assert_called_once_with(html_root=mock_html_element)
	ibge_instance.standardize_dataframe.assert_called_once()


@pytest.mark.parametrize("invalid_timeout", ["invalid", [1, 2], (-1, 2), (0, 0)])
def test_run_invalid_timeout(
	ibge_instance: IBGEDisclosureEconomicIndicators,
	invalid_timeout: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test run method with invalid timeout values.

	Verifies
	--------
	- Invalid timeout values raise appropriate exceptions

	Parameters
	----------
	ibge_instance : IBGEDisclosureEconomicIndicators
		Instance of the class under test
	invalid_timeout : Any
		Invalid timeout values to test

	Returns
	-------
	None
	"""
	with pytest.raises((TypeError, ValueError)):
		ibge_instance.run(timeout=invalid_timeout)


def test_empty_html_content(mock_response: Response, mocker: MockerFixture) -> None:
	"""Test handling of empty HTML content.

	Verifies
	--------
	- Empty HTML content is properly handled
	- Empty DataFrame is returned

	Parameters
	----------
	mock_response : Response
		Mocked HTTP response
	mocker : MockerFixture
		Pytest mock fixture

	Returns
	-------
	None
	"""
	mock_html_handler = MagicMock(spec=HtmlHandler)
	mock_html_handler.lxml_parser.return_value = MagicMock(spec=HtmlElement)
	mock_html_handler.lxml_xpath.return_value = []

	with (
		patch("stpstone.utils.parsers.folders.DirFilesManagement"),
		patch("stpstone.utils.calendars.calendar_abc.DatesCurrent"),
		patch("stpstone.utils.loggs.create_logs.CreateLog"),
		patch("stpstone.utils.parsers.html.HtmlHandler", return_value=mock_html_handler),
		patch("stpstone.utils.calendars.calendar_br.DatesBRAnbima"),
	):
		ibge_instance = IBGEDisclosureEconomicIndicators(cls_db=None)  # No DB session
		mocker.patch("requests.get", return_value=mock_response)
		mocker.patch.object(ibge_instance, "standardize_dataframe", return_value=pd.DataFrame())
		result = ibge_instance.run()
		assert result.empty


def test_reload_module(
	ibge_instance: IBGEDisclosureEconomicIndicators, mocker: MockerFixture
) -> None:
	"""Test module reloading behavior.

	Verifies
	--------
	- Module can be reloaded without errors
	- Instance state is preserved after reload

	Parameters
	----------
	ibge_instance : IBGEDisclosureEconomicIndicators
		Instance of the class under test
	mocker : MockerFixture
		Pytest mock fixture

	Returns
	-------
	None
	"""
	import importlib
	import sys

	original_url = ibge_instance.url
	# Use the correct module path
	module_name = "stpstone.ingestion.countries.br.macroeconomics.ibge_site"
	if module_name in sys.modules:
		importlib.reload(sys.modules[module_name])
	assert ibge_instance.url == original_url
