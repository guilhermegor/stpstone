"""Unit tests for BCBCurrenciesCodesQuotesBulletins ingestion module.

Tests the ingestion functionality for BCB quotes bulletins currency codes with various scenarios:
- Initialization with valid and invalid inputs
- Response handling and parsing
- Data transformation and standardization
- Database insertion and fallback logic
"""

from datetime import date
from logging import Logger
from unittest.mock import MagicMock

from lxml.html import HtmlElement
import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.macroeconomics.bcb_currencies_codes_quotes_bulletins import (
	BCBCurrenciesCodesQuotesBulletins,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response(mocker: MockerFixture) -> Response:
	"""Mock Response object with sample content.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects.

	Returns
	-------
	Response
		Mocked Response object with HTML content.
	"""
	response = MagicMock(spec=Response)
	response.content = b"Sample content"
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def mock_html_element(mocker: MockerFixture) -> HtmlElement:
	"""Mock HtmlElement for parsing tests.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects.

	Returns
	-------
	HtmlElement
		Mocked HtmlElement.
	"""
	return mocker.MagicMock(spec=HtmlElement)


@pytest.fixture
def mock_session(mocker: MockerFixture) -> Session:
	"""Mock database Session object.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects.

	Returns
	-------
	Session
		Mocked database Session.
	"""
	return mocker.MagicMock(spec=Session)


@pytest.fixture
def sample_date() -> date:
	"""Provide a sample date for testing.

	Returns
	-------
	date
		A fixed date for consistent testing.
	"""
	return date(2025, 9, 17)


@pytest.fixture
def mock_dates_current(mocker: MockerFixture) -> DatesCurrent:
	"""Mock DatesCurrent for consistent date handling.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects.

	Returns
	-------
	DatesCurrent
		Mocked DatesCurrent.
	"""
	mock = mocker.MagicMock(spec=DatesCurrent)
	mock.curr_date.return_value = date(2025, 9, 17)
	return mock


@pytest.fixture
def mock_dates_br(mocker: MockerFixture, sample_date: date) -> DatesBRAnbima:
	"""Mock DatesBRAnbima for working days calculation.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects.
	sample_date : date
		A fixed date for consistent testing.

	Returns
	-------
	DatesBRAnbima
		Mocked DatesBRAnbima.
	"""
	mock = mocker.MagicMock(spec=DatesBRAnbima)
	mock.add_working_days.return_value = sample_date
	return mock


@pytest.fixture
def sample_html_currencies() -> tuple[list[str], list[str]]:
	"""Provide sample currencies data for HTML parsing.

	Returns
	-------
	tuple[list[str], list[str]]
		A tuple containing two lists: names and codes.
	"""
	return (
		["Dolar Americano", "Euro"],
		["USD", "EUR"],
	)


# --------------------------
# Tests for BCBCurrenciesCodesQuotesBulletins
# --------------------------
class TestBCBCurrenciesCodesQuotesBulletins:
	"""Test cases for BCBCurrenciesCodesQuotesBulletins class."""

	@pytest.fixture
	def bcb_currencies_codes_quotes(
		self,
		mocker: MockerFixture,
		mock_dates_current: DatesCurrent,
		mock_dates_br: DatesBRAnbima,
	) -> BCBCurrenciesCodesQuotesBulletins:
		"""Fixture for BCBCurrenciesCodesQuotesBulletins instance.

		Parameters
		----------
		mocker : MockerFixture
			Pytest mocker for creating mock objects.
		mock_dates_current : DatesCurrent
			Mocked DatesCurrent.
		mock_dates_br : DatesBRAnbima
			Mocked DatesBRAnbima.

		Returns
		-------
		BCBCurrenciesCodesQuotesBulletins
			Mocked BCBCurrenciesCodesQuotesBulletins.
		"""
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.DirFilesManagement"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.CreateLog"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.HtmlHandler"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.DatesCurrent",
			return_value=mock_dates_current,
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.DatesBRAnbima",
			return_value=mock_dates_br,
		)
		mocker.patch("requests.get")
		mocker.patch.object(BCBCurrenciesCodesQuotesBulletins, "get_file")
		mocker.patch.object(BCBCurrenciesCodesQuotesBulletins, "insert_table_db")
		mocker.patch.object(BCBCurrenciesCodesQuotesBulletins, "standardize_dataframe")
		return BCBCurrenciesCodesQuotesBulletins()

	def test_init_with_default_params(
		self,
		mocker: MockerFixture,
		mock_dates_current: DatesCurrent,
		mock_dates_br: DatesBRAnbima,
		sample_date: date,
	) -> None:
		"""Test initialization with default parameters.

		Parameters
		----------
		mocker : MockerFixture
			Pytest mocker for creating mock objects.
		mock_dates_current : DatesCurrent
			Mocked DatesCurrent.
		mock_dates_br : DatesBRAnbima
			Mocked DatesBRAnbima.
		sample_date : date
			A fixed date for consistent testing.

		Returns
		-------
		None
		"""
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.DirFilesManagement"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.CreateLog"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.HtmlHandler"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.DatesCurrent",
			return_value=mock_dates_current,
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.DatesBRAnbima",
			return_value=mock_dates_br,
		)

		instance = BCBCurrenciesCodesQuotesBulletins()
		assert instance.date_ref == sample_date
		assert "exibeFormularioConsultaBoletim" in instance.url
		assert isinstance(instance.logger, type(None))
		assert isinstance(instance.cls_db, type(None))
		mock_dates_br.add_working_days.assert_called_once()

	def test_init_with_custom_params(
		self,
		sample_date: date,
		mock_session: Session,
		mocker: MockerFixture,
	) -> None:
		"""Test initialization with custom parameters.

		Parameters
		----------
		sample_date : date
			Sample date for testing.
		mock_session : Session
			Mocked database session.
		mocker : MockerFixture
			Pytest mocker for creating mock objects.

		Returns
		-------
		None
		"""
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.DirFilesManagement"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.CreateLog"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.HtmlHandler"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.DatesCurrent"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_currencies_codes_quotes_bulletins.DatesBRAnbima"
		)

		logger = MagicMock(spec=Logger)
		instance = BCBCurrenciesCodesQuotesBulletins(
			date_ref=sample_date, logger=logger, cls_db=mock_session
		)
		assert instance.date_ref == sample_date
		assert instance.logger == logger
		assert instance.cls_db == mock_session

	def test_get_response_success(
		self,
		bcb_currencies_codes_quotes: BCBCurrenciesCodesQuotesBulletins,
		mock_response: Response,
		mocker: MockerFixture,
	) -> None:
		"""Test successful HTTP response retrieval.

		Parameters
		----------
		bcb_currencies_codes_quotes : BCBCurrenciesCodesQuotesBulletins
			Instance of BCBCurrenciesCodesQuotesBulletins.
		mock_response : Response
			Mocked HTTP response.
		mocker : MockerFixture
			Pytest mocker for creating mock objects.

		Returns
		-------
		None
		"""
		mocker.patch("requests.get", return_value=mock_response)
		result = bcb_currencies_codes_quotes.get_response(timeout=1000)
		assert result == mock_response
		mock_response.raise_for_status.assert_called_once()

	def test_parse_raw_file(
		self,
		bcb_currencies_codes_quotes: BCBCurrenciesCodesQuotesBulletins,
		mock_response: Response,
		mock_html_element: HtmlElement,
		mocker: MockerFixture,
	) -> None:
		"""Test parsing of raw HTML content.

		Parameters
		----------
		bcb_currencies_codes_quotes : BCBCurrenciesCodesQuotesBulletins
			Instance of BCBCurrenciesCodesQuotesBulletins.
		mock_response : Response
			Mocked HTTP response.
		mock_html_element : HtmlElement
			Mocked HtmlElement.
		mocker : MockerFixture
			Pytest mocker for creating mock objects.

		Returns
		-------
		None
		"""
		mocker.patch.object(
			bcb_currencies_codes_quotes.cls_html_handler,
			"lxml_parser",
			return_value=mock_html_element,
		)
		result = bcb_currencies_codes_quotes.parse_raw_file(mock_response)
		assert result == mock_html_element

	def test_transform_data(
		self,
		bcb_currencies_codes_quotes: BCBCurrenciesCodesQuotesBulletins,
		mock_html_element: HtmlElement,
		sample_html_currencies: tuple[list[str], list[str]],
		mocker: MockerFixture,
	) -> None:
		"""Test transformation of HTML content to DataFrame.

		Parameters
		----------
		bcb_currencies_codes_quotes : BCBCurrenciesCodesQuotesBulletins
			Instance of BCBCurrenciesCodesQuotesBulletins.
		mock_html_element : HtmlElement
			Mocked HtmlElement.
		sample_html_currencies : tuple[list[str], list[str]]
			Sample HTML content for currencies.
		mocker : MockerFixture
			Pytest mocker for creating mock objects.

		Returns
		-------
		None
		"""
		mocker.patch.object(
			bcb_currencies_codes_quotes.cls_html_handler,
			"lxml_xpath",
			side_effect=sample_html_currencies,
		)
		result = bcb_currencies_codes_quotes.transform_data(mock_html_element)
		assert isinstance(result, pd.DataFrame)
		assert list(result.columns) == ["NOME_MOEDA", "CODIGO_MOEDA"]
		assert len(result) == 2
		assert result.iloc[0]["CODIGO_MOEDA"] == "USD"
