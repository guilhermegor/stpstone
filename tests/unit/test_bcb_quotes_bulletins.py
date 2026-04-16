"""Unit tests for BCBQuotesBulletins ingestion module.

Tests the ingestion functionality for BCB quotes bulletins with various scenarios:
- Initialization with valid and invalid inputs
- Response handling and parsing
- Data transformation and standardization
- Database insertion and fallback logic
"""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins import (
	BCBQuotesBulletins,
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
def sample_csv_content() -> StringIO:
	"""Provide sample CSV content for parsing.

	Returns
	-------
	StringIO
		A StringIO object with sample CSV content.
	"""
	return StringIO(
		"""CODIGO;NOME;SIMBOLO;CODIGO_PAIS;PAIS;TIPO;DATA_EXCLUSAO_PTAX
USD;Dolar Americano;US$;840;EUA;A;"""
	)


# --------------------------
# Tests for BCBQuotesBulletins
# --------------------------
class TestBCBQuotesBulletins:
	"""Test cases for BCBQuotesBulletins class."""

	@pytest.fixture
	def bcb_quotes_bulletins(
		self,
		mocker: MockerFixture,
		mock_dates_current: DatesCurrent,
		mock_dates_br: DatesBRAnbima,
	) -> BCBQuotesBulletins:
		"""Fixture for BCBQuotesBulletins instance.

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
		BCBQuotesBulletins
			Mocked BCBQuotesBulletins.
		"""
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_quotes_bulletins.DirFilesManagement"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins.CreateLog"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins.ListHandler"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins.DatesCurrent",
			return_value=mock_dates_current,
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins.DatesBRAnbima",
			return_value=mock_dates_br,
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_quotes_bulletins.BCBCurrenciesCodesPTAX"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_quotes_bulletins.BCBCurrenciesCodesQuotesBulletins"
		)
		mocker.patch("requests.get")
		mocker.patch.object(BCBQuotesBulletins, "get_file")
		mocker.patch.object(BCBQuotesBulletins, "insert_table_db")
		mocker.patch.object(BCBQuotesBulletins, "standardize_dataframe")
		return BCBQuotesBulletins()

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
			".bcb_quotes_bulletins.DirFilesManagement"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins.CreateLog"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins.ListHandler"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins.DatesCurrent",
			return_value=mock_dates_current,
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins.DatesBRAnbima",
			return_value=mock_dates_br,
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_quotes_bulletins.BCBCurrenciesCodesPTAX"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_quotes_bulletins.BCBCurrenciesCodesQuotesBulletins"
		)

		instance = BCBQuotesBulletins()
		assert instance.date_start == sample_date
		assert instance.date_end == sample_date
		assert isinstance(instance.logger, type(None))
		assert isinstance(instance.cls_db, type(None))
		assert mock_dates_br.add_working_days.call_count == 2

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
			".bcb_quotes_bulletins.DirFilesManagement"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins.CreateLog"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins.ListHandler"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins.DatesCurrent"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins.DatesBRAnbima"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_quotes_bulletins.BCBCurrenciesCodesPTAX"
		)
		mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics"
			".bcb_quotes_bulletins.BCBCurrenciesCodesQuotesBulletins"
		)

		logger = MagicMock(spec=Logger)
		instance = BCBQuotesBulletins(
			date_start=sample_date,
			date_end=sample_date,
			logger=logger,
			cls_db=mock_session,
		)
		assert instance.date_start == sample_date
		assert instance.date_end == sample_date
		assert instance.logger == logger
		assert instance.cls_db == mock_session

	def test_get_currencies_codes(
		self,
		bcb_quotes_bulletins: BCBQuotesBulletins,
		mocker: MockerFixture,
	) -> None:
		"""Test retrieval of currency codes.

		Parameters
		----------
		bcb_quotes_bulletins : BCBQuotesBulletins
			Instance of BCBQuotesBulletins.
		mocker : MockerFixture
			Pytest mocker for creating mock objects.

		Returns
		-------
		None
		"""
		mock_df_ptax = pd.DataFrame({"CODIGO": ["USD", "EUR"]})
		mock_df_qb = pd.DataFrame({"CODIGO_MOEDA": ["EUR", "GBP"]})
		mocker.patch.object(
			bcb_quotes_bulletins.cls_bcb_currencies_codes_ptax,
			"run",
			return_value=mock_df_ptax,
		)
		mocker.patch.object(
			bcb_quotes_bulletins.cls_bcb_currencies_codes_quotes_bulletins,
			"run",
			return_value=mock_df_qb,
		)
		mocker.patch.object(
			bcb_quotes_bulletins.cls_list_handler,
			"extend_lists",
			return_value=["USD", "EUR", "GBP"],
		)
		result = bcb_quotes_bulletins._get_currencies_codes(timeout=1000)
		assert result == ["USD", "EUR", "GBP"]

	def test_run_with_value_error(
		self,
		bcb_quotes_bulletins: BCBQuotesBulletins,
		mock_response: Response,
		sample_csv_content: StringIO,
		mocker: MockerFixture,
	) -> None:
		"""Test run method handling ValueError in standardization.

		Parameters
		----------
		bcb_quotes_bulletins : BCBQuotesBulletins
			Instance of BCBQuotesBulletins.
		mock_response : Response
			Mocked HTTP response.
		sample_csv_content : StringIO
			Sample CSV content.
		mocker : MockerFixture
			Pytest mocker for creating mock objects.

		Returns
		-------
		None
		"""
		mocker.patch.object(bcb_quotes_bulletins, "get_response", return_value=mock_response)
		mocker.patch.object(
			bcb_quotes_bulletins, "parse_raw_file", return_value=sample_csv_content
		)
		mocker.patch.object(
			bcb_quotes_bulletins,
			"transform_data",
			return_value=pd.read_csv(
				sample_csv_content,
				sep=";",
				names=[
					"DATA",
					"CODIGO_MOEDA",
					"TIPO_MOEDA",
					"SIMBOLO_MOEDA",
					"TAXA_COMPRA",
					"TAXA_VENDA",
					"PARIDADE_COMPRA",
					"PARIDADE_VENDA",
				],
				header=None,
			),
		)
		mocker.patch.object(bcb_quotes_bulletins, "_get_currencies_codes", return_value=["USD"])
		mocker.patch.object(
			bcb_quotes_bulletins,
			"standardize_dataframe",
			side_effect=ValueError("Invalid format"),
		)

		result = bcb_quotes_bulletins.run(timeout=1000)
		assert isinstance(result, pd.DataFrame)
		assert result.empty
