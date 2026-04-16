"""Unit tests for BCBOlindaCurrenciesTS ingestion class."""

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_annual_market_expectations import (
	BCBOlindaAnnualMarketExpectations,
)
from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies import (
	BCBOlindaCurrencies,
)
from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies_ts import (
	BCBOlindaCurrenciesTS,
)
from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_ptax_usd_brl import (
	BCBOlindaPTAXUSDBRL,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------


@pytest.fixture
def mock_response_success() -> Response:
	"""Fixture providing a successful HTTP response mock for currencies.

	Returns
	-------
	Response
		Mocked Response object with successful status and JSON data.
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.status_code = 200
	mock_resp.raise_for_status = MagicMock()
	mock_resp.json.return_value = {
		"value": [
			{
				"simbolo": "USD",
				"nomeFormatado": "Dólar americano",
				"tipoMoeda": "Real",
			},
			{
				"simbolo": "EUR",
				"nomeFormatado": "Euro",
				"tipoMoeda": "Real",
			},
		]
	}
	return mock_resp


@pytest.fixture
def mock_response_currencies_ts() -> Response:
	"""Fixture providing successful currencies time series response mock.

	Returns
	-------
	Response
		Mocked Response object with currencies TS data.
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.status_code = 200
	mock_resp.raise_for_status = MagicMock()
	mock_resp.json.return_value = {
		"value": [
			{
				"paridadeCompra": 1.0,
				"paridadeVenda": 1.0,
				"cotacaoCompra": 5.12,
				"cotacaoVenda": 5.15,
				"dataHoraCotacao": "2025-09-15T13:00:00",
				"tipoBoletim": "Fechamento",
			}
		]
	}
	return mock_resp


@pytest.fixture
def mock_dates_current(mocker: MockerFixture) -> MagicMock:
	"""Fixture mocking the DatesCurrent class.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker fixture.

	Returns
	-------
	MagicMock
		Mocked DatesCurrent instance.
	"""
	mock_dates = mocker.patch(
		"stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies_ts.DatesCurrent"
	)
	mock_instance = MagicMock(spec=DatesCurrent)
	mock_dates.return_value = mock_instance
	mock_instance.curr_date.return_value = date(2025, 9, 16)
	return mock_instance


@pytest.fixture
def mock_dates_br(mocker: MockerFixture, mock_dates_current: MagicMock) -> MagicMock:
	"""Fixture mocking the DatesBRAnbima class.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker fixture.
	mock_dates_current : MagicMock
		Mocked DatesCurrent instance.

	Returns
	-------
	MagicMock
		Mocked DatesBRAnbima instance.
	"""
	mock_br = mocker.patch(
		"stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies_ts.DatesBRAnbima"
	)
	mock_instance = MagicMock(spec=DatesBRAnbima)
	mock_br.return_value = mock_instance
	mock_instance.add_working_days.return_value = date(2025, 9, 15)
	return mock_instance


@pytest.fixture
def mock_dir_files(mocker: MockerFixture) -> MagicMock:
	"""Fixture mocking the DirFilesManagement class.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker fixture.

	Returns
	-------
	MagicMock
		Mocked DirFilesManagement instance.
	"""
	mock_files = mocker.patch(
		"stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies_ts.DirFilesManagement"
	)
	mock_instance = MagicMock(spec=DirFilesManagement)
	mock_files.return_value = mock_instance
	return mock_instance


@pytest.fixture
def mock_create_log(mocker: MockerFixture) -> MagicMock:
	"""Fixture mocking the CreateLog class.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker fixture.

	Returns
	-------
	MagicMock
		Mocked CreateLog instance.
	"""
	mock_log = mocker.patch(
		"stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies_ts.CreateLog"
	)
	mock_instance = MagicMock(spec=CreateLog)
	mock_log.return_value = mock_instance
	return mock_instance


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> MagicMock:
	"""Fixture mocking requests.get to prevent real HTTP calls.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker fixture.

	Returns
	-------
	MagicMock
		Mocked requests.get function.
	"""
	return mocker.patch("requests.get")


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> MagicMock:
	"""Fixture mocking backoff decorator to bypass retries.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker fixture.

	Returns
	-------
	MagicMock
		Mocked backoff.on_exception decorator.
	"""
	return mocker.patch("backoff.on_exception", return_value=lambda func: func)


@pytest.fixture
def mock_insert_table_db(mocker: MockerFixture) -> MagicMock:
	"""Fixture mocking the insert_table_db method.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker fixture.

	Returns
	-------
	MagicMock
		Mocked insert_table_db method.
	"""
	return mocker.patch(
		"stpstone.ingestion.abc.ingestion_abc.ABCIngestionOperations.insert_table_db"
	)


@pytest.fixture
def sample_date_range() -> tuple[date, date]:
	"""Fixture providing sample date range for testing.

	Returns
	-------
	tuple[date, date]
		Sample start and end dates (2025-08-17 to 2025-09-15).
	"""
	return date(2025, 8, 17), date(2025, 9, 15)


# --------------------------
# Tests
# --------------------------


class TestBCBOlindaCurrenciesTS:
	"""Test cases for BCBOlindaCurrenciesTS class."""

	def test_init_with_valid_date_range(
		self,
		sample_date_range: tuple[date, date],
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test initialization with valid date range for time series.

		Parameters
		----------
		sample_date_range : tuple[date, date]
			Sample date range for testing.
		mock_dates_current : MagicMock
			Mocked DatesCurrent instance.
		mock_dates_br : MagicMock
			Mocked DatesBRAnbima instance.
		mock_dir_files : MagicMock
			Mocked DirFilesManagement instance.
		mock_create_log : MagicMock
			Mocked CreateLog instance.

		Returns
		-------
		None
		"""
		date_start, date_end = sample_date_range

		instance = BCBOlindaCurrenciesTS(
			date_start=date_start,
			date_end=date_end,
		)

		assert instance.date_start == date_start
		assert instance.date_end == date_end

		assert "CotacaoMoedaPeriodo" in instance.fstr_url
		assert "@moeda='{}'" in instance.fstr_url
		assert "@dataInicial='{}'" in instance.fstr_url
		assert "@dataFinalCotacao='{}'" in instance.fstr_url

	def test__currencies_method_retrieves_list(
		self,
		mocker: MockerFixture,
		mock_requests_get: MagicMock,
		mock_response_success: Response,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test _currencies method retrieves currency symbols.

		Parameters
		----------
		mocker : MockerFixture
			Pytest mocker fixture.
		mock_requests_get : MagicMock
			Mocked requests.get function.
		mock_response_success : Response
			Mocked successful HTTP response.
		mock_dates_current : MagicMock
			Mocked DatesCurrent instance.
		mock_dates_br : MagicMock
			Mocked DatesBRAnbima instance.
		mock_dir_files : MagicMock
			Mocked DirFilesManagement instance.
		mock_create_log : MagicMock
			Mocked CreateLog instance.

		Returns
		-------
		None
		"""
		mock_requests_get.return_value = mock_response_success

		standardized_df = pd.DataFrame(
			{
				"SIMBOLO": ["USD", "EUR", "GBP"],
				"NOME_FORMATADO": ["Dólar", "Euro", "Libra"],
				"TIPO_MOEDA": ["Real", "Real", "Real"],
			}
		)

		mock_standardize = mocker.patch(
			"stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies."
			"BCBOlindaCurrencies.standardize_dataframe",
			return_value=standardized_df,
		)

		instance = BCBOlindaCurrenciesTS(date_end=date(2025, 9, 15))
		currencies = instance._currencies()

		assert isinstance(currencies, list)
		assert currencies == ["USD", "EUR", "GBP"]
		assert len(currencies) == 3

		mock_requests_get.assert_called_once()
		mock_standardize.assert_called_once()


# --------------------------
# Cross-Class Integration Tests
# --------------------------


def test_class_inheritance_structure() -> None:
	"""Test proper inheritance from ABCIngestionOperations.

	Returns
	-------
	None
	"""
	classes_to_test = [
		BCBOlindaCurrencies,
		BCBOlindaPTAXUSDBRL,
		BCBOlindaCurrenciesTS,
		BCBOlindaAnnualMarketExpectations,
	]

	for cls in classes_to_test:
		instance = cls()
		assert isinstance(instance, ABCIngestionOperations)
		assert hasattr(instance, "insert_table_db")
		assert hasattr(instance, "standardize_dataframe")
		assert hasattr(instance, "parse_raw_file")


def test_url_format_consistency() -> None:
	"""Test URL format consistency across all classes.

	Returns
	-------
	None
	"""
	instances = [
		BCBOlindaCurrencies(),
		BCBOlindaPTAXUSDBRL(),
		BCBOlindaCurrenciesTS(),
		BCBOlindaAnnualMarketExpectations(),
	]

	common_patterns = [
		"olinda.bcb.gov.br",
		"/olinda/servico/",
		"/versao/v1/odata/",
		"$format=json",
	]

	for instance in instances:
		url = getattr(instance, "url", None) or getattr(instance, "fstr_url", None)
		assert url is not None
		for pattern in common_patterns:
			assert pattern in url
		assert url.startswith("https://")
