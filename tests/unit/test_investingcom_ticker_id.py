"""Unit tests for InvestingComTickerId ingestion class."""

from datetime import date
import importlib
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests as req
from requests import Response, Session

from stpstone.ingestion.countries.ww.exchange.markets.investingcom_ticker_id import (
	InvestingComTickerId,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------


@pytest.fixture
def mock_logger() -> Logger:
	"""Fixture providing a mock logger instance.

	Returns
	-------
	Logger
		Mocked logger object for testing.
	"""
	return MagicMock(spec=Logger)


@pytest.fixture
def mock_db_session() -> Session:
	"""Fixture providing a mock database session.

	Returns
	-------
	Session
		Mocked database session object.
	"""
	return MagicMock(spec=Session)


@pytest.fixture
def sample_ticker_data() -> list:
	"""Fixture providing sample ticker ID data for testing.

	Returns
	-------
	list
		List of sample ticker symbol records.
	"""
	return [
		{
			"name": "Petrobras",
			"exchange_traded": "BVMF",
			"exchange_listed": "BVMF",
			"timezone": "America/Sao_Paulo",
			"pricescale": 100,
			"pointvalue": 1,
			"ticker": "PETR4",
			"type": "stock",
		}
	]


@pytest.fixture
def mock_response_success(sample_ticker_data: list) -> Response:
	"""Fixture providing a successful HTTP response mock.

	Parameters
	----------
	sample_ticker_data : list
		Sample ticker data list.

	Returns
	-------
	Response
		Mocked Response object with successful status and JSON data.
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.status_code = 200
	mock_resp.raise_for_status = MagicMock()
	mock_resp.json.return_value = sample_ticker_data
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
	mock_cls = mocker.patch(
		"stpstone.ingestion.countries.ww.exchange.markets"
		".investingcom_ticker_id.DatesCurrent"
	)
	mock_instance = MagicMock(spec=DatesCurrent)
	mock_cls.return_value = mock_instance
	mock_instance.curr_date.return_value = date(2025, 4, 15)
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
	mock_cls = mocker.patch(
		"stpstone.ingestion.countries.ww.exchange.markets"
		".investingcom_ticker_id.DatesBRAnbima"
	)
	mock_instance = MagicMock(spec=DatesBRAnbima)
	mock_cls.return_value = mock_instance
	mock_instance.add_working_days.return_value = date(2025, 4, 14)
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
	mock_cls = mocker.patch(
		"stpstone.ingestion.countries.ww.exchange.markets"
		".investingcom_ticker_id.DirFilesManagement"
	)
	mock_instance = MagicMock(spec=DirFilesManagement)
	mock_cls.return_value = mock_instance
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
	mock_cls = mocker.patch(
		"stpstone.ingestion.countries.ww.exchange.markets"
		".investingcom_ticker_id.CreateLog"
	)
	mock_instance = MagicMock(spec=CreateLog)
	mock_cls.return_value = mock_instance
	return mock_instance


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> MagicMock:
	"""Fixture mocking requests.get at the module level to prevent real HTTP calls.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker fixture.

	Returns
	-------
	MagicMock
		Mocked requests.get function.
	"""
	return mocker.patch(
		"stpstone.ingestion.countries.ww.exchange.markets.investingcom_ticker_id.requests.get"
	)


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


# --------------------------
# Tests
# --------------------------


class TestInvestingComTickerId:
	"""Test cases for InvestingComTickerId class."""

	def test_init_with_valid_inputs(
		self,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test initialization with valid parameters sets attributes correctly.

		Parameters
		----------
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
		test_date = date(2025, 4, 10)
		mock_logger = MagicMock(spec=Logger)
		mock_session = MagicMock(spec=Session)

		instance = InvestingComTickerId(
			str_ticker="PETR4",
			date_ref=test_date,
			logger=mock_logger,
			cls_db=mock_session,
		)

		assert instance.str_ticker == "PETR4"
		assert instance.date_ref == test_date
		assert instance.logger == mock_logger
		assert instance.cls_db == mock_session
		assert "symbols?symbol=PETR4" in instance.url
		assert hasattr(instance, "cls_dir_files_management")
		assert hasattr(instance, "cls_dates_current")
		assert hasattr(instance, "cls_create_log")
		assert hasattr(instance, "cls_dates_br")

	def test_init_default_date_uses_previous_working_day(
		self,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test that omitting date_ref defaults to the previous working day.

		Parameters
		----------
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
		instance = InvestingComTickerId()

		assert instance.date_ref == date(2025, 4, 14)
		mock_dates_br.add_working_days.assert_called_once_with(date(2025, 4, 15), -1)

	@pytest.mark.parametrize("invalid_date", ["2025-09-15", 123, 3.14])
	def test_init_invalid_date_ref_type(
		self,
		invalid_date: object,
	) -> None:
		"""Test initialization with invalid date_ref types raises TypeError.

		Parameters
		----------
		invalid_date : object
			Invalid date_ref input to test.

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be one of types|must be of type"):
			InvestingComTickerId(date_ref=invalid_date)

	def test_get_response_success(
		self,
		mock_requests_get: MagicMock,
		mock_response_success: Response,
		mock_backoff: MagicMock,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test get_response returns Response on successful HTTP call.

		Parameters
		----------
		mock_requests_get : MagicMock
			Mocked requests.get function.
		mock_response_success : Response
			Mocked successful HTTP response.
		mock_backoff : MagicMock
			Mocked backoff decorator.
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

		instance = InvestingComTickerId()
		result = instance.get_response(timeout=10.0, bool_verify=False)

		assert result is mock_response_success
		mock_response_success.raise_for_status.assert_called_once()

	def test_get_response_raises_on_http_error(
		self,
		mock_requests_get: MagicMock,
		mock_backoff: MagicMock,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test get_response propagates HTTPError from raise_for_status.

		Parameters
		----------
		mock_requests_get : MagicMock
			Mocked requests.get function.
		mock_backoff : MagicMock
			Mocked backoff decorator.
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
		mock_resp = MagicMock(spec=Response)
		mock_resp.raise_for_status.side_effect = req.exceptions.HTTPError("404 Not Found")
		mock_requests_get.return_value = mock_resp

		instance = InvestingComTickerId()

		with pytest.raises(req.exceptions.HTTPError):
			instance.get_response()

	@pytest.mark.parametrize("timeout", [None, 10, 10.0, (5.0, 15.0), (5, 15)])
	def test_get_response_valid_timeout(
		self,
		mock_requests_get: MagicMock,
		mock_response_success: Response,
		mock_backoff: MagicMock,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
		timeout: object,
	) -> None:
		"""Test get_response accepts various valid timeout inputs.

		Parameters
		----------
		mock_requests_get : MagicMock
			Mocked requests.get function.
		mock_response_success : Response
			Mocked successful HTTP response.
		mock_backoff : MagicMock
			Mocked backoff decorator.
		mock_dates_current : MagicMock
			Mocked DatesCurrent instance.
		mock_dates_br : MagicMock
			Mocked DatesBRAnbima instance.
		mock_dir_files : MagicMock
			Mocked DirFilesManagement instance.
		mock_create_log : MagicMock
			Mocked CreateLog instance.
		timeout : object
			Valid timeout value to test.

		Returns
		-------
		None
		"""
		mock_requests_get.return_value = mock_response_success

		instance = InvestingComTickerId()
		result = instance.get_response(timeout=timeout)

		assert result is mock_response_success

	@pytest.mark.parametrize("invalid_timeout", ["10", [10, 20], {10, 20}])
	def test_get_response_invalid_timeout(
		self,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
		invalid_timeout: object,
	) -> None:
		"""Test get_response with invalid timeout types raises TypeError.

		Parameters
		----------
		mock_dates_current : MagicMock
			Mocked DatesCurrent instance.
		mock_dates_br : MagicMock
			Mocked DatesBRAnbima instance.
		mock_dir_files : MagicMock
			Mocked DirFilesManagement instance.
		mock_create_log : MagicMock
			Mocked CreateLog instance.
		invalid_timeout : object
			Invalid timeout input to test.

		Returns
		-------
		None
		"""
		instance = InvestingComTickerId()

		with pytest.raises(TypeError, match="must be one of types|must be of type"):
			instance.get_response(timeout=invalid_timeout)

	def test_parse_raw_file_returns_response_unchanged(
		self,
		mock_response_success: Response,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test parse_raw_file passes the response through unchanged.

		Parameters
		----------
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
		instance = InvestingComTickerId()
		result = instance.parse_raw_file(mock_response_success)

		assert result is mock_response_success

	def test_transform_data_returns_dataframe(
		self,
		mock_response_success: Response,
		sample_ticker_data: list,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test transform_data builds a DataFrame from a JSON response.

		Parameters
		----------
		mock_response_success : Response
			Mocked successful HTTP response.
		sample_ticker_data : list
			Sample ticker data.
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
		instance = InvestingComTickerId()
		result = instance.transform_data(resp_req=mock_response_success)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
		mock_response_success.json.assert_called_once()

	def test_transform_data_empty_response(
		self,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test transform_data returns empty DataFrame for empty JSON array.

		Parameters
		----------
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
		mock_resp = MagicMock(spec=Response)
		mock_resp.json.return_value = []

		instance = InvestingComTickerId()
		result = instance.transform_data(resp_req=mock_resp)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0

	def test_run_without_cls_db_returns_dataframe(
		self,
		mock_requests_get: MagicMock,
		mock_response_success: Response,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
		mocker: MockerFixture,
	) -> None:
		"""Test run returns a DataFrame when no database session is provided.

		Parameters
		----------
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
		mocker : MockerFixture
			Pytest mocker fixture.

		Returns
		-------
		None
		"""
		mock_requests_get.return_value = mock_response_success

		standardized_df = pd.DataFrame({"TICKER": ["PETR4"], "NAME": ["Petrobras"]})

		instance = InvestingComTickerId()
		mocker.patch.object(instance, "standardize_dataframe", return_value=standardized_df)

		result = instance.run()

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1

	def test_run_with_cls_db_calls_insert(
		self,
		mock_requests_get: MagicMock,
		mock_response_success: Response,
		mock_db_session: Session,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
		mocker: MockerFixture,
	) -> None:
		"""Test run inserts data and returns None when a database session is provided.

		Parameters
		----------
		mock_requests_get : MagicMock
			Mocked requests.get function.
		mock_response_success : Response
			Mocked successful HTTP response.
		mock_db_session : Session
			Mocked database session.
		mock_dates_current : MagicMock
			Mocked DatesCurrent instance.
		mock_dates_br : MagicMock
			Mocked DatesBRAnbima instance.
		mock_dir_files : MagicMock
			Mocked DirFilesManagement instance.
		mock_create_log : MagicMock
			Mocked CreateLog instance.
		mocker : MockerFixture
			Pytest mocker fixture.

		Returns
		-------
		None
		"""
		mock_requests_get.return_value = mock_response_success

		standardized_df = pd.DataFrame({"TICKER": ["PETR4"]})

		instance = InvestingComTickerId(cls_db=mock_db_session)
		mocker.patch.object(instance, "standardize_dataframe", return_value=standardized_df)
		mock_insert = mocker.patch.object(instance, "insert_table_db", return_value=None)

		result = instance.run()

		assert result is None
		mock_insert.assert_called_once()

	@pytest.mark.parametrize("invalid_table_name", [None, 123])
	def test_run_invalid_table_name(
		self,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
		invalid_table_name: object,
	) -> None:
		"""Test run with invalid table name raises TypeError.

		Parameters
		----------
		mock_dates_current : MagicMock
			Mocked DatesCurrent instance.
		mock_dates_br : MagicMock
			Mocked DatesBRAnbima instance.
		mock_dir_files : MagicMock
			Mocked DirFilesManagement instance.
		mock_create_log : MagicMock
			Mocked CreateLog instance.
		invalid_table_name : object
			Invalid table name input to test.

		Returns
		-------
		None
		"""
		instance = InvestingComTickerId()

		with pytest.raises(TypeError, match="must be one of types|must be of type"):
			instance.run(str_table_name=invalid_table_name)

	def test_reload_module(
		self,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test module reload preserves functionality.

		Parameters
		----------
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
		import stpstone.ingestion.countries.ww.exchange.markets.investingcom_ticker_id as module

		importlib.reload(module)

		instance = module.InvestingComTickerId()
		assert isinstance(instance, module.InvestingComTickerId)
		assert "symbols?symbol=" in instance.url
