"""Unit tests for InvestingComDailyOhlcv ingestion class."""

from datetime import date, datetime
import importlib
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests as req
from requests import Response, Session

from stpstone.ingestion.countries.ww.exchange.markets.investingcom_daily_ohlcv import (
	InvestingComDailyOhlcv,
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
def sample_ohlcv_data() -> list:
	"""Fixture providing sample OHLCV data for testing.

	Returns
	-------
	list
		List of sample OHLCV records.
	"""
	return [
		{
			"time": 1717200000,
			"open": 37.5,
			"high": 38.2,
			"low": 37.0,
			"close": 37.9,
			"volume": 12345678,
		}
	]


@pytest.fixture
def mock_response_success(sample_ohlcv_data: list) -> Response:
	"""Fixture providing a successful HTTP response mock.

	Parameters
	----------
	sample_ohlcv_data : list
		Sample OHLCV data list.

	Returns
	-------
	Response
		Mocked Response object with successful status and JSON data.
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.status_code = 200
	mock_resp.raise_for_status = MagicMock()
	mock_resp.json.return_value = sample_ohlcv_data
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
		"stpstone.ingestion.countries.ww.exchange.markets.investingcom_daily_ohlcv.DatesCurrent"
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
		"stpstone.ingestion.countries.ww.exchange.markets.investingcom_daily_ohlcv.DatesBRAnbima"
	)
	mock_instance = MagicMock(spec=DatesBRAnbima)
	mock_cls.return_value = mock_instance
	mock_instance.add_working_days.side_effect = lambda d, n: (
		date(2025, 4, 14) if n == -1 else date(2025, 4, 7)
	)
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
		".investingcom_daily_ohlcv.DirFilesManagement"
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
		"stpstone.ingestion.countries.ww.exchange.markets.investingcom_daily_ohlcv.CreateLog"
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
		"stpstone.ingestion.countries.ww.exchange.markets.investingcom_daily_ohlcv.requests.get"
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


class TestInvestingComDailyOhlcv:
	"""Test cases for InvestingComDailyOhlcv class."""

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
		test_start = date(2025, 4, 3)
		test_end = date(2025, 4, 10)
		mock_logger_instance = MagicMock(spec=Logger)
		mock_session = MagicMock(spec=Session)

		instance = InvestingComDailyOhlcv(
			ticker_id=6408,
			str_ticker="PETR4",
			date_ref=test_date,
			date_start=test_start,
			date_end=test_end,
			logger=mock_logger_instance,
			cls_db=mock_session,
		)

		assert instance.ticker_id == 6408
		assert instance.str_ticker == "PETR4"
		assert instance.date_ref == test_date
		assert instance.date_start == test_start
		assert instance.date_end == test_end
		assert instance.logger == mock_logger_instance
		assert instance.cls_db == mock_session
		assert "history?symbol=6408" in instance.url
		assert hasattr(instance, "date_start_unix_ts")
		assert hasattr(instance, "date_end_unix_ts")

	def test_init_default_dates_use_previous_working_day(
		self,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test that omitting date_ref and date ranges defaults to working-day calculations.

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
		instance = InvestingComDailyOhlcv(ticker_id=6408)

		assert instance.date_ref == date(2025, 4, 14)
		assert instance.date_end == date(2025, 4, 14)

	def test_init_unix_timestamps_computed_correctly(
		self,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test that Unix timestamps for date_start and date_end are computed correctly.

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
		test_start = date(2025, 1, 1)
		test_end = date(2025, 1, 10)
		expected_start_ts = int(datetime.combine(test_start, datetime.min.time()).timestamp())
		expected_end_ts = int(datetime.combine(test_end, datetime.min.time()).timestamp())

		instance = InvestingComDailyOhlcv(
			ticker_id=6408,
			date_ref=date(2025, 1, 10),
			date_start=test_start,
			date_end=test_end,
		)

		assert instance.date_start_unix_ts == expected_start_ts
		assert instance.date_end_unix_ts == expected_end_ts

	def test_init_ticker_id_required(self) -> None:
		"""Test that omitting ticker_id raises TypeError.

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			InvestingComDailyOhlcv()  # type: ignore[call-arg]

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
			InvestingComDailyOhlcv(ticker_id=6408, date_ref=invalid_date)

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

		instance = InvestingComDailyOhlcv(ticker_id=6408)
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

		instance = InvestingComDailyOhlcv(ticker_id=6408)

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

		instance = InvestingComDailyOhlcv(ticker_id=6408)
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
		instance = InvestingComDailyOhlcv(ticker_id=6408)

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
		instance = InvestingComDailyOhlcv(ticker_id=6408)
		result = instance.parse_raw_file(mock_response_success)

		assert result is mock_response_success

	def test_transform_data_returns_dataframe(
		self,
		mock_response_success: Response,
		sample_ohlcv_data: list,
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
		sample_ohlcv_data : list
			Sample OHLCV data.
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
		instance = InvestingComDailyOhlcv(ticker_id=6408)
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

		instance = InvestingComDailyOhlcv(ticker_id=6408)
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

		standardized_df = pd.DataFrame({"CLOSE": [37.9], "VOLUME": [12345678]})

		instance = InvestingComDailyOhlcv(ticker_id=6408)
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

		standardized_df = pd.DataFrame({"CLOSE": [37.9]})

		instance = InvestingComDailyOhlcv(ticker_id=6408, cls_db=mock_db_session)
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
		instance = InvestingComDailyOhlcv(ticker_id=6408)

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
		import stpstone.ingestion.countries.ww.exchange.markets.investingcom_daily_ohlcv as module

		importlib.reload(module)

		instance = module.InvestingComDailyOhlcv(ticker_id=6408)
		assert isinstance(instance, module.InvestingComDailyOhlcv)
		assert "history?symbol=6408" in instance.url
