"""Unit tests for FMPStocksOhlcvYesterday ingestion class."""

from datetime import date
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.ww.exchange.markets.fmp_stocks_ohlcv_yesterday import (
	FMPStocksOhlcvYesterday,
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
def sample_stock_data() -> list:
	"""Fixture providing sample batch stock quote records.

	Returns
	-------
	list
		List of sample stock quote dictionaries.
	"""
	return [
		{
			"symbol": "AAPL",
			"name": "Apple Inc.",
			"price": 170.0,
			"changePercentage": 0.5,
			"change": 0.85,
			"volume": 55000000.0,
			"dayLow": 168.0,
			"dayHigh": 171.0,
			"yearHigh": 199.0,
			"yearLow": 124.0,
			"marketCap": 2700000000000.0,
			"priceAvg50": 165.0,
			"priceAvg200": 158.0,
			"exchange": "NASDAQ",
			"open": 169.0,
			"previousClose": 169.15,
			"timestamp": 1713139200,
		},
		{
			"symbol": "MSFT",
			"name": "Microsoft Corporation",
			"price": 420.0,
			"changePercentage": -0.3,
			"change": -1.26,
			"volume": 20000000.0,
			"dayLow": 418.0,
			"dayHigh": 422.0,
			"yearHigh": 430.0,
			"yearLow": 310.0,
			"marketCap": 3100000000000.0,
			"priceAvg50": 415.0,
			"priceAvg200": 390.0,
			"exchange": "NASDAQ",
			"open": 421.0,
			"previousClose": 421.26,
			"timestamp": 1713139200,
		},
	]


@pytest.fixture
def mock_response_success(sample_stock_data: list) -> Response:
	"""Fixture providing a successful HTTP response mock.

	Parameters
	----------
	sample_stock_data : list
		Sample stock data list.

	Returns
	-------
	Response
		Mocked Response object with successful status and JSON data.
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.status_code = 200
	mock_resp.raise_for_status = MagicMock()
	mock_resp.json.return_value = sample_stock_data
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
		".fmp_stocks_ohlcv_yesterday.DatesCurrent"
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
		".fmp_stocks_ohlcv_yesterday.DatesBRAnbima"
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
		".fmp_stocks_ohlcv_yesterday.DirFilesManagement"
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
		".fmp_stocks_ohlcv_yesterday.CreateLog"
	)
	mock_instance = MagicMock(spec=CreateLog)
	mock_cls.return_value = mock_instance
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


# --------------------------
# Tests
# --------------------------


class TestFMPStocksOhlcvYesterday:
	"""Test cases for FMPStocksOhlcvYesterday class."""

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
		slugs = ["AAPL", "MSFT"]
		test_date = date(2025, 4, 10)

		instance = FMPStocksOhlcvYesterday(
			token="tok",  # noqa: S106
			list_slugs=slugs,
			date_ref=test_date,
		)

		assert instance.token == "tok"  # noqa: S105
		assert instance.list_slugs == slugs
		assert instance.date_ref == test_date

	def test_init_chunk_size_constant(
		self,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
	) -> None:
		"""Test that _CHUNK_SIZE class constant is 10.

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
		assert FMPStocksOhlcvYesterday._CHUNK_SIZE == 10

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

		instance = FMPStocksOhlcvYesterday(token="tok", list_slugs=["AAPL"])  # noqa: S106
		instance.url = (
			"https://financialmodelingprep.com/stable/batch-quote?symbols=AAPL&apikey=tok"
		)
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
		import requests as req

		mock_resp = MagicMock(spec=Response)
		mock_resp.raise_for_status.side_effect = req.exceptions.HTTPError("403 Forbidden")
		mock_requests_get.return_value = mock_resp

		instance = FMPStocksOhlcvYesterday(token="tok", list_slugs=["AAPL"])  # noqa: S106

		with pytest.raises(req.exceptions.HTTPError):
			instance.get_response()

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
		instance = FMPStocksOhlcvYesterday(token="tok", list_slugs=["AAPL"])  # noqa: S106
		result = instance.parse_raw_file(mock_response_success)

		assert result is mock_response_success

	def test_transform_data_returns_dataframe(
		self,
		mock_response_success: Response,
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
		instance = FMPStocksOhlcvYesterday(token="tok", list_slugs=["AAPL", "MSFT"])  # noqa: S106
		result = instance.transform_data(resp_req=mock_response_success)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 2
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

		instance = FMPStocksOhlcvYesterday(token="tok", list_slugs=["AAPL"])  # noqa: S106
		result = instance.transform_data(resp_req=mock_resp)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0

	def test_run_chunks_slugs_correctly(
		self,
		mock_requests_get: MagicMock,
		mock_response_success: Response,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_dir_files: MagicMock,
		mock_create_log: MagicMock,
		mocker: MockerFixture,
	) -> None:
		"""Test run issues one request per chunk of 10 symbols.

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
		standardized_df = pd.DataFrame({"SYMBOL": ["AAPL", "MSFT"]})

		slugs = [f"SYM{i}" for i in range(15)]
		instance = FMPStocksOhlcvYesterday(token="tok", list_slugs=slugs)  # noqa: S106
		mocker.patch.object(instance, "standardize_dataframe", return_value=standardized_df)

		result = instance.run()

		assert mock_requests_get.call_count == 2
		assert isinstance(result, pd.DataFrame)

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
		standardized_df = pd.DataFrame({"SYMBOL": ["AAPL"]})

		instance = FMPStocksOhlcvYesterday(
			token="tok", list_slugs=["AAPL"], cls_db=mock_db_session  # noqa: S106
		)
		mocker.patch.object(instance, "standardize_dataframe", return_value=standardized_df)
		mock_insert = mocker.patch.object(instance, "insert_table_db", return_value=None)

		result = instance.run()

		assert result is None
		mock_insert.assert_called_once()
