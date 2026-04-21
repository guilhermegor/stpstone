"""Unit tests for MT5 MetaTrader5 interface.

Tests the functionality for interacting with MetaTrader5 trading platform including:
- Connection initialization and shutdown
- Symbol information retrieval
- Tick data fetching
- Market depth analysis
- Error handling and validation
"""

from datetime import datetime
from logging import Logger
import platform
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from pytest_mock import MockerFixture


if platform.system() != "Windows":
	pytest.skip("MetaTrader5 tests require Windows", allow_module_level=True)
else:
	import MetaTrader5 as mt5

	from stpstone.utils.trading_platforms.mt5.mt5 import MT5


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> Logger:
	"""Fixture providing a mock logger instance.

	Returns
	-------
	Logger
		Mocked logger object
	"""
	return MagicMock(spec=Logger)


@pytest.fixture
def mt5_instance(mock_logger: Logger) -> Any:  # noqa ANN401: typing.Any is disallowed
	"""Fixture providing an MT5 instance with mocked logger.

	Parameters
	----------
	mock_logger : Logger
		Mocked logger instance

	Returns
	-------
	Any
		MT5 instance with mocked dependencies
	"""
	return MT5(logger=mock_logger)


@pytest.fixture
def sample_symbol_info() -> dict:
	"""Fixture providing sample symbol information.

	Returns
	-------
	dict
		Dictionary with sample symbol information
	"""
	return {
		"name": "EURUSD",
		"path": "Forex\\Major\\EURUSD",
		"spread": 10,
		"digits": 5,
		"expiration_time": 1234567890,
		"time": 1234567890,
	}


@pytest.fixture
def sample_tick_data() -> dict:
	"""Fixture providing sample tick data.

	Returns
	-------
	dict
		Dictionary with sample tick data
	"""
	return {
		"time": [1234567890, 1234567891],
		"bid": [1.12000, 1.12005],
		"ask": [1.12010, 1.12015],
		"last": [1.12005, 1.12010],
		"volume": [1000, 1500],
		"time_msc": [1234567890000, 1234567891000],
	}


# --------------------------
# Tests
# --------------------------
class TestMT5Initialization:
	"""Test cases for MT5 class initialization and basic methods."""

	def test_init_with_valid_logger(self, mock_logger: Logger) -> None:
		"""Test initialization with valid logger.

		Verifies
		--------
		- MT5 can be initialized with a Logger instance
		- The logger is correctly stored in the instance

		Parameters
		----------
		mock_logger : Logger
			Mocked logger instance

		Returns
		-------
		None
		"""
		instance = MT5(logger=mock_logger)
		assert instance.logger is mock_logger

	def test_package_info_logs_correctly(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
	) -> None:
		"""Test package_info logs author and version.

		Verifies
		--------
		- package_info logs the MT5 package author and version
		- Log messages are correctly formatted

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mocker.patch.object(mt5, "__author__", "MetaQuotes")
		mocker.patch.object(mt5, "__version__", "5.0")

		mt5_instance.package_info()

		mock_logger.info.assert_any_call("MetaTrader5 package author: MetaQuotes")
		mock_logger.info.assert_any_call("MetaTrader5 package version: 5.0")


class TestCredentialValidation:
	"""Test cases for credential validation."""

	@pytest.mark.parametrize(
		"path,login,server,password,expected_error",
		[
			("", 123, "server", "pass", "Path cannot be empty"),
			("path", 0, "server", "pass", "Login cannot be empty"),
			("path", 123, "", "pass", "Server cannot be empty"),
			("path", 123, "server", "", "Password cannot be empty"),
		],
	)
	def test_validate_credentials_raises_on_invalid_input(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		path: str,
		login: int,
		server: str,
		password: str,
		expected_error: str,
	) -> None:
		"""Test _validate_credentials raises ValueError on invalid inputs.

		Verifies
		--------
		- _validate_credentials raises ValueError when any credential is empty
		- Error message matches expected value

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		path : str
			Path to MetaTrader5 terminal executable
		login : int
			Account login number
		server : str
			Trading server name
		password : str
			Account password
		expected_error : str
			Expected error message

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError) as excinfo:
			mt5_instance._validate_credentials(path, login, server, password)
		assert expected_error in str(excinfo.value)


class TestConnectionManagement:
	"""Test cases for connection initialization and shutdown."""

	def test_initialize_success(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
	) -> None:
		"""Test successful initialization.

		Verifies
		--------
		- initialize returns True when MT5 connection succeeds
		- No error messages are logged

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mocker.patch("MetaTrader5.initialize", return_value=True)
		result = mt5_instance.initialize("path", 123, "server", "password")
		assert result is True
		mock_logger.warning.assert_not_called()

	def test_initialize_failure(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
	) -> None:
		"""Test failed initialization.

		Verifies
		--------
		- initialize returns False when MT5 connection fails
		- Error message is logged
		- shutdown is called

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mocker.patch("MetaTrader5.initialize", return_value=False)
		mocker.patch("MetaTrader5.last_error", return_value=123)
		mocker.patch("MetaTrader5.shutdown")

		result = mt5_instance.initialize("path", 123, "server", "password")

		assert result is False
		mock_logger.warning.assert_called_with("initialize() failed, error code =123")
		mt5.shutdown.assert_called_once()

	def test_shutdown_calls_mt5_shutdown(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mocker: MockerFixture,
	) -> None:
		"""Test shutdown calls MT5 shutdown.

		Verifies
		--------
		- shutdown method calls mt5.shutdown()

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mocker.patch("MetaTrader5.shutdown")
		mt5_instance.shutdown()
		mt5.shutdown.assert_called_once()


class TestSymbolInfoRetrieval:
	"""Test cases for symbol information retrieval."""

	def test_symbols_get_calls_mt5(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mocker: MockerFixture,
	) -> None:
		"""Test symbols_get calls mt5.symbols_get.

		Verifies
		--------
		- symbols_get calls the MT5 API function
		- Returns the result directly

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mock_result = ("EURUSD", "GBPUSD")
		mocker.patch("MetaTrader5.symbols_get", return_value=mock_result)
		result = mt5_instance.symbols_get()
		assert result == mock_result

	def test_symbols_total_logs_and_returns_on_success(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
	) -> None:
		"""Test symbols_total logs and returns count when symbols exist.

		Verifies
		--------
		- Returns symbol count when > 0
		- Logs info message with count

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mocker.patch("MetaTrader5.symbols_total", return_value=10)
		result = mt5_instance.symbols_total()
		assert result == 10
		mock_logger.info.assert_called_with("Total symbols ={symbols}")

	def test_symbols_total_logs_and_returns_none_on_empty(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
	) -> None:
		"""Test symbols_total logs and returns None when no symbols.

		Verifies
		--------
		- Returns None when symbol count is 0
		- Logs info message about no symbols

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mocker.patch("MetaTrader5.symbols_total", return_value=0)
		result = mt5_instance.symbols_total()
		assert result is None
		mock_logger.info.assert_called_with("Symbols not found")


class TestGetSymbolsInfo:
	"""Test cases for get_symbols_info method."""

	def test_get_symbols_info_with_market_data(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
		sample_symbol_info: dict,
	) -> None:
		"""Test get_symbols_info with market_data=True.

		Verifies
		--------
		- Handles symbol batches correctly
		- Performs symbol selection and deselection
		- Processes symbol info correctly
		- Logs appropriate messages

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture
		sample_symbol_info : dict
			Sample symbol info dictionary

		Returns
		-------
		None
		"""
		# Setup mock symbols
		mock_symbols = [MagicMock(name="SYM1"), MagicMock(name="SYM2")]
		mock_symbols[0].name = "SYM1"
		mock_symbols[1].name = "SYM2"

		# Mock MT5 functions
		mocker.patch("MetaTrader5.symbols_get", return_value=mock_symbols)
		mocker.patch("MetaTrader5.symbol_select")
		mocker.patch(
			"MetaTrader5.symbol_info",
			return_value=MagicMock(_asdict=lambda: sample_symbol_info),
		)

		# Mock sleep to avoid actual delays
		mocker.patch("time.sleep")

		# Call method
		result = mt5_instance.get_symbols_info(market_data=True)

		# Verify results
		assert isinstance(result, pd.DataFrame)
		assert not result.empty
		mock_logger.info.assert_any_call("Lower bound: {lim_inf}")
		mock_logger.info.assert_any_call("Upper bound: {lim_sup}")
		mock_logger.info.assert_any_call("Loading")
		mock_logger.info.assert_any_call(f"Number of symbols = {result.shape[0]}")

	def test_get_symbols_info_without_market_data(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
		sample_symbol_info: dict,
	) -> None:
		"""Test get_symbols_info with market_data=False.

		Verifies
		--------
		- Processes symbol info directly without market data
		- Returns DataFrame with correct structure

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture
		sample_symbol_info : dict
			Sample symbol info dictionary

		Returns
		-------
		None
		"""
		mock_symbol = MagicMock()
		mock_symbol._asdict.return_value = sample_symbol_info
		mocker.patch("MetaTrader5.symbols_get", return_value=[mock_symbol])

		result = mt5_instance.get_symbols_info(market_data=False)

		assert isinstance(result, pd.DataFrame)
		assert not result.empty
		assert "CLASSE1" in result.columns
		assert "CLASSE2" in result.columns
		assert "CLASSE3" in result.columns

	def test_get_symbols_info_returns_none_on_empty(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
	) -> None:
		"""Test get_symbols_info returns None when no symbols.

		Verifies
		--------
		- Returns None when symbols_get returns empty
		- Logs critical message

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mocker.patch("MetaTrader5.symbols_get", return_value=[])
		result = mt5_instance.get_symbols_info()
		assert result is None
		mock_logger.critical.assert_called_with("Symbols not found")


class TestTickDataRetrieval:
	"""Test cases for tick data retrieval methods."""

	def test_get_ticks_from_success(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
		sample_tick_data: dict,
	) -> None:
		"""Test get_ticks_from with successful data retrieval.

		Verifies
		--------
		- Returns DataFrame with correct tick data
		- Converts time columns correctly
		- Logs tick count

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture
		sample_tick_data : dict
			Sample tick data dictionary

		Returns
		-------
		None
		"""
		mock_ticks = np.array(
			[
				(1234567890, 1.12000, 1.12010, 1.12005, 1000, 1234567890000),
				(1234567891, 1.12005, 1.12015, 1.12010, 1500, 1234567891000),
			],
			dtype=[
				("time", "i8"),
				("bid", "f8"),
				("ask", "f8"),
				("last", "f8"),
				("volume", "i8"),
				("time_msc", "i8"),
			],
		)

		mocker.patch("MetaTrader5.copy_ticks_from", return_value=mock_ticks)

		result = mt5_instance.get_ticks_from(
			symbol="EURUSD", date_ref=datetime.now(), ticks_qty=10
		)

		assert isinstance(result, pd.DataFrame)
		assert not result.empty
		assert "time" in result.columns
		assert "time_msc" in result.columns
		mock_logger.info.assert_called_with(f"Ticks recebidos: {result.shape[0]}")

	def test_get_ticks_from_empty(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
	) -> None:
		"""Test get_ticks_from with empty result.

		Verifies
		--------
		- Returns empty DataFrame when no ticks
		- Still logs the count (0)

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mocker.patch("MetaTrader5.copy_ticks_from", return_value=None)
		result = mt5_instance.get_ticks_from(
			symbol="EURUSD", date_ref=datetime.now(), ticks_qty=10
		)

		assert result is None
		mock_logger.info.assert_called_with("Ticks recebidos: 0")

	def test_get_ticks_range_success(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
		sample_tick_data: dict,
	) -> None:
		"""Test get_ticks_range with successful data retrieval.

		Verifies
		--------
		- Returns DataFrame with correct tick data
		- Converts time columns correctly
		- Logs tick count

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture
		sample_tick_data : dict
			Sample tick data dictionary

		Returns
		-------
		None
		"""
		mock_ticks = np.array(
			[
				(1234567890, 1.12000, 1.12010, 1.12005, 1000, 1234567890000),
				(1234567891, 1.12005, 1.12015, 1.12010, 1500, 1234567891000),
			],
			dtype=[
				("time", "i8"),
				("bid", "f8"),
				("ask", "f8"),
				("last", "f8"),
				("volume", "i8"),
				("time_msc", "i8"),
			],
		)

		mocker.patch("MetaTrader5.copy_ticks_range", return_value=mock_ticks)

		result = mt5_instance.get_ticks_range(
			symbol="EURUSD", date_ref=datetime.now(), datetime_to=datetime.now()
		)

		assert isinstance(result, pd.DataFrame)
		assert not result.empty
		assert "time" in result.columns
		assert "time_msc" in result.columns
		mock_logger.info.assert_called_with(f"Ticks recebidos: {result.shape[0]}")

	def test_get_last_tick_success(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
	) -> None:
		"""Test get_last_tick with successful retrieval.

		Verifies
		--------
		- Returns tick info when available
		- Doesn't log errors

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mock_tick = MagicMock()
		mocker.patch("MetaTrader5.symbol_info_tick", return_value=mock_tick)

		result = mt5_instance.get_last_tick("EURUSD")
		assert result is mock_tick
		mock_logger.error.assert_not_called()

	def test_get_last_tick_failure(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
	) -> None:
		"""Test get_last_tick when retrieval fails.

		Verifies
		--------
		- Returns None when no tick info
		- Logs error message

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mocker.patch("MetaTrader5.symbol_info_tick", return_value=None)
		mocker.patch("MetaTrader5.last_error", return_value=123)

		result = mt5_instance.get_last_tick("EURUSD")
		assert result is None
		mock_logger.error.assert_called_with(
			"mt5.symbol_info_tick(EURUSD) failed, error code = 123"
		)


class TestMarketDepth:
	"""Test cases for market depth functionality."""

	def test_get_market_depth_success(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
	) -> None:
		"""Test get_market_depth with successful retrieval.

		Verifies
		--------
		- Returns market book data when available
		- Calls add/get/release in correct sequence
		- Logs appropriate messages

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mock_items = [MagicMock(), MagicMock()]
		mock_items[0]._asdict.return_value = {"price": 1.12, "volume": 1000}
		mock_items[1]._asdict.return_value = {"price": 1.13, "volume": 2000}

		mocker.patch("MetaTrader5.market_book_add", return_value=True)
		mocker.patch("MetaTrader5.market_book_get", return_value=mock_items)
		mocker.patch("MetaTrader5.market_book_release")
		mocker.patch("time.sleep")

		result = mt5_instance.get_market_depth("EURUSD", n_times=2)

		assert result == mock_items
		mt5.market_book_add.assert_called_once_with("EURUSD")
		assert mt5.market_book_get.call_count == 2
		mt5.market_book_release.assert_called_once_with("EURUSD")
		assert mock_logger.info.call_count >= 4  # At least 2 items * 2 times

	def test_get_market_depth_failure(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
	) -> None:
		"""Test get_market_depth when add fails.

		Verifies
		--------
		- Returns None when market_book_add fails
		- Logs info message
		- Doesn't attempt to get data

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mocker.patch("MetaTrader5.market_book_add", return_value=False)
		mocker.patch("MetaTrader5.last_error", return_value=123)

		result = mt5_instance.get_market_depth("EURUSD")
		assert result is None
		mock_logger.info.assert_called_with("mt5.market_book_add(EURUSD) failed, error code = 123")
		mt5.market_book_get.assert_not_called()


class TestSymbolProperties:
	"""Test cases for symbol property retrieval."""

	def test_get_symbol_info_tick_success(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
	) -> None:
		"""Test get_symbol_info_tick with successful retrieval.

		Verifies
		--------
		- Returns dictionary with tick info
		- Logs appropriate messages

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mock_tick = MagicMock()
		mock_tick._asdict.return_value = {"bid": 1.12, "ask": 1.13}
		mocker.patch("MetaTrader5.symbol_info_tick", return_value=mock_tick)

		result = mt5_instance.get_symbol_info_tick("EURUSD")

		assert isinstance(result, dict)
		assert "bid" in result
		assert "ask" in result
		mock_logger.info.assert_any_call(f"lasttick = {mock_tick}")
		mock_logger.info.assert_any_call("Show symbol_info_tick(EURUSD)._asdict():")
		mock_logger.info.assert_any_call("  bid=1.12")
		mock_logger.info.assert_any_call("  ask=1.13")

	def test_get_symbol_properties_success(
		self,
		mt5_instance: Any,  # noqa ANN401: typing.Any is disallowed
		mock_logger: Logger,
		mocker: MockerFixture,
		sample_symbol_info: dict,
	) -> None:
		"""Test get_symbol_properties with successful retrieval.

		Verifies
		--------
		- Returns dictionary with symbol properties
		- Logs spread and digits
		- Logs full property dictionary

		Parameters
		----------
		mt5_instance : Any
			MT5 instance with mocked dependencies
		mock_logger : Logger
			Mocked logger instance
		mocker : MockerFixture
			Pytest mocker fixture
		sample_symbol_info : dict
			Sample symbol info dictionary

		Returns
		-------
		None
		"""
		mock_info = MagicMock()
		mock_info._asdict.return_value = sample_symbol_info
		mock_info.spread = 10
		mock_info.digits = 5
		mocker.patch("MetaTrader5.symbol_info", return_value=mock_info)

		result = mt5_instance.get_symbol_properties("EURUSD")

		assert isinstance(result, dict)
		assert result == sample_symbol_info
		mock_logger.info.assert_any_call(mock_info)
		mock_logger.info.assert_any_call("EURUSD: spread = 10 digits = 5")
		mock_logger.info.assert_any_call("Show symbol_info(EURUSD)._asdict():")
		mock_logger.info.assert_any_call(f"  name={sample_symbol_info['name']}")
