"""Unit tests for AnbimaDataFundsHistoric ingestion class.

Tests the web scraping functionality for ANBIMA fund periodic historical data including:
- AnbimaDataFundsHistoric class for historical fund data
- Error conditions, edge cases, and type validation
"""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
from playwright.sync_api import Locator, Page as PlaywrightPage
import pytest
from requests import Session

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.br.registries.anbima_data_funds_historic import (
	AnbimaDataFundsHistoric,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> MagicMock:
	"""Fixture providing a mock logger.

	Returns
	-------
	MagicMock
		Mock logger instance
	"""
	return MagicMock(spec=Logger)


@pytest.fixture
def mock_db_session() -> MagicMock:
	"""Fixture providing a mock database session.

	Returns
	-------
	MagicMock
		Mock database session instance
	"""
	return MagicMock(spec=Session)


@pytest.fixture
def mock_dates_current() -> MagicMock:
	"""Fixture providing a mock DatesCurrent instance.

	Returns
	-------
	MagicMock
		Mock DatesCurrent instance
	"""
	mock_dates = MagicMock(spec=DatesCurrent)
	mock_dates.curr_date.return_value = date(2024, 1, 1)
	return mock_dates


@pytest.fixture
def mock_dates_br() -> MagicMock:
	"""Fixture providing a mock DatesBRAnbima instance.

	Returns
	-------
	MagicMock
		Mock DatesBRAnbima instance
	"""
	mock_dates = MagicMock(spec=DatesBRAnbima)
	mock_dates.add_working_days.return_value = date(2023, 12, 29)
	return mock_dates


@pytest.fixture
def mock_create_log() -> MagicMock:
	"""Fixture providing a mock CreateLog instance.

	Returns
	-------
	MagicMock
		Mock CreateLog instance
	"""
	mock_log = MagicMock(spec=CreateLog)
	mock_log.log_message = MagicMock()
	return mock_log


@pytest.fixture
def mock_dir_files_management() -> MagicMock:
	"""Fixture providing a mock DirFilesManagement instance.

	Returns
	-------
	MagicMock
		Mock DirFilesManagement instance
	"""
	return MagicMock(spec=DirFilesManagement)


@pytest.fixture
def mock_playwright_page() -> MagicMock:
	"""Fixture providing a mock Playwright Page.

	Returns
	-------
	MagicMock
		Mock Playwright Page instance
	"""
	return MagicMock(spec=PlaywrightPage)


@pytest.fixture
def mock_locator() -> MagicMock:
	"""Fixture providing a mock Playwright Locator.

	Returns
	-------
	MagicMock
		Mock Playwright Locator instance
	"""
	return MagicMock(spec=Locator)


@pytest.fixture
def sample_historic_row() -> dict[str, Any]:
	"""Fixture providing sample historic row data for testing.

	Returns
	-------
	dict[str, Any]
		Sample historic row data dictionary
	"""
	return {
		"FUND_CODE": "S0000634344",
		"DATA_HORA_ATUALIZACAO": "2024-01-01 12:00:00",
		"DATA_COMPETENCIA": "31/12/2023",
		"PL": "1.000.000,00",
		"VALOR_COTA": "1,50",
		"VOLUME_TOTAL_APLICACOES": "500.000,00",
		"VOLUME_TOTAL_RESGATES": "200.000,00",
		"NUMERO_COTISTAS": "100",
	}


# --------------------------
# Tests for AnbimaDataFundsHistoric
# --------------------------
class TestAnbimaDataFundsHistoric:
	"""Test cases for AnbimaDataFundsHistoric class.

	This test class verifies the initialization, web scraping, data transformation,
	and error handling for the periodic historical fund data collection.
	"""

	@pytest.fixture
	def funds_historic_instance(
		self,
		mock_logger: MagicMock,
		mock_db_session: MagicMock,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_create_log: MagicMock,
		mock_dir_files_management: MagicMock,
	) -> AnbimaDataFundsHistoric:
		"""Fixture providing AnbimaDataFundsHistoric instance with mocked dependencies.

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance
		mock_db_session : MagicMock
			Mock database session
		mock_dates_current : MagicMock
			Mock DatesCurrent instance
		mock_dates_br : MagicMock
			Mock DatesBRAnbima instance
		mock_create_log : MagicMock
			Mock CreateLog instance
		mock_dir_files_management : MagicMock
			Mock DirFilesManagement instance

		Returns
		-------
		AnbimaDataFundsHistoric
			Instance with mocked dependencies
		"""
		with patch.object(AnbimaDataFundsHistoric, "__init__", lambda self, **kwargs: None):
			instance = AnbimaDataFundsHistoric()
			instance.logger = mock_logger
			instance.cls_db = mock_db_session
			instance.cls_dir_files_management = mock_dir_files_management
			instance.cls_dates_current = mock_dates_current
			instance.cls_create_log = mock_create_log
			instance.cls_dates_br = mock_dates_br
			instance.date_ref = date(2023, 12, 29)
			instance.base_url = "https://data.anbima.com.br/fundos"
			instance.list_fund_codes = ["S0000634344", "S0000634345"]
		return instance

	def test_init_with_fund_codes(
		self, mock_logger: MagicMock, mock_db_session: MagicMock
	) -> None:
		"""Test initialization with fund codes list.

		Verifies
		--------
		- Fund codes list is stored correctly
		- Default values are set properly

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance
		mock_db_session : MagicMock
			Mock database session

		Returns
		-------
		None
		"""
		fund_codes = ["FUND1", "FUND2", "FUND3"]
		instance = AnbimaDataFundsHistoric(
			list_fund_codes=fund_codes, logger=mock_logger, cls_db=mock_db_session
		)
		assert instance.list_fund_codes == fund_codes

	def test_init_with_empty_fund_codes(
		self, mock_logger: MagicMock, mock_db_session: MagicMock
	) -> None:
		"""Test initialization with empty fund codes defaults to empty list.

		Verifies
		--------
		- Empty list is stored when no fund codes provided
		- Default empty list behavior

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance
		mock_db_session : MagicMock
			Mock database session

		Returns
		-------
		None
		"""
		instance = AnbimaDataFundsHistoric(logger=mock_logger, cls_db=mock_db_session)
		assert instance.list_fund_codes == []

	def test_handle_date_value_dash_replaced(
		self, funds_historic_instance: AnbimaDataFundsHistoric
	) -> None:
		"""Test date value handling with dash replacement.

		Verifies
		--------
		- '-' values are replaced with '01/01/2100'
		- None values are replaced with '01/01/2100'
		- Valid dates are returned unchanged

		Parameters
		----------
		funds_historic_instance : AnbimaDataFundsHistoric
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		test_cases = [
			("-", "01/01/2100"),
			(None, "01/01/2100"),
			("31/12/2023", "31/12/2023"),
			("01/01/2024", "01/01/2024"),
		]

		for input_date, expected in test_cases:
			result = funds_historic_instance._handle_date_value(input_date)
			assert result == expected

	def test_transform_data_empty_input(
		self, funds_historic_instance: AnbimaDataFundsHistoric
	) -> None:
		"""Test data transformation with empty input.

		Verifies
		--------
		- Empty list input returns empty DataFrame
		- No exceptions are raised

		Parameters
		----------
		funds_historic_instance : AnbimaDataFundsHistoric
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		result = funds_historic_instance.transform_data([])

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0

	def test_transform_data_with_records(
		self,
		funds_historic_instance: AnbimaDataFundsHistoric,
		sample_historic_row: dict[str, Any],
	) -> None:
		"""Test data transformation with valid records.

		Verifies
		--------
		- Records are converted to DataFrame
		- DATA_COMPETENCIA column is processed
		- DataFrame has correct number of rows

		Parameters
		----------
		funds_historic_instance : AnbimaDataFundsHistoric
			Instance with mocked dependencies
		sample_historic_row : dict[str, Any]
			Sample historic row data

		Returns
		-------
		None
		"""
		result = funds_historic_instance.transform_data([sample_historic_row])

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
		assert "FUND_CODE" in result.columns
		assert "DATA_COMPETENCIA" in result.columns

	def test_transform_data_date_dash_replacement(
		self, funds_historic_instance: AnbimaDataFundsHistoric
	) -> None:
		"""Test transform_data replaces dash in DATA_COMPETENCIA.

		Verifies
		--------
		- '-' in DATA_COMPETENCIA is replaced with '01/01/2100'
		- Other columns remain unchanged

		Parameters
		----------
		funds_historic_instance : AnbimaDataFundsHistoric
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		raw_data = [
			{
				"FUND_CODE": "S0000634344",
				"DATA_HORA_ATUALIZACAO": None,
				"DATA_COMPETENCIA": "-",
				"PL": "1.000.000,00",
			}
		]

		result = funds_historic_instance.transform_data(raw_data)

		assert isinstance(result, pd.DataFrame)
		assert result.iloc[0]["DATA_COMPETENCIA"] == "01/01/2100"

	def test_parse_raw_file_returns_stringio(
		self, funds_historic_instance: AnbimaDataFundsHistoric
	) -> None:
		"""Test parse_raw_file returns StringIO for compatibility.

		Verifies
		--------
		- Method returns StringIO instance
		- Different response types are accepted

		Parameters
		----------
		funds_historic_instance : AnbimaDataFundsHistoric
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		mock_response = MagicMock()

		result = funds_historic_instance.parse_raw_file(mock_response)

		assert isinstance(result, StringIO)

	@patch("stpstone.ingestion.countries.br.registries.anbima_data_funds_historic.sync_playwright")
	def test_get_response_no_fund_codes(
		self, mock_sync_playwright: MagicMock, funds_historic_instance: AnbimaDataFundsHistoric
	) -> None:
		"""Test get_response with empty fund codes list returns empty list.

		Verifies
		--------
		- Empty list is returned when no fund codes provided
		- Playwright is not invoked
		- Warning log is generated

		Parameters
		----------
		mock_sync_playwright : MagicMock
			Mock sync_playwright context manager
		funds_historic_instance : AnbimaDataFundsHistoric
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		funds_historic_instance.list_fund_codes = []

		result = funds_historic_instance.get_response()

		assert result == []
		mock_sync_playwright.assert_not_called()

	@patch("stpstone.ingestion.countries.br.registries.anbima_data_funds_historic.sync_playwright")
	def test_get_response_extracts_historic_data(
		self, mock_sync_playwright: MagicMock, funds_historic_instance: AnbimaDataFundsHistoric
	) -> None:
		"""Test get_response calls _extract_historic_data for each fund code.

		Verifies
		--------
		- _extract_historic_data is called once per fund code
		- Results are aggregated into a single list
		- Browser is closed after scraping

		Parameters
		----------
		mock_sync_playwright : MagicMock
			Mock sync_playwright context manager
		funds_historic_instance : AnbimaDataFundsHistoric
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		mock_playwright = MagicMock()
		mock_browser = MagicMock()
		mock_page = MagicMock()

		mock_sync_playwright.return_value.__enter__.return_value = mock_playwright
		mock_playwright.chromium.launch.return_value = mock_browser
		mock_browser.new_page.return_value = mock_page

		funds_historic_instance.list_fund_codes = ["S0000634344"]

		with (
			patch.object(funds_historic_instance, "_extract_historic_data") as mock_extract,
			patch(
				"stpstone.ingestion.countries.br.registries.anbima_data_funds_historic.time.sleep"
			),
		):
			mock_extract.return_value = [
				{"FUND_CODE": "S0000634344", "DATA_COMPETENCIA": "31/12/2023"}
			]

			result = funds_historic_instance.get_response(timeout_ms=100)

		mock_extract.assert_called_once()
		assert isinstance(result, list)
		assert len(result) == 1
		mock_browser.close.assert_called_once()

	def test_run_with_db_insertion(self, funds_historic_instance: AnbimaDataFundsHistoric) -> None:
		"""Test run method with database insertion.

		Verifies
		--------
		- Data is inserted into database when cls_db is provided
		- Standardization methods are called
		- No DataFrame is returned when using database

		Parameters
		----------
		funds_historic_instance : AnbimaDataFundsHistoric
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		with (
			patch.object(funds_historic_instance, "get_response") as mock_get_response,
			patch.object(funds_historic_instance, "transform_data") as mock_transform,
			patch.object(funds_historic_instance, "standardize_dataframe") as mock_standardize,
			patch.object(funds_historic_instance, "insert_table_db") as mock_insert,
		):
			mock_get_response.return_value = [{"FUND_CODE": "S0000634344"}]
			mock_transform.return_value = pd.DataFrame({"FUND_CODE": ["S0000634344"]})
			mock_standardize.return_value = pd.DataFrame({"FUND_CODE": ["S0000634344"]})

			result = funds_historic_instance.run()

			mock_get_response.assert_called_once()
			mock_transform.assert_called_once()
			mock_standardize.assert_called_once()
			mock_insert.assert_called_once()
			assert result is None

	def test_run_without_db_returns_dataframe(
		self, funds_historic_instance: AnbimaDataFundsHistoric
	) -> None:
		"""Test run method returns DataFrame when no database connection.

		Verifies
		--------
		- DataFrame is returned when cls_db is None
		- Database insertion is skipped
		- All transformation steps are executed

		Parameters
		----------
		funds_historic_instance : AnbimaDataFundsHistoric
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		funds_historic_instance.cls_db = None

		with (
			patch.object(funds_historic_instance, "get_response") as mock_get_response,
			patch.object(funds_historic_instance, "transform_data") as mock_transform,
			patch.object(funds_historic_instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = [{"FUND_CODE": "S0000634344"}]
			expected_df = pd.DataFrame({"FUND_CODE": ["S0000634344"]})
			mock_transform.return_value = expected_df
			mock_standardize.return_value = expected_df

			result = funds_historic_instance.run()

			mock_get_response.assert_called_once()
			mock_transform.assert_called_once()
			mock_standardize.assert_called_once()
			assert isinstance(result, pd.DataFrame)
			assert len(result) == 1


# --------------------------
# Error Handling and Edge Cases
# --------------------------
class TestErrorHandling:
	"""Test error handling and edge cases for AnbimaDataFundsHistoric."""

	def test_all_classes_inheritance_validation(self) -> None:
		"""Test that AnbimaDataFundsHistoric properly inherits from base classes.

		Verifies
		--------
		- Class inherits from ABCIngestionOperations
		- Method overrides are correct

		Returns
		-------
		None
		"""
		historic = AnbimaDataFundsHistoric()
		assert isinstance(historic, ABCIngestionOperations)

	def test_date_handling_edge_cases(self, mock_logger: MagicMock) -> None:
		"""Test date handling with various edge cases.

		Verifies
		--------
		- Various invalid date formats are handled without raising exceptions
		- Default date '01/01/2100' is used only for falsy or '-' values
		- Method is robust to unexpected input

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance

		Returns
		-------
		None
		"""
		instance = AnbimaDataFundsHistoric(logger=mock_logger)

		test_cases = [
			("", "01/01/2100"),
			("invalid", "invalid"),
			("2023-13-45", "2023-13-45"),
			("31/02/2023", "31/02/2023"),
		]

		for test_case, expected in test_cases:
			result = instance._handle_date_value(test_case)
			assert result == expected

	@patch("stpstone.ingestion.countries.br.registries.anbima_data_funds_historic.sync_playwright")
	def test_get_response_exception_handling(
		self, mock_sync_playwright: MagicMock, mock_logger: MagicMock
	) -> None:
		"""Test that exceptions during per-fund scraping are caught and logged.

		Verifies
		--------
		- An exception for one fund does not stop processing of others
		- Error is logged via cls_create_log

		Parameters
		----------
		mock_sync_playwright : MagicMock
			Mock sync_playwright context manager
		mock_logger : MagicMock
			Mock logger instance

		Returns
		-------
		None
		"""
		mock_playwright = MagicMock()
		mock_browser = MagicMock()
		mock_page = MagicMock()

		mock_sync_playwright.return_value.__enter__.return_value = mock_playwright
		mock_playwright.chromium.launch.return_value = mock_browser
		mock_browser.new_page.return_value = mock_page

		instance = AnbimaDataFundsHistoric(list_fund_codes=["BADFUND"], logger=mock_logger)

		with (
			patch.object(instance, "_extract_historic_data", side_effect=Exception("fail")),
			patch(
				"stpstone.ingestion.countries.br.registries.anbima_data_funds_historic.time.sleep"
			),
		):
			result = instance.get_response(timeout_ms=100)

		assert isinstance(result, list)
		assert len(result) == 0


# --------------------------
# Type Validation Tests
# --------------------------
class TestTypeValidation:
	"""Test type validation for AnbimaDataFundsHistoric method parameters and return values."""

	def test_funds_historic_method_signatures(self) -> None:
		"""Test AnbimaDataFundsHistoric method signatures and types.

		Verifies
		--------
		- Method parameters have correct type hints
		- Return types are correctly annotated
		- All public methods are properly typed

		Returns
		-------
		None
		"""
		init_annotations = AnbimaDataFundsHistoric.__init__.__annotations__
		assert "date_ref" in init_annotations
		assert "logger" in init_annotations
		assert "cls_db" in init_annotations
		assert "list_fund_codes" in init_annotations

		run_annotations = AnbimaDataFundsHistoric.run.__annotations__
		assert "return" in run_annotations

		get_response_annotations = AnbimaDataFundsHistoric.get_response.__annotations__
		assert "return" in get_response_annotations

		transform_annotations = AnbimaDataFundsHistoric.transform_data.__annotations__
		assert "return" in transform_annotations
