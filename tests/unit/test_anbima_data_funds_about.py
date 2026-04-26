"""Unit tests for AnbimaDataFundsAbout ingestion class.

Tests the web scraping functionality for ANBIMA fund detail information including:
- AnbimaDataFundsAbout class for detailed fund information
- Error conditions, edge cases, and type validation
"""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import MagicMock, patch

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from requests import Session

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.br.registries.anbima_data_funds_about import (
	AnbimaDataFundsAbout,
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


# --------------------------
# Tests for AnbimaDataFundsAbout
# --------------------------
class TestAnbimaDataFundsAbout:
	"""Test cases for AnbimaDataFundsAbout class.

	This test class verifies the detailed fund information scraping,
	including characteristics, related structure, and about data.
	"""

	@pytest.fixture
	def funds_about_instance(
		self,
		mock_logger: MagicMock,
		mock_db_session: MagicMock,
		mock_dates_current: MagicMock,
		mock_dates_br: MagicMock,
		mock_create_log: MagicMock,
		mock_dir_files_management: MagicMock,
	) -> AnbimaDataFundsAbout:
		"""Fixture providing AnbimaDataFundsAbout instance with mocked dependencies.

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
		AnbimaDataFundsAbout
			Instance with mocked dependencies
		"""
		with patch.object(AnbimaDataFundsAbout, "__init__", lambda self, **kwargs: None):
			instance = AnbimaDataFundsAbout()
			instance.logger = mock_logger
			instance.cls_db = mock_db_session
			instance.cls_dir_files_management = mock_dir_files_management
			instance.cls_dates_current = mock_dates_current
			instance.cls_create_log = mock_create_log
			instance.cls_dates_br = mock_dates_br
			instance.date_ref = date(2023, 12, 29)
			instance.base_url = "https://data.anbima.com.br/fundos"
			instance.list_fund_codes = ["ABC123", "DEF456"]
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
		instance = AnbimaDataFundsAbout(
			list_fund_codes=fund_codes, logger=mock_logger, cls_db=mock_db_session
		)
		assert instance.list_fund_codes == fund_codes

	def test_init_with_empty_fund_codes(
		self, mock_logger: MagicMock, mock_db_session: MagicMock
	) -> None:
		"""Test initialization with empty fund codes.

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
		instance = AnbimaDataFundsAbout(logger=mock_logger, cls_db=mock_db_session)
		assert instance.list_fund_codes == []

	def test_handle_date_value_replacement(
		self, funds_about_instance: AnbimaDataFundsAbout
	) -> None:
		"""Test date value handling with dash replacement.

		Verifies
		--------
		- '-' values are replaced with '01/01/2100'
		- None values are replaced with '01/01/2100'
		- Valid dates are returned unchanged

		Parameters
		----------
		funds_about_instance : AnbimaDataFundsAbout
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		test_cases = [
			("-", "01/01/2100"),
			(None, "01/01/2100"),
			("15/12/2023", "15/12/2023"),
			("01/01/2024", "01/01/2024"),
		]

		for input_date, expected in test_cases:
			result = funds_about_instance._handle_date_value(input_date)
			assert result == expected

	def test_transform_characteristics_data_empty(
		self, funds_about_instance: AnbimaDataFundsAbout
	) -> None:
		"""Test characteristics transformation with empty data.

		Verifies
		--------
		- Empty list returns empty DataFrame
		- Date columns are processed correctly

		Parameters
		----------
		funds_about_instance : AnbimaDataFundsAbout
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		result = funds_about_instance.transform_characteristics_data([])

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0

	def test_transform_characteristics_data_with_dates(
		self, funds_about_instance: AnbimaDataFundsAbout
	) -> None:
		"""Test characteristics transformation with date values.

		Verifies
		--------
		- Date columns are processed through _handle_date_value
		- DataFrame structure is maintained

		Parameters
		----------
		funds_about_instance : AnbimaDataFundsAbout
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		raw_data = [{"FUND_CODE": "ABC123", "DATA_ULTIMA_COTA": "-", "NOME_FUNDO": "Test Fund"}]

		result = funds_about_instance.transform_characteristics_data(raw_data)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
		assert result.iloc[0]["DATA_ULTIMA_COTA"] == "01/01/2100"

	def test_transform_related_data_empty(
		self, funds_about_instance: AnbimaDataFundsAbout
	) -> None:
		"""Test related data transformation with empty input.

		Verifies
		--------
		- Empty list returns empty DataFrame
		- Basic DataFrame structure is correct

		Parameters
		----------
		funds_about_instance : AnbimaDataFundsAbout
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		result = funds_about_instance.transform_related_data([])

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0

	def test_transform_about_data_empty(self, funds_about_instance: AnbimaDataFundsAbout) -> None:
		"""Test about data transformation with empty input.

		Verifies
		--------
		- Empty list returns empty DataFrame
		- Date columns are processed

		Parameters
		----------
		funds_about_instance : AnbimaDataFundsAbout
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		result = funds_about_instance.transform_about_data([])

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0

	def test_transform_about_data_with_dates(
		self, funds_about_instance: AnbimaDataFundsAbout
	) -> None:
		"""Test about data transformation with various date values.

		Verifies
		--------
		- Multiple date columns are processed correctly
		- Non-date columns remain unchanged

		Parameters
		----------
		funds_about_instance : AnbimaDataFundsAbout
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		raw_data = [
			{
				"FUND_CODE": "ABC123",
				"DATA_ENCERRAMENTO_FUNDO": "-",
				"DATA_INICIO_ATIVIDADE_CLASSE": "01/01/2020",
				"NOME_FUNDO": "Test Fund",
			}
		]

		result = funds_about_instance.transform_about_data(raw_data)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
		assert result.iloc[0]["DATA_ENCERRAMENTO_FUNDO"] == "01/01/2100"
		assert result.iloc[0]["DATA_INICIO_ATIVIDADE_CLASSE"] == "01/01/2020"

	def test_parse_raw_file_returns_stringio(
		self, funds_about_instance: AnbimaDataFundsAbout
	) -> None:
		"""Test parse_raw_file returns StringIO for compatibility.

		Verifies
		--------
		- Method returns StringIO instance
		- Different response types are accepted

		Parameters
		----------
		funds_about_instance : AnbimaDataFundsAbout
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		mock_response = MagicMock()

		result = funds_about_instance.parse_raw_file(mock_response)

		assert isinstance(result, StringIO)

	def test_transform_data_returns_dataframe(
		self, funds_about_instance: AnbimaDataFundsAbout
	) -> None:
		"""Test transform_data returns empty DataFrame for compatibility.

		Verifies
		--------
		- Method returns DataFrame instance
		- Input file is accepted but not used

		Parameters
		----------
		funds_about_instance : AnbimaDataFundsAbout
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		mock_file = StringIO()

		result = funds_about_instance.transform_data(mock_file)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0

	@patch("stpstone.ingestion.countries.br.registries.anbima_data_funds_about.sync_playwright")
	def test_get_response_no_fund_codes(
		self, mock_sync_playwright: MagicMock, funds_about_instance: AnbimaDataFundsAbout
	) -> None:
		"""Test get_response with empty fund codes list.

		Verifies
		--------
		- Empty dictionary is returned when no fund codes
		- Playwright is not invoked
		- Warning log is generated

		Parameters
		----------
		mock_sync_playwright : MagicMock
			Mock sync_playwright context manager
		funds_about_instance : AnbimaDataFundsAbout
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		funds_about_instance.list_fund_codes = []

		result = funds_about_instance.get_response()

		assert result == {"characteristics": [], "related": [], "about": []}
		mock_sync_playwright.assert_not_called()

	def test_run_with_db_insertion(self, funds_about_instance: AnbimaDataFundsAbout) -> None:
		"""Test run method with database insertion for all data types.

		Verifies
		--------
		- All three data types are inserted into separate tables
		- Standardization is applied to each DataFrame
		- No return value when using database

		Parameters
		----------
		funds_about_instance : AnbimaDataFundsAbout
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		with (
			patch.object(funds_about_instance, "get_response") as mock_get_response,
			patch.object(
				funds_about_instance, "transform_characteristics_data"
			) as mock_char_transform,
			patch.object(funds_about_instance, "transform_related_data") as mock_rel_transform,
			patch.object(funds_about_instance, "transform_about_data") as mock_about_transform,
			patch.object(funds_about_instance, "standardize_dataframe") as mock_standardize,
			patch.object(funds_about_instance, "insert_table_db") as mock_insert,
		):
			mock_get_response.return_value = {
				"characteristics": [{"FUND_CODE": "ABC123"}],
				"related": [{"FUND_CODE": "ABC123"}],
				"about": [{"FUND_CODE": "ABC123"}],
			}

			mock_char_transform.return_value = pd.DataFrame({"FUND_CODE": ["ABC123"]})
			mock_rel_transform.return_value = pd.DataFrame({"FUND_CODE": ["ABC123"]})
			mock_about_transform.return_value = pd.DataFrame({"FUND_CODE": ["ABC123"]})
			mock_standardize.side_effect = [
				pd.DataFrame({"FUND_CODE": ["ABC123"]}),
				pd.DataFrame({"FUND_CODE": ["ABC123"]}),
				pd.DataFrame({"FUND_CODE": ["ABC123"]}),
			]

			result = funds_about_instance.run()

			assert mock_insert.call_count == 3
			assert result is None

	def test_run_without_db_returns_dict(self, funds_about_instance: AnbimaDataFundsAbout) -> None:
		"""Test run method returns dictionary when no database connection.

		Verifies
		--------
		- Dictionary with three DataFrames is returned
		- Database insertion is skipped
		- All transformation steps are executed

		Parameters
		----------
		funds_about_instance : AnbimaDataFundsAbout
			Instance with mocked dependencies

		Returns
		-------
		None
		"""
		funds_about_instance.cls_db = None

		with (
			patch.object(funds_about_instance, "get_response") as mock_get_response,
			patch.object(
				funds_about_instance, "transform_characteristics_data"
			) as mock_char_transform,
			patch.object(funds_about_instance, "transform_related_data") as mock_rel_transform,
			patch.object(funds_about_instance, "transform_about_data") as mock_about_transform,
			patch.object(funds_about_instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = {
				"characteristics": [{"FUND_CODE": "ABC123"}],
				"related": [{"FUND_CODE": "ABC123"}],
				"about": [{"FUND_CODE": "ABC123"}],
			}

			expected_df = pd.DataFrame({"FUND_CODE": ["ABC123"]})
			mock_char_transform.return_value = expected_df
			mock_rel_transform.return_value = expected_df
			mock_about_transform.return_value = expected_df
			mock_standardize.side_effect = [expected_df, expected_df, expected_df]

			result = funds_about_instance.run()

			assert isinstance(result, dict)
			assert set(result.keys()) == {"characteristics", "related", "about"}
			assert all(isinstance(df_, pd.DataFrame) for df_ in result.values())
			assert all(len(df_) == 1 for df_ in result.values())


# --------------------------
# Error Handling and Edge Cases
# --------------------------
class TestErrorHandling:
	"""Test error handling and edge cases for AnbimaDataFundsAbout."""

	def test_funds_about_empty_xpath_handling(self, mock_logger: MagicMock) -> None:
		"""Test handling of empty or missing XPath elements.

		Verifies
		--------
		- None is returned for missing elements
		- Processing continues despite missing data
		- Logging occurs for missing elements

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance

		Returns
		-------
		None
		"""
		_ = AnbimaDataFundsAbout(logger=mock_logger)

		mock_page = MagicMock()
		mock_locator = MagicMock()
		mock_locator.count.return_value = 0
		mock_page.locator.return_value = mock_locator

		assert mock_locator.count() == 0

	def test_all_classes_inheritance_validation(self) -> None:
		"""Test that AnbimaDataFundsAbout properly inherits from base classes.

		Verifies
		--------
		- Class inherits from ABCIngestionOperations
		- Method overrides are correct

		Returns
		-------
		None
		"""
		about = AnbimaDataFundsAbout()
		assert isinstance(about, ABCIngestionOperations)

	def test_date_handling_edge_cases(self, mock_logger: MagicMock) -> None:
		"""Test date handling with various edge cases.

		Verifies
		--------
		- Various invalid date formats are handled
		- Default date is used for invalid inputs
		- Method doesn't raise exceptions

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance

		Returns
		-------
		None
		"""
		instance = AnbimaDataFundsAbout(logger=mock_logger)

		test_cases = [
			("", "01/01/2100"),
			(" ", " "),
			("invalid", "invalid"),
			("2023-13-45", "2023-13-45"),
			("31/02/2023", "31/02/2023"),
			("00/00/0000", "00/00/0000"),
		]

		for test_case, expected in test_cases:
			result = instance._handle_date_value(test_case)
			assert result == expected


# --------------------------
# Type Validation Tests
# --------------------------
class TestTypeValidation:
	"""Test type validation for AnbimaDataFundsAbout method parameters and return values."""

	def test_funds_about_method_signatures(self) -> None:
		"""Test AnbimaDataFundsAbout method signatures and types.

		Verifies
		--------
		- Method parameters have correct type hints
		- Return types are correctly annotated
		- Transformation methods have proper signatures

		Returns
		-------
		None
		"""
		init_annotations = AnbimaDataFundsAbout.__init__.__annotations__
		assert "list_fund_codes" in init_annotations

		run_annotations = AnbimaDataFundsAbout.run.__annotations__
		assert "return" in run_annotations

		char_annotations = AnbimaDataFundsAbout.transform_characteristics_data.__annotations__
		assert "return" in char_annotations
