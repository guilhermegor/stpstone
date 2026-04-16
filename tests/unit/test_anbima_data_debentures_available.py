"""Unit tests for AnbimaDataDebenturesAvailable ingestion class."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Any
from unittest.mock import Mock, patch

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_debentures_available import (
	AnbimaDataDebenturesAvailable,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> Mock:
	"""Fixture providing a mock logger.

	Returns
	-------
	Mock
		Mocked logger instance
	"""
	return Mock(spec=Logger)


@pytest.fixture
def mock_db_session() -> Mock:
	"""Fixture providing a mock database session.

	Returns
	-------
	Mock
		Mocked database session
	"""
	return Mock(spec=Session)


@pytest.fixture
def mock_dates_current() -> Mock:
	"""Fixture providing a mock DatesCurrent instance.

	Returns
	-------
	Mock
		Mocked DatesCurrent instance
	"""
	mock_dates = Mock(spec=DatesCurrent)
	mock_dates.curr_date.return_value = date(2023, 1, 1)
	return mock_dates


@pytest.fixture
def mock_dates_br() -> Mock:
	"""Fixture providing a mock DatesBRAnbima instance.

	Returns
	-------
	Mock
		Mocked DatesBRAnbima instance
	"""
	mock_dates = Mock(spec=DatesBRAnbima)
	mock_dates.add_working_days.return_value = date(2023, 1, 1)
	return mock_dates


@pytest.fixture
def mock_create_log() -> Mock:
	"""Fixture providing a mock CreateLog instance.

	Returns
	-------
	Mock
		Mocked CreateLog instance
	"""
	mock_log = Mock(spec=CreateLog)
	mock_log.log_message = Mock()
	return mock_log


@pytest.fixture
def mock_dir_files_management() -> Mock:
	"""Fixture providing a mock DirFilesManagement instance.

	Returns
	-------
	Mock
		Mocked DirFilesManagement instance
	"""
	return Mock(spec=DirFilesManagement)


@pytest.fixture
def sample_debenture_data() -> list[dict[str, Any]]:
	"""Fixture providing sample debenture data for testing.

	Returns
	-------
	list[dict[str, Any]]
		List of sample debenture records
	"""
	return [
		{
			"NOME": "DEBENTURE A",
			"EMISSOR": "COMPANY A",
			"REMUNERACAO": "10.5%",
			"DATA_VENCIMENTO": "01/01/2030",
			"DURATION": "5 years",
			"SETOR": "FINANCEIRO",
			"DATA_EMISSAO": "01/01/2023",
			"PU_PAR": "1000.00",
			"PU_INDICATIVO": "1050.00",
			"pagina": 1,
		},
		{
			"NOME": "DEBENTURE B",
			"EMISSOR": "COMPANY B",
			"REMUNERACAO": "8.5%",
			"DATA_VENCIMENTO": "01/01/2035",
			"DURATION": "10 years",
			"SETOR": "INDUSTRIAL",
			"DATA_EMISSAO": "01/01/2022",
			"PU_PAR": "950.00",
			"PU_INDICATIVO": "980.00",
			"pagina": 1,
		},
	]


@pytest.fixture
def mock_playwright_page() -> Mock:
	"""Fixture providing a mock Playwright page.

	Returns
	-------
	Mock
		Mocked Playwright page
	"""
	mock_page = Mock(spec=PlaywrightPage)
	mock_page.query_selector_all.return_value = []
	mock_page.query_selector.return_value = None
	mock_page.locator.return_value = Mock()
	mock_page.goto = Mock()
	mock_page.wait_for_timeout = Mock()
	return mock_page


# --------------------------
# Tests for AnbimaDataDebenturesAvailable
# --------------------------
class TestAnbimaDataDebenturesAvailable:
	"""Test cases for AnbimaDataDebenturesAvailable class.

	This test class verifies the behavior of debentures listing functionality
	with various input scenarios and edge cases.
	"""

	def test_init_with_valid_parameters(
		self,
		mock_logger: Mock,
		mock_db_session: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
	) -> None:
		"""Test initialization with valid parameters.

		Verifies
		--------
		- The class can be initialized with valid parameters
		- Default values are set correctly
		- All dependencies are properly injected

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_db_session : Mock
			Mocked database session
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_available",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesAvailable(
				date_ref=date(2023, 1, 1),
				logger=mock_logger,
				cls_db=mock_db_session,
				start_page=0,
				end_page=10,
			)

		assert instance.logger == mock_logger
		assert instance.cls_db == mock_db_session
		assert instance.date_ref == date(2023, 1, 1)
		assert instance.start_page == 0
		assert instance.end_page == 10
		assert instance.base_url == "https://data.anbima.com.br/busca/debentures"

	def test_init_with_default_parameters(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
	) -> None:
		"""Test initialization with default parameters.

		Verifies
		--------
		- The class uses default values when parameters are not provided
		- Date reference defaults to previous working day
		- Page range defaults to 0-20

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_available",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

		assert instance.logger == mock_logger
		assert instance.cls_db is None
		assert instance.date_ref == date(2023, 1, 1)
		assert instance.start_page == 0
		assert instance.end_page == 20

	@pytest.mark.parametrize("start_page,end_page", [(-1, 10), (10, 5)])
	def test_init_with_invalid_page_range_raises_value_error(
		self,
		start_page: int,
		end_page: int,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
	) -> None:
		"""Test initialization with invalid page range raises ValueError.

		Verifies
		--------
		- Negative start_page raises ValueError
		- end_page less than start_page raises ValueError

		Parameters
		----------
		start_page : int
			Invalid start page value
		end_page : int
			Invalid end page value
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance

		Returns
		-------
		None
		"""
		with (
			patch.multiple(
				"stpstone.ingestion.countries.br.registries.anbima_data_debentures_available",
				DatesCurrent=Mock(return_value=mock_dates_current),
				DatesBRAnbima=Mock(return_value=mock_dates_br),
				CreateLog=Mock(return_value=mock_create_log),
				DirFilesManagement=Mock(return_value=mock_dir_files_management),
			),
			pytest.raises(ValueError),
		):
			AnbimaDataDebenturesAvailable(
				logger=mock_logger, start_page=start_page, end_page=end_page
			)

	def test_extract_debenture_data_success(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		mock_playwright_page: Mock,
	) -> None:
		"""Test successful extraction of debenture data.

		Verifies
		--------
		- Data extraction works with valid page and element
		- All fields are properly extracted
		- Missing fields are handled gracefully

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance
		mock_playwright_page : Mock
			Mocked Playwright page

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_available",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

		mock_element = Mock()
		mock_element.inner_text.return_value = "Test Debenture"
		mock_element.get_attribute.return_value = "item-nome-123"

		mock_field_element = Mock()
		mock_field_element.inner_text.return_value = "Field Value"
		mock_playwright_page.query_selector.return_value = mock_field_element

		result = instance._extract_debenture_data(mock_playwright_page, "123", "Test Debenture")

		assert result["NOME"] == "Test Debenture"
		assert "EMISSOR" in result
		assert "REMUNERACAO" in result

	def test_extract_debenture_data_with_missing_fields(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		mock_playwright_page: Mock,
	) -> None:
		"""Test debenture data extraction with missing fields.

		Verifies
		--------
		- Missing fields are set to None
		- Extraction continues despite individual field errors
		- Error logging is performed for failed extractions

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance
		mock_playwright_page : Mock
			Mocked Playwright page

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_available",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

		mock_playwright_page.query_selector.return_value = None

		result = instance._extract_debenture_data(mock_playwright_page, "123", "Test Debenture")

		assert result["NOME"] == "Test Debenture"
		assert result["EMISSOR"] is None
		assert result["REMUNERACAO"] is None

	def test_transform_data_with_valid_input(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		sample_debenture_data: list[dict[str, Any]],
	) -> None:
		"""Test data transformation with valid input.

		Verifies
		--------
		- DataFrame is created correctly from raw data
		- Page column is dropped if present
		- Empty input returns empty DataFrame

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance
		sample_debenture_data : list[dict[str, Any]]
			Sample debenture data for testing

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_available",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

		result = instance.transform_data(sample_debenture_data)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 2
		assert "pagina" not in result.columns

	def test_transform_data_with_empty_input(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
	) -> None:
		"""Test data transformation with empty input.

		Verifies
		--------
		- Empty list input returns empty DataFrame

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_available",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

		result_empty = instance.transform_data([])

		assert isinstance(result_empty, pd.DataFrame)
		assert len(result_empty) == 0

	def test_parse_raw_file_returns_string_io(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
	) -> None:
		"""Test parse_raw_file returns StringIO for compatibility.

		Verifies
		--------
		- Method returns StringIO instance for compatibility
		- Different response types are handled

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_available",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

		mock_response = Mock()
		result = instance.parse_raw_file(mock_response)

		assert isinstance(result, StringIO)

	@patch(
		"stpstone.ingestion.countries.br.registries"
		".anbima_data_debentures_available.sync_playwright"
	)
	def test_get_response_success(
		self,
		mock_sync_playwright: Mock,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		mock_playwright_page: Mock,
	) -> None:
		"""Test successful response retrieval with mocked Playwright.

		Verifies
		--------
		- Playwright browser is properly launched and closed
		- Page navigation and element queries are performed
		- Data extraction methods are called

		Parameters
		----------
		mock_sync_playwright : Mock
			Mocked sync_playwright context manager
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance
		mock_playwright_page : Mock
			Mocked Playwright page

		Returns
		-------
		None
		"""
		mock_browser = Mock()
		mock_browser.new_page.return_value = mock_playwright_page
		mock_context = Mock()
		mock_context.chromium.launch.return_value = mock_browser
		mock_sync_playwright.return_value.__enter__.return_value = mock_context

		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_available",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesAvailable(logger=mock_logger, start_page=0, end_page=1)

		mock_element = Mock()
		mock_element.inner_text.return_value = "Test Debenture"
		mock_element.get_attribute.return_value = "item-nome-123"
		mock_playwright_page.query_selector_all.return_value = [mock_element]

		result = instance.get_response(timeout_ms=1000)

		assert isinstance(result, list)
		mock_browser.close.assert_called_once()

	@patch(
		"stpstone.ingestion.countries.br.registries"
		".anbima_data_debentures_available.sync_playwright"
	)
	def test_get_response_with_no_elements(
		self,
		mock_sync_playwright: Mock,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		mock_playwright_page: Mock,
	) -> None:
		"""Test response retrieval when no elements are found.

		Verifies
		--------
		- Empty list is returned when no elements found
		- Browser is properly closed
		- Logging indicates no items found

		Parameters
		----------
		mock_sync_playwright : Mock
			Mocked sync_playwright context manager
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance
		mock_playwright_page : Mock
			Mocked Playwright page

		Returns
		-------
		None
		"""
		mock_browser = Mock()
		mock_browser.new_page.return_value = mock_playwright_page
		mock_context = Mock()
		mock_context.chromium.launch.return_value = mock_browser
		mock_sync_playwright.return_value.__enter__.return_value = mock_context

		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_available",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesAvailable(logger=mock_logger, start_page=0, end_page=1)

		mock_playwright_page.query_selector_all.return_value = []

		result = instance.get_response(timeout_ms=1000)

		assert isinstance(result, list)
		assert len(result) == 0
		mock_browser.close.assert_called_once()


# --------------------------
# Integration Tests
# --------------------------
class TestAnbimaDebenturesAvailableIntegration:
	"""Integration tests for AnbimaDataDebenturesAvailable class.

	This test class verifies the integrated behavior of the
	available debentures class end-to-end.
	"""

	def test_end_to_end_without_database(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		sample_debenture_data: list[dict[str, Any]],
	) -> None:
		"""Test end-to-end workflow without database insertion.

		Verifies
		--------
		- Complete workflow from data extraction to transformation
		- DataFrame is returned when no database session provided
		- Standardization process is applied

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance
		sample_debenture_data : list[dict[str, Any]]
			Sample debenture data for testing

		Returns
		-------
		None
		"""
		with (
			patch.multiple(
				"stpstone.ingestion.countries.br.registries.anbima_data_debentures_available",
				DatesCurrent=Mock(return_value=mock_dates_current),
				DatesBRAnbima=Mock(return_value=mock_dates_br),
				CreateLog=Mock(return_value=mock_create_log),
				DirFilesManagement=Mock(return_value=mock_dir_files_management),
			),
			patch.object(
				AnbimaDataDebenturesAvailable, "get_response", return_value=sample_debenture_data
			),
			patch.object(
				AnbimaDataDebenturesAvailable, "standardize_dataframe", return_value=pd.DataFrame()
			),
		):
			instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

			result = instance.run(bool_insert_or_ignore=False)

		assert isinstance(result, pd.DataFrame)

	def test_end_to_end_with_database(
		self,
		mock_logger: Mock,
		mock_db_session: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		sample_debenture_data: list[dict[str, Any]],
	) -> None:
		"""Test end-to-end workflow with database insertion.

		Verifies
		--------
		- Database insertion is called when session is provided
		- Insert method receives correct parameters
		- None is returned when data is inserted to database

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_db_session : Mock
			Mocked database session
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance
		sample_debenture_data : list[dict[str, Any]]
			Sample debenture data for testing

		Returns
		-------
		None
		"""
		with (
			patch.multiple(
				"stpstone.ingestion.countries.br.registries.anbima_data_debentures_available",
				DatesCurrent=Mock(return_value=mock_dates_current),
				DatesBRAnbima=Mock(return_value=mock_dates_br),
				CreateLog=Mock(return_value=mock_create_log),
				DirFilesManagement=Mock(return_value=mock_dir_files_management),
			),
			patch.object(
				AnbimaDataDebenturesAvailable, "get_response", return_value=sample_debenture_data
			),
			patch.object(
				AnbimaDataDebenturesAvailable, "standardize_dataframe", return_value=pd.DataFrame()
			),
		):
			mock_insert = Mock(return_value=None)

			instance = AnbimaDataDebenturesAvailable(logger=mock_logger, cls_db=mock_db_session)

			with patch.object(instance, "insert_table_db", mock_insert):
				result = instance.run(bool_insert_or_ignore=True)

		assert result is None
		mock_insert.assert_called_once()
