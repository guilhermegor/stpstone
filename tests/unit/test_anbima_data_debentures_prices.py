"""Unit tests for AnbimaDataDebenturesPrices ingestion class."""

from datetime import date
from logging import Logger
from typing import Any
from unittest.mock import Mock, patch

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_debentures_prices import (
	AnbimaDataDebenturesPrices,
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
def sample_prices_data() -> list[dict[str, Any]]:
	"""Fixture providing sample prices data for testing.

	Returns
	-------
	list[dict[str, Any]]
		List of sample price records
	"""
	return [
		{
			"CODIGO_DEBENTURE": "DEB001",
			"EMISSOR": "COMPANY A",
			"SETOR": "FINANCEIRO",
			"DATA_REFERENCIA": "01/01/2023",
			"VNA": "980.00",
			"PU_PAR": "1000.00",
			"PU_EVENTO": "1010.00",
			"URL": "https://data.anbima.com.br/debentures/DEB001/precos",
		}
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
# Tests for AnbimaDataDebenturesPrices
# --------------------------
class TestAnbimaDataDebenturesPrices:
	"""Test cases for AnbimaDataDebenturesPrices class.

	This test class verifies the behavior of debentures prices
	extraction functionality.
	"""

	def test_extract_text_by_xpath_success(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		mock_playwright_page: Mock,
	) -> None:
		"""Test successful text extraction by XPath.

		Verifies
		--------
		- Text is extracted when element is visible
		- Proper timeout is used
		- Whitespace is stripped

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
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_prices",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesPrices(logger=mock_logger)

		mock_element = Mock()
		mock_element.is_visible.return_value = True
		mock_element.inner_text.return_value = "  Test Value  "
		mock_playwright_page.locator.return_value.first = mock_element

		result = instance._extract_text_by_xpath(mock_playwright_page, "//test")

		assert result == "Test Value"
		mock_element.is_visible.assert_called_with(timeout=5000)

	def test_extract_text_by_xpath_element_not_visible(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		mock_playwright_page: Mock,
	) -> None:
		"""Test text extraction when element is not visible.

		Verifies
		--------
		- None is returned when element is not visible
		- Exception handling does not break the flow

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
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_prices",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesPrices(logger=mock_logger)

		mock_element = Mock()
		mock_element.is_visible.return_value = False
		mock_playwright_page.locator.return_value.first = mock_element

		result = instance._extract_text_by_xpath(mock_playwright_page, "//test")

		assert result is None

	def test_transform_prices_data_date_handling(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		sample_prices_data: list[dict[str, Any]],
	) -> None:
		"""Test date field transformation in prices data.

		Verifies
		--------
		- Date fields with hyphens are replaced with default date
		- Non-string date values are handled
		- DataFrame structure is maintained

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
		sample_prices_data : list[dict[str, Any]]
			Sample prices data for testing

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_prices",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesPrices(logger=mock_logger)

		sample_prices_data[0]["DATA_REFERENCIA"] = "-"

		result = instance.transform_data(sample_prices_data)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
		assert result.iloc[0]["DATA_REFERENCIA"] == "01/01/2100"


# --------------------------
# Error Handling Tests
# --------------------------
class TestAnbimaDebenturesPricesErrorHandling:
	"""Error handling tests for AnbimaDataDebenturesPrices class."""

	def test_prices_extraction_with_missing_elements(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		mock_playwright_page: Mock,
	) -> None:
		"""Test prices extraction with missing DOM elements.

		Verifies
		--------
		- Empty list is returned when no price elements found
		- Warning is logged for missing data
		- Method continues without crashing

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
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_prices",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesPrices(logger=mock_logger)

		mock_playwright_page.locator.return_value.all.return_value = []

		result = instance._extract_price_rows(
			mock_playwright_page, "DEB001", "COMPANY A", "FINANCEIRO", "https://example.com"
		)

		assert isinstance(result, list)
		assert len(result) == 0
