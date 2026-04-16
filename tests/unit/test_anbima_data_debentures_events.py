"""Unit tests for AnbimaDataDebenturesEvents ingestion class."""

from datetime import date
from logging import Logger
from typing import Any
from unittest.mock import Mock, patch

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_debentures_events import (
	AnbimaDataDebenturesEvents,
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
def sample_events_data() -> list[dict[str, Any]]:
	"""Fixture providing sample events data for testing.

	Returns
	-------
	list[dict[str, Any]]
		List of sample event records
	"""
	return [
		{
			"CODIGO_DEBENTURE": "DEB001",
			"EMISSOR": "COMPANY A",
			"SETOR": "FINANCEIRO",
			"DATA_EVENTO": "01/01/2023",
			"DATA_LIQUIDACAO": "05/01/2023",
			"EVENTO": "PAGAMENTO DE JUROS",
			"PERCENTUAL_TAXA": "10.5%",
			"VALOR_PAGO": "1050.00",
			"STATUS": "LIQUIDADO",
			"URL": "https://data.anbima.com.br/debentures/DEB001/agenda",
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
# Tests for AnbimaDataDebenturesEvents
# --------------------------
class TestAnbimaDataDebenturesEvents:
	"""Test cases for AnbimaDataDebenturesEvents class.

	This test class verifies the behavior of debentures events
	extraction functionality.
	"""

	def test_get_total_pages_success(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		mock_playwright_page: Mock,
	) -> None:
		"""Test successful extraction of total pages from pagination.

		Verifies
		--------
		- Total pages are extracted from pagination text using regex
		- First number in text is extracted

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
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_events",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesEvents(logger=mock_logger)

		mock_text_element = Mock()
		mock_text_element.is_visible.return_value = True
		mock_text_element.inner_text.return_value = "10"
		mock_text_locator = Mock()
		mock_text_locator.first = mock_text_element

		mock_playwright_page.locator.return_value = mock_text_locator

		result = instance._get_total_pages(mock_playwright_page)

		assert result == 10

	def test_get_total_pages_fallback(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		mock_playwright_page: Mock,
	) -> None:
		"""Test fallback behavior when pagination extraction fails.

		Verifies
		--------
		- Default value of 1 is returned when extraction fails
		- Method handles exceptions gracefully

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
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_events",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesEvents(logger=mock_logger)

		mock_element = Mock()
		mock_element.is_visible.return_value = False
		mock_playwright_page.locator.return_value.first = mock_element

		result = instance._get_total_pages(mock_playwright_page)

		assert result == 1

	def test_transform_events_data_date_handling(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		sample_events_data: list[dict[str, Any]],
	) -> None:
		"""Test date field transformation in events data.

		Verifies
		--------
		- Multiple date fields with hyphens are replaced with default date
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
		sample_events_data : list[dict[str, Any]]
			Sample events data for testing

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_events",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesEvents(logger=mock_logger)

		sample_events_data[0]["DATA_EVENTO"] = "-"
		sample_events_data[0]["DATA_LIQUIDACAO"] = "-"

		result = instance.transform_data(sample_events_data)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
		assert result.iloc[0]["DATA_EVENTO"] == "01/01/2100"
		assert result.iloc[0]["DATA_LIQUIDACAO"] == "01/01/2100"
