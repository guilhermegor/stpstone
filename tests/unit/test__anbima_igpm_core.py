"""Unit tests for AnbimaIGPMCore private base class.

Tests the functionality of AnbimaIGPMCore for initialization,
parse_raw_file, run (with and without db), and transform_data,
covering normal operations, edge cases, and type validation.
"""

from datetime import date
from logging import Logger
import sys

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Session

from stpstone.ingestion.countries.br.macroeconomics._anbima_igpm_core import AnbimaIGPMCore
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger(mocker: MockerFixture) -> Logger:
	"""Mock logger instance for testing logging behavior.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	Logger
		Mocked logger object.
	"""
	return mocker.patch("logging.Logger")


@pytest.fixture
def mock_session(mocker: MockerFixture) -> Session:
	"""Mock database session for testing database operations.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	Session
		Mocked database session object.
	"""
	return mocker.patch("requests.Session")


@pytest.fixture
def mock_playwright_scraper(mocker: MockerFixture) -> PlaywrightScraper:
	"""Mock PlaywrightScraper for testing web scraping operations.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	PlaywrightScraper
		Mocked PlaywrightScraper object.
	"""
	scraper = mocker.patch("stpstone.utils.webdriver_tools.playwright_wd.PlaywrightScraper")
	scraper_instance = scraper.return_value
	scraper_instance.launch.return_value.__enter__.return_value = None
	scraper_instance.navigate.return_value = True
	return scraper_instance


@pytest.fixture
def anbima_igpm_core(mock_logger: Logger, mock_session: Session) -> AnbimaIGPMCore:
	"""Fixture providing AnbimaIGPMCore instance with mocked dependencies.

	Parameters
	----------
	mock_logger : Logger
		Mocked logger instance.
	mock_session : Session
		Mocked database session.

	Returns
	-------
	AnbimaIGPMCore
		Initialized AnbimaIGPMCore instance.
	"""
	return AnbimaIGPMCore(date_ref=date(2025, 1, 1), logger=mock_logger, cls_db=mock_session)


# --------------------------
# Tests for AnbimaIGPMCore
# --------------------------
class TestAnbimaIGPMCore:
	"""Test cases for AnbimaIGPMCore base class."""

	def test_init_with_valid_inputs(self, anbima_igpm_core: AnbimaIGPMCore) -> None:
		"""Test initialization with valid inputs.

		Verifies
		--------
		- Instance is properly initialized with provided date_ref, logger, and cls_db.
		- Inherited classes are initialized correctly.
		- Default URL is set.

		Parameters
		----------
		anbima_igpm_core : AnbimaIGPMCore
			Initialized AnbimaIGPMCore instance.

		Returns
		-------
		None
		"""
		assert anbima_igpm_core.date_ref == date(2025, 1, 1)
		assert anbima_igpm_core.logger is not None
		assert anbima_igpm_core.cls_db is not None
		assert isinstance(anbima_igpm_core.cls_dir_files_management, DirFilesManagement)
		assert isinstance(anbima_igpm_core.cls_dates_current, DatesCurrent)
		assert isinstance(anbima_igpm_core.cls_create_log, CreateLog)
		assert isinstance(anbima_igpm_core.cls_dates_br, DatesBRAnbima)
		assert isinstance(anbima_igpm_core.cls_html_handler, HtmlHandler)
		assert isinstance(anbima_igpm_core.cls_dict_handler, HandlingDicts)
		assert isinstance(anbima_igpm_core.cls_list_handler, ListHandler)
		assert anbima_igpm_core.url == "FILL_ME"

	def test_init_with_default_date(
		self,
		mock_logger: Logger,
		mock_session: Session,
		mocker: MockerFixture,
	) -> None:
		"""Test initialization with default date.

		Verifies
		--------
		- date_ref is set to previous working day when not provided.

		Parameters
		----------
		mock_logger : Logger
			Mocked logger instance.
		mock_session : Session
			Mocked database session.
		mocker : MockerFixture
			Pytest mocker for patching dependencies.

		Returns
		-------
		None
		"""
		mocker.patch.object(DatesCurrent, "curr_date", return_value=date(2025, 1, 2))
		mocker.patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1))
		core = AnbimaIGPMCore(logger=mock_logger, cls_db=mock_session)
		assert core.date_ref == date(2025, 1, 1)

	def test_init_invalid_date_type_string(
		self,
		mock_logger: Logger,
		mock_session: Session,
	) -> None:
		"""Test initialization with invalid date type raises TypeError.

		Parameters
		----------
		mock_logger : Logger
			Mocked logger instance.
		mock_session : Session
			Mocked database session.

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			AnbimaIGPMCore(date_ref="2025-01-01", logger=mock_logger, cls_db=mock_session)

	def test_init_invalid_date_type_int(
		self,
		mock_logger: Logger,
		mock_session: Session,
	) -> None:
		"""Test initialization with invalid integer date type raises TypeError.

		Parameters
		----------
		mock_logger : Logger
			Mocked logger instance.
		mock_session : Session
			Mocked database session.

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			AnbimaIGPMCore(date_ref=123, logger=mock_logger, cls_db=mock_session)

	def test_init_invalid_date_type_list(
		self,
		mock_logger: Logger,
		mock_session: Session,
	) -> None:
		"""Test initialization with invalid list date type raises TypeError.

		Parameters
		----------
		mock_logger : Logger
			Mocked logger instance.
		mock_session : Session
			Mocked database session.

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			AnbimaIGPMCore(date_ref=[], logger=mock_logger, cls_db=mock_session)

	def test_parse_raw_file(self, anbima_igpm_core: AnbimaIGPMCore) -> None:
		"""Test parse_raw_file returns PlaywrightScraper instance.

		Verifies
		--------
		- Returns PlaywrightScraper with correct configuration.
		- Headless mode and timeout are set correctly.

		Parameters
		----------
		anbima_igpm_core : AnbimaIGPMCore
			AnbimaIGPMCore instance.

		Returns
		-------
		None
		"""
		scraper = anbima_igpm_core.parse_raw_file()
		assert isinstance(scraper, PlaywrightScraper)
		assert scraper.bool_headless is True
		assert scraper.int_default_timeout == 5_000

	def test_run_with_db(
		self,
		anbima_igpm_core: AnbimaIGPMCore,
		mock_playwright_scraper: PlaywrightScraper,
		mocker: MockerFixture,
	) -> None:
		"""Test run method with database session.

		Verifies
		--------
		- Calls parse_raw_file, transform_data, standardize_dataframe, and insert_table_db.
		- Does not return DataFrame when cls_db is provided.

		Parameters
		----------
		anbima_igpm_core : AnbimaIGPMCore
			AnbimaIGPMCore instance.
		mock_playwright_scraper : PlaywrightScraper
			Mocked PlaywrightScraper instance.
		mocker : MockerFixture
			Pytest mocker for patching dependencies.

		Returns
		-------
		None
		"""
		mock_df = pd.DataFrame({"MES_COLETA": ["Janeiro 2025"], "DATA": ["01/01/25"]})
		mocker.patch.object(AnbimaIGPMCore, "parse_raw_file", return_value=mock_playwright_scraper)
		mocker.patch.object(AnbimaIGPMCore, "transform_data", return_value=mock_df)
		mocker.patch.object(AnbimaIGPMCore, "standardize_dataframe", return_value=mock_df)
		mock_insert = mocker.patch.object(AnbimaIGPMCore, "insert_table_db")

		dict_dtypes = {"MES_COLETA": str, "DATA": "date"}
		result = anbima_igpm_core.run(
			dict_dtypes,
			timeout=(12.0, 21.0),
			bool_verify=True,
			bool_insert_or_ignore=False,
			str_table_name="test_table",
		)

		assert result is None
		mock_insert.assert_called_once_with(
			cls_db=anbima_igpm_core.cls_db,
			str_table_name="test_table",
			df_=mock_df,
			bool_insert_or_ignore=False,
		)

	def test_run_without_db(
		self,
		anbima_igpm_core: AnbimaIGPMCore,
		mock_playwright_scraper: PlaywrightScraper,
		mocker: MockerFixture,
	) -> None:
		"""Test run method without database session returns DataFrame.

		Verifies
		--------
		- Returns transformed DataFrame.
		- Does not call insert_table_db.

		Parameters
		----------
		anbima_igpm_core : AnbimaIGPMCore
			AnbimaIGPMCore instance.
		mock_playwright_scraper : PlaywrightScraper
			Mocked PlaywrightScraper instance.
		mocker : MockerFixture
			Pytest mocker for patching dependencies.

		Returns
		-------
		None
		"""
		anbima_igpm_core.cls_db = None
		mock_df = pd.DataFrame({"MES_COLETA": ["Janeiro 2025"], "DATA": ["01/01/25"]})
		mocker.patch.object(AnbimaIGPMCore, "parse_raw_file", return_value=mock_playwright_scraper)
		mocker.patch.object(AnbimaIGPMCore, "transform_data", return_value=mock_df)
		mocker.patch.object(AnbimaIGPMCore, "standardize_dataframe", return_value=mock_df)
		mock_insert = mocker.patch.object(AnbimaIGPMCore, "insert_table_db")

		dict_dtypes = {"MES_COLETA": str, "DATA": "date"}
		result = anbima_igpm_core.run(
			dict_dtypes,
			timeout=(12.0, 21.0),
			bool_verify=True,
			bool_insert_or_ignore=False,
			str_table_name="test_table",
		)

		assert result.equals(mock_df)
		mock_insert.assert_not_called()


# --------------------------
# Tests for module reload
# --------------------------
def test_module_reload(
	mock_logger: Logger,
	mock_session: Session,
	mocker: MockerFixture,
) -> None:
	"""Test module reloading behavior.

	Verifies
	--------
	- Module can be reloaded without errors.
	- New instance initializes correctly.

	Parameters
	----------
	mock_logger : Logger
		Mocked logger instance.
	mock_session : Session
		Mocked database session.
	mocker : MockerFixture
		Pytest mocker for patching dependencies.

	Returns
	-------
	None
	"""
	import importlib

	mocker.patch.object(DatesCurrent, "curr_date", return_value=date(2025, 1, 2))
	mocker.patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1))

	importlib.reload(
		sys.modules["stpstone.ingestion.countries.br.macroeconomics._anbima_igpm_core"]
	)
	new_instance = AnbimaIGPMCore()
	assert new_instance.url == "FILL_ME"
	assert new_instance.date_ref == date(2025, 1, 1)
