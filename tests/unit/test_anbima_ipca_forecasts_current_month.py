"""Unit tests for AnbimaIPCAForecastsCurrentMonth ingestion module.

Tests initialization, run (with and without db), and transform_data,
covering normal operations, edge cases, and type validation.
"""

from datetime import date
from logging import Logger
import sys

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Session

from stpstone.ingestion.countries.br.macroeconomics._anbima_ipca_core import AnbimaIPCACore
from stpstone.ingestion.countries.br.macroeconomics.anbima_ipca_forecasts_current_month import (
	AnbimaIPCAForecastsCurrentMonth,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.parsers.dicts import HandlingDicts
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
def anbima_ipca_current_month(
	mock_logger: Logger,
	mock_session: Session,
) -> AnbimaIPCAForecastsCurrentMonth:
	"""Fixture providing AnbimaIPCAForecastsCurrentMonth instance.

	Parameters
	----------
	mock_logger : Logger
		Mocked logger instance.
	mock_session : Session
		Mocked database session.

	Returns
	-------
	AnbimaIPCAForecastsCurrentMonth
		Initialized AnbimaIPCAForecastsCurrentMonth instance.
	"""
	return AnbimaIPCAForecastsCurrentMonth(logger=mock_logger, cls_db=mock_session)


# --------------------------
# Tests for AnbimaIPCAForecastsCurrentMonth
# --------------------------
class TestAnbimaIPCAForecastsCurrentMonth:
	"""Test cases for AnbimaIPCAForecastsCurrentMonth class."""

	def test_init(
		self,
		anbima_ipca_current_month: AnbimaIPCAForecastsCurrentMonth,
	) -> None:
		"""Test initialization of AnbimaIPCAForecastsCurrentMonth.

		Verifies
		--------
		- Inherits from AnbimaIPCACore correctly.
		- Sets correct URL.

		Parameters
		----------
		anbima_ipca_current_month : AnbimaIPCAForecastsCurrentMonth
			AnbimaIPCAForecastsCurrentMonth instance.

		Returns
		-------
		None
		"""
		assert isinstance(anbima_ipca_current_month, AnbimaIPCACore)
		assert anbima_ipca_current_month.url == (
			"https://www.anbima.com.br/pt_br/informar/estatisticas/"
			"precos-e-indices/projecao-de-inflacao-gp-m.htm"
		)

	def test_run_returns_dataframe(
		self,
		anbima_ipca_current_month: AnbimaIPCAForecastsCurrentMonth,
		mock_playwright_scraper: PlaywrightScraper,
		mocker: MockerFixture,
	) -> None:
		"""Test run method returns DataFrame when no database session is provided.

		Verifies
		--------
		- Uses correct dictionary of data types.
		- Returns DataFrame with expected columns.

		Parameters
		----------
		anbima_ipca_current_month : AnbimaIPCAForecastsCurrentMonth
			AnbimaIPCAForecastsCurrentMonth instance.
		mock_playwright_scraper : PlaywrightScraper
			Mocked PlaywrightScraper instance.
		mocker : MockerFixture
			Pytest mocker for patching dependencies.

		Returns
		-------
		None
		"""
		mock_df = pd.DataFrame(
			{
				"MES_COLETA": ["Janeiro 2025"],
				"DATA": ["01/01/25"],
				"PROJECAO_PCT": [2.5],
				"DATA_VALIDADE": ["31/01/25"],
			}
		)
		mocker.patch.object(AnbimaIPCACore, "parse_raw_file", return_value=mock_playwright_scraper)
		mocker.patch.object(
			AnbimaIPCAForecastsCurrentMonth, "transform_data", return_value=mock_df
		)
		mocker.patch.object(AnbimaIPCACore, "standardize_dataframe", return_value=mock_df)

		anbima_ipca_current_month.cls_db = None
		result = anbima_ipca_current_month.run()

		assert result.equals(mock_df)
		assert result.columns.tolist() == ["MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE"]

	def test_run_with_db_calls_insert(
		self,
		anbima_ipca_current_month: AnbimaIPCAForecastsCurrentMonth,
		mock_playwright_scraper: PlaywrightScraper,
		mocker: MockerFixture,
	) -> None:
		"""Test run method with database session calls insert_table_db.

		Verifies
		--------
		- Does not return a DataFrame when cls_db is provided.
		- Calls insert_table_db with the correct table name.

		Parameters
		----------
		anbima_ipca_current_month : AnbimaIPCAForecastsCurrentMonth
			AnbimaIPCAForecastsCurrentMonth instance.
		mock_playwright_scraper : PlaywrightScraper
			Mocked PlaywrightScraper instance.
		mocker : MockerFixture
			Pytest mocker for patching dependencies.

		Returns
		-------
		None
		"""
		mock_df = pd.DataFrame({"MES_COLETA": ["Janeiro 2025"]})
		mocker.patch.object(AnbimaIPCACore, "parse_raw_file", return_value=mock_playwright_scraper)
		mocker.patch.object(
			AnbimaIPCAForecastsCurrentMonth, "transform_data", return_value=mock_df
		)
		mocker.patch.object(AnbimaIPCACore, "standardize_dataframe", return_value=mock_df)
		mock_insert = mocker.patch.object(AnbimaIPCACore, "insert_table_db")

		result = anbima_ipca_current_month.run()

		assert result is None
		mock_insert.assert_called_once()
		call_kwargs = mock_insert.call_args.kwargs
		assert call_kwargs["str_table_name"] == "br_anbima_ipca_forecasts_current_month"

	def test_transform_data(
		self,
		anbima_ipca_current_month: AnbimaIPCAForecastsCurrentMonth,
		mock_playwright_scraper: PlaywrightScraper,
		mocker: MockerFixture,
	) -> None:
		"""Test transform_data method for current month forecasts.

		Verifies
		--------
		- Correctly transforms scraped data into DataFrame.
		- Handles comma to dot conversion.

		Parameters
		----------
		anbima_ipca_current_month : AnbimaIPCAForecastsCurrentMonth
			AnbimaIPCAForecastsCurrentMonth instance.
		mock_playwright_scraper : PlaywrightScraper
			Mocked PlaywrightScraper instance.
		mocker : MockerFixture
			Pytest mocker for patching dependencies.

		Returns
		-------
		None
		"""
		mock_playwright_scraper.get_elements.return_value = [
			{"text": "Janeiro de 2025"},
			{"text": "01/01/25"},
			{"text": "2,5"},
			{"text": "31/01/25"},
		]
		mocker.patch.object(
			HandlingDicts,
			"pair_headers_with_data",
			return_value=[
				{
					"MES_COLETA": "Janeiro de 2025",
					"DATA": "01/01/25",
					"PROJECAO_PCT": "2.5",
					"DATA_VALIDADE": "31/01/25",
				}
			],
		)

		result = anbima_ipca_current_month.transform_data(mock_playwright_scraper)

		assert isinstance(result, pd.DataFrame)
		assert result.columns.tolist() == ["MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE"]


# --------------------------
# Tests for module reload
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
	"""Test module reloading behavior.

	Verifies
	--------
	- Module can be reloaded without errors.

	Parameters
	----------
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
		sys.modules[
			"stpstone.ingestion.countries.br.macroeconomics.anbima_ipca_forecasts_current_month"
		]
	)
	new_instance = AnbimaIPCAForecastsCurrentMonth()
	assert new_instance.date_ref == date(2025, 1, 1)
