"""Unit tests for MaisRetornoAvlFunds ingestion class.

Tests cover initialisation, Playwright-based scraping, data transformation,
and database insertion / DataFrame return logic.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.ingestion.countries.br.registries.mais_retorno_avl_funds import (
	MaisRetornoAvlFunds,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date() -> date:
	"""Provide a fixed reference date for deterministic testing.

	Returns
	-------
	date
		Fixed date 2025-01-01.
	"""
	return date(2025, 1, 1)


@pytest.fixture
def avl_funds_instance(sample_date: date) -> MaisRetornoAvlFunds:
	"""Return an initialised MaisRetornoAvlFunds instance.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	MaisRetornoAvlFunds
		Initialised instance.
	"""
	return MaisRetornoAvlFunds(date_ref=sample_date, list_slugs=[1])


@pytest.fixture
def mock_scraper(mocker: MockerFixture) -> MagicMock:
	"""Return a mocked PlaywrightScraper instance.

	The scraper's launch() is patched as a context manager that yields the
	scraper itself, matching the real PlaywrightScraper.launch() signature.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	MagicMock
		Mocked PlaywrightScraper.
	"""
	from contextlib import contextmanager

	scraper = MagicMock()
	scraper.navigate.return_value = True
	scraper.selector_exists.side_effect = [True, False]
	scraper.get_element.return_value = {"text": "TEST"}
	scraper.get_element_attrb.return_value = "lista-fundos-investimentos/aasl-fia"

	@contextmanager
	def _launch() -> None:
		"""Yield the scraper as a context manager."""
		yield scraper

	scraper.launch = _launch
	return scraper


@pytest.fixture
def sample_df() -> pd.DataFrame:
	"""Return a minimal valid DataFrame for standardisation mocking.

	Returns
	-------
	pd.DataFrame
		Minimal DataFrame with expected columns.
	"""
	return pd.DataFrame(
		{
			"CNPJ": ["12.345.678/0001-99"],
			"URL_FUND": ["https://maisretorno.com/lista-fundos-investimentos/aasl-fia"],
			"FUND_NAME": ["TEST FUND"],
			"CATEGORY": ["Ações"],
			"STATUS_FUND": ["Aberto"],
			"PAGE_POSITION": [1],
		}
	)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialisation with explicit date and slug list.

	Verifies
	--------
	- Attributes are stored correctly.
	- Helper classes are instantiated.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	None
	"""
	instance = MaisRetornoAvlFunds(date_ref=sample_date, list_slugs=[1, 2])
	assert instance.date_ref == sample_date
	assert instance.list_slugs == [1, 2]
	assert isinstance(instance.cls_create_log, CreateLog)
	assert isinstance(instance.cls_dates_current, DatesCurrent)
	assert isinstance(instance.cls_dates_br, DatesBRAnbima)
	assert isinstance(instance.cls_dir_files_management, DirFilesManagement)


def test_init_default_date() -> None:
	"""Test that default date_ref falls back to previous working day.

	Verifies
	--------
	- DatesBRAnbima.add_working_days is called to compute the default date.

	Returns
	-------
	None
	"""
	with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
		instance = MaisRetornoAvlFunds()
		assert instance.date_ref == date(2025, 1, 1)


def test_init_default_slugs() -> None:
	"""Test that list_slugs defaults to range 1-10 when not provided.

	Verifies
	--------
	- Default slug list is [1..10].

	Returns
	-------
	None
	"""
	instance = MaisRetornoAvlFunds()
	assert instance.list_slugs == list(range(1, 11))


def test_get_response_returns_none(avl_funds_instance: MaisRetornoAvlFunds) -> None:
	"""Test that get_response is a no-op returning None.

	Verifies
	--------
	- Method returns None without raising.

	Parameters
	----------
	avl_funds_instance : MaisRetornoAvlFunds
		Initialised instance.

	Returns
	-------
	None
	"""
	result = avl_funds_instance.get_response()
	assert result is None


def test_parse_raw_file_returns_playwright_scraper(
	avl_funds_instance: MaisRetornoAvlFunds,
	mocker: MockerFixture,
) -> None:
	"""Test that parse_raw_file returns a PlaywrightScraper instance.

	Verifies
	--------
	- PlaywrightScraper is constructed with headless and timeout params.

	Parameters
	----------
	avl_funds_instance : MaisRetornoAvlFunds
		Initialised instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mock_cls = mocker.patch(
		"stpstone.ingestion.countries.br.registries.mais_retorno_avl_funds.PlaywrightScraper"
	)
	mock_instance = mock_cls.return_value
	result = avl_funds_instance.parse_raw_file()
	mock_cls.assert_called_once_with(
		bool_headless=avl_funds_instance.bool_headless,
		int_default_timeout=avl_funds_instance.int_wait_load_seconds * 1_000,
		logger=avl_funds_instance.logger,
	)
	assert result is mock_instance


def test_transform_data_normal(
	avl_funds_instance: MaisRetornoAvlFunds,
	mock_scraper: MagicMock,
) -> None:
	"""Test transform_data returns a non-empty DataFrame on valid scraping.

	Verifies
	--------
	- DataFrame has expected columns.
	- PAGE_POSITION is populated.

	Parameters
	----------
	avl_funds_instance : MaisRetornoAvlFunds
		Initialised instance.
	mock_scraper : MagicMock
		Mocked PlaywrightScraper.

	Returns
	-------
	None
	"""
	df_ = avl_funds_instance.transform_data(scraper_playwright=mock_scraper)
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty
	assert "CNPJ" in df_.columns
	assert "URL_FUND" in df_.columns
	assert "PAGE_POSITION" in df_.columns


def test_transform_data_navigation_failure(
	avl_funds_instance: MaisRetornoAvlFunds,
	mock_scraper: MagicMock,
) -> None:
	"""Test transform_data handles failed navigation gracefully.

	Verifies
	--------
	- Returns empty DataFrame when all page navigations fail.

	Parameters
	----------
	avl_funds_instance : MaisRetornoAvlFunds
		Initialised instance.
	mock_scraper : MagicMock
		Mocked PlaywrightScraper with navigate returning False.

	Returns
	-------
	None
	"""
	mock_scraper.navigate.return_value = False
	df_ = avl_funds_instance.transform_data(scraper_playwright=mock_scraper)
	assert isinstance(df_, pd.DataFrame)
	assert df_.empty


def test_run_without_db(
	avl_funds_instance: MaisRetornoAvlFunds,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run returns a DataFrame when no database session is provided.

	Verifies
	--------
	- parse_raw_file, transform_data, and standardize_dataframe are called.
	- A DataFrame is returned.

	Parameters
	----------
	avl_funds_instance : MaisRetornoAvlFunds
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame for mock return.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch.object(avl_funds_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(avl_funds_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(avl_funds_instance, "standardize_dataframe", return_value=sample_df)

	result = avl_funds_instance.run()
	assert isinstance(result, pd.DataFrame)
	avl_funds_instance.standardize_dataframe.assert_called_once()


def test_run_with_db(
	avl_funds_instance: MaisRetornoAvlFunds,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run inserts to database and returns None when cls_db is set.

	Verifies
	--------
	- insert_table_db is called once.
	- run returns None.

	Parameters
	----------
	avl_funds_instance : MaisRetornoAvlFunds
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame for mock return.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	avl_funds_instance.cls_db = MagicMock()
	mocker.patch.object(avl_funds_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(avl_funds_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(avl_funds_instance, "standardize_dataframe", return_value=sample_df)
	mocker.patch.object(avl_funds_instance, "insert_table_db")

	result = avl_funds_instance.run()
	assert result is None
	avl_funds_instance.insert_table_db.assert_called_once()


@pytest.mark.parametrize(
	"list_slugs",
	[[1], [1, 2, 3], list(range(1, 6))],
)
def test_init_parametrized_slugs(
	sample_date: date,
	list_slugs: list,
) -> None:
	"""Test initialisation accepts different slug lists.

	Verifies
	--------
	- list_slugs attribute matches provided value.

	Parameters
	----------
	sample_date : date
		Fixed reference date.
	list_slugs : list
		Parametrised slug list.

	Returns
	-------
	None
	"""
	instance = MaisRetornoAvlFunds(date_ref=sample_date, list_slugs=list_slugs)
	assert instance.list_slugs == list_slugs


def test_reload_module() -> None:
	"""Test that the module can be reloaded without errors.

	Verifies
	--------
	- Fresh instance after reload has consistent attributes.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.registries.mais_retorno_avl_funds as mod

	original = MaisRetornoAvlFunds(date_ref=date(2025, 1, 1))
	importlib.reload(mod)
	reloaded = mod.MaisRetornoAvlFunds(date_ref=date(2025, 1, 1))
	assert reloaded.date_ref == original.date_ref
