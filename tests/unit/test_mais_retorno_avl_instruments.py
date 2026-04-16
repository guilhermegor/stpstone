"""Unit tests for MaisRetornoAvlInstruments ingestion class."""

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.ingestion.countries.br.registries.mais_retorno_avl_instruments import (
	MaisRetornoAvlInstruments,
)
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date() -> date:
	"""Provide a fixed reference date.

	Returns
	-------
	date
		Fixed date 2025-01-01.
	"""
	return date(2025, 1, 1)


@pytest.fixture
def avl_instruments_instance(sample_date: date) -> MaisRetornoAvlInstruments:
	"""Return an initialised MaisRetornoAvlInstruments instance.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	MaisRetornoAvlInstruments
		Initialised instance.
	"""
	return MaisRetornoAvlInstruments(
		date_ref=sample_date,
		list_slugs=[1],
		instruments_class="lista-fi-infra",
	)


@pytest.fixture
def mock_scraper() -> MagicMock:
	"""Return a mocked PlaywrightScraper.

	Returns
	-------
	MagicMock
		Mocked PlaywrightScraper instance.
	"""
	from contextlib import contextmanager

	scraper = MagicMock()
	scraper.navigate.return_value = True
	scraper.selector_exists.side_effect = [True, False]
	scraper.get_element.return_value = {"text": "TEST"}
	scraper.get_element_attrb.return_value = "lista-fi-infra/bidb11"

	@contextmanager
	def _launch() -> None:
		"""Yield the scraper as a context manager."""
		yield scraper

	scraper.launch = _launch
	return scraper


@pytest.fixture
def sample_df() -> pd.DataFrame:
	"""Return a minimal valid DataFrame.

	Returns
	-------
	pd.DataFrame
		Minimal DataFrame with expected columns.
	"""
	return pd.DataFrame(
		{
			"CNPJ": ["12.345.678/0001-99"],
			"INSTRUMENT_CODE": ["BIDB11"],
			"INSTRUMENT_NAME": ["Test Instrument"],
			"URL_INSTRUMENT": ["https://maisretorno.com/lista-fi-infra/bidb11"],
			"SEGMENT": ["FI-Infra"],
			"SECTOR": ["Infraestrutura"],
			"INSTRUMENTS_CLASS": ["lista-fi-infra"],
			"PAGE_POSITION": [1],
		}
	)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialisation stores attributes correctly.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	None
	"""
	instance = MaisRetornoAvlInstruments(
		date_ref=sample_date,
		list_slugs=[1, 2],
		instruments_class="lista-fip",
	)
	assert instance.date_ref == sample_date
	assert instance.list_slugs == [1, 2]
	assert instance.instruments_class == "lista-fip"


def test_init_default_date() -> None:
	"""Test default date_ref uses previous working day.

	Returns
	-------
	None
	"""
	from unittest.mock import patch

	with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
		instance = MaisRetornoAvlInstruments()
		assert instance.date_ref == date(2025, 1, 1)


def test_get_response_returns_none(
	avl_instruments_instance: MaisRetornoAvlInstruments,
) -> None:
	"""Test get_response is a no-op returning None.

	Parameters
	----------
	avl_instruments_instance : MaisRetornoAvlInstruments
		Initialised instance.

	Returns
	-------
	None
	"""
	assert avl_instruments_instance.get_response() is None


def test_parse_raw_file_returns_playwright_scraper(
	avl_instruments_instance: MaisRetornoAvlInstruments,
	mocker: MockerFixture,
) -> None:
	"""Test parse_raw_file constructs a PlaywrightScraper with correct params.

	Parameters
	----------
	avl_instruments_instance : MaisRetornoAvlInstruments
		Initialised instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mock_cls = mocker.patch(
		"stpstone.ingestion.countries.br.registries.mais_retorno_avl_instruments.PlaywrightScraper"
	)
	mock_inst = mock_cls.return_value
	result = avl_instruments_instance.parse_raw_file()
	mock_cls.assert_called_once_with(
		bool_headless=avl_instruments_instance.bool_headless,
		int_default_timeout=avl_instruments_instance.int_wait_load_seconds * 1_000,
		logger=avl_instruments_instance.logger,
	)
	assert result is mock_inst


def test_transform_data_normal(
	avl_instruments_instance: MaisRetornoAvlInstruments,
	mock_scraper: MagicMock,
) -> None:
	"""Test transform_data returns a DataFrame with expected columns.

	Parameters
	----------
	avl_instruments_instance : MaisRetornoAvlInstruments
		Initialised instance.
	mock_scraper : MagicMock
		Mocked PlaywrightScraper.

	Returns
	-------
	None
	"""
	df_ = avl_instruments_instance.transform_data(scraper_playwright=mock_scraper)
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty
	assert "INSTRUMENT_CODE" in df_.columns
	assert "INSTRUMENTS_CLASS" in df_.columns


def test_transform_data_navigation_failure(
	avl_instruments_instance: MaisRetornoAvlInstruments,
	mock_scraper: MagicMock,
) -> None:
	"""Test transform_data handles failed navigation gracefully.

	Parameters
	----------
	avl_instruments_instance : MaisRetornoAvlInstruments
		Initialised instance.
	mock_scraper : MagicMock
		Mocked PlaywrightScraper.

	Returns
	-------
	None
	"""
	mock_scraper.navigate.return_value = False
	df_ = avl_instruments_instance.transform_data(scraper_playwright=mock_scraper)
	assert df_.empty


def test_run_without_db(
	avl_instruments_instance: MaisRetornoAvlInstruments,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run returns DataFrame when no database session is provided.

	Parameters
	----------
	avl_instruments_instance : MaisRetornoAvlInstruments
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch.object(avl_instruments_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(avl_instruments_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(avl_instruments_instance, "standardize_dataframe", return_value=sample_df)

	result = avl_instruments_instance.run()
	assert isinstance(result, pd.DataFrame)


def test_run_with_db(
	avl_instruments_instance: MaisRetornoAvlInstruments,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run calls insert_table_db and returns None when cls_db is set.

	Parameters
	----------
	avl_instruments_instance : MaisRetornoAvlInstruments
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	avl_instruments_instance.cls_db = MagicMock()
	mocker.patch.object(avl_instruments_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(avl_instruments_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(avl_instruments_instance, "standardize_dataframe", return_value=sample_df)
	mocker.patch.object(avl_instruments_instance, "insert_table_db")

	result = avl_instruments_instance.run()
	assert result is None
	avl_instruments_instance.insert_table_db.assert_called_once()


def test_reload_module() -> None:
	"""Test module reload preserves class behaviour.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.registries.mais_retorno_avl_instruments as mod

	original = MaisRetornoAvlInstruments(date_ref=date(2025, 1, 1))
	importlib.reload(mod)
	reloaded = mod.MaisRetornoAvlInstruments(date_ref=date(2025, 1, 1))
	assert reloaded.date_ref == original.date_ref
