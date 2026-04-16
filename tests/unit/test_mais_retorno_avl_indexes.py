"""Unit tests for MaisRetornoAvlIndexes ingestion class."""

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.ingestion.countries.br.registries.mais_retorno_avl_indexes import (
	MaisRetornoAvlIndexes,
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
def avl_indexes_instance(sample_date: date) -> MaisRetornoAvlIndexes:
	"""Return an initialised MaisRetornoAvlIndexes instance.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	MaisRetornoAvlIndexes
		Initialised instance.
	"""
	return MaisRetornoAvlIndexes(date_ref=sample_date, list_slugs=[1])


@pytest.fixture
def mock_scraper() -> MagicMock:
	"""Return a mocked PlaywrightScraper.

	Returns
	-------
	MagicMock
		Mocked PlaywrightScraper.
	"""
	from contextlib import contextmanager

	scraper = MagicMock()
	scraper.navigate.return_value = True
	scraper.selector_exists.side_effect = [True, False]
	scraper.get_element.return_value = {"text": "CDI"}
	scraper.get_element_attrb.return_value = "indice/cdi"

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
		Minimal DataFrame.
	"""
	return pd.DataFrame(
		{
			"INDEX_NAME": ["CDI"],
			"INDEX_CODE": ["cdi"],
			"URL_INDEX": ["https://maisretorno.com/indice/cdi"],
			"STATUS": ["Ativo"],
			"PAGE_POSITION": [1],
		}
	)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialisation with explicit inputs.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	None
	"""
	instance = MaisRetornoAvlIndexes(date_ref=sample_date, list_slugs=[1, 2])
	assert instance.date_ref == sample_date
	assert instance.list_slugs == [1, 2]


def test_init_default_date() -> None:
	"""Test that default date_ref is set to previous working day.

	Returns
	-------
	None
	"""
	from unittest.mock import patch

	with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
		instance = MaisRetornoAvlIndexes()
		assert instance.date_ref == date(2025, 1, 1)


def test_get_response_returns_none(avl_indexes_instance: MaisRetornoAvlIndexes) -> None:
	"""Test get_response is a no-op.

	Parameters
	----------
	avl_indexes_instance : MaisRetornoAvlIndexes
		Initialised instance.

	Returns
	-------
	None
	"""
	assert avl_indexes_instance.get_response() is None


def test_parse_raw_file_returns_playwright_scraper(
	avl_indexes_instance: MaisRetornoAvlIndexes,
	mocker: MockerFixture,
) -> None:
	"""Test parse_raw_file constructs PlaywrightScraper.

	Parameters
	----------
	avl_indexes_instance : MaisRetornoAvlIndexes
		Initialised instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mock_cls = mocker.patch(
		"stpstone.ingestion.countries.br.registries.mais_retorno_avl_indexes.PlaywrightScraper"
	)
	mock_inst = mock_cls.return_value
	result = avl_indexes_instance.parse_raw_file()
	assert result is mock_inst


def test_transform_data_normal(
	avl_indexes_instance: MaisRetornoAvlIndexes,
	mock_scraper: MagicMock,
) -> None:
	"""Test transform_data returns a DataFrame with expected columns.

	Parameters
	----------
	avl_indexes_instance : MaisRetornoAvlIndexes
		Initialised instance.
	mock_scraper : MagicMock
		Mocked PlaywrightScraper.

	Returns
	-------
	None
	"""
	df_ = avl_indexes_instance.transform_data(scraper_playwright=mock_scraper)
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty
	assert "INDEX_NAME" in df_.columns
	assert "PAGE_POSITION" in df_.columns


def test_transform_data_navigation_failure(
	avl_indexes_instance: MaisRetornoAvlIndexes,
	mock_scraper: MagicMock,
) -> None:
	"""Test transform_data returns empty DataFrame on navigation failure.

	Parameters
	----------
	avl_indexes_instance : MaisRetornoAvlIndexes
		Initialised instance.
	mock_scraper : MagicMock
		Mocked PlaywrightScraper with navigate=False.

	Returns
	-------
	None
	"""
	mock_scraper.navigate.return_value = False
	df_ = avl_indexes_instance.transform_data(scraper_playwright=mock_scraper)
	assert df_.empty


def test_run_without_db(
	avl_indexes_instance: MaisRetornoAvlIndexes,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run returns DataFrame without database session.

	Parameters
	----------
	avl_indexes_instance : MaisRetornoAvlIndexes
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch.object(avl_indexes_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(avl_indexes_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(avl_indexes_instance, "standardize_dataframe", return_value=sample_df)

	result = avl_indexes_instance.run()
	assert isinstance(result, pd.DataFrame)


def test_run_with_db(
	avl_indexes_instance: MaisRetornoAvlIndexes,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run calls insert_table_db and returns None with database session.

	Parameters
	----------
	avl_indexes_instance : MaisRetornoAvlIndexes
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	avl_indexes_instance.cls_db = MagicMock()
	mocker.patch.object(avl_indexes_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(avl_indexes_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(avl_indexes_instance, "standardize_dataframe", return_value=sample_df)
	mocker.patch.object(avl_indexes_instance, "insert_table_db")

	result = avl_indexes_instance.run()
	assert result is None
	avl_indexes_instance.insert_table_db.assert_called_once()


def test_reload_module() -> None:
	"""Test module reload preserves class behaviour.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.registries.mais_retorno_avl_indexes as mod

	original = MaisRetornoAvlIndexes(date_ref=date(2025, 1, 1))
	importlib.reload(mod)
	reloaded = mod.MaisRetornoAvlIndexes(date_ref=date(2025, 1, 1))
	assert reloaded.date_ref == original.date_ref
