"""Unit tests for MaisRetornoStats ingestion class."""

from datetime import date
from math import nan
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.ingestion.countries.br.registries.mais_retorno_stats import (
	MaisRetornoStats,
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
def stats_instance(sample_date: date) -> MaisRetornoStats:
	"""Return an initialised MaisRetornoStats instance.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	MaisRetornoStats
		Initialised instance.
	"""
	return MaisRetornoStats(
		date_ref=sample_date,
		list_slugs=["aasl-fia"],
		instruments_class="fundo",
	)


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
	scraper.get_current_url.return_value = "https://maisretorno.com/fundo/aasl-fia"
	scraper.get_elements.return_value = [{"text": "Volatilidade"}] * 8

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
			"INSTRUMENT": ["AASL-FIA"],
			"STATISTIC": ["Volatilidade"],
			"YTD": [0.05],
			"MTD": [0.01],
			"LTM": [0.12],
			"L_24M": [0.10],
			"L_36M": [0.09],
			"L_48M": [0.08],
			"L_60M": [0.07],
			"SINCE_INCEPTION": [0.15],
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
	instance = MaisRetornoStats(
		date_ref=sample_date,
		list_slugs=["abcp11"],
		instruments_class="fii",
	)
	assert instance.date_ref == sample_date
	assert instance.list_slugs == ["abcp11"]
	assert instance.instruments_class == "fii"


def test_init_default_date() -> None:
	"""Test that default date_ref uses previous working day.

	Returns
	-------
	None
	"""
	from unittest.mock import patch

	with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
		instance = MaisRetornoStats()
		assert instance.date_ref == date(2025, 1, 1)


def test_get_response_returns_none(stats_instance: MaisRetornoStats) -> None:
	"""Test get_response is a no-op.

	Parameters
	----------
	stats_instance : MaisRetornoStats
		Initialised instance.

	Returns
	-------
	None
	"""
	assert stats_instance.get_response() is None


def test_convert_nums_dash_to_nan(stats_instance: MaisRetornoStats) -> None:
	"""Test _convert_nums converts dash strings to nan.

	Verifies
	--------
	- Dash values become nan in the output list.

	Parameters
	----------
	stats_instance : MaisRetornoStats
		Initialised instance.

	Returns
	-------
	None
	"""
	result = stats_instance._convert_nums(["-"])
	assert result[0] is nan or (isinstance(result[0], float) and result[0] != result[0])


def test_parse_raw_file_returns_playwright_scraper(
	stats_instance: MaisRetornoStats,
	mocker: MockerFixture,
) -> None:
	"""Test parse_raw_file constructs PlaywrightScraper.

	Parameters
	----------
	stats_instance : MaisRetornoStats
		Initialised instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mock_cls = mocker.patch(
		"stpstone.ingestion.countries.br.registries.mais_retorno_stats.PlaywrightScraper"
	)
	mock_inst = mock_cls.return_value
	result = stats_instance.parse_raw_file()
	assert result is mock_inst


def test_transform_data_navigation_failure(
	stats_instance: MaisRetornoStats,
	mock_scraper: MagicMock,
) -> None:
	"""Test transform_data returns empty DataFrame on navigation failure.

	Parameters
	----------
	stats_instance : MaisRetornoStats
		Initialised instance.
	mock_scraper : MagicMock
		Mocked PlaywrightScraper.

	Returns
	-------
	None
	"""
	mock_scraper.navigate.return_value = False
	df_ = stats_instance.transform_data(scraper_playwright=mock_scraper)
	assert df_.empty


def test_run_without_db(
	stats_instance: MaisRetornoStats,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run returns DataFrame without database session.

	Parameters
	----------
	stats_instance : MaisRetornoStats
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch.object(stats_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(stats_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(stats_instance, "standardize_dataframe", return_value=sample_df)

	result = stats_instance.run()
	assert isinstance(result, pd.DataFrame)


def test_run_with_db(
	stats_instance: MaisRetornoStats,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run calls insert_table_db and returns None with database session.

	Parameters
	----------
	stats_instance : MaisRetornoStats
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	stats_instance.cls_db = MagicMock()
	mocker.patch.object(stats_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(stats_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(stats_instance, "standardize_dataframe", return_value=sample_df)
	mocker.patch.object(stats_instance, "insert_table_db")

	result = stats_instance.run()
	assert result is None
	stats_instance.insert_table_db.assert_called_once()


def test_reload_module() -> None:
	"""Test module reload preserves class behaviour.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.registries.mais_retorno_stats as mod

	original = MaisRetornoStats(date_ref=date(2025, 1, 1))
	importlib.reload(mod)
	reloaded = mod.MaisRetornoStats(date_ref=date(2025, 1, 1))
	assert reloaded.date_ref == original.date_ref
