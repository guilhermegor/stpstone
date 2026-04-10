"""Unit tests for MaisRetornoHistoricalRentability ingestion class."""

from datetime import date
from math import nan
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.ingestion.countries.br.registries.mais_retorno_historical_rentability import (
	MaisRetornoHistoricalRentability,
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
def hist_rentability_instance(sample_date: date) -> MaisRetornoHistoricalRentability:
	"""Return an initialised MaisRetornoHistoricalRentability instance.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	MaisRetornoHistoricalRentability
		Initialised instance.
	"""
	return MaisRetornoHistoricalRentability(
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
	scraper.get_elements.return_value = [{"text": "2024"}]

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
	cols = [
		"YEAR", "INSTRUMENT", "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
		"JUL", "AUG", "SEP", "OCT", "NOV", "DEC", "YTD", "SINCE_INCEPTION",
	]
	return pd.DataFrame([[2024, "AASL-FIA"] + [0.01] * 14], columns=cols)


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
	instance = MaisRetornoHistoricalRentability(
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
		instance = MaisRetornoHistoricalRentability()
		assert instance.date_ref == date(2025, 1, 1)


def test_get_response_returns_none(
	hist_rentability_instance: MaisRetornoHistoricalRentability,
) -> None:
	"""Test get_response is a no-op.

	Parameters
	----------
	hist_rentability_instance : MaisRetornoHistoricalRentability
		Initialised instance.

	Returns
	-------
	None
	"""
	assert hist_rentability_instance.get_response() is None


def test_parse_raw_file_returns_playwright_scraper(
	hist_rentability_instance: MaisRetornoHistoricalRentability,
	mocker: MockerFixture,
) -> None:
	"""Test parse_raw_file constructs PlaywrightScraper.

	Parameters
	----------
	hist_rentability_instance : MaisRetornoHistoricalRentability
		Initialised instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mock_cls = mocker.patch(
		"stpstone.ingestion.countries.br.registries"
		".mais_retorno_historical_rentability.PlaywrightScraper"
	)
	mock_inst = mock_cls.return_value
	result = hist_rentability_instance.parse_raw_file()
	assert result is mock_inst


def test_convert_nums_with_dash(
	hist_rentability_instance: MaisRetornoHistoricalRentability,
) -> None:
	"""Test _convert_nums converts dash values to nan.

	Verifies
	--------
	- Dash strings become nan.

	Parameters
	----------
	hist_rentability_instance : MaisRetornoHistoricalRentability
		Initialised instance.

	Returns
	-------
	None
	"""
	result = hist_rentability_instance._convert_nums(["-"])
	assert result[0] is nan or (isinstance(result[0], float) and result[0] != result[0])


def test_convert_nums_with_instrument(
	hist_rentability_instance: MaisRetornoHistoricalRentability,
) -> None:
	"""Test _convert_nums labels p.p. values with instrument prefix.

	Parameters
	----------
	hist_rentability_instance : MaisRetornoHistoricalRentability
		Initialised instance.

	Returns
	-------
	None
	"""
	result = hist_rentability_instance._convert_nums(["1,5 p.p."], str_instrument="aasl-fia")
	assert isinstance(result[0], str)
	assert "AASL-FIA" in result[0]


def test_transform_data_navigation_failure(
	hist_rentability_instance: MaisRetornoHistoricalRentability,
	mock_scraper: MagicMock,
) -> None:
	"""Test transform_data returns empty DataFrame on navigation failure.

	Parameters
	----------
	hist_rentability_instance : MaisRetornoHistoricalRentability
		Initialised instance.
	mock_scraper : MagicMock
		Mocked PlaywrightScraper.

	Returns
	-------
	None
	"""
	mock_scraper.navigate.return_value = False
	df_ = hist_rentability_instance.transform_data(scraper_playwright=mock_scraper)
	assert df_.empty


def test_run_without_db(
	hist_rentability_instance: MaisRetornoHistoricalRentability,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run returns DataFrame without database session.

	Parameters
	----------
	hist_rentability_instance : MaisRetornoHistoricalRentability
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch.object(hist_rentability_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(hist_rentability_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(hist_rentability_instance, "standardize_dataframe", return_value=sample_df)

	result = hist_rentability_instance.run()
	assert isinstance(result, pd.DataFrame)


def test_run_with_db(
	hist_rentability_instance: MaisRetornoHistoricalRentability,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run calls insert_table_db and returns None with database session.

	Parameters
	----------
	hist_rentability_instance : MaisRetornoHistoricalRentability
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	hist_rentability_instance.cls_db = MagicMock()
	mocker.patch.object(hist_rentability_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(hist_rentability_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(
		hist_rentability_instance, "standardize_dataframe", return_value=sample_df
	)
	mocker.patch.object(hist_rentability_instance, "insert_table_db")

	result = hist_rentability_instance.run()
	assert result is None
	hist_rentability_instance.insert_table_db.assert_called_once()


def test_reload_module() -> None:
	"""Test module reload preserves class behaviour.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.registries.mais_retorno_historical_rentability as mod

	original = MaisRetornoHistoricalRentability(date_ref=date(2025, 1, 1))
	importlib.reload(mod)
	reloaded = mod.MaisRetornoHistoricalRentability(date_ref=date(2025, 1, 1))
	assert reloaded.date_ref == original.date_ref
