"""Unit tests for MaisRetornoConsistency ingestion class."""

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.ingestion.countries.br.registries.mais_retorno_consistency import (
	MaisRetornoConsistency,
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
def consistency_instance(sample_date: date) -> MaisRetornoConsistency:
	"""Return an initialised MaisRetornoConsistency instance.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	MaisRetornoConsistency
		Initialised instance.
	"""
	return MaisRetornoConsistency(
		date_ref=sample_date,
		list_slugs=["aasl-fia"],
		instruments_class="fundo",
	)


@pytest.fixture
def mock_scraper() -> MagicMock:
	"""Return a mocked PlaywrightScraper with consistency element responses.

	Returns
	-------
	MagicMock
		Mocked PlaywrightScraper.
	"""
	from contextlib import contextmanager

	scraper = MagicMock()
	scraper.navigate.return_value = True
	scraper.get_current_url.return_value = "https://maisretorno.com/fundo/aasl-fia"
	scraper.get_element.return_value = {"text": "10"}

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
			"POSITIVE_MONTHS": [36],
			"NEGATIVE_MONTHS": [12],
			"GREATEST_RETURN": [0.05],
			"LEAST_RETURN": [-0.03],
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
	instance = MaisRetornoConsistency(
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
		instance = MaisRetornoConsistency()
		assert instance.date_ref == date(2025, 1, 1)


def test_get_response_returns_none(consistency_instance: MaisRetornoConsistency) -> None:
	"""Test get_response is a no-op.

	Parameters
	----------
	consistency_instance : MaisRetornoConsistency
		Initialised instance.

	Returns
	-------
	None
	"""
	assert consistency_instance.get_response() is None


def test_parse_raw_file_returns_playwright_scraper(
	consistency_instance: MaisRetornoConsistency,
	mocker: MockerFixture,
) -> None:
	"""Test parse_raw_file constructs PlaywrightScraper.

	Parameters
	----------
	consistency_instance : MaisRetornoConsistency
		Initialised instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mock_cls = mocker.patch(
		"stpstone.ingestion.countries.br.registries"
		".mais_retorno_consistency.PlaywrightScraper"
	)
	mock_inst = mock_cls.return_value
	result = consistency_instance.parse_raw_file()
	assert result is mock_inst


def test_transform_data_normal(
	consistency_instance: MaisRetornoConsistency,
	mock_scraper: MagicMock,
) -> None:
	"""Test transform_data returns a DataFrame with consistency columns.

	Parameters
	----------
	consistency_instance : MaisRetornoConsistency
		Initialised instance.
	mock_scraper : MagicMock
		Mocked PlaywrightScraper.

	Returns
	-------
	None
	"""
	df_ = consistency_instance.transform_data(scraper_playwright=mock_scraper)
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty
	assert "INSTRUMENT" in df_.columns
	assert "POSITIVE_MONTHS" in df_.columns


def test_transform_data_navigation_failure(
	consistency_instance: MaisRetornoConsistency,
	mock_scraper: MagicMock,
) -> None:
	"""Test transform_data returns empty DataFrame on navigation failure.

	Parameters
	----------
	consistency_instance : MaisRetornoConsistency
		Initialised instance.
	mock_scraper : MagicMock
		Mocked PlaywrightScraper.

	Returns
	-------
	None
	"""
	mock_scraper.navigate.return_value = False
	df_ = consistency_instance.transform_data(scraper_playwright=mock_scraper)
	assert df_.empty


def test_run_without_db(
	consistency_instance: MaisRetornoConsistency,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run returns DataFrame without database session.

	Parameters
	----------
	consistency_instance : MaisRetornoConsistency
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch.object(consistency_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(consistency_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(consistency_instance, "standardize_dataframe", return_value=sample_df)

	result = consistency_instance.run()
	assert isinstance(result, pd.DataFrame)


def test_run_with_db(
	consistency_instance: MaisRetornoConsistency,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run calls insert_table_db and returns None with database session.

	Parameters
	----------
	consistency_instance : MaisRetornoConsistency
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	consistency_instance.cls_db = MagicMock()
	mocker.patch.object(consistency_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(consistency_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(consistency_instance, "standardize_dataframe", return_value=sample_df)
	mocker.patch.object(consistency_instance, "insert_table_db")

	result = consistency_instance.run()
	assert result is None
	consistency_instance.insert_table_db.assert_called_once()


def test_extract_consistency_with_none_elements(
	consistency_instance: MaisRetornoConsistency,
) -> None:
	"""Test _extract_consistency handles None element responses.

	Verifies
	--------
	- Method returns a list with one dict even when scraper returns None.

	Parameters
	----------
	consistency_instance : MaisRetornoConsistency
		Initialised instance.

	Returns
	-------
	None
	"""
	mock_scraper = MagicMock()
	mock_scraper.get_element.return_value = None
	result = consistency_instance._extract_consistency(mock_scraper, "AASL-FIA")
	assert len(result) == 1
	assert result[0]["INSTRUMENT"] == "AASL-FIA"


def test_reload_module() -> None:
	"""Test module reload preserves class behaviour.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.registries.mais_retorno_consistency as mod

	original = MaisRetornoConsistency(date_ref=date(2025, 1, 1))
	importlib.reload(mod)
	reloaded = mod.MaisRetornoConsistency(date_ref=date(2025, 1, 1))
	assert reloaded.date_ref == original.date_ref
