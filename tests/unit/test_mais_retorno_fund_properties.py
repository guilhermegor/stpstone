"""Unit tests for MaisRetornoFundProperties ingestion class."""

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.ingestion.countries.br.registries.mais_retorno_fund_properties import (
	MaisRetornoFundProperties,
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
def fund_properties_instance(sample_date: date) -> MaisRetornoFundProperties:
	"""Return an initialised MaisRetornoFundProperties instance.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	MaisRetornoFundProperties
		Initialised instance.
	"""
	return MaisRetornoFundProperties(
		date_ref=sample_date,
		list_slugs=["aasl-fia"],
	)


@pytest.fixture
def mock_scraper() -> MagicMock:
	"""Return a mocked PlaywrightScraper with fund property element responses.

	Returns
	-------
	MagicMock
		Mocked PlaywrightScraper.
	"""
	from contextlib import contextmanager

	scraper = MagicMock()
	scraper.navigate.return_value = True
	scraper.get_element.return_value = {"text": "TEST VALUE"}
	scraper.get_element_attrb.return_value = None

	@contextmanager
	def _launch() -> None:
		"""Yield the scraper as a context manager."""
		yield scraper

	scraper.launch = _launch
	return scraper


@pytest.fixture
def sample_df() -> pd.DataFrame:
	"""Return a minimal valid DataFrame with fund property columns.

	Returns
	-------
	pd.DataFrame
		Minimal DataFrame.
	"""
	return pd.DataFrame(
		{
			"NICKNAME": ["TEST FUND"],
			"FUND_NAME": ["TEST FUND FULL NAME"],
			"STATUS": ["Aberto"],
			"BL_POOL_OPEN": [False],
			"BL_QUALIFIED_INVESTOR": [False],
			"BL_EXCLUSIVE_FUND": [False],
			"BL_LONG_TERM_TAXATION": [False],
			"BL_PENSION_FUND": [False],
			"CNPJ": ["12.345.678/0001-99"],
			"BENCHMARK": ["CDI"],
			"FUND_INITIAL_DATE": [None],
			"FUND_TYPE": ["FIA"],
			"ADMINISTRATOR": ["Admin"],
			"CLASS": ["Ações"],
			"SUBCLASS": ["Livre"],
			"MANAGER": ["Manager"],
			"RENTABILITY_LTM": [None],
			"AUM": ["R$ 1.000.000"],
			"AVERAGE_AUM_LTM": ["R$ 900.000"],
			"VOLATILITY_LTM": [None],
			"SHARPE_LTM": [None],
			"QTY_UNITHOLDERS": ["100"],
			"MANAGER_FULL_NAME": ["Gestora X"],
			"MANAGER_DIRECTOR": ["Director Y"],
			"MANAGER_EMAIL": ["email@x.com"],
			"MANAGER_SITE": ["www.x.com"],
			"MANAGER_TELEPHONE": ["(11) 1234-5678"],
			"MINIMUM_INVESTMENT_AMOUNT": [None],
			"MINIMUM_BALANCE_REQUIRED": [None],
			"ADMINISTRATION_FEE": [None],
			"ADMINISTRATION_FEE_MAX": [None],
			"PERFORMANCE_FEE": [None],
			"FUND_QUOTATION_PERIOD": [None],
			"FUND_SETTLEMENT_PERIOD": [None],
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
	instance = MaisRetornoFundProperties(
		date_ref=sample_date,
		list_slugs=["aasl-fia", "spx-falcon-2-fif-cic-acoes-rl"],
	)
	assert instance.date_ref == sample_date
	assert instance.list_slugs == ["aasl-fia", "spx-falcon-2-fif-cic-acoes-rl"]


def test_init_default_date() -> None:
	"""Test that default date_ref uses previous working day.

	Returns
	-------
	None
	"""
	from unittest.mock import patch

	with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
		instance = MaisRetornoFundProperties()
		assert instance.date_ref == date(2025, 1, 1)


def test_get_response_returns_none(fund_properties_instance: MaisRetornoFundProperties) -> None:
	"""Test get_response is a no-op.

	Parameters
	----------
	fund_properties_instance : MaisRetornoFundProperties
		Initialised instance.

	Returns
	-------
	None
	"""
	assert fund_properties_instance.get_response() is None


def test_parse_raw_file_returns_playwright_scraper(
	fund_properties_instance: MaisRetornoFundProperties,
	mocker: MockerFixture,
) -> None:
	"""Test parse_raw_file constructs PlaywrightScraper.

	Parameters
	----------
	fund_properties_instance : MaisRetornoFundProperties
		Initialised instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mock_cls = mocker.patch(
		"stpstone.ingestion.countries.br.registries.mais_retorno_fund_properties.PlaywrightScraper"
	)
	mock_inst = mock_cls.return_value
	result = fund_properties_instance.parse_raw_file()
	assert result is mock_inst


def test_transform_data_normal(
	fund_properties_instance: MaisRetornoFundProperties,
	mock_scraper: MagicMock,
	mocker: MockerFixture,
) -> None:
	"""Test transform_data returns a DataFrame.

	Parameters
	----------
	fund_properties_instance : MaisRetornoFundProperties
		Initialised instance.
	mock_scraper : MagicMock
		Mocked PlaywrightScraper.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch.object(
		fund_properties_instance,
		"_extract_fund_properties",
		return_value=[{"NICKNAME": "TEST FUND", "CNPJ": "12.345.678/0001-99"}],
	)
	df_ = fund_properties_instance.transform_data(scraper_playwright=mock_scraper)
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty
	assert "NICKNAME" in df_.columns


def test_transform_data_navigation_failure(
	fund_properties_instance: MaisRetornoFundProperties,
	mock_scraper: MagicMock,
) -> None:
	"""Test transform_data handles navigation failures gracefully.

	Parameters
	----------
	fund_properties_instance : MaisRetornoFundProperties
		Initialised instance.
	mock_scraper : MagicMock
		Mocked PlaywrightScraper with navigate=False.

	Returns
	-------
	None
	"""
	mock_scraper.navigate.return_value = False
	df_ = fund_properties_instance.transform_data(scraper_playwright=mock_scraper)
	assert df_.empty


def test_run_without_db(
	fund_properties_instance: MaisRetornoFundProperties,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run returns DataFrame without database session.

	Parameters
	----------
	fund_properties_instance : MaisRetornoFundProperties
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch.object(fund_properties_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(fund_properties_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(fund_properties_instance, "standardize_dataframe", return_value=sample_df)

	result = fund_properties_instance.run()
	assert isinstance(result, pd.DataFrame)


def test_run_with_db(
	fund_properties_instance: MaisRetornoFundProperties,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run calls insert_table_db and returns None with database session.

	Parameters
	----------
	fund_properties_instance : MaisRetornoFundProperties
		Initialised instance.
	sample_df : pd.DataFrame
		Minimal DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	fund_properties_instance.cls_db = MagicMock()
	mocker.patch.object(fund_properties_instance, "parse_raw_file", return_value=MagicMock())
	mocker.patch.object(fund_properties_instance, "transform_data", return_value=sample_df)
	mocker.patch.object(fund_properties_instance, "standardize_dataframe", return_value=sample_df)
	mocker.patch.object(fund_properties_instance, "insert_table_db")

	result = fund_properties_instance.run()
	assert result is None
	fund_properties_instance.insert_table_db.assert_called_once()


def test_reload_module() -> None:
	"""Test module reload preserves class behaviour.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.registries.mais_retorno_fund_properties as mod

	original = MaisRetornoFundProperties(date_ref=date(2025, 1, 1))
	importlib.reload(mod)
	reloaded = mod.MaisRetornoFundProperties(date_ref=date(2025, 1, 1))
	assert reloaded.date_ref == original.date_ref
