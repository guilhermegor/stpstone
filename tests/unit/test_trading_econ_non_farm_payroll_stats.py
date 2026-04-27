"""Unit tests for TradingEconNonFarmPayrollStats class."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.countries.ww.macroeconomics.trading_econ_non_farm_payroll_stats import (
	TradingEconNonFarmPayrollStats,
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
	"""Provide a fixed date for testing.

	Returns
	-------
	date
		Fixed reference date.
	"""
	return date(2025, 1, 1)


@pytest.fixture
def instance(sample_date: date) -> TradingEconNonFarmPayrollStats:
	"""Provide a TradingEconNonFarmPayrollStats instance for testing.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	TradingEconNonFarmPayrollStats
		Initialized instance.
	"""
	return TradingEconNonFarmPayrollStats(date_ref=sample_date)


@pytest.fixture
def mock_web_driver() -> MagicMock:
	"""Provide a mocked Selenium WebDriver.

	Returns
	-------
	MagicMock
		Mocked SeleniumWebDriver instance.
	"""
	driver = MagicMock(spec=SeleniumWebDriver)
	driver.quit = MagicMock()
	return driver


@pytest.fixture
def sample_df() -> pd.DataFrame:
	"""Provide a minimal sample DataFrame matching the class schema.

	Returns
	-------
	pd.DataFrame
		Sample DataFrame.
	"""
	return pd.DataFrame(
		{
			"N_A": ["NFP"],
			"ACTUAL": [256.0],
			"PREVIOUS": [212.0],
			"HIGHEST": [1084.0],
			"LOWEST": [-20500.0],
			"DATES": ["1939-2024"],
			"UNIT": ["Thousand"],
			"FREQUENCY": ["Monthly"],
			"N_A_2": ["BLS"],
		}
	)


# --------------------------
# Tests
# --------------------------
def test_init_default_date_ref() -> None:
	"""Test that date_ref is a date when not provided.

	Returns
	-------
	None
	"""
	with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
		obj = TradingEconNonFarmPayrollStats()
	assert isinstance(obj.date_ref, date)


def test_init_sets_expected_attributes(instance: TradingEconNonFarmPayrollStats) -> None:
	"""Test initialization sets expected helper attributes.

	Parameters
	----------
	instance : TradingEconNonFarmPayrollStats
		Ingestion instance.

	Returns
	-------
	None
	"""
	assert isinstance(instance.cls_dates_current, DatesCurrent)
	assert isinstance(instance.cls_dates_br, DatesBRAnbima)
	assert isinstance(instance.cls_create_log, CreateLog)
	assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
	assert "united-states/non-farm-payrolls" in instance.url


def test_run_returns_dataframe(
	instance: TradingEconNonFarmPayrollStats,
	mock_web_driver: MagicMock,
	sample_df: pd.DataFrame,
) -> None:
	"""Test that run returns a non-empty DataFrame when no cls_db is set.

	Parameters
	----------
	instance : TradingEconNonFarmPayrollStats
		Ingestion instance.
	mock_web_driver : MagicMock
		Mocked Selenium WebDriver.
	sample_df : pd.DataFrame
		Sample DataFrame.

	Returns
	-------
	None
	"""
	with (
		patch.object(instance, "get_response", return_value=mock_web_driver),
		patch.object(instance, "transform_data", return_value=sample_df),
		patch.object(instance, "standardize_dataframe", return_value=sample_df),
	):
		result = instance.run()
	assert isinstance(result, pd.DataFrame)
	assert not result.empty


def test_run_with_cls_db_inserts(
	instance: TradingEconNonFarmPayrollStats,
	mock_web_driver: MagicMock,
	sample_df: pd.DataFrame,
) -> None:
	"""Test that run calls insert_table_db and returns None when cls_db is set.

	Parameters
	----------
	instance : TradingEconNonFarmPayrollStats
		Ingestion instance.
	mock_web_driver : MagicMock
		Mocked Selenium WebDriver.
	sample_df : pd.DataFrame
		Sample DataFrame.

	Returns
	-------
	None
	"""
	instance.cls_db = MagicMock()
	with (
		patch.object(instance, "get_response", return_value=mock_web_driver),
		patch.object(instance, "transform_data", return_value=sample_df),
		patch.object(instance, "standardize_dataframe", return_value=sample_df),
		patch.object(instance, "insert_table_db") as mock_insert,
	):
		result = instance.run()
	assert result is None
	mock_insert.assert_called_once()


def test_get_response_returns_webdriver(
	instance: TradingEconNonFarmPayrollStats,
	mock_web_driver: MagicMock,
) -> None:
	"""Test that get_response returns the Selenium WebDriver.

	Parameters
	----------
	instance : TradingEconNonFarmPayrollStats
		Ingestion instance.
	mock_web_driver : MagicMock
		Mocked Selenium WebDriver.

	Returns
	-------
	None
	"""
	mock_selenium_cls = MagicMock()
	mock_selenium_cls.get_web_driver.return_value = mock_web_driver
	with patch(
		"stpstone.ingestion.countries.ww.macroeconomics._trading_econ_base.SeleniumWD",
		return_value=mock_selenium_cls,
	):
		result = instance.get_response()
	assert result is mock_web_driver


def test_transform_data_quits_driver(
	instance: TradingEconNonFarmPayrollStats,
	mock_web_driver: MagicMock,
) -> None:
	"""Test that transform_data calls web_driver.quit() even on success.

	Parameters
	----------
	instance : TradingEconNonFarmPayrollStats
		Ingestion instance.
	mock_web_driver : MagicMock
		Mocked Selenium WebDriver.

	Returns
	-------
	None
	"""
	instance._cls_selenium = MagicMock()
	fixed_texts = [
		"NFP",
		"256.0",
		"212.0",
		"1084.0",
		"-20500.0",
		"1939-2024",
		"Thousand",
		"Monthly",
		"BLS",
	]
	with (
		patch.object(
			TradingEconNonFarmPayrollStats,
			"_list_web_elements",
			return_value=fixed_texts,
		),
		patch(
			"stpstone.ingestion.countries.ww.macroeconomics._trading_econ_base.HandlingDicts"
		) as mock_hd,
	):
		mock_hd.return_value.pair_headers_with_data.return_value = [
			{
				"N_A": "NFP",
				"ACTUAL": 256.0,
				"PREVIOUS": 212.0,
				"HIGHEST": 1084.0,
				"LOWEST": -20500.0,
				"DATES": "1939-2024",
				"UNIT": "Thousand",
				"FREQUENCY": "Monthly",
				"N_A_2": "BLS",
			}
		]
		instance.transform_data(resp_req=mock_web_driver)
	mock_web_driver.quit.assert_called_once()
