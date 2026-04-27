"""Unit tests for TradingEconWagesUsa class."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.countries.ww.macroeconomics.trading_econ_wages_usa import (
	TradingEconWagesUsa,
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
def instance(sample_date: date) -> TradingEconWagesUsa:
	"""Provide a TradingEconWagesUsa instance for testing.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	TradingEconWagesUsa
		Initialized instance.
	"""
	return TradingEconWagesUsa(date_ref=sample_date)


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
			"RELATED": ["Average Hourly Earnings"],
			"LAST": [34.5],
			"PREVIOUS": [34.3],
			"UNIT": ["USD/Hour"],
			"REFERENCE": ["Dec"],
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
		obj = TradingEconWagesUsa()
	assert isinstance(obj.date_ref, date)


def test_init_sets_expected_attributes(instance: TradingEconWagesUsa) -> None:
	"""Test initialization sets expected helper attributes.

	Parameters
	----------
	instance : TradingEconWagesUsa
		Ingestion instance.

	Returns
	-------
	None
	"""
	assert isinstance(instance.cls_dates_current, DatesCurrent)
	assert isinstance(instance.cls_dates_br, DatesBRAnbima)
	assert isinstance(instance.cls_create_log, CreateLog)
	assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
	assert "united-states/wages" in instance.url


def test_run_returns_dataframe(
	instance: TradingEconWagesUsa,
	mock_web_driver: MagicMock,
	sample_df: pd.DataFrame,
) -> None:
	"""Test that run returns a non-empty DataFrame when no cls_db is set.

	Parameters
	----------
	instance : TradingEconWagesUsa
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
	instance: TradingEconWagesUsa,
	mock_web_driver: MagicMock,
	sample_df: pd.DataFrame,
) -> None:
	"""Test that run calls insert_table_db and returns None when cls_db is set.

	Parameters
	----------
	instance : TradingEconWagesUsa
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
	instance: TradingEconWagesUsa,
	mock_web_driver: MagicMock,
) -> None:
	"""Test that get_response returns the Selenium WebDriver.

	Parameters
	----------
	instance : TradingEconWagesUsa
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
	instance: TradingEconWagesUsa,
	mock_web_driver: MagicMock,
) -> None:
	"""Test that transform_data calls web_driver.quit() even on success.

	Parameters
	----------
	instance : TradingEconWagesUsa
		Ingestion instance.
	mock_web_driver : MagicMock
		Mocked Selenium WebDriver.

	Returns
	-------
	None
	"""
	instance._cls_selenium = MagicMock()
	fixed_texts = ["Average Hourly Earnings", "34.5", "34.3", "USD/Hour", "Dec"]
	with (
		patch.object(
			TradingEconWagesUsa,
			"_list_web_elements",
			return_value=fixed_texts,
		),
		patch(
			"stpstone.ingestion.countries.ww.macroeconomics._trading_econ_base.HandlingDicts"
		) as mock_hd,
	):
		mock_hd.return_value.pair_headers_with_data.return_value = [
			{
				"RELATED": "Average Hourly Earnings",
				"LAST": 34.5,
				"PREVIOUS": 34.3,
				"UNIT": "USD/Hour",
				"REFERENCE": "Dec",
			}
		]
		instance.transform_data(resp_req=mock_web_driver)
	mock_web_driver.quit.assert_called_once()
