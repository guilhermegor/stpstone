"""Unit tests for AnbimaIPCAForecastsLTM ingestion module.

Tests initialization, run (with and without db), transform_data,
_restructure_table_with_missing_columns, and _debug_flat_list_structure,
covering normal operations, edge cases, and type validation.
"""

from datetime import date
from logging import Logger
import sys
from typing import Any, Optional, Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Session

from stpstone.ingestion.countries.br.macroeconomics._anbima_ipca_core import AnbimaIPCACore
from stpstone.ingestion.countries.br.macroeconomics.anbima_ipca_forecasts_ltm import (
	AnbimaIPCAForecastsLTM,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
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
def sample_flat_list() -> list[str]:
	"""Sample flat list data for testing _restructure_table_with_missing_columns.

	Returns
	-------
	list[str]
		Sample list simulating table data.
	"""
	return [
		"Janeiro de 2025",
		"01/01/25",
		"2.5",
		"31/01/25",
		"2.3",
		"Fevereiro de 2025",
		"01/02/25",
		"2.7",
		"28/02/25",
		"",
		"01/03/25",
		"2.8",
		"31/03/25",
		"2.9",
	]


@pytest.fixture
def expected_columns_ltm() -> list[str]:
	"""Return expected columns for AnbimaIPCAForecastsLTM.

	Returns
	-------
	list[str]
		List of expected column names.
	"""
	return ["MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE", "IPCA_EFETIVO_PCT"]


@pytest.fixture
def anbima_ipca_ltm(
	mock_logger: Logger,
	mock_session: Session,
) -> AnbimaIPCAForecastsLTM:
	"""Fixture providing AnbimaIPCAForecastsLTM instance.

	Parameters
	----------
	mock_logger : Logger
		Mocked logger instance.
	mock_session : Session
		Mocked database session.

	Returns
	-------
	AnbimaIPCAForecastsLTM
		Initialized AnbimaIPCAForecastsLTM instance.
	"""
	return AnbimaIPCAForecastsLTM(logger=mock_logger, cls_db=mock_session)


# --------------------------
# Tests for AnbimaIPCAForecastsLTM
# --------------------------
class TestAnbimaIPCAForecastsLTM:
	"""Test cases for AnbimaIPCAForecastsLTM class."""

	def test_init(self, anbima_ipca_ltm: AnbimaIPCAForecastsLTM) -> None:
		"""Test initialization of AnbimaIPCAForecastsLTM.

		Verifies
		--------
		- Inherits from AnbimaIPCACore correctly.
		- Sets correct URL.

		Parameters
		----------
		anbima_ipca_ltm : AnbimaIPCAForecastsLTM
			AnbimaIPCAForecastsLTM instance.

		Returns
		-------
		None
		"""
		assert isinstance(anbima_ipca_ltm, AnbimaIPCACore)
		assert anbima_ipca_ltm.url == (
			"https://www.anbima.com.br/pt_br/informar/estatisticas/"
			"precos-e-indices/projecao-de-inflacao-gp-m.htm"
		)

	def test_run_returns_dataframe(
		self,
		anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
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
		anbima_ipca_ltm : AnbimaIPCAForecastsLTM
			AnbimaIPCAForecastsLTM instance.
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
				"IPCA_EFETIVO_PCT": [2.3],
			}
		)
		mocker.patch.object(AnbimaIPCACore, "parse_raw_file", return_value=mock_playwright_scraper)
		mocker.patch.object(AnbimaIPCAForecastsLTM, "transform_data", return_value=mock_df)
		mocker.patch.object(AnbimaIPCACore, "standardize_dataframe", return_value=mock_df)

		anbima_ipca_ltm.cls_db = None
		result = anbima_ipca_ltm.run()

		assert result.equals(mock_df)
		assert result.columns.tolist() == [
			"MES_COLETA",
			"DATA",
			"PROJECAO_PCT",
			"DATA_VALIDADE",
			"IPCA_EFETIVO_PCT",
		]

	def test_run_with_db_calls_insert(
		self,
		anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
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
		anbima_ipca_ltm : AnbimaIPCAForecastsLTM
			AnbimaIPCAForecastsLTM instance.
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
		mocker.patch.object(AnbimaIPCAForecastsLTM, "transform_data", return_value=mock_df)
		mocker.patch.object(AnbimaIPCACore, "standardize_dataframe", return_value=mock_df)
		mock_insert = mocker.patch.object(AnbimaIPCACore, "insert_table_db")

		result = anbima_ipca_ltm.run()

		assert result is None
		mock_insert.assert_called_once()
		call_kwargs = mock_insert.call_args.kwargs
		assert call_kwargs["str_table_name"] == "br_anbima_ipca_forecasts_ltm"

	def test_transform_data(
		self,
		anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
		mock_playwright_scraper: PlaywrightScraper,
		mocker: MockerFixture,
	) -> None:
		"""Test transform_data method for last twelve months forecasts.

		Verifies
		--------
		- Correctly transforms scraped data into DataFrame.
		- Calls _restructure_table_with_missing_columns.
		- Applies forward fill for IPCA_EFETIVO_PCT.

		Parameters
		----------
		anbima_ipca_ltm : AnbimaIPCAForecastsLTM
			AnbimaIPCAForecastsLTM instance.
		mock_playwright_scraper : PlaywrightScraper
			Mocked PlaywrightScraper instance.
		mocker : MockerFixture
			Pytest mocker for patching dependencies.

		Returns
		-------
		None
		"""
		mock_playwright_scraper.get_elements.return_value = [
			{"text": "Janeiro 2025"},
			{"text": "01/01/25"},
			{"text": "2,5"},
			{"text": "31/01/25"},
			{"text": "2,3"},
		]
		mock_restructure = mocker.patch.object(
			AnbimaIPCAForecastsLTM,
			"_restructure_table_with_missing_columns",
			return_value=[
				{
					"MES_COLETA": "Janeiro 2025",
					"DATA": "01/01/25",
					"PROJECAO_PCT": "2.5",
					"DATA_VALIDADE": "31/01/25",
					"IPCA_EFETIVO_PCT": "2.3",
				}
			],
		)

		result = anbima_ipca_ltm.transform_data(mock_playwright_scraper)

		assert isinstance(result, pd.DataFrame)
		assert result.columns.tolist() == [
			"MES_COLETA",
			"DATA",
			"PROJECAO_PCT",
			"DATA_VALIDADE",
			"IPCA_EFETIVO_PCT",
		]
		mock_restructure.assert_called_once()
		assert result.loc[0, "IPCA_EFETIVO_PCT"] == "2.3"

	@pytest.mark.parametrize("timeout", [None, 10, 10.5, (10.0, 20.0), (10, 20)])
	def test_run_timeout_validation(
		self,
		anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
		timeout: Optional[Union[int, float, tuple]],
	) -> None:
		"""Test run method with various timeout values.

		Verifies
		--------
		- Accepts valid timeout values (int, float, tuple of int/float).

		Parameters
		----------
		anbima_ipca_ltm : AnbimaIPCAForecastsLTM
			AnbimaIPCAForecastsLTM instance.
		timeout : Optional[Union[int, float, tuple]]
			Timeout value to test.

		Returns
		-------
		None
		"""
		anbima_ipca_ltm.cls_db = None
		with (
			patch.object(AnbimaIPCACore, "parse_raw_file") as mock_parse,
			patch.object(AnbimaIPCAForecastsLTM, "transform_data"),
			patch.object(AnbimaIPCACore, "standardize_dataframe") as mock_standardize,
		):
			mock_standardize.return_value = pd.DataFrame()
			anbima_ipca_ltm.run(timeout=timeout)
			mock_parse.assert_called_once()


# --------------------------
# Tests for _restructure_table_with_missing_columns
# --------------------------
class TestRestructureTableWithMissingColumns:
	"""Test cases for _restructure_table_with_missing_columns method."""

	def test_restructure_complete_row(
		self,
		anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
		sample_flat_list: list[str],
		expected_columns_ltm: list[str],
	) -> None:
		"""Test restructuring with complete row pattern.

		Verifies
		--------
		- Correctly handles complete row (month, date, numeric, date, numeric).
		- Produces correct dictionary structure.
		- Logs appropriate messages.

		Parameters
		----------
		anbima_ipca_ltm : AnbimaIPCAForecastsLTM
			AnbimaIPCAForecastsLTM instance.
		sample_flat_list : list[str]
			Sample flat list data.
		expected_columns_ltm : list[str]
			Expected column names.

		Returns
		-------
		None
		"""
		mock_log = MagicMock()
		anbima_ipca_ltm.cls_create_log.log_message = mock_log

		result = anbima_ipca_ltm._restructure_table_with_missing_columns(
			flat_list=sample_flat_list[:5],
			expected_columns=expected_columns_ltm,
			missing_value=None,
		)
		expected = [
			{
				"MES_COLETA": "Janeiro de 2025",
				"DATA": "01/01/25",
				"PROJECAO_PCT": "2.5",
				"DATA_VALIDADE": "31/01/25",
				"IPCA_EFETIVO_PCT": "2.3",
			}
		]
		assert result == expected
		assert mock_log.call_count > 0

	def test_restructure_missing_last_column(
		self,
		anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
		sample_flat_list: list[str],
		expected_columns_ltm: list[str],
	) -> None:
		"""Test restructuring with missing last column.

		Verifies
		--------
		- Handles pattern: month, date, numeric, date.
		- Fills missing IPCA_EFETIVO_PCT with None.

		Parameters
		----------
		anbima_ipca_ltm : AnbimaIPCAForecastsLTM
			AnbimaIPCAForecastsLTM instance.
		sample_flat_list : list[str]
			Sample flat list data.
		expected_columns_ltm : list[str]
			Expected column names.

		Returns
		-------
		None
		"""
		mock_log = MagicMock()
		anbima_ipca_ltm.cls_create_log.log_message = mock_log

		result = anbima_ipca_ltm._restructure_table_with_missing_columns(
			flat_list=sample_flat_list[5:9],
			expected_columns=expected_columns_ltm,
			missing_value=None,
		)
		expected = [
			{
				"MES_COLETA": "Fevereiro de 2025",
				"DATA": "01/02/25",
				"PROJECAO_PCT": "2.7",
				"DATA_VALIDADE": "28/02/25",
				"IPCA_EFETIVO_PCT": None,
			}
		]
		assert result == expected
		assert mock_log.call_count > 0

	def test_restructure_missing_first_column(
		self,
		anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
		sample_flat_list: list[str],
		expected_columns_ltm: list[str],
	) -> None:
		"""Test restructuring with missing first column.

		Verifies
		--------
		- Handles pattern: date, numeric, date, numeric.
		- Uses previous month for MES_COLETA.

		Parameters
		----------
		anbima_ipca_ltm : AnbimaIPCAForecastsLTM
			AnbimaIPCAForecastsLTM instance.
		sample_flat_list : list[str]
			Sample flat list data.
		expected_columns_ltm : list[str]
			Expected column names.

		Returns
		-------
		None
		"""
		mock_log = MagicMock()
		anbima_ipca_ltm.cls_create_log.log_message = mock_log

		result = anbima_ipca_ltm._restructure_table_with_missing_columns(
			flat_list=sample_flat_list[9:],
			expected_columns=expected_columns_ltm,
			missing_value=None,
		)
		expected = [
			{
				"MES_COLETA": None,
				"DATA": "01/03/25",
				"PROJECAO_PCT": "2.8",
				"DATA_VALIDADE": "31/03/25",
				"IPCA_EFETIVO_PCT": "2.9",
			}
		]
		assert result == expected
		assert mock_log.call_count > 0

	def test_restructure_empty_list(
		self,
		anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
		expected_columns_ltm: list[str],
	) -> None:
		"""Test restructuring with empty input list returns empty list.

		Verifies
		--------
		- Returns empty list for empty input.
		- No logging occurs.

		Parameters
		----------
		anbima_ipca_ltm : AnbimaIPCAForecastsLTM
			AnbimaIPCAForecastsLTM instance.
		expected_columns_ltm : list[str]
			Expected column names.

		Returns
		-------
		None
		"""
		mock_log = MagicMock()
		anbima_ipca_ltm.cls_create_log.log_message = mock_log

		result = anbima_ipca_ltm._restructure_table_with_missing_columns(
			flat_list=[],
			expected_columns=expected_columns_ltm,
			missing_value=None,
		)
		assert result == []
		mock_log.assert_not_called()

	def test_restructure_invalid_values(
		self,
		anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
		expected_columns_ltm: list[str],
	) -> None:
		"""Test restructuring with invalid values skips unmatched elements.

		Verifies
		--------
		- Skips invalid patterns.
		- Logs warning for unmatched elements.
		- Returns empty list.

		Parameters
		----------
		anbima_ipca_ltm : AnbimaIPCAForecastsLTM
			AnbimaIPCAForecastsLTM instance.
		expected_columns_ltm : list[str]
			Expected column names.

		Returns
		-------
		None
		"""
		mock_log = MagicMock()
		anbima_ipca_ltm.cls_create_log.log_message = mock_log

		result = anbima_ipca_ltm._restructure_table_with_missing_columns(
			flat_list=["invalid"],
			expected_columns=expected_columns_ltm,
			missing_value=None,
		)
		assert result == []
		assert mock_log.call_count > 0


# --------------------------
# Tests for _debug_flat_list_structure
# --------------------------
class TestDebugFlatListStructure:
	"""Test cases for _debug_flat_list_structure method."""

	def test_debug_flat_list_structure(
		self,
		anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
		sample_flat_list: list[str],
	) -> None:
		"""Test _debug_flat_list_structure logs structure analysis.

		Verifies
		--------
		- Correctly analyzes and logs structure of flat list.
		- Identifies month, date, and numeric patterns.
		- Logs month positions and distances.

		Parameters
		----------
		anbima_ipca_ltm : AnbimaIPCAForecastsLTM
			AnbimaIPCAForecastsLTM instance.
		sample_flat_list : list[str]
			Sample flat list data.

		Returns
		-------
		None
		"""
		mock_log = MagicMock()
		anbima_ipca_ltm.cls_create_log.log_message = mock_log

		anbima_ipca_ltm._debug_flat_list_structure(sample_flat_list)

		assert mock_log.call_count >= 5
		logged_messages = [call.args[1] for call in mock_log.call_args_list]
		assert any("FLAT LIST STRUCTURE ANALYSIS" in msg for msg in logged_messages)

	def test_debug_flat_list_structure_empty(
		self,
		anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
	) -> None:
		"""Test _debug_flat_list_structure with empty list logs header only.

		Verifies
		--------
		- Handles empty input gracefully.
		- Logs at least the header.

		Parameters
		----------
		anbima_ipca_ltm : AnbimaIPCAForecastsLTM
			AnbimaIPCAForecastsLTM instance.

		Returns
		-------
		None
		"""
		mock_log = MagicMock()
		anbima_ipca_ltm.cls_create_log.log_message = mock_log

		anbima_ipca_ltm._debug_flat_list_structure([])

		assert mock_log.call_count >= 1
		logged_messages = [call.args[1] for call in mock_log.call_args_list]
		assert any("FLAT LIST STRUCTURE ANALYSIS" in msg for msg in logged_messages)


# --------------------------
# Edge Cases and Error Conditions
# --------------------------
@pytest.mark.parametrize("invalid_timeout", ["invalid", [10, 20], {1: 2}])
def test_run_invalid_timeout(
	anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
	invalid_timeout: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test run method with invalid timeout values raises TypeError.

	Verifies
	--------
	- Raises TypeError for invalid timeout types.

	Parameters
	----------
	anbima_ipca_ltm : AnbimaIPCAForecastsLTM
		AnbimaIPCAForecastsLTM instance.
	invalid_timeout : Any
		Invalid timeout value.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError):
		anbima_ipca_ltm.run(timeout=invalid_timeout)


def test_run_with_empty_table_name(
	anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
	mock_playwright_scraper: PlaywrightScraper,
	mocker: MockerFixture,
) -> None:
	"""Test run method with empty table name raises ValueError.

	Verifies
	--------
	- Raises ValueError for empty str_table_name when using database.

	Parameters
	----------
	anbima_ipca_ltm : AnbimaIPCAForecastsLTM
		AnbimaIPCAForecastsLTM instance.
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
	mocker.patch.object(AnbimaIPCAForecastsLTM, "transform_data", return_value=mock_df)
	mocker.patch.object(AnbimaIPCACore, "standardize_dataframe", return_value=mock_df)

	with pytest.raises(ValueError, match="str_table_name cannot be empty"):
		anbima_ipca_ltm.run(str_table_name="")


# --------------------------
# Tests for module reload
# --------------------------
def test_module_reload(
	anbima_ipca_ltm: AnbimaIPCAForecastsLTM,
	mocker: MockerFixture,
) -> None:
	"""Test module reloading behavior.

	Verifies
	--------
	- Module can be reloaded without errors.
	- New instance initializes correctly.

	Parameters
	----------
	anbima_ipca_ltm : AnbimaIPCAForecastsLTM
		AnbimaIPCAForecastsLTM instance.
	mocker : MockerFixture
		Pytest mocker for patching dependencies.

	Returns
	-------
	None
	"""
	import importlib

	original_url = anbima_ipca_ltm.url
	mocker.patch.object(DatesCurrent, "curr_date", return_value=date(2025, 1, 2))
	mocker.patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1))

	importlib.reload(
		sys.modules["stpstone.ingestion.countries.br.macroeconomics.anbima_ipca_forecasts_ltm"]
	)
	new_instance = AnbimaIPCAForecastsLTM()

	assert new_instance.url == original_url
	assert new_instance.date_ref == date(2025, 1, 1)
