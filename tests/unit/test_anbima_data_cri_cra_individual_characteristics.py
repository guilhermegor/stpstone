"""Unit tests for AnbimaDataCRICRAIndividualCharacteristics ingestion class."""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_individual_characteristics import (  # noqa: E501
	AnbimaDataCRICRAIndividualCharacteristics,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> Logger:
	"""Fixture providing mock logger.

	Returns
	-------
	Logger
		Mock logger instance.
	"""
	return Mock(spec=Logger)


@pytest.fixture
def mock_db_session() -> Session:
	"""Fixture providing mock database session.

	Returns
	-------
	Session
		Mock database session instance.
	"""
	return Mock(spec=Session)


@pytest.fixture
def sample_date_ref() -> date:
	"""Fixture providing sample reference date.

	Returns
	-------
	date
		Reference date for testing.
	"""
	return date(2024, 1, 15)


# --------------------------
# Tests
# --------------------------
class TestAnbimaDataCRICRAIndividualCharacteristics:
	"""Test cases for AnbimaDataCRICRAIndividualCharacteristics class."""

	def test_init_with_empty_asset_codes(self, mock_logger: Logger) -> None:
		"""Test initialization with empty asset codes list.

		Verifies
		--------
		- Class initializes successfully with empty list.
		- list_asset_codes is empty list.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAIndividualCharacteristics(
			logger=mock_logger, list_asset_codes=[]
		)

		assert instance.list_asset_codes == []

	def test_init_with_asset_codes(self, mock_logger: Logger) -> None:
		"""Test initialization with asset codes list.

		Verifies
		--------
		- Asset codes list is correctly stored.
		- List contains expected values.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		asset_codes = ["TEST001", "TEST002", "TEST003"]
		instance = AnbimaDataCRICRAIndividualCharacteristics(
			logger=mock_logger, list_asset_codes=asset_codes
		)

		assert instance.list_asset_codes == asset_codes
		assert len(instance.list_asset_codes) == 3

	def test_init_with_none_date_ref(self, mock_logger: Logger) -> None:
		"""Test initialization with None date_ref uses default date.

		Verifies
		--------
		- When date_ref is None, a default date is calculated.
		- The default date is not None.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAIndividualCharacteristics(date_ref=None, logger=mock_logger)

		assert instance.date_ref is not None
		assert isinstance(instance.date_ref, date)

	@patch(
		"stpstone.ingestion.countries.br.registries"
		".anbima_data_cri_cra_individual_characteristics.sync_playwright"
	)
	def test_get_response_with_empty_asset_codes(
		self, mock_playwright: Mock, mock_logger: Logger
	) -> None:
		"""Test get_response with empty asset codes list.

		Verifies
		--------
		- Returns empty list when no asset codes provided.
		- No Playwright browser is launched.

		Parameters
		----------
		mock_playwright : Mock
			Mock sync_playwright.
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAIndividualCharacteristics(
			logger=mock_logger, list_asset_codes=[]
		)

		result = instance.get_response(timeout_ms=1000)

		assert result == []
		mock_playwright.assert_not_called()

	def test_transform_data_with_empty_list(self, mock_logger: Logger) -> None:
		"""Test transform_data with empty input.

		Verifies
		--------
		- Returns empty DataFrame.
		- No errors are raised.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAIndividualCharacteristics(logger=mock_logger)
		result = instance.transform_data(raw_data=[])

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0

	def test_handle_date_value_with_none(self, mock_logger: Logger) -> None:
		"""Test _handle_date_value with None input.

		Verifies
		--------
		- None value is replaced with '01/01/2100'.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAIndividualCharacteristics(logger=mock_logger)
		result = instance._handle_date_value(None)

		assert result == "01/01/2100"

	def test_handle_date_value_with_dash(self, mock_logger: Logger) -> None:
		"""Test _handle_date_value with dash input.

		Verifies
		--------
		- Dash value is replaced with '01/01/2100'.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAIndividualCharacteristics(logger=mock_logger)
		result = instance._handle_date_value("-")

		assert result == "01/01/2100"

	def test_handle_date_value_with_valid_date(self, mock_logger: Logger) -> None:
		"""Test _handle_date_value with valid date string.

		Verifies
		--------
		- Valid date string is returned unchanged.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAIndividualCharacteristics(logger=mock_logger)
		result = instance._handle_date_value("15/01/2024")

		assert result == "15/01/2024"

	def test_parse_raw_file_returns_stringio(self, mock_logger: Logger) -> None:
		"""Test parse_raw_file returns StringIO instance.

		Verifies
		--------
		- parse_raw_file returns StringIO.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAIndividualCharacteristics(logger=mock_logger)
		result = instance.parse_raw_file(resp_req=Mock())

		assert isinstance(result, StringIO)

	def test_base_url_is_correct(self, mock_logger: Logger) -> None:
		"""Test base URL is correctly set.

		Verifies
		--------
		- base_url points to the ANBIMA CRI/CRA endpoint.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAIndividualCharacteristics(logger=mock_logger)

		assert instance.base_url == "https://data.anbima.com.br/certificado-de-recebiveis"


# --------------------------
# List Asset Codes Tests
# --------------------------
class TestListAssetCodes:
	"""Test list_asset_codes parameter handling for individual characteristics."""

	@pytest.mark.parametrize("asset_count", [0, 1, 5, 100])
	def test_various_asset_list_sizes(self, asset_count: int, mock_logger: Logger) -> None:
		"""Test handling of various asset list sizes.

		Verifies
		--------
		- Different list sizes are handled correctly.

		Parameters
		----------
		asset_count : int
			Number of assets in list.
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		asset_codes = [f"ASSET{i:03d}" for i in range(asset_count)]

		instance = AnbimaDataCRICRAIndividualCharacteristics(
			logger=mock_logger, list_asset_codes=asset_codes
		)

		assert len(instance.list_asset_codes) == asset_count
		assert instance.list_asset_codes == asset_codes
