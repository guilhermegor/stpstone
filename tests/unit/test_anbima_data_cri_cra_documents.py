"""Unit tests for AnbimaDataCRICRADocuments ingestion class."""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_documents import (
	AnbimaDataCRICRADocuments,
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
class TestAnbimaDataCRICRADocuments:
	"""Test cases for AnbimaDataCRICRADocuments class."""

	def test_init_with_asset_codes(self, mock_logger: Logger) -> None:
		"""Test initialization with asset codes.

		Verifies
		--------
		- Asset codes list is properly stored.
		- Base URL is correctly set.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		asset_codes = ["DOC001", "DOC002"]
		instance = AnbimaDataCRICRADocuments(logger=mock_logger, list_asset_codes=asset_codes)

		assert instance.list_asset_codes == asset_codes
		assert instance.base_url == "https://data.anbima.com.br/certificado-de-recebiveis"

	def test_init_with_empty_asset_codes(self, mock_logger: Logger) -> None:
		"""Test initialization with empty asset codes list.

		Verifies
		--------
		- Class initializes successfully with empty list.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRADocuments(logger=mock_logger, list_asset_codes=[])

		assert instance.list_asset_codes == []

	def test_init_with_none_date_ref(self, mock_logger: Logger) -> None:
		"""Test initialization with None date_ref uses default date.

		Verifies
		--------
		- When date_ref is None, a default date is calculated.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRADocuments(date_ref=None, logger=mock_logger)

		assert instance.date_ref is not None
		assert isinstance(instance.date_ref, date)

	@patch(
		"stpstone.ingestion.countries.br.registries"
		".anbima_data_cri_cra_documents.sync_playwright"
	)
	def test_get_response_empty_asset_codes(
		self, mock_playwright: Mock, mock_logger: Logger
	) -> None:
		"""Test get_response with no asset codes.

		Verifies
		--------
		- Returns empty list.
		- Playwright is not invoked.

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
		instance = AnbimaDataCRICRADocuments(logger=mock_logger, list_asset_codes=[])

		result = instance.get_response(timeout_ms=1000)

		assert result == []
		mock_playwright.assert_not_called()

	def test_transform_data_with_valid_data(self, mock_logger: Logger) -> None:
		"""Test transform_data with valid document data.

		Verifies
		--------
		- DataFrame is created correctly.
		- Date fields are properly handled.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		raw_data = [
			{
				"COD_ATIVO": "DOC001",
				"IS_CRI_CRA": "CRI",
				"DATA_DIVULGACAO_DOCUMENTO": "15/01/2024",
			}
		]

		instance = AnbimaDataCRICRADocuments(logger=mock_logger)
		result = instance.transform_data(raw_data=raw_data)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
		assert result["DATA_DIVULGACAO_DOCUMENTO"].iloc[0] == "15/01/2024"

	def test_transform_data_with_dash_date(self, mock_logger: Logger) -> None:
		"""Test transform_data with dash in date field.

		Verifies
		--------
		- Dash is replaced with default date.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		raw_data = [{"COD_ATIVO": "DOC001", "DATA_DIVULGACAO_DOCUMENTO": "-"}]

		instance = AnbimaDataCRICRADocuments(logger=mock_logger)
		result = instance.transform_data(raw_data=raw_data)

		assert result["DATA_DIVULGACAO_DOCUMENTO"].iloc[0] == "01/01/2100"

	def test_transform_data_with_empty_list(self, mock_logger: Logger) -> None:
		"""Test transform_data with empty input list.

		Verifies
		--------
		- Returns empty DataFrame when input is empty.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRADocuments(logger=mock_logger)
		result = instance.transform_data(raw_data=[])

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0

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
		instance = AnbimaDataCRICRADocuments(logger=mock_logger)
		result = instance.parse_raw_file(resp_req=Mock())

		assert isinstance(result, StringIO)


# --------------------------
# List Asset Codes Tests
# --------------------------
class TestListAssetCodes:
	"""Test list_asset_codes parameter handling for documents."""

	def test_asset_codes_immutability(self, mock_logger: Logger) -> None:
		"""Test asset codes list is stored correctly.

		Verifies
		--------
		- Original list is preserved.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		original_list = ["ASSET001", "ASSET002"]
		instance = AnbimaDataCRICRADocuments(
			logger=mock_logger, list_asset_codes=original_list
		)

		assert instance.list_asset_codes == original_list
		assert len(instance.list_asset_codes) == len(original_list)
