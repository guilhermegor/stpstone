"""Unit tests for AnbimaDataCRICRAEvents ingestion class."""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_events import (
	AnbimaDataCRICRAEvents,
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
class TestAnbimaDataCRICRAEvents:
	"""Test cases for AnbimaDataCRICRAEvents class."""

	def test_init_with_asset_codes_list(self, mock_logger: Logger) -> None:
		"""Test initialization with asset codes list.

		Verifies
		--------
		- Asset codes are stored correctly.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		codes = ["EVT001", "EVT002"]
		instance = AnbimaDataCRICRAEvents(logger=mock_logger, list_asset_codes=codes)

		assert instance.list_asset_codes == codes

	def test_init_with_empty_asset_codes(self, mock_logger: Logger) -> None:
		"""Test initialization with empty asset codes list.

		Verifies
		--------
		- Class initializes successfully with empty list.
		- Base URL is correctly set.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAEvents(logger=mock_logger, list_asset_codes=[])

		assert instance.list_asset_codes == []
		assert instance.base_url == "https://data.anbima.com.br/certificado-de-recebiveis"

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
		instance = AnbimaDataCRICRAEvents(date_ref=None, logger=mock_logger)

		assert instance.date_ref is not None
		assert isinstance(instance.date_ref, date)

	def test_transform_data_with_event_dates(self, mock_logger: Logger) -> None:
		"""Test transform_data handles event dates.

		Verifies
		--------
		- Date fields are properly processed.
		- DataFrame is created correctly.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		raw_data = [{"COD_ATIVO": "EVT001", "DATA_EVENTO": "-", "DATA_LIQUIDACAO": "-"}]

		instance = AnbimaDataCRICRAEvents(logger=mock_logger)
		result = instance.transform_data(raw_data=raw_data)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
		assert "DATA_EVENTO" in result.columns
		assert "DATA_LIQUIDACAO" in result.columns

	def test_transform_data_with_empty_list(self, mock_logger: Logger) -> None:
		"""Test transform_data with empty input list.

		Verifies
		--------
		- Returns empty DataFrame.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAEvents(logger=mock_logger)
		result = instance.transform_data(raw_data=[])

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0

	def test_transform_data_structure(self, mock_logger: Logger) -> None:
		"""Test events DataFrame structure.

		Verifies
		--------
		- Events DataFrame has expected columns after transform.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		raw_data = [{"COD_ATIVO": "EVT001", "DATA_EVENTO": "15/01/2024", "STATUS": "Paid"}]

		instance = AnbimaDataCRICRAEvents(logger=mock_logger)
		result = instance.transform_data(raw_data=raw_data)

		assert "COD_ATIVO" in result.columns
		assert "DATA_EVENTO" in result.columns
		assert "STATUS" in result.columns

	@patch("stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_events.sync_playwright")
	def test_get_response_no_assets(self, mock_playwright: Mock, mock_logger: Logger) -> None:
		"""Test get_response with no asset codes.

		Verifies
		--------
		- Returns empty list.
		- No browser operations occur.

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
		instance = AnbimaDataCRICRAEvents(logger=mock_logger, list_asset_codes=[])

		result = instance.get_response(timeout_ms=1000)

		assert result == []
		mock_playwright.assert_not_called()

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
		instance = AnbimaDataCRICRAEvents(logger=mock_logger)
		result = instance.parse_raw_file(resp_req=Mock())

		assert isinstance(result, StringIO)
