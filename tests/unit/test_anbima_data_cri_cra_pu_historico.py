"""Unit tests for AnbimaDataCRICRAPUHistorico ingestion class."""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import Mock

import pandas as pd
import pytest
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_pu_historico import (
	AnbimaDataCRICRAPUHistorico,
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
class TestAnbimaDataCRICRAPUHistorico:
	"""Test cases for AnbimaDataCRICRAPUHistorico class."""

	def test_init_creates_instance(self, mock_logger: Logger) -> None:
		"""Test successful instance creation.

		Verifies
		--------
		- Instance is created with default values.
		- Base URL is set correctly.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAPUHistorico(logger=mock_logger)

		assert instance.list_asset_codes == []
		assert isinstance(instance.base_url, str)

	def test_init_with_asset_codes(self, mock_logger: Logger) -> None:
		"""Test initialization with asset codes list.

		Verifies
		--------
		- Asset codes are stored correctly.
		- Base URL is correctly set.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		asset_codes = ["HIST001", "HIST002"]
		instance = AnbimaDataCRICRAPUHistorico(logger=mock_logger, list_asset_codes=asset_codes)

		assert instance.list_asset_codes == asset_codes
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
		instance = AnbimaDataCRICRAPUHistorico(date_ref=None, logger=mock_logger)

		assert instance.date_ref is not None
		assert isinstance(instance.date_ref, date)

	def test_transform_data_handles_date_fields(self, mock_logger: Logger) -> None:
		"""Test date field handling in transform_data.

		Verifies
		--------
		- Date replacement works correctly.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		raw_data = [{"COD_ATIVO": "HIST001", "DATA_REFERENCIA": "-"}]

		instance = AnbimaDataCRICRAPUHistorico(logger=mock_logger)
		result = instance.transform_data(raw_data=raw_data)

		assert result["DATA_REFERENCIA"].iloc[0] == "01/01/2100"

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
		instance = AnbimaDataCRICRAPUHistorico(logger=mock_logger)
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
		instance = AnbimaDataCRICRAPUHistorico(logger=mock_logger)
		result = instance.parse_raw_file(resp_req=Mock())

		assert isinstance(result, StringIO)
