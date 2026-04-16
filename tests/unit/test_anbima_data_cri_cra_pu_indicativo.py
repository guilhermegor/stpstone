"""Unit tests for AnbimaDataCRICRAPUIndicativo ingestion class."""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import Mock

import pandas as pd
import pytest
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_pu_indicativo import (
	AnbimaDataCRICRAPUIndicativo,
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
class TestAnbimaDataCRICRAPUIndicativo:
	"""Test cases for AnbimaDataCRICRAPUIndicativo class."""

	def test_init_with_default_values(self, mock_logger: Logger) -> None:
		"""Test initialization with default values.

		Verifies
		--------
		- Default empty list for asset codes.
		- Base URL is correctly set.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)

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
		instance = AnbimaDataCRICRAPUIndicativo(date_ref=None, logger=mock_logger)

		assert instance.date_ref is not None
		assert isinstance(instance.date_ref, date)

	def test_transform_data_date_replacement(self, mock_logger: Logger) -> None:
		"""Test transform_data replaces dash with default date.

		Verifies
		--------
		- Date fields with dashes are replaced.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		raw_data = [{"COD_ATIVO": "PU001", "DATA_REFERENCIA": "-", "DATA_REFERENCIA_NTNB": "-"}]

		instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)
		result = instance.transform_data(raw_data=raw_data)

		assert result["DATA_REFERENCIA"].iloc[0] == "01/01/2100"
		assert result["DATA_REFERENCIA_NTNB"].iloc[0] == "01/01/2100"

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
		instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)
		result = instance.transform_data(raw_data=[])

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0

	def test_transform_data_with_none_values(self, mock_logger: Logger) -> None:
		"""Test transform_data handles None values in data.

		Verifies
		--------
		- None values don't cause errors.
		- None values are handled appropriately.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		raw_data = [{"COD_ATIVO": None, "DATA_REFERENCIA": None, "IS_CRI_CRA": None}]

		instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)
		result = instance.transform_data(raw_data=raw_data)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1

	def test_transform_data_with_mixed_date_formats(self, mock_logger: Logger) -> None:
		"""Test transform_data with mixed date formats.

		Verifies
		--------
		- Various date formats are handled.
		- Dashes and valid dates both work.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		raw_data = [
			{"DATA_REFERENCIA": "15/01/2024", "DATA_REFERENCIA_NTNB": "-"},
			{"DATA_REFERENCIA": "-", "DATA_REFERENCIA_NTNB": "20/02/2024"},
		]

		instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)
		result = instance.transform_data(raw_data=raw_data)

		assert result["DATA_REFERENCIA"].iloc[0] == "15/01/2024"
		assert result["DATA_REFERENCIA_NTNB"].iloc[0] == "01/01/2100"
		assert result["DATA_REFERENCIA"].iloc[1] == "01/01/2100"
		assert result["DATA_REFERENCIA_NTNB"].iloc[1] == "20/02/2024"

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
		instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)
		result = instance.parse_raw_file(resp_req=Mock())

		assert isinstance(result, StringIO)


# --------------------------
# String Handling Tests
# --------------------------
class TestStringHandling:
	"""Test string handling and cleaning for PU Indicativo."""

	def test_date_string_with_whitespace(self, mock_logger: Logger) -> None:
		"""Test date strings with whitespace are handled.

		Verifies
		--------
		- Whitespace in dates doesn't cause errors.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		raw_data = [{"DATA_REFERENCIA": " 15/01/2024 ", "COD_ATIVO": " TEST001 "}]

		instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)
		result = instance.transform_data(raw_data=raw_data)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1

	def test_empty_string_handling(self, mock_logger: Logger) -> None:
		"""Test empty string handling in data.

		Verifies
		--------
		- Empty strings don't cause errors.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		raw_data = [{"COD_ATIVO": "", "DATA_REFERENCIA": "", "IS_CRI_CRA": ""}]

		instance = AnbimaDataCRICRAPUIndicativo(logger=mock_logger)
		result = instance.transform_data(raw_data=raw_data)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
