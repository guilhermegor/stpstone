"""Unit tests for AnbimaDataDebenturesCharacteristics ingestion class."""

from datetime import date
from logging import Logger
from typing import Any
from unittest.mock import Mock, patch

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_debentures_characteristics import (
	AnbimaDataDebenturesCharacteristics,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> Mock:
	"""Fixture providing a mock logger.

	Returns
	-------
	Mock
		Mocked logger instance
	"""
	return Mock(spec=Logger)


@pytest.fixture
def mock_db_session() -> Mock:
	"""Fixture providing a mock database session.

	Returns
	-------
	Mock
		Mocked database session
	"""
	return Mock(spec=Session)


@pytest.fixture
def mock_dates_current() -> Mock:
	"""Fixture providing a mock DatesCurrent instance.

	Returns
	-------
	Mock
		Mocked DatesCurrent instance
	"""
	mock_dates = Mock(spec=DatesCurrent)
	mock_dates.curr_date.return_value = date(2023, 1, 1)
	return mock_dates


@pytest.fixture
def mock_dates_br() -> Mock:
	"""Fixture providing a mock DatesBRAnbima instance.

	Returns
	-------
	Mock
		Mocked DatesBRAnbima instance
	"""
	mock_dates = Mock(spec=DatesBRAnbima)
	mock_dates.add_working_days.return_value = date(2023, 1, 1)
	return mock_dates


@pytest.fixture
def mock_create_log() -> Mock:
	"""Fixture providing a mock CreateLog instance.

	Returns
	-------
	Mock
		Mocked CreateLog instance
	"""
	mock_log = Mock(spec=CreateLog)
	mock_log.log_message = Mock()
	return mock_log


@pytest.fixture
def mock_dir_files_management() -> Mock:
	"""Fixture providing a mock DirFilesManagement instance.

	Returns
	-------
	Mock
		Mocked DirFilesManagement instance
	"""
	return Mock(spec=DirFilesManagement)


@pytest.fixture
def sample_characteristics_data() -> list[dict[str, Any]]:
	"""Fixture providing sample characteristics data for testing.

	Returns
	-------
	list[dict[str, Any]]
		List of sample characteristics records
	"""
	return [
		{
			"CODIGO_DEBENTURE": "DEB001",
			"NUMERO_SERIE": "SERIE A",  # codespell:ignore
			"REMUNERACAO": "10.5%",
			"DATA_INICIO_RENTABILIDADE": "01/01/2023",
			"PERIODO_CAPITALIZACAO_PAPEL": "ANUAL",  # codespell:ignore
			"QUANTIDADE_SERIE_DATA_EMISSAO": "1000",
			"VOLUME_SERIE_DATA_EMISSAO": "1000000.00",
			"VNE": "950.00",
			"VNA": "980.00",
			"QUANTIDADE_MERCADO_B3": "500",
			"ESTOQUE_MERCADO_B3": "500000.00",
			"DATA_EMISSAO": "01/01/2023",
			"DATA_VENCIMENTO": "01/01/2030",
			"DATA_PROXIMA_REPACTUACAO": "01/01/2024",
			"PRAZO_EMISSAO": "7 years",
			"PRAZO_REMANESCENTE": "6 years",
			"RESGATE_ANTECIPADO": "SIM",
			"ISIN": "BRDEB001123456",
			"DATA_PROXIMO_EVENTO_AGENDA": "01/02/2023",
			"LEI_12_431": "SIM",
			"ARTIGO": "ARTIGO 2",
			"EMISSAO": "EMISSAO PRINCIPAL",
			"EMPRESA": "COMPANY A",
			"SETOR": "FINANCEIRO",
			"CNPJ": "12.345.678/0001-90",
			"VOLUME_EMISSAO": "1000000.00",
			"QUANTIDADE_EMISSAO": "1000",
			"COORDENADOR_LIDER_NOME": "BANCO A",
			"COORDENADOR_LIDER_CNPJ": "12.345.678/0001-91",
			"AGENTE_FIDUCIARIO_NOME": "AGENTE A",
			"AGENTE_FIDUCIARIO_CNPJ": "12.345.678/0001-92",
			"BANCO_MANDATARIO_NOME": "BANCO B",
			"GARANTIA": "GARANTIA REAL",
			"PU_PAR": "1000.00",
			"PU_INDICATIVO": "1050.00",
		}
	]


@pytest.fixture
def mock_playwright_page() -> Mock:
	"""Fixture providing a mock Playwright page.

	Returns
	-------
	Mock
		Mocked Playwright page
	"""
	mock_page = Mock(spec=PlaywrightPage)
	mock_page.query_selector_all.return_value = []
	mock_page.query_selector.return_value = None
	mock_page.locator.return_value = Mock()
	mock_page.goto = Mock()
	mock_page.wait_for_timeout = Mock()
	return mock_page


# --------------------------
# Tests for AnbimaDataDebenturesCharacteristics
# --------------------------
class TestAnbimaDataDebenturesCharacteristics:
	"""Test cases for AnbimaDataDebenturesCharacteristics class.

	This test class verifies the behavior of debentures characteristics
	extraction functionality.
	"""

	def test_init_with_debenture_codes(
		self,
		mock_logger: Mock,
		mock_db_session: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
	) -> None:
		"""Test initialization with debenture codes.

		Verifies
		--------
		- Debenture codes are properly stored
		- Base URL is set correctly
		- All dependencies are injected

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_db_session : Mock
			Mocked database session
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_characteristics",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesCharacteristics(
				logger=mock_logger,
				cls_db=mock_db_session,
				debenture_codes=["DEB001", "DEB002"],
			)

		assert instance.debenture_codes == ["DEB001", "DEB002"]
		assert instance.base_url == "https://data.anbima.com.br/debentures"

	def test_init_without_debenture_codes(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
	) -> None:
		"""Test initialization without debenture codes.

		Verifies
		--------
		- Empty list is used as default for debenture codes
		- Class can be initialized without codes

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_characteristics",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesCharacteristics(logger=mock_logger)

		assert instance.debenture_codes == []

	@patch(
		"stpstone.ingestion.countries.br.registries"
		".anbima_data_debentures_characteristics.sync_playwright"
	)
	def test_get_response_with_no_codes(
		self,
		mock_sync_playwright: Mock,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
	) -> None:
		"""Test response retrieval with no debenture codes.

		Verifies
		--------
		- Empty list is returned when no codes provided
		- Warning message is logged
		- Playwright is not used

		Parameters
		----------
		mock_sync_playwright : Mock
			Mocked sync_playwright context manager
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_characteristics",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesCharacteristics(logger=mock_logger)

		result = instance.get_response(timeout_ms=1000)

		assert isinstance(result, list)
		assert len(result) == 0
		mock_sync_playwright.assert_not_called()

	def test_transform_characteristics_data_date_handling(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		sample_characteristics_data: list[dict[str, Any]],
	) -> None:
		"""Test date field transformation in characteristics data.

		Verifies
		--------
		- Date fields with hyphens are replaced with default date
		- Other fields remain unchanged
		- DataFrame structure is maintained

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance
		sample_characteristics_data : list[dict[str, Any]]
			Sample characteristics data for testing

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_characteristics",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesCharacteristics(logger=mock_logger)

		sample_characteristics_data[0]["DATA_INICIO_RENTABILIDADE"] = "-"
		sample_characteristics_data[0]["DATA_EMISSAO"] = "-"

		result = instance.transform_data(sample_characteristics_data)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
		assert result.iloc[0]["DATA_INICIO_RENTABILIDADE"] == "01/01/2100"
		assert result.iloc[0]["DATA_EMISSAO"] == "01/01/2100"


# --------------------------
# Error Handling Tests
# --------------------------
class TestAnbimaDebenturesCharacteristicsErrorHandling:
	"""Error handling tests for AnbimaDataDebenturesCharacteristics class."""

	def test_characteristics_extraction_with_invalid_xpath(
		self,
		mock_logger: Mock,
		mock_dates_current: Mock,
		mock_dates_br: Mock,
		mock_create_log: Mock,
		mock_dir_files_management: Mock,
		mock_playwright_page: Mock,
	) -> None:
		"""Test characteristics extraction with invalid XPath.

		Verifies
		--------
		- Extraction fails gracefully when XPath errors occur
		- All fields are set to None when extraction fails
		- Error logging is performed

		Parameters
		----------
		mock_logger : Mock
			Mocked logger instance
		mock_dates_current : Mock
			Mocked DatesCurrent instance
		mock_dates_br : Mock
			Mocked DatesBRAnbima instance
		mock_create_log : Mock
			Mocked CreateLog instance
		mock_dir_files_management : Mock
			Mocked DirFilesManagement instance
		mock_playwright_page : Mock
			Mocked Playwright page

		Returns
		-------
		None
		"""
		with patch.multiple(
			"stpstone.ingestion.countries.br.registries.anbima_data_debentures_characteristics",
			DatesCurrent=Mock(return_value=mock_dates_current),
			DatesBRAnbima=Mock(return_value=mock_dates_br),
			CreateLog=Mock(return_value=mock_create_log),
			DirFilesManagement=Mock(return_value=mock_dir_files_management),
		):
			instance = AnbimaDataDebenturesCharacteristics(logger=mock_logger)

		mock_playwright_page.locator.side_effect = Exception("XPath error")

		result = instance._extract_debenture_data(mock_playwright_page, "123", "Test Debenture")

		assert result["CODIGO_DEBENTURE"] is None
		assert result["NUMERO_SERIE"] is None
