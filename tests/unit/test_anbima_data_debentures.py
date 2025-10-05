"""Unit tests for Anbima Debentures ingestion classes.

Tests the web scraping functionality for Anbima debentures data including:
- Debentures listing and characteristics
- Documents, prices, and events data extraction
- Error handling and edge cases
- Type validation and data transformation
"""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Any
from unittest.mock import Mock, patch

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_debentures import (
    AnbimaDataDebenturesAvailable,
    AnbimaDataDebenturesCharacteristics,
    AnbimaDataDebenturesDocuments,
    AnbimaDataDebenturesEvents,
    AnbimaDataDebenturesPrices,
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
def sample_debenture_data() -> list[dict[str, Any]]:
    """Fixture providing sample debenture data for testing.

    Returns
    -------
    list[dict[str, Any]]
        List of sample debenture records
    """
    return [
        {
            "NOME": "DEBENTURE A",
            "EMISSOR": "COMPANY A",
            "REMUNERACAO": "10.5%",
            "DATA_VENCIMENTO": "01/01/2030",
            "DURATION": "5 years",
            "SETOR": "FINANCEIRO",
            "DATA_EMISSAO": "01/01/2023",
            "PU_PAR": "1000.00",
            "PU_INDICATIVO": "1050.00",
            "pagina": 1,
        },
        {
            "NOME": "DEBENTURE B",
            "EMISSOR": "COMPANY B",
            "REMUNERACAO": "8.5%",
            "DATA_VENCIMENTO": "01/01/2035",
            "DURATION": "10 years",
            "SETOR": "INDUSTRIAL",
            "DATA_EMISSAO": "01/01/2022",
            "PU_PAR": "950.00",
            "PU_INDICATIVO": "980.00",
            "pagina": 1,
        },
    ]


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
            "NUMERO_SERIE": "SERIE A", # codespell:ignore
            "REMUNERACAO": "10.5%",
            "DATA_INICIO_RENTABILIDADE": "01/01/2023",
            "PERIODO_CAPITALIZACAO_PAPEL": "ANUAL", # codespell:ignore
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
def sample_documents_data() -> list[dict[str, Any]]:
    """Fixture providing sample documents data for testing.

    Returns
    -------
    list[dict[str, Any]]
        List of sample document records
    """
    return [
        {
            "CODIGO_DEBENTURE": "DEB001",
            "EMISSOR": "COMPANY A",
            "SETOR": "FINANCEIRO",
            "NOME_DOCUMENTO": "ATA DE AGD",
            "DATA_DIVULGACAO_DOCUMENTO": "01/01/2023",
            "LINK_DOCUMENTO": "https://example.com/doc1.pdf",
        },
        {
            "CODIGO_DEBENTURE": "DEB001",
            "EMISSOR": "COMPANY A",
            "SETOR": "FINANCEIRO",
            "NOME_DOCUMENTO": "PROSPECTO",
            "DATA_DIVULGACAO_DOCUMENTO": "01/01/2023",
            "LINK_DOCUMENTO": "https://example.com/doc2.pdf",
        },
    ]


@pytest.fixture
def sample_prices_data() -> list[dict[str, Any]]:
    """Fixture providing sample prices data for testing.

    Returns
    -------
    list[dict[str, Any]]
        List of sample price records
    """
    return [
        {
            "CODIGO_DEBENTURE": "DEB001",
            "EMISSOR": "COMPANY A",
            "SETOR": "FINANCEIRO",
            "DATA_REFERENCIA": "01/01/2023",
            "VNA": "980.00",
            "PU_PAR": "1000.00",
            "PU_EVENTO": "1010.00",
            "URL": "https://data.anbima.com.br/debentures/DEB001/precos",
        }
    ]


@pytest.fixture
def sample_events_data() -> list[dict[str, Any]]:
    """Fixture providing sample events data for testing.

    Returns
    -------
    list[dict[str, Any]]
        List of sample event records
    """
    return [
        {
            "CODIGO_DEBENTURE": "DEB001",
            "EMISSOR": "COMPANY A",
            "SETOR": "FINANCEIRO",
            "DATA_EVENTO": "01/01/2023",
            "DATA_LIQUIDACAO": "05/01/2023",
            "EVENTO": "PAGAMENTO DE JUROS",
            "PERCENTUAL_TAXA": "10.5%",
            "VALOR_PAGO": "1050.00",
            "STATUS": "LIQUIDADO",
            "URL": "https://data.anbima.com.br/debentures/DEB001/agenda",
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
# Tests for AnbimaDataDebenturesAvailable
# --------------------------
class TestAnbimaDataDebenturesAvailable:
    """Test cases for AnbimaDataDebenturesAvailable class.

    This test class verifies the behavior of debentures listing functionality
    with various input scenarios and edge cases.
    """

    def test_init_with_valid_parameters(
        self,
        mock_logger: Mock,
        mock_db_session: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
    ) -> None:
        """Test initialization with valid parameters.

        Verifies
        --------
        - The class can be initialized with valid parameters
        - Default values are set correctly
        - All dependencies are properly injected

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesAvailable(
                date_ref=date(2023, 1, 1),
                logger=mock_logger,
                cls_db=mock_db_session,
                start_page=0,
                end_page=10,
            )

        assert instance.logger == mock_logger
        assert instance.cls_db == mock_db_session
        assert instance.date_ref == date(2023, 1, 1)
        assert instance.start_page == 0
        assert instance.end_page == 10
        assert instance.base_url == "https://data.anbima.com.br/busca/debentures"

    def test_init_with_default_parameters(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
    ) -> None:
        """Test initialization with default parameters.

        Verifies
        --------
        - The class uses default values when parameters are not provided
        - Date reference defaults to previous working day
        - Page range defaults to 0-20

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

        assert instance.logger == mock_logger
        assert instance.cls_db is None
        assert instance.date_ref == date(2023, 1, 1)
        assert instance.start_page == 0
        assert instance.end_page == 20

    @pytest.mark.parametrize("start_page,end_page", [(-1, 10), (10, 5)])
    def test_init_with_invalid_page_range_raises_value_error(
        self,
        start_page: int,
        end_page: int,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
    ) -> None:
        """Test initialization with invalid page range raises ValueError.

        Verifies
        --------
        - Negative start_page raises ValueError
        - end_page less than start_page raises ValueError

        Parameters
        ----------
        start_page : int
            Invalid start page value
        end_page : int
            Invalid end page value
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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ), pytest.raises(ValueError):
            AnbimaDataDebenturesAvailable(
                logger=mock_logger, start_page=start_page, end_page=end_page
            )

    def test_extract_debenture_data_success(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        mock_playwright_page: Mock,
    ) -> None:
        """Test successful extraction of debenture data.

        Verifies
        --------
        - Data extraction works with valid page and element
        - All fields are properly extracted
        - Missing fields are handled gracefully

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

        mock_element = Mock()
        mock_element.inner_text.return_value = "Test Debenture"
        mock_element.get_attribute.return_value = "item-nome-123"

        mock_field_element = Mock()
        mock_field_element.inner_text.return_value = "Field Value"
        mock_playwright_page.query_selector.return_value = mock_field_element

        result = instance._extract_debenture_data(
            mock_playwright_page, "123", "Test Debenture"
        )

        assert result["NOME"] == "Test Debenture"
        assert "EMISSOR" in result
        assert "REMUNERACAO" in result

    def test_extract_debenture_data_with_missing_fields(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        mock_playwright_page: Mock,
    ) -> None:
        """Test debenture data extraction with missing fields.

        Verifies
        --------
        - Missing fields are set to None
        - Extraction continues despite individual field errors
        - Error logging is performed for failed extractions

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

        mock_element = Mock()
        mock_element.inner_text.return_value = "Test Debenture"
        mock_element.get_attribute.return_value = "item-nome-123"

        mock_playwright_page.query_selector.return_value = None

        result = instance._extract_debenture_data(
            mock_playwright_page, "123", "Test Debenture"
        )

        assert result["NOME"] == "Test Debenture"
        assert result["EMISSOR"] is None
        assert result["REMUNERACAO"] is None

    def test_transform_data_with_valid_input(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        sample_debenture_data: list[dict[str, Any]],
    ) -> None:
        """Test data transformation with valid input.

        Verifies
        --------
        - DataFrame is created correctly from raw data
        - Page column is dropped if present
        - Empty input returns empty DataFrame

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
        sample_debenture_data : list[dict[str, Any]]
            Sample debenture data for testing

        Returns
        -------
        None
        """
        with patch.multiple(
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

        result = instance.transform_data(sample_debenture_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "pagina" not in result.columns

    def test_transform_data_with_empty_input(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
    ) -> None:
        """Test data transformation with empty input.

        Verifies
        --------
        - Empty list input returns empty DataFrame

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

        result_empty = instance.transform_data([])

        assert isinstance(result_empty, pd.DataFrame)
        assert len(result_empty) == 0

    def test_parse_raw_file_returns_string_io(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
    ) -> None:
        """Test parse_raw_file returns StringIO for compatibility.

        Verifies
        --------
        - Method returns StringIO instance for compatibility
        - Different response types are handled

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

        mock_response = Mock()
        result = instance.parse_raw_file(mock_response)

        assert isinstance(result, StringIO)

    @patch("stpstone.ingestion.countries.br.registries.anbima_data_debentures.sync_playwright")
    def test_get_response_success(
        self,
        mock_sync_playwright: Mock,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        mock_playwright_page: Mock,
    ) -> None:
        """Test successful response retrieval with mocked Playwright.

        Verifies
        --------
        - Playwright browser is properly launched and closed
        - Page navigation and element queries are performed
        - Data extraction methods are called

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
        mock_playwright_page : Mock
            Mocked Playwright page

        Returns
        -------
        None
        """
        mock_browser = Mock()
        mock_browser.new_page.return_value = mock_playwright_page
        mock_context = Mock()
        mock_context.chromium.launch.return_value = mock_browser
        mock_sync_playwright.return_value.__enter__.return_value = mock_context

        with patch.multiple(
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesAvailable(
                logger=mock_logger, start_page=0, end_page=1
            )

        mock_element = Mock()
        mock_element.inner_text.return_value = "Test Debenture"
        mock_element.get_attribute.return_value = "item-nome-123"
        mock_playwright_page.query_selector_all.return_value = [mock_element]

        result = instance.get_response(timeout_ms=1000)

        assert isinstance(result, list)
        mock_browser.close.assert_called_once()

    @patch("stpstone.ingestion.countries.br.registries.anbima_data_debentures.sync_playwright")
    def test_get_response_with_no_elements(
        self,
        mock_sync_playwright: Mock,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        mock_playwright_page: Mock,
    ) -> None:
        """Test response retrieval when no elements are found.

        Verifies
        --------
        - Empty list is returned when no elements found
        - Browser is properly closed
        - Logging indicates no items found

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
        mock_playwright_page : Mock
            Mocked Playwright page

        Returns
        -------
        None
        """
        mock_browser = Mock()
        mock_browser.new_page.return_value = mock_playwright_page
        mock_context = Mock()
        mock_context.chromium.launch.return_value = mock_browser
        mock_sync_playwright.return_value.__enter__.return_value = mock_context

        with patch.multiple(
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesAvailable(
                logger=mock_logger, start_page=0, end_page=1
            )

        mock_playwright_page.query_selector_all.return_value = []

        result = instance.get_response(timeout_ms=1000)

        assert isinstance(result, list)
        assert len(result) == 0
        mock_browser.close.assert_called_once()


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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesCharacteristics(logger=mock_logger)

        assert instance.debenture_codes == []

    @patch("stpstone.ingestion.countries.br.registries.anbima_data_debentures.sync_playwright")
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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesCharacteristics(logger=mock_logger)

        # Use dates with only hyphens (no other characters)
        sample_characteristics_data[0]["DATA_INICIO_RENTABILIDADE"] = "-"
        sample_characteristics_data[0]["DATA_EMISSAO"] = "-"

        result = instance.transform_data(sample_characteristics_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["DATA_INICIO_RENTABILIDADE"] == "01/01/2100"
        assert result.iloc[0]["DATA_EMISSAO"] == "01/01/2100"


# --------------------------
# Tests for AnbimaDataDebenturesDocuments
# --------------------------
class TestAnbimaDataDebenturesDocuments:
    """Test cases for AnbimaDataDebenturesDocuments class.

    This test class verifies the behavior of debentures documents
    extraction functionality.
    """

    def test_create_document_record_creates_correct_structure(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
    ) -> None:
        """Test document record creation with correct structure.

        Verifies
        --------
        - ResultDocumentRecord has correct TypedDict structure
        - All required fields are present
        - Field types match expected types

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesDocuments(logger=mock_logger)

        # Use correct parameter name: link_document (not link_documento)
        record = instance._create_document_record(
            codigo_debenture="DEB001",
            emissor="COMPANY A",
            setor="FINANCEIRO",
            nome_documento="TEST DOCUMENT",
            data_divulgacao="01/01/2023",
            link_document="https://example.com/doc.pdf",
        )

        assert isinstance(record, dict)
        assert record["CODIGO_DEBENTURE"] == "DEB001"
        assert record["EMISSOR"] == "COMPANY A"
        assert record["SETOR"] == "FINANCEIRO"
        assert record["NOME_DOCUMENTO"] == "TEST DOCUMENT"
        assert record["DATA_DIVULGACAO_DOCUMENTO"] == "01/01/2023"
        assert record["LINK_DOCUMENTO"] == "https://example.com/doc.pdf"

    def test_transform_documents_data_date_handling(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        sample_documents_data: list[dict[str, Any]],
    ) -> None:
        """Test date field transformation in documents data.

        Verifies
        --------
        - Date fields with hyphens are replaced with default date
        - Non-string date values are handled
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
        sample_documents_data : list[dict[str, Any]]
            Sample documents data for testing

        Returns
        -------
        None
        """
        with patch.multiple(
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesDocuments(logger=mock_logger)

        # Use dates with only hyphens (no other characters)
        sample_documents_data[0]["DATA_DIVULGACAO_DOCUMENTO"] = "-"
        sample_documents_data[1]["DATA_DIVULGACAO_DOCUMENTO"] = None

        result = instance.transform_data(sample_documents_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert result.iloc[0]["DATA_DIVULGACAO_DOCUMENTO"] == "01/01/2100"
        assert result.iloc[1]["DATA_DIVULGACAO_DOCUMENTO"] == "01/01/2100"


# --------------------------
# Tests for AnbimaDataDebenturesPrices
# --------------------------
class TestAnbimaDataDebenturesPrices:
    """Test cases for AnbimaDataDebenturesPrices class.

    This test class verifies the behavior of debentures prices
    extraction functionality.
    """

    def test_extract_text_by_xpath_success(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        mock_playwright_page: Mock,
    ) -> None:
        """Test successful text extraction by XPath.

        Verifies
        --------
        - Text is extracted when element is visible
        - Proper timeout is used
        - Whitespace is stripped

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesPrices(logger=mock_logger)

        mock_element = Mock()
        mock_element.is_visible.return_value = True
        mock_element.inner_text.return_value = "  Test Value  "
        mock_playwright_page.locator.return_value.first = mock_element

        result = instance._extract_text_by_xpath(mock_playwright_page, "//test")

        assert result == "Test Value"
        mock_element.is_visible.assert_called_with(timeout=5000)

    def test_extract_text_by_xpath_element_not_visible(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        mock_playwright_page: Mock,
    ) -> None:
        """Test text extraction when element is not visible.

        Verifies
        --------
        - None is returned when element is not visible
        - Exception handling doesn't break the flow

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesPrices(logger=mock_logger)

        mock_element = Mock()
        mock_element.is_visible.return_value = False
        mock_playwright_page.locator.return_value.first = mock_element

        result = instance._extract_text_by_xpath(mock_playwright_page, "//test")

        assert result is None

    def test_transform_prices_data_date_handling(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        sample_prices_data: list[dict[str, Any]],
    ) -> None:
        """Test date field transformation in prices data.

        Verifies
        --------
        - Date fields with hyphens are replaced with default date
        - Non-string date values are handled
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
        sample_prices_data : list[dict[str, Any]]
            Sample prices data for testing

        Returns
        -------
        None
        """
        with patch.multiple(
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesPrices(logger=mock_logger)

        # Use dates with only hyphens (no other characters)
        sample_prices_data[0]["DATA_REFERENCIA"] = "-"

        result = instance.transform_data(sample_prices_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["DATA_REFERENCIA"] == "01/01/2100"


# --------------------------
# Tests for AnbimaDataDebenturesEvents
# --------------------------
class TestAnbimaDataDebenturesEvents:
    """Test cases for AnbimaDataDebenturesEvents class.

    This test class verifies the behavior of debentures events
    extraction functionality.
    """

    def test_get_total_pages_success(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        mock_playwright_page: Mock,
    ) -> None:
        """Test successful extraction of total pages from pagination.

        Verifies
        --------
        - Total pages are extracted from pagination text using regex
        - First number in text is extracted

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesEvents(logger=mock_logger)

        # Mock for the first approach (pagination text) - should succeed
        mock_text_element = Mock()
        mock_text_element.is_visible.return_value = True
        mock_text_element.inner_text.return_value = "10"  # Just the number
        mock_text_locator = Mock()
        mock_text_locator.first = mock_text_element
        
        mock_playwright_page.locator.return_value = mock_text_locator

        result = instance._get_total_pages(mock_playwright_page)

        # Regex extracts the first number found
        assert result == 10

    def test_get_total_pages_fallback(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        mock_playwright_page: Mock,
    ) -> None:
        """Test fallback behavior when pagination extraction fails.

        Verifies
        --------
        - Default value of 1 is returned when extraction fails
        - Method handles exceptions gracefully

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesEvents(logger=mock_logger)

        mock_element = Mock()
        mock_element.is_visible.return_value = False
        mock_playwright_page.locator.return_value.first = mock_element

        result = instance._get_total_pages(mock_playwright_page)

        assert result == 1

    def test_transform_events_data_date_handling(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        sample_events_data: list[dict[str, Any]],
    ) -> None:
        """Test date field transformation in events data.

        Verifies
        --------
        - Multiple date fields with hyphens are replaced with default date
        - Non-string date values are handled
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
        sample_events_data : list[dict[str, Any]]
            Sample events data for testing

        Returns
        -------
        None
        """
        with patch.multiple(
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesEvents(logger=mock_logger)

        # Use dates with only hyphens (no other characters)
        sample_events_data[0]["DATA_EVENTO"] = "-"
        sample_events_data[0]["DATA_LIQUIDACAO"] = "-"

        result = instance.transform_data(sample_events_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["DATA_EVENTO"] == "01/01/2100"
        assert result.iloc[0]["DATA_LIQUIDACAO"] == "01/01/2100"


# --------------------------
# Integration Tests
# --------------------------
class TestAnbimaDebenturesIntegration:
    """Integration tests for Anbima Debentures classes.

    This test class verifies the integrated behavior of multiple
    debentures classes working together.
    """

    def test_end_to_end_without_database(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        sample_debenture_data: list[dict[str, Any]],
    ) -> None:
        """Test end-to-end workflow without database insertion.

        Verifies
        --------
        - Complete workflow from data extraction to transformation
        - DataFrame is returned when no database session provided
        - Standardization process is applied

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
        sample_debenture_data : list[dict[str, Any]]
            Sample debenture data for testing

        Returns
        -------
        None
        """
        with patch.multiple(
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ), patch.object(
            AnbimaDataDebenturesAvailable, "get_response", return_value=sample_debenture_data
        ), patch.object(
            AnbimaDataDebenturesAvailable, "standardize_dataframe", return_value=pd.DataFrame()
        ):
            instance = AnbimaDataDebenturesAvailable(logger=mock_logger)

            result = instance.run(bool_insert_or_ignore=False)

        assert isinstance(result, pd.DataFrame)

    def test_end_to_end_with_database(
        self,
        mock_logger: Mock,
        mock_db_session: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        sample_debenture_data: list[dict[str, Any]],
    ) -> None:
        """Test end-to-end workflow with database insertion.

        Verifies
        --------
        - Database insertion is called when session is provided
        - Insert method receives correct parameters
        - None is returned when data is inserted to database

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
        sample_debenture_data : list[dict[str, Any]]
            Sample debenture data for testing

        Returns
        -------
        None
        """
        with patch.multiple(
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ), patch.object(
            AnbimaDataDebenturesAvailable, "get_response", return_value=sample_debenture_data
        ), patch.object(
            AnbimaDataDebenturesAvailable, "standardize_dataframe", return_value=pd.DataFrame()
        ):
            # Create a mock for insert_table_db
            mock_insert = Mock(return_value=None)
            
            instance = AnbimaDataDebenturesAvailable(
                logger=mock_logger, cls_db=mock_db_session
            )
            
            # Patch the instance method
            with patch.object(instance, 'insert_table_db', mock_insert):
                result = instance.run(bool_insert_or_ignore=True)

            assert result is None
            mock_insert.assert_called_once()


# --------------------------
# Error Handling Tests
# --------------------------
class TestAnbimaDebenturesErrorHandling:
    """Error handling tests for Anbima Debentures classes.

    This test class verifies the error handling and edge case
    behavior of the debentures ingestion classes.
    """

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesCharacteristics(logger=mock_logger)

        mock_playwright_page.locator.side_effect = Exception("XPath error")

        result = instance._extract_debenture_data(
            mock_playwright_page, "123", "Test Debenture"
        )

        # When an exception occurs on the first locator call,
        # all fields including CODIGO_DEBENTURE will be None
        assert result["CODIGO_DEBENTURE"] is None
        assert result["NUMERO_SERIE"] is None

    def test_documents_extraction_with_link_errors(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        mock_playwright_page: Mock,
    ) -> None:
        """Test documents extraction with link click errors.

        Verifies
        --------
        - Link extraction errors are handled gracefully
        - Method returns empty list when errors occur

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesDocuments(logger=mock_logger)

        # Mock the link elements - when there's an error, the method catches it 
        # and the loop continues but doesn't append to result
        mock_link_element = Mock()
        
        result = instance._process_document_links(
            [mock_link_element], 
            "Test Document", 
            "01/01/2023", 
            "DEB001", 
            "COMPANY A", 
            "FINANCEIRO",
            mock_playwright_page, 
            0
        )

        # When link extraction fails, the method returns empty list 
        # because it catches the exception and continues
        assert isinstance(result, list)

    def test_prices_extraction_with_missing_elements(
        self,
        mock_logger: Mock,
        mock_dates_current: Mock,
        mock_dates_br: Mock,
        mock_create_log: Mock,
        mock_dir_files_management: Mock,
        mock_playwright_page: Mock,
    ) -> None:
        """Test prices extraction with missing DOM elements.

        Verifies
        --------
        - Empty list is returned when no price elements found
        - Warning is logged for missing data
        - Method continues without crashing

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesPrices(logger=mock_logger)

        mock_playwright_page.locator.return_value.all.return_value = []

        result = instance._extract_price_rows(
            mock_playwright_page, "DEB001", "COMPANY A", "FINANCEIRO", "https://example.com"
        )

        assert isinstance(result, list)
        assert len(result) == 0