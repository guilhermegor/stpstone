"""Unit tests for AnbimaDataDebenturesDocuments ingestion class."""

from datetime import date
from logging import Logger
from typing import Any
from unittest.mock import Mock, patch

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_debentures_documents import (
    AnbimaDataDebenturesDocuments,
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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures_documents",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesDocuments(logger=mock_logger)

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures_documents",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesDocuments(logger=mock_logger)

        sample_documents_data[0]["DATA_DIVULGACAO_DOCUMENTO"] = "-"
        sample_documents_data[1]["DATA_DIVULGACAO_DOCUMENTO"] = None

        result = instance.transform_data(sample_documents_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert result.iloc[0]["DATA_DIVULGACAO_DOCUMENTO"] == "01/01/2100"
        assert result.iloc[1]["DATA_DIVULGACAO_DOCUMENTO"] == "01/01/2100"


# --------------------------
# Error Handling Tests
# --------------------------
class TestAnbimaDebenturesDocumentsErrorHandling:
    """Error handling tests for AnbimaDataDebenturesDocuments class."""

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
        - Method returns list regardless of link errors

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
            "stpstone.ingestion.countries.br.registries.anbima_data_debentures_documents",
            DatesCurrent=Mock(return_value=mock_dates_current),
            DatesBRAnbima=Mock(return_value=mock_dates_br),
            CreateLog=Mock(return_value=mock_create_log),
            DirFilesManagement=Mock(return_value=mock_dir_files_management),
        ):
            instance = AnbimaDataDebenturesDocuments(logger=mock_logger)

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

        assert isinstance(result, list)
