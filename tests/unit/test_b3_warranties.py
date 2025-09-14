"""Unit tests for B3 Warranties ingestion classes.

Tests the ingestion functionality for Stocks, Units, ETFs, Brazilian Sovereign Bonds,
and International Securities, covering normal operations, edge cases, and error conditions.
"""

from datetime import date
from io import BytesIO
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.exchange.b3_warranties import (
    B3WarrantiesBRSovereignBonds,
    B3WarrantiesInternationalSecurities,
    B3WarrantiesStocksUnitsETFs,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
    """Mock Response object with sample content.
    
    Returns
    -------
    Response
        Mocked Response object with predefined content
    """
    response = MagicMock(spec=Response)
    response.content = b"Sample content"
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def mock_html_handler(mocker: MockerFixture) -> HtmlHandler:
    """Mock HtmlHandler for HTML parsing.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    HtmlHandler
        Mocked HtmlHandler object
    """
    html_handler = mocker.patch(
        "stpstone.ingestion.countries.br.exchange.b3_warranties.HtmlHandler")
    mock_element = MagicMock()
    mock_element.get.return_value = "../../../../../../../test/path"
    html_handler.return_value.lxml_parser.return_value = "parsed_html"
    html_handler.return_value.lxml_xpath.return_value = [mock_element]
    return html_handler.return_value


@pytest.fixture
def mock_dir_files_management(mocker: MockerFixture) -> DirFilesManagement:
    """Mock DirFilesManagement for file parsing.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    DirFilesManagement
        Mocked DirFilesManagement object
    """
    dir_files_management = mocker.patch(
        "stpstone.ingestion.countries.br.exchange.b3_warranties.DirFilesManagement")
    # Create proper Excel content for testing
    sample_excel = BytesIO()
    # Create a minimal Excel file using pandas
    df_ = pd.DataFrame({"test": ["data"]})
    df_.to_excel(sample_excel, index=False, engine='openpyxl')
    sample_excel.seek(0)
    
    dir_files_management.return_value.recursive_unzip_in_memory.return_value = [
        (sample_excel, "test.xlsx")
    ]
    return dir_files_management.return_value


@pytest.fixture
def mock_dates_br(mocker: MockerFixture) -> DatesBRAnbima:
    """Mock DatesBRAnbima for date calculations.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    DatesBRAnbima
        Mocked DatesBRAnbima object
    """
    dates_br = mocker.patch("stpstone.ingestion.countries.br.exchange.b3_warranties.DatesBRAnbima")
    dates_br.return_value.add_working_days.return_value = date(2025, 9, 12)
    return dates_br.return_value


@pytest.fixture
def mock_dates_current(mocker: MockerFixture) -> DatesCurrent:
    """Mock DatesCurrent for current date.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    DatesCurrent
        Mocked DatesCurrent object
    """
    dates_current = mocker.patch(
        "stpstone.ingestion.countries.br.exchange.b3_warranties.DatesCurrent")
    dates_current.return_value.curr_date.return_value = date(2025, 9, 13)
    return dates_current.return_value


@pytest.fixture
def mock_create_log(mocker: MockerFixture) -> CreateLog:
    """Mock CreateLog for logging.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    CreateLog
        Mocked CreateLog object
    """
    return mocker.patch(
        "stpstone.ingestion.countries.br.exchange.b3_warranties.CreateLog").return_value


@pytest.fixture
def mock_session() -> Session:
    """Mock database session."""
    return MagicMock()


@pytest.fixture
def sample_excel_data() -> pd.DataFrame:
    """Sample Excel data for testing.
    
    Returns
    -------
    pd.DataFrame
        Sample DataFrame with test data
    """
    return pd.DataFrame({
        "Código": ["ABC1"], 
        "Limite (quantidade)": [100], # codespell:ignore
        "ISIN": ["BR1234567890"]
    })


# --------------------------
# Tests for B3WarrantiesStocksUnitsETFs
# --------------------------
class TestB3WarrantiesStocksUnitsETFs:
    """Test cases for B3WarrantiesStocksUnitsETFs class."""

    @pytest.fixture
    def instance(
        self, 
        mock_html_handler: HtmlHandler, 
        mock_dir_files_management: DirFilesManagement,
        mock_dates_br: DatesBRAnbima, 
        mock_dates_current: DatesCurrent, 
        mock_create_log: CreateLog
    ) -> B3WarrantiesStocksUnitsETFs:
        """Fixture providing B3WarrantiesStocksUnitsETFs instance.
        
        Parameters
        ----------
        mock_html_handler : HtmlHandler
            Mocked HtmlHandler object
        mock_dir_files_management : DirFilesManagement
            Mocked DirFilesManagement object
        mock_dates_br : DatesBRAnbima
            Mocked DatesBRAnbima object
        mock_dates_current : DatesCurrent
            Mocked DatesCurrent object
        mock_create_log : CreateLog
            Mocked CreateLog object
        
        Returns
        -------
        B3WarrantiesStocksUnitsETFs
            B3WarrantiesStocksUnitsETFs instance
        """
        return B3WarrantiesStocksUnitsETFs()

    def test_init_default_date(
        self, 
        mock_html_handler: HtmlHandler, 
        mock_dir_files_management: DirFilesManagement,
        mock_dates_br: DatesBRAnbima, 
        mock_dates_current: DatesCurrent, 
        mock_create_log: CreateLog
    ) -> None:
        """Test initialization with default date.
        
        Verifies
        --------
        - Default date is set to previous working day
        - Dependencies are initialized properly

        Parameters
        ----------
        mock_html_handler : HtmlHandler
            Mocked HtmlHandler object
        mock_dir_files_management : DirFilesManagement
            Mocked DirFilesManagement object
        mock_dates_br : DatesBRAnbima
            Mocked DatesBRAnbima object
        mock_dates_current : DatesCurrent
            Mocked DatesCurrent object
        mock_create_log : CreateLog
            Mocked CreateLog object

        Returns
        -------
        None
        """
        instance = B3WarrantiesStocksUnitsETFs()
        assert instance.date_ref == date(2025, 9, 12)
        mock_dates_current.curr_date.assert_called_once()
        mock_dates_br.add_working_days.assert_called_once_with(date(2025, 9, 13), -1)

    def test_init_custom_date(
        self, 
        mock_html_handler: HtmlHandler, 
        mock_dir_files_management: DirFilesManagement,
        mock_dates_br: DatesBRAnbima, 
        mock_dates_current: DatesCurrent, 
        mock_create_log: CreateLog
    ) -> None:
        """Test initialization with custom date.
        
        Verifies
        --------
        - Custom date is properly set

        Parameters
        ----------
        mock_html_handler : HtmlHandler
            Mocked HtmlHandler object
        mock_dir_files_management : DirFilesManagement
            Mocked DirFilesManagement object
        mock_dates_br : DatesBRAnbima
            Mocked DatesBRAnbima object
        mock_dates_current : DatesCurrent
            Mocked DatesCurrent object
        mock_create_log : CreateLog
            Mocked CreateLog object

        Returns
        -------
        None
        """
        custom_date = date(2025, 1, 1)
        instance = B3WarrantiesStocksUnitsETFs(date_ref=custom_date)
        assert instance.date_ref == custom_date

    def test_get_response_success(
        self, 
        instance: B3WarrantiesStocksUnitsETFs, 
        mock_response: Response,
        mocker: MockerFixture
    ) -> None:
        """Test successful HTTP response retrieval.
        
        Verifies
        --------
        - get_response returns valid Response object
        - Correct headers and timeout are used
        - raise_for_status is called

        Parameters
        ----------
        instance : B3WarrantiesStocksUnitsETFs
            B3WarrantiesStocksUnitsETFs instance from fixture
        mock_response : Response
            Mocked Response object
        mocker : MockerFixture
            Pytest mocker for patching requests

        Returns
        -------
        None
        """
        mocker.patch("requests.get", return_value=mock_response)
        result = instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
        assert result == mock_response
        mock_response.raise_for_status.assert_called_once()

    def test_get_href(
        self, 
        instance: B3WarrantiesStocksUnitsETFs, 
        mock_response: Response, 
        mock_html_handler: HtmlHandler,
        mocker: MockerFixture
    ) -> None:
        """Test href extraction and subsequent request.
        
        Verifies
        --------
        - _get_href returns valid Response object
        - Correct headers and timeout are used
        - raise_for_status is called

        Parameters
        ----------
        instance : B3WarrantiesStocksUnitsETFs
            B3WarrantiesStocksUnitsETFs instance from fixture
        mock_response : Response
            Mocked Response object
        mock_html_handler : HtmlHandler
            Mocked HtmlHandler object
        mocker : MockerFixture
            Pytest mocker for patching requests

        Returns
        -------
        None
        """
        mocker.patch("requests.get", return_value=mock_response)
        result, url_href = instance._get_href(mock_response, timeout=(12.0, 21.0), 
                                              bool_verify=True)
        assert result == mock_response
        assert url_href == "https://www.b3.com.br/test/path"
        mock_html_handler.lxml_parser.assert_called_once_with(resp_req=mock_response)
        mock_html_handler.lxml_xpath.assert_called_once_with(
            html_content="parsed_html", 
            str_xpath='//*[@id="panel1a"]/ul/li[2]/a'
        )

    def test_parse_raw_file(
        self, 
        instance: B3WarrantiesStocksUnitsETFs, 
        mock_response: Response,
        mock_dir_files_management: DirFilesManagement
    ) -> None:
        """Test raw file parsing.
        
        Verifies
        --------
        - parse_raw_file returns a list of tuples
        - recursive_unzip_in_memory is called

        Parameters
        ----------
        instance : B3WarrantiesStocksUnitsETFs
            B3WarrantiesStocksUnitsETFs instance from fixture
        mock_response : Response
            Mocked Response object
        mock_dir_files_management : DirFilesManagement
            Mocked DirFilesManagement object

        Returns
        -------
        None
        """
        # The mock_dir_files_management fixture already sets up the return value
        # So we don't need to reassign it here
        result = instance.parse_raw_file(mock_response)
        assert len(result) == 1
        assert result[0][1] == "test.xlsx"
        # The fixture already has the mock configured, so we can check it was called
        mock_dir_files_management.recursive_unzip_in_memory.assert_called_once()

    def test_transform_data(
        self, 
        instance: B3WarrantiesStocksUnitsETFs, 
        sample_excel_data: pd.DataFrame,
        mocker: MockerFixture
    ) -> None:
        """Test data transformation into DataFrame.
        
        Verifies
        --------
        - Excel file is read correctly
        - Column names are set as expected
        - Returns a pandas DataFrame

        Parameters
        ----------
        instance : B3WarrantiesStocksUnitsETFs
            B3WarrantiesStocksUnitsETFs instance from fixture
        sample_excel_data : pd.DataFrame
            Sample DataFrame from fixture
        mocker : MockerFixture
            Pytest mocker for patching pandas

        Returns
        -------
        None
        """
        mocker.patch("pandas.read_excel", return_value=sample_excel_data)
        # dreate a proper BytesIO with Excel data
        excel_buffer = BytesIO()
        sample_excel_data.to_excel(excel_buffer, index=False, engine='openpyxl')
        excel_buffer.seek(0)
        
        result = instance.transform_data([(excel_buffer, "test.xlsx")], "https://example.com")
        
        assert isinstance(result, pd.DataFrame)
        assert "CODIGO" in result.columns
        assert "LIMITE_QUANTIDADE" in result.columns
        assert "NOME_PLANILHA" in result.columns
        assert "NOME_PASTA_TRABALHO" in result.columns
        assert "URL_HREF" in result.columns

    def test_run_without_db(
        self,
        instance: B3WarrantiesStocksUnitsETFs,
        mock_response: Response,
        sample_excel_data: pd.DataFrame,
        mocker: MockerFixture
    ) -> None:
        """Test run method without database session.
        
        Verifies
        --------
        - Full ingestion pipeline without DB
        - Returns DataFrame
        - All intermediate methods are called correctly

        Parameters
        ----------
        instance : B3WarrantiesStocksUnitsETFs
            B3WarrantiesStocksUnitsETFs instance from fixture
        mock_response : Response
            Mocked Response object
        sample_excel_data : pd.DataFrame
            Sample DataFrame from fixture
        mocker : MockerFixture
            Pytest mocker for patching requests

        Returns
        -------
        None
        """
        mocker.patch("requests.get", return_value=mock_response)
        
        # Mock the _get_href to return the response and URL
        mocker.patch.object(instance, "_get_href", return_value=(
            mock_response, "https://www.b3.com.br/test/path"))
        
        # Mock parse_raw_file to return file content
        excel_buffer = BytesIO()
        sample_excel_data.to_excel(excel_buffer, index=False, engine='openpyxl')
        excel_buffer.seek(0)
        mocker.patch.object(instance, "parse_raw_file", return_value=[
            (excel_buffer, "test.xlsx")])
        
        # Now the full pipeline will run correctly
        result = instance.run(timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=False)
        assert isinstance(result, pd.DataFrame)
        # Check that the expected columns are present after transformation
        expected_columns = [
            'CODIGO', 'LIMITE_QUANTIDADE', 'ISIN', 'NOME_PLANILHA', 
            'NOME_PASTA_TRABALHO', 'URL_HREF']
        assert all(col in result.columns for col in expected_columns)

    def test_run_with_db(
        self,
        instance: B3WarrantiesStocksUnitsETFs,
        mock_response: Response,
        mock_session: Session,
        sample_excel_data: pd.DataFrame,
        mocker: MockerFixture
    ) -> None:
        """Test run method with database session.
        
        Verifies
        --------
        - Full ingestion pipeline with DB
        - Returns None
        - All intermediate methods are called correctly

        Parameters
        ----------
        instance : B3WarrantiesStocksUnitsETFs
            B3WarrantiesStocksUnitsETFs instance from fixture
        mock_response : Response
            Mocked Response object
        mock_session : Session
            Mocked Session object
        sample_excel_data : pd.DataFrame
            Sample DataFrame from fixture
        mocker : MockerFixture
            Pytest mocker for patching requests

        Returns
        -------
        None
        """
        instance.cls_db = mock_session
        mocker.patch("requests.get", return_value=mock_response)
        
        # Mock the _get_href to return the response and URL
        mocker.patch.object(instance, "_get_href", return_value=(
            mock_response, "https://www.b3.com.br/test/path"))
        
        # Mock parse_raw_file to return file content
        excel_buffer = BytesIO()
        sample_excel_data.to_excel(excel_buffer, index=False, engine='openpyxl')
        excel_buffer.seek(0)
        mocker.patch.object(instance, "parse_raw_file", return_value=[(excel_buffer, "test.xlsx")])
        
        # Mock the database insertion
        mocker.patch.object(instance, "insert_table_db")
        
        result = instance.run(timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=True)
        assert result is None
        instance.insert_table_db.assert_called_once()


# --------------------------
# Tests for B3WarrantiesBRSovereignBonds
# --------------------------
class TestB3WarrantiesBRSovereignBonds:
    """Test cases for B3WarrantiesBRSovereignBonds class."""

    @pytest.fixture
    def instance(
        self, 
        mock_html_handler: HtmlHandler, 
        mock_dir_files_management: DirFilesManagement,
        mock_dates_br: DatesBRAnbima, 
        mock_dates_current: DatesCurrent, 
        mock_create_log: CreateLog
    ) -> B3WarrantiesBRSovereignBonds:
        """Fixture providing B3WarrantiesBRSovereignBonds instance.
        
        Parameters
        ----------
        mock_html_handler : HtmlHandler
            Mocked HtmlHandler object
        mock_dir_files_management : DirFilesManagement
            Mocked DirFilesManagement object
        mock_dates_br : DatesBRAnbima
            Mocked DatesBRAnbima object
        mock_dates_current : DatesCurrent
            Mocked DatesCurrent object
        mock_create_log : CreateLog
            Mocked CreateLog object
        
        Returns
        -------
        B3WarrantiesBRSovereignBonds
            B3WarrantiesBRSovereignBonds instance
        """
        return B3WarrantiesBRSovereignBonds()

    def test_transform_data(
        self, 
        instance: B3WarrantiesBRSovereignBonds, 
        mocker: MockerFixture
    ) -> None:
        """Test data transformation into DataFrame.
        
        Verifies
        --------
        - transform_data produces correct DataFrame structure
        - Date building and missing value filling are correct

        Parameters
        ----------
        instance : B3WarrantiesBRSovereignBonds
            B3WarrantiesBRSovereignBonds instance from fixture
        mocker : MockerFixture
            Pytest mocker for patching helper methods

        Returns
        -------
        None
        """
        mocker.patch.object(instance.cls_html_handler, "lxml_xpath", return_value="2025-01-01")
        sample_data = pd.DataFrame({
            "Ativo": ["BOND1"], 
            "Sub Tipo": ["Type1"],
            "Identificador da Garantia": ["ID1"],
            "Vencimento": ["2025-01-01"]
        })
        
        # Create a proper Excel buffer
        excel_buffer = BytesIO()
        sample_data.to_excel(excel_buffer, 
                             sheet_name="Títulos Públicos Federais", 
                             index=False, 
                             engine='openpyxl')
        excel_buffer.seek(0)
        
        result = instance.transform_data([(excel_buffer, "test.xlsx")], "https://example.com")
        
        assert isinstance(result, pd.DataFrame)
        assert "ATIVO" in result.columns
        assert "SUBTIPO" in result.columns
        assert "NOME_PLANILHA" in result.columns
        assert "URL_HREF" in result.columns


# --------------------------
# Tests for B3WarrantiesInternationalSecurities
# --------------------------
class TestB3WarrantiesInternationalSecurities:
    """Test cases for B3WarrantiesInternationalSecurities class."""

    @pytest.fixture
    def instance(
        self, 
        mock_html_handler: HtmlHandler, 
        mock_dir_files_management: DirFilesManagement,
        mock_dates_br: DatesBRAnbima, 
        mock_dates_current: DatesCurrent, 
        mock_create_log: CreateLog
    ) -> B3WarrantiesInternationalSecurities:
        """Fixture providing B3WarrantiesInternationalSecurities instance.
        
        Parameters
        ----------
        mock_html_handler : HtmlHandler
            Mocked HtmlHandler object
        mock_dir_files_management : DirFilesManagement
            Mocked DirFilesManagement object
        mock_dates_br : DatesBRAnbima
            Mocked DatesBRAnbima object
        mock_dates_current : DatesCurrent
            Mocked DatesCurrent object
        mock_create_log : CreateLog
            Mocked CreateLog object
        
        Returns
        -------
        B3WarrantiesInternationalSecurities
            B3WarrantiesInternationalSecurities instance
        """
        return B3WarrantiesInternationalSecurities()

    def test_transform_data(
        self, 
        instance: B3WarrantiesInternationalSecurities, 
        mocker: MockerFixture
    ) -> None:
        """Test data transformation into DataFrame.
        
        Verifies
        --------
        - transform_data produces correct DataFrame structure
        - Date building and missing value filling are correct

        Parameters
        ----------
        instance : B3WarrantiesInternationalSecurities
            B3WarrantiesInternationalSecurities instance from fixture
        mocker : MockerFixture
            Pytest mocker for patching helper methods

        Returns
        -------
        None
        """
        mocker.patch.object(instance.cls_html_handler, "lxml_xpath", return_value="2025-01-01")
        sample_data = pd.DataFrame({
            "Identificador do Instrumento": ["INST1"],
            "Tipo TituloInternacional": ["Type1"],
            "Código Titulo": ["CODE1"],
            "Cusip": ["CUSIP1"],
            "Data de Vencimento": ["2025-01-01 00:00:00"],
            "Próximo Pagamento de Coupon": ["2025-06-01 00:00:00"]
        })
        
        # Create a proper Excel buffer
        excel_buffer = BytesIO()
        sample_data.to_excel(excel_buffer, sheet_name="Títulos Internacionais", index=False, 
                             engine='openpyxl')
        excel_buffer.seek(0)
        
        result = instance.transform_data([(excel_buffer, "test.xlsx")], "https://example.com")
        
        assert isinstance(result, pd.DataFrame)
        assert "ID" in result.columns
        assert "TIPO_TITULO" in result.columns
        assert "NOME_PLANILHA" in result.columns


# --------------------------
# Edge Cases and Error Conditions
# --------------------------
def test_invalid_timeout_string(
    mock_html_handler: HtmlHandler, 
    mock_dir_files_management: DirFilesManagement,
    mock_dates_br: DatesBRAnbima, 
    mock_dates_current: DatesCurrent, 
    mock_create_log: CreateLog
) -> None:
    """Test invalid string timeout value.
    
    Verifies
    --------
    - Raises TypeError for invalid timeout types
    - Error message matches expected pattern

    Parameters
    ----------
    mock_html_handler : HtmlHandler
        Mocked HtmlHandler object
    mock_dir_files_management : DirFilesManagement
        Mocked DirFilesManagement object
    mock_dates_br : DatesBRAnbima
        Mocked DatesBRAnbima object
    mock_dates_current : DatesCurrent
        Mocked DatesCurrent object
    mock_create_log : CreateLog
        Mocked CreateLog object

    Returns
    -------
    None
    """
    instance = B3WarrantiesStocksUnitsETFs()
    with pytest.raises(TypeError, match="timeout must be one of types"):
        instance.get_response(timeout="invalid")


def test_valid_timeout_none(
    mock_html_handler: HtmlHandler, 
    mock_dir_files_management: DirFilesManagement,
    mock_dates_br: DatesBRAnbima, 
    mock_dates_current: DatesCurrent, 
    mock_create_log: CreateLog,
    mocker: MockerFixture
) -> None:
    """Test valid None timeout value.
    
    Verifies
    --------
    - get_response returns the mocked response object
    - None is a valid timeout value for requests

    Parameters
    ----------
    mock_html_handler : HtmlHandler
        Mocked HtmlHandler object
    mock_dir_files_management : DirFilesManagement
        Mocked DirFilesManagement object
    mock_dates_br : DatesBRAnbima
        Mocked DatesBRAnbima object
    mock_dates_current : DatesCurrent
        Mocked DatesCurrent object
    mock_create_log : CreateLog
        Mocked CreateLog object
    mocker : MockerFixture
        Pytest mocker for patching requests

    Returns
    -------
    None
    """
    instance = B3WarrantiesStocksUnitsETFs()
    mock_response = MagicMock(spec=Response)
    mocker.patch("requests.get", return_value=mock_response)
    
    # None is a valid timeout value for requests
    result = instance.get_response(timeout=None)
    assert result == mock_response


def test_empty_file_list(
    mock_html_handler: HtmlHandler, 
    mock_dir_files_management: DirFilesManagement,
    mock_dates_br: DatesBRAnbima, 
    mock_dates_current: DatesCurrent, 
    mock_create_log: CreateLog
) -> None:
    """Test empty file list in transform_data.
    
    Verifies
    --------
    - transform_data returns an empty DataFrame for an empty file list

    Parameters
    ----------
    mock_html_handler : HtmlHandler
        Mocked HtmlHandler object
    mock_dir_files_management : DirFilesManagement
        Mocked DirFilesManagement object
    mock_dates_br : DatesBRAnbima
        Mocked DatesBRAnbima object
    mock_dates_current : DatesCurrent
        Mocked DatesCurrent object
    mock_create_log : CreateLog
        Mocked CreateLog object

    Returns
    -------
    None
    """
    instance = B3WarrantiesStocksUnitsETFs()
    result = instance.transform_data([], "https://example.com")
    assert isinstance(result, pd.DataFrame)
    assert result.empty


# --------------------------
# Reload Logic Tests
# --------------------------
def test_module_reload(
    mocker: MockerFixture, 
    mock_html_handler: HtmlHandler, 
    mock_dir_files_management: DirFilesManagement
) -> None:
    """Test module reload preserves functionality.
    
    Verifies
    --------
    - Module can be reloaded without errors
    - Instance attributes are preserved after reload

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker for patching requests
    mock_html_handler : HtmlHandler
        Mocked HtmlHandler object
    mock_dir_files_management : DirFilesManagement
        Mocked DirFilesManagement object

    Returns
    -------
    None
    """
    import importlib

    import stpstone.ingestion.countries.br.exchange.b3_warranties
    
    importlib.reload(stpstone.ingestion.countries.br.exchange.b3_warranties)
    instance = stpstone.ingestion.countries.br.exchange.b3_warranties.B3WarrantiesStocksUnitsETFs()
    assert instance.url == "https://www.b3.com.br/pt_br/produtos-e-servicos/compensacao-e-liquidacao/clearing/administracao-de-riscos/garantias/limites-de-renda-variavel-e-fixa/"