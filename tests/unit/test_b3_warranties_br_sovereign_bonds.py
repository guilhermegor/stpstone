"""Unit tests for B3WarrantiesBRSovereignBonds ingestion class."""

from datetime import date
import importlib
from io import BytesIO
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.exchange.b3_warranties_br_sovereign_bonds import (
    B3WarrantiesBRSovereignBonds,
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
        Mocked Response object with predefined content.
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
        Pytest-mock fixture for creating mocks.

    Returns
    -------
    HtmlHandler
        Mocked HtmlHandler object.
    """
    html_handler = mocker.patch(
        "stpstone.ingestion.countries.br.exchange.b3_warranties_br_sovereign_bonds.HtmlHandler"
    )
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
        Pytest-mock fixture for creating mocks.

    Returns
    -------
    DirFilesManagement
        Mocked DirFilesManagement object.
    """
    dir_files_management = mocker.patch(
        "stpstone.ingestion.countries.br.exchange"
        ".b3_warranties_br_sovereign_bonds.DirFilesManagement"
    )
    sample_excel = BytesIO()
    df_ = pd.DataFrame({"test": ["data"]})
    df_.to_excel(sample_excel, index=False, engine="openpyxl")
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
        Pytest-mock fixture for creating mocks.

    Returns
    -------
    DatesBRAnbima
        Mocked DatesBRAnbima object.
    """
    dates_br = mocker.patch(
        "stpstone.ingestion.countries.br.exchange"
        ".b3_warranties_br_sovereign_bonds.DatesBRAnbima"
    )
    dates_br.return_value.add_working_days.return_value = date(2025, 9, 12)
    return dates_br.return_value


@pytest.fixture
def mock_dates_current(mocker: MockerFixture) -> DatesCurrent:
    """Mock DatesCurrent for current date.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks.

    Returns
    -------
    DatesCurrent
        Mocked DatesCurrent object.
    """
    dates_current = mocker.patch(
        "stpstone.ingestion.countries.br.exchange"
        ".b3_warranties_br_sovereign_bonds.DatesCurrent"
    )
    dates_current.return_value.curr_date.return_value = date(2025, 9, 13)
    return dates_current.return_value


@pytest.fixture
def mock_create_log(mocker: MockerFixture) -> CreateLog:
    """Mock CreateLog for logging.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks.

    Returns
    -------
    CreateLog
        Mocked CreateLog object.
    """
    return mocker.patch(
        "stpstone.ingestion.countries.br.exchange"
        ".b3_warranties_br_sovereign_bonds.CreateLog"
    ).return_value


@pytest.fixture
def mock_session() -> Session:
    """Mock database session.

    Returns
    -------
    Session
        Mocked SQLAlchemy session.
    """
    return MagicMock()


@pytest.fixture
def sample_excel_data() -> pd.DataFrame:
    """Sample Excel data for testing.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with test data.
    """
    return pd.DataFrame(
        {
            "Ativo": ["BOND1"],
            "Sub Tipo": ["Type1"],
            "Identificador da Garantia": ["ID1"],
            "Vencimento": ["2025-01-01"],
            "ISIN": ["BRBOND12345"],
        }
    )


@pytest.fixture
def instance(
    mock_html_handler: HtmlHandler,
    mock_dir_files_management: DirFilesManagement,
    mock_dates_br: DatesBRAnbima,
    mock_dates_current: DatesCurrent,
    mock_create_log: CreateLog,
) -> B3WarrantiesBRSovereignBonds:
    """Fixture providing B3WarrantiesBRSovereignBonds instance.

    Parameters
    ----------
    mock_html_handler : HtmlHandler
        Mocked HtmlHandler object.
    mock_dir_files_management : DirFilesManagement
        Mocked DirFilesManagement object.
    mock_dates_br : DatesBRAnbima
        Mocked DatesBRAnbima object.
    mock_dates_current : DatesCurrent
        Mocked DatesCurrent object.
    mock_create_log : CreateLog
        Mocked CreateLog object.

    Returns
    -------
    B3WarrantiesBRSovereignBonds
        B3WarrantiesBRSovereignBonds instance.
    """
    return B3WarrantiesBRSovereignBonds()


# --------------------------
# Tests
# --------------------------
def test_init_default_date(
    mock_html_handler: HtmlHandler,
    mock_dir_files_management: DirFilesManagement,
    mock_dates_br: DatesBRAnbima,
    mock_dates_current: DatesCurrent,
    mock_create_log: CreateLog,
) -> None:
    """Test initialization with default date.

    Verifies
    --------
    - Default date is set to previous working day.
    - Dependencies are initialized properly.

    Parameters
    ----------
    mock_html_handler : HtmlHandler
        Mocked HtmlHandler object.
    mock_dir_files_management : DirFilesManagement
        Mocked DirFilesManagement object.
    mock_dates_br : DatesBRAnbima
        Mocked DatesBRAnbima object.
    mock_dates_current : DatesCurrent
        Mocked DatesCurrent object.
    mock_create_log : CreateLog
        Mocked CreateLog object.

    Returns
    -------
    None
    """
    inst = B3WarrantiesBRSovereignBonds()
    assert inst.date_ref == date(2025, 9, 12)
    mock_dates_current.curr_date.assert_called_once()
    mock_dates_br.add_working_days.assert_called_once_with(date(2025, 9, 13), -1)


def test_init_custom_date(
    mock_html_handler: HtmlHandler,
    mock_dir_files_management: DirFilesManagement,
    mock_dates_br: DatesBRAnbima,
    mock_dates_current: DatesCurrent,
    mock_create_log: CreateLog,
) -> None:
    """Test initialization with custom date.

    Verifies
    --------
    - Custom date is properly set.

    Parameters
    ----------
    mock_html_handler : HtmlHandler
        Mocked HtmlHandler object.
    mock_dir_files_management : DirFilesManagement
        Mocked DirFilesManagement object.
    mock_dates_br : DatesBRAnbima
        Mocked DatesBRAnbima object.
    mock_dates_current : DatesCurrent
        Mocked DatesCurrent object.
    mock_create_log : CreateLog
        Mocked CreateLog object.

    Returns
    -------
    None
    """
    custom_date = date(2025, 1, 1)
    inst = B3WarrantiesBRSovereignBonds(date_ref=custom_date)
    assert inst.date_ref == custom_date


def test_get_response_success(
    instance: B3WarrantiesBRSovereignBonds,
    mock_response: Response,
    mocker: MockerFixture,
) -> None:
    """Test successful HTTP response retrieval.

    Verifies
    --------
    - get_response returns valid Response object.
    - raise_for_status is called.

    Parameters
    ----------
    instance : B3WarrantiesBRSovereignBonds
        B3WarrantiesBRSovereignBonds instance from fixture.
    mock_response : Response
        Mocked Response object.
    mocker : MockerFixture
        Pytest mocker for patching requests.

    Returns
    -------
    None
    """
    mocker.patch("requests.get", return_value=mock_response)
    result = instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
    assert result == mock_response
    mock_response.raise_for_status.assert_called_once()


def test_get_href(
    instance: B3WarrantiesBRSovereignBonds,
    mock_response: Response,
    mock_html_handler: HtmlHandler,
    mocker: MockerFixture,
) -> None:
    """Test href extraction and subsequent request.

    Verifies
    --------
    - _get_href returns valid Response object and URL.
    - lxml_parser and lxml_xpath are called with correct args.

    Parameters
    ----------
    instance : B3WarrantiesBRSovereignBonds
        B3WarrantiesBRSovereignBonds instance from fixture.
    mock_response : Response
        Mocked Response object.
    mock_html_handler : HtmlHandler
        Mocked HtmlHandler object.
    mocker : MockerFixture
        Pytest mocker for patching requests.

    Returns
    -------
    None
    """
    mocker.patch("requests.get", return_value=mock_response)
    result, url_href = instance._get_href(mock_response, timeout=(12.0, 21.0), bool_verify=True)
    assert result == mock_response
    assert url_href == "https://www.b3.com.br/test/path"
    mock_html_handler.lxml_parser.assert_called_once_with(resp_req=mock_response)
    mock_html_handler.lxml_xpath.assert_called_once_with(
        html_content="parsed_html",
        str_xpath='//*[@id="conteudo-principal"]/div[4]/div/div/div[2]/div[1]/div[2]/p/a',
    )


def test_parse_raw_file(
    instance: B3WarrantiesBRSovereignBonds,
    mock_response: Response,
    mock_dir_files_management: DirFilesManagement,
) -> None:
    """Test raw file parsing.

    Verifies
    --------
    - parse_raw_file returns a list of tuples.
    - recursive_unzip_in_memory is called.

    Parameters
    ----------
    instance : B3WarrantiesBRSovereignBonds
        B3WarrantiesBRSovereignBonds instance from fixture.
    mock_response : Response
        Mocked Response object.
    mock_dir_files_management : DirFilesManagement
        Mocked DirFilesManagement object.

    Returns
    -------
    None
    """
    result = instance.parse_raw_file(mock_response)
    assert len(result) == 1
    assert result[0][1] == "test.xlsx"
    mock_dir_files_management.recursive_unzip_in_memory.assert_called_once()


def test_transform_data(
    instance: B3WarrantiesBRSovereignBonds,
    mocker: MockerFixture,
) -> None:
    """Test data transformation into DataFrame.

    Verifies
    --------
    - Column names are renamed as expected.
    - Returns a pandas DataFrame.

    Parameters
    ----------
    instance : B3WarrantiesBRSovereignBonds
        B3WarrantiesBRSovereignBonds instance from fixture.
    mocker : MockerFixture
        Pytest mocker for patching helper methods.

    Returns
    -------
    None
    """
    sample_data = pd.DataFrame(
        {
            "Ativo": ["BOND1"],
            "Sub Tipo": ["Type1"],
            "Identificador da Garantia": ["ID1"],
            "Vencimento": ["2025-01-01"],
        }
    )
    excel_buffer = BytesIO()
    sample_data.to_excel(
        excel_buffer, sheet_name="Títulos Públicos Federais", index=False, engine="openpyxl"
    )
    excel_buffer.seek(0)

    result = instance.transform_data([(excel_buffer, "test.xlsx")], "https://example.com")

    assert isinstance(result, pd.DataFrame)
    assert "ATIVO" in result.columns
    assert "SUBTIPO" in result.columns
    assert "IDENTIFICADOR_GARANTIA" in result.columns
    assert "DATA_VENCIMENTO" in result.columns
    assert "NOME_PLANILHA" in result.columns
    assert "URL_HREF" in result.columns


def test_run_without_db(
    instance: B3WarrantiesBRSovereignBonds,
    mock_response: Response,
    sample_excel_data: pd.DataFrame,
    mocker: MockerFixture,
) -> None:
    """Test run method without database session.

    Verifies
    --------
    - Full ingestion pipeline without DB returns DataFrame.
    - Expected columns are present after transformation.

    Parameters
    ----------
    instance : B3WarrantiesBRSovereignBonds
        B3WarrantiesBRSovereignBonds instance from fixture.
    mock_response : Response
        Mocked Response object.
    sample_excel_data : pd.DataFrame
        Sample DataFrame from fixture.
    mocker : MockerFixture
        Pytest mocker for patching requests.

    Returns
    -------
    None
    """
    mocker.patch("requests.get", return_value=mock_response)
    mocker.patch.object(
        instance, "_get_href", return_value=(mock_response, "https://www.b3.com.br/test/path")
    )

    excel_buffer = BytesIO()
    sample_excel_data.to_excel(
        excel_buffer, sheet_name="Títulos Públicos Federais", index=False, engine="openpyxl"
    )
    excel_buffer.seek(0)
    mocker.patch.object(instance, "parse_raw_file", return_value=[(excel_buffer, "test.xlsx")])

    expected_df = pd.DataFrame(
        {"ATIVO": ["BOND1"], "SUBTIPO": ["Type1"], "IDENTIFICADOR_GARANTIA": ["ID1"]}
    )
    mocker.patch(
        "stpstone.ingestion.abc.ingestion_abc.ABCIngestionOperations.standardize_dataframe",
        return_value=expected_df,
    )

    result = instance.run(timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=False)
    assert isinstance(result, pd.DataFrame)
    expected_columns = ["ATIVO", "SUBTIPO", "IDENTIFICADOR_GARANTIA"]
    assert all(col in result.columns for col in expected_columns)


def test_run_with_db(
    instance: B3WarrantiesBRSovereignBonds,
    mock_response: Response,
    mock_session: Session,
    sample_excel_data: pd.DataFrame,
    mocker: MockerFixture,
) -> None:
    """Test run method with database session.

    Verifies
    --------
    - Full ingestion pipeline with DB returns None.
    - insert_table_db is called once.

    Parameters
    ----------
    instance : B3WarrantiesBRSovereignBonds
        B3WarrantiesBRSovereignBonds instance from fixture.
    mock_response : Response
        Mocked Response object.
    mock_session : Session
        Mocked Session object.
    sample_excel_data : pd.DataFrame
        Sample DataFrame from fixture.
    mocker : MockerFixture
        Pytest mocker for patching requests.

    Returns
    -------
    None
    """
    instance.cls_db = mock_session
    mocker.patch("requests.get", return_value=mock_response)
    mocker.patch.object(
        instance, "_get_href", return_value=(mock_response, "https://www.b3.com.br/test/path")
    )

    excel_buffer = BytesIO()
    sample_excel_data.to_excel(
        excel_buffer, sheet_name="Títulos Públicos Federais", index=False, engine="openpyxl"
    )
    excel_buffer.seek(0)
    mocker.patch.object(instance, "parse_raw_file", return_value=[(excel_buffer, "test.xlsx")])
    mocker.patch.object(instance, "insert_table_db")
    mocker.patch(
        "stpstone.ingestion.abc.ingestion_abc.ABCIngestionOperations.standardize_dataframe",
        return_value=pd.DataFrame({"ATIVO": ["BOND1"]}),
    )

    result = instance.run(timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=True)
    assert result is None
    instance.insert_table_db.assert_called_once()


def test_invalid_timeout_string(
    mock_html_handler: HtmlHandler,
    mock_dir_files_management: DirFilesManagement,
    mock_dates_br: DatesBRAnbima,
    mock_dates_current: DatesCurrent,
    mock_create_log: CreateLog,
) -> None:
    """Test invalid string timeout value.

    Verifies
    --------
    - Raises TypeError for invalid timeout types.
    - Error message matches expected pattern.

    Parameters
    ----------
    mock_html_handler : HtmlHandler
        Mocked HtmlHandler object.
    mock_dir_files_management : DirFilesManagement
        Mocked DirFilesManagement object.
    mock_dates_br : DatesBRAnbima
        Mocked DatesBRAnbima object.
    mock_dates_current : DatesCurrent
        Mocked DatesCurrent object.
    mock_create_log : CreateLog
        Mocked CreateLog object.

    Returns
    -------
    None
    """
    inst = B3WarrantiesBRSovereignBonds()
    with pytest.raises(TypeError, match="timeout must be one of types"):
        inst.get_response(timeout="invalid")


def test_valid_timeout_none(
    mock_html_handler: HtmlHandler,
    mock_dir_files_management: DirFilesManagement,
    mock_dates_br: DatesBRAnbima,
    mock_dates_current: DatesCurrent,
    mock_create_log: CreateLog,
    mocker: MockerFixture,
) -> None:
    """Test valid None timeout value.

    Verifies
    --------
    - get_response returns the mocked response object.
    - None is a valid timeout value.

    Parameters
    ----------
    mock_html_handler : HtmlHandler
        Mocked HtmlHandler object.
    mock_dir_files_management : DirFilesManagement
        Mocked DirFilesManagement object.
    mock_dates_br : DatesBRAnbima
        Mocked DatesBRAnbima object.
    mock_dates_current : DatesCurrent
        Mocked DatesCurrent object.
    mock_create_log : CreateLog
        Mocked CreateLog object.
    mocker : MockerFixture
        Pytest mocker for patching requests.

    Returns
    -------
    None
    """
    inst = B3WarrantiesBRSovereignBonds()
    mock_resp = MagicMock(spec=Response)
    mocker.patch("requests.get", return_value=mock_resp)
    result = inst.get_response(timeout=None)
    assert result == mock_resp


def test_empty_file_list(
    mock_html_handler: HtmlHandler,
    mock_dir_files_management: DirFilesManagement,
    mock_dates_br: DatesBRAnbima,
    mock_dates_current: DatesCurrent,
    mock_create_log: CreateLog,
) -> None:
    """Test empty file list in transform_data.

    Verifies
    --------
    - transform_data returns an empty DataFrame for an empty file list.

    Parameters
    ----------
    mock_html_handler : HtmlHandler
        Mocked HtmlHandler object.
    mock_dir_files_management : DirFilesManagement
        Mocked DirFilesManagement object.
    mock_dates_br : DatesBRAnbima
        Mocked DatesBRAnbima object.
    mock_dates_current : DatesCurrent
        Mocked DatesCurrent object.
    mock_create_log : CreateLog
        Mocked CreateLog object.

    Returns
    -------
    None
    """
    inst = B3WarrantiesBRSovereignBonds()
    result = inst.transform_data([], "https://example.com")
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_module_reload(
    mocker: MockerFixture,
    mock_html_handler: HtmlHandler,
    mock_dir_files_management: DirFilesManagement,
) -> None:
    """Test module reload preserves functionality.

    Verifies
    --------
    - Module can be reloaded without errors.
    - URL attribute is preserved after reload.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker for patching requests.
    mock_html_handler : HtmlHandler
        Mocked HtmlHandler object.
    mock_dir_files_management : DirFilesManagement
        Mocked DirFilesManagement object.

    Returns
    -------
    None
    """
    import stpstone.ingestion.countries.br.exchange.b3_warranties_br_sovereign_bonds

    importlib.reload(stpstone.ingestion.countries.br.exchange.b3_warranties_br_sovereign_bonds)
    inst = (
        stpstone.ingestion.countries.br.exchange.b3_warranties_br_sovereign_bonds
        .B3WarrantiesBRSovereignBonds()
    )
    assert "garantias-aceitas" in inst.url
