"""Unit tests for ABCIngestionOperations class.

Tests the ingestion operations functionality with various input scenarios including:
- Initialization with valid and invalid inputs
- Response handling
- File parsing and transformation
- Data standardization and database insertion
- Edge cases and error conditions
"""

from datetime import date
from io import BytesIO, StringIO
from typing import Optional, TypedDict, Union
from unittest.mock import MagicMock, Mock

import fitz
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentAggregator,
    ContentParser,
)
from stpstone.transformations.standardization.standardizer_df import DFStandardization
from stpstone.utils.loggs.db_logs import DBLogs
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler


# --------------------------
# Concrete Class for Testing
# --------------------------
class ConcreteIngestion(ABCIngestionOperations):
    """Concrete implementation of ABCIngestionOperations for testing."""

    def __init__(self, cls_db: Optional[Session] = None) -> None:
        """Initialize the ConcreteIngestion class.

        Parameters
        ----------
        cls_db : Optional[Session]
            The database session, by default None.

        Returns
        -------
        None
        """
        super().__init__(cls_db=cls_db)
        self.cls_db_logs = DBLogs()
        self.cls_handling_dicts = HandlingDicts()
        self.cls_str_handler = StrHandler()

    def get_response(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0)
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a mock response object for testing.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout value, by default (12.0, 21.0)

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            Mock response object
        """
        response = MagicMock(spec=Response)
        response.text = "mock content"
        return response

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> StringIO:
        """Parse the raw file content for testing.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.

        Returns
        -------
        StringIO
            Mock parsed content
        """
        return StringIO(resp_req.text)

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame for testing.

        Parameters
        ----------
        file : StringIO
            The file content.

        Returns
        -------
        pd.DataFrame
            Mock DataFrame
        """
        return pd.DataFrame({"content": [file.getvalue()]})

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = True,
        str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>"
    ) -> None:
        """Run the ingestion process for testing.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default True
        str_table_name : str
            The name of the table, by default "<COUNTRY>_<SOURCE>_<TABLE_NAME>"

        Returns
        -------
        None
        """
        pass


# --------------------------
# Type Definitions
# --------------------------
class ReturnStandardizeDataFrame(TypedDict):
    """Type definition for standardize_dataframe return value."""
    
    df_: pd.DataFrame


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def ingestion_instance() -> ConcreteIngestion:
    """Fixture providing ConcreteIngestion instance.

    Returns
    -------
    ConcreteIngestion
        Instance of the ConcreteIngestion class
    """
    return ConcreteIngestion()

@pytest.fixture
def mock_response() -> Response:
    """Fixture providing a mock Response object.

    Returns
    -------
    Response
        Mocked Response object with sample text
    """
    response = MagicMock(spec=Response)
    response.text = "sample content"
    return response

@pytest.fixture
def mock_playwright_page() -> PlaywrightPage:
    """Fixture providing a mock PlaywrightPage object.

    Returns
    -------
    PlaywrightPage
        Mocked PlaywrightPage object with sample text
    """
    page = MagicMock(spec=PlaywrightPage)
    page.text = "sample content"
    return page

@pytest.fixture
def mock_selenium_webdriver() -> SeleniumWebDriver:
    """Fixture providing a mock SeleniumWebDriver object.

    Returns
    -------
    SeleniumWebDriver
        Mocked SeleniumWebDriver object with sample text
    """
    driver = MagicMock(spec=SeleniumWebDriver)
    driver.text = "sample content"
    return driver

@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Fixture providing a sample DataFrame.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with test data
    """
    return pd.DataFrame({
        "col1": ["A", "B", "C"],
        "col2": [1, 2, 3],
        "date_col": ["2023-01-01", "2023-01-02", "2023-01-03"]
    })

@pytest.fixture
def sample_bytes_file() -> BytesIO:
    """Fixture providing a sample BytesIO file.

    Returns
    -------
    BytesIO
        Sample BytesIO file with dummy content
    """
    return BytesIO(b"dummy pdf content")

@pytest.fixture
def mock_fitz_document(mocker: MockerFixture) -> fitz.Document:
    """Fixture providing a mocked fitz.Document.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    fitz.Document
        Mocked fitz.Document with sample text
    """
    doc = MagicMock(spec=fitz.Document)
    doc.__len__.return_value = 2
    doc.__getitem__.side_effect = lambda i: MagicMock(get_text=lambda x: f"page {i} content")
    return doc

@pytest.fixture
def mock_pdfplumber(mocker: MockerFixture) -> Mock:
    """Fixture providing a mocked pdfplumber.open context.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    Mock
        Mocked pdfplumber.open context
    """
    mock_pdf = mocker.patch("pdfplumber.open")
    page = MagicMock()
    page.extract_tables.return_value = [[["header1", "header2"], ["value1", "value2"]]]
    mock_pdf.return_value.__enter__.return_value.pages = [page]
    return mock_pdf

@pytest.fixture
def sample_regex_patterns() -> dict[str, dict[str, str]]:
    """Fixture providing sample regex patterns.

    Returns
    -------
    dict[str, dict[str, str]]
        Sample dictionary of regex patterns
    """
    return {
        "event1": {"pattern1": r"test\d+"},
        "event2": {"pattern2": r"sample\d+"}
    }


# --------------------------
# Tests for ConcreteIngestion
# --------------------------
def test_init_with_no_db_session(ingestion_instance: ConcreteIngestion) -> None:
    """Test initialization without database session.

    Verifies
    -------
    - Instance is created successfully
    - cls_db is None
    - Inherited classes are properly initialized

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class

    Returns
    -------
    None
    """
    assert ingestion_instance.cls_db is None
    assert isinstance(ingestion_instance.cls_db_logs, DBLogs)
    assert isinstance(ingestion_instance.cls_handling_dicts, HandlingDicts)
    assert isinstance(ingestion_instance.cls_str_handler, StrHandler)

def test_init_with_db_session(mocker: MockerFixture) -> None:
    """Test initialization with valid database session.

    Verifies
    -------
    - Instance is created with valid Session
    - cls_db is properly set

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_session = MagicMock(spec=Session)
    instance = ConcreteIngestion(cls_db=mock_session)
    assert instance.cls_db == mock_session

def test_init_type_checker_invalid_db(mocker: MockerFixture) -> None:
    """Test TypeChecker metaclass enforcement for invalid db session.

    Verifies
    -------
    - TypeError is raised for invalid cls_db types
    - Proper error message is generated

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="cls_db must be one of types: Session, NoneType, got str"):
        ConcreteIngestion(cls_db="invalid")

# --------------------------
# Tests for get_response
# --------------------------
def test_get_response_valid(ingestion_instance: ConcreteIngestion) -> None:
    """Test get_response with valid timeout.

    Verifies
    -------
    - Returns Response object
    - Response contains expected text

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class

    Returns
    -------
    None
    """
    result = ingestion_instance.get_response(timeout=(12.0, 21.0))
    assert isinstance(result, Response)
    assert result.text == "mock content"

@pytest.mark.parametrize("timeout", [10, 10.5, (10, 20), (10.5, 20.5)])
def test_get_response_timeout_variations(
    ingestion_instance: ConcreteIngestion, 
    timeout: Union[int, float, tuple]
) -> None:
    """Test get_response with various timeout types.

    Verifies
    -------
    - Handles different valid timeout formats
    - Returns Response object for all cases

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    timeout : Union[int, float, tuple]
        Various timeout formats to test

    Returns
    -------
    None
    """
    result = ingestion_instance.get_response(timeout=timeout)
    assert isinstance(result, Response)
    assert result.text == "mock content"

# --------------------------
# Tests for parse_raw_file
# --------------------------
@pytest.mark.parametrize("resp_type", ["response", "playwright", "selenium"])
def test_parse_raw_file_valid(
    ingestion_instance: ConcreteIngestion,
    mock_response: Response,
    mock_playwright_page: PlaywrightPage,
    mock_selenium_webdriver: SeleniumWebDriver,
    resp_type: str
) -> None:
    """Test parse_raw_file with valid response types.

    Verifies
    -------
    - Returns StringIO for valid response types
    - Correctly handles Response, PlaywrightPage, and SeleniumWebDriver

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    mock_response : Response
        Mocked Response object from fixture
    mock_playwright_page : PlaywrightPage
        Mocked PlaywrightPage object from fixture
    mock_selenium_webdriver : SeleniumWebDriver
        Mocked SeleniumWebDriver object from fixture
    resp_type : str
        Type of response to test

    Returns
    -------
    None
    """
    resp = {
        "response": mock_response,
        "playwright": mock_playwright_page,
        "selenium": mock_selenium_webdriver
    }[resp_type]
    
    result = ingestion_instance.parse_raw_file(resp)
    
    assert isinstance(result, StringIO)
    assert result.getvalue() == "sample content"

# --------------------------
# Tests for transform_data
# --------------------------
def test_transform_data_valid(ingestion_instance: ConcreteIngestion) -> None:
    """Test transform_data with valid StringIO input.

    Verifies
    -------
    - Returns DataFrame with correct content
    - DataFrame has expected structure

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class

    Returns
    -------
    None
    """
    file = StringIO("test content")
    result = ingestion_instance.transform_data(file)
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.iloc[0]["content"] == "test content"

# --------------------------
# Tests for run
# --------------------------
def test_run_no_db_session(ingestion_instance: ConcreteIngestion) -> None:
    """Test run method with no database session.

    Verifies
    -------
    - Runs without errors
    - No database operations performed

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class

    Returns
    -------
    None
    """
    ingestion_instance.run()
    # No assertions needed as method is a no-op in test implementation

# --------------------------
# Tests for standardize_dataframe
# --------------------------
def test_standardize_dataframe_valid_input(
    ingestion_instance: ConcreteIngestion,
    sample_dataframe: pd.DataFrame,
    mocker: MockerFixture
) -> None:
    """Test standardize_dataframe with valid inputs.

    Verifies
    -------
    - DataFrame is standardized correctly
    - DFStandardization and audit_log are called
    - Returns expected DataFrame

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    dict_dtypes = {"col1": str, "col2": int, "date_col": date}
    date_ref = date(2023, 1, 1)
    
    mock_df_standardization = mocker.patch.object(DFStandardization, "pipeline", 
                                                  return_value=sample_dataframe)
    mock_audit_log = mocker.patch.object(DBLogs, "audit_log", return_value=sample_dataframe)
    
    result = ingestion_instance.standardize_dataframe(
        df_=sample_dataframe,
        date_ref=date_ref,
        dict_dtypes=dict_dtypes
    )
    
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, sample_dataframe)
    mock_df_standardization.assert_called_once()
    mock_audit_log.assert_called_once()

@pytest.mark.parametrize("invalid_df", [None, "not_a_dataframe", 123])
def test_standardize_dataframe_invalid_df(
    ingestion_instance: ConcreteIngestion,
    invalid_df: Union[None, str, int]
) -> None:
    """Test standardize_dataframe with invalid DataFrame input.

    Verifies
    -------
    - TypeError is raised for non-DataFrame inputs
    - Error message matches TypeChecker expectation

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    invalid_df : Union[None, str, int]
        Invalid DataFrame inputs

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="df_ must be of type DataFrame"):
        ingestion_instance.standardize_dataframe(
            df_=invalid_df,
            date_ref=date(2023, 1, 1),
            dict_dtypes={"col1": str}
        )

@pytest.mark.parametrize("invalid_date", [None, "not_a_date", 123])
def test_standardize_dataframe_invalid_date(
    ingestion_instance: ConcreteIngestion,
    sample_dataframe: pd.DataFrame,
    invalid_date: Union[None, str, int]
) -> None:
    """Test standardize_dataframe with invalid date input.

    Verifies
    -------
    - TypeError is raised for non-date date_ref inputs
    - Error message matches TypeChecker expectation

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    invalid_date : Union[None, str, int]
        Invalid date inputs

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="date_ref must be of type date"):
        ingestion_instance.standardize_dataframe(
            df_=sample_dataframe,
            date_ref=invalid_date,
            dict_dtypes={"col1": str}
        )

# --------------------------
# Tests for insert_table_db
# --------------------------
def test_insert_table_db_valid(
    ingestion_instance: ConcreteIngestion,
    sample_dataframe: pd.DataFrame,
    mocker: MockerFixture
) -> None:
    """Test insert_table_db with valid inputs.

    Verifies
    -------
    - Database insertion is called with correct parameters
    - No exceptions are raised

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_session = MagicMock()
    mock_insert = mocker.patch.object(mock_session, "insert")
    ingestion_instance.insert_table_db(
        cls_db=mock_session,
        str_table_name="test_table",
        df_=sample_dataframe,
        bool_insert_or_ignore=True
    )
    mock_insert.assert_called_once()

@pytest.mark.parametrize("invalid_db", [None, "not_a_session", 123])
def test_insert_table_db_invalid_session(
    ingestion_instance: ConcreteIngestion,
    sample_dataframe: pd.DataFrame,
    invalid_db: Union[None, str, int]
) -> None:
    """Test insert_table_db with invalid database session.

    Verifies
    -------
    - TypeError is raised for non-Session inputs
    - Error message matches TypeChecker expectation

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    invalid_db : Union[None, str, int]
        Invalid database session inputs

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="cls_db must be of type Session"):
        ingestion_instance.insert_table_db(
            cls_db=invalid_db,
            str_table_name="test_table",
            df_=sample_dataframe
        )

@pytest.mark.parametrize("invalid_table_name,expected_exception,expected_message", [
    (None, TypeError, "str_table_name must be of type str"),
    (123, TypeError, "str_table_name must be of type str"),
    ("", ValueError, "str_table_name cannot be empty")
])
def test_insert_table_db_invalid_table_name(
    ingestion_instance: ConcreteIngestion,
    sample_dataframe: pd.DataFrame,
    invalid_table_name: Union[None, int, str],
    expected_exception: type[Exception],
    expected_message: str
) -> None:
    """Test insert_table_db with invalid table name.

    Verifies
    -------
    - TypeError is raised for non-string table names
    - ValueError is raised for empty table names
    - Error message matches expectation

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    invalid_table_name : Union[None, int, str]
        Invalid table name inputs
    expected_exception : Type[Exception]
        Expected exception type
    expected_message : str
        Expected exception message

    Returns
    -------
    None
    """
    mock_session = MagicMock()
    with pytest.raises(expected_exception, match=expected_message):
        ingestion_instance.insert_table_db(
            cls_db=mock_session,
            str_table_name=invalid_table_name,
            df_=sample_dataframe
        )

def test_insert_table_db_empty_table_name(
    ingestion_instance: ConcreteIngestion,
    sample_dataframe: pd.DataFrame,
) -> None:
    """Test insert_table_db with empty table name.
    
    Verifies
    -------
    - ValueError is raised for empty table names
    - Error message matches expectation

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture

    Returns
    -------
    None
    """
    mock_session = MagicMock()
    with pytest.raises(ValueError, match="str_table_name cannot be empty"):
        ingestion_instance.insert_table_db(
            cls_db=mock_session,
            str_table_name="",
            df_=sample_dataframe
        )

# --------------------------
# Tests for paginate_text_blocks
# --------------------------
def test_paginate_text_blocks_valid(
    ingestion_instance: ConcreteIngestion,
    mock_fitz_document: fitz.Document,
    mocker: MockerFixture
) -> None:
    """Test paginate_text_blocks with valid inputs.

    Verifies
    -------
    - Text blocks are paginated correctly
    - String handler is called for diacritics removal
    - Returns expected list of strings

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    mock_fitz_document : fitz.Document
        Mocked fitz.Document from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch.object(StrHandler, "remove_diacritics_nfkd", side_effect=lambda x, 
                        **kwargs: x.lower())
    
    result = ingestion_instance.paginate_text_blocks(mock_fitz_document, int_pages_join=1)
    
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == "\npage 0 content"
    assert result[1] == "\npage 1 content"

def test_paginate_text_blocks_empty_document(
    ingestion_instance: ConcreteIngestion,
    mocker: MockerFixture
) -> None:
    """Test paginate_text_blocks with empty document.

    Verifies
    -------
    - Returns list with empty string for empty document
    - Handles edge case correctly

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    empty_doc = MagicMock(spec=fitz.Document)
    empty_doc.__len__.return_value = 0
    mocker.patch.object(StrHandler, "remove_diacritics_nfkd", return_value="")
    
    result = ingestion_instance.paginate_text_blocks(empty_doc, int_pages_join=1)
    
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0] == ""

@pytest.mark.parametrize("invalid_pages_join", [-1, 0, "not_an_int"])
def test_paginate_text_blocks_invalid_pages_join(
    ingestion_instance: ConcreteIngestion,
    mock_fitz_document: fitz.Document,
    invalid_pages_join: Union[int, str],
    mocker: MockerFixture
) -> None:
    """Test paginate_text_blocks with invalid pages_join.

    Verifies
    -------
    - ValueError is raised for non-positive pages_join
    - TypeError is raised for non-integer pages_join
    - Error messages match expectations

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    mock_fitz_document : fitz.Document
        Mocked fitz.Document from fixture
    invalid_pages_join : Union[int, str]
        Invalid pages_join inputs
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    if not isinstance(invalid_pages_join, int):
        with pytest.raises(TypeError, match="int_pages_join must be of type int"):
            ingestion_instance.paginate_text_blocks(mock_fitz_document, 
                                                    int_pages_join=invalid_pages_join)
    else:
        with pytest.raises(ValueError, match="int_pages_join must be a positive integer"):
            ingestion_instance.paginate_text_blocks(mock_fitz_document, 
                                                    int_pages_join=invalid_pages_join)

# --------------------------
# Tests for get_file
# --------------------------
@pytest.mark.parametrize("resp_type", ["response", "playwright", "selenium"])
def test_get_file_valid(
    ingestion_instance: ConcreteIngestion,
    mock_response: Response,
    mock_playwright_page: PlaywrightPage,
    mock_selenium_webdriver: SeleniumWebDriver,
    resp_type: str
) -> None:
    """Test get_file with valid response types.

    Verifies
    -------
    - Returns StringIO for valid response types
    - Correctly handles Response, PlaywrightPage, and SeleniumWebDriver

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    mock_response : Response
        Mocked Response object from fixture
    mock_playwright_page : PlaywrightPage
        Mocked PlaywrightPage object from fixture
    mock_selenium_webdriver : SeleniumWebDriver
        Mocked SeleniumWebDriver object from fixture
    resp_type : str
        Type of response to test

    Returns
    -------
    None
    """
    resp = {
        "response": mock_response,
        "playwright": mock_playwright_page,
        "selenium": mock_selenium_webdriver
    }[resp_type]
    
    result = ingestion_instance.get_file(resp)
    
    assert isinstance(result, StringIO)
    assert result.getvalue() == "sample content"

@pytest.mark.parametrize("invalid_resp", [None, "not_a_response", 123])
def test_get_file_invalid_response(
    ingestion_instance: ConcreteIngestion,
    invalid_resp: Union[None, str, int]
) -> None:
    """Test get_file with invalid response inputs.

    Verifies
    -------
    - TypeError is raised for invalid response types
    - Error message matches TypeChecker expectation

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    invalid_resp : Union[None, str, int]
        Invalid response inputs

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, 
                       match="resp_req must be one of types: Response, Page, WebDriver"):
        ingestion_instance.get_file(invalid_resp)

# --------------------------
# Tests for pdf_docx_tables_response
# --------------------------
def test_pdf_docx_tables_response_valid(
    ingestion_instance: ConcreteIngestion,
    sample_bytes_file: BytesIO,
    mock_pdfplumber: Mock,
    mocker: MockerFixture
) -> None:
    """Test pdf_docx_tables_response with valid input.

    Verifies
    -------
    - Correctly parses tables from BytesIO
    - Returns expected DataFrame
    - HandlingDicts is called correctly

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    sample_bytes_file : BytesIO
        Sample BytesIO file from fixture
    mock_pdfplumber : Mock
        Mocked pdfplumber.open context from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch.object(
        HandlingDicts,
        "pair_keys_with_values",
        return_value=[{"header1": "value1", "header2": "value2"}]
    )
    
    result = ingestion_instance.pdf_docx_tables_response(sample_bytes_file)
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert list(result.columns) == ["header1", "header2"]

@pytest.mark.parametrize("invalid_file", [None, "not_a_bytesio", 123])
def test_pdf_docx_tables_response_invalid_file(
    ingestion_instance: ConcreteIngestion,
    invalid_file: Union[None, str, int]
) -> None:
    """Test pdf_docx_tables_response with invalid file input.

    Verifies
    -------
    - TypeError is raised for non-BytesIO inputs
    - Error message matches TypeChecker expectation

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    invalid_file : Union[None, str, int]
        Invalid file inputs

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="bytes_file must be of type BytesIO"):
        ingestion_instance.pdf_docx_tables_response(invalid_file)

# --------------------------
# Tests for pdf_docx_regex
# --------------------------
def test_pdf_docx_regex_valid(
    ingestion_instance: ConcreteIngestion,
    sample_bytes_file: BytesIO,
    sample_regex_patterns: dict[str, dict[str, str]],
    mocker: MockerFixture
) -> None:
    """Test pdf_docx_regex with valid inputs.

    Verifies
    -------
    - Correctly processes PDF with regex patterns
    - Returns expected DataFrame
    - Handles pagination and regex matching

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    sample_bytes_file : BytesIO
        Sample BytesIO file from fixture
    sample_regex_patterns : dict[str, dict[str, str]]
        Sample regex patterns from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch("fitz.open", return_value=MagicMock(spec=fitz.Document))
    mocker.patch.object(ContentAggregator, "paginate_text_blocks", return_value=["test123"])
    mocker.patch.object(
        ContentParser,
        "_regex_patterns_match",
        return_value=[{"EVENT": "event1", "MATCH_PATTERN": "pattern1"}]
    )
    
    result = ingestion_instance.pdf_docx_regex(
        bytes_file=sample_bytes_file,
        str_file_extension="pdf",
        int_pages_join=1,
        dict_regex_patterns=sample_regex_patterns
    )
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert "EVENT" in result.columns

@pytest.mark.parametrize("invalid_extension", [None, 123, ""])
def test_pdf_docx_regex_invalid_extension(
    ingestion_instance: ConcreteIngestion,
    sample_bytes_file: BytesIO,
    sample_regex_patterns: dict[str, dict[str, str]],
    invalid_extension: Union[None, int, str],
    mocker: MockerFixture
) -> None:
    """Test pdf_docx_regex with invalid file extension.

    Verifies
    -------
    - TypeError is raised for non-string or empty file extensions
    - Error message matches TypeChecker expectation

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    sample_bytes_file : BytesIO
        Sample BytesIO file from fixture
    sample_regex_patterns : dict[str, dict[str, str]]
        Sample regex patterns from fixture
    invalid_extension : Union[None, int, str]
        Invalid file extension inputs
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch(
        "fitz.open",
        side_effect=lambda *args, **kwargs: (
            ValueError("str_file_extension cannot be empty")
            if kwargs.get("filetype", "") == ""
            else MagicMock(spec=fitz.Document)
        )
    )
    
    with pytest.raises(TypeError, match="must be of type"):
        ingestion_instance.pdf_docx_regex(
            bytes_file=sample_bytes_file,
            str_file_extension=invalid_extension,
            int_pages_join=1,
            dict_regex_patterns=sample_regex_patterns
        )

# --------------------------
# Tests for _regex_patterns_match
# --------------------------
def test_regex_patterns_match_valid(
    ingestion_instance: ConcreteIngestion,
    sample_regex_patterns: dict[str, dict[str, str]],
    mocker: MockerFixture
) -> None:
    """Test _regex_patterns_match with valid inputs.

    Verifies
    -------
    - Correctly matches regex patterns
    - Returns expected list of matches
    - Handles regex groups correctly

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    sample_regex_patterns : dict[str, dict[str, str]]
        Sample regex patterns from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch.object(StrHandler, "remove_diacritics_nfkd", side_effect=lambda x, **kwargs: x)
    mock_match = MagicMock()
    mock_match.groups.return_value = ["test"]
    mock_match.group.side_effect = lambda i: "test" if i == 0 else "group1"
    mocker.patch(
        "re.search",
        side_effect=lambda pattern, string: mock_match if pattern == r"test\d+" else None
    )
    result = ingestion_instance._regex_patterns_match(
        list_blocks_pages=["test123"],
        dict_regex_patterns=sample_regex_patterns
    )
    assert isinstance(result, list)
    assert len(result) == 2  # One match for event1, one fallback for event2
    assert result[0]["EVENT"] == "EVENT1"
    assert result[0]["MATCH_PATTERN"] == "PATTERN1"
    assert result[0]["REGEX_GROUP_0"] == "TEST"
    assert result[0]["REGEX_GROUP_1"] == "GROUP1"
    assert result[1]["EVENT"] == "EVENT2"
    assert result[1]["MATCH_PATTERN"] == "ZZNN/A"

def test_regex_patterns_match_no_matches(
    ingestion_instance: ConcreteIngestion,
    sample_regex_patterns: dict[str, dict[str, str]],
    mocker: MockerFixture
) -> None:
    """Test _regex_patterns_match with no matches.

    Verifies
    -------
    - Returns fallback entries when no matches found
    - Correctly handles empty matches

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    sample_regex_patterns : dict[str, dict[str, str]]
        Sample regex patterns from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch.object(StrHandler, "remove_diacritics_nfkd", side_effect=lambda x, **kwargs: x)
    mocker.patch("re.search", return_value=None)
    
    result = ingestion_instance._regex_patterns_match(
        list_blocks_pages=["no match"],
        dict_regex_patterns=sample_regex_patterns
    )
    
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["MATCH_PATTERN"] == "ZZNN/A"
    assert result[0]["PATTERN_REGEX"] == "ZZN/A"

@pytest.mark.parametrize("invalid_blocks", [None, "not_a_list", 123])
def test_regex_patterns_match_invalid_blocks(
    ingestion_instance: ConcreteIngestion,
    sample_regex_patterns: dict[str, dict[str, str]],
    invalid_blocks: Union[None, str, int]
) -> None:
    """Test _regex_patterns_match with invalid text blocks.

    Verifies
    -------
    - TypeError is raised for non-list text blocks
    - Error message matches TypeChecker expectation

    Parameters
    ----------
    ingestion_instance : ConcreteIngestion
        Instance of the ConcreteIngestion class
    sample_regex_patterns : dict[str, dict[str, str]]
        Sample regex patterns from fixture
    invalid_blocks : Union[None, str, int]
        Invalid text block inputs

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="list_blocks_pages must be of type list"):
        ingestion_instance._regex_patterns_match(
            list_blocks_pages=invalid_blocks,
            dict_regex_patterns=sample_regex_patterns
        )

# --------------------------
# Test Module Reload
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
    """Test ConcreteIngestion class behavior.

    Verifies
    -------
    - Class instance maintains functionality
    - Methods are accessible

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch("fitz.open", return_value=MagicMock(spec=fitz.Document))
    
    instance = ConcreteIngestion()
    assert isinstance(instance, ConcreteIngestion)
    assert hasattr(instance, "get_file")