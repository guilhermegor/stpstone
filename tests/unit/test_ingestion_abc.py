"""Unit tests for ingestion operations.

Tests the functionality of ABCIngestion, CoreIngestion, ContentAggregator, ContentParser,
and ABCIngestionOperations classes, covering initialization, data processing, and error handling.
"""

from datetime import date
import importlib
from io import BytesIO, StringIO
import sys
from typing import Any, Union
from unittest.mock import MagicMock

import fitz
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestion,
    ABCIngestionOperations,
    ContentAggregator,
    ContentParser,
    CoreIngestion,
)
from stpstone.utils.loggs.db_logs import DBLogs
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
    """Fixture providing a mock Response object.

    Returns
    -------
    Response
        Mocked Response object with sample text content.
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
        Mocked PlaywrightPage object with sample text content.
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
        Mocked SeleniumWebDriver object with sample text content.
    """
    driver = MagicMock(spec=SeleniumWebDriver)
    driver.text = "sample content"
    return driver


@pytest.fixture
def mock_session() -> Session:
    """Fixture providing a mock Session object.

    Returns
    -------
    Session
        Mocked Session object with insert method.
    """
    session = MagicMock(spec=Session)
    session.insert = MagicMock()
    return session


@pytest.fixture
def mock_bytes_file() -> BytesIO:
    """Fixture providing a mock BytesIO object.

    Returns
    -------
    BytesIO
        Mocked BytesIO object with sample content.
    """
    return BytesIO(b"sample pdf content")


@pytest.fixture
def mock_fitz_document() -> fitz.Document:
    """Fixture providing a mock fitz.Document object.

    Returns
    -------
    fitz.Document
        Mocked fitz.Document with sample text pages.
    """
    doc = MagicMock(spec=fitz.Document)
    doc.__len__.return_value = 2
    doc.__getitem__.side_effect = lambda i: MagicMock(
        get_text=lambda x: f"page {i} content"
    )
    return doc


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Fixture providing a sample DataFrame.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with test data.
    """
    return pd.DataFrame({
        "col1": ["A", "B", "C"],
        "col2": [1, 2, 3],
        "date": ["2023-01-01", "2023-01-02", "2023-01-03"]
    })


@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample date.

    Returns
    -------
    date
        Sample date object.
    """
    return date(2023, 1, 1)


@pytest.fixture
def sample_dtypes() -> dict[str, Union[str, int, date]]:
    """Fixture providing a sample dictionary of data types.

    Returns
    -------
    dict[str, Union[str, int, date]]
        Dictionary mapping column names to their expected types.
    """
    return {
        "col1": str,
        "col2": int,
        "date": str  # Changed from date to str to fix pandas compatibility
    }


@pytest.fixture
def sample_regex_patterns() -> dict[str, dict[str, str]]:
    """Fixture providing sample regex patterns.

    Returns
    -------
    dict[str, dict[str, str]]
        Dictionary of regex patterns for testing.
    """
    return {
        "event1": {"pattern1": r"test\d+"},
        "event2": {"pattern2": r"sample\d+"}
    }


# --------------------------
# Tests for ABCIngestion
# --------------------------
def test_abc_ingestion_init(mock_session: Session) -> None:
    """Test initialization of ABCIngestion with valid and None inputs.

    Verifies
    --------
    - Initialization with a valid Session object
    - Initialization with None
    - Correct storage of cls_db attribute

    Returns
    -------
    None
    """
    class TestIngestion(ABCIngestion):
        def __init__(self, cls_db=None):
            super().__init__(cls_db)
            
        def get_response(self, timeout=(12.0, 21.0)):
            pass
        def parse_raw_file(self, resp_req):
            pass
        def transform_data(self, file):
            pass
        def run(self, timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=True, str_table_name="table"):
            pass

    ingestion = TestIngestion(cls_db=mock_session)
    assert ingestion.cls_db == mock_session

    ingestion_none = TestIngestion(cls_db=None)
    assert ingestion_none.cls_db is None


def test_abc_ingestion_abstract_methods() -> None:
    """Test that ABCIngestion abstract methods raise NotImplementedError.

    Verifies
    --------
    - All abstract methods raise NotImplementedError when called

    Returns
    -------
    None
    """
    class TestIngestion(ABCIngestion):
        def __init__(self, cls_db=None):
            super().__init__(cls_db)

    ingestion = TestIngestion()
    
    with pytest.raises(NotImplementedError):
        ingestion.get_response()
    
    with pytest.raises(NotImplementedError):
        ingestion.parse_raw_file(None)
    
    with pytest.raises(NotImplementedError):
        ingestion.transform_data(None)
    
    with pytest.raises(NotImplementedError):
        ingestion.run()


# --------------------------
# Tests for CoreIngestion
# --------------------------
def test_core_ingestion_init() -> None:
    """Test initialization of CoreIngestion.

    Verifies
    --------
    - Correct initialization of cls_db_logs
    - Instance creation without errors

    Returns
    -------
    None
    """
    ingestion = CoreIngestion()
    assert isinstance(ingestion.cls_db_logs, DBLogs)


def test_standardize_dataframe(
    mocker: MockerFixture,
    sample_dataframe: pd.DataFrame,
    sample_date: date,
    sample_dtypes: dict[str, Union[str, int, date]]
) -> None:
    """Test standardize_dataframe method with valid inputs.

    Verifies
    --------
    - Correct standardization of DataFrame
    - Proper interaction with DFStandardization and DBLogs
    - Return type is pd.DataFrame

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    sample_date : date
        Sample date from fixture
    sample_dtypes : dict[str, Union[str, int, date]]
        Sample data types from fixture

    Returns
    -------
    None
    """
    ingestion = CoreIngestion()
    mock_df_standardization = mocker.patch(
        "stpstone.transformations.standardization.standardizer_df.DFStandardization"
    )
    mock_df_standardization.return_value.pipeline.return_value = sample_dataframe
    mock_audit_log = mocker.patch.object(DBLogs, "audit_log", return_value=sample_dataframe)

    result = ingestion.standardize_dataframe(
        df_=sample_dataframe,
        date_ref=sample_date,
        dict_dtypes=sample_dtypes,
        str_data_fillna="-99999"
    )

    assert isinstance(result, pd.DataFrame)
    mock_df_standardization.assert_called_once()
    mock_audit_log.assert_called_once_with(
        sample_dataframe, None, sample_date, True
    )


def test_standardize_dataframe_invalid_types(
    sample_date: date,
    sample_dtypes: dict[str, Union[str, int, date]]
) -> None:
    """Test standardize_dataframe with invalid DataFrame type.

    Verifies
    --------
    - Raises TypeError for non-DataFrame input
    - Error message contains "must be of type"

    Parameters
    ----------
    sample_date : date
        Sample date from fixture
    sample_dtypes : dict[str, Union[str, int, date]]
        Sample data types from fixture

    Returns
    -------
    None
    """
    ingestion = CoreIngestion()
    with pytest.raises(TypeError, match="must be of type"):
        ingestion.standardize_dataframe(
            df_="not a dataframe",
            date_ref=sample_date,
            dict_dtypes=sample_dtypes
        )


def test_insert_table_db(
    mock_session: Session,
    sample_dataframe: pd.DataFrame
) -> None:
    """Test insert_table_db method with valid inputs.

    Verifies
    --------
    - Correct interaction with Session.insert
    - Proper conversion of DataFrame to records
    - Handling of bool_insert_or_ignore parameter

    Parameters
    ----------
    mock_session : Session
        Mocked Session object from fixture
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture

    Returns
    -------
    None
    """
    ingestion = CoreIngestion()

    ingestion.insert_table_db(
        cls_db=mock_session,
        str_table_name="test_table",
        df_=sample_dataframe,
        bool_insert_or_ignore=True
    )

    mock_session.insert.assert_called_once_with(
        sample_dataframe.to_dict(orient="records"),
        str_table_name="test_table",
        bool_insert_or_ignore=True
    )


# --------------------------
# Tests for ContentAggregator
# --------------------------
def test_content_aggregator_init() -> None:
    """Test initialization of ContentAggregator.

    Verifies
    --------
    - Correct initialization of cls_handling_dicts and cls_str_handler
    - Instance creation without errors

    Returns
    -------
    None
    """
    aggregator = ContentAggregator()
    assert isinstance(aggregator.cls_handling_dicts, HandlingDicts)
    assert isinstance(aggregator.cls_str_handler, StrHandler)


def test_paginate_text_blocks(
    mocker: MockerFixture,
    mock_fitz_document: fitz.Document
) -> None:
    """Test paginate_text_blocks method with valid inputs.

    Verifies
    --------
    - Correct pagination of text blocks
    - Proper handling of page joining
    - Interaction with StrHandler for diacritics removal

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    mock_fitz_document : fitz.Document
        Mocked fitz.Document from fixture

    Returns
    -------
    None
    """
    aggregator = ContentAggregator()
    mocker.patch.object(
        aggregator.cls_str_handler,
        "remove_diacritics_nfkd",
        return_value="cleaned text"
    )

    result = aggregator.paginate_text_blocks(
        stream_file=mock_fitz_document,
        int_pages_join=1
    )

    assert isinstance(result, list)
    assert len(result) == 2
    assert result == ["cleaned text", "cleaned text"]


def test_paginate_text_blocks_empty_document(
    mocker: MockerFixture
) -> None:
    """Test paginate_text_blocks with empty document.

    Verifies
    --------
    - Returns list with empty string for empty document
    - Handles edge case correctly

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    aggregator = ContentAggregator()
    mock_doc = MagicMock(spec=fitz.Document)
    mock_doc.__len__.return_value = 0

    result = aggregator.paginate_text_blocks(
        stream_file=mock_doc,
        int_pages_join=1
    )

    assert result == [""]


# --------------------------
# Tests for ContentParser
# --------------------------
def test_get_file(mock_response: Response) -> None:
    """Test get_file method with valid response.

    Verifies
    --------
    - Correct conversion of response text to StringIO
    - Return type is StringIO

    Parameters
    ----------
    mock_response : Response
        Mocked Response object from fixture

    Returns
    -------
    None
    """
    parser = ContentParser()
    result = parser.get_file(mock_response)
    assert isinstance(result, StringIO)
    assert result.getvalue() == "sample content"


def test_pdf_docx_tables_response(
    mocker: MockerFixture,
    mock_bytes_file: BytesIO
) -> None:
    """Test pdf_docx_tables_response with valid BytesIO input.

    Verifies
    --------
    - Correct parsing of tables from PDF
    - Interaction with pdfplumber and HandlingDicts
    - Return type is pd.DataFrame

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    mock_bytes_file : BytesIO
        Mocked BytesIO object from fixture

    Returns
    -------
    None
    """
    parser = ContentParser()
    mock_pdfplumber = mocker.patch("pdfplumber.open")
    mock_page = MagicMock()
    mock_page.extract_tables.return_value = [["header1", "header2"], ["data1", "data2"]]
    mock_pdfplumber.return_value.__enter__.return_value.pages = [mock_page]
    mocker.patch.object(
        parser.cls_handling_dicts,
        "pair_keys_with_values",
        return_value=[{"header1": "data1", "header2": "data2"}]
    )

    result = parser.pdf_docx_tables_response(mock_bytes_file)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.to_dict(orient="records") == [{"header1": "data1", "header2": "data2"}]


def test_pdf_docx_regex(
    mocker: MockerFixture,
    mock_bytes_file: BytesIO,
    sample_regex_patterns: dict[str, dict[str, str]]
) -> None:
    """Test pdf_docx_regex with valid inputs.

    Verifies
    --------
    - Correct parsing using regex patterns
    - Interaction with paginate_text_blocks and regex matching
    - Proper DataFrame processing (drop duplicates, sorting)

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    mock_bytes_file : BytesIO
        Mocked BytesIO object from fixture
    sample_regex_patterns : dict[str, dict[str, str]]
        Sample regex patterns from fixture

    Returns
    -------
    None
    """
    parser = ContentParser()
    # Mock fitz.open to avoid actual PDF parsing
    mock_fitz_open = mocker.patch("fitz.open")
    mock_doc = MagicMock()
    mock_fitz_open.return_value = mock_doc
    
    mocker.patch.object(
        parser,
        "paginate_text_blocks",
        return_value=["test123", "sample456"]
    )
    mocker.patch.object(
        parser,
        "_regex_patterns_match",
        return_value=[
            {"EVENT": "EVENT1", "MATCH_PATTERN": "PATTERN1", "PATTERN_REGEX": r"test\d+"}
        ]
    )

    result = parser.pdf_docx_regex(
        bytes_file=mock_bytes_file,
        str_file_extension="pdf",
        int_pages_join=1,
        dict_regex_patterns=sample_regex_patterns
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.to_dict(orient="records") == [
        {"EVENT": "EVENT1", "MATCH_PATTERN": "PATTERN1", "PATTERN_REGEX": r"test\d+"}
    ]


def test_regex_patterns_match(
    mocker: MockerFixture,
    sample_regex_patterns: dict[str, dict[str, str]]
) -> None:
    """Test _regex_patterns_match with valid inputs.

    Verifies
    --------
    - Correct matching of regex patterns
    - Proper handling of matches and fallbacks
    - Return type is list of dictionaries

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    sample_regex_patterns : dict[str, dict[str, str]]
        Sample regex patterns from fixture

    Returns
    -------
    None
    """
    parser = ContentParser()
    mocker.patch.object(
        parser.cls_str_handler,
        "remove_diacritics_nfkd",
        return_value=r"test\d+"
    )
    
    # Mock re.search to return match only for event1
    def mock_search(pattern, text):
        if pattern == r"test\d+" and "test123" in text:
            return MagicMock(
                group=lambda i: "test123" if i == 0 else "123",
                groups=lambda: ["123"]
            )
        return None
    
    mocker.patch("re.search", side_effect=mock_search)

    result = parser._regex_patterns_match(
        list_blocks_pages=["test123"],
        dict_regex_patterns=sample_regex_patterns
    )

    assert isinstance(result, list)
    assert len(result) == 2  # One match for event1, one fallback for event2
    assert result[0]["EVENT"] == "EVENT1"
    assert result[0]["MATCH_PATTERN"] == "PATTERN1"
    assert result[0]["REGEX_GROUP_0"] == "TEST123"
    assert result[1]["EVENT"] == "EVENT2"
    assert result[1]["MATCH_PATTERN"] == "ZZNN/A"


def test_regex_patterns_match_no_matches(
    mocker: MockerFixture,
    sample_regex_patterns: dict[str, dict[str, str]]
) -> None:
    """Test _regex_patterns_match with no matches.

    Verifies
    --------
    - Fallback behavior when no regex matches
    - Correct return of fallback dictionary

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    sample_regex_patterns : dict[str, dict[str, str]]
        Sample regex patterns from fixture

    Returns
    -------
    None
    """
    parser = ContentParser()
    mocker.patch.object(
        parser.cls_str_handler,
        "remove_diacritics_nfkd",
        return_value=r"test\d+"
    )
    mocker.patch("re.search", return_value=None)

    result = parser._regex_patterns_match(
        list_blocks_pages=["no match"],
        dict_regex_patterns=sample_regex_patterns
    )

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == {
        "EVENT": "EVENT1",
        "MATCH_PATTERN": "ZZNN/A",
        "PATTERN_REGEX": "ZZN/A"
    }
    assert result[1] == {
        "EVENT": "EVENT2",
        "MATCH_PATTERN": "ZZNN/A",
        "PATTERN_REGEX": "ZZN/A"
    }


# --------------------------
# Tests for ABCIngestionOperations
# --------------------------
def test_abc_ingestion_operations_init(mock_session: Session) -> None:
    """Test initialization of ABCIngestionOperations.

    Verifies
    --------
    - Correct initialization of inherited attributes
    - Proper setup of cls_db, cls_db_logs, cls_handling_dicts, and cls_str_handler

    Parameters
    ----------
    mock_session : Session
        Mocked Session object from fixture

    Returns
    -------
    None
    """
    # Create a concrete implementation for testing
    class TestABCIngestionOperations(ABCIngestionOperations):
        def __init__(self, cls_db=None):
            super().__init__(cls_db)
            
        def get_response(self, timeout=(12.0, 21.0)):
            pass
        def parse_raw_file(self, resp_req):
            pass
        def transform_data(self, file):
            pass
        def run(self, timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=True, str_table_name="table"):
            pass
    
    ingestion = TestABCIngestionOperations(cls_db=mock_session)
    assert ingestion.cls_db == mock_session
    assert isinstance(ingestion.cls_db_logs, DBLogs)
    assert isinstance(ingestion.cls_handling_dicts, HandlingDicts)
    assert isinstance(ingestion.cls_str_handler, StrHandler)


# --------------------------
# Edge Cases and Error Conditions
# --------------------------
@pytest.mark.parametrize("invalid_timeout", [
    "invalid",  # wrong type
    -1,         # negative value
    (0, 0),     # zero values
])
def test_get_response_invalid_timeout(invalid_timeout: Any) -> None:
    """Test get_response with invalid timeout values.

    Verifies
    --------
    - Raises TypeError for invalid timeout types/values

    Parameters
    ----------
    invalid_timeout : Any
        Invalid timeout value to test

    Returns
    -------
    None
    """
    class TestIngestion(ABCIngestion):
        def __init__(self, cls_db=None):
            super().__init__(cls_db)
            
        def get_response(self, timeout=(12.0, 21.0)):
            return super().get_response(timeout)
        def parse_raw_file(self, resp_req):
            pass
        def transform_data(self, file):
            pass
        def run(self, timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=True, str_table_name="table"):
            pass

    ingestion = TestIngestion()
    with pytest.raises(TypeError, match="must be of type"):
        ingestion.get_response(timeout=invalid_timeout)


def test_parse_raw_file_invalid_input() -> None:
    """Test parse_raw_file with invalid input type.

    Verifies
    --------
    - Raises TypeError for invalid response type

    Returns
    -------
    None
    """
    class TestIngestion(ABCIngestion):
        def __init__(self, cls_db=None):
            super().__init__(cls_db)
            
        def get_response(self, timeout=(12.0, 21.0)):
            pass
        def parse_raw_file(self, resp_req):
            return super().parse_raw_file(resp_req)
        def transform_data(self, file):
            pass
        def run(self, timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=True, str_table_name="table"):
            pass

    ingestion = TestIngestion()
    with pytest.raises(TypeError, match="must be of type"):
        ingestion.parse_raw_file("invalid")


def test_transform_data_invalid_input() -> None:
    """Test transform_data with invalid input type.

    Verifies
    --------
    - Raises TypeError for invalid file input

    Returns
    -------
    None
    """
    class TestIngestion(ABCIngestion):
        def __init__(self, cls_db=None):
            super().__init__(cls_db)
            
        def get_response(self, timeout=(12.0, 21.0)):
            pass
        def parse_raw_file(self, resp_req):
            pass
        def transform_data(self, file):
            return super().transform_data(file)
        def run(self, timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=True, str_table_name="table"):
            pass

    ingestion = TestIngestion()
    with pytest.raises(TypeError, match="must be of type"):
        ingestion.transform_data("invalid")


def test_run_invalid_table_name() -> None:
    """Test run method with invalid table name.

    Verifies
    --------
    - Raises TypeError for non-string table name

    Returns
    -------
    None
    """
    class TestIngestion(ABCIngestion):
        def __init__(self, cls_db=None):
            super().__init__(cls_db)
            
        def get_response(self, timeout=(12.0, 21.0)):
            pass
        def parse_raw_file(self, resp_req):
            pass
        def transform_data(self, file):
            pass
        def run(self, timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=True, str_table_name="table"):
            return super().run(timeout, bool_verify, bool_insert_or_ignore, str_table_name)

    ingestion = TestIngestion()
    with pytest.raises(TypeError, match="must be of type"):
        ingestion.run(str_table_name=123)


# --------------------------
# Reload Logic Tests
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
    """Test module reload behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - Classes maintain their functionality post-reload

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    importlib.reload(sys.modules["stpstone.ingestion.abc.ingestion_abc"])
    
    # Create a concrete implementation for testing
    class TestABCIngestionOperations(ABCIngestionOperations):
        def __init__(self, cls_db=None):
            super().__init__(cls_db)
            
        def get_response(self, timeout=(12.0, 21.0)):
            pass
        def parse_raw_file(self, resp_req):
            pass
        def transform_data(self, file):
            pass
        def run(self, timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=True, str_table_name="table"):
            pass
    
    ingestion = TestABCIngestionOperations()
    assert isinstance(ingestion.cls_db_logs, DBLogs)
    assert isinstance(ingestion.cls_handling_dicts, HandlingDicts)
    assert isinstance(ingestion.cls_str_handler, StrHandler)


# --------------------------
# Fallback Logic Tests
# --------------------------
def test_pdf_docx_regex_no_pages(
    mocker: MockerFixture,
    mock_bytes_file: BytesIO,
    sample_regex_patterns: dict[str, dict[str, str]]
) -> None:
    """Test pdf_docx_regex with empty document.

    Verifies
    --------
    - Fallback behavior when document has no pages
    - Returns empty DataFrame

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    mock_bytes_file : BytesIO
        Mocked BytesIO object from fixture
    sample_regex_patterns : dict[str, dict[str, str]]
        Sample regex patterns from fixture

    Returns
    -------
    None
    """
    parser = ContentParser()
    # Mock fitz.open to avoid actual PDF parsing
    mock_fitz_open = mocker.patch("fitz.open")
    mock_doc = MagicMock()
    mock_fitz_open.return_value = mock_doc
    
    mocker.patch.object(
        parser,
        "paginate_text_blocks",
        return_value=[]
    )

    result = parser.pdf_docx_regex(
        bytes_file=mock_bytes_file,
        str_file_extension="pdf",
        int_pages_join=1,
        dict_regex_patterns=sample_regex_patterns
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0